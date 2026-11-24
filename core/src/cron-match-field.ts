// Split from cron.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

export interface CronJob {
  id: string;
  cron: string;
  prompt: string;
  /** Optional human-readable label chosen by the caller so a job is identifiable by
   *  purpose (not just its opaque auto-generated id) in list/get/delete flows.
   *  Mirrors the reference's required trigger `name` field (src/skills/bundled/scheduleRemoteAgents.ts).
   *  AMI keeps it OPTIONAL (superset): an unnamed job still works and is addressed by id. */
  name?: string;
  recurring: boolean;
  durable: boolean;
  createdAt: number;
  lastRun?: number;
  lastFiredAt?: number;
  nextRun?: number;
  maxAgeDays?: number;
  /** Teammate/sub-agent that owns this job. Used to scope listing + authorize
   *  deletion so one agent cannot see or remove another's jobs. */
  agentId?: string;
  /** Permanent jobs are exempt from age-based expiry. */
  permanent?: boolean;
  /** When true, the 5-field cron expression is interpreted in UTC instead of the
   *  host's local wall clock, so the job fires on the same absolute instant no
   *  matter which timezone the session runs in (server/remote-trigger parity).
   *  Undefined/false = local time (the default). Mirrors the reference's `{ utc }` cron
   *  handling for server/CCR-defined schedules (src/utils/cron.ts). */
  utc?: boolean;
  /** Optional per-job run budget. A recurring job is retired (stops firing and is
   *  removed) once it has fired `maxRuns` times. Undefined = unlimited. */
  maxRuns?: number;
  /** How many times this job has fired. Only tracked when `maxRuns` is set. */
  runCount?: number;
  /** Optional per-job MODEL override (parity with the reference's scheduled-job
   *  `session_context.model`, scheduleRemoteAgents.ts:L206/L289). When set, the fired
   *  prompt instructs the run to use this model instead of whatever the session
   *  currently has. Undefined = inherit the session model. Surfaced in the fire notice. */
  model?: string;
  /** Optional per-job ALLOWED-TOOLS whitelist (parity with the reference's scheduled-job
   *  `allowed_tools`, scheduleRemoteAgents.ts:L210). When set, the fired prompt is
   *  restricted to this toolset. Undefined = inherit the session tools. Surfaced in the
   *  fire notice. */
  allowedTools?: string[];
}

export const MAX_AGE_DAYS = 7;

/** Upper bound on a job's human-readable name. A name is a short label, not a
 *  document; anything longer is almost always a mistake and would bloat the
 *  persisted store, so it is rejected as clean validation. */
export const MAX_CRON_NAME_LEN = 200;

export const MAX_JOBS = 50;

export const LOCK_STALE_MS = 120_000;

/** Recurring jobs may not fire more often than once per minute (abuse floor). */
export const MIN_INTERVAL_MS = 60_000;

/** Tunable jitter parameters (recurring forward jitter vs one-shot backward jitter). */
export interface CronJitterConfig {
  /** Fraction of the inter-fire gap used as the max forward jitter for recurring jobs. */
  recurringFrac: number;
  /** Hard cap (ms) on recurring forward jitter. */
  recurringCapMs: number;
  /** Max backward (early) jitter (ms) for one-shot jobs. */
  oneShotMaxMs: number;
  /** Minimum backward (early) jitter (ms) for one-shot jobs: a non-pinned one-shot
   *  always fires at least this far ahead of its nominal time so it never lands
   *  exactly on a round instant shared by the herd. Must be ≤ oneShotMaxMs; 0
   *  disables the floor (pure 0..max spread). Mirrors the reference's CronJitterConfig
   *  oneShotFloorMs (src/utils/cronJitterConfig.ts). */
  oneShotFloorMs: number;
  /** One-shot jitter only applies when the fire minute is divisible by this (preserves :00/:30 pins). */
  oneShotMinuteMod: number;
}

/**
 * PARITY (additive): the host's resolved IANA timezone name (e.g.
 * "America/New_York"), or "local" as a fallback when the runtime does not expose
 * Intl timezone data. Mirrors the reference's `Intl.DateTimeFormat().resolvedOptions().timeZone`
 * surfacing so cron output/guidance can state WHICH timezone a non-UTC schedule is
 * interpreted in — a bare "9:00 AM" cannot otherwise be told apart from UTC.
 */
export function localTimeZoneLabel(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || 'local';
  } catch {
    return 'local';
  }
}

/** Cross-field validation of a jitter config (mirrors the reference's zod `.refine` on
 *  CronJitterConfig): every knob must be a finite, non-negative number and the
 *  one-shot floor can never exceed the one-shot cap (an inverted window would make
 *  oneShotBackJitter produce a negative/empty span). Throws a clear error on an
 *  invalid config so a mis-specified scheduler fails fast at construction rather
 *  than silently mis-jittering every one-shot fire. */
export function validateJitterConfig(cfg: CronJitterConfig): void {
  const fields: Array<[keyof CronJitterConfig, number]> = [
    ['recurringFrac', cfg.recurringFrac],
    ['recurringCapMs', cfg.recurringCapMs],
    ['oneShotMaxMs', cfg.oneShotMaxMs],
    ['oneShotFloorMs', cfg.oneShotFloorMs],
    ['oneShotMinuteMod', cfg.oneShotMinuteMod],
  ];
  for (const [name, value] of fields) {
    if (!Number.isFinite(value) || value < 0) {
      throw new Error(`CronJitterConfig.${String(name)} must be a non-negative finite number (got ${value}).`);
    }
  }
  if (cfg.oneShotMinuteMod < 1) {
    throw new Error(`CronJitterConfig.oneShotMinuteMod must be ≥ 1 (got ${cfg.oneShotMinuteMod}).`);
  }
  if (cfg.oneShotFloorMs > cfg.oneShotMaxMs) {
    throw new Error(`CronJitterConfig.oneShotFloorMs (${cfg.oneShotFloorMs}) must not exceed oneShotMaxMs (${cfg.oneShotMaxMs}).`);
  }
}

/** True if a process with the given pid is currently alive. Used as the primary
 *  staleness signal for the scheduler lock (take over a dead owner immediately,
 *  never steal a live owner's lease). */
export function isProcessRunning(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0); // signal 0 = liveness probe, no signal sent
    return true;
  } catch (err: unknown) {
    // EPERM means the process exists but we can't signal it → still alive.
    return !!(err && typeof err === 'object' && 'code' in err && (err as { code?: string }).code === 'EPERM');
  }
}

/** A scheduler lock-file record, written by the owning session each tick. */
export interface CronLockRecord {
  sessionId: string;
  pid: number;
  timestamp: number;
}

/** Non-throwing, schema-validated parse of an untrusted cron lock-file body.
 *  Returns null for anything that is not a well-formed {sessionId, pid, timestamp}
 *  record — unparseable JSON, a non-object, or a missing/mis-typed field — so every
 *  caller treats a corrupt or partially-written lock as "no valid owner" (stale)
 *  rather than trusting ad-hoc field casts on untrusted on-disk JSON. Mirrors
 *  the reference's zod-validated readLock (src/utils/cronTasksLock.ts). */
export function parseCronLock(raw: string): CronLockRecord | null {
  let data: unknown;
  try { data = JSON.parse(raw); } catch { return null; }
  if (!data || typeof data !== 'object') return null;
  const r = data as Record<string, unknown>;
  if (typeof r.sessionId !== 'string') return null;
  if (typeof r.pid !== 'number' || !Number.isFinite(r.pid)) return null;
  if (typeof r.timestamp !== 'number' || !Number.isFinite(r.timestamp)) return null;
  return { sessionId: r.sessionId, pid: r.pid, timestamp: r.timestamp };
}

/** Validate an optional human-readable job name. Returns an error message string,
 *  or null when valid (including when absent — the field is optional). A name must
 *  be a non-empty, non-whitespace string no longer than {@link MAX_CRON_NAME_LEN}.
 *  Single canonical validator reused by create() (engine boundary) and the
 *  cron_create tool (clean tool-level error) so the rule exists in one place. */
export function validateCronName(name: string | undefined): string | null {
  if (name === undefined) return null;
  if (typeof name !== 'string' || !name.trim()) return 'name must be a non-empty string';
  if (name.length > MAX_CRON_NAME_LEN) return `name is too long (${name.length} chars; max ${MAX_CRON_NAME_LEN})`;
  return null;
}

/** Validate a 5-field cron expression's structure and field ranges. Returns an
 *  error message string, or null when valid. */
export function validateCronExpression(cron: string): string | null {
  const parts = cron.trim().split(/\s+/);
  if (parts.length !== 5) return `cron must have 5 fields (got ${parts.length}): "${cron}"`;
  const ranges: Array<[number, number]> = [[0, 59], [0, 23], [1, 31], [1, 12], [0, 7]];
  for (let i = 0; i < 5; i++) {
    const err = validateCronField(parts[i], ranges[i][0], ranges[i][1]);
    if (err) return `field ${i + 1} ("${parts[i]}"): ${err}`;
  }
  return null;
}

export function validateCronField(spec: string, min: number, max: number): string | null {
  if (spec === '*') return null;
  for (const part of spec.split(',')) {
    if (part === '') return 'empty element';
    let body = part;
    if (part.includes('/')) {
      const [range, stepStr] = part.split('/');
      const step = Number(stepStr);
      if (!Number.isInteger(step) || step <= 0) return `invalid step "${stepStr}"`;
      if (range === '*') continue;
      body = range;
    }
    if (body.includes('-')) {
      const [lo, hi] = body.split('-').map(Number);
      if (!Number.isInteger(lo) || !Number.isInteger(hi)) return `invalid range "${body}"`;
      if (lo < min || hi > max) return `range ${lo}-${hi} out of bounds ${min}-${max}`;
      if (lo > hi) return `reversed range "${body}"`;
    } else {
      const v = Number(body);
      if (!Number.isInteger(v)) return `non-integer "${body}"`;
      if (v < min || v > max) return `value ${v} out of bounds ${min}-${max}`;
    }
  }
  return null;
}

/** Size a CommonMark code fence so it is STRICTLY longer than the longest run of
 *  backticks inside `body`. Per the CommonMark fence-matching rule a fenced block
 *  is closed only by a fence at least as long as its opener, so a fence sized to
 *  `max(3, longestRun + 1)` guarantees an injected ``` inside the body can never
 *  close the fence early and un-wrap any trailing guidance text (prompt-injection
 *  containment). Mirrors the reference's content-derived fence sizing (src/utils/cronScheduler.ts).
 *  Single canonical fence helper — reused by every scheduled-prompt wrapper so the
 *  sizing logic exists in exactly one place. */
export function fenceFor(body: string): string {
  let longestRun = 0;
  let current = 0;
  for (const ch of body) {
    if (ch === '`') { current++; if (current > longestRun) longestRun = current; }
    else current = 0;
  }
  return '`'.repeat(Math.max(3, longestRun + 1));
}

export function wrapPromptSafely(prompt: string): string {
  const fence = fenceFor(prompt);
  return `Previously scheduled task (missed while offline). Ask the user to confirm before running it:\n${fence}\n${prompt}\n${fence}`;
}

/** Render an (hour, minute) pair as a 12-hour wall-clock time like "9:05 am". */
export function to12Hour(hour: number, minute: number): string {
  const period = hour < 12 ? 'am' : 'pm';
  const h12 = hour % 12 === 0 ? 12 : hour % 12;
  return `${h12}:${String(minute).padStart(2, '0')} ${period}`;
}

/** Render a cron (hour, minute) for display. In local mode it is the wall-clock
 *  time verbatim (AMI's lowercase am/pm style). In UTC mode the pair is treated as
 *  UTC and rendered in the host's local timezone with a short TZ label (e.g.
 *  "9:00 am PST") so a server/remote-defined schedule shows when it fires HERE.
 *  Mirrors the reference's formatLocalTime / formatUtcTimeAsLocal split (src/utils/cron.ts). */
export function formatClockTime(hour: number, minute: number, utc: boolean): string {
  if (!utc) return to12Hour(hour, minute);
  const d = new Date();
  d.setUTCHours(hour, minute, 0, 0);
  // Keep the TZ label (uppercase) but lowercase only the AM/PM to match AMI's style.
  return d
    .toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZoneName: 'short' })
    .replace(/\b([AP]M)\b/, m => m.toLowerCase());
}

/** The LOCAL weekday short-name for a schedule whose day-of-week + hour:minute are
 *  expressed in UTC. A UTC midnight-crossing offset can push a "Monday 23:00 UTC"
 *  fire onto Sunday or Tuesday locally, so resolve the weekday from the actual UTC
 *  instant. Mirrors the reference's utc dayIndex recomputation (src/utils/cron.ts). */
export function localWeekdayForUtc(dowIndex: number, hour: number, minute: number, names: string[]): string {
  const ref = new Date();
  const daysToAdd = (dowIndex - ref.getUTCDay() + 7) % 7;
  ref.setUTCDate(ref.getUTCDate() + daysToAdd);
  ref.setUTCHours(hour, minute, 0, 0);
  return names[ref.getDay()];
}

export function matchField(value: number, spec: string, min: number, max: number): boolean {
  if (spec === '*') return true;

  for (const part of spec.split(',')) {
    if (part.includes('/')) {
      const [range, stepStr] = part.split('/');
      const step = parseInt(stepStr, 10);
      if (isNaN(step) || step <= 0) continue;
      let start: number, end: number;
      if (range === '*') {
        start = min;
        end = max;
      } else if (range.includes('-')) {
        const [rLo, rHi] = range.split('-').map(Number);
        if (isNaN(rLo) || isNaN(rHi)) continue;
        start = rLo;
        end = rHi;
      } else {
        start = parseInt(range, 10);
        if (isNaN(start)) continue;
        end = max;
      }
      if ((value - start) % step === 0 && value >= start && value <= end) return true;
    } else if (part.includes('-')) {
      let [lo, hi] = part.split('-').map(Number);
      if (isNaN(lo) || isNaN(hi)) continue;
      if (max === 6) {
        // FUNCTIONAL: DOW ranges ending at 7 (Sunday ≡ 0 ≡ 7). A span like "0-7"
        // or "1-7" means "that span through Saturday, plus Sunday" — previously the
        // raw `%7` collapsed "0-7" to 0-0 (Sunday only), silently dropping every
        // other day. Handle the Sunday-via-7 span explicitly; leave "7-7" (Sunday
        // only) and non-7 ranges to the normal path below.
        const rawLo = lo;
        const rawHi = hi;
        lo = lo % 7;
        hi = hi % 7;
        if (rawHi === 7 && rawLo < 7) {
          if (value === 0 || (value >= rawLo && value <= 6)) return true;
          continue;
        }
      }
      if (hi >= lo) {
        if (value >= lo && value <= hi) return true;
      } else {
        if (value >= lo || value <= hi) return true;
      }
    } else {
      let parsed = parseInt(part, 10);
      if (max === 6) parsed = parsed % 7; // DOW: Sunday 7 → 0
      if (parsed === value) return true;
    }
  }
  return false;
}

/** True when a cron field, although not the literal "*", still matches EVERY value
 *  in [min,max] — e.g. "1-31" for day-of-month, "0-6"/"0-7"/"*​/1" for day-of-week.
 *  Such a field is a semantic wildcard: day-of-month/day-of-week OR-matching must
 *  treat it as unrestricted, otherwise "0 9 1-31 * 1" (every DOM AND Monday) would
 *  wrongly fire every day instead of only on Mondays. Mirrors the reference's expand-to-set
 *  then length===rangeSize wildcard detection (src/utils/cron.ts). */
export function fieldCoversFullRange(spec: string, min: number, max: number): boolean {
  if (spec === '*') return true;
  for (let v = min; v <= max; v++) {
    if (!matchField(v, spec, min, max)) return false;
  }
  return true;
}
