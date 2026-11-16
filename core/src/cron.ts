import * as fs from 'fs';
import { projectStateDir } from './reference-journal';
import * as path from 'path';
import { log as coreLog } from './logger';

export interface CronJob {
  id: string;
  cron: string;
  prompt: string;
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
  /** Optional per-job run budget. A recurring job is retired (stops firing and is
   *  removed) once it has fired `maxRuns` times. Undefined = unlimited. */
  maxRuns?: number;
  /** How many times this job has fired. Only tracked when `maxRuns` is set. */
  runCount?: number;
}

export const MAX_AGE_DAYS = 7;
const MAX_AGE_MS = MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
const MAX_JOBS = 50;
const LOCK_STALE_MS = 120_000;
/** Recurring jobs may not fire more often than once per minute (abuse floor). */
const MIN_INTERVAL_MS = 60_000;
/** Minute-resolution scan horizon for next-fire computation. Covers a full leap
 *  cycle (~4 years) so valid-but-rare schedules like "0 0 29 2 *" (Feb 29) resolve
 *  instead of being wrongly reported as unsatisfiable when the next leap day is more
 *  than a year out. Truly impossible dates (e.g. Feb 30) still fall through to null.
 *  (Century non-leap boundaries like 2100, which make the gap 8 years, are out of
 *  scope — they'd read as unsatisfiable, the same conservative behavior as before.) */
const SCAN_HORIZON_MIN = 4 * 366 * 24 * 60;

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
   *  disables the floor (pure 0..max spread). Mirrors Claude's CronJitterConfig
   *  oneShotFloorMs (src/utils/cronJitterConfig.ts). */
  oneShotFloorMs: number;
  /** One-shot jitter only applies when the fire minute is divisible by this (preserves :00/:30 pins). */
  oneShotMinuteMod: number;
}

export const DEFAULT_CRON_JITTER: CronJitterConfig = {
  recurringFrac: 0.1,
  recurringCapMs: 15 * 60 * 1000,
  oneShotMaxMs: 30 * 1000,
  oneShotFloorMs: 0,
  oneShotMinuteMod: 30,
};

/** Cross-field validation of a jitter config (mirrors Claude's zod `.refine` on
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
 *  Claude's zod-validated readLock (src/utils/cronTasksLock.ts). */
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

/** Errno codes meaning "the file is not there / can't be gotten at" — a missing or
 *  inaccessible persisted-state file is the normal empty state, not a fault worth a
 *  diagnostic. Mirrors Claude's isFsInaccessible set (src/utils/errors.ts). */
const FS_INACCESSIBLE_CODES = new Set(['ENOENT', 'EACCES', 'EPERM', 'ENOTDIR', 'ELOOP']);

/** True when a caught fs error is an expected "missing / inaccessible" condition
 *  (so the caller can treat it as an empty state silently) vs a genuine, unexpected
 *  read fault (I/O error, EISDIR, …) that deserves a logged diagnostic trail. */
export function isFsInaccessible(err: unknown): boolean {
  const code = err && typeof err === 'object' ? (err as { code?: unknown }).code : undefined;
  return typeof code === 'string' && FS_INACCESSIBLE_CODES.has(code);
}

/** Deterministic per-job forward jitter for a recurring fire, proportional to the
 *  ACTUAL gap between its next two fires — so at the defaults an hourly job spreads
 *  across minutes while a per-minute job spreads by only a few seconds — capped by
 *  cfg.recurringCapMs. A null/zero gap (no second match, e.g. a pinned yearly date)
 *  carries no herd risk, so it gets no jitter. Mirrors Claude's jitteredNextCronRunMs
 *  (src/utils/cronTasks.ts), which proportions jitter against `t2 - t1` rather than a
 *  crude per-pattern period estimate. */
function computeJitter(gapMs: number | null, jobId: string, cfg: CronJitterConfig = DEFAULT_CRON_JITTER): number {
  if (gapMs === null || gapMs <= 0) return 0;
  const hash = jobId.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0);
  const normalizedHash = Math.abs(hash) / 2147483647;
  const maxJitterMs = Math.min(gapMs * cfg.recurringFrac, cfg.recurringCapMs);
  return Math.floor(normalizedHash * maxJitterMs);
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

function validateCronField(spec: string, min: number, max: number): string | null {
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
 *  containment). Mirrors Claude's content-derived fence sizing (src/utils/cronScheduler.ts).
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

export class CronScheduler {
  private jobs: Map<string, CronJob> = new Map();
  private cwd: string;
  private persistPath: string;
  private lockPath: string;
  private sessionId: string;
  private timers: Map<string, NodeJS.Timeout> = new Map();
  private tickInterval: NodeJS.Timeout | null = null;
  private jitterConfig: CronJitterConfig;
  /** Immutable baseline jitter (constructor value). `jitterConfig` is re-derived
   *  from this every tick so an operator env override can be widened/narrowed AND
   *  reverted live without a restart. */
  private baseJitter: CronJitterConfig;
  /** True while THIS scheduler currently owns the durable cron lock. Drives the
   *  per-tick heartbeat (lease refresh) and compromise detection. */
  private holdsLock = false;
  /** Directory watcher on the persisted-state file; picks up durable jobs added or
   *  removed by another process (or a hand edit) without a restart. */
  private watcher: fs.FSWatcher | null = null;
  /** When set and returning true, the tick defers firing (session is mid-turn). */
  private busyCheck: (() => boolean) | null = null;
  /** Last session id we logged a "lock held by" contention line for. Suppresses
   *  repeat diagnostics while passively polling the same live owner every tick. */
  private lastBlockedBy: string | null = null;
  /** Bound process-exit release registered while this scheduler owns the lock, so
   *  an abrupt termination still frees the lock. Null when not registered. */
  private exitRelease: (() => void) | null = null;

  constructor(cwd: string, sessionId?: string, jitterConfig: CronJitterConfig = DEFAULT_CRON_JITTER) {
    // Fail fast on a mis-specified jitter config (inverted one-shot window, a
    // negative/NaN knob, …) rather than silently mis-jittering every fire.
    validateJitterConfig(jitterConfig);
    this.cwd = cwd;
    this.persistPath = path.join(projectStateDir(cwd), 'scheduled_tasks.json');
    this.lockPath = path.join(projectStateDir(cwd), 'cron.lock');
    this.sessionId = sessionId || `session_${process.pid}_${Date.now().toString(36)}`;
    this.jitterConfig = jitterConfig;
    this.baseJitter = jitterConfig;
    this.loadDurable();
  }

  /** True when the cron subsystem is disabled by operator kill-switch env var. */
  static isDisabled(): boolean {
    return (
      process.env.AMI_DISABLE_CRON === '1' ||
      process.env.AMI_DISABLE_CRON === 'true' ||
      process.env.CLAUDE_CODE_DISABLE_CRON === '1' ||
      process.env.CLAUDE_CODE_DISABLE_CRON === 'true'
    );
  }

  /** Narrower kill-switch than isDisabled(): when durable crons are gated off,
   *  new jobs are silently degraded to session-only (in-memory) rather than
   *  persisted to disk — the whole scheduler keeps working, but the model-chosen
   *  prompt is never written to scheduled_tasks.json. Returns true when disk
   *  persistence is allowed (the default). Mirrors Claude's isDurableCronEnabled()
   *  gate and reuses the same truthy-env convention as isDisabled(). */
  static isDurableEnabled(): boolean {
    return !(
      process.env.AMI_DISABLE_DURABLE_CRON === '1' ||
      process.env.AMI_DISABLE_DURABLE_CRON === 'true' ||
      process.env.CLAUDE_CODE_DISABLE_DURABLE_CRON === '1' ||
      process.env.CLAUDE_CODE_DISABLE_DURABLE_CRON === 'true'
    );
  }

  /** Register a predicate; while it returns true the ticker defers firing jobs
   *  (e.g. the session is loading / mid-turn) so a fire never interrupts work. */
  setBusyCheck(fn: (() => boolean) | null): void {
    this.busyCheck = fn;
  }

  /** The jitter config in effect for the most recent tick (baseline merged with any
   *  live operator env override). Introspection surface for diagnostics. */
  getJitterConfig(): CronJitterConfig {
    return this.jitterConfig;
  }

  /** Resolve the effective jitter config for a tick. An operator may retune jitter
   *  LIVE (no restart) via env vars; each is applied over the construction baseline
   *  and reverts the moment the var is cleared. Mirrors Claude's getJitterConfig()
   *  being polled once per tick. Invalid/negative values fall back to the baseline. */
  private resolveRuntimeJitter(): CronJitterConfig {
    const base = this.baseJitter;
    const num = (raw: string | undefined, fallback: number): number => {
      if (raw === undefined) return fallback;
      const n = Number(raw);
      return Number.isFinite(n) && n >= 0 ? n : fallback;
    };
    const oneShotMaxMs = num(process.env.AMI_CRON_ONESHOT_JITTER_MS, base.oneShotMaxMs);
    // Clamp the resolved floor to the resolved cap: an env-set floor larger than
    // the cap would invert the one-shot window, so collapse it to the cap (a floor
    // == cap means "always fire exactly `cap` early") instead of throwing on a
    // live, already-running scheduler.
    const oneShotFloorMs = Math.min(num(process.env.AMI_CRON_ONESHOT_FLOOR_MS, base.oneShotFloorMs), oneShotMaxMs);
    return {
      recurringFrac: num(process.env.AMI_CRON_JITTER_FRAC, base.recurringFrac),
      recurringCapMs: num(process.env.AMI_CRON_JITTER_CAP_MS, base.recurringCapMs),
      oneShotMaxMs,
      oneShotFloorMs,
      oneShotMinuteMod: num(process.env.AMI_CRON_ONESHOT_MINUTE_MOD, base.oneShotMinuteMod) || base.oneShotMinuteMod,
    };
  }

  create(opts: { cron: string; prompt: string; recurring?: boolean; durable?: boolean; agentId?: string; permanent?: boolean; maxRuns?: number; maxAgeDays?: number }): CronJob {
    if (this.jobs.size >= MAX_JOBS) {
      throw new Error(`Maximum of ${MAX_JOBS} cron jobs reached. Delete existing jobs first.`);
    }
    // Field-level validation up front with a clear error (not a brute-force throw).
    const validationError = validateCronExpression(opts.cron);
    if (validationError) {
      throw new Error(`Invalid cron expression: ${validationError}`);
    }
    // FUNCTIONAL: validate an optional per-job run budget — must be a positive
    // integer (a non-positive / fractional budget would retire the job instantly
    // or never, both of which are silent surprises).
    if (opts.maxRuns !== undefined && (!Number.isInteger(opts.maxRuns) || opts.maxRuns < 1)) {
      throw new Error(`maxRuns must be a positive integer (got ${opts.maxRuns}).`);
    }
    // FUNCTIONAL: expose the per-job expiry window at creation. Must be a
    // non-negative integer number of days; 0 means "never expire" (unlimited),
    // mirroring Claude's recurringMaxAgeMs===0 sentinel. A fractional/negative
    // value would silently truncate or disable expiry, so reject it up front.
    if (opts.maxAgeDays !== undefined && (!Number.isInteger(opts.maxAgeDays) || opts.maxAgeDays < 0)) {
      throw new Error(`maxAgeDays must be a non-negative integer (got ${opts.maxAgeDays}); use 0 for unlimited.`);
    }
    const recurring = opts.recurring ?? true;
    // Runaway-schedule guard: a recurring job may not fire more often than the floor.
    if (recurring) {
      const nextTwo = this.computeNextRun(opts.cron);
      if (nextTwo !== null) {
        const following = this.computeNextRun(opts.cron, nextTwo);
        if (following !== null && following - nextTwo < MIN_INTERVAL_MS) {
          throw new Error(`Recurring cron fires more often than the ${MIN_INTERVAL_MS / 1000}s minimum interval.`);
        }
      }
    }
    const nextRun = this.computeNextRun(opts.cron);
    // FUNCTIONAL: reject a structurally-valid-but-unsatisfiable schedule at the
    // engine boundary (e.g. "0 0 30 2 *", "0 0 31 4 *"). Previously such a job was
    // created with nextRun undefined and silently never fired — a dead job the
    // caller believed was scheduled. Now every consumer (not only the cron tool)
    // gets this guarantee.
    if (nextRun === null) {
      throw new Error(`Cron expression "${opts.cron}" never matches a real date (unsatisfiable schedule).`);
    }
    const id = `cron_${this.getNextIdNum()}_${Date.now().toString(36)}`;
    const job: CronJob = {
      id,
      cron: opts.cron,
      prompt: opts.prompt,
      recurring,
      // Durable kill-switch: a requested durable job is silently degraded to
      // session-only when disk persistence is gated off (no schema change, so
      // the model never sees a validation error if the gate flips mid-session).
      durable: (opts.durable ?? false) && CronScheduler.isDurableEnabled(),
      createdAt: Date.now(),
      nextRun,
      ...(opts.agentId ? { agentId: opts.agentId } : {}),
      ...(opts.permanent ? { permanent: true } : {}),
      ...(opts.maxRuns !== undefined ? { maxRuns: opts.maxRuns, runCount: 0 } : {}),
      ...(opts.maxAgeDays !== undefined ? { maxAgeDays: opts.maxAgeDays } : {}),
    };
    this.jobs.set(id, job);
    if (job.durable) this.saveDurable();
    return job;
  }

  /** Partially update a job's cron/prompt/recurring. Recomputes nextRun when the
   *  schedule changes. Returns the updated job or undefined if not found. */
  update(id: string, opts: { cron?: string; prompt?: string; recurring?: boolean }, agentId?: string): CronJob | undefined {
    const job = this.jobs.get(id);
    if (!job) return undefined;
    if (!this.isAuthorized(job, agentId)) {
      throw new Error(`Not authorized to update cron job ${id} (owned by another agent).`);
    }
    if (opts.cron !== undefined) {
      const err = validateCronExpression(opts.cron);
      if (err) throw new Error(`Invalid cron expression: ${err}`);
      job.cron = opts.cron;
      job.nextRun = this.computeNextRun(opts.cron) ?? undefined;
    }
    if (opts.prompt !== undefined) job.prompt = opts.prompt;
    if (opts.recurring !== undefined) job.recurring = opts.recurring;
    if (job.durable) this.saveDurable();
    return job;
  }

  /** Fire a job immediately (run-now), returning it so the caller can dispatch
   *  the prompt. Advances the schedule exactly as a natural fire would. */
  runNow(id: string, agentId?: string): CronJob | undefined {
    const job = this.jobs.get(id);
    if (!job) return undefined;
    if (!this.isAuthorized(job, agentId)) {
      throw new Error(`Not authorized to run cron job ${id} (owned by another agent).`);
    }
    const snapshot = { ...job };
    this.markRun(id);
    return snapshot;
  }

  /** Ownership check: a job with no agentId is global; otherwise only its owner
   *  (or an unscoped/team-lead caller with no agentId) may act on it. */
  private isAuthorized(job: CronJob, agentId?: string): boolean {
    if (!job.agentId) return true;      // global job
    if (agentId === undefined) return true; // team-lead / unscoped caller sees all
    return job.agentId === agentId;
  }

  private getNextIdNum(): number {
    let maxId = 0;
    for (const [id] of this.jobs) {
      const match = id.match(/^cron_(\d+)/);
      if (match) {
        const num = parseInt(match[1], 10);
        if (num > maxId) maxId = num;
      }
    }
    return maxId + 1;
  }

  delete(id: string, agentId?: string): boolean {
    const job = this.jobs.get(id);
    if (!job) return false;
    // Cross-agent authorization: a teammate may only delete its own jobs.
    if (!this.isAuthorized(job, agentId)) {
      throw new Error(`Not authorized to delete cron job ${id} (owned by another agent).`);
    }
    const timer = this.timers.get(id);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(id);
    }
    this.jobs.delete(id);
    if (job.durable) this.saveDurable();
    return true;
  }

  /** List jobs. With an agentId, a teammate sees only its own jobs; an unscoped
   *  (team-lead) caller sees all. */
  list(agentId?: string): CronJob[] {
    const all = Array.from(this.jobs.values());
    if (agentId === undefined) return all;
    return all.filter(j => !j.agentId || j.agentId === agentId);
  }

  get(id: string): CronJob | undefined {
    return this.jobs.get(id);
  }

  /** Epoch ms of the soonest scheduled fire across all loaded jobs, or null when
   *  nothing is pending (no jobs, or none with a computed nextRun). Lets a caller
   *  decide whether to keep an idle session warm for an imminent fire or let it go
   *  idle. Mirrors Claude's createCronScheduler().getNextFireTime(). */
  getNextFireTime(): number | null {
    let min = Infinity;
    for (const job of this.jobs.values()) {
      if (typeof job.nextRun === 'number' && job.nextRun < min) min = job.nextRun;
    }
    return min === Infinity ? null : min;
  }

  getDueJobs(): CronJob[] {
    const now = Date.now();
    const due: CronJob[] = [];
    for (const job of this.jobs.values()) {
      if (!job.nextRun) continue;
      if (job.recurring) {
        // Forward jitter spreads recurring fires to avoid thundering herds; the
        // window is proportional to the REAL gap between this fire and the next.
        const nextAfter = this.safeComputeNextRun(job.cron, job.nextRun);
        const gap = nextAfter !== null ? nextAfter - job.nextRun : null;
        const jitter = computeJitter(gap, job.id, this.jitterConfig);
        if (now >= job.nextRun + jitter) due.push(job);
      } else {
        // One-shot: small BACKWARD (early) jitter, gated to non-pinned minutes so
        // a user-pinned :00/:30 fire time is preserved exactly.
        const fireMinute = new Date(job.nextRun).getMinutes();
        const pinned = fireMinute % this.jitterConfig.oneShotMinuteMod === 0;
        const back = pinned ? 0 : this.oneShotBackJitter(job.id);
        if (now >= job.nextRun - back) due.push(job);
      }
    }
    return due;
  }

  private oneShotBackJitter(jobId: string): number {
    const hash = jobId.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0);
    const norm = Math.abs(hash) / 2147483647;
    // Deterministic early-fire in [floor, max]: the configured floor guarantees a
    // non-pinned one-shot never lands exactly on its nominal (herd) instant, while
    // the hash spreads the remaining span. floor defaults to 0 (pure 0..max spread).
    const { oneShotFloorMs: floor, oneShotMaxMs: max } = this.jitterConfig;
    return floor + Math.floor(norm * (max - floor));
  }

  markRun(id: string, deferSave = false): void {
    const job = this.jobs.get(id);
    if (!job) return;
    job.lastRun = Date.now();
    job.lastFiredAt = Date.now();
    // FUNCTIONAL: per-job run budget. Count this fire when a budget is set, and
    // retire the job once the budget is exhausted so a "run N times" schedule stops
    // firing on its own (kill-switch at the job level, not just the global env).
    if (job.maxRuns !== undefined) {
      job.runCount = (job.runCount ?? 0) + 1;
    }
    const budgetExhausted = job.maxRuns !== undefined && (job.runCount ?? 0) >= job.maxRuns;
    if (job.recurring && !budgetExhausted) {
      // Never throw inside the fire loop: a cron that no longer has a future
      // match leaves nextRun undefined (the job simply stops firing).
      job.nextRun = this.safeComputeNextRun(job.cron) ?? undefined;
    } else {
      // One-shot, or a recurring job that has used up its run budget → remove it.
      this.jobs.delete(id);
    }
    // Callers firing several jobs in one tick pass deferSave=true and persist
    // once afterwards (batched write); single-fire callers persist immediately.
    if (!deferSave) this.saveDurable();
  }

  /** computeNextRun that never throws — returns null on invalid/no-match. Used
   *  in the fire loop and missed-task detection so a single bad job can't crash
   *  the scheduler. */
  safeComputeNextRun(cron: string, fromTime?: number): number | null {
    try {
      return this.computeNextRun(cron, fromTime);
    } catch {
      return null;
    }
  }

  startTicker(onDue: (job: CronJob) => void): void {
    if (this.tickInterval) return;
    this.tickInterval = setInterval(() => this.tick(onDue), 5_000);
    this.tickInterval.unref();
    this.startWatching();
  }

  /** Watch the persisted-state file so durable jobs added/removed by another
   *  process (or a hand edit) are reflected live, without restarting. The directory
   *  is watched (not the file) because the atomic temp+rename save replaces the
   *  inode, which breaks a watch bound directly to the file on most platforms. */
  private startWatching(): void {
    if (this.watcher) return;
    try {
      const dir = path.dirname(this.persistPath);
      fs.mkdirSync(dir, { recursive: true });
      const target = path.basename(this.persistPath);
      this.watcher = fs.watch(dir, (_event, filename) => {
        if (filename && filename.toString() === target) this.reloadDurable();
      });
      // Never keep the event loop alive just for the watcher.
      this.watcher.unref?.();
    } catch { /* fs.watch unsupported here → degrade to load-once semantics */ }
  }

  private stopWatching(): void {
    if (this.watcher) {
      try { this.watcher.close(); } catch { /* already closed */ }
      this.watcher = null;
    }
  }

  /** Re-read the durable state file and reconcile it against the in-memory jobs:
   *  upsert every valid durable entry, and drop durable jobs that disappeared from
   *  disk. Session-only (non-durable) jobs are left untouched. Deliberately lenient
   *  (never deletes the file) since a watch event can arrive mid-write. */
  private reloadDurable(): void {
    let raw: string;
    try {
      raw = fs.readFileSync(this.persistPath, 'utf-8');
    } catch {
      // File removed externally → forget every durable job.
      for (const [id, job] of this.jobs) if (job.durable) this.jobs.delete(id);
      return;
    }
    let data: unknown;
    try { data = JSON.parse(raw); } catch { return; } // mid-write / corrupt → ignore
    if (!Array.isArray(data)) return;
    const seen = new Set<string>();
    for (const entry of data) {
      if (!this.isValidPersistedJob(entry) || !entry.durable) continue;
      seen.add(entry.id);
      this.jobs.set(entry.id, entry);
    }
    for (const [id, job] of this.jobs) {
      if (job.durable && !seen.has(id)) this.jobs.delete(id);
    }
  }

  /** One scheduler tick: expire stale jobs, then run every due job once. Firing
   *  is deferred while the session is busy (mid-turn) so a cron never interrupts
   *  in-flight work — the job stays due and fires on a later tick. */
  private tick(onDue: (job: CronJob) => void): void {
    // Mid-session kill-switch: the operator may flip the disable env var AFTER
    // the scheduler started. Poll it every tick so an already-running scheduler
    // stops firing (and ageing) the moment the gate flips, without being torn
    // down — the timer keeps running and firing resumes if the gate clears.
    // Mirrors Claude's isKilled() option (polled once per check() tick).
    if (CronScheduler.isDisabled()) return;
    // Operator may retune jitter live (no restart); re-resolve once per tick.
    this.jitterConfig = this.resolveRuntimeJitter();
    // Keep our durable lease fresh (heartbeat) and detect if it was stolen/removed.
    this.heartbeatLock();
    this.expireOld(onDue);
    if (this.busyCheck?.()) return;
    const due = this.getDueJobs();
    // Batched fire-stamp persistence: N durable fires in one tick coalesce into
    // a single saveDurable() write instead of one write per fired job. markRun is
    // called with deferSave=true so the schedule mutations accumulate in memory,
    // then one write persists them all at the end of the tick.
    let durableFired = false;
    for (const job of due) {
      if (job.durable && !this.acquireLock()) continue;
      if (job.durable) durableFired = true;
      this.markRun(job.id, true);
      onDue(job);
    }
    if (durableFired) this.saveDurable();
  }

  stopTicker(): void {
    if (this.tickInterval) {
      clearInterval(this.tickInterval);
      this.tickInterval = null;
    }
    this.stopWatching();
    this.releaseLock();
  }

  /** Per-tick lease heartbeat. While we hold the durable lock, refresh its
   *  timestamp so a long-lived-but-alive holder is never judged stale by the
   *  timestamp fallback (used when the owning pid is unknown, e.g. a different
   *  host). If the lock file vanished or a different session overwrote it, our
   *  lease is compromised — we yield it so the new owner fires unopposed. */
  private heartbeatLock(): void {
    if (!this.holdsLock) return;
    let raw: string | null = null;
    try {
      raw = fs.readFileSync(this.lockPath, 'utf-8');
    } catch {
      raw = null; // missing or unreadable
    }
    const lock = raw !== null ? parseCronLock(raw) : null;
    if (!lock || lock.sessionId !== this.sessionId) {
      this.onLockCompromised(lock ? `taken over by ${lock.sessionId}` : 'removed');
      return;
    }
    try {
      fs.writeFileSync(this.lockPath, JSON.stringify({
        sessionId: this.sessionId,
        pid: process.pid,
        timestamp: Date.now(),
      }));
    } catch { /* best effort — we still believe we hold the lease */ }
  }

  /** Handle loss of our durable lease: stop considering ourselves the owner and
   *  drop the exit-release registration so we never delete the new owner's lock.
   *  The next tick's acquireLock() will re-contend cleanly. */
  private onLockCompromised(reason: string): void {
    if (this.exitRelease) {
      activeCronLockReleases.delete(this.exitRelease);
      this.exitRelease = null;
    }
    this.holdsLock = false;
    this.lastBlockedBy = null;
    coreLog('cron', `cron lock compromised (${reason}); yielding the lease`);
  }

  private acquireLock(): boolean {
    try {
      if (fs.existsSync(this.lockPath)) {
        let raw: string | null = null;
        try {
          raw = fs.readFileSync(this.lockPath, 'utf-8');
        } catch {
          raw = null; // unreadable → treat as stale and reclaim
        }
        // Schema-validated parse: anything that is not a well-formed lock record
        // (unparseable, non-object, or missing/mis-typed fields) is reclaimed as stale.
        const lock = raw !== null ? parseCronLock(raw) : null;
        if (lock) {
          // We already own it (idempotent re-acquire). After --resume the sessionId
          // is restored but the process has a new PID; rewrite the lock with the
          // live PID so other sessions see a live owner and don't steal the lease.
          if (lock.sessionId === this.sessionId) {
            if (lock.pid !== process.pid) {
              try {
                fs.writeFileSync(this.lockPath, JSON.stringify({
                  sessionId: this.sessionId,
                  pid: process.pid,
                  timestamp: Date.now(),
                }));
                coreLog('cron', `Re-acquired cron lock with new pid ${process.pid} (session ${this.sessionId}, was pid ${lock.pid})`);
              } catch { /* best effort — we still own the lease */ }
            }
            this.lastBlockedBy = null;
            this.registerExitRelease();
            return true;
          }
          // Liveness = a live owning PID OR a recently-refreshed heartbeat. The
          // holder refreshes lock.timestamp every tick (heartbeatLock), so a
          // long-running-but-alive owner whose pid we can't probe (e.g. a different
          // host) is kept alive by its advancing timestamp instead of being falsely
          // reclaimed after the fixed stale window.
          const heartbeatFresh = Date.now() - lock.timestamp < LOCK_STALE_MS;
          const ownerAlive = isProcessRunning(lock.pid) || heartbeatFresh;
          if (ownerAlive) {
            // Contention diagnostic, de-duplicated: log once per distinct owner so a
            // passive session polling a live owner every tick does not spam the log.
            if (this.lastBlockedBy !== lock.sessionId) {
              this.lastBlockedBy = lock.sessionId;
              coreLog('cron', `cron lock held by session ${lock.sessionId} (pid ${lock.pid})`);
            }
            return false; // live owner — never steal the lease
          }
          coreLog('cron', `Reclaiming stale lock from dead owner (pid ${lock.pid}, session ${lock.sessionId})`);
        } else {
          coreLog('cron', 'Reclaiming corrupt/unparseable cron lock');
        }
        try { fs.unlinkSync(this.lockPath); } catch { /* race with another cleaner */ }
      }
      const dir = path.dirname(this.lockPath);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this.lockPath, JSON.stringify({
        sessionId: this.sessionId,
        pid: process.pid,
        timestamp: Date.now(),
      }), { flag: 'wx' });
      this.lastBlockedBy = null;
      this.registerExitRelease();
      coreLog('cron', `Acquired cron lock (session ${this.sessionId}, pid ${process.pid})`);
      return true;
    } catch {
      return false;
    }
  }

  /** Register a process-exit release for the lock once per acquisition, so an
   *  abrupt termination (signal/crash) still frees the lock even if stopTicker()
   *  is never reached. Idempotent while already registered. */
  private registerExitRelease(): void {
    this.holdsLock = true;
    if (this.exitRelease) return;
    this.exitRelease = () => this.releaseLock();
    activeCronLockReleases.add(this.exitRelease);
    installCronExitHook();
  }

  private releaseLock(): void {
    if (this.exitRelease) {
      activeCronLockReleases.delete(this.exitRelease);
      this.exitRelease = null;
    }
    this.holdsLock = false;
    this.lastBlockedBy = null;
    try {
      if (fs.existsSync(this.lockPath)) {
        const lock = parseCronLock(fs.readFileSync(this.lockPath, 'utf-8'));
        if (lock && lock.sessionId === this.sessionId) {
          fs.unlinkSync(this.lockPath);
          // Lock-lifecycle introspection: pair the acquire/reclaim logs with a
          // release line so the full lease lifecycle is observable in the log.
          coreLog('cron', `Released cron lock (session ${this.sessionId}, pid ${process.pid})`);
        }
      }
    } catch { /* ignore */ }
  }

  detectMissedTasks(): CronJob[] {
    const now = Date.now();
    const missed: CronJob[] = [];
    for (const job of this.jobs.values()) {
      if (job.recurring) {
        // Recurring: a scheduled fire fell between the last fire and now.
        const nextRun = this.safeComputeNextRun(job.cron, job.lastFiredAt ?? job.createdAt);
        if (nextRun !== null && nextRun < now) missed.push(job);
      } else {
        // One-shot: never fired AND its scheduled time has already passed.
        if (job.lastFiredAt) continue;
        const nextRun = job.nextRun ?? this.safeComputeNextRun(job.cron, job.createdAt);
        if (nextRun !== null && nextRun < now) missed.push(job);
      }
    }
    return missed;
  }

  /** Build a single user-facing notification for one or more missed scheduled
   *  tasks. Plural-aware; states the task(s) were missed while AMI was not running
   *  and have already been removed from the schedule; and — crucially — instructs
   *  the model to confirm with the user (AskUserQuestion) BEFORE running any of them,
   *  so a missed prompt never silently re-runs autonomously. Each prompt is wrapped
   *  in a backtick-run-safe fence (fenceFor) so an embedded ``` cannot break out and
   *  un-wrap the trailing guidance (prompt-injection containment), and carries a
   *  `[<schedule>, created <timestamp>]` meta line. Mirrors Claude's
   *  buildMissedTaskNotification (src/utils/cronScheduler.ts). Returns '' for an
   *  empty list so callers can treat "no missed tasks" uniformly. */
  buildMissedTaskNotification(jobs: CronJob[]): string {
    if (jobs.length === 0) return '';
    const plural = jobs.length > 1;
    const header = plural
      ? `${jobs.length} scheduled tasks were missed while AMI was not running and have been removed from the schedule.`
      : `A scheduled task was missed while AMI was not running and has been removed from the schedule.`;
    const guidance = `Do NOT execute ${plural ? 'these prompts' : 'this prompt'} yet. First use the AskUserQuestion tool to ask whether to run ${plural ? 'each one' : 'it'} now. Only execute if the user confirms.`;
    const blocks = jobs.map(job => {
      const fence = fenceFor(job.prompt);
      const schedule = this.formatSchedule(job.cron);
      const created = new Date(job.createdAt).toLocaleString();
      return `[${schedule}, created ${created}]\n${fence}\n${job.prompt}\n${fence}`;
    });
    return `${header}\n\n${guidance}\n\n${blocks.join('\n\n')}`;
  }

  /** Startup catch-up for ONE-SHOT tasks whose fire window elapsed while AMI was
   *  offline. Removes each missed one-shot from the schedule BEFORE returning (so it
   *  can never auto-fire unconfirmed on the next tick, and is not re-detected on a
   *  later call — dedup + anti-re-fire guard), and returns a single ask-before-run
   *  notification, or null when nothing was missed. Recurring missed jobs are
   *  intentionally left untouched — they resume firing naturally on the next tick.
   *  Mirrors Claude's load(initial) one-shot-only missed-task surfacing (surface,
   *  delete-before-notify). This is the wired consumer of detectMissedTasks +
   *  buildMissedTaskNotification. */
  takeMissedOneShotNotification(): string | null {
    const missedOneShots = this.detectMissedTasks().filter(job => !job.recurring);
    if (missedOneShots.length === 0) return null;
    const notification = this.buildMissedTaskNotification(missedOneShots);
    for (const job of missedOneShots) this.delete(job.id);
    return notification;
  }

  computeNextRun(cron: string, fromTime?: number): number | null {
    const parts = cron.trim().split(/\s+/);
    if (parts.length !== 5) {
      throw new Error(`Invalid cron expression: ${cron}`);
    }
    const [minSpec, hourSpec, domSpec, monSpec, dowSpec] = parts;
    const now = fromTime ? new Date(fromTime) : new Date();
    const check = new Date(now);
    check.setSeconds(0, 0);
    check.setMinutes(check.getMinutes() + 1);

    // A field that matches EVERY value in its range (e.g. "1-31" for day-of-month,
    // "0-6"/"0-7"/"*/1" for day-of-week) is a semantic wildcard, not a restriction —
    // so the dom/dow OR-matching below must treat it as unrestricted. Mirrors Claude's
    // expand-to-set then length===rangeSize wildcard detection (src/utils/cron.ts).
    const domRestricted = !fieldCoversFullRange(domSpec, 1, 31);
    const dowRestricted = !fieldCoversFullRange(dowSpec, 0, 6);

    // Coarse skip-ahead search. The former implementation walked strictly
    // minute-by-minute and re-matched all five fields on every candidate, burning
    // O(minutes-in-the-horizon) iterations for sparse crons (yearly, Feb-29, …).
    // Instead, when a coarse field fails we jump the whole unmatched unit — to the
    // first minute of the next month, next day, or next hour — so a yearly cron
    // converges in a handful of jumps. This is behaviour-preserving: the scan
    // window is bounded by exactly the former horizon (the last candidate the old
    // loop examined was firstCandidate + (SCAN_HORIZON_MIN-1) minutes), and a
    // fully-matching candidate is never skipped, so the earliest match found is
    // identical. A match beyond the window still reads as unsatisfiable (null).
    const horizonMs = check.getTime() + (SCAN_HORIZON_MIN - 1) * 60_000;
    while (check.getTime() <= horizonMs) {
      if (!matchField(check.getMonth() + 1, monSpec, 1, 12)) {
        check.setMonth(check.getMonth() + 1, 1);
        check.setHours(0, 0, 0, 0);
        continue;
      }
      const domMatch = matchField(check.getDate(), domSpec, 1, 31);
      const dowMatch = matchField(check.getDay(), dowSpec, 0, 6);
      // Standard cron: when BOTH dom and dow are restricted, use OR semantics
      const dayMatch = (domRestricted && dowRestricted) ? (domMatch || dowMatch) : (domMatch && dowMatch);
      if (!dayMatch) {
        check.setDate(check.getDate() + 1);
        check.setHours(0, 0, 0, 0);
        continue;
      }
      if (!matchField(check.getHours(), hourSpec, 0, 23)) {
        check.setHours(check.getHours() + 1, 0, 0, 0);
        continue;
      }
      if (!matchField(check.getMinutes(), minSpec, 0, 59)) {
        check.setMinutes(check.getMinutes() + 1, 0, 0);
        continue;
      }
      return check.getTime();
    }
    // Structurally valid but no match within the leap-cycle horizon (e.g. Feb 30):
    // return null rather than throwing, so the fire loop degrades gracefully.
    return null;
  }

  formatSchedule(cron: string): string {
    const parts = cron.trim().split(/\s+/);
    if (parts.length !== 5) return cron;
    const [min, hour, dom, mon, dow] = parts;
    const pieces: string[] = [];

    const minPlain = min !== '*' && !min.includes('/') && !min.includes(',') && !min.includes('-');
    const hourStepMatch = /^\*\/(\d+)$/.exec(hour);

    // A specific minute + specific hour reads best as a 12-hour wall-clock time.
    if (minPlain &&
        hour !== '*' && !hour.includes('/') && !hour.includes(',') && !hour.includes('-')) {
      pieces.push(`at ${to12Hour(parseInt(hour, 10), parseInt(min, 10))}`);
    } else if (hourStepMatch && minPlain) {
      // "every N hours at :MM": an hour STEP paired with a fixed minute. The :MM
      // suffix is dropped when the minute is :00 (the common top-of-hour case), so
      // "0 */2 * * *" reads "every 2 hours" while "30 */2 * * *" reads
      // "every 2 hours at :30". Mirrors Claude's cronToHuman every-N-hours branch.
      const n = parseInt(hourStepMatch[1], 10);
      const m = parseInt(min, 10);
      const suffix = m === 0 ? '' : ` at :${String(m).padStart(2, '0')}`;
      pieces.push((n === 1 ? 'every hour' : `every ${n} hours`) + suffix);
    } else {
      if (min.startsWith('*/')) {
        const n = min.slice(2);
        pieces.push(n === '1' ? 'every minute' : `every ${n} minutes`);
      } else if (min !== '*') pieces.push(`at minute ${min}`);
      if (hour.startsWith('*/')) {
        const n = hour.slice(2);
        pieces.push(n === '1' ? 'every hour' : `every ${n} hours`);
      } else if (hour !== '*') pieces.push(`at hour ${hour}`);
    }
    if (dom !== '*') pieces.push(`on day ${dom}`);
    if (mon !== '*') pieces.push(`in month ${mon}`);
    if (dow !== '*') {
      const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      // The Monday-through-Friday range reads best as "Weekdays"; keep AMI's
      // explicit day expansion alongside it (superset) so the prose names the
      // concept Claude labels while still showing the underlying range.
      if (dow === '1-5') {
        pieces.push('on Weekdays (Mon to Fri)');
        return pieces.join(', ') || 'every minute';
      }
      const dayNames = dow.split(',').map(d => {
        if (d.includes('-')) {
          const [lo, hi] = d.split('-').map(Number);
          return `${days[lo % 7] || lo} to ${days[hi % 7] || hi}`;
        }
        return days[parseInt(d) % 7] || d;
      }).join(', ');
      pieces.push(`on ${dayNames}`);
    }
    return pieces.join(', ') || 'every minute';
  }

  private expireOld(onDue?: (job: CronJob) => void): void {
    const now = Date.now();
    for (const [id, job] of this.jobs) {
      if (!job.recurring) continue;
      if (job.permanent) continue;            // permanent jobs never age out
      const maxAgeDays = job.maxAgeDays ?? MAX_AGE_DAYS;
      if (maxAgeDays === 0) continue;         // 0 = unlimited
      const maxAge = maxAgeDays * 86400000;
      if (now - job.createdAt > maxAge) {
        // Aged recurring task fires one FINAL time before deletion so a
        // long-lived reminder isn't silently dropped.
        if (onDue && job.nextRun && now >= job.nextRun) {
          this.markRun(id);
          onDue(job);
        }
        this.delete(id);
        coreLog('cron', `Expired recurring job ${id} after ${maxAgeDays} days`);
      }
    }
  }

  private loadDurable(): void {
    let raw: string;
    try {
      raw = fs.readFileSync(this.persistPath, 'utf-8');
    } catch (err) {
      // A missing or inaccessible file is the normal empty state and is silent;
      // an UNEXPECTED read fault (I/O error, EISDIR, a directory where the file
      // should be, …) is a real failure and must leave a diagnostic trail instead
      // of vanishing identically to "no tasks yet". Mirrors Claude's readCronTasks
      // isFsInaccessible→silent vs logError split (src/utils/cronTasks.ts).
      if (!isFsInaccessible(err)) {
        const msg = err instanceof Error ? err.message : String(err);
        coreLog('cron', `Failed to read scheduled_tasks.json: ${msg}`);
      }
      return;
    }
    let data: unknown;
    try {
      data = JSON.parse(raw);
    } catch {
      // Corrupt persisted state — delete it so one bad file never blocks startup.
      coreLog('cron', 'Deleting corrupt scheduled_tasks.json');
      try { fs.unlinkSync(this.persistPath); } catch { /* best effort */ }
      return;
    }
    if (!Array.isArray(data)) {
      try { fs.unlinkSync(this.persistPath); } catch { /* best effort */ }
      return;
    }
    for (const entry of data) {
      // Per-task validation of untrusted on-disk JSON: shape + cron re-validation.
      if (!this.isValidPersistedJob(entry)) {
        coreLog('cron', `Dropping invalid persisted cron entry: ${JSON.stringify(entry)?.slice(0, 80)}`);
        continue;
      }
      if (entry.durable) this.jobs.set(entry.id, entry);
    }
  }

  private isValidPersistedJob(entry: unknown): entry is CronJob {
    if (!entry || typeof entry !== 'object') return false;
    const j = entry as Record<string, unknown>;
    if (typeof j.id !== 'string' || typeof j.cron !== 'string' || typeof j.prompt !== 'string') return false;
    if (typeof j.recurring !== 'boolean' || typeof j.durable !== 'boolean') return false;
    if (typeof j.createdAt !== 'number') return false;
    if (validateCronExpression(j.cron) !== null) return false; // re-validate the stored cron
    // Validate the optional run-budget fields so a corrupt on-disk maxRuns/runCount
    // cannot smuggle in a job that fires forever or is retired with a bogus count.
    if (j.maxRuns !== undefined && (!Number.isInteger(j.maxRuns) || (j.maxRuns as number) < 1)) return false;
    if (j.runCount !== undefined && (!Number.isInteger(j.runCount) || (j.runCount as number) < 0)) return false;
    // A corrupt on-disk maxAgeDays (negative / fractional) could silently disable or
    // truncate expiry, so reject the whole entry rather than trust it.
    if (j.maxAgeDays !== undefined && (!Number.isInteger(j.maxAgeDays) || (j.maxAgeDays as number) < 0)) return false;
    return true;
  }

  /** Low-level file write seam. Overridable in tests to simulate a crash mid-write
   *  so the atomic-save guarantee can be verified. Production impl is a plain write. */
  protected rawWriteFile(filePath: string, data: string): void {
    fs.writeFileSync(filePath, data, 'utf-8');
  }

  private saveDurable(): void {
    const dir = path.dirname(this.persistPath);
    const durable = Array.from(this.jobs.values()).filter(j => j.durable);
    const data = JSON.stringify(durable, null, 2);
    try {
      fs.mkdirSync(dir, { recursive: true });
      // FUNCTIONAL: atomic persistence. Write to a sibling temp file, then rename
      // it over the target (rename is atomic on the same filesystem). A crash or
      // disk error mid-write can no longer leave a half-written
      // scheduled_tasks.json that loadDurable would reject and delete, losing every
      // durable job. On write failure the temp file is cleaned up and the existing
      // persisted state is left untouched.
      const tmp = `${this.persistPath}.${process.pid}.tmp`;
      try {
        this.rawWriteFile(tmp, data);
        fs.renameSync(tmp, this.persistPath);
      } catch {
        try { fs.unlinkSync(tmp); } catch { /* temp already gone */ }
      }
    } catch { /* best effort */ }
  }
}

/** Render an (hour, minute) pair as a 12-hour wall-clock time like "9:05 am". */
export function to12Hour(hour: number, minute: number): string {
  const period = hour < 12 ? 'am' : 'pm';
  const h12 = hour % 12 === 0 ? 12 : hour % 12;
  return `${h12}:${String(minute).padStart(2, '0')} ${period}`;
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
 *  wrongly fire every day instead of only on Mondays. Mirrors Claude's expand-to-set
 *  then length===rangeSize wildcard detection (src/utils/cron.ts). */
export function fieldCoversFullRange(spec: string, min: number, max: number): boolean {
  if (spec === '*') return true;
  for (let v = min; v <= max; v++) {
    if (!matchField(v, spec, min, max)) return false;
  }
  return true;
}

// Release handlers for cron locks held by THIS process, run on graceful exit so a
// lock is freed even if stopTicker() is never reached (signal / crash / forgotten
// cleanup). Mirrors the session-lock exit-cleanup registry in session.ts.
const activeCronLockReleases = new Set<() => void>();
let cronExitHookInstalled = false;
function installCronExitHook(): void {
  if (cronExitHookInstalled) return;
  cronExitHookInstalled = true;
  process.once('exit', releaseAllCronLocks);
}

/** Release every cron lock this process still holds. Registered as the
 *  process-exit cleanup and callable directly (introspection / shutdown). */
export function releaseAllCronLocks(): void {
  for (const release of [...activeCronLockReleases]) {
    try { release(); } catch { /* best effort */ }
  }
}

let _scheduler: CronScheduler | null = null;
export function getScheduler(cwd?: string, sessionId?: string): CronScheduler {
  if (!_scheduler) _scheduler = new CronScheduler(cwd || process.cwd(), sessionId);
  return _scheduler;
}

export function resetScheduler(): void {
  if (_scheduler) _scheduler.stopTicker();
  _scheduler = null;
}
