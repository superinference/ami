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
}

const MAX_AGE_DAYS = 7;
const MAX_AGE_MS = MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
const MAX_JOBS = 50;
const LOCK_STALE_MS = 120_000;
/** Recurring jobs may not fire more often than once per minute (abuse floor). */
const MIN_INTERVAL_MS = 60_000;

/** Tunable jitter parameters (recurring forward jitter vs one-shot backward jitter). */
export interface CronJitterConfig {
  /** Fraction of the inter-fire gap used as the max forward jitter for recurring jobs. */
  recurringFrac: number;
  /** Hard cap (ms) on recurring forward jitter. */
  recurringCapMs: number;
  /** Max backward (early) jitter (ms) for one-shot jobs. */
  oneShotMaxMs: number;
  /** One-shot jitter only applies when the fire minute is divisible by this (preserves :00/:30 pins). */
  oneShotMinuteMod: number;
}

export const DEFAULT_CRON_JITTER: CronJitterConfig = {
  recurringFrac: 0.1,
  recurringCapMs: 15 * 60 * 1000,
  oneShotMaxMs: 30 * 1000,
  oneShotMinuteMod: 30,
};

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

function computeJitter(cronExpression: string, jobId: string, cfg: CronJitterConfig = DEFAULT_CRON_JITTER): number {
  const hash = jobId.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0);
  const normalizedHash = Math.abs(hash) / 2147483647;
  const estimatedPeriodMs = estimatePeriod(cronExpression);
  const maxJitterMs = Math.min(estimatedPeriodMs * cfg.recurringFrac, cfg.recurringCapMs);
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

function estimatePeriod(cron: string): number {
  // Crons reaching here are always validated (5 fields), so min/hour exist.
  const [min, hour] = cron.split(/\s+/);
  if (min.startsWith('*/')) return parseInt(min.slice(2), 10) * 60000;
  if (hour.startsWith('*/')) return parseInt(hour.slice(2), 10) * 3600000;
  if (hour === '*') return 3600000;
  return 86400000;
}

export function wrapPromptSafely(prompt: string): string {
  // CommonMark: a fenced block is closed only by a fence at least as long as the
  // opening one. Size the fence to exceed the longest backtick run in the prompt
  // so an injected ``` cannot break out of the fence. Minimum 3.
  let longestRun = 0;
  let current = 0;
  for (const ch of prompt) {
    if (ch === '`') { current++; if (current > longestRun) longestRun = current; }
    else current = 0;
  }
  const fence = '`'.repeat(Math.max(3, longestRun + 1));
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
  /** When set and returning true, the tick defers firing (session is mid-turn). */
  private busyCheck: (() => boolean) | null = null;

  constructor(cwd: string, sessionId?: string, jitterConfig: CronJitterConfig = DEFAULT_CRON_JITTER) {
    this.cwd = cwd;
    this.persistPath = path.join(projectStateDir(cwd), 'scheduled_tasks.json');
    this.lockPath = path.join(projectStateDir(cwd), 'cron.lock');
    this.sessionId = sessionId || `session_${process.pid}_${Date.now().toString(36)}`;
    this.jitterConfig = jitterConfig;
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

  /** Register a predicate; while it returns true the ticker defers firing jobs
   *  (e.g. the session is loading / mid-turn) so a fire never interrupts work. */
  setBusyCheck(fn: (() => boolean) | null): void {
    this.busyCheck = fn;
  }

  create(opts: { cron: string; prompt: string; recurring?: boolean; durable?: boolean; agentId?: string; permanent?: boolean }): CronJob {
    if (this.jobs.size >= MAX_JOBS) {
      throw new Error(`Maximum of ${MAX_JOBS} cron jobs reached. Delete existing jobs first.`);
    }
    // Field-level validation up front with a clear error (not a brute-force throw).
    const validationError = validateCronExpression(opts.cron);
    if (validationError) {
      throw new Error(`Invalid cron expression: ${validationError}`);
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
    const id = `cron_${this.getNextIdNum()}_${Date.now().toString(36)}`;
    const nextRun = this.computeNextRun(opts.cron);
    const job: CronJob = {
      id,
      cron: opts.cron,
      prompt: opts.prompt,
      recurring,
      durable: opts.durable ?? false,
      createdAt: Date.now(),
      nextRun: nextRun ?? undefined,
      ...(opts.agentId ? { agentId: opts.agentId } : {}),
      ...(opts.permanent ? { permanent: true } : {}),
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

  getDueJobs(): CronJob[] {
    const now = Date.now();
    const due: CronJob[] = [];
    for (const job of this.jobs.values()) {
      if (!job.nextRun) continue;
      if (job.recurring) {
        // Forward jitter spreads recurring fires to avoid thundering herds.
        const jitter = computeJitter(job.cron, job.id, this.jitterConfig);
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
    return Math.floor(norm * this.jitterConfig.oneShotMaxMs);
  }

  markRun(id: string): void {
    const job = this.jobs.get(id);
    if (!job) return;
    job.lastRun = Date.now();
    job.lastFiredAt = Date.now();
    if (job.recurring) {
      // Never throw inside the fire loop: a cron that no longer has a future
      // match leaves nextRun undefined (the job simply stops firing).
      job.nextRun = this.safeComputeNextRun(job.cron) ?? undefined;
    } else {
      this.jobs.delete(id);
    }
    this.saveDurable();
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
  }

  /** One scheduler tick: expire stale jobs, then run every due job once. Firing
   *  is deferred while the session is busy (mid-turn) so a cron never interrupts
   *  in-flight work — the job stays due and fires on a later tick. */
  private tick(onDue: (job: CronJob) => void): void {
    this.expireOld(onDue);
    if (this.busyCheck?.()) return;
    const due = this.getDueJobs();
    for (const job of due) {
      if (job.durable && !this.acquireLock()) continue;
      this.markRun(job.id);
      onDue(job);
    }
  }

  stopTicker(): void {
    if (this.tickInterval) {
      clearInterval(this.tickInterval);
      this.tickInterval = null;
    }
    this.releaseLock();
  }

  private acquireLock(): boolean {
    try {
      if (fs.existsSync(this.lockPath)) {
        let lock: { sessionId?: string; pid?: number; timestamp?: number } | null = null;
        try {
          lock = JSON.parse(fs.readFileSync(this.lockPath, 'utf-8'));
        } catch {
          lock = null; // corrupt/unparseable → treat as stale and reclaim
        }
        if (lock && typeof lock === 'object' && lock.sessionId && typeof lock.timestamp === 'number') {
          // We already own it.
          if (lock.sessionId === this.sessionId) return true;
          // Primary staleness signal: is the owning PID still alive? Fall back to
          // the age window when the pid is unknown.
          const ownerAlive = typeof lock.pid === 'number'
            ? isProcessRunning(lock.pid)
            : Date.now() - lock.timestamp < LOCK_STALE_MS;
          if (ownerAlive) return false; // live owner — never steal the lease
          coreLog('cron', `Reclaiming stale lock from dead owner (pid ${lock.pid ?? '?'}, session ${lock.sessionId})`);
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
      coreLog('cron', `Acquired cron lock (session ${this.sessionId}, pid ${process.pid})`);
      return true;
    } catch {
      return false;
    }
  }

  private releaseLock(): void {
    try {
      if (fs.existsSync(this.lockPath)) {
        const lock = JSON.parse(fs.readFileSync(this.lockPath, 'utf-8'));
        if (lock.sessionId === this.sessionId) {
          fs.unlinkSync(this.lockPath);
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

    const domRestricted = domSpec !== '*';
    const dowRestricted = dowSpec !== '*';

    for (let i = 0; i < 527040; i++) {
      const minMatch = matchField(check.getMinutes(), minSpec, 0, 59);
      const hourMatch = matchField(check.getHours(), hourSpec, 0, 23);
      const monMatch = matchField(check.getMonth() + 1, monSpec, 1, 12);
      const domMatch = matchField(check.getDate(), domSpec, 1, 31);
      const dowMatch = matchField(check.getDay(), dowSpec, 0, 6);

      // Standard cron: when BOTH dom and dow are restricted, use OR semantics
      let dayMatch: boolean;
      if (domRestricted && dowRestricted) {
        dayMatch = domMatch || dowMatch;
      } else {
        dayMatch = domMatch && dowMatch;
      }

      if (minMatch && hourMatch && monMatch && dayMatch) {
        return check.getTime();
      }
      check.setMinutes(check.getMinutes() + 1);
    }
    // Structurally valid but no match within ~366 days (e.g. Feb 30): return null
    // rather than throwing, so the fire loop degrades gracefully.
    return null;
  }

  formatSchedule(cron: string): string {
    const parts = cron.trim().split(/\s+/);
    if (parts.length !== 5) return cron;
    const [min, hour, dom, mon, dow] = parts;
    const pieces: string[] = [];

    // A specific minute + specific hour reads best as a 12-hour wall-clock time.
    if (min !== '*' && !min.includes('/') && !min.includes(',') && !min.includes('-') &&
        hour !== '*' && !hour.includes('/') && !hour.includes(',') && !hour.includes('-')) {
      pieces.push(`at ${to12Hour(parseInt(hour, 10), parseInt(min, 10))}`);
    } else {
      if (min.startsWith('*/')) pieces.push(`every ${min.slice(2)} minutes`);
      else if (min !== '*') pieces.push(`at minute ${min}`);
      if (hour.startsWith('*/')) pieces.push(`every ${hour.slice(2)} hours`);
      else if (hour !== '*') pieces.push(`at hour ${hour}`);
    }
    if (dom !== '*') pieces.push(`on day ${dom}`);
    if (mon !== '*') pieces.push(`in month ${mon}`);
    if (dow !== '*') {
      const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
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
    } catch {
      return; // no file
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
    return true;
  }

  private saveDurable(): void {
    const dir = path.dirname(this.persistPath);
    const durable = Array.from(this.jobs.values()).filter(j => j.durable);
    const data = JSON.stringify(durable, null, 2);
    try {
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this.persistPath, data, 'utf-8');
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
      if (max === 6) { lo = lo % 7; hi = hi % 7; }
      if (isNaN(lo) || isNaN(hi)) continue;
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

let _scheduler: CronScheduler | null = null;
export function getScheduler(cwd?: string, sessionId?: string): CronScheduler {
  if (!_scheduler) _scheduler = new CronScheduler(cwd || process.cwd(), sessionId);
  return _scheduler;
}

export function resetScheduler(): void {
  if (_scheduler) _scheduler.stopTicker();
  _scheduler = null;
}
