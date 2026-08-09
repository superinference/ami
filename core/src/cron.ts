import * as fs from 'fs';
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
}

const MAX_AGE_DAYS = 7;
const MAX_AGE_MS = MAX_AGE_DAYS * 24 * 60 * 60 * 1000;
const MAX_JOBS = 50;
const LOCK_STALE_MS = 120_000;

function computeJitter(cronExpression: string, jobId: string): number {
  const hash = jobId.split('').reduce((a, c) => ((a << 5) - a + c.charCodeAt(0)) | 0, 0);
  const normalizedHash = Math.abs(hash) / 2147483647;
  const estimatedPeriodMs = estimatePeriod(cronExpression);
  const maxJitterMs = Math.min(estimatedPeriodMs * 0.1, 15 * 60 * 1000);
  return Math.floor(normalizedHash * maxJitterMs);
}

function estimatePeriod(cron: string): number {
  const parts = cron.split(/\s+/);
  if (parts.length < 2) return 86400000;
  const [min, hour] = parts;
  if (min.startsWith('*/')) return parseInt(min.slice(2), 10) * 60000;
  if (hour.startsWith('*/')) return parseInt(hour.slice(2), 10) * 3600000;
  if (hour === '*') return 3600000;
  return 86400000;
}

export function wrapPromptSafely(prompt: string): string {
  const fenceLength = 4 + Math.floor(Math.random() * 8);
  const fence = '`'.repeat(fenceLength);
  return `Previously scheduled task (missed while offline):\n${fence}\n${prompt}\n${fence}`;
}

export class CronScheduler {
  private jobs: Map<string, CronJob> = new Map();
  private cwd: string;
  private persistPath: string;
  private lockPath: string;
  private sessionId: string;
  private timers: Map<string, NodeJS.Timeout> = new Map();
  private tickInterval: NodeJS.Timeout | null = null;

  constructor(cwd: string, sessionId?: string) {
    this.cwd = cwd;
    this.persistPath = path.join(cwd, '.superinference', 'scheduled_tasks.json');
    this.lockPath = path.join(cwd, '.superinference', 'cron.lock');
    this.sessionId = sessionId || `session_${process.pid}_${Date.now().toString(36)}`;
    this.loadDurable();
  }

  create(opts: { cron: string; prompt: string; recurring?: boolean; durable?: boolean }): CronJob {
    if (this.jobs.size >= MAX_JOBS) {
      throw new Error(`Maximum of ${MAX_JOBS} cron jobs reached. Delete existing jobs first.`);
    }
    const id = `cron_${this.getNextIdNum()}_${Date.now().toString(36)}`;
    const job: CronJob = {
      id,
      cron: opts.cron,
      prompt: opts.prompt,
      recurring: opts.recurring ?? true,
      durable: opts.durable ?? false,
      createdAt: Date.now(),
      nextRun: this.computeNextRun(opts.cron),
    };
    this.jobs.set(id, job);
    if (job.durable) this.saveDurable();
    return job;
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

  delete(id: string): boolean {
    const job = this.jobs.get(id);
    if (!job) return false;
    const timer = this.timers.get(id);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(id);
    }
    this.jobs.delete(id);
    if (job.durable) this.saveDurable();
    return true;
  }

  list(): CronJob[] {
    return Array.from(this.jobs.values());
  }

  get(id: string): CronJob | undefined {
    return this.jobs.get(id);
  }

  getDueJobs(): CronJob[] {
    const now = Date.now();
    const due: CronJob[] = [];
    for (const job of this.jobs.values()) {
      if (job.nextRun) {
        const jitter = computeJitter(job.cron, job.id);
        if (now >= job.nextRun + jitter) {
          due.push(job);
        }
      }
    }
    return due;
  }

  markRun(id: string): void {
    const job = this.jobs.get(id);
    if (!job) return;
    job.lastRun = Date.now();
    job.lastFiredAt = Date.now();
    if (job.recurring) {
      job.nextRun = this.computeNextRun(job.cron);
    } else {
      this.jobs.delete(id);
    }
    this.saveDurable();
  }

  startTicker(onDue: (job: CronJob) => void): void {
    if (this.tickInterval) return;
    this.tickInterval = setInterval(() => {
      this.expireOld();
      const due = this.getDueJobs();
      for (const job of due) {
        if (job.durable && !this.acquireLock()) continue;
        this.markRun(job.id);
        onDue(job);
      }
    }, 5_000);
    this.tickInterval.unref();
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
        const lock = JSON.parse(fs.readFileSync(this.lockPath, 'utf-8'));
        if (Date.now() - lock.timestamp < LOCK_STALE_MS) {
          return lock.sessionId === this.sessionId;
        }
      }
      const dir = path.dirname(this.lockPath);
      fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this.lockPath, JSON.stringify({
        sessionId: this.sessionId,
        pid: process.pid,
        timestamp: Date.now(),
      }));
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
      if (!job.recurring) continue;
      const nextRun = this.computeNextRun(job.cron, job.lastFiredAt ?? job.createdAt);
      if (nextRun && nextRun < now) missed.push(job);
    }
    return missed;
  }

  computeNextRun(cron: string, fromTime?: number): number {
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
    throw new Error(`Invalid cron expression: ${cron}`);
  }

  formatSchedule(cron: string): string {
    const parts = cron.trim().split(/\s+/);
    if (parts.length !== 5) return cron;
    const [min, hour, dom, mon, dow] = parts;
    const pieces: string[] = [];
    if (min.startsWith('*/')) pieces.push(`every ${min.slice(2)} minutes`);
    else if (min !== '*') pieces.push(`at minute ${min}`);
    if (hour !== '*') pieces.push(`at hour ${hour}`);
    if (dom !== '*') pieces.push(`on day ${dom}`);
    if (mon !== '*') pieces.push(`in month ${mon}`);
    if (dow !== '*') {
      const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      const dayNames = dow.split(',').map(d => days[parseInt(d)] || d).join(', ');
      pieces.push(`on ${dayNames}`);
    }
    return pieces.join(', ') || 'every minute';
  }

  private expireOld(): void {
    const now = Date.now();
    for (const [id, job] of this.jobs) {
      if (job.recurring) {
        const maxAge = (job.maxAgeDays ?? MAX_AGE_DAYS) * 86400000;
        if (now - job.createdAt > maxAge) {
          this.delete(id);
          coreLog('cron', `Expired recurring job ${id} after ${job.maxAgeDays ?? MAX_AGE_DAYS} days`);
        }
      }
    }
  }

  private loadDurable(): void {
    try {
      const raw = fs.readFileSync(this.persistPath, 'utf-8');
      const data = JSON.parse(raw) as CronJob[];
      for (const job of data) {
        if (job.durable) {
          this.jobs.set(job.id, job);
        }
      }
    } catch {
      // No file or invalid
    }
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
