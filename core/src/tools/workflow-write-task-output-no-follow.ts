// Split from workflow.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';

/**
 * Write a workflow sub-agent's captured output WITHOUT following a symlink at
 * the destination path. The tasks directory is writable by sandboxed agents, so
 * a plain `fs.writeFileSync` would follow a planted symlink and clobber whatever
 * it points to on the host. Opening with `O_NOFOLLOW | O_EXCL`-equivalent flags
 * (fail on symlink → ELOOP) closes that vector — parity with the reference task
 * output writer, which opens every output file with O_NOFOLLOW for exactly this
 * reason. On platforms lacking O_NOFOLLOW (Windows) the constant is 0 and the
 * open degrades to a normal truncating create; the sandbox attack vector is
 * Unix-only. The file is truncated so a resumed/retried agent overwrites a prior
 * run's output.
 */
export function writeTaskOutputNoFollow(filePath: string, data: string): void {
  const O_NOFOLLOW = fs.constants.O_NOFOLLOW ?? 0;
  const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_TRUNC | O_NOFOLLOW;
  const fd = fs.openSync(filePath, flags, 0o600);
  try {
    fs.writeFileSync(fd, data);
  } finally {
    fs.closeSync(fd);
  }
}

/**
 * Minimum gap between on-disk flushes of a running agent's partial output. The
 * first chunk is flushed immediately; subsequent flushes are throttled to this
 * interval so a chatty agent does not thrash the filesystem while still letting
 * `task_output` tail live progress (parity: the reference streams incremental
 * agent output rather than revealing it only on completion).
 */
export const PROGRESS_FLUSH_INTERVAL_MS = 500;

/** Reduce an opaque caller-supplied id to a single safe path segment so a
 *  runId / resumeKey can never escape the `.ami/workflows/` tree. Mirrors the
 *  sanitization already applied to the resume cache namespace. */
export function sanitizeWorkflowRunId(id: string): string {
  return id.replace(/[^A-Za-z0-9_-]/g, '_');
}

/** Persisted record for an opt-in, resumable workflow run (keyed by runId). */
export interface WorkflowRunRecord {
  /** The fully-resolved script source — persisted so a resume reproduces the
   *  exact body even if the original scriptPath is later moved or deleted. */
  script: string;
  /** The scriptPath the run was launched from, if any (informational). */
  scriptPath?: string;
  /** The saved-workflow name the run was launched from, if any (informational). */
  name?: string;
  meta: { name: string; description: string };
  createdAt: string;
}

/** Exported for direct unit coverage of the runs-directory path. */
export function workflowRunsDir(cwd: string): string {
  return path.join(cwd, '.ami', 'workflows', '.runs');
}

/** Best-effort persistence of a run record so `resumeFromRunId` can reload it.
 *  Failures (read-only cwd, `.ami` is a file, …) are swallowed — persistence is
 *  an optimization, never a hard requirement for the run to complete. */
export function persistWorkflowRun(cwd: string, runId: string, record: WorkflowRunRecord): void {
  try {
    const dir = workflowRunsDir(cwd);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, `${runId}.json`), JSON.stringify(record));
  } catch { /* best-effort: a run must not fail because its record could not be saved */ }
}

/** Load a persisted run record by id, or null if absent / unreadable / malformed. */
export function loadWorkflowRun(cwd: string, runId: string): WorkflowRunRecord | null {
  try {
    const raw = fs.readFileSync(path.join(workflowRunsDir(cwd), `${runId}.json`), 'utf-8');
    const rec = JSON.parse(raw) as WorkflowRunRecord;
    return typeof rec?.script === 'string' ? rec : null;
  } catch {
    return null;
  }
}

/** A persisted workflow run record with its id attached (for enumeration). */
export interface WorkflowRunListing extends WorkflowRunRecord {
  runId: string;
}

/** Enumerate all persisted (resumable, runId-tagged) workflow runs, most-recent
 *  first. Powers `/flows` "recent/saved runs". Returns [] when the runs dir is
 *  absent / unreadable, and skips any malformed record (best-effort). */
export function listWorkflowRuns(cwd: string): WorkflowRunListing[] {
  let files: string[];
  try {
    files = fs.readdirSync(workflowRunsDir(cwd)).filter(f => f.endsWith('.json'));
  } catch {
    return [];
  }
  const runs: WorkflowRunListing[] = [];
  for (const f of files) {
    const runId = f.replace(/\.json$/, '');
    const rec = loadWorkflowRun(cwd, runId);
    if (rec) runs.push({ ...rec, runId });
  }
  // Newest first by createdAt (ISO strings sort lexicographically); ties keep order.
  runs.sort((a, b) => (b.createdAt || '').localeCompare(a.createdAt || ''));
  return runs;
}

/** Save a workflow SCRIPT to `.ami/workflows/<name>.js` so it can be relaunched by
 *  `name` later (the reference's "/flows to monitor and SAVE"). The name is reduced
 *  to a single safe path segment so it can never escape the workflows dir. Returns
 *  the absolute path written. Throws on an empty/invalid name. */
export function saveWorkflowScript(cwd: string, name: string, script: string): { path: string; name: string } {
  const safe = sanitizeWorkflowRunId(name).replace(/^_+|_+$/g, '');
  if (!safe) throw new Error('A non-empty workflow name is required.');
  const dir = path.join(cwd, '.ami', 'workflows');
  fs.mkdirSync(dir, { recursive: true });
  const dest = path.join(dir, `${safe}.js`);
  // O_NOFOLLOW-safe write (same hardening as task output) so a planted symlink at
  // the destination cannot redirect the write outside the workflows dir.
  writeTaskOutputNoFollow(dest, script);
  return { path: dest, name: safe };
}
