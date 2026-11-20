/**
 * Turn-scoped workload (execution-origin) tag via AsyncLocalStorage.
 *
 * This is AMI's single canonical primitive for "what triggered the current
 * turn" — a cron-scheduled turn, a forked/detached slash command, a sub-agent,
 * etc. — queryable from ANY code on the async chain via {@link getWorkload}
 * without threading a handle. Mirrors the reference's `utils/workloadContext.ts`.
 *
 * WHY AsyncLocalStorage (not a global mutable slot / push-pop stack):
 * void-detached background work (cron fire callbacks, forked slash commands,
 * sub-agent runs) yields at its first `await`. The parent turn's synchronous
 * continuation — including any `finally` that would reset a global slot — runs
 * to completion BEFORE the detached closure resumes, so a `global.workload =
 * 'cron'` set at the top of the closure is deterministically clobbered, and a
 * leaked tag then becomes sticky across every subsequent turn. ALS captures the
 * context at invocation time and survives every `await` in that chain, isolated
 * from the parent and from sibling chains.
 *
 * {@link AnalyticsTracker} consumes this storage for invocation-source
 * resolution/attribution, so there is exactly ONE ALS backing both the
 * low-level `runWithSource`/`currentSource` API and any direct
 * {@link getWorkload} consumer — no parallel context mechanism.
 */

import { AsyncLocalStorage } from 'async_hooks';

/**
 * The fixed, low-cardinality workload vocabulary. Kept lowercase so it matches
 * the server-side entrypoint sanitizer (`[a-z0-9_-]{0,32}`) and AMI's analytics
 * invocation-source segmentation. `cron` distinguishes a scheduler-fired turn
 * from an interactive/detached one (the reference's `WORKLOAD_CRON`).
 */
export type Workload = 'cron';

/** Invocation-origin tag for a turn fired by the cron scheduler. */
export const WORKLOAD_CRON: Workload = 'cron';

/**
 * The one shared store. Holds `{ workload }` (possibly `undefined`) whenever a
 * boundary is active; `getStore()` returns `undefined` only when there is NO
 * active boundary at all — letting consumers distinguish "no boundary" from "a
 * boundary deliberately carrying no workload".
 */
const workloadStorage = new AsyncLocalStorage<{ workload: string | undefined }>();

/** The current turn's workload tag, or `undefined` when none is in scope. */
export function getWorkload(): string | undefined {
  return workloadStorage.getStore()?.workload;
}

/**
 * Whether an ALS workload boundary is active on this async chain (regardless of
 * whether its workload is defined). A boundary established with an `undefined`
 * workload still reports `true`, so a fresh {@link runWithWorkload}`(undefined,…)`
 * can be detected as deliberately shadowing any ambient/leaked outer context.
 */
export function hasWorkloadBoundary(): boolean {
  return workloadStorage.getStore() !== undefined;
}

/**
 * Run `fn` within a workload ALS boundary: {@link getWorkload} resolves to
 * `workload` for the ENTIRE async chain of `fn` — across every `await` — and is
 * isolated from sibling/parent chains.
 *
 * ALWAYS establishes a fresh boundary, even when `workload` is `undefined`:
 * a pass-through (`return fn()`) would let `getWorkload()` inside `fn` see a
 * leaked ambient context, which — once leaked — is sticky forever as each
 * turn's completion re-propagates it to the next turn's scheduling chain.
 * Calling `.run()` unconditionally guarantees `getWorkload()` inside `fn`
 * returns exactly what the caller passed (including `undefined`).
 *
 * Returns `fn`'s return value (including a Promise, whose continuation keeps
 * the boundary). Prefer this for void-detached background work (cron fire,
 * forked slash commands, sub-agents) whose continuation outlives the caller.
 */
export function runWithWorkload<T>(workload: string | undefined, fn: () => T): T {
  return workloadStorage.run({ workload }, fn);
}
