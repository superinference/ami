/**
 * Wave/lane orchestration planner — pure building blocks (no engine, no UI).
 *
 * This is the file-disjoint wave planner + serial-integration ordering + stable
 * resume primitives that drive a "swarm" of parallel sub-agents:
 *
 *  - {@link planWaves} partitions work units — each declaring the files it will
 *    touch and its upstream dependencies — into an ORDERED list of WAVES. Every
 *    unit inside a single wave is file-disjoint from the others in that wave, so
 *    the wave can be fanned out in parallel with no write conflict; successive
 *    waves are integrated serially (a later wave only starts once its
 *    dependencies have been scheduled).
 *  - {@link computeUnitSignature} / {@link computeRunId} derive stable content
 *    hashes so a run has a durable identity independent of cosmetic relabels.
 *  - {@link computeResumePlan} computes the longest unchanged PREFIX of a prior
 *    completed run (keyed by a stable run id): on re-launch, that prefix is
 *    replayed instantly from cache and only new/edited units (and everything
 *    after the first change) are re-run.
 *
 * These are consumed by the workflow runtime's `wave()` capability
 * (`tools/workflow-runtime.ts`), which fans each wave out through its bounded
 * concurrency scheduler and persists progress to its agent-result cache so a
 * crashed/stopped run is cheaply resumable. Keeping the planning/resume logic
 * here (pure, dependency-free) keeps it unit-testable in isolation and reusable
 * by any future orchestration consumer without pulling in orchestration glue.
 */

import * as crypto from 'crypto';

/** A single unit of file-disjoint work in a wave plan. */
export interface WorkUnit {
  /** Stable, unique identifier for this unit within the run. */
  readonly id: string;
  /** Paths this unit will read/write. Two units sharing any path cannot run in
   *  the same wave (they would race on the same file). */
  readonly files: readonly string[];
  /** Ids of units that must be scheduled in an EARLIER wave than this one. */
  readonly deps?: readonly string[];
  /** Opaque content that participates in the unit's signature (so editing the
   *  work — e.g. the prompt — busts the resume cache for this unit onward). */
  readonly payload?: unknown;
}

/** An ordered wave plan: a serial list of parallelizable waves. */
export interface WavePlan {
  /** Ordered waves; each inner array lists the ids runnable concurrently. */
  readonly waves: ReadonlyArray<readonly string[]>;
  /** Flattened serial-integration order (waves concatenated in order). */
  readonly order: readonly string[];
}

/** A unit paired with its content signature, for resume comparison. */
export interface ResumeUnit {
  readonly id: string;
  readonly signature: string;
}

/** A durable record of a previously-completed run. */
export interface PriorRun {
  /** The run id this manifest belongs to (see {@link computeRunId}). */
  readonly runId: string;
  /** Map of completed unit id → the signature it completed with. */
  readonly completed: Readonly<Record<string, string>>;
}

/** The result of planning a resume against a prior run. */
export interface ResumePlan {
  /** Stable id of the current run. */
  readonly runId: string;
  /** Index of the first unit that must be re-run (end of the unchanged prefix). */
  readonly cutIndex: number;
  /** Unit ids replayable from cache (the longest unchanged prefix). */
  readonly replay: readonly string[];
  /** Unit ids that must be re-executed (the changed unit and everything after). */
  readonly rerun: readonly string[];
}

function assertUniqueIds(units: readonly { id: string }[]): void {
  const seen = new Set<string>();
  for (const u of units) {
    if (typeof u.id !== 'string' || u.id.length === 0) {
      throw new Error('planWaves: every unit must have a non-empty string id');
    }
    if (seen.has(u.id)) {
      throw new Error(`planWaves: duplicate unit id '${u.id}'`);
    }
    seen.add(u.id);
  }
}

/**
 * Partition `units` into an ordered list of file-disjoint waves.
 *
 * A unit is placed in the earliest wave in which (a) all of its `deps` were
 * placed in a STRICTLY earlier wave, and (b) it shares no file with any other
 * unit already placed in that same wave. Within a wave, input order is preserved
 * for deterministic output. Throws on duplicate ids, unknown dependency ids, or
 * an unschedulable set (a dependency cycle).
 */
export function planWaves(units: readonly WorkUnit[]): WavePlan {
  assertUniqueIds(units);
  const ids = new Set(units.map((u) => u.id));
  for (const u of units) {
    for (const d of u.deps ?? []) {
      if (!ids.has(d)) {
        throw new Error(`planWaves: unit '${u.id}' depends on unknown unit '${d}'`);
      }
      if (d === u.id) {
        throw new Error(`planWaves: unit '${u.id}' cannot depend on itself`);
      }
    }
  }

  const scheduled = new Set<string>();
  const waves: string[][] = [];
  const remaining = [...units];

  while (remaining.length > 0) {
    const wave: WorkUnit[] = [];
    const claimedFiles = new Set<string>();
    for (const u of remaining) {
      // Dependencies must already be in an earlier wave (not merely this one).
      const depsReady = (u.deps ?? []).every((d) => scheduled.has(d));
      if (!depsReady) continue;
      // File-disjointness within the wave: skip if any file is already claimed.
      if (u.files.some((f) => claimedFiles.has(f))) continue;
      for (const f of u.files) claimedFiles.add(f);
      wave.push(u);
    }
    if (wave.length === 0) {
      throw new Error(
        `planWaves: unschedulable units (dependency cycle) among: ${remaining.map((u) => u.id).join(', ')}`,
      );
    }
    for (const u of wave) {
      scheduled.add(u.id);
      const idx = remaining.indexOf(u);
      remaining.splice(idx, 1);
    }
    waves.push(wave.map((u) => u.id));
  }

  return { waves, order: waves.flat() };
}

/** Stable content hash of a work unit (id + sorted files + sorted deps + payload). */
export function computeUnitSignature(unit: WorkUnit): string {
  const norm = {
    id: unit.id,
    files: [...unit.files].sort(),
    deps: [...(unit.deps ?? [])].sort(),
    payload: unit.payload ?? null,
  };
  return crypto.createHash('sha256').update(JSON.stringify(norm)).digest('hex');
}

/**
 * Stable id for a run, derived from the ORDERED list of unit ids. Reordering,
 * adding, or removing units changes the run id (it is a different run shape);
 * editing a unit's content does NOT (that is detected per-unit by signature so
 * the unchanged prefix can still be replayed).
 */
export function computeRunId(units: readonly { id: string }[]): string {
  assertUniqueIds(units);
  return crypto
    .createHash('sha256')
    .update(JSON.stringify(units.map((u) => u.id)))
    .digest('hex');
}

/**
 * Compute the resume plan for `units` against an optional `prior` run. When the
 * prior run id does not match the current one (or there is no prior run), the
 * whole run is re-executed. Otherwise the longest leading run of units whose
 * recorded signature matches the current signature is replayed from cache, and
 * the first changed unit — plus everything after it — is re-run.
 */
export function computeResumePlan(prior: PriorRun | null, units: readonly ResumeUnit[]): ResumePlan {
  const runId = computeRunId(units);
  if (!prior || prior.runId !== runId) {
    return { runId, cutIndex: 0, replay: [], rerun: units.map((u) => u.id) };
  }
  let cutIndex = 0;
  for (; cutIndex < units.length; cutIndex++) {
    const u = units[cutIndex]!;
    if (prior.completed[u.id] !== u.signature) break;
  }
  return {
    runId,
    cutIndex,
    replay: units.slice(0, cutIndex).map((u) => u.id),
    rerun: units.slice(cutIndex).map((u) => u.id),
  };
}
