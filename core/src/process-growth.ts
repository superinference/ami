/**
 * Sustained-descendant-growth (fork-bomb) breaker.
 *
 * AMI's bash tool can spawn whole process trees, and an agent stuck in a loop
 * can keep spawning them — a slow fork bomb that starves the host without any
 * single command looking abusive. A naive "too many descendants => kill"
 * breaker is wrong twice over: a legitimate parallel build momentarily has many
 * children (false trip), and killing the tree destroys the user's in-flight
 * work.
 *
 * This breaker trips ONLY on growth that is both fast AND sustained: the
 * descendant count must rise faster than {@link ProcessGrowthLimits.growthPerSecond}
 * across at least {@link ProcessGrowthLimits.minRisingSamples} consecutive
 * samples spanning at least {@link ProcessGrowthLimits.sustainMs}. A high but
 * flat count is merely "elevated" and never trips on its own; one long sampling
 * interval cannot fabricate a trip. When the table can't be read the sample is
 * `unknown` and neither trips nor clears an open streak (a blind spot must not
 * look like calm).
 *
 * The verdict is advisory: the caller PAUSES new autonomous spawns while a trip
 * stands, rather than killing running descendants — so existing work finishes
 * and the storm simply stops growing. Clock is carried on each sample so the
 * evaluation is pure and deterministically testable.
 *
 * Growth is tracked per scope (e.g. per-agent subtree) as well as host-wide, so
 * a single runaway agent trips without blaming the whole host.
 */

export interface ProcessGrowthSample {
  atMs: number;
  /** Descendants of the whole host, or null when the table could not be read. */
  hostDescendants: number | null;
  /** Descendant count per scope id (e.g. per agent/session). */
  scopes: ReadonlyMap<string, number>;
}

export interface ProcessGrowthLimits {
  /** A total this high is reported as `elevated`; on its own it never trips. */
  hostElevatedDescendants: number;
  /** New descendants per second — per scope or host-wide — that count as a storm. */
  growthPerSecond: number;
  /** How long growth must hold to trip. */
  sustainMs: number;
  /** How many consecutive rising samples growth must hold across to trip. */
  minRisingSamples: number;
}

export const DEFAULT_PROCESS_GROWTH_LIMITS: ProcessGrowthLimits = {
  hostElevatedDescendants: 1500,
  growthPerSecond: 40,
  sustainMs: 10_000,
  minRisingSamples: 3,
};

export interface ProcessGrowthTrip {
  /** null: the host total tripped; otherwise the scope whose tree grew. */
  scopeId: string | null;
  descendants: number;
  growthPerSecond: number;
}

interface RisingStreak {
  since: number;
  samples: number;
}

export interface ProcessGrowthState {
  previous: ProcessGrowthSample | null;
  /** Current rising streak per key (`''` = host). */
  streaks: Map<string, RisingStreak>;
}

export interface ProcessGrowthVerdict {
  level: 'ok' | 'elevated' | 'critical' | 'unknown';
  trips: ProcessGrowthTrip[];
  /** A rising streak is open somewhere; confirm it with short follow-up samples. */
  rising: boolean;
}

const HOST_KEY = '';

export function createProcessGrowthState(): ProcessGrowthState {
  return { previous: null, streaks: new Map() };
}

function growthRate(previous: number | undefined, current: number, elapsedMs: number): number {
  return previous === undefined || elapsedMs <= 0 ? 0 : ((current - previous) * 1000) / elapsedMs;
}

/**
 * Fold a new sample into the state and return the current verdict. Trips only on
 * sustained growth — never on high load alone, and never on a single long
 * interval. Unknown samples neither trip nor clear open streaks.
 */
export function evaluateProcessGrowth(
  state: ProcessGrowthState,
  sample: ProcessGrowthSample,
  limits: ProcessGrowthLimits,
): ProcessGrowthVerdict {
  if (sample.hostDescendants === null) {
    return { level: 'unknown', trips: [], rising: state.streaks.size > 0 };
  }
  const previous = state.previous;
  const elapsedMs = previous ? sample.atMs - previous.atMs : 0;
  const trips: ProcessGrowthTrip[] = [];
  let growing = false;
  const observe = (key: string, before: number | undefined, current: number): void => {
    const rate = growthRate(before, current, elapsedMs);
    if (rate < limits.growthPerSecond) {
      state.streaks.delete(key);
      return;
    }
    growing = true;
    const streak = state.streaks.get(key) ?? { since: previous?.atMs ?? sample.atMs, samples: 0 };
    streak.samples += 1;
    state.streaks.set(key, streak);
    if (streak.samples >= limits.minRisingSamples && sample.atMs - streak.since >= limits.sustainMs) {
      trips.push({
        scopeId: key === HOST_KEY ? null : key,
        descendants: current,
        growthPerSecond: Math.round(rate),
      });
    }
  };
  observe(HOST_KEY, previous?.hostDescendants ?? undefined, sample.hostDescendants);
  for (const [scopeId, count] of sample.scopes) {
    observe(scopeId, previous?.scopes.get(scopeId), count);
  }
  // Drop streaks for scopes that disappeared this sample (their tree is gone).
  for (const key of [...state.streaks.keys()]) {
    if (key !== HOST_KEY && !sample.scopes.has(key)) {
      state.streaks.delete(key);
    }
  }
  state.previous = sample;
  if (trips.length > 0) {
    return { level: 'critical', trips, rising: true };
  }
  return {
    level: growing || sample.hostDescendants >= limits.hostElevatedDescendants ? 'elevated' : 'ok',
    trips,
    rising: growing,
  };
}

/**
 * Count descendants per root pid from a flat pid/ppid table (e.g. `ps -eo pid,ppid`).
 * Self-parented rows (pid === ppid) are ignored so a row that is its own parent
 * cannot inflate a count or loop the walk; `skip` prunes whole subtrees (the walk
 * stops at a skipped pid).
 */
export function countDescendantsByRoot(
  rows: readonly { pid: number; ppid: number }[],
  roots: readonly number[],
  skip: ReadonlySet<number> = new Set(),
): Map<number, number> {
  const children = new Map<number, number[]>();
  for (const row of rows) {
    if (row.pid === row.ppid) {
      continue;
    }
    const siblings = children.get(row.ppid);
    if (siblings) {
      siblings.push(row.pid);
    } else {
      children.set(row.ppid, [row.pid]);
    }
  }
  const counts = new Map<number, number>();
  for (const root of roots) {
    let count = 0;
    const seen = new Set<number>([root]);
    const stack = [...(children.get(root) ?? [])];
    while (stack.length > 0) {
      const pid = stack.pop();
      if (pid === undefined || seen.has(pid) || skip.has(pid)) {
        continue;
      }
      seen.add(pid);
      count += 1;
      stack.push(...(children.get(pid) ?? []));
    }
    counts.set(root, count);
  }
  return counts;
}
