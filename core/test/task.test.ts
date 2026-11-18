import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  planWaves,
  computeUnitSignature,
  computeRunId,
  computeResumePlan,
  type WorkUnit,
  type PriorRun,
} from '../src/task';
import { executeWorkflow, type WorkflowAgentCache } from '../src/tools/workflow-runtime';

describe('planWaves', () => {
  it('returns empty plan for no units', () => {
    assert.deepEqual(planWaves([]), { waves: [], order: [] });
  });

  it('puts file-disjoint units in the same wave', () => {
    const plan = planWaves([
      { id: 'a', files: ['f1'] },
      { id: 'b', files: ['f2'] },
      { id: 'c', files: ['f3'] },
    ]);
    assert.deepEqual(plan.waves, [['a', 'b', 'c']]);
    assert.deepEqual(plan.order, ['a', 'b', 'c']);
  });

  it('splits file-conflicting units into separate waves, input order preserved', () => {
    const plan = planWaves([
      { id: 'a', files: ['shared'] },
      { id: 'b', files: ['shared'] },
      { id: 'c', files: ['shared'] },
    ]);
    assert.deepEqual(plan.waves, [['a'], ['b'], ['c']]);
  });

  it('packs a conflicting unit with the next disjoint one into the same wave', () => {
    const plan = planWaves([
      { id: 'a', files: ['x'] },
      { id: 'b', files: ['x'] }, // conflicts with a -> wave 2
      { id: 'c', files: ['y'] }, // disjoint from a -> wave 1
    ]);
    assert.deepEqual(plan.waves, [['a', 'c'], ['b']]);
  });

  it('respects deps: a dependent lands in a strictly later wave', () => {
    const plan = planWaves([
      { id: 'a', files: ['f1'] },
      { id: 'b', files: ['f2'], deps: ['a'] },
    ]);
    assert.deepEqual(plan.waves, [['a'], ['b']]);
  });

  it('a dependent waits even when its files are disjoint from its dep', () => {
    const plan = planWaves([
      { id: 'root', files: ['r'] },
      { id: 'leaf1', files: ['l1'], deps: ['root'] },
      { id: 'leaf2', files: ['l2'], deps: ['root'] },
    ]);
    assert.deepEqual(plan.waves, [['root'], ['leaf1', 'leaf2']]);
  });

  it('units with no files never conflict', () => {
    const plan = planWaves([
      { id: 'a', files: [] },
      { id: 'b', files: [] },
    ]);
    assert.deepEqual(plan.waves, [['a', 'b']]);
  });

  it('throws on a duplicate id', () => {
    assert.throws(
      () => planWaves([{ id: 'a', files: [] }, { id: 'a', files: [] }]),
      /duplicate unit id 'a'/,
    );
  });

  it('throws on an empty id', () => {
    assert.throws(
      () => planWaves([{ id: '', files: [] }]),
      /non-empty string id/,
    );
  });

  it('throws on a dependency on an unknown unit', () => {
    assert.throws(
      () => planWaves([{ id: 'a', files: [], deps: ['ghost'] }]),
      /depends on unknown unit 'ghost'/,
    );
  });

  it('throws on a self-dependency', () => {
    assert.throws(
      () => planWaves([{ id: 'a', files: [], deps: ['a'] }]),
      /cannot depend on itself/,
    );
  });

  it('throws on a dependency cycle', () => {
    assert.throws(
      () => planWaves([
        { id: 'a', files: [], deps: ['b'] },
        { id: 'b', files: [], deps: ['a'] },
      ]),
      /dependency cycle/,
    );
  });
});

describe('computeUnitSignature / computeRunId', () => {
  const base: WorkUnit = { id: 'a', files: ['z', 'a'], deps: ['y', 'b'], payload: { p: 1 } };

  it('is stable regardless of file/dep order', () => {
    const reordered: WorkUnit = { id: 'a', files: ['a', 'z'], deps: ['b', 'y'], payload: { p: 1 } };
    assert.equal(computeUnitSignature(base), computeUnitSignature(reordered));
  });

  it('changes when the payload changes', () => {
    const edited: WorkUnit = { ...base, payload: { p: 2 } };
    assert.notEqual(computeUnitSignature(base), computeUnitSignature(edited));
  });

  it('treats missing deps/payload as empty/null', () => {
    const a = computeUnitSignature({ id: 'a', files: ['f'] });
    const b = computeUnitSignature({ id: 'a', files: ['f'], deps: [], payload: null });
    assert.equal(a, b);
  });

  it('run id depends only on the ordered id list', () => {
    const r1 = computeRunId([{ id: 'a' }, { id: 'b' }]);
    const r2 = computeRunId([{ id: 'b' }, { id: 'a' }]);
    assert.notEqual(r1, r2);
    // Editing content (not ids) keeps the run id stable.
    assert.equal(
      computeRunId([{ id: 'a', files: ['x'] } as WorkUnit, { id: 'b', files: ['y'] } as WorkUnit]),
      computeRunId([{ id: 'a', files: ['CHANGED'] } as WorkUnit, { id: 'b', files: ['y2'] } as WorkUnit]),
    );
  });

  it('run id rejects duplicate ids', () => {
    assert.throws(() => computeRunId([{ id: 'a' }, { id: 'a' }]), /duplicate unit id/);
  });
});

describe('computeResumePlan', () => {
  const units = [
    { id: 'a', signature: 'sa' },
    { id: 'b', signature: 'sb' },
    { id: 'c', signature: 'sc' },
  ];
  const runId = computeRunId(units);

  it('re-runs everything when there is no prior run', () => {
    const plan = computeResumePlan(null, units);
    assert.deepEqual(plan.replay, []);
    assert.deepEqual(plan.rerun, ['a', 'b', 'c']);
    assert.equal(plan.cutIndex, 0);
    assert.equal(plan.runId, runId);
  });

  it('re-runs everything when the prior run id does not match', () => {
    const prior: PriorRun = { runId: 'different', completed: { a: 'sa', b: 'sb', c: 'sc' } };
    const plan = computeResumePlan(prior, units);
    assert.deepEqual(plan.rerun, ['a', 'b', 'c']);
    assert.deepEqual(plan.replay, []);
  });

  it('replays the entire unchanged run', () => {
    const prior: PriorRun = { runId, completed: { a: 'sa', b: 'sb', c: 'sc' } };
    const plan = computeResumePlan(prior, units);
    assert.deepEqual(plan.replay, ['a', 'b', 'c']);
    assert.deepEqual(plan.rerun, []);
    assert.equal(plan.cutIndex, 3);
  });

  it('replays the longest unchanged prefix and re-runs from the first change', () => {
    // b changed its signature since last run.
    const prior: PriorRun = { runId, completed: { a: 'sa', b: 'OLD', c: 'sc' } };
    const plan = computeResumePlan(prior, units);
    assert.deepEqual(plan.replay, ['a']);
    assert.deepEqual(plan.rerun, ['b', 'c']);
    assert.equal(plan.cutIndex, 1);
  });

  it('re-runs from a never-completed middle unit (missing from the manifest)', () => {
    const prior: PriorRun = { runId, completed: { a: 'sa', c: 'sc' } };
    const plan = computeResumePlan(prior, units);
    assert.deepEqual(plan.replay, ['a']);
    assert.deepEqual(plan.rerun, ['b', 'c']);
  });
});

/** Minimal in-memory resume cache for the wave() e2e. */
function memCache(): WorkflowAgentCache & { store: Map<string, unknown> } {
  const store = new Map<string, unknown>();
  return {
    store,
    get: (k) => store.get(k),
    set: (k, v) => { store.set(k, v); },
  };
}

describe('wave() workflow capability (e2e through executeWorkflow)', () => {
  const script = `
export const meta = { name: 'wave-e2e', description: 'fan out file-disjoint units' }
const units = [
  { id: 'a', files: ['f1'] },
  { id: 'b', files: ['f2'] },
  { id: 'c', files: ['f1'], deps: ['a'] },
]
const res = await wave(units, async (u) => { log('ran:' + u.id); return 'out:' + u.id })
return res
`;

  it('fans out disjoint lanes and integrates dependent/conflicting lanes serially', async () => {
    const result = await executeWorkflow(script, async () => 'unused');
    assert.deepEqual(result.result, ['out:a', 'out:b', 'out:c']);
    const ran = result.logs.filter((l) => l.startsWith('ran:'));
    // c shares f1 with a AND depends on a, so it must run after both a and b.
    assert.equal(ran[ran.length - 1], 'ran:c');
    assert.deepEqual([...ran].sort(), ['ran:a', 'ran:b', 'ran:c']);
  });

  it('replays an unchanged run from the resume cache (no re-execution)', async () => {
    const cache = memCache();
    const first = await executeWorkflow(script, async () => 'unused', { cache });
    assert.deepEqual(first.result, ['out:a', 'out:b', 'out:c']);
    assert.equal(first.logs.filter((l) => l.startsWith('ran:')).length, 3);
    // The wave manifest is now cached under a stable run id.
    assert.ok([...cache.store.keys()].some((k) => k.startsWith('wave:')));

    // Second run with the SAME cache: every unit replays from cache, so the
    // `run` callback never fires again, yet the results are identical.
    const second = await executeWorkflow(script, async () => 'unused', { cache });
    assert.deepEqual(second.result, ['out:a', 'out:b', 'out:c']);
    assert.equal(second.logs.filter((l) => l.startsWith('ran:')).length, 0);
  });

  it('re-runs only the changed unit and its successors on the second run', async () => {
    const cache = memCache();
    const v1 = `
export const meta = { name: 'wave-change', description: 'x' }
const units = [
  { id: 'a', files: ['f1'], payload: 'v1' },
  { id: 'b', files: ['f2'], payload: 'v1' },
]
return await wave(units, async (u) => { log('ran:' + u.id); return u.id })
`;
    const v2 = v1.replace("{ id: 'b', files: ['f2'], payload: 'v1' }", "{ id: 'b', files: ['f2'], payload: 'v2' }");

    await executeWorkflow(v1, async () => 'x', { cache });
    const second = await executeWorkflow(v2, async () => 'x', { cache });
    // a is unchanged -> replayed; b's payload changed -> re-run.
    assert.deepEqual(second.logs.filter((l) => l.startsWith('ran:')), ['ran:b']);
  });

  it('propagates a lane failure and persists the successful lanes for resume', async () => {
    const cache = memCache();
    const failing = `
export const meta = { name: 'wave-fail', description: 'x' }
const units = [
  { id: 'ok', files: ['f1'] },
  { id: 'boom', files: ['f2'] },
]
return await wave(units, async (u) => { log('ran:' + u.id); if (u.id === 'boom') throw new Error('nope'); return u.id })
`;
    await assert.rejects(
      () => executeWorkflow(failing, async () => 'x', { cache }),
      /wave\(\) failed/,
    );
    // The successful lane was persisted, so a resume would not re-run it.
    const manifestKey = [...cache.store.keys()].find((k) => k.startsWith('wave:'))!;
    const manifest = cache.store.get(manifestKey) as { completed: Record<string, string> };
    assert.ok('ok' in manifest.completed);
    assert.ok(!('boom' in manifest.completed));
  });
});
