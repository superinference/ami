import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  LineageGraph,
  canSpawnMore,
  DEFAULT_MAX_SPAWN_DEPTH,
  LineageUnknownParentError,
  LineageCycleError,
  SpawnDepthExceededError,
} from '../src/orchestration/depth';

describe('canSpawnMore (depth fence predicate)', () => {
  it('allows spawning while depth is below the cap', () => {
    assert.equal(canSpawnMore(0, 3), true);
    assert.equal(canSpawnMore(2, 3), true);
  });

  it('refuses spawning once depth reaches/exceeds the cap', () => {
    assert.equal(canSpawnMore(3, 3), false);
    assert.equal(canSpawnMore(4, 3), false);
  });

  it('a zero cap forbids any spawn', () => {
    assert.equal(canSpawnMore(0, 0), false);
  });

  it('exposes a sane default cap', () => {
    assert.equal(DEFAULT_MAX_SPAWN_DEPTH, 3);
  });
});

describe('LineageGraph depth derivation', () => {
  it('a system-created root is depth 0', () => {
    const g = new LineageGraph();
    const root = g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    assert.equal(root.depth, 0);
    assert.equal(root.parentId, null);
    assert.equal(g.depthOf('root'), 0);
  });

  it('derives a child depth from its creating agent (parent.depth + 1)', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    const child = g.registerSpawn({ id: 'c1', creator: { kind: 'agent', id: 'root' } });
    const grand = g.registerSpawn({ id: 'c2', creator: { kind: 'agent', id: 'c1' } });
    assert.equal(child.depth, 1);
    assert.equal(grand.depth, 2);
    assert.equal(child.parentId, 'root');
  });

  it('closes the run-create bypass: a fresh run does not reset creator depth', () => {
    // A worker at depth 1 that starts its own run+task still spawns children at depth 2.
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'worker', creator: { kind: 'agent', id: 'root' }, runId: 'run_a' });
    const sub = g.registerSpawn({
      id: 'sub',
      creator: { kind: 'agent', id: 'worker' },
      runId: 'run_worker_owned',
    });
    assert.equal(sub.depth, 2);
  });

  it('records runId/taskId origin associations and dual keys', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    const node = g.registerSpawn({
      id: 'w',
      instanceId: 'w#inc2',
      creator: { kind: 'agent', id: 'root' },
      runId: 'run_1',
      taskId: 'task_1',
    });
    assert.equal(node.runId, 'run_1');
    assert.equal(node.taskId, 'task_1');
    assert.equal(g.get('w')?.instanceId, 'w#inc2');
    assert.equal(g.getByInstanceId('w#inc2')?.id, 'w');
  });
});

describe('LineageGraph depth cap enforcement', () => {
  it('refuses a spawn that would exceed maxDepth with Orca-style wording', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'worker', creator: { kind: 'agent', id: 'root' } }); // depth 1
    assert.throws(
      () => g.registerSpawn({ id: 'sub', creator: { kind: 'agent', id: 'worker' }, maxDepth: 1 }),
      (err: unknown) => {
        assert.ok(err instanceof SpawnDepthExceededError);
        assert.equal(err.code, 'spawn_depth_exceeded');
        assert.match(err.message, /at most 1 level deep, so this worker can't start another \(depth 2\)/);
        return true;
      },
    );
  });

  it('allows a spawn exactly at the cap boundary', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    // depth 1 with maxDepth 1 is permitted (depth <= maxDepth); depth 2 is not.
    const ok = g.registerSpawn({ id: 'w', creator: { kind: 'agent', id: 'root' }, maxDepth: 1 });
    assert.equal(ok.depth, 1);
  });
});

describe('LineageGraph integrity: unknown parent + cycles', () => {
  it('rejects registration under an unknown parent', () => {
    const g = new LineageGraph();
    assert.throws(
      () => g.registerSpawn({ id: 'c', creator: { kind: 'agent', id: 'ghost' } }),
      (err: unknown) => err instanceof LineageUnknownParentError && err.code === 'lineage_unknown_parent',
    );
  });

  it('rejects a self-parent edge', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'a', creator: { kind: 'system' } });
    assert.throws(
      () => g.registerSpawn({ id: 'a', creator: { kind: 'agent', id: 'a' } }),
      (err: unknown) => err instanceof LineageCycleError,
    );
  });

  it('rejects an edge that would form a cycle', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'a', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'b', creator: { kind: 'agent', id: 'a' } });
    g.registerSpawn({ id: 'c', creator: { kind: 'agent', id: 'b' } });
    // re-registering 'a' under 'c' (its own descendant) is a cycle
    assert.throws(
      () => g.registerSpawn({ id: 'a', creator: { kind: 'agent', id: 'c' } }),
      (err: unknown) => err instanceof LineageCycleError && err.code === 'lineage_cycle',
    );
  });
});

describe('LineageGraph.canSpawnMore', () => {
  it('reflects the recorded depth', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'w', creator: { kind: 'agent', id: 'root' } }); // depth 1
    assert.equal(g.canSpawnMore('root', 2), true);
    assert.equal(g.canSpawnMore('w', 2), true);
    assert.equal(g.canSpawnMore('w', 1), false);
  });

  it('treats an unknown id as a root (depth 0) so a top-level agent can delegate', () => {
    const g = new LineageGraph();
    assert.equal(g.canSpawnMore('never-registered', 1), true);
  });
});

describe('LineageGraph lineage-authority gating', () => {
  it('accepts a settle whose sender matches the recorded assignee id', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'child', creator: { kind: 'agent', id: 'root' } });
    assert.equal(g.isAuthorizedToSettle('child', 'child'), true);
  });

  it('accepts a settle whose sender matches the incarnation id', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'child', instanceId: 'child#inc7', creator: { kind: 'agent', id: 'root' } });
    assert.equal(g.isAuthorizedToSettle('child', 'child#inc7'), true);
  });

  it('rejects a spoofed settle from a sibling or unknown sender', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'a', creator: { kind: 'agent', id: 'root' } });
    g.registerSpawn({ id: 'b', creator: { kind: 'agent', id: 'root' } });
    assert.equal(g.isAuthorizedToSettle('a', 'b'), false); // sibling can't settle a
    assert.equal(g.isAuthorizedToSettle('a', 'ghost'), false);
    assert.equal(g.isAuthorizedToSettle('unknown-child', 'anyone'), false);
  });
});

describe('LineageGraph fold-tree surface', () => {
  it('exposes children, ancestors, size, and a flat list', () => {
    const g = new LineageGraph();
    g.registerSpawn({ id: 'root', creator: { kind: 'system' } });
    g.registerSpawn({ id: 'a', creator: { kind: 'agent', id: 'root' } });
    g.registerSpawn({ id: 'b', creator: { kind: 'agent', id: 'root' } });
    g.registerSpawn({ id: 'a1', creator: { kind: 'agent', id: 'a' } });
    assert.equal(g.size, 4);
    assert.deepEqual(g.children('root').map((n) => n.id).sort(), ['a', 'b']);
    assert.deepEqual(g.ancestors('a1').map((n) => n.id), ['a', 'root']);
    assert.equal(g.list().length, 4);
    assert.deepEqual(g.children('a1'), []);
    assert.deepEqual(g.ancestors('root'), []);
  });
});
