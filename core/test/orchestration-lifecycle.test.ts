import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  LifecycleMachine,
  applyTransition,
  isTerminalState,
  canTransition,
  LifecycleNotFoundError,
  LifecycleConflictError,
} from '../src/orchestration/lifecycle';
import { LEGAL_TRANSITIONS, type AgentLifecycleState } from '../src/types';

describe('LEGAL_TRANSITIONS declarative graph', () => {
  it('terminal states have no outgoing edges', () => {
    assert.deepEqual(LEGAL_TRANSITIONS.done, []);
    assert.deepEqual(LEGAL_TRANSITIONS.failed, []);
    assert.deepEqual(LEGAL_TRANSITIONS.circuit_broken, []);
  });

  it('live states can reach every terminal state', () => {
    for (const terminal of ['done', 'failed', 'circuit_broken'] as const) {
      assert.ok(LEGAL_TRANSITIONS.starting.includes(terminal));
      assert.ok(LEGAL_TRANSITIONS.running.includes(terminal));
    }
  });

  it('non-terminal states have an idempotent self-edge', () => {
    assert.ok(LEGAL_TRANSITIONS.starting.includes('starting'));
    assert.ok(LEGAL_TRANSITIONS.running.includes('running'));
  });
});

describe('isTerminalState / canTransition', () => {
  it('classifies terminal vs live', () => {
    assert.equal(isTerminalState('done'), true);
    assert.equal(isTerminalState('failed'), true);
    assert.equal(isTerminalState('circuit_broken'), true);
    assert.equal(isTerminalState('starting'), false);
    assert.equal(isTerminalState('running'), false);
  });

  it('answers legal-successor queries', () => {
    assert.equal(canTransition('starting', 'running'), true);
    assert.equal(canTransition('running', 'done'), true);
    assert.equal(canTransition('done', 'running'), false);
    assert.equal(canTransition('running', 'starting'), false);
  });
});

describe('applyTransition (pure guarded CAS)', () => {
  it('returns the new state on a legal transition inside the from-set', () => {
    assert.equal(applyTransition('x', 'running', ['running'], 'done'), 'done');
    assert.equal(applyTransition('x', 'starting', 'starting', 'running'), 'running');
  });

  it('rejects when current is not in the from-set (lost CAS)', () => {
    assert.throws(
      () => applyTransition('x', 'done', ['running'], 'done'),
      (err: unknown) => {
        assert.ok(err instanceof LifecycleConflictError);
        assert.equal(err.code, 'lifecycle_conflict');
        assert.equal(err.state, 'done');
        assert.match(err.message, /is done; expected running/);
        return true;
      },
    );
  });

  it('rejects an illegal successor even when the from-set matches', () => {
    assert.throws(
      () => applyTransition('x', 'done', ['done'], 'running'),
      (err: unknown) => {
        assert.ok(err instanceof LifecycleConflictError);
        assert.match(err.message, /cannot transition from done to running/);
        return true;
      },
    );
  });
});

describe('LifecycleMachine', () => {
  it('registers at starting by default and reads back', () => {
    const m = new LifecycleMachine();
    assert.equal(m.register('a'), 'starting');
    assert.equal(m.get('a'), 'starting');
    assert.equal(m.has('a'), true);
    assert.equal(m.has('b'), false);
    assert.equal(m.size, 1);
  });

  it('a duplicate register never resets a live/terminal entity', () => {
    const m = new LifecycleMachine();
    m.register('a');
    m.transition({ id: 'a', from: 'starting', to: 'running' });
    assert.equal(m.register('a'), 'running'); // not reset to starting
    assert.equal(m.get('a'), 'running');
  });

  it('drives a full legal lifecycle', () => {
    const m = new LifecycleMachine();
    m.register('a');
    assert.deepEqual(m.transition({ id: 'a', from: 'starting', to: 'running' }), { changed: true });
    assert.deepEqual(m.transition({ id: 'a', from: 'running', to: 'done' }), { changed: true });
    assert.equal(m.get('a'), 'done');
    assert.equal(m.isSettled('a'), true);
  });

  it('throws LifecycleNotFoundError for an unknown id', () => {
    const m = new LifecycleMachine();
    assert.throws(
      () => m.transition({ id: 'ghost', from: 'starting', to: 'running' }),
      (err: unknown) => err instanceof LifecycleNotFoundError && err.code === 'lifecycle_not_found',
    );
  });

  it('two settlers: first wins, second is a lifecycle_conflict', () => {
    const m = new LifecycleMachine();
    m.register('sub');
    m.transition({ id: 'sub', from: ['starting', 'running'], to: 'running' });
    // writer A settles
    m.transition({ id: 'sub', from: ['running'], to: 'done' });
    // writer B, still believing it was running, is rejected (optimistic concurrency)
    assert.throws(
      () => m.transition({ id: 'sub', from: ['running'], to: 'failed' }),
      (err: unknown) => err instanceof LifecycleConflictError && (err as LifecycleConflictError).state === 'done',
    );
    assert.equal(m.get('sub'), 'done');
  });

  it('isSettled is false for a live entity and lists all entities', () => {
    const m = new LifecycleMachine();
    m.register('a');
    m.register('b', 'running' as AgentLifecycleState);
    assert.equal(m.isSettled('a'), false);
    assert.equal(m.isSettled('missing'), false);
    const listed = m.list().sort((x, y) => x.id.localeCompare(y.id));
    assert.deepEqual(listed, [
      { id: 'a', state: 'starting' },
      { id: 'b', state: 'running' },
    ]);
  });
});
