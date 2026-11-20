import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  createSubagentRelay,
  buildSubagentIdleEvent,
  createAgentInvocationLedger,
  resolveEnableSummarization,
} from '../src/tools/subagent-relay';

/* ---------------------------------------------------------------- *
 * U237 / w88 — GAP 1: idle-teammate lifecycle state
 *
 * Parity: the reference's TeamCreateTool prompt treats a teammate going idle
 * after a turn as a first-class, NON-error waiting state — an idle teammate can
 * still receive messages and must not be treated as an error or as done/gone.
 * The relay models this with a non-terminal 'idle' state distinct from the
 * terminal 'completed'/'failed' states.
 * ---------------------------------------------------------------- */
describe('createSubagentRelay — idle-teammate lifecycle (U237)', () => {
  it('markIdle transitions a pending node to the non-terminal idle state', () => {
    const r = createSubagentRelay();
    r.register('a');
    const res = r.markIdle('a');
    assert.equal(res.transitioned, true);
    assert.equal(res.state, 'idle');
    assert.equal(r.state('a'), 'idle');
  });

  it('an idle teammate is still alive (idle is not an error / not done)', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.markIdle('a');
    assert.equal(r.isAlive('a'), true);
  });

  it('an idle teammate can still receive messages (parity: idle != unavailable)', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.markIdle('a');
    const send = r.send('a', 'wake up, more work');
    assert.equal(send.accepted, true);
    assert.equal(r.pending('a'), 1);
    assert.equal(r.deliver('a')[0].body, 'wake up, more work');
    assert.equal(r.deadLetters().length, 0);
  });

  it('markIdle is idempotent — re-marking an idle node is a no-op transition', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.markIdle('a');
    const again = r.markIdle('a');
    assert.equal(again.transitioned, false);
    assert.equal(again.state, 'idle');
  });

  it('markIdle on an unknown node is a non-transition no-op', () => {
    const r = createSubagentRelay();
    const res = r.markIdle('ghost');
    assert.equal(res.transitioned, false);
    assert.equal(res.state, 'unknown');
    assert.equal(r.state('ghost'), 'unknown');
  });

  it('markIdle cannot resurrect a finished node (idle only from alive)', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.complete('a');
    const res = r.markIdle('a');
    assert.equal(res.transitioned, false);
    assert.equal(res.state, 'completed');
    assert.equal(r.state('a'), 'completed');
  });

  it('an idle node can still transition to a terminal state, flushing mail', () => {
    const r = createSubagentRelay();
    r.register('a');
    r.markIdle('a');
    r.send('a', 'unread');
    const done = r.complete('a', 'completed');
    assert.equal(done.transitioned, true);
    assert.equal(done.state, 'completed');
    assert.deepEqual(done.dropped.map((m) => m.body), ['unread']);
    assert.equal(r.state('a'), 'completed');
  });

  it('buildSubagentIdleEvent produces an idle progress notice (idle is not an error)', () => {
    const ev = buildSubagentIdleEvent('agent-7', 3);
    assert.equal(ev.type, 'subagent_progress');
    if (ev.type !== 'subagent_progress') return;
    assert.equal(ev.nodeId, 'agent-7');
    assert.equal(ev.depth, 3);
    assert.equal(ev.activity, 'text');
    assert.match(ev.detail, /idle/i);
    assert.match(ev.detail, /not an error/i);
  });

  it('buildSubagentIdleEvent carries a custom reason when supplied', () => {
    const ev = buildSubagentIdleEvent('a', 1, 'awaiting review');
    if (ev.type !== 'subagent_progress') return;
    assert.match(ev.detail, /awaiting review/);
    assert.match(ev.detail, /not an error/i);
  });
});

/* ---------------------------------------------------------------- *
 * U237 / w88 — GAP 2: spawn-vs-resume invocation accounting +
 * once-per-invocation sparse-edge emission.
 *
 * Parity: the reference's agentContext carries invokingRequestId +
 * invocationKind ('spawn'|'resume') and consumeInvokingRequestId() returns the
 * id EXACTLY ONCE per spawn/resume boundary (a sparse edge), undefined
 * thereafter — so a parent->child invocation graph can be reconstructed and a
 * fresh spawn distinguished from a relay/resume.
 * ---------------------------------------------------------------- */
describe('createAgentInvocationLedger — spawn/resume accounting (U237)', () => {
  it('records a spawn with its kind, parent edge and invoking request id', () => {
    const led = createAgentInvocationLedger();
    led.record('child', 'spawn', { parentId: 'root', invokingRequestId: 'req-1' });
    assert.equal(led.kind('child'), 'spawn');
    assert.equal(led.parentOf('child'), 'root');
  });

  it('consumeInvokingRequestId yields the id exactly once per invocation (sparse edge)', () => {
    const led = createAgentInvocationLedger();
    led.record('child', 'spawn', { parentId: 'root', invokingRequestId: 'req-1' });
    assert.equal(led.consumeInvokingRequestId('child'), 'req-1');
    // Already emitted → undefined thereafter (idempotent sparse edge).
    assert.equal(led.consumeInvokingRequestId('child'), undefined);
    assert.equal(led.consumeInvokingRequestId('child'), undefined);
  });

  it('a resume re-arms the sparse edge and flips the invocation kind', () => {
    const led = createAgentInvocationLedger();
    led.record('child', 'spawn', { parentId: 'root', invokingRequestId: 'req-1' });
    assert.equal(led.consumeInvokingRequestId('child'), 'req-1');
    led.record('child', 'resume', { invokingRequestId: 'req-2' });
    assert.equal(led.kind('child'), 'resume');
    assert.equal(led.consumeInvokingRequestId('child'), 'req-2');
    assert.equal(led.consumeInvokingRequestId('child'), undefined);
  });

  it('unknown nodes report undefined for kind/parent/consume', () => {
    const led = createAgentInvocationLedger();
    assert.equal(led.kind('nobody'), undefined);
    assert.equal(led.parentOf('nobody'), undefined);
    assert.equal(led.consumeInvokingRequestId('nobody'), undefined);
  });

  it('counts spawn vs resume invocations separately', () => {
    const led = createAgentInvocationLedger();
    led.record('a', 'spawn', { invokingRequestId: 'r1' });
    led.record('b', 'spawn', { invokingRequestId: 'r2' });
    led.record('a', 'resume', { invokingRequestId: 'r3' });
    assert.deepEqual(led.counts(), { spawn: 2, resume: 1 });
  });

  it('pendingEdges lists only unconsumed sparse edges, one per boundary', () => {
    const led = createAgentInvocationLedger();
    led.record('a', 'spawn', { parentId: 'root', invokingRequestId: 'r1' });
    led.record('b', 'spawn', { parentId: 'a', invokingRequestId: 'r2' });
    assert.deepEqual(
      led.pendingEdges().map((e) => e.nodeId).sort(),
      ['a', 'b'],
    );
    led.consumeInvokingRequestId('a');
    assert.deepEqual(led.pendingEdges().map((e) => e.nodeId), ['b']);
  });

  it('a record without an invoking request id still tracks kind/parent but emits no sparse edge', () => {
    const led = createAgentInvocationLedger();
    led.record('a', 'spawn', { parentId: 'root' });
    assert.equal(led.kind('a'), 'spawn');
    assert.equal(led.parentOf('a'), 'root');
    assert.equal(led.consumeInvokingRequestId('a'), undefined);
    assert.deepEqual(led.pendingEdges(), []);
  });
});

/* ---------------------------------------------------------------- *
 * U237 / w88 — GAP 3: enableSummarization knob for background/resumed agents.
 *
 * Parity: the reference gates progress-summary generation on
 * enableSummarization = isCoordinatorMode() || isForkSubagentEnabled() ||
 * getSdkAgentProgressSummariesEnabled(). AMI keeps summaries ON by default
 * (superset — never regress) but exposes an explicit disable knob, and the
 * three coordinator/fork/sdk mode flags force-enable regardless.
 * ---------------------------------------------------------------- */
describe('resolveEnableSummarization — progress-summary knob (U237)', () => {
  it('defaults to enabled (AMI superset — summaries on by default)', () => {
    assert.equal(resolveEnableSummarization({}), true);
  });

  it('is disabled by an explicit off/0/false/no', () => {
    assert.equal(resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: 'off' }), false);
    assert.equal(resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: '0' }), false);
    assert.equal(resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: 'false' }), false);
    assert.equal(resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: 'no' }), false);
  });

  it('coordinator / fork / sdk mode flags force-enable even over an explicit off', () => {
    assert.equal(
      resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: 'off', AMI_COORDINATOR_MODE: '1' }),
      true,
    );
    assert.equal(resolveEnableSummarization({ AMI_FORK_SUBAGENT: '1' }), true);
    assert.equal(resolveEnableSummarization({ AMI_SDK_PROGRESS_SUMMARIES: 'true' }), true);
  });

  it('an explicit on wins over nothing', () => {
    assert.equal(resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: 'on' }), true);
  });

  it('a disabled knob with mode flags off stays disabled', () => {
    assert.equal(
      resolveEnableSummarization({ AMI_SUBAGENT_SUMMARIES: 'false', AMI_FORK_SUBAGENT: '0' }),
      false,
    );
  });
});
