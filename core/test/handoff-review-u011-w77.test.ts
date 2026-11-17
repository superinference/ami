import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  reviewSubagentHandoff,
  builtinHandoffClassifier,
  isAutonomousHandoffMode,
  HANDOFF_SECURITY_WARNING_PREFIX,
  HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE,
  type HandoffAction,
  type HandoffClassifier,
} from '../src/tools/handoff-review';

describe('isAutonomousHandoffMode', () => {
  it('is true for the yolo/auto modes that auto-approve tool calls', () => {
    assert.equal(isAutonomousHandoffMode('auto-allow'), true);
    assert.equal(isAutonomousHandoffMode('bypassPermissions'), true);
  });
  it('is false for interactive / gated / undefined modes', () => {
    for (const m of ['ask', 'plan', 'deny-all', 'acceptEdits', undefined, '']) {
      assert.equal(isAutonomousHandoffMode(m as string | undefined), false, `mode=${m}`);
    }
  });
});

describe('builtinHandoffClassifier — reuses core dangerous-command detectors', () => {
  const cf = builtinHandoffClassifier('/tmp/work');

  it('flags a catastrophic removal', async () => {
    const r = await cf([{ tool: 'bash', command: 'rm -rf /' }]);
    assert.equal(r.flagged, true);
    assert.match(r.reason!, /removal/);
  });
  it('flags a nuclear command (fork bomb)', async () => {
    const r = await cf([{ tool: 'bash', command: ':(){ :|:& };:' }]);
    assert.equal(r.flagged, true);
  });
  it('flags a detached git-discard (reset --hard)', async () => {
    const r = await cf([{ tool: 'bash', command: 'git reset --hard HEAD~3' }]);
    assert.equal(r.flagged, true);
    assert.match(r.reason!, /discard/);
  });
  it('does NOT flag a read-only command', async () => {
    const r = await cf([{ tool: 'bash', command: 'ls -la && git status' }]);
    assert.equal(r.flagged, false);
  });
  it('ignores non-bash tools and bash calls without a command', async () => {
    const actions: HandoffAction[] = [
      { tool: 'file_read' },
      { tool: 'grep', command: 'rm -rf /' }, // command on a non-bash tool is ignored
      { tool: 'bash' },
    ];
    const r = await cf(actions);
    assert.equal(r.flagged, false);
  });
});

describe('reviewSubagentHandoff', () => {
  const OUT = 'the sub-agent said done';

  it('is a NO-OP in non-autonomous modes (even with a dangerous action)', async () => {
    for (const mode of ['ask', 'plan', 'acceptEdits', undefined]) {
      const out = await reviewSubagentHandoff({
        output: OUT, permissionMode: mode as string | undefined,
        actions: [{ tool: 'bash', command: 'rm -rf /' }], cwd: '/tmp',
      });
      assert.equal(out, OUT, `mode=${mode} should pass through unchanged`);
    }
  });

  it('prepends a SECURITY WARNING when the built-in heuristic flags a dangerous action (yolo)', async () => {
    const out = await reviewSubagentHandoff({
      output: OUT, permissionMode: 'auto-allow',
      actions: [{ tool: 'bash', command: 'rm -rf /' }], cwd: '/tmp',
    });
    assert.ok(out.startsWith(HANDOFF_SECURITY_WARNING_PREFIX), out);
    assert.match(out, /Reason:/);
    assert.ok(out.endsWith(OUT));
  });

  it('passes a clean autonomous run through unchanged', async () => {
    const out = await reviewSubagentHandoff({
      output: OUT, permissionMode: 'bypassPermissions',
      actions: [{ tool: 'bash', command: 'ls -la' }], cwd: '/tmp',
    });
    assert.equal(out, OUT);
  });

  it('honors an injected classifier verdict over the built-in heuristic', async () => {
    const classifier: HandoffClassifier = () => ({ flagged: true, reason: 'wrote to /etc/passwd' });
    const out = await reviewSubagentHandoff({
      output: OUT, permissionMode: 'auto-allow',
      actions: [{ tool: 'file_edit' }], cwd: '/tmp', classifier,
    });
    assert.ok(out.startsWith(HANDOFF_SECURITY_WARNING_PREFIX));
    assert.match(out, /wrote to \/etc\/passwd/);
  });

  it('falls back to the unavailable note when the classifier reports unavailable', async () => {
    const classifier: HandoffClassifier = () => ({ flagged: false, unavailable: true });
    const out = await reviewSubagentHandoff({
      output: OUT, permissionMode: 'auto-allow',
      actions: [{ tool: 'bash', command: 'rm -rf /' }], cwd: '/tmp', classifier,
    });
    assert.ok(out.startsWith(HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE), out);
    assert.ok(out.endsWith(OUT), 'output is still propagated, never dropped');
  });

  it('falls back to the unavailable note when the classifier THROWS (never drops output)', async () => {
    const classifier: HandoffClassifier = () => { throw new Error('reviewer offline'); };
    const out = await reviewSubagentHandoff({
      output: OUT, permissionMode: 'auto-allow',
      actions: [{ tool: 'bash', command: 'echo hi' }], cwd: '/tmp', classifier,
    });
    assert.ok(out.startsWith(HANDOFF_CLASSIFIER_UNAVAILABLE_NOTE));
    assert.ok(out.endsWith(OUT));
  });

  it('flagged verdict with no reason omits the Reason clause', async () => {
    const classifier: HandoffClassifier = () => ({ flagged: true });
    const out = await reviewSubagentHandoff({
      output: OUT, permissionMode: 'auto-allow', actions: [], cwd: '/tmp', classifier,
    });
    assert.ok(out.startsWith(HANDOFF_SECURITY_WARNING_PREFIX));
    assert.ok(!/Reason:/.test(out.split('\n')[0]));
  });
});
