import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  PermissionManager,
  type AutoModeSafetyClassifier,
  type AutoModeSafetyContext,
  type AutoModeSafetyVerdict,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w99 — injectable auto-mode safety classifier seam (parity with Claude
// Code's transcript / yolo classifier: classifierApprovals.ts + useCanUseTool).
//
// The deterministic permission engine is the floor. On the AUTONOMOUS auto-approve
// path (`auto-allow` / `bypassPermissions` / inherited plan-bypass / screened
// `auto`) an injected classifier gets a chance to TIGHTEN a would-be auto-approval
// to `ask` / `deny` with a recorded reason — it can never loosen it, and any
// undefined / allow / throw FAILS OPEN to the deterministic auto-approval.
//
// RED/GREEN discriminator: WITHOUT setAutoModeSafetyClassifier +
// applyAutoModeSafetyClassifier, auto-allow mode always returns `allow` for a safe
// bash command and the `AutoModeSafetyClassifier`/`setAutoModeSafetyClassifier`
// symbols do not resolve (whole-file red). WITH them, the injected classifier can
// downgrade the verdict.
// ---------------------------------------------------------------------------

/** A spying classifier that records each invocation and returns a fixed verdict. */
function spyClassifier(
  verdict: AutoModeSafetyVerdict | undefined | (() => never),
): AutoModeSafetyClassifier & { calls: AutoModeSafetyContext[] } {
  const calls: AutoModeSafetyContext[] = [];
  const fn = ((ctx: AutoModeSafetyContext) => {
    calls.push(ctx);
    if (typeof verdict === 'function') verdict();
    return verdict as AutoModeSafetyVerdict | undefined;
  }) as AutoModeSafetyClassifier & { calls: AutoModeSafetyContext[] };
  fn.calls = calls;
  return fn;
}

const LS = { command: 'ls -la' };

describe('AutoModeSafetyClassifier seam — baseline (unset)', () => {
  it('auto-allow mode auto-approves a safe bash command when no classifier is installed', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(pm.hasAutoModeSafetyClassifier(), false);
    assert.equal(await pm.check('bash', LS), 'allow');
  });
});

describe('AutoModeSafetyClassifier seam — downgrade in auto-allow', () => {
  it('downgrades an auto-approval to ask with a classifier reason', async () => {
    const pm = new PermissionManager('auto-allow');
    const classifier = spyClassifier({ decision: 'ask', reason: 'needs a closer look' });
    pm.setAutoModeSafetyClassifier(classifier);
    assert.equal(pm.hasAutoModeSafetyClassifier(), true);

    const decision = await pm.check('bash', LS);
    assert.equal(decision, 'ask');
    assert.equal(classifier.calls.length, 1);
    assert.equal(classifier.calls[0].toolName, 'bash');
    assert.equal(classifier.calls[0].mode, 'auto-allow');
    assert.equal(classifier.calls[0].staticDecision, 'allow');
  });

  it('surfaces the classifier reason through checkWithReason', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'ask', reason: 'policy X' }));
    const res = await pm.checkWithReason('bash', LS);
    assert.equal(res.decision, 'ask');
    assert.equal(res.reason?.type, 'classifier');
    assert.equal((res.reason as { reason?: string }).reason, 'policy X');
    assert.match(res.message ?? '', /policy X/);
  });

  it('downgrades to deny and records the denial with source "classifier"', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'deny', reason: 'blocked by model' }));
    assert.equal(await pm.check('bash', LS), 'deny');
    const denials = pm.getRecentDenials();
    assert.equal(denials.length, 1);
    assert.equal(denials[0].source, 'classifier');
    assert.equal(denials[0].reason, 'blocked by model');
    assert.equal(denials[0].display, 'ls -la');
  });

  it('an ask downgrade does NOT record a denial (a prompt is not a denial of intent)', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'ask', reason: 'r' }));
    assert.equal(await pm.check('bash', LS), 'ask');
    assert.equal(pm.getRecentDenials().length, 0);
  });
});

describe('AutoModeSafetyClassifier seam — fail-open semantics', () => {
  it('an undefined verdict leaves the auto-approval intact', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier(undefined));
    assert.equal(await pm.check('bash', LS), 'allow');
  });

  it('an explicit allow verdict leaves the auto-approval intact', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'allow' }));
    assert.equal(await pm.check('bash', LS), 'allow');
  });

  it('a thrown classifier error fails OPEN to the deterministic auto-approval', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier(() => { throw new Error('model timeout'); }));
    assert.equal(await pm.check('bash', LS), 'allow');
  });

  it('an async-rejected classifier also fails OPEN', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(async () => { throw new Error('async boom'); });
    assert.equal(await pm.check('bash', LS), 'allow');
  });
});

describe('AutoModeSafetyClassifier seam — never consulted for safe / non-autonomous paths', () => {
  it('skips the classifier for an auto-mode-safe (coordination) tool', async () => {
    const pm = new PermissionManager('auto-allow');
    const classifier = spyClassifier({ decision: 'deny', reason: 'should not matter' });
    pm.setAutoModeSafetyClassifier(classifier);
    // send_message is in AUTO_MODE_SAFE_TOOLS → the classifier is bypassed, stays allow.
    assert.equal(await pm.check('send_message', { to: 'peer', message: 'hi' }), 'allow');
    assert.equal(classifier.calls.length, 0);
  });

  it('is NOT consulted in ask mode (deterministic prompting path only)', async () => {
    const pm = new PermissionManager('ask');
    const classifier = spyClassifier({ decision: 'deny', reason: 'nope' });
    pm.setAutoModeSafetyClassifier(classifier);
    await pm.check('bash', LS);
    assert.equal(classifier.calls.length, 0);
  });

  it('is NOT consulted for an explicit deny rule under auto-allow (deny wins first)', async () => {
    const pm = new PermissionManager('auto-allow', [{ tool: 'bash', pattern: 'ls *', action: 'deny' }]);
    const classifier = spyClassifier({ decision: 'allow' });
    pm.setAutoModeSafetyClassifier(classifier);
    assert.equal(await pm.check('bash', LS), 'deny');
    assert.equal(classifier.calls.length, 0);
  });
});

describe('AutoModeSafetyClassifier seam — bypassPermissions + auto modes', () => {
  it('downgrades an auto-approval in bypassPermissions mode', async () => {
    const pm = new PermissionManager('bypassPermissions');
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'ask', reason: 'bypass gated' }));
    assert.equal(await pm.check('bash', LS), 'ask');
  });

  it('is consulted in auto mode only after the static screen allows', async () => {
    const pm = new PermissionManager('auto');
    const classifier = spyClassifier({ decision: 'deny', reason: 'screen-then-model' });
    pm.setAutoModeSafetyClassifier(classifier);
    // `ls -la` passes the static auto-mode screen → classifier runs and denies.
    assert.equal(await pm.check('bash', LS), 'deny');
    assert.equal(classifier.calls.length, 1);
  });
});

describe('AutoModeSafetyClassifier seam — lifecycle', () => {
  it('clearing the classifier restores the deterministic behavior', async () => {
    const pm = new PermissionManager('auto-allow');
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'deny', reason: 'x' }));
    assert.equal(await pm.check('bash', LS), 'deny');
    pm.setAutoModeSafetyClassifier(undefined);
    assert.equal(pm.hasAutoModeSafetyClassifier(), false);
    assert.equal(await pm.check('bash', LS), 'allow');
  });

  it('does not leak a classifier reason into a later non-classifier verdict', async () => {
    const pm = new PermissionManager('auto-allow', [{ tool: 'bash', pattern: 'rm *', action: 'deny' }]);
    pm.setAutoModeSafetyClassifier(spyClassifier({ decision: 'ask', reason: 'first call' }));
    // First call: classifier downgrades → classifier reason.
    const first = await pm.checkWithReason('bash', LS);
    assert.equal(first.reason?.type, 'classifier');
    // Second call hits an explicit deny rule (classifier skipped) → rule reason, not classifier.
    const second = await pm.checkWithReason('bash', { command: 'rm -rf /tmp/x' });
    assert.equal(second.decision, 'deny');
    assert.notEqual(second.reason?.type, 'classifier');
  });
});
