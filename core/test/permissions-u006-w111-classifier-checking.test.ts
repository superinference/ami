import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  PermissionManager,
  type AutoModeSafetyClassifier,
  type AutoModeSafetyVerdict,
} from '../src/permissions';
import { ToolConfirmationService } from '../src/tool-confirmation';

// ---------------------------------------------------------------------------
// U006 / w111 — in-flight safety-classifier CHECKING signal.
//
// AMI already tracks classifier APPROVALS (the settled verdict) via
// ToolConfirmationService; it was missing the pre-decision CHECKING half of
// Claude's classifierApprovals.ts (CLASSIFIER_CHECKING set +
// setClassifierChecking/clearClassifierChecking/isClassifierChecking +
// subscribeClassifierChecking). This adds a PermissionManager listener seam
// (setClassifierCheckingListener) fired around the ACTUAL async classifier
// consult, plus the ToolConfirmationService in-flight set.
//
// RED/GREEN discriminator: WITHOUT setClassifierCheckingListener +
// the firing in applyAutoModeSafetyClassifier, no (checking,toolName) transition
// is observed around an autonomous auto-approve classifier call; WITH them, a
// `true` fires before the async query and `false` after it settles — including
// when the classifier THROWS (finally).
// ---------------------------------------------------------------------------

/** A classifier that resolves asynchronously, letting the test observe that the
 *  `true` transition fires BEFORE the verdict and the `false` transition after. */
function asyncClassifier(
  verdict: AutoModeSafetyVerdict | undefined,
  onEnter?: () => void,
): AutoModeSafetyClassifier {
  return async () => {
    onEnter?.();
    await Promise.resolve();
    return verdict;
  };
}

const LS = { command: 'ls -la' };

describe('classifier-checking listener seam — PermissionManager', () => {
  it('brackets an actual classifier consult with true then false', async () => {
    const pm = new PermissionManager('auto-allow');
    const transitions: Array<[boolean, string]> = [];
    pm.setClassifierCheckingListener((checking, toolName) => transitions.push([checking, toolName]));

    let checkingAtEntry: boolean | undefined;
    pm.setAutoModeSafetyClassifier(
      asyncClassifier({ decision: 'allow' }, () => {
        // At the moment the async classifier body runs, the `true` transition must
        // already have been emitted (the signal is live DURING the query).
        checkingAtEntry = transitions.length === 1 && transitions[0][0] === true;
      }),
    );

    const decision = await pm.check('bash', LS);
    assert.equal(decision, 'allow');
    assert.equal(checkingAtEntry, true, 'checking:true must fire before the async query');
    assert.deepEqual(transitions, [[true, 'bash'], [false, 'bash']]);
  });

  it('still clears the checking signal when the classifier throws', async () => {
    const pm = new PermissionManager('auto-allow');
    const transitions: Array<[boolean, string]> = [];
    pm.setClassifierCheckingListener((checking, toolName) => transitions.push([checking, toolName]));
    pm.setAutoModeSafetyClassifier((() => { throw new Error('boom'); }) as AutoModeSafetyClassifier);

    // A classifier failure FAILS OPEN to allow, but the in-flight signal must still
    // be cleared (the `false` transition runs in `finally`).
    const decision = await pm.check('bash', LS);
    assert.equal(decision, 'allow');
    assert.deepEqual(transitions, [[true, 'bash'], [false, 'bash']]);
  });

  it('does NOT fire on the fast-path allow (no classifier installed)', async () => {
    const pm = new PermissionManager('auto-allow');
    const transitions: Array<[boolean, string]> = [];
    pm.setClassifierCheckingListener((checking, toolName) => transitions.push([checking, toolName]));
    const decision = await pm.check('bash', LS);
    assert.equal(decision, 'allow');
    assert.deepEqual(transitions, [], 'no classifier → no checking signal');
  });

  it('does NOT fire for an auto-mode-safe tool (classifier skipped)', async () => {
    const pm = new PermissionManager('auto-allow');
    const transitions: Array<[boolean, string]> = [];
    pm.setClassifierCheckingListener((checking, toolName) => transitions.push([checking, toolName]));
    pm.setAutoModeSafetyClassifier(asyncClassifier({ decision: 'ask', reason: 'x' }));
    // A read-only tool on the auto-mode-safe allowlist short-circuits before the
    // classifier is consulted, so no checking signal is emitted regardless of the
    // resolved decision.
    // `task` is a coordination tool on the auto-mode-safe allowlist: it reaches the
    // classifier gate but short-circuits to allow before the classifier is consulted.
    const decision = await pm.check('task', { prompt: 'do a thing' });
    assert.equal(decision, 'allow');
    assert.deepEqual(transitions, [], 'auto-mode-safe tool must not trigger a classifier check');
  });

  it('can be cleared with undefined (seam goes inert)', async () => {
    const pm = new PermissionManager('auto-allow');
    const transitions: Array<[boolean, string]> = [];
    pm.setClassifierCheckingListener((checking, toolName) => transitions.push([checking, toolName]));
    pm.setClassifierCheckingListener(undefined);
    pm.setAutoModeSafetyClassifier(asyncClassifier({ decision: 'allow' }));
    await pm.check('bash', LS);
    assert.deepEqual(transitions, []);
  });
});

describe('ToolConfirmationService — in-flight classifier CHECKING set', () => {
  it('tracks, reports, and clears the checking state', () => {
    const svc = new ToolConfirmationService();
    assert.equal(svc.isClassifierChecking('bash'), false);
    assert.equal(svc.setClassifierChecking('bash'), true, 'first mark transitions idle→checking');
    assert.equal(svc.setClassifierChecking('bash'), false, 'repeat mark is a no-op');
    assert.equal(svc.isClassifierChecking('bash'), true);
    assert.deepEqual(svc.getClassifierCheckingTools(), ['bash']);
    assert.equal(svc.clearClassifierChecking('bash'), true);
    assert.equal(svc.clearClassifierChecking('bash'), false, 'clearing an idle tool returns false');
    assert.equal(svc.isClassifierChecking('bash'), false);
    assert.deepEqual(svc.getClassifierCheckingTools(), []);
  });

  it('is independent of the classifier APPROVAL ledger', () => {
    const svc = new ToolConfirmationService();
    svc.setClassifierChecking('bash');
    // Checking (in-flight) and approval (settled) are distinct surfaces.
    assert.equal(svc.getClassifierApproval('bash'), undefined);
    assert.equal(svc.isClassifierChecking('bash'), true);
  });
});
