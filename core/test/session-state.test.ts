import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  SESSION_STATES,
  isSessionState,
  SessionStateMachine,
  STATUS_CATEGORIES,
  isStatusCategory,
  validatePostTurnStatus,
} from '../src/session-state';
import type { RequiresActionDetails, PostTurnStatusSummary } from '../src/session-state';

const details: RequiresActionDetails = {
  toolName: 'bash',
  actionDescription: 'Running npm test',
  toolUseId: 'tu-1',
  requestId: 'req-1',
  input: { command: 'npm test' },
};

describe('session-state: tri-state + guard', () => {
  it('SESSION_STATES enumerates the three states and isSessionState narrows', () => {
    assert.deepEqual([...SESSION_STATES], ['idle', 'running', 'requires_action']);
    assert.equal(isSessionState('running'), true);
    assert.equal(isSessionState('bogus'), false);
    assert.equal(isSessionState(42), false);
  });
});

describe('session-state: SessionStateMachine', () => {
  it('starts idle and tracks the current state', () => {
    const m = new SessionStateMachine();
    assert.equal(m.state, 'idle');
    m.transition('running');
    assert.equal(m.state, 'running');
  });

  it('running with no pending action produces no metadata patch', () => {
    const m = new SessionStateMachine();
    assert.equal(m.transition('running'), null);
  });

  it('requires_action with details sets pendingAction', () => {
    const m = new SessionStateMachine();
    m.transition('running');
    const patch = m.transition('requires_action', details);
    assert.deepEqual(patch, { pendingAction: details });
  });

  it('leaving requires_action clears pendingAction to null (RFC-7396)', () => {
    const m = new SessionStateMachine();
    m.transition('requires_action', details);
    const patch = m.transition('running');
    assert.deepEqual(patch, { pendingAction: null });
  });

  it('reaching idle clears taskSummary (and pendingAction when one was set)', () => {
    const m = new SessionStateMachine();
    m.transition('requires_action', details);
    const patch = m.transition('idle');
    assert.deepEqual(patch, { pendingAction: null, taskSummary: null });
  });

  it('idle from running (no pending action) clears only taskSummary', () => {
    const m = new SessionStateMachine();
    m.transition('running');
    assert.deepEqual(m.transition('idle'), { taskSummary: null });
  });

  it('requires_action without details does not set pendingAction', () => {
    const m = new SessionStateMachine();
    assert.equal(m.transition('requires_action'), null);
  });

  it('reset returns to idle with no pending action', () => {
    const m = new SessionStateMachine();
    m.transition('requires_action', details);
    m.reset();
    assert.equal(m.state, 'idle');
    // after reset, a running transition yields no clear patch (pending already gone)
    assert.equal(m.transition('running'), null);
  });
});

describe('session-state: post-turn status summary', () => {
  const valid: PostTurnStatusSummary = {
    summarizesUuid: 'uuid-1',
    statusCategory: 'completed',
    statusDetail: 'done',
    isNoteworthy: true,
    title: 'Implemented feature',
    description: 'Added the thing',
    recentAction: 'wrote code',
    needsAction: '',
    artifactUrls: ['https://example.com/pr/1'],
  };

  it('STATUS_CATEGORIES + isStatusCategory', () => {
    assert.deepEqual([...STATUS_CATEGORIES], ['blocked', 'waiting', 'completed', 'review_ready', 'failed']);
    assert.equal(isStatusCategory('review_ready'), true);
    assert.equal(isStatusCategory('nope'), false);
  });

  it('validates a well-formed summary', () => {
    assert.deepEqual(validatePostTurnStatus(valid), []);
  });

  it('rejects a null/non-object summary', () => {
    assert.deepEqual(validatePostTurnStatus(null), ['summary must be an object']);
    assert.deepEqual(validatePostTurnStatus(undefined), ['summary must be an object']);
  });

  it('collects every field error', () => {
    const errs = validatePostTurnStatus({
      summarizesUuid: '',
      statusCategory: 'xxx' as PostTurnStatusSummary['statusCategory'],
      title: '',
      isNoteworthy: 'yes' as unknown as boolean,
      artifactUrls: [1 as unknown as string],
    });
    assert.ok(errs.some((e) => e.includes('summarizesUuid')));
    assert.ok(errs.some((e) => e.includes('statusCategory')));
    assert.ok(errs.some((e) => e.includes('title')));
    assert.ok(errs.some((e) => e.includes('isNoteworthy')));
    assert.ok(errs.some((e) => e.includes('artifactUrls')));
  });

  it('accepts missing optional fields but rejects a non-array artifactUrls', () => {
    assert.deepEqual(
      validatePostTurnStatus({ summarizesUuid: 'u', statusCategory: 'failed', title: 't' }),
      [],
    );
    const errs = validatePostTurnStatus({
      summarizesUuid: 'u',
      statusCategory: 'failed',
      title: 't',
      artifactUrls: 'nope' as unknown as string[],
    });
    assert.deepEqual(errs, ['artifactUrls must be an array of strings']);
  });
});
