/**
 * U006 (w88) — core/core/src/types.ts parity additions (building-block layer).
 *
 * Each block covers one confirmed parity gap targeting types.ts, as a self-contained
 * typed schema field + a pure (non-display) helper/validator, plus the one live-wired
 * win (worktree human-friendly activity labels through the real tool-activity
 * resolver). Mirrors the established U006 pattern (types-u006-w86.test.ts).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  MIN_SLOW_PROGRESS_REVEAL_SECONDS,
  shouldRevealSlowProgress,
  sanitizeToolInputSummary,
  resolveSentAt,
  selectModelFacingOutput,
  selectUserFacingOutput,
  shouldRenderGroupedToolUse,
  isSuggestionMessage,
  shouldForcePermissionCheck,
  validateSummarizeMetadata,
} from '../src/types';
import type {
  EngineEvent,
  ToolResult,
  ToolDefinition,
  SystemMessage,
  SummarizeMetadata,
  SystemMessageKind,
} from '../src/types';
import {
  describeToolActivity,
  createActivityDescriptionResolver,
} from '../src/tools/tool-activity-class';
import { enterWorktreeTool } from '../src/tools/enter-worktree';
import { exitWorktreeTool } from '../src/tools/exit-worktree';

// ---------------------------------------------------------------------------
// Gap: toolInputSummary on the user_question event (PromptDialog titleRight)
// ---------------------------------------------------------------------------
describe('user_question toolInputSummary', () => {
  it('is carried on the user_question event type', () => {
    const ev: EngineEvent = {
      type: 'user_question',
      toolCallId: 't1',
      question: 'Proceed?',
      options: [],
      allowFreeText: false,
      toolInputSummary: 'rm -rf build/',
    };
    assert.equal(ev.type === 'user_question' ? ev.toolInputSummary : undefined, 'rm -rf build/');
  });

  it('sanitizeToolInputSummary collapses null/empty/whitespace to undefined and trims', () => {
    assert.equal(sanitizeToolInputSummary(null), undefined);
    assert.equal(sanitizeToolInputSummary(undefined), undefined);
    assert.equal(sanitizeToolInputSummary('   '), undefined);
    assert.equal(sanitizeToolInputSummary('  cat x.ts  '), 'cat x.ts');
  });
});

// ---------------------------------------------------------------------------
// Gap: structured shell progress fields (elapsedSeconds/totalLines) + 2s reveal
// ---------------------------------------------------------------------------
describe('tool_use_progress structured fields + slow-reveal threshold', () => {
  it('carries elapsedSeconds/totalLines on the tool_use_progress event', () => {
    const ev: EngineEvent = {
      type: 'tool_use_progress',
      toolCallId: 't1',
      data: 'installing…',
      elapsedSeconds: 63,
      totalLines: 240,
    };
    assert.equal(ev.type === 'tool_use_progress' ? ev.totalLines : undefined, 240);
  });

  it('MIN_SLOW_PROGRESS_REVEAL_SECONDS matches the reference (2)', () => {
    assert.equal(MIN_SLOW_PROGRESS_REVEAL_SECONDS, 2);
  });

  it('shouldRevealSlowProgress gates on the threshold; undefined never reveals', () => {
    assert.equal(shouldRevealSlowProgress(undefined), false);
    assert.equal(shouldRevealSlowProgress(1.9), false);
    assert.equal(shouldRevealSlowProgress(2), true);
    assert.equal(shouldRevealSlowProgress(10), true);
    assert.equal(shouldRevealSlowProgress(1, 0.5), true);
  });
});

// ---------------------------------------------------------------------------
// Gap: dedicated last_tool_name + summary fields on subagent progress
// ---------------------------------------------------------------------------
describe('subagent_progress summary + lastToolName', () => {
  it('carries both the mid-turn summary and lastToolName fields', () => {
    const ev: EngineEvent = {
      type: 'subagent_progress',
      nodeId: 'n1',
      depth: 1,
      activity: 'tool',
      detail: 'running bash',
      lastToolName: 'bash',
      summary: 'Investigating the failing test suite',
    };
    if (ev.type !== 'subagent_progress') throw new Error('unreachable');
    assert.equal(ev.lastToolName, 'bash');
    assert.equal(ev.summary, 'Investigating the failing test suite');
  });
});

// ---------------------------------------------------------------------------
// Gap: ToolResult.sentAt emit timestamp (optional-for-replay invariant)
// ---------------------------------------------------------------------------
describe('ToolResult.sentAt + resolveSentAt', () => {
  it('preserves an already-stamped timestamp (resume replays verbatim)', () => {
    const original = '2020-01-01T00:00:00.000Z';
    assert.equal(resolveSentAt(original, '2099-12-31T23:59:59.000Z'), original);
  });

  it('stamps the provided clock when absent', () => {
    assert.equal(resolveSentAt(undefined, '2026-11-17T12:00:00.000Z'), '2026-11-17T12:00:00.000Z');
  });

  it('defaults to the current clock when no now is given (valid ISO)', () => {
    const stamped = resolveSentAt(undefined);
    assert.ok(!Number.isNaN(Date.parse(stamped)));
  });

  it('is an optional field — a pre-sentAt result still type-checks and renders', () => {
    const legacy: ToolResult = { output: 'ok' };
    assert.equal(legacy.sentAt, undefined);
  });
});

// ---------------------------------------------------------------------------
// Gap: decoupled model-receipt vs user-facing payload on ToolResult
// ---------------------------------------------------------------------------
describe('ToolResult userFacingContent decoupling', () => {
  it('model sees output; user sees userFacingContent when set', () => {
    const r: ToolResult = { output: 'Message delivered to user.', userFacingContent: 'Hi team, see attached.' };
    assert.equal(selectModelFacingOutput(r), 'Message delivered to user.');
    assert.equal(selectUserFacingOutput(r), 'Hi team, see attached.');
  });

  it('both audiences fall back to output when userFacingContent is absent', () => {
    const r: ToolResult = { output: 'plain' };
    assert.equal(selectModelFacingOutput(r), 'plain');
    assert.equal(selectUserFacingOutput(r), 'plain');
  });
});

// ---------------------------------------------------------------------------
// Gap: per-tool renderGroupedToolUse opt-in on ToolDefinition
// ---------------------------------------------------------------------------
describe('ToolDefinition.renderGroupedToolUse', () => {
  it('shouldRenderGroupedToolUse is true only when explicitly opted in', () => {
    assert.equal(shouldRenderGroupedToolUse({ renderGroupedToolUse: true }), true);
    assert.equal(shouldRenderGroupedToolUse({ renderGroupedToolUse: false }), false);
    assert.equal(shouldRenderGroupedToolUse({}), false);
  });
});

// ---------------------------------------------------------------------------
// Gap: SystemMessage 'suggestion' subtype (createSystemMessage subtype)
// ---------------------------------------------------------------------------
describe("SystemMessage 'suggestion' kind", () => {
  it('isSuggestionMessage detects the suggestion subtype', () => {
    const suggestion: SystemMessage = { role: 'system', content: 'Skill "x" updated with improvements.', kind: 'suggestion' };
    const plain: SystemMessage = { role: 'system', content: 'ready' };
    assert.equal(isSuggestionMessage(suggestion), true);
    assert.equal(isSuggestionMessage(plain), false);
  });

  it('SystemMessageKind is a valid literal', () => {
    const k: SystemMessageKind = 'suggestion';
    assert.equal(k, 'suggestion');
  });
});

// ---------------------------------------------------------------------------
// Gap: requireCanUseTool forked-agent override
// ---------------------------------------------------------------------------
describe('shouldForcePermissionCheck (requireCanUseTool override)', () => {
  it('override forces a real check even past auto-approval', () => {
    assert.equal(shouldForcePermissionCheck({ requireCanUseTool: true, autoApproved: true }), true);
    assert.equal(shouldForcePermissionCheck({ requireCanUseTool: true, autoApproved: false }), true);
  });

  it('without the override, an auto-approval skips the check', () => {
    assert.equal(shouldForcePermissionCheck({ autoApproved: true }), false);
    assert.equal(shouldForcePermissionCheck({ requireCanUseTool: false, autoApproved: true }), false);
  });

  it('without the override and not auto-approved, a check is required', () => {
    assert.equal(shouldForcePermissionCheck({ autoApproved: false }), true);
    assert.equal(shouldForcePermissionCheck({}), true);
  });
});

// ---------------------------------------------------------------------------
// Gap: persisted summarizeMetadata on the compaction summary marker
// ---------------------------------------------------------------------------
describe('SummarizeMetadata + validateSummarizeMetadata', () => {
  it('accepts a well-formed object and normalizes userContext', () => {
    const meta = validateSummarizeMetadata({ messagesSummarized: 12, direction: 'before', userContext: '  focus on auth  ' });
    assert.deepEqual(meta, { messagesSummarized: 12, direction: 'before', userContext: 'focus on auth' } satisfies SummarizeMetadata);
  });

  it('drops an empty/whitespace userContext and an invalid direction', () => {
    const meta = validateSummarizeMetadata({ messagesSummarized: 3, direction: 'sideways', userContext: '   ' });
    assert.deepEqual(meta, { messagesSummarized: 3 });
  });

  it('rejects a missing/negative/non-integer count and non-objects', () => {
    assert.equal(validateSummarizeMetadata(null), null);
    assert.equal(validateSummarizeMetadata('x'), null);
    assert.equal(validateSummarizeMetadata({}), null);
    assert.equal(validateSummarizeMetadata({ messagesSummarized: -1 }), null);
    assert.equal(validateSummarizeMetadata({ messagesSummarized: 2.5 }), null);
  });
});

// ---------------------------------------------------------------------------
// Gap: human-friendly worktree progress labels — LIVE-WIRED through the real
// activity resolver that task.ts uses (createActivityDescriptionResolver).
// ---------------------------------------------------------------------------
describe('worktree human-friendly activity labels (live-wired)', () => {
  it('describeToolActivity labels enter_worktree / exit_worktree', () => {
    assert.equal(describeToolActivity('enter_worktree', {}), 'Creating worktree');
    assert.equal(describeToolActivity('enter_worktree', { name: 'feature-x' }), 'Creating worktree feature-x');
    assert.equal(describeToolActivity('exit_worktree', {}), 'Exiting worktree');
    assert.equal(describeToolActivity('exit_worktree', { action: 'merge' }), 'Exiting worktree (merge)');
  });

  it('is reached through createActivityDescriptionResolver with the real tools', () => {
    const resolve = createActivityDescriptionResolver([enterWorktreeTool, exitWorktreeTool]);
    assert.equal(resolve('enter_worktree', { description: 'build the thing' }), 'Creating worktree build the thing');
    assert.equal(resolve('exit_worktree', { action: 'keep' }), 'Exiting worktree (keep)');
  });
});
