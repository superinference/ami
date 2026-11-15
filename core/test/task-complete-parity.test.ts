import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  taskCompleteTool,
  resetCompletedTasks,
  isTaskCompleted,
  normalizeTaskStatus,
  VALID_TASK_STATUSES,
} from '../src/tools/task-complete';
import type { ToolContext } from '../src/types';

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

describe('taskCompleteTool parity (superset)', () => {
  beforeEach(() => resetCompletedTasks());

  // ---- Gap 1: status transition validation -------------------------------
  describe('status transition validation', () => {
    it('validateInput rejects an unknown status with valid options', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', status: 'bogus' },
        ctx(),
      );
      assert.equal(res.result, false);
      if (res.result === false) {
        assert.match(res.message, /Invalid status/);
        // Surfaces the valid values so the model can self-correct.
        assert.match(res.message, /completed/);
        assert.equal(typeof res.errorCode, 'number');
      }
    });

    it('validateInput accepts every valid status token', async () => {
      for (const s of VALID_TASK_STATUSES) {
        const res = await taskCompleteTool.validateInput!(
          { summary: 's', status: s },
          ctx(),
        );
        assert.equal(res.result, true, `status ${s} should validate`);
      }
    });

    it('validateInput accepts missing status (optional)', async () => {
      const res = await taskCompleteTool.validateInput!({ summary: 's' }, ctx());
      assert.equal(res.result, true);
    });

    it('normalizeTaskStatus buckets aliases and rejects junk', () => {
      assert.equal(normalizeTaskStatus('success'), 'completed');
      assert.equal(normalizeTaskStatus('DONE'), 'completed');
      assert.equal(normalizeTaskStatus(' partial '), 'partial');
      assert.equal(normalizeTaskStatus('failure'), 'failed');
      assert.equal(normalizeTaskStatus('nope'), undefined);
      assert.equal(normalizeTaskStatus(42), undefined);
    });
  });

  // ---- Gap 2: result/summary capture in metadata -------------------------
  describe('result capture', () => {
    it('captures summary, status, success, timestamp, and result in metadata', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 'did the thing', result: { files: 3 } },
        ctx(),
      );
      assert.equal(res.metadata!.taskComplete, true);
      assert.equal(res.metadata!.status, 'completed');
      assert.equal(res.metadata!.success, true);
      assert.equal(res.metadata!.summary, 'did the thing');
      assert.deepEqual(res.metadata!.result, { files: 3 });
      assert.equal(typeof res.metadata!.completedAt, 'string');
    });

    it('omits result from metadata when not provided', async () => {
      const res = await taskCompleteTool.execute({ summary: 's' }, ctx());
      assert.ok(!('result' in (res.metadata as object)));
    });
  });

  // ---- Gap 3: idempotent completion --------------------------------------
  describe('idempotent completion', () => {
    it('second completion on same session is a no-op ack', async () => {
      const c = ctx({ sessionId: 'sess-1' });
      const first = await taskCompleteTool.execute({ summary: 'one' }, c);
      assert.ok(!first.metadata!.alreadyComplete);
      assert.ok(isTaskCompleted(c));

      const second = await taskCompleteTool.execute({ summary: 'two' }, c);
      assert.equal(second.metadata!.alreadyComplete, true);
      assert.match(second.output, /already marked complete/);
      assert.equal(second.isError, false);
    });

    it('distinct taskIds in one session each complete once', async () => {
      const c = ctx({ sessionId: 'sess-2' });
      const a = await taskCompleteTool.execute({ summary: 'a', taskId: 'A' }, c);
      const b = await taskCompleteTool.execute({ summary: 'b', taskId: 'B' }, c);
      assert.ok(!a.metadata!.alreadyComplete);
      assert.ok(!b.metadata!.alreadyComplete);
      assert.ok(isTaskCompleted(c, 'A'));
      assert.ok(isTaskCompleted(c, 'B'));

      const aAgain = await taskCompleteTool.execute(
        { summary: 'a2', taskId: 'A' },
        c,
      );
      assert.equal(aAgain.metadata!.alreadyComplete, true);
    });

    it('bare context without sessionId never dedups (legacy single-shot)', async () => {
      const c = ctx();
      const first = await taskCompleteTool.execute({ summary: 'x' }, c);
      const second = await taskCompleteTool.execute({ summary: 'y' }, c);
      assert.ok(!first.metadata!.alreadyComplete);
      assert.ok(!second.metadata!.alreadyComplete);
      assert.equal(isTaskCompleted(c), false);
    });

    it('agentId scopes completion separately from the lead session', async () => {
      const lead = ctx({ sessionId: 'sess-3' });
      const sub = ctx({ sessionId: 'sess-3', agentId: 'agent-9' });
      await taskCompleteTool.execute({ summary: 'lead' }, lead);
      assert.ok(isTaskCompleted(lead));
      assert.equal(isTaskCompleted(sub), false);
    });
  });

  // ---- Gap 4: unknown / invalid id handling ------------------------------
  describe('invalid taskId handling', () => {
    it('validateInput rejects an empty/whitespace taskId', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', taskId: '   ' },
        ctx(),
      );
      assert.equal(res.result, false);
      if (res.result === false) assert.match(res.message, /taskId/);
    });

    it('validateInput rejects a non-string taskId', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', taskId: 123 },
        ctx(),
      );
      assert.equal(res.result, false);
    });

    it('validateInput accepts a valid taskId', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', taskId: 'build-42' },
        ctx(),
      );
      assert.equal(res.result, true);
    });
  });

  // ---- Gap 5: error surfacing for non-success outcomes -------------------
  describe('error surfacing', () => {
    it('status=failed surfaces as an error result', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 'tests still red', status: 'failed' },
        ctx(),
      );
      assert.equal(res.isError, true);
      assert.equal(res.metadata!.success, false);
      assert.equal(res.metadata!.status, 'failed');
      assert.match(res.output, /FAILED/);
      assert.match(res.output, /NOT complete/);
    });

    it('status=partial surfaces as a non-error "not fully complete" signal', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 'half done', status: 'partial' },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.equal(res.metadata!.success, false);
      assert.equal(res.metadata!.status, 'partial');
      assert.match(res.output, /partial/);
      assert.match(res.output, /NOT fully complete/);
    });

    it('status=blocked surfaces as non-error incomplete signal', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 'waiting on input', status: 'blocked' },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.equal(res.metadata!.status, 'blocked');
      assert.match(res.output, /blocked/);
    });

    it('status=success is a clean completion (legacy signal)', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 'all green', status: 'success' },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.equal(res.metadata!.success, true);
      assert.match(res.output, /Task complete/);
      assert.match(res.output, /Do not make any more tool calls/);
    });

    it('invalid status reaching execute falls back to completed (never breaks signal)', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 's', status: 'garbage' },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.equal(res.metadata!.status, 'completed');
    });
  });

  // ---- Back-compat: original behaviour preserved -------------------------
  describe('legacy behaviour preserved', () => {
    it('only summary is required', () => {
      assert.deepEqual(taskCompleteTool.inputSchema.required, ['summary']);
    });

    it('default summary on empty string, no error', async () => {
      const res = await taskCompleteTool.execute({ summary: '' }, ctx());
      assert.equal(res.isError, false);
      assert.match(res.output, /Task complete/);
    });

    it('is read-only', () => {
      assert.ok(taskCompleteTool.isReadOnly);
    });
  });
});
