import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  taskCompleteTool,
  resetCompletedTasks,
  isTaskCompleted,
} from '../src/tools/task-complete';
import type { ToolContext } from '../src/types';

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

describe('taskCompleteTool U292 parity gaps', () => {
  beforeEach(() => resetCompletedTasks());

  // ---- Gap: tool input validation / edge-case rejection (non-string summary)
  describe('summary type guard', () => {
    it('validateInput rejects a non-string summary (object)', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: { a: 1 } },
        ctx(),
      );
      assert.equal(res.result, false);
      if (res.result === false) {
        assert.match(res.message, /summary/i);
        assert.equal(typeof res.errorCode, 'number');
      }
    });

    it('validateInput rejects a numeric summary', async () => {
      const res = await taskCompleteTool.validateInput!({ summary: 42 }, ctx());
      assert.equal(res.result, false);
    });

    it('validateInput rejects an array summary', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: ['x'] },
        ctx(),
      );
      assert.equal(res.result, false);
    });

    it('validateInput accepts a string summary', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 'ok' },
        ctx(),
      );
      assert.equal(res.result, true);
    });

    it('execute coerces a non-string summary to the default (never [object Object])', async () => {
      const res = await taskCompleteTool.execute(
        { summary: { a: 1 } as unknown as string },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.doesNotMatch(res.output, /\[object Object\]/);
      assert.match(res.output, /Task complete/);
    });
  });

  // ---- Gap: schema-driven / validated final output on the concluding tool
  describe('schema-validated final output (resultSchema)', () => {
    const schema = {
      type: 'object',
      properties: { files: { type: 'number' }, ok: { type: 'boolean' } },
      required: ['files'],
    };

    it('rejects a result that violates the caller-supplied schema', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', result: { ok: true }, resultSchema: schema },
        ctx(),
      );
      assert.equal(res.result, false);
      if (res.result === false) {
        assert.match(res.message, /Output does not match required schema/);
        assert.match(res.message, /files/);
        assert.equal(typeof res.errorCode, 'number');
      }
    });

    it('rejects a wrong-typed field against the schema', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', result: { files: 'three' }, resultSchema: schema },
        ctx(),
      );
      assert.equal(res.result, false);
      if (res.result === false) {
        assert.match(res.message, /Output does not match required schema/);
      }
    });

    it('accepts a result that conforms to the schema', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', result: { files: 3, ok: true }, resultSchema: schema },
        ctx(),
      );
      assert.equal(res.result, true);
    });

    it('rejects a malformed resultSchema itself', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', result: {}, resultSchema: { type: 'banana' } },
        ctx(),
      );
      assert.equal(res.result, false);
      if (res.result === false) assert.match(res.message, /resultSchema/i);
    });

    it('ignores resultSchema when absent (legacy free-form result)', async () => {
      const res = await taskCompleteTool.validateInput!(
        { summary: 's', result: { anything: 1 } },
        ctx(),
      );
      assert.equal(res.result, true);
    });

    it('resultSchema is advertised on the input schema', () => {
      const props = taskCompleteTool.inputSchema.properties as Record<
        string,
        unknown
      >;
      assert.ok(props.resultSchema);
    });
  });

  // ---- Gap: TaskCompleted hook can block completion and inject feedback
  describe('TaskCompleted hook veto', () => {
    function hookCtx(
      impl: () => Promise<{ blockingErrors: string[] } | void>,
      overrides: Partial<ToolContext> = {},
    ): ToolContext {
      return ctx({
        sessionId: 'hook-sess',
        _hookManager: {
          executeTaskCompleted: impl,
        } as unknown as ToolContext['_hookManager'],
        ...overrides,
      });
    }

    it('blocks a clean completion and surfaces the hook feedback', async () => {
      const c = hookCtx(async () => ({ blockingErrors: ['run the tests first'] }));
      const res = await taskCompleteTool.execute({ summary: 'done' }, c);
      assert.equal(res.isError, true);
      assert.match(res.output, /BLOCKED/);
      assert.match(res.output, /run the tests first/);
      assert.equal(res.metadata!.completionBlocked, true);
      assert.deepEqual(res.metadata!.blockingErrors, ['run the tests first']);
      // A blocked completion is NOT recorded — the model can retry after fixing.
      assert.equal(isTaskCompleted(c), false);
    });

    it('allows completion when the hook returns no blocking errors', async () => {
      const c = hookCtx(async () => undefined);
      const res = await taskCompleteTool.execute({ summary: 'done' }, c);
      assert.equal(res.isError, false);
      assert.match(res.output, /Task complete/);
      assert.equal(isTaskCompleted(c), true);
    });

    it('a throwing completion hook never blocks the signal', async () => {
      const c = hookCtx(async () => {
        throw new Error('boom');
      });
      const res = await taskCompleteTool.execute({ summary: 'done' }, c);
      assert.equal(res.isError, false);
      assert.equal(isTaskCompleted(c), true);
    });

    it('does NOT consult the hook for a failed status (already not-complete)', async () => {
      let called = false;
      const c = hookCtx(async () => {
        called = true;
        return { blockingErrors: ['x'] };
      });
      const res = await taskCompleteTool.execute(
        { summary: 'nope', status: 'failed' },
        c,
      );
      assert.equal(called, false);
      assert.equal(res.isError, true); // failed surfaces as error, not a veto
      assert.match(res.output, /FAILED/);
    });
  });

  // ---- Gap: verification-nudge / anti-self-certification on completion
  describe('verification nudge', () => {
    it('appends a verification nudge on a clean completion with no verified flag', async () => {
      const res = await taskCompleteTool.execute({ summary: 'green' }, ctx());
      assert.equal(res.isError, false);
      assert.match(res.output, /verif/i);
      assert.match(res.output, /self-assign|verifier|verify/i);
      assert.equal(res.metadata!.verified, false);
    });

    it('omits the nudge when verified=true is reported', async () => {
      const res = await taskCompleteTool.execute(
        { summary: 'green', verified: true },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.doesNotMatch(res.output, /self-assign/i);
      assert.equal(res.metadata!.verified, true);
    });

    it('verified is advertised on the input schema', () => {
      const props = taskCompleteTool.inputSchema.properties as Record<
        string,
        unknown
      >;
      assert.ok(props.verified);
    });
  });
});
