import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { taskKillTool } from '../src/tools/task-kill';
import { taskOutputTool } from '../src/tools/task-output';
import { taskCompleteTool } from '../src/tools/task-complete';
import { mapChildToSubagentEvent, summarizeToolInput } from '../src/tools/subagent-relay';
import { SubagentLimiter, parseEnvIntInRange } from '../src/subagent-limiter';
import type { ToolContext } from '../src/types';

describe('task_kill structured result + codes', () => {
  it('rejects unknown input keys', async () => {
    const ctx = { cwd: '/tmp', processManager: {} } as unknown as ToolContext;
    const r = await taskKillTool.execute!({ task_id: 'x', force: true } as Record<string, unknown>, ctx);
    assert.ok(r.isError && r.output.includes('unknown input key') && r.output.includes('force'));
  });

  it('tags not_found and kill_failed with machine-readable codes and echoes command', async () => {
    const notFound = { cwd: '/tmp', processManager: { get: () => undefined } } as unknown as ToolContext;
    const nf = await taskKillTool.execute!({ task_id: 'ghost' }, notFound);
    assert.ok(nf.isError && nf.output.includes('[code: not_found]'));

    const running = {
      cwd: '/tmp',
      processManager: {
        get: () => ({ status: 'running', pid: 42, command: 'npm test', exitCode: null }),
        kill: () => false,
      },
    } as unknown as ToolContext;
    const kf = await taskKillTool.execute!({ task_id: 'a' }, running);
    assert.ok(kf.isError && kf.output.includes('[code: kill_failed]'));

    const ok = {
      cwd: '/tmp',
      processManager: {
        get: () => ({ status: 'running', pid: 42, command: 'npm test', exitCode: null }),
        kill: () => true,
      },
    } as unknown as ToolContext;
    const okRes = await taskKillTool.execute!({ task_id: 'a' }, ok);
    assert.ok(!okRes.isError && okRes.output.includes('killed successfully') && okRes.output.includes('npm test'));
  });
});

describe('task_output clamp + cap + distinct wait-timeout', () => {
  it('marks a wait that returned while still running as a timed-out retrieval', async () => {
    const ctx = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      processManager: { getOutput: () => ({ status: 'running', exitCode: null, elapsedMs: 10, output: '' }) },
    } as unknown as ToolContext;
    const r = await taskOutputTool.execute!({ task_id: 'w', wait: true, timeout_ms: 20 }, ctx);
    assert.ok(r.output.includes('running'));
    assert.ok(r.output.includes('wait timed out'));
  });

  it('caps very large output with a truncation marker', async () => {
    const big = 'x'.repeat(200_000);
    const ctx = {
      cwd: '/tmp',
      abortSignal: new AbortController().signal,
      processManager: { getOutput: () => ({ status: 'completed', exitCode: 0, elapsedMs: 10, output: big }) },
    } as unknown as ToolContext;
    const r = await taskOutputTool.execute!({ task_id: 'd' }, ctx);
    assert.ok(r.output.includes('output truncated to the last'));
    assert.ok(r.output.length < 200_000);
  });
});

describe('task_complete anti self-certification', () => {
  it('description warns against self-certifying', () => {
    assert.ok(taskCompleteTool.description.toLowerCase().includes('self-certify'));
    assert.ok(taskCompleteTool.description.toLowerCase().includes('verified'));
  });
});

describe('subagent-relay human-readable activity', () => {
  it('summarizes the salient argument per tool', () => {
    assert.equal(summarizeToolInput('bash', { command: 'npm test' }), 'npm test');
    assert.equal(summarizeToolInput('file_read', { file_path: 'src/x.ts' }), 'src/x.ts');
    assert.equal(summarizeToolInput('grep', { pattern: 'TODO' }), 'TODO');
    assert.equal(summarizeToolInput('bash', {}), '');
    assert.equal(summarizeToolInput('x', 'notobj' as unknown), '');
    assert.equal(summarizeToolInput('bash', { command: 'y'.repeat(200) }).length, 80);
  });

  it('tool_use_start detail carries the argument summary, bare name when empty', () => {
    const withArg = mapChildToSubagentEvent({ type: 'tool_use_start', toolName: 'bash', toolCallId: 'c', input: { command: 'ls' } }, 'n', 1);
    assert.equal((withArg as { detail: string }).detail, 'bash ls');
    const bare = mapChildToSubagentEvent({ type: 'tool_use_start', toolName: 'grep', toolCallId: 'c', input: {} }, 'n', 1);
    assert.equal((bare as { detail: string }).detail, 'grep');
  });
});

describe('subagent-limiter range-validated env knobs', () => {
  const ORIG = process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
  afterEach(() => {
    if (ORIG === undefined) delete process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS;
    else process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = ORIG;
  });

  it('parseEnvIntInRange rejects out-of-range values back to the fallback', () => {
    assert.equal(parseEnvIntInRange('5', 20, { min: 1, max: 1000 }), 5);
    assert.equal(parseEnvIntInRange('0', 20, { min: 1, max: 1000 }), 20); // below min
    assert.equal(parseEnvIntInRange('99999', 20, { min: 1, max: 1000 }), 20); // above max
    assert.equal(parseEnvIntInRange('-3', 20, { min: 1, max: 1000 }), 20); // negative
    assert.equal(parseEnvIntInRange('nonsense', 20, { min: 1, max: 1000 }), 20);
  });

  it('a limiter never adopts a degenerate zero concurrency from the env', () => {
    process.env.CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS = '0';
    const lim = new SubagentLimiter();
    assert.equal(lim.maxConcurrent, 20);
  });
});
