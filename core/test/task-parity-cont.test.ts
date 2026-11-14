import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  ALL_AGENT_DISALLOWED_TOOLS,
  resolveSubagentModel,
  validateSubagentCwd,
  buildWorktreeNotice,
  INHERIT_MODEL_SENTINEL,
} from '../src/tools/task';
import type { ToolContext, ToolDefinition } from '../src/types';

async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
): Promise<{ isError?: boolean; output: string; cfg: { provider?: { model?: string }; cwd?: string } | null; toolNames: string[]; submittedPrompt: string | null }> {
  let cfg: { provider?: { model?: string }; cwd?: string } | null = null;
  let toolNames: string[] = [];
  let submittedPrompt: string | null = null;
  async function* fakeSubmit(p: string) { submittedPrompt = p; yield { type: 'text_delta' as const, text: 'ok' }; }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => { cfg = c as typeof cfg; toolNames = (c.tools as ToolDefinition[]).map(t => t.name); return { submit: fakeSubmit }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, toolNames, submittedPrompt };
}

describe("'inherit' model sentinel (gap: inherit default subagent model)", () => {
  beforeEach(() => resetAgentRegistry());

  it('resolveSubagentModel returns the parent EXACT model for "inherit"', () => {
    assert.equal(resolveSubagentModel(INHERIT_MODEL_SENTINEL, 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentModel('inherit', 'gpt-4o'), 'gpt-4o');
  });

  it('resolveSubagentModel("inherit") with no parent returns undefined (no override)', () => {
    assert.equal(resolveSubagentModel('inherit', undefined), undefined);
  });

  it('task tool with model "inherit" keeps the child on the parent model', async () => {
    const { cfg } = await runTask({ prompt: 'go', model: 'inherit' });
    assert.equal(cfg!.provider!.model, 'claude-opus-4-8');
  });
});

describe('expanded ALL_AGENT_DISALLOWED_TOOLS baseline denylist (gap)', () => {
  beforeEach(() => resetAgentRegistry());

  it('denylist unions interactive + lead-only coordination tools', () => {
    for (const t of ['plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill']) {
      assert.ok(ALL_AGENT_DISALLOWED_TOOLS.has(t), `${t} must be in the baseline denylist`);
    }
  });

  it('a plain general subagent never receives the denied tools', async () => {
    const { toolNames } = await runTask({ prompt: 'go' });
    for (const t of ['plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill']) {
      assert.ok(!toolNames.includes(t), `${t} must be stripped`);
    }
    assert.ok(toolNames.includes('file_read'));
  });
});

describe('validateSubagentCwd boundary (gap: subagent cwd inherited from parent context)', () => {
  let tmp: string;
  beforeEach(() => { resetAgentRegistry(); tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'cwdv-')); });

  it('rejects a relative path', () => {
    assert.match(validateSubagentCwd('rel/path', tmp)!, /absolute/);
  });

  it('rejects a non-existent path', () => {
    assert.match(validateSubagentCwd(path.join(tmp, 'nope'), tmp)!, /does not exist/);
  });

  it('rejects a path that is a file, not a directory', () => {
    const f = path.join(tmp, 'f.txt');
    fs.writeFileSync(f, 'x');
    assert.match(validateSubagentCwd(f, tmp)!, /not a directory/);
  });

  it('rejects a directory outside the parent + approved roots', () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'out-'));
    assert.match(validateSubagentCwd(outside, tmp)!, /outside the parent/);
  });

  it('accepts a subdirectory of the parent cwd', () => {
    const sub = path.join(tmp, 'sub');
    fs.mkdirSync(sub);
    assert.equal(validateSubagentCwd(sub, tmp), null);
  });

  it('accepts a directory inside an approved additional root', () => {
    const extra = fs.mkdtempSync(path.join(os.tmpdir(), 'extra-'));
    const sub = path.join(extra, 'deep');
    fs.mkdirSync(sub);
    assert.equal(validateSubagentCwd(sub, tmp, [extra]), null);
  });

  it('task tool rejects an out-of-bounds cwd before spawning', async () => {
    const outside = fs.mkdtempSync(path.join(os.tmpdir(), 'outt-'));
    const { isError, output, cfg } = await runTask({ prompt: 'go', cwd: outside });
    assert.ok(isError);
    assert.match(output, /outside the parent/);
    assert.equal(cfg, null, 'no engine should be created when cwd is rejected');
  });
});

describe('buildWorktreeNotice (gap: worktree-isolation notice)', () => {
  it('includes path translation, re-read, and isolation guidance', () => {
    const notice = buildWorktreeNotice('/parent/dir', '/tmp/wt-abc');
    assert.match(notice, /\/tmp\/wt-abc/);
    assert.match(notice, /\/parent\/dir/);
    assert.match(notice, /translate/i);
    assert.match(notice, /[Rr]e-read/);
    assert.match(notice, /isolated|will not affect/i);
  });
});
