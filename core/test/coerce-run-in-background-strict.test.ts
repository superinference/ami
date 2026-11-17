// E2E red-green reproducer: the bash and task tools STRICT-parse the optional
// `run_in_background` flag via parseOptionalBool (core/core/src/utils/coerce.ts),
// so a malformed value like the string "yes" (or a number) is REJECTED with an
// error instead of being silently collapsed to `false` by the lenient coerceBool
// and running in the wrong mode. Recognized spellings (true/false and their
// string forms) still work unchanged.
//
// RED on revert: restore the old `coerceBool(input.run_in_background)` path and
// the "yes" cases stop erroring (bash echoes "hi" in the foreground; task runs
// the agent and returns its text), so the isError assertions fail.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { bashTool } from '../src/tools/bash';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import type { ToolContext } from '../src/types';

describe('bashTool — strict run_in_background parse', () => {
  function ctx(): ToolContext {
    return {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
    } as unknown as ToolContext;
  }

  it('rejects a malformed run_in_background ("yes") instead of running foreground', async () => {
    const res = await bashTool.execute(
      { command: 'echo hi', run_in_background: 'yes' },
      ctx(),
    );
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /run_in_background must be a boolean/);
    assert.match(res.output, /"yes"/);
    // Proves it bailed BEFORE executing the command.
    assert.ok(!res.output.includes('hi'), res.output);
  });

  it('rejects a numeric run_in_background (1)', async () => {
    const res = await bashTool.execute(
      { command: 'echo hi', run_in_background: 1 as unknown as boolean },
      ctx(),
    );
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /received 1/);
  });

  it('still runs foreground for an explicit false / "false" (regression)', async () => {
    for (const v of [false, 'false'] as const) {
      const res = await bashTool.execute(
        { command: 'echo hi', run_in_background: v as unknown as boolean },
        ctx(),
      );
      assert.ok(!res.isError, `${JSON.stringify(v)} should not error: ${res.output}`);
      assert.match(res.output, /hi/);
    }
  });

  it('omitting run_in_background runs foreground (no error)', async () => {
    const res = await bashTool.execute({ command: 'echo plain' }, ctx());
    assert.ok(!res.isError, res.output);
    assert.match(res.output, /plain/);
  });
});

describe('taskTool — strict run_in_background parse', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetAgentRegistry();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-rib-strict-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function gen(events: unknown[]) {
    return async function* (_p: string) {
      for (const e of events) yield e;
    };
  }

  function ctx(agentDef: unknown): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
      _skillManager: {
        getAgent: () => agentDef,
        listAgents: () => [agentDef],
      },
      _engineFactory: () => ({ submit: gen([{ type: 'text_delta', text: 'ok' }]) }),
    } as unknown as ToolContext;
  }

  it('rejects a malformed run_in_background ("yes") instead of silently running foreground', async () => {
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'fgagent', run_in_background: 'yes' },
      ctx({ name: 'fgagent', systemPrompt: 'be a fg agent', tools: ['file_read'] }),
    );
    assert.equal(res.isError, true, res.output);
    assert.match(res.output, /run_in_background must be a boolean/);
    assert.ok(!/ok/.test(res.output), res.output);
  });

  it('still runs foreground for an explicit "false" (regression)', async () => {
    const res = await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'fgagent', run_in_background: 'false' },
      ctx({ name: 'fgagent', systemPrompt: 'be a fg agent', tools: ['file_read'] }),
    );
    assert.ok(!res.isError, res.output);
    assert.match(res.output, /ok/);
  });
});
