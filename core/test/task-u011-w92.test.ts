import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  taskTool,
  resetAgentRegistry,
  resolveAgentLabel,
} from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Gap U011/w92: Reverse-lookup of a (background) agent's DISPLAY LABEL — prefer
// the agent's registered @name (reverse-looked-up from the registry by taskId),
// then its task description, then a prompt preview, then the raw taskId. Parity
// with Claude Code's useSwarmBanner, which reverse-looks-up the agent's @name
// from the registry and only falls back to the task description when no name is
// registered (components/PromptInput/useSwarmBanner.ts:L105-L122).
//
// AMI already had the reverse-lookup primitive (getAgentNameById) but the
// background-agent roster/task_list label was built as
// `taskDescription || prompt.slice(0,80)`, dropping the agent's @name entirely —
// a NAMED background agent surfaced in the roster under its description/prompt,
// never under the addressable @name a leader messages it by.
// ---------------------------------------------------------------------------

interface Harness {
  isError?: boolean;
  output: string;
  childEvents: Array<Record<string, unknown>>;
}

async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext>,
): Promise<Harness> {
  const childEvents: Array<Record<string, unknown>> = [];
  async function* fakeSubmit(_p: string) {
    yield { type: 'text_delta', text: 'ok' } as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: (e: Record<string, unknown>) => { childEvents.push(e); },
    _engineFactory: (_c) => ({ submit: fakeSubmit, shutdown: () => {} }),
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, childEvents };
}

describe('resolveAgentLabel — reverse-lookup display label with fallback chain', () => {
  let tmp: string;
  let prevData: string | undefined;

  beforeEach(() => {
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w92u-'));
    prevData = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  afterEach(() => {
    if (prevData === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevData;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best effort */ }
    resetAgentRegistry();
  });

  it('reverse-looks-up the registered @name from a taskId (FOUND branch)', async () => {
    // A real foreground spawn registers agentNameRegistry[name] = taskId.
    const spawn = await runTask({ prompt: 'start', mode: 'general', name: 'builder' }, { cwd: tmp });
    assert.ok(!spawn.isError, `spawn should succeed: ${spawn.output}`);
    const start = spawn.childEvents.find(e => e.type === 'subagent_start');
    assert.ok(start, 'spawn emitted a subagent_start');
    const taskId = String(start!.nodeId);
    // Even with a description/prompt present, the registered @name wins.
    assert.equal(
      resolveAgentLabel({ taskId, description: 'fix the bug', prompt: 'do work' }),
      'builder',
    );
  });

  it('falls back to the description when no name is registered for the id', () => {
    assert.equal(resolveAgentLabel({ taskId: 'agent-unknown', description: 'fix the bug' }), 'fix the bug');
  });

  it('falls back to a prompt preview (first 80 chars) when no name/description', () => {
    const longPrompt = 'x'.repeat(200);
    assert.equal(resolveAgentLabel({ taskId: 'agent-x', prompt: longPrompt }), 'x'.repeat(80));
  });

  it('ignores a blank description/prompt and falls back to the taskId', () => {
    assert.equal(resolveAgentLabel({ taskId: 'agent-4', description: '   ', prompt: '  ' }), 'agent-4');
  });

  it('final fallback is the literal "agent" when nothing is known', () => {
    assert.equal(resolveAgentLabel({}), 'agent');
  });
});

// ---------------------------------------------------------------------------
// Integration / E2E reproducer: a NAMED background agent's process-manager
// roster entry must carry the agent's @name in its label — not its description.
// RED on revert: the old `label = taskDescription || prompt.slice(0,80)` made
// the entry read "[agent] fix the bug" / "Background agent: fix the bug",
// never surfacing the "builder" name a leader addresses it by.
// ---------------------------------------------------------------------------

describe('named background agent roster label surfaces the @name (U011/w92)', () => {
  let tmp: string;
  let prevData: string | undefined;

  beforeEach(() => {
    resetAgentRegistry();
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w92-'));
    prevData = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = tmp;
  });
  afterEach(() => {
    if (prevData === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevData;
    try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best effort */ }
    resetAgentRegistry();
  });

  it('registers the background agent under its @name, not its description', async () => {
    const pm = new ProcessManager(tmp);
    const res = await runTask(
      { prompt: 'do a lot of work here', mode: 'general', name: 'builder', description: 'fix the bug', run_in_background: true },
      { cwd: tmp, processManager: pm as unknown as ToolContext['processManager'] },
    );
    assert.ok(!res.isError, `background spawn should succeed: ${res.output}`);

    const agents = pm.list();
    assert.equal(agents.length, 1, 'exactly one background agent registered');
    const entry = agents[0];
    assert.match(entry.command, /builder/, `command should surface the @name: ${entry.command}`);
    assert.match(String(entry.description), /builder/, `description should surface the @name: ${entry.description}`);
    assert.doesNotMatch(entry.command, /fix the bug/, 'the @name wins over the task description in the label');
  });

  it('an UNNAMED background agent still falls back to its description', async () => {
    const pm = new ProcessManager(tmp);
    const res = await runTask(
      { prompt: 'do work', mode: 'general', description: 'fix the bug', run_in_background: true },
      { cwd: tmp, processManager: pm as unknown as ToolContext['processManager'] },
    );
    assert.ok(!res.isError, `background spawn should succeed: ${res.output}`);
    const entry = pm.list()[0];
    assert.match(entry.command, /fix the bug/, 'unnamed agent label still uses the description');
  });
});
