import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTool,
  resetAgentRegistry,
  OUTPUT_FILE_OPEN_FLAGS,
} from '../src/tools/task';
import { mapChildToSubagentEvent } from '../src/tools/subagent-relay';
import type { ToolContext, EngineEvent } from '../src/types';

/** Run the task tool against a fake engine, capturing every child EngineEvent the
 *  tool relays to the parent UI via `_emitChild` (the live-roster feed). */
async function runTaskCapture(
  input: Record<string, unknown>,
  over: Partial<ToolContext> = {},
): Promise<{ isError?: boolean; output: string; emitted: EngineEvent[] }> {
  const emitted: EngineEvent[] = [];
  async function* fakeSubmit() {
    yield { type: 'text_delta', text: 'ok' } as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _emitChild: (e: EngineEvent) => { emitted.push(e); },
    _engineFactory: () => ({ submit: fakeSubmit, shutdown: () => {} }),
    ...over,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, emitted };
}

function startEvent(emitted: EngineEvent[]): Extract<EngineEvent, { type: 'subagent_start' }> {
  const ev = emitted.find(e => e.type === 'subagent_start');
  assert.ok(ev, 'a subagent_start event must be emitted to the roster');
  return ev as Extract<EngineEvent, { type: 'subagent_start' }>;
}

// ---------------------------------------------------------------------------
// Gap: color spawn parameter + per-agent color knob → subagent_start roster badge
// (swarm/backends/InProcessBackend.tsx / spawnInProcess.ts color field)
// ---------------------------------------------------------------------------
describe('task tool — per-spawn color on the subagent_start roster event', () => {
  beforeEach(() => resetAgentRegistry());

  it('threads an explicit per-call color onto the subagent_start event', async () => {
    const { emitted } = await runTaskCapture({ prompt: 'go', description: 'do it', color: 'green' });
    assert.equal(startEvent(emitted).color, 'green');
  });

  it('falls back to the named agent definition color when no per-call color', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'ag', systemPrompt: 'be an agent', color: 'magenta' }),
      listAgents: () => [{ name: 'ag' }],
    };
    const { emitted } = await runTaskCapture(
      { prompt: 'go', description: 'do it', subagent_type: 'ag' },
      { _skillManager: skillManager as never },
    );
    assert.equal(startEvent(emitted).color, 'magenta');
  });

  it('per-call color overrides the agent definition color', async () => {
    const skillManager = {
      getAgent: () => ({ name: 'ag', systemPrompt: 'be an agent', color: 'magenta' }),
      listAgents: () => [{ name: 'ag' }],
    };
    const { emitted } = await runTaskCapture(
      { prompt: 'go', description: 'do it', subagent_type: 'ag', color: 'cyan' },
      { _skillManager: skillManager as never },
    );
    assert.equal(startEvent(emitted).color, 'cyan');
  });

  it('omits color (undefined) when neither the call nor the agent set one', async () => {
    const { emitted } = await runTaskCapture({ prompt: 'go', description: 'do it' });
    assert.equal(startEvent(emitted).color, undefined);
  });

  it('rejects an invalid color with the valid options listed', async () => {
    const { isError, output } = await runTaskCapture({ prompt: 'go', description: 'do it', color: 'chartreuse' });
    assert.equal(isError, true);
    assert.match(output, /invalid color "chartreuse"/);
    assert.match(output, /Valid values:/);
  });

  it('advertises color in the input schema as an enum of known palette names', () => {
    const prop = (taskTool.inputSchema.properties as Record<string, { enum?: string[] }>).color;
    assert.ok(prop && Array.isArray(prop.enum) && prop.enum.includes('green'));
  });
});

// ---------------------------------------------------------------------------
// Gap: Proactive-agent affordance (AgentTool/prompt.ts:L270)
// ---------------------------------------------------------------------------
describe('task tool description — proactive-agent affordance', () => {
  it('tells the model to use a proactively-flagged agent without being asked', () => {
    const d = taskTool.description.toLowerCase();
    assert.ok(
      d.includes('proactively') && (d.includes('without') || d.includes("on your own")),
      'description must cue proactive use of agents flagged for it',
    );
  });
});

// ---------------------------------------------------------------------------
// Gap: O_NOFOLLOW symlink-guard on command-output file descriptors (utils/Shell.ts:L299-313)
// ---------------------------------------------------------------------------
describe('task output file — O_NOFOLLOW symlink guard', () => {
  let dir: string;
  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nofollow-')); });
  afterEach(() => { fs.rmSync(dir, { recursive: true, force: true }); });

  it('sets the O_NOFOLLOW flag on the output-file open flags', () => {
    assert.equal(OUTPUT_FILE_OPEN_FLAGS & fs.constants.O_NOFOLLOW, fs.constants.O_NOFOLLOW);
    // Start-from-empty semantics preserved (same as the old 'w' flag).
    assert.equal(OUTPUT_FILE_OPEN_FLAGS & fs.constants.O_TRUNC, fs.constants.O_TRUNC);
  });

  it('refuses to open a pre-planted symlink at the output path (ELOOP)', () => {
    const realTarget = path.join(dir, 'victim.txt');
    fs.writeFileSync(realTarget, 'original');
    const outputPath = path.join(dir, 'task.output');
    fs.symlinkSync(realTarget, outputPath);
    assert.throws(
      () => fs.closeSync(fs.openSync(outputPath, OUTPUT_FILE_OPEN_FLAGS)),
      (err: NodeJS.ErrnoException) => err.code === 'ELOOP',
    );
    // The guard means the attacker-chosen target was never truncated/overwritten.
    assert.equal(fs.readFileSync(realTarget, 'utf8'), 'original');
  });

  it('opens a normal (non-symlink) output path successfully', () => {
    const outputPath = path.join(dir, 'task.output');
    const fd = fs.openSync(outputPath, OUTPUT_FILE_OPEN_FLAGS);
    fs.writeSync(fd, 'hi');
    fs.closeSync(fd);
    assert.equal(fs.readFileSync(outputPath, 'utf8'), 'hi');
  });
});

// ---------------------------------------------------------------------------
// Gap: Omit the internal synthetic-output tool from the activity preview
// (tasks/LocalAgentTask/LocalAgentTask.tsx:L79-90)
// ---------------------------------------------------------------------------
describe('subagent activity relay — omit structured_output from the preview', () => {
  it('drops the structured_output tool_use_start', () => {
    const relayed = mapChildToSubagentEvent(
      { type: 'tool_use_start', toolName: 'structured_output', input: {} } as EngineEvent,
      'node-1', 1,
    );
    assert.equal(relayed, null);
  });

  it('drops the structured_output tool_use_result', () => {
    const relayed = mapChildToSubagentEvent(
      { type: 'tool_use_result', toolName: 'structured_output', isError: false } as EngineEvent,
      'node-1', 1,
    );
    assert.equal(relayed, null);
  });

  it('still relays a real tool call so the preview is not empty', () => {
    const relayed = mapChildToSubagentEvent(
      { type: 'tool_use_start', toolName: 'file_read', input: { file_path: 'src/x.ts' } } as EngineEvent,
      'node-1', 1,
    );
    assert.ok(relayed && relayed.type === 'subagent_progress');
    assert.match((relayed as { detail: string }).detail, /file_read/);
  });
});
