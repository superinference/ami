// U011 / w104 — stable caller-assigned `agent_id` distinct from the display `name`
// (parity: Claude Code's teammate identity separating a stable machine `agentId`
// from the human `agentName`, utils/teammate.ts:L44-45). AMI previously exposed only
// a `name` (both display label AND send_message/resume address); the taskId — the
// durable machine id that keys the output file, file-cache, and process-manager
// entry — was always randomly generated and could not be pinned by the caller. This
// adds an optional `agent_id` spawn field: when supplied (and path-safe) it becomes
// the taskId, so a caller can address/resume/collect the agent under a predictable id
// while the display `name` diverges freely.
//
// RED on revert: drop the `requestedAgentId` argument from the resolveSpawnTaskId
// call (so the taskId is always generated) and the "agent_id becomes the started
// taskId" integration test fails — the started nodeId is a random `agent-…` id, not
// the caller's stable id.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  taskTool,
  resetAgentRegistry,
  isSafeAgentId,
  resolveSpawnTaskId,
  AGENT_ID_MAX_LENGTH,
  type ResumeResolution,
} from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

let tmpDir: string;
let pm: ProcessManager | null = null;

beforeEach(() => {
  resetAgentRegistry();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-agentid-'));
});

afterEach(() => {
  pm?.cleanup();
  pm = null;
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

function waitUntil(pred: () => boolean, timeoutMs = 4000): Promise<void> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = (): void => {
      if (pred()) return resolve();
      if (Date.now() - started > timeoutMs) return reject(new Error('timeout waiting for condition'));
      setTimeout(tick, 10);
    };
    tick();
  });
}

interface Harness {
  ctx: ToolContext;
  getStartedTaskId: () => string | undefined;
}

// A mock sub-engine that completes immediately (one text-bearing turn, then the
// generator ends) so the foreground run resolves without a real provider.
function makeHarness(): Harness {
  const agentDef = { name: 'id_agent', systemPrompt: 'be an agent', tools: ['file_read'] };
  let startedTaskId: string | undefined;

  const engineFactory = (): { submit: (p: string) => AsyncGenerator<unknown> } => ({
    submit: async function* () {
      yield { type: 'turn_complete', content: 'done' };
    },
  });

  const ctx = {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    processManager: pm!,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'm' },
    _skillManager: { getAgent: () => agentDef, listAgents: () => [agentDef] },
    _engineFactory: engineFactory,
    _emitChild: (ev: { type: string; nodeId?: string }) => {
      if (ev.type === 'subagent_start') startedTaskId = ev.nodeId;
    },
  } as unknown as ToolContext;

  return { ctx, getStartedTaskId: () => startedTaskId };
}

describe('task agent_id — isSafeAgentId (U011/w104)', () => {
  it('accepts a plain alphanumeric-plus-separators id', () => {
    assert.equal(isSafeAgentId('worker-7'), true);
    assert.equal(isSafeAgentId('build.worker_2'), true);
    assert.equal(isSafeAgentId('a'), true);
    assert.equal(isSafeAgentId('A1'), true);
  });

  it('rejects path separators, traversal, empties, leading separators, and over-length', () => {
    assert.equal(isSafeAgentId(''), false);
    assert.equal(isSafeAgentId('../etc/passwd'), false);
    assert.equal(isSafeAgentId('a/b'), false);
    assert.equal(isSafeAgentId('a\\b'), false);
    assert.equal(isSafeAgentId('a..b'), false, 'embedded traversal segment rejected');
    assert.equal(isSafeAgentId('-leading'), false, 'must start with an alphanumeric');
    assert.equal(isSafeAgentId('.hidden'), false);
    assert.equal(isSafeAgentId('has space'), false);
    assert.equal(isSafeAgentId('a'.repeat(AGENT_ID_MAX_LENGTH)), true, 'exactly max is allowed');
    assert.equal(isSafeAgentId('a'.repeat(AGENT_ID_MAX_LENGTH + 1)), false, 'over max rejected');
  });
});

describe('task agent_id — resolveSpawnTaskId (U011/w104)', () => {
  it('uses a valid caller agent_id for a FRESH spawn', () => {
    assert.equal(resolveSpawnTaskId(undefined, 'worker-7'), 'worker-7');
  });

  it('falls back to a generated id when agent_id is absent or unsafe', () => {
    assert.match(resolveSpawnTaskId(undefined, undefined), /^agent-[0-9a-f]{16}$/);
    assert.match(resolveSpawnTaskId(undefined, '../bad'), /^agent-[0-9a-f]{16}$/);
  });

  it('RESUME always wins over a freshly-supplied agent_id (keeps the original id)', () => {
    const resume: ResumeResolution = { sessionId: 's1', resolvedId: 'agent-original' };
    assert.equal(resolveSpawnTaskId(resume, 'worker-7'), 'agent-original');
  });
});

describe('task agent_id — tool schema (U011/w104)', () => {
  it('advertises an optional string agent_id distinct from name', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string; description?: string }>;
    assert.ok(props.agent_id, 'agent_id property present');
    assert.equal(props.agent_id.type, 'string');
    assert.ok(/stable/i.test(props.agent_id.description ?? ''), 'description mentions stability');
    // name stays its own field (display/address), proving the two identities diverge.
    assert.ok(props.name, 'name property still present');
    assert.notEqual(props.agent_id.description, props.name.description);
  });
});

describe('task agent_id — execute wiring (U011/w104)', () => {
  it('a valid agent_id becomes the spawned taskId (red-green)', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'id_agent', agent_id: 'my-stable-worker' },
      h.ctx,
    );
    await waitUntil(() => !!h.getStartedTaskId());
    assert.equal(h.getStartedTaskId(), 'my-stable-worker',
      'the caller-supplied stable agent_id must be used verbatim as the taskId');
  });

  it('with NO agent_id the taskId is a generated random id', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();
    await taskTool.execute({ prompt: 'do the work', subagent_type: 'id_agent' }, h.ctx);
    await waitUntil(() => !!h.getStartedTaskId());
    assert.match(h.getStartedTaskId()!, /^agent-[0-9a-f]{16}$/);
  });

  it('rejects an unsafe agent_id with a clear error before spawning', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();
    const result = await taskTool.execute(
      { prompt: 'x', subagent_type: 'id_agent', agent_id: '../escape' },
      h.ctx,
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('invalid agent_id'), 'error names the bad field');
    assert.equal(h.getStartedTaskId(), undefined, 'no agent was spawned');
  });

  it('rejects an agent_id that collides with a still-running agent', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();
    // Register a live agent under the id the new spawn will try to claim.
    pm.registerAgent('taken-id', {
      command: 'task',
      description: 'running',
      outputPath: path.join(tmpDir, 'taken-id.output'),
      abortFn: () => {},
    });
    const result = await taskTool.execute(
      { prompt: 'x', subagent_type: 'id_agent', agent_id: 'taken-id' },
      h.ctx,
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('already in use'), 'collision is reported');
    assert.equal(h.getStartedTaskId(), undefined, 'no second agent spawned under the taken id');
  });

  it('allows reusing an agent_id whose prior holder has TERMINATED', async () => {
    pm = new ProcessManager(tmpDir);
    const h = makeHarness();
    pm.registerAgent('reusable-id', {
      command: 'task',
      description: 'old',
      outputPath: path.join(tmpDir, 'reusable-id.output'),
      abortFn: () => {},
    });
    // Drive the prior holder to a terminal state, then it is free to reclaim.
    pm.kill('reusable-id');
    await taskTool.execute(
      { prompt: 'do the work', subagent_type: 'id_agent', agent_id: 'reusable-id' },
      h.ctx,
    );
    await waitUntil(() => !!h.getStartedTaskId());
    assert.equal(h.getStartedTaskId(), 'reusable-id',
      'a terminated holder does not block reusing its id');
  });
});
