import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  taskTool,
  resetAgentRegistry,
  filterTeammateOnlyTools,
  IN_PROCESS_TEAMMATE_ONLY_TOOLS,
} from '../src/tools/task';
import { restoreTeam, resetTeam } from '../src/tools/team';
import type { ToolContext, ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// U011 (w100) gap: in-process teammate-only tool gating.
// Parity with the reference's IN_PROCESS_TEAMMATE_ALLOWED_TOOLS — task CRUD, send_message,
// and the scheduler are injected ONLY for in-process teammates (isInProcessTeammate),
// never handed to a plain general/agent-def sub-agent. AMI previously exposed them to
// every sub-agent via the one default registry.
// ---------------------------------------------------------------------------

/** Run the task tool with a fake engine, capturing the child's resolved tool names. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
): Promise<{ isError?: boolean; output: string; toolNames: string[] }> {
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    yield { type: 'text_delta', text: 'ok' } as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => {
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, toolNames };
}

const fakeTools = (names: string[]): ToolDefinition[] =>
  names.map(name => ({ name } as unknown as ToolDefinition));

const TEAMMATE_ONLY = [
  'send_message', 'task_list', 'task_tracker',
  'team_create', 'team_delete',
  'cron_create', 'cron_delete', 'cron_list', 'cron_get', 'cron_run',
  'schedule_wakeup',
];

describe('IN_PROCESS_TEAMMATE_ONLY_TOOLS constant', () => {
  it('reserves task-CRUD + messaging + team + scheduling tools', () => {
    for (const name of TEAMMATE_ONLY) {
      assert.ok(IN_PROCESS_TEAMMATE_ONLY_TOOLS.has(name), `${name} reserved`);
    }
  });
  it('does NOT reserve ordinary worker tools', () => {
    for (const name of ['file_read', 'bash', 'grep', 'file_edit', 'task', 'structured_output']) {
      assert.ok(!IN_PROCESS_TEAMMATE_ONLY_TOOLS.has(name), `${name} not reserved`);
    }
  });
});

describe('filterTeammateOnlyTools', () => {
  it('strips the reserved tools for a non-teammate spawn', () => {
    const all = fakeTools([...TEAMMATE_ONLY, 'file_read', 'bash', 'task']);
    const kept = filterTeammateOnlyTools(all, false).map(t => t.name);
    assert.deepEqual(kept.sort(), ['bash', 'file_read', 'task']);
  });
  it('keeps everything (same array identity) for an in-process teammate', () => {
    const all = fakeTools([...TEAMMATE_ONLY, 'file_read']);
    const kept = filterTeammateOnlyTools(all, true);
    assert.equal(kept, all, 'teammate path is a no-op passthrough');
  });
});

describe('task tool execute — teammate-only gating', () => {
  beforeEach(() => {
    resetAgentRegistry();
    resetTeam();
  });
  afterEach(() => resetTeam());

  it('a solo general sub-agent does NOT inherit coordination/scheduling tools', async () => {
    const { toolNames } = await runTask({ description: 'd', prompt: 'p', mode: 'general' });
    for (const name of TEAMMATE_ONLY) {
      assert.ok(!toolNames.includes(name), `${name} must be gated out of a general sub-agent`);
    }
    // it still has ordinary worker tools so it can act
    assert.ok(toolNames.includes('file_read'));
    assert.ok(toolNames.includes('bash'));
  });

  it('an unnamed sub-agent inside an active team is still NOT a teammate (gated)', async () => {
    const teamDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u011-gate-'));
    try {
      restoreTeam({ name: 'alpha', leadAgentId: 'lead-1', members: [], createdAt: new Date().toISOString() }, teamDir);
      const { toolNames } = await runTask({ description: 'd', prompt: 'p', mode: 'general' });
      assert.ok(!toolNames.includes('send_message'), 'unnamed spawn is not a teammate');
      assert.ok(!toolNames.includes('task_list'));
    } finally {
      try { fs.rmSync(teamDir, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  it('a NAMED teammate spawned into an active team KEEPS the coordination tools', async () => {
    const teamDir = fs.mkdtempSync(path.join(os.tmpdir(), 'u011-gate-'));
    try {
      restoreTeam({ name: 'alpha', leadAgentId: 'lead-1', members: [], createdAt: new Date().toISOString() }, teamDir);
      const { toolNames } = await runTask({ description: 'd', prompt: 'p', mode: 'general', name: 'researcher' });
      assert.ok(toolNames.includes('send_message'), 'teammate keeps send_message');
      assert.ok(toolNames.includes('task_list'), 'teammate keeps the shared task list');
      assert.ok(toolNames.includes('task_tracker'), 'teammate keeps task_tracker');
    } finally {
      try { fs.rmSync(teamDir, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  it('coordinator mode is exempt — it retains send_message via its own allowlist', async () => {
    const { toolNames } = await runTask({ description: 'd', prompt: 'p', mode: 'coordinator' });
    assert.ok(toolNames.includes('send_message'), 'coordinator keeps send_message');
    // but a coordinator never had the shared task list / scheduler in the first place
    assert.ok(!toolNames.includes('task_list'));
    assert.ok(!toolNames.includes('cron_create'));
  });
});
