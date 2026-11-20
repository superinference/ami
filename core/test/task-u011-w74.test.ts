import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  filterToolsForAgent,
  filterToolsForAsyncAgent,
  AGENT_MODES,
  EXPLORE_READ_ONLY_TOOLS,
  COORDINATOR_MODE_ALLOWED_TOOLS,
  ASYNC_AGENT_ALLOWED_TOOLS,
} from '../src/tools/task';
import { isMcpTool, getToolCategory } from '../src/tools/tool-categories';
import type { ToolContext, ToolDefinition } from '../src/types';

const fakeTools = (names: string[]): ToolDefinition[] =>
  names.map(name => ({ name } as unknown as ToolDefinition));

type Ev = Record<string, unknown>;

/** Run the task tool with a fake engine, capturing the resolved tool names. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{ isError?: boolean; output: string; toolNames: string[] }> {
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    for (const e of events) yield e as never;
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

// ---------------------------------------------------------------------------
// gap U011: MCP tools unconditionally allowed for all agent modes (mcp__ bypass)
// ---------------------------------------------------------------------------
describe('isMcpTool — single MCP discriminator', () => {
  it('detects the mcp__ name prefix', () => {
    assert.equal(isMcpTool({ name: 'mcp__github__create_issue' }), true);
    assert.equal(isMcpTool({ name: 'mcp__srv__tool' }), true);
  });
  it('detects the dynamic isMcp flag even without the prefix', () => {
    assert.equal(isMcpTool({ name: 'weird_name', isMcp: true }), true);
  });
  it('is false for ordinary built-in tools', () => {
    assert.equal(isMcpTool({ name: 'bash' }), false);
    assert.equal(isMcpTool({ name: 'file_read' }), false);
  });
  it('getToolCategory still routes MCP tools to the mcp bucket (shared discriminator)', () => {
    assert.equal(getToolCategory({ name: 'mcp__srv__t' }), 'mcp');
    assert.equal(getToolCategory({ name: 'x', isMcp: true }), 'mcp');
  });
});

describe('filterToolsForAgent — MCP tools survive EVERY mode', () => {
  const withMcp = (extra: string[]) => fakeTools([...extra, 'mcp__srv__read', 'mcp__srv__write']);

  it('explore keeps read-only tools (incl. read-only bash) AND mcp tools, drops edit/task', () => {
    const resolved = filterToolsForAgent(withMcp(['file_read', 'bash', 'file_edit', 'task']), 'explore', 0, 5);
    const kept = resolved.map(t => t.name);
    assert.ok(kept.includes('file_read'), 'read-only kept');
    assert.ok(kept.includes('mcp__srv__read'), 'mcp read kept in explore');
    assert.ok(kept.includes('mcp__srv__write'), 'mcp write kept in explore (unconditional)');
    // Explore RETAINS bash (parity: the reference's read-only Explore agent) — but as the
    // read-only-guarded variant (isReadOnly pinned true), never the raw writable tool.
    assert.ok(kept.includes('bash'), 'read-only bash kept in explore');
    assert.equal(resolved.find(t => t.name === 'bash')!.isReadOnly, true, 'explore bash is read-only-guarded');
    for (const forbidden of ['file_edit', 'task']) {
      assert.ok(!kept.includes(forbidden), `${forbidden} dropped in explore`);
    }
  });

  it('coordinator keeps the orchestration allowlist AND mcp tools, drops file/shell', () => {
    const kept = filterToolsForAgent(withMcp(['task', 'send_message', 'file_edit', 'bash']), 'coordinator', 0, 5)
      .map(t => t.name);
    assert.ok(kept.includes('task') && kept.includes('send_message'), 'orchestration tools kept');
    assert.ok(kept.includes('mcp__srv__read') && kept.includes('mcp__srv__write'), 'mcp kept in coordinator');
    for (const forbidden of ['file_edit', 'bash']) {
      assert.ok(!kept.includes(forbidden), `${forbidden} dropped in coordinator`);
    }
  });

  it('general keeps mcp tools (and everything but plan_mode / over-depth task)', () => {
    const kept = filterToolsForAgent(withMcp(['bash', 'plan_mode']), 'general', 0, 5).map(t => t.name);
    assert.ok(kept.includes('mcp__srv__read') && kept.includes('mcp__srv__write'), 'mcp kept in general');
    assert.ok(kept.includes('bash'));
    assert.ok(!kept.includes('plan_mode'), 'plan_mode still stripped in general');
  });

  it('mcp bypass does not re-grant plan_mode-style exclusions (only mcp names pass)', () => {
    // An mcp-flagged plan_mode is a contrived impossibility, but confirm a plain
    // plan_mode (non-mcp) is still stripped in general despite the mcp branch.
    const kept = filterToolsForAgent(fakeTools(['plan_mode', 'file_read']), 'general', 0, 5).map(t => t.name);
    assert.ok(!kept.includes('plan_mode'));
  });
});

describe('filterToolsForAsyncAgent — MCP tools survive the background allowlist', () => {
  it('keeps the async allowlist AND mcp tools, still strips task/send_message', () => {
    const kept = filterToolsForAsyncAgent(
      fakeTools(['file_read', 'bash', 'task', 'send_message', 'mcp__srv__do']),
    ).map(t => t.name);
    assert.ok(kept.includes('file_read') && kept.includes('bash'), 'async-allowed tools kept');
    assert.ok(kept.includes('mcp__srv__do'), 'mcp tool kept on background path');
    assert.ok(!kept.includes('task'), 'task (recursion) still stripped from background');
    assert.ok(!kept.includes('send_message'), 'send_message still stripped from background');
  });
});

// ---------------------------------------------------------------------------
// gap U011: AGENT_MODES — discoverable source of truth for per-mode availability
// ---------------------------------------------------------------------------
describe('AGENT_MODES source of truth', () => {
  it('maps each mode to its canonical allowlist (reused by the filters, not duplicated)', () => {
    assert.equal(AGENT_MODES.explore.allow, EXPLORE_READ_ONLY_TOOLS);
    assert.equal(AGENT_MODES.coordinator.allow, COORDINATOR_MODE_ALLOWED_TOOLS);
    assert.equal(AGENT_MODES.async.allow, ASYNC_AGENT_ALLOWED_TOOLS);
    assert.equal(AGENT_MODES.general.allow, undefined, 'general = all tools');
  });

  it('encodes the depth/plan-mode policy per mode', () => {
    assert.equal(AGENT_MODES.general.stripsPlanMode, true);
    assert.equal(AGENT_MODES.general.depthGated, true);
    assert.equal(AGENT_MODES.coordinator.depthGated, true);
    assert.equal(AGENT_MODES.explore.depthGated, false);
    assert.equal(AGENT_MODES.explore.stripsPlanMode, false);
  });

  it('explore read-only set is the read/search/web class PLUS read-only bash (no edit/task)', () => {
    for (const t of ['file_read', 'grep', 'glob', 'list_dir', 'search_symbols', 'web_search', 'web_fetch', 'bash']) {
      assert.ok(EXPLORE_READ_ONLY_TOOLS.has(t), `${t} is in the explore pool`);
    }
    // bash is admitted but filterToolsForAgent swaps it for a read-only-guarded variant.
    for (const t of ['file_edit', 'file_write', 'task']) {
      assert.ok(!EXPLORE_READ_ONLY_TOOLS.has(t), `${t} is NOT in the explore pool`);
    }
  });

  it('an unknown mode falls back to the general (all-tools) spec', () => {
    const kept = filterToolsForAgent(fakeTools(['bash', 'file_read', 'plan_mode']), 'totally-unknown', 0, 5)
      .map(t => t.name);
    assert.ok(kept.includes('bash') && kept.includes('file_read'));
    assert.ok(!kept.includes('plan_mode'), 'unknown mode treated as general → plan_mode stripped');
  });
});

// ---------------------------------------------------------------------------
// end-to-end through execute(): the filter wiring governs the spawned pool
// ---------------------------------------------------------------------------
describe('task.execute wires filterToolsForAgent (behavior preserved)', () => {
  beforeEach(() => resetAgentRegistry());

  it('an explore spawn gets a read-only pool (read-only bash, no edit/task), proving the filter is wired', async () => {
    const { isError, toolNames } = await runTask({ prompt: 'look around', mode: 'explore' });
    assert.ok(!isError, 'explore spawn ok');
    assert.ok(toolNames.includes('file_read'), 'has read tool');
    // Explore retains bash for read-only shell ops (parity: the reference Explore agent).
    assert.ok(toolNames.includes('bash'), 'explore keeps read-only bash');
    for (const forbidden of ['file_edit', 'task']) {
      assert.ok(!toolNames.includes(forbidden), `explore must not get ${forbidden}`);
    }
  });
});
