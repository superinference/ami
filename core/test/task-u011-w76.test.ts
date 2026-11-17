import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  getToolSearchOrReadInfo,
  isReadOrSearchActivity,
  collapseReadSearchRun,
} from '../src/tools/tool-activity-class';
import {
  SUBAGENT_MODEL_OPTIONS,
  buildSubagentModelParamDescription,
  agentResultPath,
  taskTool,
} from '../src/tools/task';
import { taskOutputTool } from '../src/tools/task-output';
import { inheritBedrockRegionPrefix } from '../src/bedrock';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext, EngineConfig } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// ===========================================================================
// Gap U011/0 — per-activity read/search classification + collapse
// ===========================================================================
describe('tool-activity-class: getToolSearchOrReadInfo (native read/search classifier)', () => {
  it('classifies native search tools as isSearch', () => {
    for (const t of ['grep', 'glob', 'search_symbols', 'web_search']) {
      assert.deepEqual(getToolSearchOrReadInfo(t), { isSearch: true, isRead: false }, t);
    }
  });

  it('classifies native read tools as isRead', () => {
    for (const t of ['file_read', 'list_dir', 'notebook_read', 'web_fetch']) {
      assert.deepEqual(getToolSearchOrReadInfo(t), { isSearch: false, isRead: true }, t);
    }
  });

  it('classifies mutations/other tools as neither (never collapsed)', () => {
    for (const t of ['bash', 'file_edit', 'task', 'send_message', 'structured_output']) {
      assert.deepEqual(getToolSearchOrReadInfo(t), { isSearch: false, isRead: false }, t);
    }
  });

  it('delegates mcp__ tool names to the canonical MCP classifier (bare name)', () => {
    assert.equal(getToolSearchOrReadInfo('mcp__srv__get_thing').isRead, true);
    assert.equal(getToolSearchOrReadInfo('mcp__srv__search_tasks').isSearch, true);
    // A two-segment mcp name (no explicit tool part) still resolves via the tail.
    assert.equal(getToolSearchOrReadInfo('mcp__list_items').isRead, true);
    // An MCP mutation is neither search nor read.
    const w = getToolSearchOrReadInfo('mcp__srv__create_issue');
    assert.equal(w.isSearch, false);
    assert.equal(w.isRead, false);
  });

  it('isReadOrSearchActivity is the OR of the two flags', () => {
    assert.equal(isReadOrSearchActivity('grep'), true);
    assert.equal(isReadOrSearchActivity('file_read'), true);
    assert.equal(isReadOrSearchActivity('bash'), false);
  });
});

describe('tool-activity-class: collapseReadSearchRun', () => {
  it('returns undefined for empty / single-op runs (nothing to fold)', () => {
    assert.equal(collapseReadSearchRun([]), undefined);
    assert.equal(collapseReadSearchRun(['grep']), undefined);
    assert.equal(collapseReadSearchRun(['file_read']), undefined);
  });

  it('collapses a trailing run of >=2 consecutive read/search ops', () => {
    assert.equal(collapseReadSearchRun(['grep', 'glob']), 'Reading/searching (2 ops)');
    assert.equal(collapseReadSearchRun(['bash', 'grep', 'glob', 'file_read']), 'Reading/searching (3 ops)');
  });

  it('does not collapse when the most recent op is a mutation', () => {
    assert.equal(collapseReadSearchRun(['grep', 'glob', 'bash']), undefined);
    assert.equal(collapseReadSearchRun(['file_read', 'file_edit']), undefined);
  });

  it('counts only the trailing contiguous run, not earlier read/search ops', () => {
    // bash breaks the run; only the trailing grep,glob (2) count.
    assert.equal(collapseReadSearchRun(['grep', 'bash', 'grep', 'glob']), 'Reading/searching (2 ops)');
  });
});

describe('taskTool – background drain collapses consecutive read/search progress', () => {
  it('sets a collapsed progressSummary for a run of read/search tool uses', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'tool_use_start' as const, toolName: 'grep', input: { pattern: 'foo' } };
      yield { type: 'tool_use_start' as const, toolName: 'glob', input: { pattern: '*.ts' } };
      yield { type: 'tool_use_start' as const, toolName: 'file_read', input: { file_path: '/a/b.ts' } };
      yield { type: 'text_delta' as const, text: 'done' };
    }
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011-collapse-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await taskTool.execute(
        { prompt: 'read a bunch', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );
      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);
      await new Promise(r => setTimeout(r, 150));
      const entry: any = pm.list().find(e => e.taskId === taskId);
      assert.ok(entry);
      assert.equal(entry.progressSummary, 'Reading/searching (3 ops)');
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('keeps the normal per-tool summary for a single read op (no collapse)', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'tool_use_start' as const, toolName: 'file_read', input: { file_path: '/only/one.ts' } };
      yield { type: 'text_delta' as const, text: 'done' };
    }
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011-single-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await taskTool.execute(
        { prompt: 'read one', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );
      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      await new Promise(r => setTimeout(r, 150));
      const entry: any = pm.list().find(e => e.taskId === taskId);
      assert.ok(entry);
      // A single read op is not collapsed; it shows the human-readable activity
      // phrase resolved from the tool (parity: getActivityDescription) instead of
      // the raw `tool:<name>` label.
      assert.equal(entry.progressSummary, 'Reading /only/one.ts', entry.progressSummary);
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ===========================================================================
// Gap U011/16 — curated selectable subagent model options + enriched description
// ===========================================================================
describe('subagent model options (gap 16)', () => {
  it('exposes an inherit choice plus the opus/sonnet/haiku tiers', () => {
    const values = SUBAGENT_MODEL_OPTIONS.map(o => o.value);
    assert.ok(values.includes('')); // omit = inherit default
    assert.ok(values.includes('inherit'));
    assert.ok(values.includes('opus'));
    assert.ok(values.includes('sonnet'));
    assert.ok(values.includes('haiku'));
    for (const o of SUBAGENT_MODEL_OPTIONS) {
      assert.ok(o.label.length > 0 && o.description.length > 0);
    }
  });

  it('builds a description that leads with inherit-by-default + per-tier guidance', () => {
    const d = buildSubagentModelParamDescription();
    assert.match(d, /INHERIT the parent/);
    assert.match(d, /"opus"/);
    assert.match(d, /"sonnet"/);
    assert.match(d, /"haiku"/);
    // AMI keeps accepting arbitrary full names / provider aliases.
    assert.match(d, /gpt-4o/);
  });

  it('is WIRED into the advertised task tool model param description', () => {
    assert.equal(
      taskTool.inputSchema.properties!.model.description,
      buildSubagentModelParamDescription(),
    );
  });
});

// ===========================================================================
// Gap U011/15 — Bedrock cross-region inference prefix inheritance for subagents
// ===========================================================================
describe('inheritBedrockRegionPrefix (gap 15)', () => {
  it('inherits the parent prefix when the subagent model has none', () => {
    assert.equal(
      inheritBedrockRegionPrefix('anthropic.claude-haiku-4-5-20251001-v1:0', 'us.anthropic.claude-opus-4-6-v1:0'),
      'us.anthropic.claude-haiku-4-5-20251001-v1:0',
    );
  });

  it('keeps the subagent own prefix when it already pins a region', () => {
    assert.equal(
      inheritBedrockRegionPrefix('eu.anthropic.claude-haiku-4-5-v1:0', 'us.anthropic.claude-opus-4-6-v1:0'),
      'eu.anthropic.claude-haiku-4-5-v1:0',
    );
  });

  it('is a no-op when the parent carries no region prefix', () => {
    assert.equal(
      inheritBedrockRegionPrefix('anthropic.claude-haiku-4-5-v1:0', 'anthropic.claude-opus-4-6-v1:0'),
      'anthropic.claude-haiku-4-5-v1:0',
    );
    assert.equal(
      inheritBedrockRegionPrefix('anthropic.claude-haiku-4-5-v1:0', undefined),
      'anthropic.claude-haiku-4-5-v1:0',
    );
  });

  it('returns an empty subagent model unchanged', () => {
    assert.equal(inheritBedrockRegionPrefix('', 'us.anthropic.claude-opus-4-6-v1:0'), '');
  });
});

describe('taskTool – threads inherited Bedrock region prefix into the subagent config', () => {
  async function* oneLine() {
    yield { type: 'text_delta' as const, text: 'ok' };
  }

  it('applies the parent region prefix for an amazon-bedrock subagent model', async () => {
    let captured: EngineConfig | undefined;
    await taskTool.execute(
      { prompt: 'do a thing', model: 'anthropic.claude-haiku-4-5-20251001-v1:0' },
      ctx({
        _providerConfig: {
          baseUrl: 'http://localhost', apiKey: 'k',
          model: 'us.anthropic.claude-opus-4-6-v1:0', provider: 'amazon-bedrock',
        },
        _engineFactory: (cfg: EngineConfig) => { captured = cfg; return { submit: oneLine }; },
      }),
    );
    assert.ok(captured);
    assert.equal(captured!.provider.model, 'us.anthropic.claude-haiku-4-5-20251001-v1:0');
  });

  it('does NOT rewrite the model for a non-bedrock provider', async () => {
    let captured: EngineConfig | undefined;
    await taskTool.execute(
      { prompt: 'do a thing', model: 'anthropic.claude-haiku-4-5-20251001-v1:0' },
      ctx({
        _providerConfig: {
          baseUrl: 'http://localhost', apiKey: 'k',
          model: 'us.anthropic.claude-opus-4-6-v1:0', provider: 'anthropic',
        },
        _engineFactory: (cfg: EngineConfig) => { captured = cfg; return { submit: oneLine }; },
      }),
    );
    assert.ok(captured);
    assert.equal(captured!.provider.model, 'anthropic.claude-haiku-4-5-20251001-v1:0');
  });
});

// ===========================================================================
// Gap U011/9 — clean in-memory final answer preferred over raw transcript
// ===========================================================================
describe('agentResultPath (gap 9)', () => {
  it('derives the .result sidecar from the .output transcript path', () => {
    assert.equal(agentResultPath('/tmp/x/agent-abc.output'), '/tmp/x/agent-abc.result');
    // No .output suffix → unchanged (defensive).
    assert.equal(agentResultPath('/tmp/x/agent-abc.log'), '/tmp/x/agent-abc.log');
  });
});

describe('taskTool + task_output – clean final answer round-trip (gap 9)', () => {
  it('writes the clean final turn to .result and task_output prefers it over the transcript', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'noisy streamed transcript chatter line\n' };
      yield { type: 'tool_use_start' as const, toolName: 'grep', input: { pattern: 'x' } };
      yield { type: 'turn_complete' as const, content: 'THE CLEAN FINAL ANSWER', toolCalls: [] };
    }
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011-result-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const spawn = await taskTool.execute(
        { prompt: 'background work', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );
      const taskId = spawn.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);
      // Wait for the detached drain to finish and write the sidecar.
      await new Promise(r => setTimeout(r, 200));

      const entry: any = pm.get!(taskId);
      assert.ok(entry?.outputPath);
      const sidecar = agentResultPath(entry.outputPath);
      assert.ok(fs.existsSync(sidecar), 'drain must write the .result sidecar');
      assert.equal(fs.readFileSync(sidecar, 'utf-8'), 'THE CLEAN FINAL ANSWER');

      const out = await taskOutputTool.execute(
        { task_id: taskId, wait: true },
        ctx({ cwd: tmpDir, processManager: pm }),
      );
      assert.ok(!out.isError);
      assert.match(out.output, /--- Final result ---/);
      assert.match(out.output, /THE CLEAN FINAL ANSWER/);
      // The noisy transcript line must NOT be the returned deliverable.
      assert.doesNotMatch(out.output, /noisy streamed transcript chatter/);
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('falls back to the transcript tail when no .result sidecar exists (bash task)', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u011-nosidecar-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const taskId = pm.spawn('echo plain_bash_output', { cwd: tmpDir });
      await new Promise(r => setTimeout(r, 400));
      const out = await taskOutputTool.execute(
        { task_id: taskId, wait: true },
        ctx({ cwd: tmpDir, processManager: pm }),
      );
      assert.ok(!out.isError);
      assert.doesNotMatch(out.output, /--- Final result ---/);
      assert.match(out.output, /plain_bash_output/);
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
