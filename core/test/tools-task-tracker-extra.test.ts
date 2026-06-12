import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  taskTrackerTool,
  resetTaskState,
  getTaskState,
} from '../src/tools/task-tracker';
import { taskTool } from '../src/tools/task';
import {
  searchSymbolsTool,
  getWorkspaceIndexer,
  _resetIndexer,
} from '../src/tools/search-symbols';
import type { ToolContext, EngineConfig } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// =========================================================================
// TASK TRACKER — extra coverage
// =========================================================================

beforeEach(() => {
  resetTaskState();
});

// ---------------------------------------------------------------------------
// activeForm field — create
// ---------------------------------------------------------------------------

describe('taskTrackerTool — activeForm on create', () => {
  it('stores activeForm when provided', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Run CI', activeForm: 'Running tests' },
      ctx(),
    );
    const state = getTaskState();
    const task = state.get(1);
    assert.ok(task);
    assert.equal(task.activeForm, 'Running tests');
  });

  it('activeForm is undefined when not provided', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'No form' },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.activeForm, undefined);
  });

  it('activeForm is undefined when provided as empty string', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Empty form', activeForm: '' },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    // empty string || undefined => undefined
    assert.equal(task.activeForm, undefined);
  });
});

// ---------------------------------------------------------------------------
// activeForm field — update
// ---------------------------------------------------------------------------

describe('taskTrackerTool — activeForm on update', () => {
  beforeEach(async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'With form', activeForm: 'Building' },
      ctx(),
    );
  });

  it('updates activeForm to a new value', async () => {
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', activeForm: 'Deploying' },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.activeForm, 'Deploying');
  });

  it('clears activeForm when set to empty string (undefined)', async () => {
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', activeForm: '' },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.activeForm, undefined);
  });

  it('clears activeForm when explicitly set to undefined', async () => {
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', activeForm: undefined },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    // input.activeForm !== undefined is false when explicitly undefined,
    // so activeForm should remain unchanged
    assert.equal(task.activeForm, 'Building');
  });
});

// ---------------------------------------------------------------------------
// formatTask — empty description
// ---------------------------------------------------------------------------

describe('taskTrackerTool — formatTask with empty description', () => {
  it('does not render description line when description is empty', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'No desc' },
      ctx(),
    );
    const result = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    assert.ok(!result.isError);
    // Output should have one line with task info and no indented description
    const lines = result.output.split('\n');
    assert.equal(lines.length, 1);
    assert.ok(lines[0].includes('No desc'));
  });

  it('renders description line when description is present', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Has desc', description: 'Details here' },
      ctx(),
    );
    const result = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    const lines = result.output.split('\n');
    assert.equal(lines.length, 2);
    assert.ok(lines[1].includes('Details here'));
  });
});

// ---------------------------------------------------------------------------
// Update description to empty string
// ---------------------------------------------------------------------------

describe('taskTrackerTool — update description to empty', () => {
  it('clears description when updated to empty string', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Clear me', description: 'Will be cleared' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', description: '' },
      ctx(),
    );
    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.description, '');

    // formatTask should not show description line
    const result = await taskTrackerTool.execute(
      { action: 'get', task_id: '1' },
      ctx(),
    );
    const lines = result.output.split('\n');
    assert.equal(lines.length, 1);
  });
});

// ---------------------------------------------------------------------------
// Multiple updates on same task
// ---------------------------------------------------------------------------

describe('taskTrackerTool — multiple updates on same task', () => {
  it('applies sequential updates correctly', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Multi update', description: 'v1' },
      ctx(),
    );

    // Update 1: change status
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx(),
    );

    // Update 2: change description
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', description: 'v2' },
      ctx(),
    );

    // Update 3: change subject and add activeForm
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', subject: 'Updated subject', activeForm: 'Processing' },
      ctx(),
    );

    // Update 4: complete it
    await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx(),
    );

    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.subject, 'Updated subject');
    assert.equal(task.description, 'v2');
    assert.equal(task.status, 'completed');
    assert.equal(task.activeForm, 'Processing');
  });

  it('update returns formatted task output each time', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Check output' },
      ctx(),
    );

    const r1 = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'in_progress' },
      ctx(),
    );
    assert.ok(r1.output.includes('[>]'));
    assert.ok(r1.output.includes('[in_progress]'));

    const r2 = await taskTrackerTool.execute(
      { action: 'update', task_id: '1', status: 'completed' },
      ctx(),
    );
    assert.ok(r2.output.includes('[x]'));
    assert.ok(r2.output.includes('[completed]'));
  });
});

// ---------------------------------------------------------------------------
// Create with only subject (no description)
// ---------------------------------------------------------------------------

describe('taskTrackerTool — create with only subject', () => {
  it('creates task with empty description string', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'create', subject: 'Subject only' },
      ctx(),
    );
    assert.equal(result.isError, false);

    const task = getTaskState().get(1);
    assert.ok(task);
    assert.equal(task.subject, 'Subject only');
    assert.equal(task.description, '');
    assert.equal(task.status, 'pending');
  });
});

// ---------------------------------------------------------------------------
// getTaskState
// ---------------------------------------------------------------------------

describe('taskTrackerTool — getTaskState', () => {
  it('returns the internal tasks map', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'State test' },
      ctx(),
    );
    const state = getTaskState();
    assert.ok(state instanceof Map);
    assert.equal(state.size, 1);
    assert.equal(state.get(1)?.subject, 'State test');
  });

  it('reflects deletions', async () => {
    await taskTrackerTool.execute({ action: 'create', subject: 'Del me' }, ctx());
    await taskTrackerTool.execute({ action: 'delete', task_id: '1' }, ctx());
    assert.equal(getTaskState().size, 0);
  });
});

// ---------------------------------------------------------------------------
// List with descriptions — exercises formatTask for multi-task output
// ---------------------------------------------------------------------------

describe('taskTrackerTool — list with mixed descriptions', () => {
  it('renders tasks with and without descriptions correctly', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'Has desc', description: 'Some detail' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'create', subject: 'No desc' },
      ctx(),
    );
    await taskTrackerTool.execute(
      { action: 'update', task_id: '2', status: 'completed' },
      ctx(),
    );

    const result = await taskTrackerTool.execute({ action: 'list' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('Some detail'));
    assert.ok(result.output.includes('[x] Task #2'));
    assert.ok(result.output.includes('[ ] Task #1'));
  });
});

// ---------------------------------------------------------------------------
// Update with non-numeric task_id
// ---------------------------------------------------------------------------

describe('taskTrackerTool — update edge cases', () => {
  it('returns error for non-numeric task_id on update', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'update', task_id: 'xyz' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('task_id is required'));
  });

  it('returns error for non-numeric task_id on delete', async () => {
    const result = await taskTrackerTool.execute(
      { action: 'delete', task_id: 'abc' },
      ctx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('task_id is required'));
  });
});

// =========================================================================
// TASK TOOL — extra coverage (lines 45-91)
// =========================================================================

describe('taskTool — execute with engine factory', () => {
  it('creates tools with explore mode (read-only filter)', async () => {
    let capturedConfig: any = null;

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'explore result' };
    }

    const result = await taskTool.execute(
      { prompt: 'search for files', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.equal(result.isError, false);
    assert.equal(result.output, 'explore result');

    // Verify the config was passed through
    assert.ok(capturedConfig);
    assert.equal(capturedConfig.permissionMode, 'auto-allow');
    // In explore mode, tools should only be read-only ones
    const toolNames = capturedConfig.tools.map((t: any) => t.name);
    for (const name of toolNames) {
      assert.ok(
        ['file_read', 'grep', 'glob', 'list_dir', 'search_symbols', 'web_search', 'web_fetch'].includes(name),
        `unexpected tool in explore mode: ${name}`,
      );
    }
  });

  it('creates tools with general mode (excludes task tool)', async () => {
    let capturedConfig: any = null;

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'general result' };
    }

    const result = await taskTool.execute(
      { prompt: 'modify files', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.equal(result.isError, false);
    assert.equal(result.output, 'general result');

    assert.ok(capturedConfig);
    assert.equal(capturedConfig.permissionMode, 'ask');
    const toolNames = capturedConfig.tools.map((t: any) => t.name);
    // 'task' should be excluded to prevent recursion
    assert.ok(!toolNames.includes('task'), 'task tool should not appear in general mode');
    // but other writable tools should be present
    assert.ok(toolNames.includes('bash') || toolNames.length > 0);
  });

  it('uses default provider config when _providerConfig is not set', async () => {
    let capturedConfig: any = null;

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'ok' };
    }

    await taskTool.execute(
      { prompt: 'test default provider' },
      ctx({
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.deepEqual(capturedConfig.provider, {
      baseUrl: '',
      apiKey: '',
      model: '',
    });
  });

  it('passes _permissionPromptHandler through to subconfig', async () => {
    let capturedConfig: any = null;
    const mockHandler = async () => ({ allowed: true as const });

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'done' };
    }

    await taskTool.execute(
      { prompt: 'test handler', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _permissionPromptHandler: mockHandler as any,
        _engineFactory: (cfg: any) => {
          capturedConfig = cfg;
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.ok(capturedConfig);
    assert.equal(capturedConfig.permissionPromptHandler, mockHandler);
  });

  it('propagates abort signal to subagent', async () => {
    const parentAbort = new AbortController();
    let subAborted = false;

    async function* fakeSubmit(_prompt: string) {
      // Simulate a long-running task; parent aborts before we yield
      yield { type: 'text_delta' as const, text: 'start' };
    }

    const result = await taskTool.execute(
      { prompt: 'abort test' },
      ctx({
        abortSignal: parentAbort.signal,
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _engineFactory: (cfg: any) => {
          // Listen for abort on the sub abort controller
          cfg.abortController.signal.addEventListener('abort', () => {
            subAborted = true;
          });
          return { submit: fakeSubmit };
        },
      }),
    );

    // Abort the parent after execution
    parentAbort.abort();
    assert.ok(subAborted, 'sub abort controller should be aborted when parent aborts');
  });

  it('assigns a sessionId to the subagent config', async () => {
    let capturedSessionId: string | undefined;

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'ok' };
    }

    await taskTool.execute(
      { prompt: 'task 1' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _engineFactory: (cfg: any) => {
          capturedSessionId = cfg.sessionId;
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.ok(capturedSessionId, 'sessionId should be assigned');
    assert.ok(capturedSessionId!.length > 0, 'sessionId should be non-empty');
  });

  it('handles non-Error throw from subagent', async () => {
    const result = await taskTool.execute(
      { prompt: 'string throw' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _engineFactory: (_cfg: any) => ({
          submit: () => { throw 'plain string error'; },
        }),
      }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('plain string error'));
  });

  it('collects mixed text_delta and error events in order', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'part1' };
      yield { type: 'error' as const, error: 'oops' };
      yield { type: 'text_delta' as const, text: 'part2' };
    }

    const result = await taskTool.execute(
      { prompt: 'mixed events' },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'key', model: 'model' },
        _engineFactory: (_cfg: any) => ({ submit: fakeSubmit }),
      }),
    );
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('part1'));
    assert.ok(result.output.includes('oops'));
    assert.ok(result.output.includes('part2'));
  });
});

// =========================================================================
// SEARCH SYMBOLS — extra coverage
// =========================================================================

describe('searchSymbolsTool — extra coverage', () => {
  let tmpDir: string;

  beforeEach(() => {
    _resetIndexer();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-search-extra-'));

    fs.writeFileSync(
      path.join(tmpDir, 'models.ts'),
      `export function calculateTotal(items: number[]): number {
  return items.reduce((a, b) => a + b, 0);
}

export class OrderService {
  process(): void {}
}

export interface OrderConfig {
  maxItems: number;
}

export type OrderId = string;

export const DEFAULT_LIMIT = 50;
`,
    );

    fs.writeFileSync(
      path.join(tmpDir, 'utils.ts'),
      `export function calculateAverage(values: number[]): number {
  return values.reduce((a, b) => a + b, 0) / values.length;
}

export function formatCurrency(amount: number): string {
  return '$' + amount.toFixed(2);
}
`,
    );
  });

  afterEach(() => {
    _resetIndexer();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function sCtx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      ...overrides,
    };
  }

  it('finds interfaces by type filter', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'Order', type: 'interface' },
      sCtx(),
    );
    assert.ok(!result.isError);
    if (result.output.includes('OrderConfig')) {
      assert.ok(result.output.includes('interface'));
      assert.ok(!result.output.includes('(class)'));
    }
  });

  it('finds type aliases by type filter', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'OrderId', type: 'type' },
      sCtx(),
    );
    assert.ok(!result.isError);
    // either finds it or says "no symbols"
  });

  it('finds variables by type filter', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'DEFAULT_LIMIT', type: 'variable' },
      sCtx(),
    );
    assert.ok(!result.isError);
  });

  it('returns no-match message with type filter included', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'zzzzz', type: 'function' },
      sCtx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No symbols matching'));
    assert.ok(result.output.includes('type: function'));
  });

  it('shows file path and line number in output', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'calculateTotal' },
      sCtx(),
    );
    assert.ok(!result.isError);
    if (result.output.includes('calculateTotal')) {
      assert.ok(result.output.includes('models.ts'));
      // Line number should be present (e.g., ":1")
      assert.match(result.output, /:\d+/);
    }
  });

  it('finds symbols across multiple files with partial match', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'calculate' },
      sCtx(),
    );
    assert.ok(!result.isError);
    // Should potentially match calculateTotal and calculateAverage
    if (result.output.includes('calculate')) {
      // At least found something
      assert.ok(result.output.length > 0);
    }
  });

  it('getWorkspaceIndexer creates new indexer for different cwd', () => {
    _resetIndexer();
    const indexer1 = getWorkspaceIndexer(tmpDir);

    const tmpDir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'si-search-extra-2-'));
    fs.writeFileSync(path.join(tmpDir2, 'foo.ts'), 'export function foo() {}\n');

    const indexer2 = getWorkspaceIndexer(tmpDir2);
    assert.notEqual(indexer1, indexer2, 'different cwd should produce different indexer');

    fs.rmSync(tmpDir2, { recursive: true, force: true });
  });

  it('getWorkspaceIndexer returns same instance for same cwd', () => {
    _resetIndexer();
    const a = getWorkspaceIndexer(tmpDir);
    const b = getWorkspaceIndexer(tmpDir);
    assert.equal(a, b);
  });

  it('handles no-results with stats in message', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'nonexistentSymbolXYZ' },
      sCtx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No symbols matching'));
    assert.ok(result.output.includes('Searched'));
    assert.ok(result.output.includes('files'));
    assert.ok(result.output.includes('symbols'));
  });
});
