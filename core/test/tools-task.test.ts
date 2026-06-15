import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { taskTool } from '../src/tools/task';
import type { ToolContext } from '../src/types';
import { ProcessManager } from '../src/process-manager';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('taskTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(taskTool.name, 'task');
  });

  it('is not read-only', () => {
    assert.equal(taskTool.isReadOnly, false);
  });

  it('has a description mentioning subagent', () => {
    assert.ok(taskTool.description.includes('subagent'));
  });

  it('schema requires prompt', () => {
    assert.ok(taskTool.inputSchema.required?.includes('prompt'));
  });

  it('schema has prompt and mode properties', () => {
    const props = taskTool.inputSchema.properties;
    assert.ok('prompt' in props);
    assert.ok('mode' in props);
  });

  it('mode has enum with explore and general', () => {
    const modeProp = taskTool.inputSchema.properties.mode;
    assert.ok(modeProp.enum?.includes('explore'));
    assert.ok(modeProp.enum?.includes('general'));
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('taskTool – validation', () => {
  it('rejects empty prompt', async () => {
    const result = await taskTool.execute({ prompt: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects whitespace-only prompt', async () => {
    const result = await taskTool.execute({ prompt: '   ' }, ctx());
    assert.equal(result.isError, true);
  });
});

// ---------------------------------------------------------------------------
// Execute (requires provider config — we test error handling)
// ---------------------------------------------------------------------------

describe('taskTool – execute error handling', () => {
  it('returns error when _engineFactory is not set', async () => {
    const result = await taskTool.execute(
      { prompt: 'list files', mode: 'explore' },
      ctx(),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('engine factory not available'));
  });

  it('defaults mode to explore and returns error without factory', async () => {
    const result = await taskTool.execute(
      { prompt: 'some task' },
      ctx(),
    );
    assert.ok(result.isError);
  });

  it('uses _engineFactory to spawn subagent and collect text_delta events', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'hello ' };
      yield { type: 'text_delta' as const, text: 'world' };
      yield { type: 'turn_complete' as const };
    }

    const result = await taskTool.execute(
      { prompt: 'test task', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
      }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello world'));
  });

  it('collects error events from subagent', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'error' as const, error: 'something failed' };
    }

    const result = await taskTool.execute(
      { prompt: 'failing task', mode: 'general' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
      }),
    );
    assert.ok(result.output.includes('something failed'));
  });

  it('handles subagent throw gracefully', async () => {
    const result = await taskTool.execute(
      { prompt: 'crash task' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg) => ({
          submit: () => { throw new Error('engine exploded'); },
        }),
      }),
    );
    assert.ok(result.isError);
    assert.ok(result.output.includes('engine exploded'));
  });

  it('returns fallback message when subagent produces no output', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'turn_complete' as const };
    }

    const result = await taskTool.execute(
      { prompt: 'quiet task' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
      }),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.toLowerCase().includes('no output') || result.output.toLowerCase().includes('agent'));
  });
});

// ---------------------------------------------------------------------------
// Background agent execution
// ---------------------------------------------------------------------------

describe('taskTool – run_in_background', () => {
  it('schema includes run_in_background property', () => {
    const props = taskTool.inputSchema.properties;
    assert.ok('run_in_background' in props);
    assert.equal(props.run_in_background.type, 'boolean');
  });

  it('returns task_id immediately when run_in_background is true', async () => {
    let submitted = false;
    async function* fakeSubmit(_prompt: string) {
      submitted = true;
      yield { type: 'text_delta' as const, text: 'bg result' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-'));
    const pm = new ProcessManager(tmpDir);

    try {
      const result = await taskTool.execute(
        { prompt: 'background work', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );

      assert.equal(result.isError, undefined);
      assert.ok(result.output.includes('Background agent started'));
      assert.ok(result.output.includes('agent-'));
      assert.ok(result.output.includes('task_output'));

      await new Promise(r => setTimeout(r, 50));
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('writes output to file when background agent completes', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'async result' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-'));
    const pm = new ProcessManager(tmpDir);

    try {
      const result = await taskTool.execute(
        { prompt: 'bg task', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );

      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);

      await new Promise(r => setTimeout(r, 100));

      const outputPath = path.join(tmpDir, '.superinference', 'tasks', `${taskId}.output`);
      const content = fs.readFileSync(outputPath, 'utf-8');
      assert.ok(content.includes('async result'));
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('works without processManager (no tracking)', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'no-pm result' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-'));

    try {
      const result = await taskTool.execute(
        { prompt: 'bg no pm', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
        }),
      );

      assert.ok(result.output.includes('Background agent started'));

      await new Promise(r => setTimeout(r, 50));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
