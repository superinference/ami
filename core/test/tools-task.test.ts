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

  it('returns error without factory regardless of default mode', async () => {
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

// ---------------------------------------------------------------------------
// Regression: default mode must be 'general' not 'explore'
// When no mode is specified, agents must have write tools (file_edit, bash)
// so they can actually perform tasks like writing tests or editing files.
// Bug: agents defaulted to 'explore' (read-only) and silently failed.
// ---------------------------------------------------------------------------

describe('taskTool – default mode gives write tools (regression)', () => {
  it('agents with no mode specified get file_edit and bash tools', async () => {
    let capturedTools: string[] = [];
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'wrote tests' };
    }

    const result = await taskTool.execute(
      { prompt: 'write unit tests for the solver module' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg) => {
          capturedTools = (cfg.tools || []).map((t: any) => t.name);
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.ok(!result.isError, `should succeed, got: ${result.output}`);
    assert.ok(capturedTools.length > 0, 'engine factory should receive tools');
    assert.ok(capturedTools.includes('file_edit'),
      `default mode must include file_edit for writing code; got: [${capturedTools.join(', ')}]`);
    assert.ok(capturedTools.includes('bash'),
      `default mode must include bash for running commands; got: [${capturedTools.join(', ')}]`);
    assert.ok(capturedTools.includes('file_read'),
      'default mode must include file_read');
    assert.ok(!capturedTools.includes('task'),
      'default mode must not include recursive task tool');
  });

  it('explicit explore mode still restricts to read-only tools', async () => {
    let capturedTools: string[] = [];
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'explored' };
    }

    await taskTool.execute(
      { prompt: 'list all files', mode: 'explore' },
      ctx({
        _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
        _engineFactory: (cfg) => {
          capturedTools = (cfg.tools || []).map((t: any) => t.name);
          return { submit: fakeSubmit };
        },
      }),
    );

    assert.ok(!capturedTools.includes('file_edit'),
      'explore mode must NOT include file_edit');
    assert.ok(!capturedTools.includes('bash'),
      'explore mode must NOT include bash');
    assert.ok(capturedTools.includes('file_read'),
      'explore mode must include file_read');
    assert.ok(capturedTools.includes('grep'),
      'explore mode must include grep');
  });

  it('background agents with no mode get write tools too', async () => {
    let capturedTools: string[] = [];
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'bg result' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-mode-'));
    const pm = new ProcessManager(tmpDir);

    try {
      await taskTool.execute(
        { prompt: 'write tests in background', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (cfg) => {
            capturedTools = (cfg.tools || []).map((t: any) => t.name);
            return { submit: fakeSubmit };
          },
          processManager: pm,
        }),
      );

      assert.ok(capturedTools.includes('file_edit'),
        `background agents with default mode must include file_edit; got: [${capturedTools.join(', ')}]`);
      assert.ok(capturedTools.includes('bash'),
        `background agents with default mode must include bash; got: [${capturedTools.join(', ')}]`);

      await new Promise(r => setTimeout(r, 50));
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Regression: background agents must stream output incrementally
// Output must be written to disk as events arrive, not only on completion.
// Bug: task_output returned empty results for running agents because output
// was accumulated in memory and written only after the agent finished.
// ---------------------------------------------------------------------------

describe('taskTool – background agents stream output incrementally (regression)', () => {
  it('output file contains partial content while agent is still running', async () => {
    let resolveBarrier!: () => void;
    const barrier = new Promise<void>(r => { resolveBarrier = r; });

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'partial result here' };
      await barrier;
      yield { type: 'text_delta' as const, text: ' and more' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-stream-'));
    const pm = new ProcessManager(tmpDir);

    try {
      const result = await taskTool.execute(
        { prompt: 'streaming test', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );

      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId, 'should return a task ID');

      await new Promise(r => setTimeout(r, 100));

      const outputPath = path.join(tmpDir, '.superinference', 'tasks', `${taskId}.output`);
      const midContent = fs.readFileSync(outputPath, 'utf-8');
      assert.ok(midContent.includes('partial result here'),
        `output file should contain streamed content during execution, got: "${midContent}"`);

      resolveBarrier();
      await new Promise(r => setTimeout(r, 100));

      const finalContent = fs.readFileSync(outputPath, 'utf-8');
      assert.ok(finalContent.includes('partial result here'),
        'final output should contain first part');
      assert.ok(finalContent.includes('and more'),
        'final output should contain second part');
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('task_output returns partial content for running background agents', async () => {
    let resolveBarrier!: () => void;
    const barrier = new Promise<void>(r => { resolveBarrier = r; });

    async function* fakeSubmit(_prompt: string) {
      yield { type: 'text_delta' as const, text: 'live progress data' };
      await barrier;
      yield { type: 'text_delta' as const, text: ' done' };
    }

    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-bg-taskout-'));
    const pm = new ProcessManager(tmpDir);

    try {
      const taskResult = await taskTool.execute(
        { prompt: 'monitored task', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: (_cfg) => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );

      const taskId = taskResult.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);

      await new Promise(r => setTimeout(r, 100));

      const { taskOutputTool } = require('../src/tools/task-output');
      const outputResult = await taskOutputTool.execute(
        { task_id: taskId },
        ctx({ processManager: pm }),
      );

      assert.ok(!outputResult.isError);
      assert.ok(outputResult.output.includes('live progress data'),
        `task_output should show streamed content while running, got: "${outputResult.output}"`);

      resolveBarrier();
      await new Promise(r => setTimeout(r, 100));
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
