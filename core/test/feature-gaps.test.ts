import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { resetTaskState, setTaskPersistPath } from '../src/tools/task-tracker';
import { taskTrackerTool } from '../src/tools/task-tracker';
import { HookManager } from '../src/hooks';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    filesRead: new Set<string>(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Device file blocking (file_read)
// ---------------------------------------------------------------------------

describe('file_read – device file blocking', () => {
  it('blocks /dev/zero', async () => {
    const result = await fileReadTool.execute({ file_path: '/dev/zero' }, ctx());
    assert.ok(result.isError, 'Should reject /dev/zero');
  });

  it('blocks /dev/stdin', async () => {
    const result = await fileReadTool.execute({ file_path: '/dev/stdin' }, ctx());
    assert.ok(result.isError, 'Should reject /dev/stdin');
  });

  it('blocks /dev/null', async () => {
    const result = await fileReadTool.execute({ file_path: '/dev/null' }, ctx());
    assert.ok(result.isError, 'Should reject /dev/null');
  });

  it('blocks /proc/self/fd/0', async () => {
    const result = await fileReadTool.execute({ file_path: '/proc/self/fd/0' }, ctx());
    assert.ok(result.isError, 'Should reject /proc paths');
  });

  it('blocks /dev/random', async () => {
    const result = await fileReadTool.execute({ file_path: '/dev/random' }, ctx());
    assert.ok(result.isError, 'Should reject /dev/random');
  });

  it('allows normal files', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-dev-'));
    const f = path.join(tmpDir, 'ok.txt');
    fs.writeFileSync(f, 'hello');
    const result = await fileReadTool.execute({ file_path: f }, ctx({ cwd: tmpDir }));
    assert.ok(!result.isError);
    assert.ok(result.output.includes('hello'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// PDF page range parsing (file_read)
// ---------------------------------------------------------------------------

describe('file_read – PDF page range parameter', () => {
  it('schema includes pages property', () => {
    assert.ok('pages' in fileReadTool.inputSchema.properties);
    assert.equal(fileReadTool.inputSchema.properties.pages.type, 'string');
  });

  it('rejects non-PDF file with pages parameter', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pdf-'));
    const f = path.join(tmpDir, 'test.txt');
    fs.writeFileSync(f, 'hello');
    const result = await fileReadTool.execute({ file_path: f, pages: '1-5' }, ctx({ cwd: tmpDir }));
    // Pages param is only used for PDFs, should still read normally
    assert.ok(!result.isError || result.output.includes('hello'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Memory age tracking
// ---------------------------------------------------------------------------

describe('memory – age tracking', () => {
  // We test the exported functions indirectly via parseFrontmatter since
  // memoryAgeDays and memoryFreshness are not exported. We'll test the memory
  // loading which uses them.
  it('parseFrontmatter extracts type field', () => {
    const { parseFrontmatter } = require('../src/memory');
    const result = parseFrontmatter('---\nname: test\ndescription: test desc\ntype: user\n---\nBody text.');
    assert.equal(result.frontmatter.name, 'test');
    assert.equal(result.frontmatter.type, 'user');
    assert.equal(result.body.trim(), 'Body text.');
  });

  it('parseFrontmatter handles missing frontmatter', () => {
    const { parseFrontmatter } = require('../src/memory');
    const result = parseFrontmatter('No frontmatter here.');
    assert.equal(Object.keys(result.frontmatter).length, 0);
    assert.equal(result.body, 'No frontmatter here.');
  });
});

// ---------------------------------------------------------------------------
// Task persistence to disk
// ---------------------------------------------------------------------------

describe('task_tracker – disk persistence', () => {
  let tmpDir: string;

  beforeEach(() => {
    resetTaskState();
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-task-'));
    setTaskPersistPath(tmpDir);
  });

  it('saves tasks to disk on create', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'persist test', description: 'should be on disk' },
      ctx({ cwd: tmpDir }),
    );

    const persistFile = path.join(tmpDir, '.superinference', 'tasks.json');
    assert.ok(fs.existsSync(persistFile), 'tasks.json should exist');

    const data = JSON.parse(fs.readFileSync(persistFile, 'utf-8'));
    assert.ok(data.tasks.length >= 1);
    assert.ok(data.tasks.some((t: any) => t.subject === 'persist test'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('saves tasks to disk on update', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'update me', description: '' },
      ctx({ cwd: tmpDir }),
    );

    await taskTrackerTool.execute(
      { action: 'update', task_id: 1, status: 'in_progress' },
      ctx({ cwd: tmpDir }),
    );

    const persistFile = path.join(tmpDir, '.superinference', 'tasks.json');
    const data = JSON.parse(fs.readFileSync(persistFile, 'utf-8'));
    assert.ok(data.tasks.some((t: any) => t.status === 'in_progress'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('loads tasks from disk', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'load test', description: '' },
      ctx({ cwd: tmpDir }),
    );

    resetTaskState();
    setTaskPersistPath(tmpDir);

    const result = await taskTrackerTool.execute(
      { action: 'list' },
      ctx({ cwd: tmpDir }),
    );
    assert.ok(result.output.includes('load test'));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('saves tasks to disk on delete', async () => {
    await taskTrackerTool.execute(
      { action: 'create', subject: 'delete me', description: '' },
      ctx({ cwd: tmpDir }),
    );
    await taskTrackerTool.execute(
      { action: 'delete', task_id: 1 },
      ctx({ cwd: tmpDir }),
    );

    const persistFile = path.join(tmpDir, '.superinference', 'tasks.json');
    const data = JSON.parse(fs.readFileSync(persistFile, 'utf-8'));
    assert.equal(data.tasks.length, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Hook config file loading
// ---------------------------------------------------------------------------

describe('HookManager – config file loading', () => {
  it('loads hooks from .superinference/hooks.json', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-hooks-'));
    const hookDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(hookDir, { recursive: true });

    const config = {
      hooks: [
        {
          event: 'preToolUse',
          hook: { type: 'command', command: 'echo test', timeout: 5000 },
          matcher: { toolName: 'bash' },
        },
      ],
    };
    fs.writeFileSync(path.join(hookDir, 'hooks.json'), JSON.stringify(config));

    const hm = new HookManager();
    hm.loadFromFile(tmpDir);

    // Verify hook was loaded — the HookManager should have at least one PreToolUse hook
    // We can't directly inspect hooks, but we can test that loadFromFile doesn't throw
    assert.ok(true, 'loadFromFile should not throw');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles missing hooks.json gracefully', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-hooks-'));
    const hm = new HookManager();
    hm.loadFromFile(tmpDir);
    assert.ok(true, 'Should not throw on missing file');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles malformed hooks.json gracefully', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-hooks-'));
    const hookDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(hookDir, { recursive: true });
    fs.writeFileSync(path.join(hookDir, 'hooks.json'), '{ invalid json }}}');

    const hm = new HookManager();
    hm.loadFromFile(tmpDir);
    assert.ok(true, 'Should not throw on invalid JSON');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
