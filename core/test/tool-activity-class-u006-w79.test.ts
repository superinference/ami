import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  classifyBashAccess,
  classifyToolAccess,
  describeToolActivity,
  createActivityDescriptionResolver,
  collapseActivityRun,
  isReadOrSearchActivity,
  getToolSearchOrReadInfo,
} from '../src/tools/tool-activity-class';
import { bashTool } from '../src/tools/bash';
import { taskTool } from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext, ToolDefinition } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

// ===========================================================================
// U006 gap [17] — input-aware isSearchOrReadCommand ({isSearch,isRead,isList})
// ===========================================================================
describe('classifyBashAccess (input-aware bash read/search/list classifier)', () => {
  it('classifies search commands (find/grep/rg/…) as isSearch', () => {
    for (const cmd of ['grep -r foo .', 'rg TODO', 'find . -name x', 'which node']) {
      assert.deepEqual(classifyBashAccess(cmd), { isSearch: true, isRead: false, isList: false }, cmd);
    }
  });

  it('classifies read commands (cat/head/wc/jq…) as isRead', () => {
    for (const cmd of ['cat a.txt', 'head -n5 x', 'wc -l x', 'jq . a.json']) {
      assert.deepEqual(classifyBashAccess(cmd), { isSearch: false, isRead: true, isList: false }, cmd);
    }
  });

  it('classifies list commands (ls/tree/du) as isList', () => {
    for (const cmd of ['ls -la', 'tree src', 'du -sh .']) {
      assert.deepEqual(classifyBashAccess(cmd), { isSearch: false, isRead: false, isList: true }, cmd);
    }
  });

  it('resolves a leading path to the base command (/bin/ls → list)', () => {
    assert.deepEqual(classifyBashAccess('/usr/bin/grep x f'), { isSearch: true, isRead: false, isList: false });
  });

  it('unions flags across a read/search pipeline, dropping neutral echo', () => {
    assert.deepEqual(classifyBashAccess('ls dir && echo --- && grep x f'), { isSearch: true, isRead: false, isList: true });
  });

  it('is none (not collapsible) when ANY sub-command is a mutation', () => {
    for (const cmd of ['rm -rf foo', 'cat a && rm b', 'grep x f | tee out && npm i', 'mkdir d']) {
      assert.deepEqual(classifyBashAccess(cmd), { isSearch: false, isRead: false, isList: false }, cmd);
    }
  });

  it('is none for an empty / neutral-only / non-string command', () => {
    assert.deepEqual(classifyBashAccess(''), { isSearch: false, isRead: false, isList: false });
    assert.deepEqual(classifyBashAccess('   '), { isSearch: false, isRead: false, isList: false });
    assert.deepEqual(classifyBashAccess('echo hi'), { isSearch: false, isRead: false, isList: false });
    assert.deepEqual(classifyBashAccess(undefined as unknown as string), { isSearch: false, isRead: false, isList: false });
  });
});

describe('bashTool per-invocation hooks (gap 17 + gap 10)', () => {
  it('isSearchOrReadCommand classifies the actual command from input', () => {
    assert.deepEqual(bashTool.isSearchOrReadCommand!({ command: 'cat x' }), { isSearch: false, isRead: true, isList: false });
    assert.deepEqual(bashTool.isSearchOrReadCommand!({ command: 'rm x' }), { isSearch: false, isRead: false, isList: false });
    // Missing/invalid command → none.
    assert.deepEqual(bashTool.isSearchOrReadCommand!({}), { isSearch: false, isRead: false, isList: false });
  });

  it('getActivityDescription renders a "Running …" phrase', () => {
    assert.equal(bashTool.getActivityDescription!({ command: 'npm test' }), 'Running npm test');
    assert.equal(bashTool.getActivityDescription!({ description: 'build the app', command: 'make' }), 'Running build the app');
    assert.equal(bashTool.getActivityDescription!({}), 'Running command');
  });
});

describe('classifyToolAccess prefers a tool hook, else name sets', () => {
  it('prefers the tool isSearchOrReadCommand hook (input-aware) over the name', () => {
    // By name alone, bash is NOT collapsible; the hook promotes a read-only command.
    assert.deepEqual(classifyToolAccess('bash'), { isSearch: false, isRead: false, isList: false });
    assert.deepEqual(
      classifyToolAccess('bash', { command: 'ls' }, bashTool),
      { isSearch: false, isRead: false, isList: true },
    );
  });

  it('classifies native search/read/list tools by name', () => {
    assert.deepEqual(classifyToolAccess('grep'), { isSearch: true, isRead: false, isList: false });
    assert.deepEqual(classifyToolAccess('file_read'), { isSearch: false, isRead: true, isList: false });
    assert.deepEqual(classifyToolAccess('list_dir'), { isSearch: false, isRead: false, isList: true });
    assert.deepEqual(classifyToolAccess('file_edit'), { isSearch: false, isRead: false, isList: false });
  });

  it('delegates mcp__ names to the MCP classifier (never isList)', () => {
    assert.equal(classifyToolAccess('mcp__srv__search_tasks').isSearch, true);
    assert.equal(classifyToolAccess('mcp__srv__get_thing').isRead, true);
    assert.equal(classifyToolAccess('mcp__srv__search_tasks').isList, false);
  });

  it('getToolSearchOrReadInfo folds a LIST op into READ for the legacy 2-flag view', () => {
    assert.deepEqual(getToolSearchOrReadInfo('list_dir'), { isSearch: false, isRead: true });
  });

  it('isReadOrSearchActivity is input-aware via the tool hook', () => {
    assert.equal(isReadOrSearchActivity('bash'), false);
    assert.equal(isReadOrSearchActivity('bash', { command: 'cat x' }, bashTool), true);
    assert.equal(isReadOrSearchActivity('bash', { command: 'rm x' }, bashTool), false);
  });
});

// ===========================================================================
// U006 gap [10] — human-readable per-tool activity description + resolver
// ===========================================================================
describe('describeToolActivity (canonical per-tool phrasing)', () => {
  it('produces an action phrase for each known native tool', () => {
    assert.equal(describeToolActivity('file_read', { file_path: 'src/a.ts' }), 'Reading src/a.ts');
    assert.equal(describeToolActivity('file_edit', { file_path: 'src/a.ts' }), 'Editing src/a.ts');
    assert.equal(describeToolActivity('write', { file_path: 'out.txt' }), 'Writing out.txt');
    assert.equal(describeToolActivity('grep', { pattern: 'TODO' }), 'Searching for TODO');
    assert.equal(describeToolActivity('glob', { pattern: '*.ts' }), 'Finding *.ts');
    assert.equal(describeToolActivity('list_dir', { path: 'src' }), 'Listing src');
    assert.equal(describeToolActivity('search_symbols', { query: 'foo' }), 'Searching for foo');
    assert.equal(describeToolActivity('web_fetch', { url: 'https://x' }), 'Fetching https://x');
    assert.equal(describeToolActivity('web_search', { query: 'news' }), 'Searching the web for news');
    assert.equal(describeToolActivity('notebook_read', { notebook_path: 'n.ipynb' }), 'Reading n.ipynb');
  });

  it('falls back to a generic phrase when the signature field is absent', () => {
    assert.equal(describeToolActivity('file_read', {}), 'Reading file');
    assert.equal(describeToolActivity('grep', {}), 'Searching');
    assert.equal(describeToolActivity('glob', {}), 'Finding files');
    assert.equal(describeToolActivity('list_dir', {}), 'Listing directory');
    assert.equal(describeToolActivity('web_fetch', {}), 'Fetching page');
    assert.equal(describeToolActivity('web_search', {}), 'Searching the web');
    assert.equal(describeToolActivity('bash', {}), 'Running command');
    assert.equal(describeToolActivity('notebook_read', {}), 'Reading notebook');
    assert.equal(describeToolActivity('file_edit', {}), 'Editing file');
    assert.equal(describeToolActivity('write', {}), 'Writing file');
    assert.equal(describeToolActivity('search_symbols', {}), 'Searching symbols');
  });

  it('truncates an over-long summary argument', () => {
    const long = 'x'.repeat(200);
    const out = describeToolActivity('grep', { pattern: long })!;
    assert.ok(out.startsWith('Searching for '));
    assert.ok(out.length < 'Searching for '.length + 65, out);
    assert.ok(out.endsWith('…'));
  });

  it('returns undefined for an unknown tool (caller keeps its own summary)', () => {
    assert.equal(describeToolActivity('some_custom_tool', { a: 1 }), undefined);
  });
});

describe('createActivityDescriptionResolver', () => {
  const fakeTool: ToolDefinition = {
    name: 'fancy',
    description: 'd',
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
    getActivityDescription: () => 'Doing a fancy thing',
    execute: async () => ({ output: '' }),
  };

  it('prefers a tool-defined getActivityDescription hook', () => {
    const resolve = createActivityDescriptionResolver([fakeTool]);
    assert.equal(resolve('fancy', {}), 'Doing a fancy thing');
  });

  it('falls back to the shared phrasing when the tool lacks the hook', () => {
    const noHook: ToolDefinition = { ...fakeTool, name: 'grep', getActivityDescription: undefined };
    const resolve = createActivityDescriptionResolver([noHook]);
    assert.equal(resolve('grep', { pattern: 'z' }), 'Searching for z');
  });

  it('resolves via an alias and falls back to describe for an unknown name', () => {
    const aliased: ToolDefinition = { ...fakeTool, name: 'fancy', aliases: ['Fancy'] };
    const resolve = createActivityDescriptionResolver([aliased]);
    assert.equal(resolve('Fancy', {}), 'Doing a fancy thing');
    assert.equal(resolve('list_dir', { path: 'p' }), 'Listing p');
    assert.equal(resolve('totally_unknown', {}), undefined);
  });
});

describe('collapseActivityRun (input-aware fold)', () => {
  it('folds a trailing run of >=2 collapsible ops; single/none → undefined', () => {
    assert.equal(collapseActivityRun([true, true, true]), 'Reading/searching (3 ops)');
    assert.equal(collapseActivityRun([false, true, true]), 'Reading/searching (2 ops)');
    assert.equal(collapseActivityRun([true]), undefined);
    assert.equal(collapseActivityRun([]), undefined);
    assert.equal(collapseActivityRun([true, true, false]), undefined);
  });
});

// ===========================================================================
// U006 E2E — background drain surfaces activity phrases + input-aware collapse
// ===========================================================================
describe('taskTool background drain — gap 10 + gap 17 end-to-end', () => {
  it('shows a getActivityDescription phrase and collapses read-only bash commands', async () => {
    async function* fakeSubmit(_prompt: string) {
      // Two read-only bash commands in a row → input-aware collapse (name-based
      // classification would NOT collapse bash).
      yield { type: 'tool_use_start' as const, toolName: 'bash', input: { command: 'cat a.txt' } };
      yield { type: 'tool_use_start' as const, toolName: 'bash', input: { command: 'ls src' } };
      yield { type: 'text_delta' as const, text: 'done' };
    }
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u006-bashcollapse-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await taskTool.execute(
        { prompt: 'read stuff', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );
      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);
      await new Promise(r => setTimeout(r, 180));
      const entry: any = pm.list().find(e => e.taskId === taskId);
      assert.ok(entry);
      assert.equal(entry.progressSummary, 'Reading/searching (2 ops)');
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('a single mutating bash command keeps its human-readable phrase (no collapse)', async () => {
    async function* fakeSubmit(_prompt: string) {
      yield { type: 'tool_use_start' as const, toolName: 'bash', input: { command: 'npm run build' } };
      yield { type: 'text_delta' as const, text: 'done' };
    }
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-u006-single-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await taskTool.execute(
        { prompt: 'build', run_in_background: true },
        ctx({
          cwd: tmpDir,
          _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
          _engineFactory: () => ({ submit: fakeSubmit }),
          processManager: pm,
        }),
      );
      const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
      assert.ok(taskId);
      await new Promise(r => setTimeout(r, 180));
      const entry: any = pm.list().find(e => e.taskId === taskId);
      assert.ok(entry);
      assert.equal(entry.progressSummary, 'Running npm run build');
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
