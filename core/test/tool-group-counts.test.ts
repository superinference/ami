import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  classifyToolGroupKind,
  aggregateToolGroupCounts,
  emptyToolGroupCounts,
  hasNonMemoryOps,
  isGitOpBashCommand,
} from '../src/tool-group-counts';

describe('tool-group-counts: classifyToolGroupKind', () => {
  it('classifies read / search / list tools', () => {
    assert.equal(classifyToolGroupKind('file_read', { file_path: '/a' }), 'read');
    assert.equal(classifyToolGroupKind('grep', { pattern: 'x' }), 'search');
    assert.equal(classifyToolGroupKind('glob', { glob: '**/*.ts' }), 'search');
    assert.equal(classifyToolGroupKind('search_symbols', {}), 'search');
    assert.equal(classifyToolGroupKind('find_references', {}), 'search');
    assert.equal(classifyToolGroupKind('scout', {}), 'search');
    assert.equal(classifyToolGroupKind('list_dir', { path: '/a' }), 'list');
  });

  it('classifies memory tools by name', () => {
    assert.equal(classifyToolGroupKind('read_memory', {}), 'memoryRead');
    assert.equal(classifyToolGroupKind('write_memory', {}), 'memoryWrite');
  });

  it('treats a search over a memory path as a memory search when the predicate matches', () => {
    const opts = { isMemoryPath: (p: string) => p.includes('/memory/') };
    assert.equal(classifyToolGroupKind('grep', { path: '/proj/.ami/memory/notes' }, opts), 'memorySearch');
    assert.equal(classifyToolGroupKind('grep', { path: '/proj/src' }, opts), 'search');
    // No predicate -> always a generic search.
    assert.equal(classifyToolGroupKind('grep', { path: '/proj/.ami/memory/notes' }), 'search');
  });

  it('absorbs tool_search silently', () => {
    assert.equal(classifyToolGroupKind('tool_search', {}), 'absorbed');
  });

  it('classifies MCP tools and ignores malformed mcp-like names', () => {
    assert.equal(classifyToolGroupKind('mcp__slack__post', {}), 'mcpCall');
    // `mcp__` with an empty server is not a valid MCP tool name -> none.
    assert.equal(classifyToolGroupKind('mcp__', {}), 'none');
  });

  it('separates git-op bash from generic bash', () => {
    assert.equal(classifyToolGroupKind('bash', { command: 'git commit -m x' }), 'gitOpBash');
    assert.equal(classifyToolGroupKind('bash', { command: 'ls -la' }), 'bash');
    assert.equal(classifyToolGroupKind('bash', {}), 'bash');
  });

  it('returns none for tools outside the collapse group', () => {
    assert.equal(classifyToolGroupKind('task', {}), 'none');
    assert.equal(classifyToolGroupKind('file_edit', { file_path: '/a' }), 'none');
  });
});

describe('tool-group-counts: isGitOpBashCommand', () => {
  it('detects commit / push / branch / switch -c / checkout -b / gh pr create', () => {
    assert.equal(isGitOpBashCommand('git commit -m "x"'), true);
    assert.equal(isGitOpBashCommand('git push origin main'), true);
    assert.equal(isGitOpBashCommand('git branch feature'), true);
    assert.equal(isGitOpBashCommand('git switch -c feature'), true);
    assert.equal(isGitOpBashCommand('git checkout -b feature'), true);
    assert.equal(isGitOpBashCommand('gh pr create --fill'), true);
    assert.equal(isGitOpBashCommand('git -C /repo push'), true);
  });
  it('is false for non-git-op commands and bad input', () => {
    assert.equal(isGitOpBashCommand('git status'), false);
    assert.equal(isGitOpBashCommand('git switch main'), false);
    assert.equal(isGitOpBashCommand('ls'), false);
    assert.equal(isGitOpBashCommand(''), false);
    assert.equal(isGitOpBashCommand(undefined as unknown as string), false);
  });
});

describe('tool-group-counts: aggregateToolGroupCounts', () => {
  it('starts from a zeroed baseline', () => {
    const z = emptyToolGroupCounts();
    assert.deepEqual(aggregateToolGroupCounts([]), z);
    assert.equal(z.mcpServerNames.length, 0);
  });

  it('accumulates per-kind counts across a run', () => {
    const c = aggregateToolGroupCounts([
      { name: 'file_read', input: { file_path: '/a' } },
      { name: 'file_read', input: { file_path: '/b' } },
      { name: 'grep', input: { pattern: 'x' } },
      { name: 'list_dir', input: { path: '/' } },
      { name: 'bash', input: { command: 'ls' } },
      { name: 'bash', input: { command: 'git push' } },
      { name: 'read_memory', input: {} },
      { name: 'write_memory', input: {} },
      { name: 'tool_search', input: {} },
      { name: 'task', input: {} },
    ]);
    assert.equal(c.readCount, 2);
    assert.equal(c.searchCount, 1);
    assert.equal(c.listCount, 1);
    assert.equal(c.bashCount, 1);
    assert.equal(c.gitOpBashCount, 1);
    assert.equal(c.memoryReadCount, 1);
    assert.equal(c.memoryWriteCount, 1);
    // absorbed + none add nothing
    assert.equal(c.mcpCallCount, 0);
  });

  it('records distinct MCP server names in first-seen order', () => {
    const c = aggregateToolGroupCounts([
      { name: 'mcp__slack__post', input: {} },
      { name: 'mcp__github__pr', input: {} },
      { name: 'mcp__slack__read', input: {} },
    ]);
    assert.equal(c.mcpCallCount, 3);
    assert.deepEqual(c.mcpServerNames, ['slack', 'github']);
  });

  it('counts memory searches via the injected predicate', () => {
    const c = aggregateToolGroupCounts(
      [{ name: 'grep', input: { path: '/x/memory/y' } }],
      { isMemoryPath: (p) => p.includes('/memory/') },
    );
    assert.equal(c.memorySearchCount, 1);
    assert.equal(c.searchCount, 0);
  });
});

describe('tool-group-counts: hasNonMemoryOps', () => {
  it('is true when there is any read/search/bash/mcp op', () => {
    const c = emptyToolGroupCounts();
    c.readCount = 1;
    assert.equal(hasNonMemoryOps(c), true);
  });
  it('is false for a pure memory-ops group', () => {
    const c = emptyToolGroupCounts();
    c.memoryReadCount = 2;
    c.memoryWriteCount = 1;
    assert.equal(hasNonMemoryOps(c), false);
  });
});
