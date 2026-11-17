import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import type { Message } from '../src/types';

import {
  extractConversationMeta,
  countToolUses,
  extractFilesModified,
  getConversationDuration,
  getLanguageFromPath,
  categorizeToolError,
  responseTimeHistogram,
  formatDuration,
  formatConversationSummary,
} from '../src/conversation-summary';

describe('countToolUses', () => {
  it('counts tool calls from assistant messages', () => {
    const messages: Message[] = [
      { role: 'user', content: 'fix the bug' },
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_read', arguments: '{}' } },
        { id: '2', type: 'function', function: { name: 'bash', arguments: '{}' } },
      ] },
      { role: 'tool', tool_call_id: '1', content: 'file content' },
      { role: 'tool', tool_call_id: '2', content: 'output' },
      { role: 'assistant', content: null, tool_calls: [
        { id: '3', type: 'function', function: { name: 'file_edit', arguments: '{}' } },
      ] },
    ];
    const counts = countToolUses(messages);
    assert.equal(counts.get('file_read'), 1);
    assert.equal(counts.get('bash'), 1);
    assert.equal(counts.get('file_edit'), 1);
  });

  it('returns empty map for no tool calls', () => {
    const messages: Message[] = [
      { role: 'user', content: 'hello' },
      { role: 'assistant', content: 'hi' },
    ];
    const counts = countToolUses(messages);
    assert.equal(counts.size, 0);
  });
});

describe('extractFilesModified', () => {
  it('extracts file paths from file_edit tool calls', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/tmp/a.ts"}' } },
        { id: '2', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/tmp/b.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.ok(files.has('/tmp/a.ts'));
    assert.ok(files.has('/tmp/b.ts'));
  });

  it('includes file_write paths', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_write', arguments: '{"file_path":"/tmp/new.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.ok(files.has('/tmp/new.ts'));
  });

  it('deduplicates files', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/tmp/a.ts"}' } },
        { id: '2', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/tmp/a.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.equal(files.size, 1);
  });
});

describe('extractConversationMeta', () => {
  it('extracts turn count and tool counts', () => {
    const messages: Message[] = [
      { role: 'user', content: 'fix bug' },
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'bash', arguments: '{"command":"ls"}' } },
      ] },
      { role: 'tool', tool_call_id: '1', content: 'files' },
      { role: 'assistant', content: 'Done!' },
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.userTurnCount, 1);
    assert.equal(meta.assistantTurnCount, 2);
    assert.equal(meta.totalToolCalls, 1);
    assert.equal(meta.filesModified.size, 0);
  });

  it('handles empty messages', () => {
    const meta = extractConversationMeta([]);
    assert.equal(meta.userTurnCount, 0);
    assert.equal(meta.assistantTurnCount, 0);
    assert.equal(meta.totalToolCalls, 0);
  });
});

describe('getConversationDuration', () => {
  it('returns 0 for empty messages', () => {
    assert.equal(getConversationDuration([]), 0);
  });

  it('returns 0 for messages without timestamps', () => {
    const messages: Message[] = [
      { role: 'user', content: 'hello' },
    ];
    assert.equal(getConversationDuration(messages), 0);
  });

  it('returns 0 for single message', () => {
    const messages: Message[] = [
      { role: 'user', content: 'hello' },
    ];
    assert.equal(getConversationDuration(messages), 0);
  });

  it('returns duration between first and last timestamped messages', () => {
    const messages: any[] = [
      { role: 'user', content: 'hello', _timestamp: 1000 },
      { role: 'assistant', content: 'hi', _timestamp: 2000 },
      { role: 'user', content: 'bye', _timestamp: 5000 },
    ];
    assert.equal(getConversationDuration(messages), 4000);
  });

  it('returns 0 when first message has no timestamp', () => {
    const messages: any[] = [
      { role: 'user', content: 'hello' },
      { role: 'assistant', content: 'hi', _timestamp: 2000 },
    ];
    assert.equal(getConversationDuration(messages), 0);
  });

  it('returns 0 when last message has no timestamp', () => {
    const messages: any[] = [
      { role: 'user', content: 'hello', _timestamp: 1000 },
      { role: 'assistant', content: 'hi' },
    ];
    assert.equal(getConversationDuration(messages), 0);
  });
});

describe('extractFilesModified — edge cases', () => {
  it('skips non-file-mutating tools', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'bash', arguments: '{"command":"ls"}' } },
        { id: '2', type: 'function', function: { name: 'file_read', arguments: '{"file_path":"/tmp/a.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.equal(files.size, 0);
  });

  it('includes notebook_edit paths', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'notebook_edit', arguments: '{"file_path":"/tmp/nb.ipynb"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.ok(files.has('/tmp/nb.ipynb'));
  });

  it('includes multi_edit paths', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'multi_edit', arguments: '{"file_path":"/tmp/multi.ts"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.ok(files.has('/tmp/multi.ts'));
  });

  it('handles malformed JSON arguments gracefully', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: 'not valid json' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.equal(files.size, 0);
  });

  it('handles missing file_path in arguments', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"command":"ls"}' } },
      ] },
    ];
    const files = extractFilesModified(messages);
    assert.equal(files.size, 0);
  });

  it('skips user and tool messages', () => {
    const messages: Message[] = [
      { role: 'user', content: 'hello' },
      { role: 'tool', tool_call_id: '1', content: 'output' },
    ];
    const files = extractFilesModified(messages);
    assert.equal(files.size, 0);
  });
});

describe('getLanguageFromPath', () => {
  it('maps known extensions to languages', () => {
    assert.equal(getLanguageFromPath('/a/b/c.ts'), 'TypeScript');
    assert.equal(getLanguageFromPath('src/App.tsx'), 'TypeScript');
    assert.equal(getLanguageFromPath('x.py'), 'Python');
    assert.equal(getLanguageFromPath('README.md'), 'Markdown');
    assert.equal(getLanguageFromPath('run.SH'), 'Shell'); // case-insensitive
  });

  it('returns null for unknown or missing extensions and dotfiles', () => {
    assert.equal(getLanguageFromPath('Makefile'), null);
    assert.equal(getLanguageFromPath('/path/to/dir/noext'), null);
    assert.equal(getLanguageFromPath('.gitignore'), null);
    assert.equal(getLanguageFromPath('weird.xyz'), null);
    assert.equal(getLanguageFromPath('/a.b/file'), null); // dot in dir, not basename
  });
});

describe('categorizeToolError', () => {
  it('buckets by substring, case-insensitively', () => {
    assert.equal(categorizeToolError('Command failed with exit code 1'), 'Command Failed');
    assert.equal(categorizeToolError('User rejected the edit'), 'User Rejected');
    assert.equal(categorizeToolError("the user doesn't want to proceed"), 'User Rejected');
    assert.equal(categorizeToolError('String to replace not found in file'), 'Edit Failed');
    assert.equal(categorizeToolError('no changes to apply'), 'Edit Failed');
    assert.equal(categorizeToolError('File has been modified since read'), 'File Changed');
    assert.equal(categorizeToolError('content exceeds maximum allowed'), 'File Too Large');
    assert.equal(categorizeToolError('the output is too large'), 'File Too Large');
    assert.equal(categorizeToolError('file not found: /x'), 'File Not Found');
    assert.equal(categorizeToolError('path does not exist'), 'File Not Found');
    assert.equal(categorizeToolError('something unexpected'), 'Other');
  });
});

describe('extractConversationMeta — transcript-derived metrics', () => {
  it('computes line deltas from file_edit diffs and file_write content', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: JSON.stringify({ file_path: '/a.ts', old_string: 'a\nb', new_string: 'a\nb\nc' }) } },
        { id: '2', type: 'function', function: { name: 'file_write', arguments: JSON.stringify({ file_path: '/b.py', content: 'line1\nline2\nline3' }) } },
      ] },
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.linesAdded, 1 + 3); // edit +1, write 3 lines
    assert.equal(meta.linesRemoved, 0);
  });

  it('builds a language histogram from mutated file paths', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/a.ts"}' } },
        { id: '2', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/b.tsx"}' } },
        { id: '3', type: 'function', function: { name: 'file_write', arguments: '{"file_path":"/c.py"}' } },
        { id: '4', type: 'function', function: { name: 'notebook_edit', arguments: '{"notebook_path":"/d.ipynb"}' } },
      ] },
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.languages.get('TypeScript'), 2);
    assert.equal(meta.languages.get('Python'), 1);
    assert.equal(meta.languages.has('.ipynb'), false); // unknown ext → omitted
  });

  it('counts git commit/push from bash command strings', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'bash', arguments: '{"command":"git add -A && git commit -m x"}' } },
        { id: '2', type: 'function', function: { name: 'bash', arguments: '{"command":"git push origin main"}' } },
        { id: '3', type: 'function', function: { name: 'bash', arguments: '{"command":"ls"}' } },
      ] },
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.gitCommits, 1);
    assert.equal(meta.gitPushes, 1);
  });

  it('derives capability flags from tool-name shape', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'mcp__server__tool', arguments: '{}' } },
        { id: '2', type: 'function', function: { name: 'web_search', arguments: '{}' } },
        { id: '3', type: 'function', function: { name: 'web_fetch', arguments: '{}' } },
        { id: '4', type: 'function', function: { name: 'task', arguments: '{}' } },
      ] },
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.usesMcp, true);
    assert.equal(meta.usesWebSearch, true);
    assert.equal(meta.usesWebFetch, true);
    assert.equal(meta.usesSubagent, true);
  });

  it('defaults capability flags to false when unused', () => {
    const meta = extractConversationMeta([
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'bash', arguments: '{"command":"ls"}' } },
      ] },
    ]);
    assert.equal(meta.usesMcp, false);
    assert.equal(meta.usesWebSearch, false);
    assert.equal(meta.usesWebFetch, false);
    assert.equal(meta.usesSubagent, false);
  });

  it('counts and categorizes tool errors from error tool messages', () => {
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'bash', arguments: '{}' } },
        { id: '2', type: 'function', function: { name: 'bash', arguments: '{}' } },
      ] },
      { role: 'tool', tool_call_id: '1', content: 'failed with exit code 2', isError: true } as Message,
      { role: 'tool', tool_call_id: '2', content: 'ok output' } as Message, // not an error
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.toolErrors, 1);
    assert.equal(meta.toolErrorCategories.get('Command Failed'), 1);
  });

  it('computes human response times with the 2s–3600s filter + median/avg', () => {
    const messages: any[] = [
      { role: 'user', content: 'hi', _timestamp: 0 },
      { role: 'assistant', content: 'a1', _timestamp: 1000 },
      { role: 'user', content: 'next', _timestamp: 11000 },   // 10s gap → kept
      { role: 'assistant', content: 'a2', _timestamp: 12000 },
      { role: 'user', content: 'quick', _timestamp: 12500 },  // 0.5s gap → filtered (<2s)
      { role: 'assistant', content: 'a3', _timestamp: 13000 },
      { role: 'user', content: 'slow', _timestamp: 43000 },   // 30s gap → kept
    ];
    const meta = extractConversationMeta(messages);
    assert.deepEqual(meta.userResponseTimes, [10, 30]);
    assert.equal(meta.responseTimeMedianSec, 20);
    assert.equal(meta.responseTimeAvgSec, 20);
  });

  it('excludes isMeta user turns from humanTurnCount and response times', () => {
    const messages: any[] = [
      { role: 'assistant', content: 'a1', _timestamp: 0 },
      { role: 'user', content: 'nudge', isMeta: true, _timestamp: 10000 }, // meta → not a human turn
      { role: 'user', content: 'real', _timestamp: 20000 },
    ];
    const meta = extractConversationMeta(messages);
    assert.equal(meta.userTurnCount, 2);
    assert.equal(meta.humanTurnCount, 1);
    // Only the real human turn: 20s from last assistant (ts 0).
    assert.deepEqual(meta.userResponseTimes, [20]);
  });

  it('returns zeroed metrics for empty input', () => {
    const meta = extractConversationMeta([]);
    assert.equal(meta.linesAdded, 0);
    assert.equal(meta.linesRemoved, 0);
    assert.equal(meta.languages.size, 0);
    assert.equal(meta.gitCommits, 0);
    assert.equal(meta.toolErrors, 0);
    assert.equal(meta.responseTimeMedianSec, 0);
    assert.equal(meta.responseTimeAvgSec, 0);
    assert.deepEqual(meta.userResponseTimes, []);
  });

  it('ignores unparseable tool arguments for metrics', () => {
    const meta = extractConversationMeta([
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'file_edit', arguments: 'not json' } },
      ] },
    ]);
    assert.equal(meta.linesAdded, 0);
    assert.equal(meta.languages.size, 0);
  });
});

describe('responseTimeHistogram', () => {
  it('buckets times and omits empty buckets', () => {
    const hist = responseTimeHistogram([5, 8, 20, 120, 2000]);
    assert.equal(hist.get('2-10s'), 2);
    assert.equal(hist.get('10-30s'), 1);
    assert.equal(hist.get('1-5m'), 1);
    assert.equal(hist.get('5-60m'), 1);
    assert.equal(hist.has('30-60s'), false);
  });

  it('skips values outside the covered range', () => {
    const hist = responseTimeHistogram([4000]);
    assert.equal(hist.size, 0);
  });
});

describe('formatDuration', () => {
  it('formats ms as compact h/m/s', () => {
    assert.equal(formatDuration(0), '0s');
    assert.equal(formatDuration(5000), '5s');
    assert.equal(formatDuration(65000), '1m 5s');
    assert.equal(formatDuration(120000), '2m');
    assert.equal(formatDuration(3_600_000), '1h');
    assert.equal(formatDuration(3_720_000), '1h 2m');
    assert.equal(formatDuration(-100), '0s');
  });
});

describe('formatConversationSummary', () => {
  it('produces a one-line summary with top tools, files, duration and affordance', () => {
    const messages: Message[] = [
      { role: 'user', content: 'go' },
      { role: 'assistant', content: null, tool_calls: [
        { id: '1', type: 'function', function: { name: 'bash', arguments: '{"command":"ls"}' } },
        { id: '2', type: 'function', function: { name: 'file_edit', arguments: '{"file_path":"/a.ts","old_string":"x","new_string":"x\\ny"}' } },
      ] },
    ];
    const meta = extractConversationMeta(messages);
    const summary = formatConversationSummary(meta, 65000);
    assert.match(summary, /1 human turn/);
    assert.match(summary, /2 tool calls/);
    assert.match(summary, /top: /);
    assert.match(summary, /1 file modified/);
    assert.match(summary, /1m 5s/);
    assert.match(summary, /\/recap for the full breakdown$/);
  });
});
