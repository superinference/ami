import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import type { Message } from '../src/types';

import { extractConversationMeta, countToolUses, extractFilesModified, getConversationDuration } from '../src/conversation-summary';

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
