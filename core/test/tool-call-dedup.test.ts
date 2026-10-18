import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { deduplicateToolCalls, toolCallFingerprint } from '../src/tools/tool-utils';

describe('toolCallFingerprint', () => {
  it('produces same fingerprint for identical calls', () => {
    const fp1 = toolCallFingerprint('file_read', { file_path: '/tmp/test.ts' });
    const fp2 = toolCallFingerprint('file_read', { file_path: '/tmp/test.ts' });
    assert.equal(fp1, fp2);
  });

  it('produces different fingerprints for different inputs', () => {
    const fp1 = toolCallFingerprint('file_read', { file_path: '/tmp/a.ts' });
    const fp2 = toolCallFingerprint('file_read', { file_path: '/tmp/b.ts' });
    assert.notEqual(fp1, fp2);
  });

  it('produces different fingerprints for different tool names', () => {
    const fp1 = toolCallFingerprint('file_read', { file_path: '/tmp/a.ts' });
    const fp2 = toolCallFingerprint('file_edit', { file_path: '/tmp/a.ts' });
    assert.notEqual(fp1, fp2);
  });

  it('handles empty input', () => {
    const fp = toolCallFingerprint('bash', {});
    assert.ok(typeof fp === 'string' && fp.length > 0);
  });

  it('ignores key ordering', () => {
    const fp1 = toolCallFingerprint('bash', { command: 'ls', timeout: '5000' });
    const fp2 = toolCallFingerprint('bash', { timeout: '5000', command: 'ls' });
    assert.equal(fp1, fp2);
  });
});

describe('deduplicateToolCalls', () => {
  it('removes duplicate tool calls', () => {
    const calls = [
      { id: '1', type: 'function' as const, function: { name: 'file_read', arguments: '{"file_path":"/tmp/a.ts"}' } },
      { id: '2', type: 'function' as const, function: { name: 'file_read', arguments: '{"file_path":"/tmp/a.ts"}' } },
      { id: '3', type: 'function' as const, function: { name: 'file_read', arguments: '{"file_path":"/tmp/b.ts"}' } },
    ];
    const result = deduplicateToolCalls(calls);
    assert.equal(result.deduplicated.length, 2);
    assert.equal(result.removedCount, 1);
    assert.deepEqual(result.removedIds, ['2']);
  });

  it('keeps all unique calls', () => {
    const calls = [
      { id: '1', type: 'function' as const, function: { name: 'bash', arguments: '{"command":"ls"}' } },
      { id: '2', type: 'function' as const, function: { name: 'bash', arguments: '{"command":"pwd"}' } },
    ];
    const result = deduplicateToolCalls(calls);
    assert.equal(result.deduplicated.length, 2);
    assert.equal(result.removedCount, 0);
  });

  it('preserves first occurrence, removes later duplicates', () => {
    const calls = [
      { id: 'first', type: 'function' as const, function: { name: 'grep', arguments: '{"pattern":"TODO"}' } },
      { id: 'second', type: 'function' as const, function: { name: 'grep', arguments: '{"pattern":"TODO"}' } },
    ];
    const result = deduplicateToolCalls(calls);
    assert.equal(result.deduplicated[0].id, 'first');
    assert.deepEqual(result.removedIds, ['second']);
  });

  it('handles empty array', () => {
    const result = deduplicateToolCalls([]);
    assert.equal(result.deduplicated.length, 0);
    assert.equal(result.removedCount, 0);
  });

  it('handles single call', () => {
    const calls = [
      { id: '1', type: 'function' as const, function: { name: 'bash', arguments: '{"command":"echo hi"}' } },
    ];
    const result = deduplicateToolCalls(calls);
    assert.equal(result.deduplicated.length, 1);
    assert.equal(result.removedCount, 0);
  });

  it('handles malformed arguments gracefully', () => {
    const calls = [
      { id: '1', type: 'function' as const, function: { name: 'bash', arguments: 'not json' } },
      { id: '2', type: 'function' as const, function: { name: 'bash', arguments: 'not json' } },
    ];
    const result = deduplicateToolCalls(calls);
    assert.equal(result.deduplicated.length, 1);
    assert.equal(result.removedCount, 1);
  });

  it('never deduplicates write tools by default', () => {
    const calls = [
      { id: '1', type: 'function' as const, function: { name: 'file_edit', arguments: '{"file_path":"/tmp/a.ts","old_string":"x","new_string":"y"}' } },
      { id: '2', type: 'function' as const, function: { name: 'file_edit', arguments: '{"file_path":"/tmp/a.ts","old_string":"x","new_string":"y"}' } },
    ];
    const result = deduplicateToolCalls(calls);
    assert.equal(result.deduplicated.length, 1, 'identical write tools should still be deduped');
  });
});
