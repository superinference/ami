import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { PolicyMemo, PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// PolicyMemo — standalone tests
// ---------------------------------------------------------------------------

describe('PolicyMemo', () => {
  it('stores and retrieves a decision', () => {
    const memo = new PolicyMemo();
    memo.set('bash', 'abc123', 'allow');
    assert.equal(memo.get('bash', 'abc123'), 'allow');
  });

  it('returns null for missing entries', () => {
    const memo = new PolicyMemo();
    assert.equal(memo.get('bash', 'missing'), null);
  });

  it('returns null for expired entries', async () => {
    const memo = new PolicyMemo(50);
    memo.set('bash', 'key1', 'deny');
    assert.equal(memo.get('bash', 'key1'), 'deny');
    await new Promise(r => setTimeout(r, 80));
    assert.equal(memo.get('bash', 'key1'), null);
  });

  it('clear() flushes all entries', () => {
    const memo = new PolicyMemo();
    memo.set('bash', 'a', 'allow');
    memo.set('grep', 'b', 'deny');
    assert.equal(memo.size(), 2);
    memo.clear();
    assert.equal(memo.size(), 0);
    assert.equal(memo.get('bash', 'a'), null);
  });

  it('size() returns correct count', () => {
    const memo = new PolicyMemo();
    assert.equal(memo.size(), 0);
    memo.set('bash', 'a', 'allow');
    assert.equal(memo.size(), 1);
    memo.set('grep', 'b', 'deny');
    assert.equal(memo.size(), 2);
    memo.set('bash', 'a', 'deny');
    assert.equal(memo.size(), 2);
  });

  it('prune() removes expired entries', async () => {
    const memo = new PolicyMemo(50);
    memo.set('bash', 'a', 'allow');
    memo.set('grep', 'b', 'deny');
    await new Promise(r => setTimeout(r, 80));
    memo.set('file_edit', 'c', 'ask');
    const removed = memo.prune();
    assert.equal(removed, 2);
    assert.equal(memo.size(), 1);
    assert.equal(memo.get('file_edit', 'c'), 'ask');
  });

  it('evicts LRU entry when max entries exceeded', () => {
    const memo = new PolicyMemo(60_000);
    for (let i = 0; i < 1001; i++) {
      memo.set('tool', `key${i}`, 'allow');
    }
    assert.equal(memo.size(), 1000);
    assert.equal(memo.get('tool', 'key0'), null, 'oldest entry should be evicted');
    assert.equal(memo.get('tool', 'key1000'), 'allow', 'newest entry should survive');
  });

  it('separates entries by tool name', () => {
    const memo = new PolicyMemo();
    memo.set('bash', 'same_hash', 'allow');
    memo.set('grep', 'same_hash', 'deny');
    assert.equal(memo.get('bash', 'same_hash'), 'allow');
    assert.equal(memo.get('grep', 'same_hash'), 'deny');
  });

  it('overwrites existing entry on set', () => {
    const memo = new PolicyMemo();
    memo.set('bash', 'key', 'allow');
    assert.equal(memo.get('bash', 'key'), 'allow');
    memo.set('bash', 'key', 'deny');
    assert.equal(memo.get('bash', 'key'), 'deny');
    assert.equal(memo.size(), 1);
  });

  it('default TTL is 5 seconds', async () => {
    const memo = new PolicyMemo();
    memo.set('bash', 'key', 'allow');
    assert.equal(memo.get('bash', 'key'), 'allow');
    // Not expired after 10ms
    await new Promise(r => setTimeout(r, 10));
    assert.equal(memo.get('bash', 'key'), 'allow');
  });
});

// ---------------------------------------------------------------------------
// PermissionManager — memo integration
// ---------------------------------------------------------------------------

describe('PermissionManager memo integration', () => {
  it('caches check results when memo is enabled', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow' },
    ]);
    pm.enableMemo();

    const result1 = await pm.check('bash', { command: 'ls -la' });
    assert.equal(result1, 'allow');
    assert.equal(pm.getMemo()!.size(), 1);

    const result2 = await pm.check('bash', { command: 'ls -la' });
    assert.equal(result2, 'allow');
    assert.equal(pm.getMemo()!.size(), 1, 'should reuse cached entry');
  });

  it('does not cache when memo is disabled', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow' },
    ]);

    await pm.check('bash', { command: 'ls -la' });
    assert.equal(pm.getMemo(), null);
  });

  it('enableMemo/disableMemo toggles caching', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow' },
    ]);

    pm.enableMemo();
    assert.ok(pm.getMemo() !== null);

    await pm.check('bash', { command: 'ls -la' });
    assert.equal(pm.getMemo()!.size(), 1);

    pm.disableMemo();
    assert.equal(pm.getMemo(), null);
  });

  it('caches deny results', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'file_edit', action: 'deny' },
    ]);
    pm.enableMemo();

    const result = await pm.check('file_edit', { file_path: '/tmp/test.ts', old_string: 'a', new_string: 'b' });
    assert.equal(result, 'deny');
    assert.ok(pm.getMemo()!.size() >= 1);
  });

  it('caches default ask result', async () => {
    const pm = new PermissionManager('ask', []);
    pm.enableMemo();

    const result = await pm.check('bash', { command: 'some-unknown-command' });
    assert.equal(result, 'ask');
    assert.equal(pm.getMemo()!.size(), 1);
  });

  it('different inputs get separate cache entries', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow' },
    ]);
    pm.enableMemo();

    await pm.check('bash', { command: 'ls -la' });
    await pm.check('bash', { command: 'ls -R' });
    assert.equal(pm.getMemo()!.size(), 2);
  });

  it('enableMemo with custom TTL', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls *', action: 'allow' },
    ]);
    pm.enableMemo(50);

    await pm.check('bash', { command: 'ls -la' });
    assert.equal(pm.getMemo()!.size(), 1);

    await new Promise(r => setTimeout(r, 80));
    const result = await pm.check('bash', { command: 'ls -la' });
    assert.equal(result, 'allow');
    // After TTL expired, the old entry was evicted and a fresh one cached
    assert.equal(pm.getMemo()!.size(), 1);
  });
});
