import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { BashResultCache, isReadOnlyBashCommand } from '../src/tools/bash-cache';

describe('isReadOnlyBashCommand', () => {
  it('identifies ls as read-only', () => {
    assert.equal(isReadOnlyBashCommand('ls -la'), true);
  });

  it('identifies cat as read-only', () => {
    assert.equal(isReadOnlyBashCommand('cat /tmp/test.txt'), true);
  });

  it('identifies grep as read-only', () => {
    assert.equal(isReadOnlyBashCommand('grep -r TODO src/'), true);
  });

  it('identifies find as read-only', () => {
    assert.equal(isReadOnlyBashCommand('find . -name "*.ts"'), true);
  });

  it('identifies wc as read-only', () => {
    assert.equal(isReadOnlyBashCommand('wc -l file.txt'), true);
  });

  it('identifies head/tail as read-only', () => {
    assert.equal(isReadOnlyBashCommand('head -20 file.txt'), true);
    assert.equal(isReadOnlyBashCommand('tail -f log.txt'), true);
  });

  it('git status/log/diff are read-only', () => {
    assert.equal(isReadOnlyBashCommand('git status'), true);
    assert.equal(isReadOnlyBashCommand('git log --oneline -5'), true);
    assert.equal(isReadOnlyBashCommand('git diff'), true);
  });

  it('identifies rm as NOT read-only', () => {
    assert.equal(isReadOnlyBashCommand('rm -rf /tmp/test'), false);
  });

  it('identifies echo with redirect as NOT read-only', () => {
    assert.equal(isReadOnlyBashCommand('echo "hello" > file.txt'), false);
  });

  it('identifies mv as NOT read-only', () => {
    assert.equal(isReadOnlyBashCommand('mv a.txt b.txt'), false);
  });

  it('identifies cp as NOT read-only', () => {
    assert.equal(isReadOnlyBashCommand('cp a.txt b.txt'), false);
  });

  it('identifies npm install as NOT read-only', () => {
    assert.equal(isReadOnlyBashCommand('npm install express'), false);
  });

  it('piped read-only commands are read-only', () => {
    assert.equal(isReadOnlyBashCommand('cat file.txt | grep TODO | wc -l'), true);
  });

  it('pipe ending with write command is NOT read-only', () => {
    assert.equal(isReadOnlyBashCommand('cat file.txt | tee output.txt'), false);
  });
});

describe('BashResultCache', () => {
  it('caches and retrieves a result', () => {
    const cache = new BashResultCache();
    cache.set('ls -la', '/tmp', 'file1.ts\nfile2.ts');
    const hit = cache.get('ls -la', '/tmp');
    assert.equal(hit, 'file1.ts\nfile2.ts');
  });

  it('returns null for uncached command', () => {
    const cache = new BashResultCache();
    assert.equal(cache.get('ls', '/tmp'), null);
  });

  it('distinguishes different cwds', () => {
    const cache = new BashResultCache();
    cache.set('ls', '/tmp', 'tmp-files');
    cache.set('ls', '/home', 'home-files');
    assert.equal(cache.get('ls', '/tmp'), 'tmp-files');
    assert.equal(cache.get('ls', '/home'), 'home-files');
  });

  it('invalidates on clear', () => {
    const cache = new BashResultCache();
    cache.set('ls', '/tmp', 'files');
    cache.clear();
    assert.equal(cache.get('ls', '/tmp'), null);
  });

  it('invalidateForCwd removes entries for a specific cwd', () => {
    const cache = new BashResultCache();
    cache.set('ls', '/tmp', 'tmp-files');
    cache.set('ls', '/home', 'home-files');
    cache.invalidateForCwd('/tmp');
    assert.equal(cache.get('ls', '/tmp'), null);
    assert.equal(cache.get('ls', '/home'), 'home-files');
  });

  it('evicts oldest entries when capacity exceeded', () => {
    const cache = new BashResultCache(3);
    cache.set('cmd1', '/tmp', 'r1');
    cache.set('cmd2', '/tmp', 'r2');
    cache.set('cmd3', '/tmp', 'r3');
    cache.set('cmd4', '/tmp', 'r4');
    assert.equal(cache.get('cmd1', '/tmp'), null, 'oldest entry should be evicted');
    assert.equal(cache.get('cmd4', '/tmp'), 'r4');
  });

  it('respects TTL', async () => {
    const cache = new BashResultCache(100, 50); // 50ms TTL
    cache.set('ls', '/tmp', 'files');
    assert.equal(cache.get('ls', '/tmp'), 'files');
    await new Promise(r => setTimeout(r, 60));
    assert.equal(cache.get('ls', '/tmp'), null, 'expired entry should return null');
  });

  it('size tracks current entry count', () => {
    const cache = new BashResultCache();
    assert.equal(cache.size, 0);
    cache.set('ls', '/tmp', 'files');
    assert.equal(cache.size, 1);
    cache.set('pwd', '/tmp', '/tmp');
    assert.equal(cache.size, 2);
  });
});
