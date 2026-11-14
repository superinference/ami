import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { scoutTool, scoutHooks, normalizeRepoUrl, cachedCloneMatches } from '../src/tools/scout';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: '/tmp/scout-parity', abortSignal: new AbortController().signal };
}

const orig = { ...scoutHooks };
afterEach(() => { Object.assign(scoutHooks, orig); });

describe('scout – normalizeRepoUrl', () => {
  it('drops credentials, trailing .git and slashes', () => {
    assert.equal(
      normalizeRepoUrl('https://user:tok@github.com/o/r.git/'),
      'https://github.com/o/r',
    );
    assert.equal(normalizeRepoUrl('https://github.com/o/r'), 'https://github.com/o/r');
  });
});

describe('scout – cachedCloneMatches', () => {
  it('true when origin matches, false on mismatch or error', () => {
    scoutHooks.execFileSync = (() => 'https://github.com/o/r.git\n') as typeof scoutHooks.execFileSync;
    assert.equal(cachedCloneMatches('/x', 'https://github.com/o/r'), true);
    assert.equal(cachedCloneMatches('/x', 'https://github.com/o/OTHER'), false);
    scoutHooks.execFileSync = (() => { throw new Error('not a repo'); }) as typeof scoutHooks.execFileSync;
    assert.equal(cachedCloneMatches('/x', 'https://github.com/o/r'), false);
  });
});

describe('scout – cache re-clone on repo mismatch', () => {
  it('purges and re-clones when the cached origin is a different repo', () => {
    let purged = false;
    let cloned = false;
    scoutHooks.existsSync = (() => true) as typeof scoutHooks.existsSync; // cache + .git present
    scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
      if (args.includes('get-url')) return 'https://github.com/someone/ELSE.git\n';
      if (args[0] === 'clone') { cloned = true; return ''; }
      return '';
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.rmSync = (() => { purged = true; }) as typeof scoutHooks.rmSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;

    // After purge the dir no longer exists; emulate that so the clone branch runs.
    let existsCalls = 0;
    scoutHooks.existsSync = ((p: string) => {
      existsCalls++;
      // First two calls (localPath, .git) see a present cache; after purge, absent.
      if (p.endsWith('.git')) return true;
      return existsCalls <= 1 ? true : purged ? false : true;
    }) as typeof scoutHooks.existsSync;

    const result = scoutTool.execute({ repo: 'owner/wanted' }, ctx());
    return result.then(() => {
      assert.equal(purged, true, 'mismatched cache must be purged');
      assert.equal(cloned, true, 'a fresh clone of the requested repo must run');
    });
  });
});
