import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { scoutTool, scoutHooks, normalizeRepoUrl, cachedCloneMatches, clearScoutRepoCache } from '../src/tools/scout';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: '/tmp/scout-parity', abortSignal: new AbortController().signal };
}

const orig = { ...scoutHooks };
afterEach(() => { Object.assign(scoutHooks, orig); clearScoutRepoCache(); });

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
    // Distinct path so the negative (throwing) probe is not served from /x's cache.
    scoutHooks.execFileSync = (() => { throw new Error('not a repo'); }) as typeof scoutHooks.execFileSync;
    assert.equal(cachedCloneMatches('/err', 'https://github.com/o/r'), false);
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

// --- git source validation: block transport-helper RCE before any clone --------
describe('scout – git source safety gate', () => {
  it('rejects a transport-helper source (ext::) WITHOUT ever invoking git', async () => {
    let cloneCalled = false;
    scoutHooks.existsSync = (() => false) as typeof scoutHooks.existsSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = (() => { cloneCalled = true; return '' as unknown as Buffer; }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;

    const r = await scoutTool.execute({ repo: 'ext::sh -c "touch /tmp/pwned"' }, ctx());
    assert.equal(r.isError, true, 'dangerous transport source must be rejected');
    assert.match(r.output, /transport-helper/i, 'reason must name the transport-helper block');
    assert.equal(cloneCalled, false, 'git must NEVER be spawned for a rejected source');
  });

  it('rejects an argument-injection source (leading -)', async () => {
    let cloneCalled = false;
    scoutHooks.existsSync = (() => false) as typeof scoutHooks.existsSync;
    scoutHooks.execFileSync = (() => { cloneCalled = true; return '' as unknown as Buffer; }) as typeof scoutHooks.execFileSync;
    const r = await scoutTool.execute({ repo: '--upload-pack=touch /tmp/x' }, ctx());
    assert.equal(r.isError, true);
    assert.equal(cloneCalled, false, 'no clone for a flag-shaped source');
  });

  it('still accepts GitHub shorthand (owner/repo) and reaches the clone', async () => {
    let cloneCalled = false;
    scoutHooks.existsSync = (() => false) as typeof scoutHooks.existsSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
      if (args[0] === 'clone') cloneCalled = true;
      return '' as unknown as Buffer;
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;
    await scoutTool.execute({ repo: 'owner/repo' }, ctx());
    assert.equal(cloneCalled, true, 'valid shorthand must proceed to clone');
  });
});

// --- clone credential/prompt hardening: stdin ignored + ssh BatchMode ----------
describe('scout – clone prompt hardening', () => {
  it('ignores stdin and sets ssh BatchMode so git cannot hang on a prompt', async () => {
    let cloneOpts: { stdio?: unknown; env?: Record<string, string | undefined> } | undefined;
    scoutHooks.existsSync = (() => false) as typeof scoutHooks.existsSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = ((cmd: string, args: string[], opts: unknown) => {
      if (args[0] === 'clone') cloneOpts = opts as typeof cloneOpts;
      return '' as unknown as Buffer;
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;

    await scoutTool.execute({ repo: 'owner/repo' }, ctx());
    assert.ok(cloneOpts, 'clone ran');
    assert.deepEqual(cloneOpts!.stdio, ['ignore', 'pipe', 'pipe'], 'stdin must be ignored');
    assert.match(String(cloneOpts!.env?.GIT_SSH_COMMAND ?? ''), /BatchMode/i, 'ssh must run in BatchMode');
    assert.equal(cloneOpts!.env?.GIT_TERMINAL_PROMPT, '0', 'terminal prompt disabled');
  });
});

// --- truncation visibility: files / README / grep get explicit markers ----------
describe('scout – truncation markers', () => {
  // Reusable cached-repo setup: cache + .git present, origin matches → no re-clone.
  function cachedRepo(over: Partial<typeof scoutHooks>) {
    scoutHooks.existsSync = (() => true) as typeof scoutHooks.existsSync;
    scoutHooks.rmSync = (() => undefined) as typeof scoutHooks.rmSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
      if (args.includes('get-url')) return 'https://github.com/owner/repo\n';
      return '';
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;
    Object.assign(scoutHooks, over);
  }

  it('marks an over-cap file listing', async () => {
    const many = Array.from({ length: 55 }, (_, i) => `./f${i}.ts`).join('\n');
    cachedRepo({ execSync: (() => many as unknown as Buffer) as typeof scoutHooks.execSync });
    const r = await scoutTool.execute({ repo: 'owner/repo' }, ctx());
    assert.match(r.output, /showing first 50; more files exist/i, 'file truncation marker missing');
  });

  it('marks a README longer than the cap', async () => {
    cachedRepo({ readFileSync: (() => 'x'.repeat(5000) as unknown as Buffer) as typeof scoutHooks.readFileSync });
    const r = await scoutTool.execute({ repo: 'owner/repo' }, ctx());
    assert.match(r.output, /README truncated to first 3000 chars/i, 'README truncation marker missing');
  });

  it('reports grep match count when matches exceed the cap', async () => {
    const grepLines = Array.from({ length: 30 }, (_, i) => `./a.ts:${i}:hit`).join('\n');
    cachedRepo({
      execFileSync: ((cmd: string, args: string[]) => {
        if (args.includes('get-url')) return 'https://github.com/owner/repo\n';
        if (cmd === 'grep') return grepLines;
        return '';
      }) as typeof scoutHooks.execFileSync,
    });
    const r = await scoutTool.execute({ repo: 'owner/repo', query: 'hit' }, ctx());
    assert.match(r.output, /showing first 20 of 30 matches/i, 'grep truncation marker missing');
  });

  it('adds no marker when nothing is truncated', async () => {
    cachedRepo({ execSync: (() => './only.ts\n' as unknown as Buffer) as typeof scoutHooks.execSync });
    const r = await scoutTool.execute({ repo: 'owner/repo' }, ctx());
    assert.doesNotMatch(r.output, /more files exist|truncated|showing first/i, 'no marker for small output');
  });
});
