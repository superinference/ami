import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  scoutTool,
  scoutHooks,
  detectCloneRemoteUrl,
  cachedCloneMatches,
  clearScoutRepoCache,
} from '../src/tools/scout';
import {
  shellHooks,
  execSyncWatched,
  execFileSyncWatched,
  SLOW_EXEC_THRESHOLD_ENV,
  type SlowCommandInfo,
} from '../src/utils/shell';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return { cwd: '/tmp/scout-detect-cache', abortSignal: new AbortController().signal };
}

const orig = { ...scoutHooks };
afterEach(() => {
  Object.assign(scoutHooks, orig);
  clearScoutRepoCache();
  delete process.env[SLOW_EXEC_THRESHOLD_ENV];
  mock.restoreAll();
});

// --- GAP: Repository detection with try/catch fallback + negative-result caching ---
describe('scout – detectCloneRemoteUrl (try/catch + negative caching)', () => {
  it('probes git once then serves the positive result from cache', () => {
    let calls = 0;
    scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
      assert.equal(cmd, 'git');
      assert.ok(args.includes('get-url'));
      calls++;
      return 'https://github.com/o/r.git\n';
    }) as typeof scoutHooks.execFileSync;

    assert.equal(detectCloneRemoteUrl('/cache/a'), 'https://github.com/o/r.git');
    assert.equal(detectCloneRemoteUrl('/cache/a'), 'https://github.com/o/r.git');
    assert.equal(calls, 1, 'origin must be probed exactly once, then cached');
  });

  it('caches a NEGATIVE result (throw → null) and does not re-probe', () => {
    let calls = 0;
    scoutHooks.execFileSync = (() => { calls++; throw new Error('not a repo'); }) as typeof scoutHooks.execFileSync;

    assert.equal(detectCloneRemoteUrl('/cache/bad'), null);
    assert.equal(detectCloneRemoteUrl('/cache/bad'), null);
    assert.equal(calls, 1, 'a failed probe caches null and is not retried');
  });

  it('treats an empty origin as a negative (null) result', () => {
    scoutHooks.execFileSync = (() => '   \n') as typeof scoutHooks.execFileSync;
    assert.equal(detectCloneRemoteUrl('/cache/empty'), null);
  });

  it('clearScoutRepoCache(path) forces a fresh probe for that path only', () => {
    let calls = 0;
    scoutHooks.execFileSync = (() => { calls++; return 'https://github.com/o/r\n'; }) as typeof scoutHooks.execFileSync;
    detectCloneRemoteUrl('/cache/x');
    detectCloneRemoteUrl('/cache/y');
    assert.equal(calls, 2);
    clearScoutRepoCache('/cache/x');
    detectCloneRemoteUrl('/cache/x'); // re-probed
    detectCloneRemoteUrl('/cache/y'); // still cached
    assert.equal(calls, 3, 'only the cleared path re-probes');
  });

  it('cachedCloneMatches is backed by the cache (single probe across calls)', () => {
    let calls = 0;
    scoutHooks.execFileSync = (() => { calls++; return 'https://github.com/o/r.git\n'; }) as typeof scoutHooks.execFileSync;
    assert.equal(cachedCloneMatches('/cache/m', 'https://github.com/o/r'), true);
    assert.equal(cachedCloneMatches('/cache/m', 'https://github.com/o/OTHER'), false);
    assert.equal(calls, 1, 'match comparisons reuse one cached origin probe');
  });
});

// --- GAP integration: a purge/re-clone invalidates the cached detection -----------
describe('scout – cache invalidation on purge/re-clone (end-to-end)', () => {
  it('re-detects the fresh clone origin after a mismatched cache is purged', async () => {
    // Seed a stale positive entry for the cache path the tool will compute.
    const projectState = await import('../src/reference-journal');
    const cacheDir = projectState.projectStateDir(ctx().cwd, 'scout-cache');
    const path = await import('path');
    const localPath = path.join(cacheDir, 'owner_wanted');

    let getUrlReturns = 'https://github.com/someone/ELSE.git\n';
    let cloned = false;
    let purged = false;

    scoutHooks.existsSync = ((p: string) => {
      if (p.endsWith('.git')) return true;
      // cache present until purged
      return !purged;
    }) as typeof scoutHooks.existsSync;
    scoutHooks.rmSync = (() => { purged = true; }) as typeof scoutHooks.rmSync;
    scoutHooks.mkdirSync = (() => undefined) as unknown as typeof scoutHooks.mkdirSync;
    scoutHooks.execFileSync = ((cmd: string, args: string[]) => {
      if (args.includes('get-url')) return getUrlReturns;
      if (args[0] === 'clone') { cloned = true; getUrlReturns = 'https://github.com/owner/wanted.git\n'; return ''; }
      return '';
    }) as typeof scoutHooks.execFileSync;
    scoutHooks.execSync = (() => '' as unknown as Buffer) as typeof scoutHooks.execSync;
    scoutHooks.readFileSync = (() => '' as unknown as Buffer) as typeof scoutHooks.readFileSync;

    // Pre-populate the cache with the stale origin so we can prove it is cleared.
    assert.equal(detectCloneRemoteUrl(localPath), 'https://github.com/someone/ELSE.git');

    await scoutTool.execute({ repo: 'owner/wanted' }, ctx());
    assert.equal(purged, true, 'mismatched cache purged');
    assert.equal(cloned, true, 'fresh clone ran');
    // After the purge the stale entry must be gone: a fresh probe returns the NEW origin.
    assert.equal(detectCloneRemoteUrl(localPath), 'https://github.com/owner/wanted.git');
  });
});

// --- GAP: Centralized execSync wrapper with slow/blocking-operation watchdog -------
describe('shell – synchronous exec watchdog boundary', () => {
  it('execSyncWatched reports a slow sync command via the shared onSlowCommand sink', () => {
    const seen: SlowCommandInfo[] = [];
    mock.method(shellHooks, 'onSlowCommand', (info: SlowCommandInfo) => { seen.push(info); });
    process.env[SLOW_EXEC_THRESHOLD_ENV] = '0'; // any duration >= 0 is "slow"
    const out = execSyncWatched('echo hi', { encoding: 'utf-8' }) as string;
    assert.match(out, /hi/);
    assert.equal(seen.length, 1, 'slow sync exec must be reported');
    assert.equal(seen[0]!.command, 'echo hi');
    assert.equal(seen[0]!.thresholdMs, 0);
  });

  it('does NOT report when the threshold env is unset (default: silent)', () => {
    const seen: SlowCommandInfo[] = [];
    mock.method(shellHooks, 'onSlowCommand', (info: SlowCommandInfo) => { seen.push(info); });
    delete process.env[SLOW_EXEC_THRESHOLD_ENV];
    execSyncWatched('echo hi', { encoding: 'utf-8' });
    assert.equal(seen.length, 0, 'no diagnostic without the opt-in env');
  });

  it('reports even when the sync command throws (non-zero exit), then re-throws', () => {
    const seen: SlowCommandInfo[] = [];
    mock.method(shellHooks, 'onSlowCommand', (info: SlowCommandInfo) => { seen.push(info); });
    process.env[SLOW_EXEC_THRESHOLD_ENV] = '0';
    assert.throws(() => execSyncWatched('exit 3', { stdio: 'pipe' }));
    assert.equal(seen.length, 1, 'a throwing slow command is still flagged (finally)');
  });

  it('execFileSyncWatched labels the diagnostic with file + args', () => {
    const seen: SlowCommandInfo[] = [];
    mock.method(shellHooks, 'onSlowCommand', (info: SlowCommandInfo) => { seen.push(info); });
    process.env[SLOW_EXEC_THRESHOLD_ENV] = '0';
    const out = execFileSyncWatched(process.execPath, ['-e', 'process.stdout.write("ok")'], { encoding: 'utf-8' }) as string;
    assert.equal(out, 'ok');
    assert.equal(seen.length, 1);
    assert.ok(seen[0]!.command.startsWith(process.execPath), 'label begins with the executable');
    assert.match(seen[0]!.command, /process\.stdout\.write/);
  });
});

// --- GAP wiring: scout routes its sync calls through the watchdog by default -------
describe('scout – wired to the sync watchdog wrappers', () => {
  it('uses execSyncWatched / execFileSyncWatched as the default hooks', () => {
    assert.equal(orig.execSync, execSyncWatched, 'find listing routes through the sync watchdog');
    assert.equal(orig.execFileSync, execFileSyncWatched, 'git/grep route through the sync watchdog');
  });
});
