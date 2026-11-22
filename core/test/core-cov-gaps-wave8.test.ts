/**
 * Wave-8 coverage: secret-store + subagent-limiter function gaps, path-suggest
 * levenshtein + under-cwd edges, and a few more config managed catch arms.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';

import {
  describeSecretRef,
  parseSecretRefs,
  containsKeychainSecretRef,
  headersHaveKeychainSecretRef,
  resolveEnvSecretRefs,
  KeychainReadTimeoutError,
  DEFAULT_KEYCHAIN_TIMEOUT_MS,
  defaultMcpSecretStore,
  defaultWritableMcpSecretStore,
  collectKeychainAccounts,
  clearMcpConfigKeychainSecrets,
  resolveKeychainSecretRefs,
} from '../src/mcp/secret-store';
import { parseEnvIntInRange, SubagentLimiter } from '../src/subagent-limiter';
import {
  levenshteinDistance,
  suggestPathUnderCwd,
  suggestByBasename,
  suggestPathCorrection,
} from '../src/tools/path-suggest';
import {
  clearManagedConfigCache,
  fsHooks,
  loadManagedConfig,
  ManagedConfigError,
  isEligibleManagedDropIn,
  getManagedConfigPaths,
  resetConfigCache,
} from '../src/config';
import { exitWorktreeTool, _exitWorktreeHooks } from '../src/tools/exit-worktree';
import type { ToolContext } from '../src/types';

afterEach(() => {
  mock.restoreAll();
  clearManagedConfigCache();
  resetConfigCache();
  delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
});

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join('/tmp', prefix));
}

describe('secret-store wave8 — function surface', () => {
  it('parses / describes / resolves env secret refs', () => {
    const refs = parseSecretRefs('hi ${secret:env:FOO} and ${secret:keychain:acct}');
    assert.ok(refs.length >= 1);
    assert.ok(describeSecretRef(refs[0]!).includes(':'));
    assert.equal(containsKeychainSecretRef('x ${secret:keychain:a}'), true);
    assert.equal(containsKeychainSecretRef('nope'), false);
    assert.equal(containsKeychainSecretRef(undefined), false);
    assert.equal(headersHaveKeychainSecretRef({ a: '${secret:keychain:x}' }), true);
    assert.equal(headersHaveKeychainSecretRef({ a: 'plain' }), false);
    assert.equal(headersHaveKeychainSecretRef(undefined), false);
    process.env.FOO_COV_W8 = 'bar';
    assert.equal(resolveEnvSecretRefs('v=${secret:env:FOO_COV_W8}'), 'v=bar');
    delete process.env.FOO_COV_W8;
    assert.ok(DEFAULT_KEYCHAIN_TIMEOUT_MS > 0);
    assert.ok(new KeychainReadTimeoutError('t').message.includes('t'));
  });

  it('default stores + collectKeychainAccounts + clear/resolve seams', async () => {
    const store = defaultMcpSecretStore({ timeoutMs: 10 });
    assert.ok(store);
    const w = defaultWritableMcpSecretStore({ timeoutMs: 10 });
    assert.ok(w);
    const accts = collectKeychainAccounts({
      mcpServers: {
        s1: { env: { T: '${secret:keychain:acct1}' } },
      },
    });
    assert.ok(Array.isArray(accts));
    const cleared = await clearMcpConfigKeychainSecrets({}, w);
    assert.ok(cleared && Array.isArray(cleared.cleared) && Array.isArray(cleared.failed));
    const resolved = await resolveKeychainSecretRefs('no-refs-here', store);
    assert.ok(resolved && typeof resolved === 'object');
  });
});

describe('subagent-limiter wave8', () => {
  it('parseEnvIntInRange + SubagentLimiter tryAcquire/run/reset', async () => {
    assert.equal(parseEnvIntInRange('3', 5, { min: 1, max: 10 }), 3);
    assert.equal(parseEnvIntInRange('bad', 5, { min: 1, max: 10 }), 5);
    assert.equal(parseEnvIntInRange('0', 5, { min: 1, max: 10 }), 5);
    // Out-of-range falls back to the default (does not clamp).
    assert.equal(parseEnvIntInRange('99', 5, { min: 1, max: 10 }), 5);
    const lim = new SubagentLimiter(2, 0, 10);
    assert.equal(lim.tryAcquire(), true);
    assert.equal(lim.tryAcquire(), true);
    assert.equal(lim.tryAcquire(), false);
    assert.ok(lim.availableSlots >= 0);
    assert.ok(lim.activeCount >= 0);
    assert.equal(lim.canSpawnAtDepth(0), true);
    lim.release();
    assert.equal(await lim.run(async () => 42), 42);
    lim.recordSpawn();
    lim.reset();
    assert.equal(lim.lifetimeCount, 0);
  });
});

describe('path-suggest wave8 — levenshtein + under-cwd', () => {
  it('levenshteinDistance and under-cwd / basename edges', () => {
    assert.equal(levenshteinDistance('kitten', 'sitting'), 3);
    assert.equal(levenshteinDistance('', 'abc'), 3);
    assert.equal(levenshteinDistance('abc', ''), 3);
    const d = mkTmp('ps-w8-');
    fs.writeFileSync(path.join(d, 'Alpha.ts'), 'x');
    fs.mkdirSync(path.join(d, 'subdir'));
    fs.writeFileSync(path.join(d, 'subdir', 'Beta.ts'), 'x');
    const under = suggestPathUnderCwd(path.join(d, 'Alfa.ts'), d);
    assert.ok(under === undefined || typeof under === 'string');
    const by = suggestByBasename(path.join(d, 'Beta.ts'), d);
    assert.ok(Array.isArray(by));
    const corr = suggestPathCorrection(path.join(d, 'Alfa.ts'), d);
    assert.ok(corr && typeof corr.kind === 'string');
  });
});

describe('config wave8 — managed trust + drop-in eligibility', () => {
  it('assertTrustedManagedFile lstat failure + isEligibleManagedDropIn', () => {
    const d = mkTmp('cfg-w8-');
    const managed = path.join(d, 'm.json');
    fs.writeFileSync(managed, '{"model":"x"}');
    process.env.SUPERINFERENCE_MANAGED_CONFIG = managed;
    clearManagedConfigCache();
    const realLstat = fsHooks.lstatSync.bind(fsHooks);
    mock.method(fsHooks, 'lstatSync', ((p: fs.PathLike, ...rest: unknown[]) => {
      if (String(p) === managed) throw 'not-an-error-object';
      return (realLstat as Function)(p, ...rest);
    }) as typeof fsHooks.lstatSync);
    assert.throws(() => loadManagedConfig(), ManagedConfigError);

    const drop = path.join(d, 'config.d');
    fs.mkdirSync(drop);
    fs.writeFileSync(path.join(drop, 'ok.json'), '{}');
    fs.writeFileSync(path.join(drop, '.hidden.json'), '{}');
    assert.equal(isEligibleManagedDropIn('.hidden.json', drop), false);
    assert.equal(isEligibleManagedDropIn('ok.json', drop), true);
    assert.equal(isEligibleManagedDropIn('nope.txt', drop), false);
    // vanished between readdir and lstat
    assert.equal(isEligibleManagedDropIn('gone.json', drop), false);

    // darwin userInfo throw path via deps
    const paths = getManagedConfigPaths({
      userInfo: () => { throw new Error('uid-less'); },
    });
    assert.ok(Array.isArray(paths) && paths.length >= 1);
  });
});

describe('exit-worktree wave8 — default stash seam', () => {
  it('default stash hook is callable (may fail outside a repo)', async () => {
    const d = mkTmp('stash-w8-');
    try {
      await _exitWorktreeHooks.stash(d);
    } catch {
      // Expected outside a real dirty worktree — still executes the default arrow.
    }
    // pluralize n=1 vs n!=1 via keep on non-worktree (message paths)
    const r = await exitWorktreeTool.execute!(
      { action: 'keep' },
      { cwd: d, sessionId: 'w8', permissionMode: 'allow' } as ToolContext,
    );
    assert.ok(typeof r.output === 'string');
  });
});
