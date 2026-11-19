import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  PLUGIN_CAPABILITIES,
  isPluginCapability,
  canonicalizePluginCapabilities,
  PLUGIN_CAPABILITY_CONSENT,
  describePluginConsent,
  gatePluginHostCall,
  PLUGIN_EXECUTION_TIERS,
  pluginTierAllows,
  derivePluginExecutionTier,
  computePluginConsentFingerprint,
  pluginUpdateAccessGrowth,
  pluginUpdateGrowsAccess,
  hashPluginTree,
  pluginTreeHashMatches,
  verifyPluginTreeHash,
  isAllowedPluginCloneUrl,
  parsePluginKillList,
  findKilledPlugin,
  isPluginKillListTooFarInFuture,
  PLUGIN_KILL_LIST_FUTURE_SKEW_MS,
  emptyPluginLockfile,
  parsePluginLockfile,
  upsertPluginLock,
  removePluginLock,
  type PluginLockEntry,
} from '../src/plugins';

const fp = computePluginConsentFingerprint;

// ---------------------------------------------------------------------------
// Least-privilege: closed-enum capabilities
// ---------------------------------------------------------------------------
test('canonicalizePluginCapabilities keeps only known capabilities, trims, lowercases, dedups, sorts', () => {
  const out = canonicalizePluginCapabilities([' Read-Files ', 'network', 'network', 'bogus', 42, 'RUN-COMMANDS']);
  assert.deepEqual(out, ['network', 'read-files', 'run-commands']);
});

test('canonicalizePluginCapabilities returns empty for non-array / unknown-only', () => {
  assert.deepEqual(canonicalizePluginCapabilities('read-files'), []);
  assert.deepEqual(canonicalizePluginCapabilities(['nope', 'also-nope']), []);
  assert.deepEqual(canonicalizePluginCapabilities(undefined), []);
});

test('isPluginCapability discriminates the closed enum', () => {
  assert.equal(isPluginCapability('read-files'), true);
  assert.equal(isPluginCapability('arbitrary'), false);
  assert.equal(isPluginCapability(5), false);
});

test('every capability has a consent string; describePluginConsent renders ordered lines', () => {
  for (const c of PLUGIN_CAPABILITIES) assert.equal(typeof PLUGIN_CAPABILITY_CONSENT[c], 'string');
  const lines = describePluginConsent(['run-commands', 'read-files']);
  assert.deepEqual(lines, [PLUGIN_CAPABILITY_CONSENT['read-files'], PLUGIN_CAPABILITY_CONSENT['run-commands']]);
});

// ---------------------------------------------------------------------------
// Deny-by-default host-call gate
// ---------------------------------------------------------------------------
test('gatePluginHostCall denies an unknown method', () => {
  const d = gatePluginHostCall({ grantedCapabilities: ['read-files'] }, 'fs.wat');
  assert.equal(d.granted, false);
  assert.equal((d as { code: string }).code, 'unknown_method');
});

test('gatePluginHostCall: null grant (disabled/stale) requires consent for any capability call', () => {
  const d = gatePluginHostCall({ grantedCapabilities: null }, 'fs.read');
  assert.equal(d.granted, false);
  assert.equal((d as { code: string }).code, 'consent_required');
});

test('gatePluginHostCall denies a capability the plugin lacks (deny-by-default)', () => {
  const d = gatePluginHostCall({ grantedCapabilities: ['read-files'] }, 'fs.write');
  assert.equal(d.granted, false);
  assert.equal((d as { code: string }).code, 'capability_denied');
});

test('gatePluginHostCall grants when the capability is held', () => {
  assert.deepEqual(gatePluginHostCall({ grantedCapabilities: ['write-files'] }, 'fs.write'), { granted: true });
});

test('gatePluginHostCall grants a no-capability method even with an empty grant', () => {
  assert.deepEqual(gatePluginHostCall({ grantedCapabilities: [] }, 'log'), { granted: true });
});

test('gatePluginHostCall: no-capability method still needs non-null grant (consent)', () => {
  const d = gatePluginHostCall({ grantedCapabilities: null }, 'log');
  assert.equal(d.granted, false);
  assert.equal((d as { code: string }).code, 'consent_required');
});

// ---------------------------------------------------------------------------
// Execution tiers
// ---------------------------------------------------------------------------
test('derivePluginExecutionTier is risk-proportional', () => {
  assert.equal(derivePluginExecutionTier({}), 'data');
  assert.equal(derivePluginExecutionTier({ hasSkills: true }), 'declarative');
  assert.equal(derivePluginExecutionTier({ capabilities: ['network'] }), 'declarative');
  assert.equal(derivePluginExecutionTier({ hasHooks: true }), 'hooks');
  assert.equal(derivePluginExecutionTier({ hasMcpServers: true }), 'hooks');
  assert.equal(derivePluginExecutionTier({ hasMain: true, hasSkills: true }), 'trusted-worker');
});

test('pluginTierAllows respects the ordering', () => {
  assert.equal(pluginTierAllows('hooks', 'declarative'), true);
  assert.equal(pluginTierAllows('declarative', 'hooks'), false);
  assert.equal(pluginTierAllows('data', 'data'), true);
  assert.equal(PLUGIN_EXECUTION_TIERS[0], 'data');
});

// ---------------------------------------------------------------------------
// Consent fingerprint + update growth
// ---------------------------------------------------------------------------
test('consent fingerprint is order-independent and reach-sensitive', () => {
  const a = computePluginConsentFingerprint({ capabilities: ['network', 'read-files'], tier: 'hooks' });
  const b = computePluginConsentFingerprint({ capabilities: ['read-files', 'network'], tier: 'hooks' });
  assert.equal(a, b);
  const c = computePluginConsentFingerprint({ capabilities: ['network', 'read-files', 'write-files'], tier: 'hooks' });
  assert.notEqual(a, c);
  const d = computePluginConsentFingerprint({ capabilities: ['network', 'read-files'], tier: 'trusted-worker' });
  assert.notEqual(a, d);
});

test('pluginUpdateGrowsAccess: content-only update keeps approval; widened reach re-prompts', () => {
  const installed = { capabilities: ['read-files' as const], tier: 'declarative' as const };
  // Same reach — no growth.
  assert.equal(pluginUpdateGrowsAccess(installed, { capabilities: ['read-files'], tier: 'declarative' }), false);
  // New capability.
  const g1 = pluginUpdateAccessGrowth(installed, { capabilities: ['read-files', 'network'], tier: 'declarative' });
  assert.deepEqual(g1.capabilities, ['network']);
  assert.equal(g1.tier, null);
  assert.equal(pluginUpdateGrowsAccess(installed, { capabilities: ['read-files', 'network'], tier: 'declarative' }), true);
  // Higher tier.
  const g2 = pluginUpdateAccessGrowth(installed, { capabilities: ['read-files'], tier: 'hooks' });
  assert.deepEqual(g2.capabilities, []);
  assert.equal(g2.tier, 'hooks');
  assert.equal(pluginUpdateGrowsAccess(installed, { capabilities: ['read-files'], tier: 'hooks' }), true);
  // Narrowing — no growth.
  assert.equal(pluginUpdateGrowsAccess({ capabilities: ['read-files', 'network'], tier: 'hooks' }, installed), false);
});

// ---------------------------------------------------------------------------
// Supply-chain: deterministic tree hash
// ---------------------------------------------------------------------------
function mkTree(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-tree-'));
  fs.mkdirSync(path.join(dir, 'sub'));
  fs.writeFileSync(path.join(dir, 'a.txt'), 'alpha');
  fs.writeFileSync(path.join(dir, 'sub', 'b.txt'), 'beta');
  return dir;
}

test('hashPluginTree is deterministic and sensitive to content, and ignores .git', () => {
  const d1 = mkTree();
  const d2 = mkTree();
  try {
    assert.match(hashPluginTree(d1), /^[0-9a-f]{64}$/);
    assert.equal(hashPluginTree(d1), hashPluginTree(d2));
    // .git is ignored by default.
    fs.mkdirSync(path.join(d1, '.git'));
    fs.writeFileSync(path.join(d1, '.git', 'HEAD'), 'ref: refs/heads/main');
    assert.equal(hashPluginTree(d1), hashPluginTree(d2));
    // A content change flips the hash.
    fs.writeFileSync(path.join(d1, 'a.txt'), 'ALPHA');
    assert.notEqual(hashPluginTree(d1), hashPluginTree(d2));
  } finally {
    fs.rmSync(d1, { recursive: true, force: true });
    fs.rmSync(d2, { recursive: true, force: true });
  }
});

test('hashPluginTree records symlinks without following them', () => {
  const d = mkTree();
  try {
    const before = hashPluginTree(d);
    fs.symlinkSync('a.txt', path.join(d, 'link'));
    const after = hashPluginTree(d);
    assert.notEqual(before, after); // a new symlink entry changes the digest
    // Re-pointing the symlink target changes the digest (target string is hashed).
    fs.unlinkSync(path.join(d, 'link'));
    fs.symlinkSync('sub/b.txt', path.join(d, 'link'));
    assert.notEqual(after, hashPluginTree(d));
  } finally {
    fs.rmSync(d, { recursive: true, force: true });
  }
});

test('verifyPluginTreeHash / pluginTreeHashMatches catch tampering and legacy prefix', () => {
  const d = mkTree();
  try {
    const h = hashPluginTree(d);
    assert.deepEqual(verifyPluginTreeHash(d, h), { ok: true });
    // Legacy 32-hex prefix accepted.
    assert.equal(pluginTreeHashMatches(h, h.slice(0, 32)), true);
    assert.deepEqual(verifyPluginTreeHash(d, h.slice(0, 32)), { ok: true });
    // Malformed expected.
    const bad = verifyPluginTreeHash(d, 'not-a-hash');
    assert.equal(bad.ok, false);
    // Tamper -> mismatch.
    fs.writeFileSync(path.join(d, 'a.txt'), 'tampered');
    const mis = verifyPluginTreeHash(d, h);
    assert.equal(mis.ok, false);
    assert.match((mis as { error: string }).error, /content hash mismatch/);
  } finally {
    fs.rmSync(d, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// Supply-chain: clone-URL allowlist
// ---------------------------------------------------------------------------
test('isAllowedPluginCloneUrl permits anonymous https and ssh, rejects risky forms', () => {
  assert.equal(isAllowedPluginCloneUrl('https://github.com/owner/repo.git'), true);
  assert.equal(isAllowedPluginCloneUrl('git@github.com:owner/repo.git'), true);
  assert.equal(isAllowedPluginCloneUrl('ssh://git@github.com/owner/repo.git'), true);
  // Password embedded.
  assert.equal(isAllowedPluginCloneUrl('https://user:pass@github.com/x/y.git'), false);
  // https with a username is rejected (anonymous only).
  assert.equal(isAllowedPluginCloneUrl('https://user@github.com/x/y.git'), false);
  // http / git / file schemes are rejected (stricter than validateGitSource).
  assert.equal(isAllowedPluginCloneUrl('http://github.com/x/y.git'), false);
  assert.equal(isAllowedPluginCloneUrl('git://github.com/x/y.git'), false);
  assert.equal(isAllowedPluginCloneUrl('file:///tmp/x'), false);
  // Option-injection user / dotty host.
  assert.equal(isAllowedPluginCloneUrl('-oProxyCommand=sh@github.com:x/y'), false);
  assert.equal(isAllowedPluginCloneUrl('git@..:x/y'), false);
  assert.equal(isAllowedPluginCloneUrl(''), false);
  assert.equal(isAllowedPluginCloneUrl(42 as unknown as string), false);
});

// ---------------------------------------------------------------------------
// Supply-chain: kill-list
// ---------------------------------------------------------------------------
test('parsePluginKillList parses, dedups, skips bad entries, finds and times out', () => {
  const now = Date.now();
  const list = parsePluginKillList({
    version: 1,
    generatedAt: new Date(now).toISOString(),
    entries: [
      { key: 'evil.plugin', reason: 'malware' },
      { key: 'evil.plugin', reason: 'dup ignored' },
      { pluginKey: 'bad.skill', reason: 'typosquat', advisoryUrl: 'https://example.com/a' },
      { key: 'no-reason' },
      { key: 'bad-url', reason: 'x', advisoryUrl: 'http://insecure' },
    ],
  });
  assert.ok(list);
  assert.equal(list!.entries.length, 3);
  assert.equal(findKilledPlugin(list!, 'evil.plugin')?.reason, 'malware');
  assert.equal(findKilledPlugin(list!, 'bad.skill')?.advisoryUrl, 'https://example.com/a');
  assert.equal(findKilledPlugin(list!, 'bad-url')?.advisoryUrl, undefined); // http stripped
  assert.equal(findKilledPlugin(list!, 'missing'), null);
  assert.equal(isPluginKillListTooFarInFuture(list!, now), false);
});

test('parsePluginKillList rejects malformed envelopes and future lists', () => {
  assert.equal(parsePluginKillList({ version: 2, generatedAt: new Date().toISOString(), entries: [] }), null);
  assert.equal(parsePluginKillList({ version: 1, generatedAt: 'nope', entries: [] }), null);
  assert.equal(parsePluginKillList(null), null);
  const future = parsePluginKillList({
    version: 1,
    generatedAt: new Date(Date.now() + 10 * PLUGIN_KILL_LIST_FUTURE_SKEW_MS).toISOString(),
    entries: [{ key: 'x.y', reason: 'z' }],
  });
  assert.ok(future);
  assert.equal(isPluginKillListTooFarInFuture(future!), true);
});

// ---------------------------------------------------------------------------
// Supply-chain: lockfile
// ---------------------------------------------------------------------------
test('lockfile upsert/remove/parse round-trips and enforces key integrity', () => {
  const entry: PluginLockEntry = {
    pluginKey: 'my-plugin',
    version: '1.0.0',
    source: 'https://github.com/owner/repo.git',
    resolvedCommit: 'a'.repeat(40),
    contentHash: 'b'.repeat(64),
    consentFingerprint: fp({ capabilities: ['read-files'], tier: 'declarative' }),
    installedAt: 123,
  };
  let lock = upsertPluginLock(emptyPluginLockfile(), entry);
  assert.deepEqual(parsePluginLockfile(lock).plugins['my-plugin'], entry);
  // Round-trip through JSON.
  assert.deepEqual(parsePluginLockfile(JSON.parse(JSON.stringify(lock))).plugins['my-plugin'], entry);
  lock = removePluginLock(lock, 'my-plugin');
  assert.deepEqual(lock.plugins, {});
  // Key-integrity: a record whose pluginKey disagrees with its map key is dropped.
  const bad = parsePluginLockfile({ version: 1, plugins: { 'a': { ...entry, pluginKey: 'b' } } });
  assert.deepEqual(bad.plugins, {});
  // Invalid content hash dropped; corrupt envelope -> empty.
  const badHash = parsePluginLockfile({ version: 1, plugins: { 'a': { ...entry, pluginKey: 'a', contentHash: 'zz' } } });
  assert.deepEqual(badHash.plugins, {});
  assert.deepEqual(parsePluginLockfile({ version: 9 }), emptyPluginLockfile());
});
