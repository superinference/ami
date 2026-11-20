/**
 * U010 (w86) — inherited per-directory FOLDER-TRUST: `trustedProjectPaths` allowlist +
 * parent-directory traversal (`isPathTrusted`) + true-only latch cache.
 *
 * Parity: the reference's `computeTrustDialogAccepted` / `isPathTrusted`, which persist
 * per-project trust and resolve it by walking from the normalized cwd up through every
 * ancestor (trusting a parent implies trust of all children), behind a latch that caches
 * only the positive result so trust never silently revokes mid-session.
 *
 * AMI previously honored only the single-workspace `hasTrustDialogAccepted` boolean for
 * exactly `cwd` — no ancestor inheritance, no latch. This adds the inherited allowlist
 * while keeping the boolean behavior as a strict superset.
 *
 * Covers:
 *  - `trustedProjectPaths` is a recognized, type-validated, schema-documented key;
 *  - it ACCUMULATES across config layers (union merge);
 *  - `normalizeTrustPath` canonicalizes `.`/`..`/trailing-sep spellings;
 *  - `isPathTrusted` trusts cwd, a listed dir, and any descendant of a listed dir;
 *  - a never-listed sibling stays untrusted (control);
 *  - the latch keeps a once-trusted path trusted after the grant is removed (RED-GREEN:
 *    without the latch the second check returns false);
 *  - END-TO-END: `HookManager.loadFromFile` loads a project hook in a SUBDIRECTORY of a
 *    trusted parent path (RED on revert of the ancestor-walk).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  mergeConfigs,
  validateProjectConfigValues,
  KNOWN_CONFIG_KEYS,
  getConfigJSONSchema,
  normalizeTrustPath,
  isPathTrusted,
  resolveWorkspaceTrustAccepted,
  type ProjectConfig,
} from '../src/config';
import { HookManager } from '../src/hooks';

describe('U010 trustedProjectPaths — config key', () => {
  it('is a recognized top-level config key', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('trustedProjectPaths'),
      'the inherited-trust allowlist must be a known key so it is not flagged as a typo');
  });

  it('flags a non-array value as an invalid-value issue', () => {
    const issues = validateProjectConfigValues({ trustedProjectPaths: '/a' as unknown as string[] });
    const issue = issues.find(i => i.field === 'trustedProjectPaths');
    assert.ok(issue, 'a string value must be reported as invalid');
    assert.match(issue!.message, /string\[\]/);
  });

  it('flags a non-string element as invalid', () => {
    const issues = validateProjectConfigValues({ trustedProjectPaths: ['/ok', 3 as unknown as string] });
    assert.ok(issues.some(i => i.field === 'trustedProjectPaths'), 'a numeric element must be rejected');
  });

  it('accepts a string[] value without issue', () => {
    assert.equal(
      validateProjectConfigValues({ trustedProjectPaths: ['/home/a', '/home/b'] }).some(i => i.field === 'trustedProjectPaths'),
      false);
  });

  it('appears in the emitted JSON schema as an array property', () => {
    const schema = getConfigJSONSchema();
    const node = schema.properties.trustedProjectPaths;
    assert.ok(node, 'the key must be documented in the config JSON schema');
    assert.equal(node.type, 'array');
  });

  it('ACCUMULATES across layers (union, not clobber)', () => {
    const merged = mergeConfigs(
      {}, {},
      { trustedProjectPaths: ['/home/project'] },   // project layer
      { trustedProjectPaths: ['/home/global'] },    // global layer
    );
    assert.deepEqual([...(merged.trustedProjectPaths ?? [])].sort(), ['/home/global', '/home/project']);
  });
});

describe('U010 normalizeTrustPath', () => {
  it('canonicalizes . / .. / trailing separators to one absolute form', () => {
    const base = path.resolve('/home/user/project');
    assert.equal(normalizeTrustPath('/home/user/project/'), base);
    assert.equal(normalizeTrustPath('/home/user/./project'), base);
    assert.equal(normalizeTrustPath('/home/user/x/../project'), base);
  });
});

describe('U010 isPathTrusted — inheritance + latch', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ptrust-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ptrust-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.HOME = home;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(home, 'no-such-managed.json');
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG; else process.env.SUPERINFERENCE_MANAGED_CONFIG = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function writeGlobalConfig(cfg: ProjectConfig): void {
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify(cfg));
  }

  it('trusts a directory listed exactly in trustedProjectPaths', () => {
    const dir = fs.mkdtempSync(path.join(tmp, 'exact-'));
    writeGlobalConfig({ trustedProjectPaths: [dir] });
    assert.equal(isPathTrusted(dir), true);
  });

  it('trusts a DESCENDANT of a listed parent path (inheritance)', () => {
    const parent = fs.mkdtempSync(path.join(tmp, 'parent-'));
    const child = path.join(parent, 'a', 'b', 'c');
    fs.mkdirSync(child, { recursive: true });
    writeGlobalConfig({ trustedProjectPaths: [parent] });
    assert.equal(isPathTrusted(child), true, 'trusting a parent implies trust of all children');
  });

  it('does NOT trust a sibling that is not under any listed path (control)', () => {
    const parent = fs.mkdtempSync(path.join(tmp, 'tparent-'));
    const sibling = fs.mkdtempSync(path.join(tmp, 'sibling-'));
    writeGlobalConfig({ trustedProjectPaths: [parent] });
    assert.equal(isPathTrusted(sibling), false);
  });

  it('is false with an empty/absent allowlist and no boolean grant', () => {
    const dir = fs.mkdtempSync(path.join(tmp, 'none-'));
    writeGlobalConfig({ trustedProjectPaths: [] });
    assert.equal(isPathTrusted(dir), false);
  });

  it('still honors the single-workspace hasTrustDialogAccepted boolean (superset)', () => {
    const dir = fs.mkdtempSync(path.join(tmp, 'bool-'));
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ hasTrustDialogAccepted: true }));
    assert.equal(isPathTrusted(dir), true);
    assert.equal(resolveWorkspaceTrustAccepted(dir), true, 'alias resolves identically');
  });

  // RED-GREEN for the latch: without the true-only cache the second check (after the
  // grant is removed) would return false.
  it('LATCHES a once-trusted path even after the grant is removed', () => {
    const dir = fs.mkdtempSync(path.join(tmp, 'latch-'));
    writeGlobalConfig({ trustedProjectPaths: [dir] });
    assert.equal(isPathTrusted(dir), true, 'trusted on first resolution');
    // Remove the grant entirely.
    fs.rmSync(path.join(home, '.ami', 'config.json'), { force: true });
    assert.equal(isPathTrusted(dir), true, 'latch keeps it trusted — trust never silently revokes mid-session');
  });

  it('does NOT latch a false result (a later grant still takes effect)', () => {
    const dir = fs.mkdtempSync(path.join(tmp, 'nolatch-'));
    assert.equal(isPathTrusted(dir), false, 'untrusted initially');
    writeGlobalConfig({ trustedProjectPaths: [dir] });
    assert.equal(isPathTrusted(dir), true, 'a grant added after a false check still promotes trust');
  });
});

describe('U010 HookManager end-to-end — inherited trust loads a subdir project hook', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ptrust-hk-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ptrust-hk-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.HOME = home;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(home, 'no-such-managed.json');
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG; else process.env.SUPERINFERENCE_MANAGED_CONFIG = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  it('loads a project hook in a subdirectory of a trusted parent path', () => {
    const parent = fs.mkdtempSync(path.join(tmp, 'ws-'));
    const sub = path.join(parent, 'packages', 'app');
    fs.mkdirSync(path.join(sub, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(sub, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo sub' } }] }));
    // Only the PARENT is trusted — the subdirectory inherits it via the ancestor walk.
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify({ trustedProjectPaths: [parent] }));

    const hm = new HookManager();     // no explicit setWorkspaceTrusted — trust must come from the inherited config
    hm.setCwd(sub);
    hm.loadFromFile(sub);
    // RED (revert the ancestor-walk in isPathTrusted): sub is not listed → untrusted → 0.
    assert.equal(hm.getSummary().preToolUse, 1, 'inherited parent trust must let the subdir project hook load');
    assert.equal(hm.isWorkspaceTrusted(), true, 'inherited trust promotes workspace trust');
  });
});
