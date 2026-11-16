/**
 * Parity (U140 / gap "managed-only policy filter"): a managed-only policy
 * predicate living in hook-protection.ts (the module that OWNS the hook
 * protection domain). Mirrors Claude Code's getHooksConfig →
 * shouldAllowManagedHooksOnly branch, which, when set, strips plugin-sourced
 * and session/frontmatter-derived hooks so only admin/managed hooks run.
 *
 * AMI's tier model can only reorder sources; this predicate lets the loader
 * EXCLUDE a non-managed source entirely by its provenance kind.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  isManagedHookSource,
  canRunHookUnderManagedOnly,
  filterManagedOnlyHooks,
  type HookSourceKind,
} from '../src/hook-protection';
import { HookManager } from '../src/hooks';

const ALL_KINDS: HookSourceKind[] = ['managed', 'plugin', 'project', 'user', 'config'];

describe('isManagedHookSource', () => {
  it('only the managed kind is managed', () => {
    assert.equal(isManagedHookSource('managed'), true);
    for (const k of ALL_KINDS.filter((k) => k !== 'managed')) {
      assert.equal(isManagedHookSource(k), false, `${k} must not be managed`);
    }
  });
});

describe('canRunHookUnderManagedOnly', () => {
  it('managed-only OFF lets every provenance run', () => {
    for (const k of ALL_KINDS) {
      assert.equal(canRunHookUnderManagedOnly(k, false), true, `${k} should run when policy off`);
    }
  });

  it('managed-only ON permits ONLY the managed kind', () => {
    assert.equal(canRunHookUnderManagedOnly('managed', true), true);
    for (const k of ALL_KINDS.filter((k) => k !== 'managed')) {
      assert.equal(canRunHookUnderManagedOnly(k, true), false, `${k} must be stripped under managed-only`);
    }
  });
});

describe('filterManagedOnlyHooks', () => {
  const regs = ALL_KINDS.map((kind) => ({ kind, id: kind }));

  it('passes everything through when policy is off', () => {
    assert.deepEqual(filterManagedOnlyHooks(regs, false), regs);
  });

  it('drops plugin, project, user, and config registrations when policy is on', () => {
    const kept = filterManagedOnlyHooks(regs, true);
    assert.deepEqual(kept.map((r) => r.kind), ['managed']);
  });

  it('is a stable identity-preserving filter (keeps original objects)', () => {
    const kept = filterManagedOnlyHooks(regs, true);
    assert.equal(kept[0], regs[0]);
  });
});

// ---------------------------------------------------------------------------
// E2E: the predicate is wired into HookManager.loadFromFile — a PLUGIN-sourced
// hook is stripped under allowManagedHooksOnly (Claude Code's getHooksConfig
// skips plugin matchers under the policy) but loads when the policy is off.
// ---------------------------------------------------------------------------
describe('HookManager.loadFromFile — managed-only strips plugin hooks (E2E)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mo-cwd-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mo-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    // A plugin bundle under <cwd>/.ami/plugins/<name> contributing a preToolUse hook.
    const bundle = path.join(tmp, '.ami', 'plugins', 'demo');
    fs.mkdirSync(path.join(bundle, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(bundle, '.ami-plugin', 'plugin.json'),
      JSON.stringify({ name: 'demo', version: '1.0.0' }),
    );
    fs.writeFileSync(
      path.join(bundle, 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo plugin' } }] }),
    );
    // A project filesystem hook (.ami/hooks.json) — a non-managed source that the
    // filterManagedOnlyHooks() call must strip under the policy.
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'postToolUse', hook: { type: 'command', command: 'echo project' } }] }),
    );
    // An admin/managed-settings hook (the only source honored under managed-only).
    const managed = path.join(home, 'managed.json');
    fs.writeFileSync(
      managed,
      JSON.stringify({ hooks: [{ event: 'notification', hook: { type: 'command', command: 'echo managed' } }] }),
    );
    process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = managed;
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  it('loads the plugin hook when managed-only is OFF', () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.loadFromFile(tmp);
    const sum = hm.getSummary();
    assert.equal(sum.preToolUse, 1, 'plugin preToolUse hook loads without the policy');
    assert.equal(sum.postToolUse, 1, 'project postToolUse hook loads without the policy');
    assert.equal(sum.notification, 1, 'managed hook also loads');
  });

  it('strips the plugin hook when managed-only is ON, keeping only the managed hook', () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
    hm.setAllowManagedHooksOnly(true);
    hm.loadFromFile(tmp);
    const sum = hm.getSummary();
    assert.equal(sum.preToolUse, 0, 'plugin preToolUse hook is stripped under managed-only');
    assert.equal(sum.postToolUse, 0, 'project postToolUse hook is stripped by the filter under managed-only');
    assert.equal(sum.notification, 1, 'managed hook still runs under managed-only');
  });
});
