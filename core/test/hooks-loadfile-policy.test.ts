/**
 * Parity tests for hook loading policies & matcher scoping (Claude->AMI parity):
 *  - matcherAppliesToCtx / HOOK_MATCHER_FIELD: matcher scoping on non-tool events.
 *  - isHookEqual + content-identity de-dup across overlapping sources.
 *  - allowManagedHooksOnly: only admin/managed-settings hooks execute.
 *  - Disabled plugins (.disabled marker) do not contribute executable hooks.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  isHookEqual,
  matcherAppliesToCtx,
  HOOK_MATCHER_FIELD,
} from '../src/hooks';

// ---------------------------------------------------------------------------
// matcherAppliesToCtx / HOOK_MATCHER_FIELD (H30/H31)
// ---------------------------------------------------------------------------

describe('matcherAppliesToCtx', () => {
  it('no matcher always applies', () => {
    assert.equal(matcherAppliesToCtx('notification', undefined, { notificationType: 'x' }), true);
  });
  it('matches the per-event field value', () => {
    assert.equal(matcherAppliesToCtx('notification', 'idle', { notificationType: 'idle' }), true);
    assert.equal(matcherAppliesToCtx('notification', 'idle', { notificationType: 'other' }), false);
  });
  it('maps elicitation to mcpServerName and configChange to source', () => {
    assert.equal(HOOK_MATCHER_FIELD.elicitation, 'mcpServerName');
    assert.equal(HOOK_MATCHER_FIELD.configChange, 'source');
    assert.equal(matcherAppliesToCtx('elicitation', 'srvA', { mcpServerName: 'srvA' }), true);
    assert.equal(matcherAppliesToCtx('configChange', 'settings.*', { source: 'settings.json' }), true);
  });
  it('an event with no matchable field treats the matcher as a no-op', () => {
    assert.equal(matcherAppliesToCtx('sessionEnd', 'anything', {}), true);
  });
  it('a missing field value only matches a wildcard', () => {
    assert.equal(matcherAppliesToCtx('notification', '*', {}), true);
    assert.equal(matcherAppliesToCtx('notification', 'specific', {}), false);
  });
});

// ---------------------------------------------------------------------------
// isHookEqual (H33)
// ---------------------------------------------------------------------------

describe('isHookEqual', () => {
  it('same type + content + if are equal (timeout ignored)', () => {
    assert.equal(isHookEqual(
      { type: 'command', command: 'x', timeout: 1 },
      { type: 'command', command: 'x', timeout: 999 },
    ), true);
  });
  it('different if makes hooks distinct', () => {
    assert.equal(isHookEqual(
      { type: 'command', command: 'x', if: 'Bash(git *)' },
      { type: 'command', command: 'x', if: 'Bash(npm *)' },
    ), false);
  });
  it('different type or content is not equal', () => {
    assert.equal(isHookEqual({ type: 'command', command: 'x' }, { type: 'prompt', prompt: 'x' }), false);
    assert.equal(isHookEqual({ type: 'http', url: 'a' }, { type: 'http', url: 'b' }), false);
    assert.equal(isHookEqual({ type: 'agent', prompt: 'p' }, { type: 'agent', prompt: 'p' }), true);
  });
});

// ---------------------------------------------------------------------------
// loadFromFile policies — isolate HOME so real user/.claude hooks don't leak in
// ---------------------------------------------------------------------------

describe('HookManager — loadFromFile policies', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-loadpol-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-home-'));
    origHome = process.env.HOME;
    origManaged = process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    process.env.HOME = home;
    delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setCwd(tmp);
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH; else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  function writeAmi(obj: unknown): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(tmp, '.ami', 'hooks.json'), JSON.stringify(obj));
  }
  function writeClaude(obj: unknown): void {
    fs.mkdirSync(path.join(tmp, '.claude'), { recursive: true });
    fs.writeFileSync(path.join(tmp, '.claude', 'settings.json'), JSON.stringify(obj));
  }

  it('de-dups an identical hook defined in two sources', () => {
    writeAmi({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo hi' } }] });
    writeClaude({ hooks: { PreToolUse: [{ hooks: [{ type: 'command', command: 'echo hi' }] }] } });
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'identical hook registers once');
  });

  it('keeps distinct hooks (different command) from two sources', () => {
    writeAmi({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo a' } }] });
    writeClaude({ hooks: { PreToolUse: [{ hooks: [{ type: 'command', command: 'echo b' }] }] } });
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 2);
  });

  it('allowManagedHooksOnly loads only managed-settings hooks', () => {
    const managed = path.join(home, 'managed.json');
    fs.writeFileSync(managed, JSON.stringify({ hooks: [{ event: 'notification', hook: { type: 'command', command: 'echo managed' } }] }));
    process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = managed;
    writeAmi({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo project' } }] });

    hm.setAllowManagedHooksOnly(true);
    hm.loadFromFile(tmp);
    const sum = hm.getSummary();
    assert.equal(sum.notification, 1, 'managed hook loads');
    assert.equal(sum.preToolUse, 0, 'project hook is dropped under managed-only');
  });

  it('without managed-only, both managed and project hooks load', () => {
    const managed = path.join(home, 'managed.json');
    fs.writeFileSync(managed, JSON.stringify({ hooks: [{ event: 'notification', hook: { type: 'command', command: 'echo managed' } }] }));
    process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = managed;
    writeAmi({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo project' } }] });

    hm.loadFromFile(tmp);
    const sum = hm.getSummary();
    assert.equal(sum.notification, 1);
    assert.equal(sum.preToolUse, 1);
  });

  it('a disabled plugin (.disabled marker) contributes no hooks', () => {
    const makePlugin = (disabled: boolean) => {
      const root = path.join(tmp, '.ami', 'plugins', 'p1');
      fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
      fs.writeFileSync(path.join(root, '.ami-plugin', 'plugin.json'), JSON.stringify({ name: 'p1', version: '1.0.0' }));
      fs.writeFileSync(path.join(root, 'hooks.json'), JSON.stringify({ hooks: [{ event: 'notification', hook: { type: 'command', command: 'echo plug' } }] }));
      if (disabled) fs.writeFileSync(path.join(root, '.disabled'), '');
    };

    makePlugin(false);
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().notification, 1, 'enabled plugin hook loads');

    const hm2 = new HookManager();
    hm2.setWorkspaceTrusted(true);
    hm2.setCwd(tmp);
    fs.writeFileSync(path.join(tmp, '.ami', 'plugins', 'p1', '.disabled'), '');
    hm2.loadFromFile(tmp);
    assert.equal(hm2.getSummary().notification, 0, 'disabled plugin hook is skipped');
  });

  it('matcher scopes a non-tool hook to its fieldToMatch (configChange source)', async () => {
    writeAmi({ hooks: [{ event: 'configChange', matcher: 'settings\\.json', hook: { type: 'command', command: 'exit 2' } }] });
    hm.loadFromFile(tmp);
    // Matching source → the (blocking) hook runs and vetoes.
    const blocked = await hm.executeConfigChange({ source: 'settings.json' });
    assert.ok(blocked && blocked.blockingErrors.length === 1, 'matching source runs the hook');
    // Non-matching source → hook is skipped, no veto.
    const ok = await hm.executeConfigChange({ source: 'other.json' });
    assert.equal(ok, undefined, 'non-matching source skips the hook');
  });
});
