import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  HookManager,
  hookContentLabel,
  getHookDisplayText,
  hookSourceDisplayString,
} from '../src/hooks';

// U007-w66 — per-hook plugin provenance + display-label introspection
// (Claude Code's ViewHookMode: hookSourceDescriptionDisplayString + pluginName,
// per-type content label, and statusMessage display text).

describe('U007-w66 G14 — hookContentLabel: per-type content field label', () => {
  it('command→Command, http→URL, prompt/agent→Prompt (agent reuses the prompt label)', () => {
    assert.equal(hookContentLabel('command'), 'Command');
    assert.equal(hookContentLabel('http'), 'URL');
    assert.equal(hookContentLabel('prompt'), 'Prompt');
    assert.equal(hookContentLabel('agent'), 'Prompt');
  });
});

describe('U007-w66 G129 — getHookDisplayText: statusMessage || content', () => {
  it('prefers a non-blank statusMessage over the raw content', () => {
    assert.equal(
      getHookDisplayText({ statusMessage: 'Running security scan…', content: 'bash scan.sh --all' }),
      'Running security scan…',
    );
  });
  it('falls back to content when statusMessage is absent or blank', () => {
    assert.equal(getHookDisplayText({ content: 'echo hi' }), 'echo hi');
    assert.equal(getHookDisplayText({ statusMessage: '   ', content: 'echo hi' }), 'echo hi');
  });
});

describe('U007-w66 G12/G15 — hookSourceDisplayString: human source + plugin attribution', () => {
  it('maps each tier to a human-readable label', () => {
    assert.equal(hookSourceDisplayString('project'), 'Project settings');
    assert.equal(hookSourceDisplayString('user'), 'User settings');
    assert.equal(hookSourceDisplayString('config'), 'Project config');
    assert.equal(hookSourceDisplayString('managed'), 'Managed policy');
    assert.equal(hookSourceDisplayString('plugin'), 'Plugin');
  });
  it('appends the plugin name in parentheses for a plugin-contributed hook', () => {
    assert.equal(hookSourceDisplayString('project', 'my-guardrails'), 'Project settings (my-guardrails)');
    assert.equal(hookSourceDisplayString('plugin', 'acme'), 'Plugin (acme)');
  });
  it('passes an unknown source through verbatim', () => {
    assert.equal(hookSourceDisplayString('weird'), 'weird');
  });
});

describe('U007-w66 G110/G115 — plugin provenance retained on loaded hooks', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w66-'));
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w66-home-'));
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
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  /** Create a project-scoped plugin bundle under <cwd>/.ami/plugins/<name>. */
  function writePluginBundle(name: string, hooks: unknown[]): void {
    const root = path.join(tmp, '.ami', 'plugins', name);
    fs.mkdirSync(path.join(root, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(root, '.ami-plugin', 'plugin.json'),
      JSON.stringify({ name, version: '1.0.0' }),
    );
    fs.writeFileSync(path.join(root, 'hooks.json'), JSON.stringify({ hooks }));
  }

  it('records the originating plugin name on a plugin-contributed hook descriptor', () => {
    writePluginBundle('my-guardrails', [
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'echo guard' } },
    ]);
    hm.loadFromFile(tmp);
    const descs = hm.describeHooks().filter(d => d.event === 'preToolUse');
    assert.equal(descs.length, 1, 'plugin hook is loaded');
    assert.equal(descs[0].pluginName, 'my-guardrails', 'descriptor attributes the hook to its plugin');
  });

  it('a filesystem (non-plugin) hook carries no pluginName', () => {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo fs' } }] }),
    );
    hm.loadFromFile(tmp);
    const d = hm.describeHooks().find(x => x.event === 'preToolUse');
    assert.ok(d, 'filesystem hook loaded');
    assert.equal(d!.pluginName, undefined, 'no plugin attribution for a .ami/hooks.json hook');
  });

  it('retains a hook-author statusMessage on the descriptor', () => {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({
        hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'bash scan.sh', statusMessage: 'Running security scan…' } }],
      }),
    );
    hm.loadFromFile(tmp);
    const d = hm.describeHooks().find(x => x.event === 'preToolUse');
    assert.equal(d!.statusMessage, 'Running security scan…');
  });

  it('getHookAuditView surfaces label + sourceLabel + displayText + plugin end-to-end', () => {
    writePluginBundle('acme', [
      { event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'bash scan.sh', statusMessage: 'Scanning…' } },
      { event: 'stop', hook: { type: 'http', url: 'https://example.test/hook' } },
    ]);
    hm.loadFromFile(tmp);
    const rows = hm.getHookAuditView();
    const cmd = rows.find(r => r.event === 'preToolUse');
    assert.ok(cmd, 'command audit row present');
    assert.equal(cmd!.contentLabel, 'Command');
    assert.equal(cmd!.content, 'bash scan.sh');
    assert.equal(cmd!.displayText, 'Scanning…', 'displayText uses statusMessage');
    assert.equal(cmd!.pluginName, 'acme');
    assert.equal(cmd!.sourceLabel, 'Project settings (acme)');
    assert.equal(cmd!.matcher, 'bash');

    const http = rows.find(r => r.event === 'stop');
    assert.ok(http, 'http audit row present');
    assert.equal(http!.contentLabel, 'URL');
    assert.equal(http!.content, 'https://example.test/hook');
    assert.equal(http!.displayText, 'https://example.test/hook', 'no statusMessage → displayText is the url');
  });

  it('reloading does not accumulate stale plugin descriptors', () => {
    writePluginBundle('acme', [
      { event: 'preToolUse', hook: { type: 'command', command: 'echo a' } },
    ]);
    hm.loadFromFile(tmp);
    hm.loadFromFile(tmp);
    const rows = hm.getHookAuditView().filter(r => r.event === 'preToolUse');
    assert.equal(rows.length, 1, 'descriptors are reset on reload, not doubled');
  });
});
