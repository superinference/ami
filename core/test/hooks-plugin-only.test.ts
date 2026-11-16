import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { HookManager } from '../src/hooks';

// strictPluginOnlyCustomization — per-surface locking of the `hooks` surface.
// HOME is isolated so real ~/.ami / ~/.claude hooks never leak into the counts.
describe('HookManager — strictPluginOnlyCustomization (hooks surface lock)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pluginonly-'));
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

  function writeProjectHook(command: string): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command } }] }),
    );
  }

  function writeManaged(obj: unknown): void {
    const file = path.join(home, 'managed.json');
    fs.writeFileSync(file, typeof obj === 'string' ? obj : JSON.stringify(obj));
    process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = file;
  }

  it('locking the hooks surface (array) drops project filesystem hooks', () => {
    writeProjectHook('echo project');
    writeManaged({ strictPluginOnlyCustomization: ['hooks'] });
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 0, 'project hook dropped when hooks surface is locked');
  });

  it('locking a different surface leaves hooks untouched', () => {
    writeProjectHook('echo project');
    writeManaged({ strictPluginOnlyCustomization: ['skills'] });
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'project hook loads when only skills is locked');
  });

  it('true locks all surfaces yet managed hooks still load', () => {
    writeProjectHook('echo project');
    writeManaged({
      strictPluginOnlyCustomization: true,
      hooks: [{ event: 'sessionStart', hook: { type: 'command', command: 'echo managed' } }],
    });
    hm.loadFromFile(tmp);
    const sum = hm.getSummary();
    assert.equal(sum.sessionStart, 1, 'managed (admin-trusted) hook survives the lock');
    assert.equal(sum.preToolUse, 0, 'project hook dropped under all-surfaces lock');
  });

  it('an unknown surface name in the array is dropped (hooks not locked by it)', () => {
    writeProjectHook('echo project');
    writeManaged({ strictPluginOnlyCustomization: ['commands'] });
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'unknown surface must not lock hooks');
  });

  it('malformed managed settings degrade to unlocked', () => {
    writeProjectHook('echo project');
    writeManaged('{ not valid json');
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 1, 'malformed policy locks nothing');
  });
});
