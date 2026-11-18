/**
 * U007 / w99 — fresh mid-session re-read of the hooks config (Claude Code's
 * updateHooksConfigSnapshot / hooksConfigSnapshot.ts:L99-124).
 *
 * AMI loaded hooks exactly once at startup; an external edit to .ami/hooks.json /
 * settings.json made after startup was invisible until a process restart, and
 * calling loadFromFile() twice double-registered (it appends without clearing the
 * registered hook fns). reloadFromDisk() clears every file/config/plugin-sourced
 * hook (identified by recorded provenance) and re-reads from disk, while PRESERVING
 * runtime/engine-registered hooks (which carry no provenance). Because AMI reads
 * each source straight from disk with no settings cache, the re-read inherently
 * beats a file-watcher debounce.
 *
 * These tests cover:
 *   - reloadFromDisk picks up a hook ADDED to the file after startup
 *   - reloadFromDisk drops a hook REMOVED from the file after startup
 *   - reloadFromDisk is NOT a double-register (count is stable, not additive)
 *   - a runtime-registered hook (onPreToolUse) survives a reload
 *   - the {cleared, registered} summary is accurate
 *   - no-arg reloadFromDisk() re-reads the directory of the last load
 *   - clearConfigHooks removes file hooks but leaves runtime hooks
 *
 * HOME is isolated so real ~/.ami / ~/.claude hooks never leak into the counts.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type PreToolUseContext } from '../src/hooks';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

describe('HookManager — reloadFromDisk (fresh mid-session re-read)', () => {
  let tmp: string;
  let home: string;
  let origHome: string | undefined;
  let origManaged: string | undefined;
  let hm: HookManager;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-reload-'));
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
    if (origManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH;
    else process.env.SUPERINFERENCE_MANAGED_SETTINGS_PATH = origManaged;
    for (const d of [tmp, home]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
  });

  /** Write the project `.ami/hooks.json` with N preToolUse command hooks. */
  function writeHooks(...commands: string[]): void {
    fs.mkdirSync(path.join(tmp, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(tmp, '.ami', 'hooks.json'),
      JSON.stringify({ hooks: commands.map(command => ({ event: 'preToolUse', hook: { type: 'command', command } })) }),
    );
  }

  it('picks up a hook ADDED to the file after startup (no restart)', () => {
    writeHooks('echo one');
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 1);

    // External edit mid-session: a second hook appears.
    writeHooks('echo one', 'echo two');
    const r = hm.reloadFromDisk(tmp);
    assert.equal(hm.getSummary().preToolUse, 2, 'the newly-added hook is now live');
    assert.deepEqual(r, { cleared: 1, registered: 2 });
  });

  it('drops a hook REMOVED from the file after startup', () => {
    writeHooks('echo one', 'echo two');
    hm.loadFromFile(tmp);
    assert.equal(hm.getSummary().preToolUse, 2);

    // External edit mid-session: the file is deleted entirely.
    fs.rmSync(path.join(tmp, '.ami', 'hooks.json'));
    const r = hm.reloadFromDisk(tmp);
    assert.equal(hm.getSummary().preToolUse ?? 0, 0, 'removed hooks no longer fire');
    assert.equal(r.cleared, 2);
    assert.equal(r.registered, 0);
  });

  it('is idempotent — reloading an unchanged file does NOT double-register', () => {
    writeHooks('echo one', 'echo two');
    hm.loadFromFile(tmp);
    hm.reloadFromDisk(tmp);
    hm.reloadFromDisk(tmp);
    assert.equal(hm.getSummary().preToolUse, 2, 'count stays stable across repeated reloads');
  });

  it('preserves a runtime-registered hook across a reload', async () => {
    writeHooks('echo file-hook');
    hm.loadFromFile(tmp);
    let runtimeFired = false;
    hm.onPreToolUse(async () => { runtimeFired = true; return { action: 'allow' }; });
    assert.equal(hm.getSummary().preToolUse, 2, 'file hook + runtime hook');

    // Change the file; reload must keep the runtime hook and refresh the file hook.
    writeHooks('echo file-hook', 'echo added');
    const r = hm.reloadFromDisk(tmp);
    // cleared counts only the provenance-carrying FILE hook (1), not the runtime one.
    assert.equal(r.cleared, 1, 'runtime hook is not cleared');
    assert.equal(r.registered, 2, 'two file hooks re-registered');
    assert.equal(hm.getSummary().preToolUse, 3, '2 file hooks + 1 preserved runtime hook');

    await hm.executePreToolUse(preCtx);
    assert.equal(runtimeFired, true, 'the preserved runtime hook still fires after reload');
  });

  it('no-arg reloadFromDisk() re-reads the directory of the last load', () => {
    writeHooks('echo one');
    hm.loadFromFile(tmp); // records tmp as the load cwd
    writeHooks('echo one', 'echo two');
    const r = hm.reloadFromDisk(); // no explicit cwd
    assert.equal(hm.getSummary().preToolUse, 2);
    assert.equal(r.registered, 2);
  });

  it('clearConfigHooks removes file hooks but leaves runtime hooks', () => {
    writeHooks('echo file');
    hm.loadFromFile(tmp);
    hm.onPreToolUse(async () => ({ action: 'allow' }));
    assert.equal(hm.getSummary().preToolUse, 2);

    const cleared = hm.clearConfigHooks();
    assert.equal(cleared, 1, 'only the file hook is cleared');
    assert.equal(hm.getSummary().preToolUse, 1, 'the runtime hook remains');
  });
});
