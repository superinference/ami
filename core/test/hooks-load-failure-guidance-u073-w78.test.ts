/**
 * U073 / w78 — Categorized, actionable remediation guidance on a hooks/config
 * load failure (parity with the reference's sessionStart plugin-hook-load triage).
 *
 * A malformed .ami/hooks.json / .ami/config.json silently disables every hook it
 * would contribute. recordConfigError now routes the failure through
 * categorizeLoadFailure() and appends a network/permissions/config remediation
 * hint to the retained error (surfaced via getConfigErrors() + the `configError`
 * event), so the operator learns HOW to fix it, not just that it failed.
 *
 * RED without the change: the retained error is the bare parser message with no
 * actionable guidance tail.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type HookConfigError } from '../src/hooks';

function tmpDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u073-w78-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  return dir;
}

describe('U073/w78 — categorized load-failure guidance', () => {
  it('appends actionable config guidance to a malformed hooks.json error', () => {
    const dir = tmpDir();
    fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), '{ this is not json ]');

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    const events: HookConfigError[] = [];
    hm.on('configError', (e: HookConfigError) => { events.push(e); });

    hm.loadFromFile(dir);

    const errs = hm.getConfigErrors();
    assert.equal(errs.length, 1);
    // A JSON parse failure categorizes as config → actionable config guidance tail.
    assert.match(errs[0].error, /config/i);
    assert.match(errs[0].error, /JSON|config file/i);
    // The event carries the same enriched, actionable message.
    assert.equal(events.length, 1);
    assert.equal(events[0].error, errs[0].error);

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('appends config guidance to a malformed .ami/config.json error', () => {
    const dir = tmpDir();
    fs.writeFileSync(path.join(dir, '.ami', 'config.json'), '{ broken ');

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const err = hm.getConfigErrors().find(e => e.path.endsWith(path.join('.ami', 'config.json')));
    assert.ok(err, 'expected a config.json error');
    assert.match(err!.error, /config/i);

    fs.rmSync(dir, { recursive: true, force: true });
  });
});
