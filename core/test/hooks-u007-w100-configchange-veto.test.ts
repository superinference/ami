/**
 * U007 / w100 — ConfigChange veto building block.
 *
 * Claude Code fires the ConfigChange hook BEFORE applying a detected settings
 * change and skips applying it entirely when `hasBlockingResult(results)` is true
 * (exit code 2 / decision 'block'). This is a security veto: a hook can block a
 * malicious or unexpected external edit of settings from being auto-loaded.
 *
 * AMI's `executeConfigChange` already aggregates `blockingErrors`, but a consumer
 * that applies the change to the live session needs a cheap, synchronous way to
 * tell whether running the (async) veto at all is necessary — otherwise every
 * config change would have to go async and lose the in-band re-enforcement
 * (killswitch / permission re-sync) existing callers depend on.
 *
 * `hasConfigChangeHooks()` is that gate: false when nothing can veto (direct-apply
 * path), true once a ConfigChange hook is registered (run the pre-apply veto path).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { HookManager } from '../src/hooks';

describe('HookManager — ConfigChange veto gate (U007/w100)', () => {
  it('hasConfigChangeHooks() is false with no registered ConfigChange hook', () => {
    const hm = new HookManager();
    assert.equal(hm.hasConfigChangeHooks(), false);
  });

  it('hasConfigChangeHooks() flips true once a ConfigChange hook is registered', () => {
    const hm = new HookManager();
    hm.onConfigChange(async () => {});
    assert.equal(hm.hasConfigChangeHooks(), true);
  });

  it('executeConfigChange aggregates a hook blockingError into blockingErrors', async () => {
    const hm = new HookManager();
    hm.onConfigChange(async () => ({ blockingError: 'policy: config change not permitted' }));
    const r = await hm.executeConfigChange({ source: 'user_settings', filePath: '/tmp/settings.json' });
    assert.ok(r && 'blockingErrors' in r, 'returns a blockingErrors object when a hook blocks');
    assert.deepEqual(r.blockingErrors, ['policy: config change not permitted']);
  });

  it('executeConfigChange returns void (no block) when the hook returns nothing', async () => {
    const hm = new HookManager();
    hm.onConfigChange(async () => {});
    const r = await hm.executeConfigChange({ source: 'user_settings', filePath: '/tmp/settings.json' });
    assert.equal(r, undefined, 'no blockingErrors → void result → change is applied');
  });

  it('a throwing ConfigChange hook does not block (change still applies)', async () => {
    const hm = new HookManager();
    hm.onConfigChange(async () => { throw new Error('boom'); });
    const r = await hm.executeConfigChange({ source: 'user_settings', filePath: '/tmp/settings.json' });
    assert.equal(r, undefined, 'a crashing veto hook must not silently block the change');
  });
});
