/**
 * session-mode-reconcile-u006.test.ts — session operating-mode reconciliation (U006, gap 14).
 *
 * Covers the pure {@link reconcileSessionMode} decision formula (parity: the reference's
 * matchSessionMode): an absent stored mode is a back-compat no-op, a matching mode is a
 * no-op, and each mismatch direction flips to the stored (resumed-session-wins) target
 * with the correct human-readable warning. Also pins the exported mode enum.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  reconcileSessionMode,
  SESSION_OPERATING_MODES,
  type SessionOperatingMode,
} from '../src/types';

describe('reconcileSessionMode (gap 14, parity: matchSessionMode)', () => {
  it('enumerates exactly the two operating modes', () => {
    assert.deepEqual([...SESSION_OPERATING_MODES], ['normal', 'coordinator']);
  });

  it('is a no-op when the stored mode is absent (pre-tracking session, back-compat)', () => {
    for (const current of SESSION_OPERATING_MODES) {
      const r = reconcileSessionMode(undefined, current);
      assert.equal(r.changed, false);
      assert.equal(r.target, current);
      assert.equal(r.warning, undefined);
    }
  });

  it('is a no-op when stored matches the live mode', () => {
    for (const mode of SESSION_OPERATING_MODES) {
      const r = reconcileSessionMode(mode, mode);
      assert.equal(r.changed, false);
      assert.equal(r.target, mode);
      assert.equal(r.warning, undefined);
    }
  });

  it('flips INTO coordinator when the resumed session was coordinator but live is normal', () => {
    const r = reconcileSessionMode('coordinator', 'normal');
    assert.equal(r.changed, true);
    assert.equal(r.target, 'coordinator');
    assert.equal(r.warning, 'Entered coordinator mode to match resumed session.');
  });

  it('flips OUT of coordinator when the resumed session was normal but live is coordinator', () => {
    const r = reconcileSessionMode('normal', 'coordinator');
    assert.equal(r.changed, true);
    assert.equal(r.target, 'normal');
    assert.equal(r.warning, 'Exited coordinator mode to match resumed session.');
  });

  it('the resumed session always wins the target on a mismatch', () => {
    const modes: SessionOperatingMode[] = ['normal', 'coordinator'];
    for (const stored of modes) {
      for (const live of modes) {
        const r = reconcileSessionMode(stored, live);
        assert.equal(r.target, stored === live ? live : stored);
        assert.equal(r.changed, stored !== live);
      }
    }
  });
});
