import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectBlockedSleep, interpretExitCode } from '../src/tools/bash';

// ---------------------------------------------------------------------------
// detectBlockedSleep — GNU `sleep` duration grammar with unit suffixes.
// GNU sleep accepts NUMBER[SUFFIX] where SUFFIX ∈ {s,m,h,d} and decimals,
// and sums multiple arguments. The blocking guard must understand all of them.
// ---------------------------------------------------------------------------

describe('detectBlockedSleep – unit suffixes', () => {
  it('blocks `sleep 2m` (120s > 10s threshold)', () => {
    assert.ok(detectBlockedSleep('sleep 2m', false, false));
  });

  it('blocks `sleep 1h`', () => {
    assert.ok(detectBlockedSleep('sleep 1h', false, false));
  });

  it('blocks `sleep 1d`', () => {
    assert.ok(detectBlockedSleep('sleep 1d', false, false));
  });

  it('blocks `sleep 15s` (explicit seconds suffix)', () => {
    assert.ok(detectBlockedSleep('sleep 15s', false, false));
  });

  it('blocks a decimal minute `sleep 0.5m` (30s)', () => {
    assert.ok(detectBlockedSleep('sleep 0.5m', false, false));
  });

  it('blocks summed args `sleep 5 8` (13s)', () => {
    assert.ok(detectBlockedSleep('sleep 5 8', false, false));
  });

  it('does NOT block a short `sleep 2s`', () => {
    assert.equal(detectBlockedSleep('sleep 2s', false, false), null);
  });

  it('does NOT block a short bare `sleep 3`', () => {
    assert.equal(detectBlockedSleep('sleep 3', false, false), null);
  });

  it('does NOT block a decimal below threshold `sleep 0.5`', () => {
    assert.equal(detectBlockedSleep('sleep 0.5', false, false), null);
  });

  it('still blocks the bare-integer legacy case `sleep 30`', () => {
    assert.ok(detectBlockedSleep('sleep 30', false, false));
  });

  it('is suppressed by runInBackground', () => {
    assert.equal(detectBlockedSleep('sleep 2m', true, false), null);
  });

  it('is suppressed by a custom timeout', () => {
    assert.equal(detectBlockedSleep('sleep 2m', false, true), null);
  });
});

// ---------------------------------------------------------------------------
// interpretExitCode — base-command normalization + robocopy bitfield codes.
// ---------------------------------------------------------------------------

describe('interpretExitCode – base-command normalization', () => {
  it('recognizes grep via an absolute path', () => {
    assert.deepEqual(interpretExitCode('/usr/bin/grep foo file', 1), { message: '(no matches found — not an error)', isNonError: true });
  });

  it('recognizes grep with a trailing .exe (Windows)', () => {
    assert.deepEqual(interpretExitCode('grep.exe foo file', 1), { message: '(no matches found — not an error)', isNonError: true });
  });

  it('recognizes a quoted command name', () => {
    assert.deepEqual(interpretExitCode('"grep" foo file', 1), { message: '(no matches found — not an error)', isNonError: true });
  });

  it('recognizes a PowerShell call-operator invocation (& grep)', () => {
    assert.deepEqual(interpretExitCode('& grep foo', 1), { message: '(no matches found — not an error)', isNonError: true });
  });
});

describe('interpretExitCode – robocopy bitfield', () => {
  it('treats robocopy exit 1 (files copied) as non-error', () => {
    const r = interpretExitCode('robocopy src dst', 1);
    assert.equal(r?.isNonError, true);
  });

  it('treats robocopy exit 3 as non-error', () => {
    assert.equal(interpretExitCode('robocopy src dst', 3)?.isNonError, true);
  });

  it('treats robocopy exit 7 as non-error (highest success bitfield)', () => {
    assert.equal(interpretExitCode('robocopy src dst', 7)?.isNonError, true);
  });

  it('treats robocopy exit 8 as a real error (not non-error)', () => {
    const r = interpretExitCode('robocopy src dst', 8);
    assert.notEqual(r?.isNonError, true);
  });

  it('treats robocopy exit 16 (fatal) as a real error', () => {
    assert.notEqual(interpretExitCode('robocopy src dst', 16)?.isNonError, true);
  });

  it('recognizes robocopy.exe success too', () => {
    assert.equal(interpretExitCode('ROBOCOPY.exe src dst', 1)?.isNonError, true);
  });
});
