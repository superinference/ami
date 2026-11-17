import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveSandboxMethod,
  describeSandboxFallbackWarning,
} from '../src/tools/bash-sandbox';

// ---------------------------------------------------------------------------
// UNIT U045 (continuation) — explicit weaker-fallback warning for the host
// bash sandbox (bash-sandbox.ts). Parity with Claude Code's degraded-mode
// "Cannot block …" notices (SandboxSettings.tsx L40-41,L235-241): when the
// namespace sandbox silently degrades to resource-limits-only, surface which
// REQUESTED protections are no longer enforced instead of running silently.
// ---------------------------------------------------------------------------

describe('resolveSandboxMethod', () => {
  it('returns ulimits on a non-linux platform regardless of unshare', () => {
    assert.equal(resolveSandboxMethod('darwin', () => true), 'ulimits');
    assert.equal(resolveSandboxMethod('win32', () => false), 'ulimits');
  });

  it('returns unshare on linux when unshare is available', () => {
    assert.equal(resolveSandboxMethod('linux', () => true), 'unshare');
  });

  it('returns ulimits on linux when unshare is unavailable', () => {
    assert.equal(resolveSandboxMethod('linux', () => false), 'ulimits');
  });
});

describe('describeSandboxFallbackWarning', () => {
  it('returns null when the full unshare backend is in use (nothing degraded)', () => {
    assert.equal(
      describeSandboxFallbackWarning({ allowedDomains: ['example.com'] }, 'unshare'),
      null,
    );
  });

  it('returns null on the ulimits fallback when no carve-out was requested', () => {
    // The bare auto-sandbox has nothing beyond the ulimits it still applies, so
    // it must stay silent (no noise for the common case).
    assert.equal(describeSandboxFallbackWarning({}, 'ulimits'), null);
    assert.equal(describeSandboxFallbackWarning(undefined, 'ulimits'), null);
  });

  it('names the lost outbound network allow/deny controls on the fallback', () => {
    const w = describeSandboxFallbackWarning(
      { allowedDomains: ['a.com'], deniedDomains: ['b.com'] },
      'ulimits',
    );
    assert.ok(w);
    assert.match(w, /resource-limits-only/);
    assert.match(w, /allowedDomains/);
    assert.match(w, /deniedDomains/);
    assert.match(w, /NOT enforced/);
  });

  it('names the lost filesystem + socket carve-outs on the fallback', () => {
    const w = describeSandboxFallbackWarning(
      {
        denyReadPaths: ['/secret'],
        denyWritePaths: ['/ro'],
        denyConfigWrites: true,
        denyBareGitPoisoning: true,
        allowedUnixSockets: ['/tmp/x.sock'],
      },
      'ulimits',
    );
    assert.ok(w);
    assert.match(w, /denyRead/);
    assert.match(w, /denyWrite/);
    assert.match(w, /denyConfigWrites/);
    assert.match(w, /denyBareGitPoisoning/);
    assert.match(w, /Unix-socket/);
  });

  it('does not warn about carve-outs that were not requested', () => {
    const w = describeSandboxFallbackWarning({ allowedDomains: ['a.com'] }, 'ulimits');
    assert.ok(w);
    assert.doesNotMatch(w, /denyRead/);
    assert.doesNotMatch(w, /Unix-socket/);
  });

  it('treats empty carve-out arrays as not-requested', () => {
    assert.equal(
      describeSandboxFallbackWarning(
        { allowedDomains: [], deniedDomains: [], denyReadPaths: [] },
        'ulimits',
      ),
      null,
    );
  });
});
