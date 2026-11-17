/**
 * U122 — persisted folder trust + parent-directory ancestor walk (safe-mode).
 *
 * Covers two parity gaps against Claude Code's TrustDialog:
 *   1. `isTrustedByAncestor` performs a TRUE parent-directory walk with a
 *      filesystem-root boundary (parity: `computeTrustDialogAccepted` /
 *      `isPathTrusted`), not a string-prefix test.
 *   2. `TrustIndicators.persistentlyTrusted` lets a durable, cross-session
 *      folder-trust grant suppress the SOFT untrusted/cloned heuristics, while the
 *      HARD escalation signals still engage safe mode (AMI superset).
 *
 * RED before the fix: `persistentlyTrusted` is not a field on TrustIndicators and
 * is not consumed by evaluateSafeMode, so the suppression assertions fail (safe
 * mode stays enabled); the deep-nesting / root-boundary walk assertions also drive
 * the explicit parent traversal.
 *
 * Run: cd core/core && node --import tsx --test test/safe-mode-u122-persist-trust.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as path from 'path';
import {
  isTrustedByAncestor,
  evaluateSafeMode,
  shouldEnableSafeMode,
} from '../src/safe-mode';

describe('isTrustedByAncestor :: parent-directory walk with root boundary', () => {
  it('trusts a deeply-nested descendant of a trusted root (walks every parent)', () => {
    assert.equal(isTrustedByAncestor('/home/u/proj/a/b/c/d', ['/home/u/proj']), true);
  });

  it('trusts the root itself', () => {
    assert.equal(isTrustedByAncestor('/home/u/proj', ['/home/u/proj']), true);
  });

  it('does NOT trust a sibling sharing a name prefix (walk, not startsWith)', () => {
    assert.equal(isTrustedByAncestor('/home/u/project-evil/src', ['/home/u/project']), false);
  });

  it('stops at the filesystem-root boundary without matching when no ancestor is trusted', () => {
    assert.equal(isTrustedByAncestor('/a/b/c', ['/x/y']), false);
  });

  it('a trusted filesystem root trusts every path beneath it', () => {
    assert.equal(isTrustedByAncestor('/anything/at/all', [path.sep]), true);
  });

  it('normalizes `.`/`..`/trailing-slash spelling on both sides', () => {
    assert.equal(isTrustedByAncestor('/home/u/proj/x/../y', ['/home/u/proj/']), true);
    assert.equal(isTrustedByAncestor('/home/u/proj', ['/home/u/other/../proj']), true);
  });

  it('is false with an empty cwd or empty/all-blank trusted roots', () => {
    assert.equal(isTrustedByAncestor('', ['/home/u/proj']), false);
    assert.equal(isTrustedByAncestor('/home/u/proj', []), false);
    assert.equal(isTrustedByAncestor('/home/u/proj', ['', '']), false);
  });
});

describe('evaluateSafeMode :: persisted folder-trust suppression', () => {
  it('persistentlyTrusted suppresses the untrusted-project soft signal', () => {
    assert.equal(evaluateSafeMode('/p', { untrustedProject: true }).enabled, true);
    assert.equal(evaluateSafeMode('/p', { untrustedProject: true, persistentlyTrusted: true }).enabled, false);
  });

  it('persistentlyTrusted suppresses the cloned-from-unknown soft signal', () => {
    assert.equal(evaluateSafeMode('/p', { clonedFromUnknown: true, persistentlyTrusted: true }).enabled, false);
  });

  it('persistentlyTrusted does NOT suppress the HARD escalation signals (AMI superset)', () => {
    assert.equal(evaluateSafeMode('/p', { hasHooks: true, persistentlyTrusted: true }).enabled, true);
    assert.equal(evaluateSafeMode('/p', { hasConfigEscalation: true, persistentlyTrusted: true }).enabled, true);
    assert.equal(evaluateSafeMode('/p', { hasBashExecution: true, persistentlyTrusted: true }).enabled, true);
    assert.equal(evaluateSafeMode('/p', { hasDangerousEnvVars: true, persistentlyTrusted: true }).enabled, true);
  });

  it('shouldEnableSafeMode honors persistentlyTrusted (boolean wrapper)', () => {
    assert.equal(shouldEnableSafeMode('/p', { untrustedProject: true, persistentlyTrusted: true }), false);
    assert.equal(shouldEnableSafeMode('/p', { untrustedProject: true }), true);
  });
});
