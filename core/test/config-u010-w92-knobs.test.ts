/**
 * U010 / w92 — three additive UI/UX config knobs on ProjectConfig and their resolvers
 * (parity: Claude Code's `showClearContextOnPlanAccept`, `prStatusFooterEnabled`, and
 * `lastReleaseNotesSeen`). Covers: merge propagation (last-layer-wins), the default-aware
 * resolvers, the persisted release-notes-seen global-state helpers, and schema / known-key
 * / base-type-validation recognition so each knob is a first-class recognized key.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  mergeConfigs,
  shouldShowClearContextOnPlanAccept,
  isPrStatusFooterEnabled,
  getLastReleaseNotesSeen,
  hasUnseenReleaseNotes,
  markReleaseNotesSeen,
  loadGlobalConfig,
  validateProjectConfigValues,
  KNOWN_CONFIG_KEYS,
  getConfigJSONSchema,
  type ProjectConfig,
} from '../src/config';

describe('w92 UI/UX config knobs', () => {
  // --- showClearContextOnPlanAccept --------------------------------------
  describe('shouldShowClearContextOnPlanAccept (default OFF)', () => {
    it('is false when unset / null / undefined', () => {
      assert.equal(shouldShowClearContextOnPlanAccept(undefined), false);
      assert.equal(shouldShowClearContextOnPlanAccept(null), false);
      assert.equal(shouldShowClearContextOnPlanAccept({}), false);
    });
    it('is true only when explicitly true', () => {
      assert.equal(shouldShowClearContextOnPlanAccept({ showClearContextOnPlanAccept: true }), true);
      assert.equal(shouldShowClearContextOnPlanAccept({ showClearContextOnPlanAccept: false }), false);
    });
  });

  // --- prStatusFooterEnabled ---------------------------------------------
  describe('isPrStatusFooterEnabled (default ON)', () => {
    it('defaults to true when unset / null / undefined', () => {
      assert.equal(isPrStatusFooterEnabled(undefined), true);
      assert.equal(isPrStatusFooterEnabled(null), true);
      assert.equal(isPrStatusFooterEnabled({}), true);
    });
    it('is false only when explicitly false', () => {
      assert.equal(isPrStatusFooterEnabled({ prStatusFooterEnabled: false }), false);
      assert.equal(isPrStatusFooterEnabled({ prStatusFooterEnabled: true }), true);
    });
  });

  // --- merge propagation (last-layer-wins) -------------------------------
  describe('mergeConfigs propagation', () => {
    it('carries all three knobs through the merge and lets a higher layer override', () => {
      const merged = mergeConfigs(
        { prStatusFooterEnabled: false },            // cli (highest)
        {},                                           // env
        { showClearContextOnPlanAccept: true },       // project
        { prStatusFooterEnabled: true, lastReleaseNotesSeen: '1.0.0' }, // global
      );
      // cli override wins over the global value
      assert.equal(merged.prStatusFooterEnabled, false);
      // project-tier value propagates
      assert.equal(merged.showClearContextOnPlanAccept, true);
      // global-tier value propagates when no higher layer sets it
      assert.equal(merged.lastReleaseNotesSeen, '1.0.0');
    });

    it('leaves the knobs undefined when no layer sets them', () => {
      const merged = mergeConfigs({}, {}, null, null);
      assert.equal(merged.showClearContextOnPlanAccept, undefined);
      assert.equal(merged.prStatusFooterEnabled, undefined);
      assert.equal(merged.lastReleaseNotesSeen, undefined);
    });
  });

  // --- validation + schema recognition -----------------------------------
  describe('recognition + base-type validation', () => {
    it('recognizes the three keys (no spurious unknown-key warning) + emits schema nodes', () => {
      assert.ok(KNOWN_CONFIG_KEYS.has('showClearContextOnPlanAccept'));
      assert.ok(KNOWN_CONFIG_KEYS.has('prStatusFooterEnabled'));
      assert.ok(KNOWN_CONFIG_KEYS.has('lastReleaseNotesSeen'));
      const schema = getConfigJSONSchema();
      assert.equal(schema.properties.showClearContextOnPlanAccept.type, 'boolean');
      assert.equal(schema.properties.prStatusFooterEnabled.type, 'boolean');
      assert.equal(schema.properties.lastReleaseNotesSeen.type, 'string');
    });
    it('accepts valid values', () => {
      assert.deepEqual(
        validateProjectConfigValues({
          showClearContextOnPlanAccept: true,
          prStatusFooterEnabled: false,
          lastReleaseNotesSeen: '2.3.4',
        }),
        [],
      );
    });
    it('flags wrong base types', () => {
      const issues = validateProjectConfigValues({
        showClearContextOnPlanAccept: 'yes' as unknown as boolean,
        prStatusFooterEnabled: 1 as unknown as boolean,
        lastReleaseNotesSeen: 42 as unknown as string,
      });
      assert.ok(issues.some(i => i.field === 'showClearContextOnPlanAccept'));
      assert.ok(issues.some(i => i.field === 'prStatusFooterEnabled'));
      assert.ok(issues.some(i => i.field === 'lastReleaseNotesSeen'));
    });
  });

  // --- lastReleaseNotesSeen persisted global state -----------------------
  describe('release-notes seen helpers (global config)', () => {
    let home: string;
    let prevHome: string | undefined;
    let prevUserProfile: string | undefined;

    beforeEach(() => {
      home = fs.mkdtempSync(path.join(os.tmpdir(), 'si-relnotes-'));
      prevHome = process.env.HOME;
      prevUserProfile = process.env.USERPROFILE;
      process.env.HOME = home;
      delete process.env.USERPROFILE;
    });
    afterEach(() => {
      if (prevHome !== undefined) process.env.HOME = prevHome; else delete process.env.HOME;
      if (prevUserProfile !== undefined) process.env.USERPROFILE = prevUserProfile; else delete process.env.USERPROFILE;
      fs.rmSync(home, { recursive: true, force: true });
    });

    it('reads undefined before any version is seen; a current version is "unseen"', () => {
      assert.equal(getLastReleaseNotesSeen(), undefined);
      assert.equal(hasUnseenReleaseNotes('1.2.3'), true);
    });

    it('marks a version seen, persists it, and then reports it as seen (idempotent)', () => {
      assert.equal(markReleaseNotesSeen('1.2.3'), true);
      assert.equal(getLastReleaseNotesSeen(), '1.2.3');
      assert.equal((loadGlobalConfig() as ProjectConfig).lastReleaseNotesSeen, '1.2.3');
      assert.equal(hasUnseenReleaseNotes('1.2.3'), false);
      // a newer release is unseen again
      assert.equal(hasUnseenReleaseNotes('1.3.0'), true);
      // second mark of the same version is a no-op write
      assert.equal(markReleaseNotesSeen('1.2.3'), false);
    });

    it('prefers a passed-in config over the global read', () => {
      markReleaseNotesSeen('1.0.0');
      assert.equal(getLastReleaseNotesSeen({ lastReleaseNotesSeen: '9.9.9' }), '9.9.9');
      assert.equal(hasUnseenReleaseNotes('9.9.9', { lastReleaseNotesSeen: '9.9.9' }), false);
    });

    it('normalizes a blank / non-string stored value to undefined', () => {
      assert.equal(getLastReleaseNotesSeen({ lastReleaseNotesSeen: '   ' }), undefined);
      assert.equal(getLastReleaseNotesSeen({ lastReleaseNotesSeen: 5 as unknown as string }), undefined);
    });

    it('guards a blank version: never "unseen", never marked', () => {
      assert.equal(hasUnseenReleaseNotes(''), false);
      assert.equal(hasUnseenReleaseNotes('   '), false);
      assert.equal(markReleaseNotesSeen(''), false);
      assert.equal(markReleaseNotesSeen('  '), false);
    });

    it('returns false (best-effort) with no resolvable global path', () => {
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      assert.equal(markReleaseNotesSeen('1.0.0'), false);
      assert.equal(getLastReleaseNotesSeen(), undefined);
    });
  });
});
