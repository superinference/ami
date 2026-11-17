/**
 * Tests for the persisted one-time preference / count-limited notice helpers and the
 * global-scoped transactional config updater (parity: Claude Code's `saveGlobalConfig`
 * updater + the `hasIdeAutoConnectDialogBeenShown` / `hasUsedBackgroundTask` one-time
 * flags and the count-limited notice seen-count knobs). All state lives in the GLOBAL
 * config (`~/.ami/config.json`) and is read/written via the lock-safe
 * {@link updateConfigFile}, so these are concurrency-safe and survive across sessions.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  saveGlobalConfig,
  wasPreferenceShown,
  markPreferenceShown,
  loadPreferenceFlags,
  getNoticeSeenCount,
  shouldShowNotice,
  recordNoticeSeen,
  loadNoticeSeenCounts,
  loadGlobalConfig,
  validateProjectConfigValues,
  KNOWN_CONFIG_KEYS,
  getConfigJSONSchema,
  type ProjectConfig,
} from '../src/config';

function readGlobal(home: string): Record<string, unknown> {
  const p = path.join(home, '.ami', 'config.json');
  return JSON.parse(fs.readFileSync(p, 'utf-8')) as Record<string, unknown>;
}

describe('persisted preference flags + notice counts (global config)', () => {
  let home: string;
  let prevHome: string | undefined;
  let prevUserProfile: string | undefined;

  beforeEach(() => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'si-pref-flags-'));
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

  // --- saveGlobalConfig ---------------------------------------------------
  describe('saveGlobalConfig', () => {
    it('writes a new key to the global config and returns the written result', () => {
      const res = saveGlobalConfig(cur => ({ ...cur, provider: 'anthropic' }));
      assert.ok(res);
      assert.equal(res!.written, true);
      assert.equal(res!.reason, 'written');
      assert.equal(readGlobal(home).provider, 'anthropic');
    });

    it('treats a same-reference return as a no-op (no write)', () => {
      const res = saveGlobalConfig(cur => cur);
      assert.ok(res);
      assert.equal(res!.written, false);
      assert.equal(res!.reason, 'no-change');
    });

    it('returns null when there is no resolvable global config path', () => {
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      assert.equal(saveGlobalConfig(cur => ({ ...cur, provider: 'x' })), null);
    });

    it('re-reads fresh state so a concurrent key is preserved (lost-update guard)', () => {
      // Pre-seed a key written "concurrently" (directly on disk) before our updater runs.
      fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify({ model: 'opus' }));
      saveGlobalConfig(cur => ({ ...cur, provider: 'anthropic' }));
      const disk = readGlobal(home);
      assert.equal(disk.model, 'opus');
      assert.equal(disk.provider, 'anthropic');
    });
  });

  // --- one-time preference flags -----------------------------------------
  describe('markPreferenceShown / wasPreferenceShown', () => {
    it('is false before being marked, true after, and survives a reload', () => {
      assert.equal(wasPreferenceShown('ideAutoConnectDialog'), false);
      assert.equal(markPreferenceShown('ideAutoConnectDialog'), true);
      assert.equal(wasPreferenceShown('ideAutoConnectDialog'), true);
      // persisted to disk, not just memory
      assert.deepEqual((loadGlobalConfig() as ProjectConfig).preferencesShown, { ideAutoConnectDialog: true });
    });

    it('is idempotent — a second mark writes nothing', () => {
      assert.equal(markPreferenceShown('bg'), true);
      assert.equal(markPreferenceShown('bg'), false);
      assert.deepEqual(loadPreferenceFlags(), { bg: true });
    });

    it('keeps independent flags separate', () => {
      markPreferenceShown('a');
      markPreferenceShown('b');
      assert.equal(wasPreferenceShown('a'), true);
      assert.equal(wasPreferenceShown('b'), true);
      assert.equal(wasPreferenceShown('c'), false);
    });

    it('rejects an empty name and never persists it', () => {
      assert.equal(markPreferenceShown(''), false);
      assert.equal(wasPreferenceShown(''), false);
      assert.equal(loadGlobalConfig(), null);
    });

    it('returns false (best-effort) with no global path', () => {
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      assert.equal(markPreferenceShown('x'), false);
      assert.equal(wasPreferenceShown('x'), false);
      assert.deepEqual(loadPreferenceFlags(), {});
    });
  });

  // --- count-limited notices ---------------------------------------------
  describe('shouldShowNotice / recordNoticeSeen / getNoticeSeenCount', () => {
    it('counts up to the cap then stops showing', () => {
      assert.equal(getNoticeSeenCount('welcome'), 0);
      assert.equal(shouldShowNotice('welcome', 3), true);
      assert.equal(recordNoticeSeen('welcome', 3), true); // 1
      assert.equal(recordNoticeSeen('welcome', 3), true); // 2
      assert.equal(getNoticeSeenCount('welcome'), 2);
      assert.equal(shouldShowNotice('welcome', 3), true);
      assert.equal(recordNoticeSeen('welcome', 3), true); // 3 -> cap
      assert.equal(getNoticeSeenCount('welcome'), 3);
      assert.equal(shouldShowNotice('welcome', 3), false);
    });

    it('does not increment past the cap', () => {
      recordNoticeSeen('n', 1);
      assert.equal(getNoticeSeenCount('n'), 1);
      assert.equal(recordNoticeSeen('n', 1), false); // already at cap -> no write
      assert.equal(getNoticeSeenCount('n'), 1);
    });

    it('persists the count across reloads', () => {
      recordNoticeSeen('n', 5);
      recordNoticeSeen('n', 5);
      assert.deepEqual(loadNoticeSeenCounts(), { n: 2 });
    });

    it('guards empty name / non-positive maxCount', () => {
      assert.equal(shouldShowNotice('', 3), false);
      assert.equal(shouldShowNotice('n', 0), false);
      assert.equal(recordNoticeSeen('', 3), false);
      assert.equal(recordNoticeSeen('n', 0), false);
      assert.equal(getNoticeSeenCount(''), 0);
    });

    it('treats a corrupt stored count as zero', () => {
      fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(home, '.ami', 'config.json'), JSON.stringify({ noticeSeenCounts: { n: -4 } }));
      assert.equal(getNoticeSeenCount('n'), 0);
      assert.equal(shouldShowNotice('n', 2), true);
    });
  });

  // --- validation + schema recognition -----------------------------------
  describe('validation + schema', () => {
    it('recognizes the new keys (no spurious unknown-key warning)', () => {
      assert.ok(KNOWN_CONFIG_KEYS.has('preferencesShown'));
      assert.ok(KNOWN_CONFIG_KEYS.has('noticeSeenCounts'));
      const schema = getConfigJSONSchema();
      assert.ok(schema.properties.preferencesShown);
      assert.ok(schema.properties.noticeSeenCounts);
    });

    it('accepts valid preferencesShown / noticeSeenCounts', () => {
      const issues = validateProjectConfigValues({
        preferencesShown: { a: true, b: false },
        noticeSeenCounts: { n: 3 },
      });
      assert.deepEqual(issues, []);
    });

    it('flags a non-object preferencesShown', () => {
      const issues = validateProjectConfigValues({ preferencesShown: 'nope' as unknown as Record<string, boolean> });
      assert.ok(issues.some(i => i.field === 'preferencesShown'));
    });

    it('flags a non-boolean preferencesShown entry', () => {
      const issues = validateProjectConfigValues({ preferencesShown: { a: 1 as unknown as boolean } });
      assert.ok(issues.some(i => i.field === 'preferencesShown.a'));
    });

    it('flags a non-number / negative noticeSeenCounts entry', () => {
      const bad = validateProjectConfigValues({ noticeSeenCounts: { n: 'x' as unknown as number } });
      assert.ok(bad.some(i => i.field === 'noticeSeenCounts.n'));
      const neg = validateProjectConfigValues({ noticeSeenCounts: { n: -2 } });
      assert.ok(neg.some(i => i.field === 'noticeSeenCounts.n'));
    });
  });
});
