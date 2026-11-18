/**
 * U010 / w102 — wiring of the config-KEY rename migration runner into the versioned
 * startup migration set (runStartupMigrations → runConfigKeyMigrations), plus the
 * one-time notice for migrated keys.
 *
 * RED-GREEN discriminators:
 *  - "exposes renamedConfigKeys on the result" fails if runStartupMigrations stops
 *    returning the field (the wiring step is removed).
 *  - "emits a one-time notice for migrated keys" fails if formatStartupMigrationNotices
 *    drops the renamedConfigKeys branch.
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-config-key-migrations-w102.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  runStartupMigrations,
  formatStartupMigrationNotices,
  CURRENT_MIGRATION_VERSION,
} from '../src/reference-journal';

const tmpdirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'refjrnl-ck-'));
  tmpdirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('config-key migration wiring', () => {
  it('runStartupMigrations exposes renamedConfigKeys (empty with no deprecated keys registered yet)', async () => {
    const home = mkTmp();
    const cfgPath = path.join(home, 'config.json');
    fs.writeFileSync(cfgPath, JSON.stringify({ model: 'claude-sonnet-4-5' }));
    const env = { HOME: home } as NodeJS.ProcessEnv;

    const res = await runStartupMigrations(env, cfgPath);

    assert.equal(res.ranVersion, CURRENT_MIGRATION_VERSION, 'the set ran');
    assert.deepEqual(res.renamedConfigKeys, [], 'no key renames registered → nothing migrated');
  });

  it('formatStartupMigrationNotices emits a one-time notice naming the migrated keys', () => {
    const notices = formatStartupMigrationNotices({
      ranVersion: CURRENT_MIGRATION_VERSION,
      relocatedDirs: [],
      modelAlias: null,
      renamedConfigKeys: ['replBridgeEnabled'],
    });
    assert.equal(notices.length, 1);
    assert.match(notices[0]!, /replBridgeEnabled/);
    assert.match(notices[0]!, /deprecated config key/i);
  });

  it('emits no key-rename notice when nothing was migrated', () => {
    const notices = formatStartupMigrationNotices({
      ranVersion: CURRENT_MIGRATION_VERSION,
      relocatedDirs: [],
      modelAlias: null,
      renamedConfigKeys: [],
    });
    assert.deepEqual(notices, []);
  });
});
