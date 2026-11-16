/**
 * Unit tests for migrateDeprecatedModelAlias — the idempotent startup migration that
 * rewrites a persisted deprecated/renamed pinned `model` id in the GLOBAL settings to
 * its current successor (parity: Claude's migrateLegacyOpusToCurrent / migrateFennecToOpus).
 * Each test discriminates the fix: a bare no-op implementation (or a non-idempotent /
 * wrong-source one) would fail.
 * Run: cd core/core && node --import tsx --test test/reference-journal-model-migration.test.ts
 */
import { describe, it, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { migrateDeprecatedModelAlias } from '../src/reference-journal';
import { fsHooks as loggerFsHooks, __resetLoggerForTest } from '../src/logger';

const tmpdirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'refjrnl-mm-'));
  tmpdirs.push(d);
  return d;
}
function cfgPath(obj: Record<string, unknown>): string {
  const dir = mkTmp();
  const p = path.join(dir, 'config.json');
  fs.writeFileSync(p, JSON.stringify(obj));
  return p;
}
afterEach(() => {
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

describe('migrateDeprecatedModelAlias', () => {
  it('rewrites a persisted deprecated pinned model to its current successor and reports the outcome', async () => {
    const p = cfgPath({ model: 'claude-3-5-sonnet', permissionMode: 'ask' });
    const out = await migrateDeprecatedModelAlias(p);
    assert.ok(out, 'a deprecated model should produce a migration outcome');
    assert.equal(out!.from, 'claude-3-5-sonnet');
    assert.equal(out!.to, 'claude-sonnet-4-5');
    assert.equal(out!.path, p);
    const after = JSON.parse(fs.readFileSync(p, 'utf8'));
    assert.equal(after.model, 'claude-sonnet-4-5', 'the settings file is rewritten');
    assert.equal(after.permissionMode, 'ask', 'other keys are preserved');
  });

  it('is idempotent — a second run is a no-op (the rewritten id is not deprecated)', async () => {
    const p = cfgPath({ model: 'claude-3-opus' });
    const first = await migrateDeprecatedModelAlias(p);
    assert.equal(first!.to, 'claude-opus-4-1');
    const before = fs.readFileSync(p, 'utf8');
    const second = await migrateDeprecatedModelAlias(p);
    assert.equal(second, null, 'nothing left to migrate on the second run');
    assert.equal(fs.readFileSync(p, 'utf8'), before, 'file untouched by the idempotent second run');
  });

  it('is a no-op when the pinned model is already current (not deprecated)', async () => {
    const p = cfgPath({ model: 'claude-sonnet-4-5' });
    assert.equal(await migrateDeprecatedModelAlias(p), null);
    assert.equal(JSON.parse(fs.readFileSync(p, 'utf8')).model, 'claude-sonnet-4-5');
  });

  it('is a no-op when no model is pinned, or the value is not a string', async () => {
    assert.equal(await migrateDeprecatedModelAlias(cfgPath({ permissionMode: 'ask' })), null);
    assert.equal(await migrateDeprecatedModelAlias(cfgPath({ model: 123 })), null);
    assert.equal(await migrateDeprecatedModelAlias(cfgPath({ model: '   ' })), null);
  });

  it('returns null when there is no resolvable global config path', async () => {
    assert.equal(await migrateDeprecatedModelAlias(null), null);
  });

  it('is a no-op when the config file does not exist yet', async () => {
    const p = path.join(mkTmp(), 'absent.json');
    assert.equal(await migrateDeprecatedModelAlias(p), null);
    assert.ok(!fs.existsSync(p), 'migration must not create a settings file');
  });

  it('refuses to rewrite a corrupt settings file and never throws', async () => {
    const dir = mkTmp();
    const p = path.join(dir, 'config.json');
    fs.writeFileSync(p, '{ this is not json');
    let out: unknown;
    // A corrupt file blocks the rewrite (updateConfigFile reason 'corrupt'); since the
    // updater never runs, nothing matched → returns null, and the file is left intact.
    await assert.doesNotReject(async () => { out = await migrateDeprecatedModelAlias(p); });
    assert.equal(out, null);
    assert.equal(fs.readFileSync(p, 'utf8'), '{ this is not json', 'corrupt file is never clobbered');
  });

  it('surfaces a successful migration through the logger (observable outcome, not silent)', async () => {
    const p = cfgPath({ model: 'claude-3-5-haiku' });
    __resetLoggerForTest();
    const lines: string[] = [];
    const spy = mock.method(loggerFsHooks, 'appendFileSync', (_f: unknown, line: unknown) => {
      lines.push(String(line));
    });
    try {
      const out = await migrateDeprecatedModelAlias(p);
      assert.equal(out!.to, 'claude-haiku-4-5');
    } finally {
      spy.mock.restore();
    }
    assert.ok(
      lines.some(l => l.includes('migrate-model-alias') && l.includes('claude-3-5-haiku')),
      'the migration must be logged (observable), not silently applied',
    );
  });
});
