/**
 * Unit + integration tests for the versioned startup migration-set runner
 * (runStartupMigrations / CURRENT_MIGRATION_VERSION / formatStartupMigrationNotices) —
 * the ordered, run-once, idempotent framework that replaces the previous single ad-hoc
 * migrateLegacyDataDir() call (parity: the reference's runMigrations()/CURRENT_MIGRATION_VERSION).
 *
 * RED-GREEN discriminators:
 *  - "runs the ordered set …" fails if the runner stops invoking the deprecated-model
 *    migration (the pre-fix state, where migrateDeprecatedModelAlias existed but nothing
 *    called it) — the pinned model would stay un-rewritten.
 *  - "is a no-op once stamped" fails if the version gate is removed (the set would re-run
 *    every startup, so ranVersion would be the current version, not null).
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-startup-migrations.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  runStartupMigrations,
  formatStartupMigrationNotices,
  CURRENT_MIGRATION_VERSION,
  type StartupMigrationResult,
} from '../src/reference-journal';
import { LEGACY_MODEL_REMAP_OPT_OUT_ENV } from '../src/model-registry';

const tmpdirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'refjrnl-sm-'));
  tmpdirs.push(d);
  return d;
}
afterEach(() => {
  for (const d of tmpdirs.splice(0)) fs.rmSync(d, { recursive: true, force: true });
});

/** A fresh tmp HOME seeded with a pre-rename ~/.superinference/<sub> durable dir. */
function seedLegacyHome(sub: string): string {
  const home = mkTmp();
  fs.mkdirSync(path.join(home, '.superinference', sub), { recursive: true });
  return home;
}

function writeConfig(dir: string, obj: Record<string, unknown>): string {
  const p = path.join(dir, 'config.json');
  fs.writeFileSync(p, JSON.stringify(obj));
  return p;
}

describe('runStartupMigrations', () => {
  it('runs the ordered set once on a fresh config: relocates legacy dirs, rewrites a deprecated model, and stamps the version', async () => {
    const home = seedLegacyHome('plugins');
    const cfgPath = writeConfig(home, { model: 'claude-3-5-sonnet', permissionMode: 'ask' });
    const env = { HOME: home } as NodeJS.ProcessEnv;

    const res = await runStartupMigrations(env, cfgPath);

    assert.equal(res.ranVersion, CURRENT_MIGRATION_VERSION, 'the set ran and reports the stamped generation');
    assert.ok(res.relocatedDirs.includes('plugins'), 'the legacy ~/.superinference/plugins dir was relocated');
    // The deprecated-model migration is now actually INVOKED by the runner (the fix).
    assert.ok(res.modelAlias, 'a deprecated pinned model produces a rewrite outcome');
    assert.equal(res.modelAlias!.from, 'claude-3-5-sonnet');
    assert.equal(res.modelAlias!.to, 'claude-sonnet-4-5');
    // On disk: config rewritten + version stamped; legacy dir moved into ~/.ami.
    const after = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    assert.equal(after.model, 'claude-sonnet-4-5', 'model id rewritten in settings');
    assert.equal(after.migrationVersion, CURRENT_MIGRATION_VERSION, 'version stamped');
    assert.equal(after.permissionMode, 'ask', 'unrelated keys preserved');
    assert.ok(fs.existsSync(path.join(home, '.ami', 'plugins')), 'plugins relocated under ~/.ami');
    assert.ok(!fs.existsSync(path.join(home, '.superinference', 'plugins')), 'legacy copy removed');
  });

  it('is a no-op once stamped (idempotent version gate): a second run does not re-run the set', async () => {
    const home = seedLegacyHome('skills');
    const cfgPath = writeConfig(home, { model: 'claude-3-5-sonnet' });
    const env = { HOME: home } as NodeJS.ProcessEnv;

    const first = await runStartupMigrations(env, cfgPath);
    assert.equal(first.ranVersion, CURRENT_MIGRATION_VERSION);

    // Re-seed a legacy dir to prove the SECOND run is gated off (would relocate it if it ran).
    fs.mkdirSync(path.join(home, '.superinference', 'agents'), { recursive: true });
    const second = await runStartupMigrations(env, cfgPath);

    assert.equal(second.ranVersion, null, 'the set is skipped because the stored version already matches');
    assert.deepEqual(second.relocatedDirs, []);
    assert.equal(second.modelAlias, null);
    assert.ok(
      fs.existsSync(path.join(home, '.superinference', 'agents')),
      'the re-seeded legacy dir was NOT touched — the gate prevented a re-run',
    );
  });

  it('runs unconditionally when there is no resolvable config path (cannot read/stamp a version)', async () => {
    const home = seedLegacyHome('marketplaces');
    const env = { HOME: home } as NodeJS.ProcessEnv;

    const res = await runStartupMigrations(env, null);

    assert.equal(res.ranVersion, CURRENT_MIGRATION_VERSION, 'reports the generation even though nothing was stamped');
    assert.ok(res.relocatedDirs.includes('marketplaces'));
    assert.equal(res.modelAlias, null, 'no config path → no model rewrite');
  });

  it('skips the legacy-dir migration when a data-dir override is set but still runs the set', async () => {
    const home = seedLegacyHome('plugins');
    const cfgPath = writeConfig(home, {});
    // An explicit override opts migrateLegacyDataDir out (returns []), but the ordered
    // set still runs + stamps so future migrations fire.
    const env = { HOME: home, SUPERINFERENCE_DATA_DIR: path.join(home, 'custom') } as NodeJS.ProcessEnv;

    const res = await runStartupMigrations(env, cfgPath);

    assert.equal(res.ranVersion, CURRENT_MIGRATION_VERSION);
    assert.deepEqual(res.relocatedDirs, [], 'legacy relocation opted out by the override');
    const after = JSON.parse(fs.readFileSync(cfgPath, 'utf8'));
    assert.equal(after.migrationVersion, CURRENT_MIGRATION_VERSION, 'version still stamped');
  });
});

describe('formatStartupMigrationNotices', () => {
  it('produces one-time notices describing each thing that changed', () => {
    const res: StartupMigrationResult = {
      ranVersion: CURRENT_MIGRATION_VERSION,
      relocatedDirs: ['plugins', 'skills'],
      modelAlias: { from: 'old-model', to: 'new-model', path: '/tmp/config.json' },
      renamedConfigKeys: [],
    };
    const notices = formatStartupMigrationNotices(res);
    assert.equal(notices.length, 2);
    assert.match(notices[0]!, /Relocated durable config \(plugins, skills\)/);
    assert.match(notices[0]!, /~\/\.superinference to ~\/\.ami/);
    assert.match(notices[1]!, /old-model.*new-model/);
    assert.match(notices[1]!, /\/tmp\/config\.json/);
  });

  it('is empty on a skipped (no-op) run and on a run that changed nothing', () => {
    assert.deepEqual(
      formatStartupMigrationNotices({ ranVersion: null, relocatedDirs: [], modelAlias: null, renamedConfigKeys: [] }),
      [],
    );
    assert.deepEqual(
      formatStartupMigrationNotices({ ranVersion: CURRENT_MIGRATION_VERSION, relocatedDirs: [], modelAlias: null, renamedConfigKeys: [] }),
      [],
    );
  });

  // U035 (w88): the model-remap notice advertises its opt-out control inline (parity with
  // Claude Code's useModelMigrationNotifications "Model updated … · Set X=1 to opt out").
  // RED on revert: without the appendOptOut DI seam the model notice is emitted unchanged,
  // so the opt-out hint is absent and this assertion fails.
  it('applies the appendOptOut renderer ONLY to the model-remap notice', () => {
    const res: StartupMigrationResult = {
      ranVersion: CURRENT_MIGRATION_VERSION,
      relocatedDirs: ['plugins'],
      modelAlias: { from: 'old-model', to: 'new-model', path: '/tmp/config.json' },
      renamedConfigKeys: [],
    };
    const notices = formatStartupMigrationNotices(res, {
      appendOptOut: msg => `${msg} [opt-out:${LEGACY_MODEL_REMAP_OPT_OUT_ENV}]`,
    });
    assert.equal(notices.length, 2);
    // Relocation notice is untouched by the model-remap opt-out renderer.
    assert.doesNotMatch(notices[0]!, /opt-out:/);
    assert.match(notices[0]!, /Relocated durable config/);
    // The model-remap notice carries the opt-out control with the real env var name.
    assert.match(notices[1]!, /old-model.*new-model/);
    assert.match(notices[1]!, /\[opt-out:CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP\]$/);
  });

  it('omits the opt-out hint when no renderer is injected (plain data)', () => {
    const res: StartupMigrationResult = {
      ranVersion: CURRENT_MIGRATION_VERSION,
      relocatedDirs: [],
      modelAlias: { from: 'old-model', to: 'new-model', path: '/tmp/config.json' },
      renamedConfigKeys: [],
    };
    const notices = formatStartupMigrationNotices(res);
    assert.equal(notices.length, 1);
    assert.doesNotMatch(notices[0]!, /opt out/);
  });
});
