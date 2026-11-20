import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  persistSkillUsageStat,
  loadPersistedSkillUsage,
  loadGlobalConfig,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  type ProjectConfig,
} from '../src/config';

/**
 * U006 — persisted skill-usage frecency (parity: the reference's global-config
 * `skillUsage`). Exercises the core disk layer: the skillUsage config field is a
 * recognized, type-validated key, and persistSkillUsageStat / loadPersistedSkillUsage
 * round-trip through the real global config file under a temp HOME.
 */
describe('skillUsage config field validation', () => {
  it('is a recognized config key (not flagged as unknown/typo)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('skillUsage'));
    assert.deepEqual(findUnknownConfigKeys({ skillUsage: {} } as ProjectConfig), []);
  });

  it('accepts an object value and rejects a non-object', () => {
    assert.deepEqual(
      validateProjectConfigValues({ skillUsage: { planner: { usageCount: 2, lastUsedAt: 10 } } }),
      [],
    );
    const issues = validateProjectConfigValues({ skillUsage: 'nope' as unknown as ProjectConfig['skillUsage'] });
    assert.equal(issues.length, 1);
    assert.equal(issues[0].field, 'skillUsage');
  });
});

describe('persistSkillUsageStat / loadPersistedSkillUsage (global config round-trip)', () => {
  let tmp: string;
  let origHome: string | undefined;
  let origUserProfile: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-skillusage-'));
    origHome = process.env.HOME;
    origUserProfile = process.env.USERPROFILE;
    process.env.HOME = tmp;
    process.env.USERPROFILE = tmp;
  });

  afterEach(() => {
    if (origHome === undefined) delete process.env.HOME; else process.env.HOME = origHome;
    if (origUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = origUserProfile;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  it('writes a fresh entry then increments it on a second call, persisting to disk', () => {
    assert.deepEqual(loadPersistedSkillUsage(), {}, 'no history initially');

    assert.equal(persistSkillUsageStat('planner', 1000), true);
    let loaded = loadPersistedSkillUsage();
    assert.deepEqual(loaded.planner, { usageCount: 1, lastUsedAt: 1000 });

    assert.equal(persistSkillUsageStat('planner', 2000), true);
    loaded = loadPersistedSkillUsage();
    assert.deepEqual(loaded.planner, { usageCount: 2, lastUsedAt: 2000 });

    // Survives a cold read from the actual file via loadGlobalConfig.
    assert.deepEqual(loadGlobalConfig()?.skillUsage?.planner, { usageCount: 2, lastUsedAt: 2000 });
  });

  it('keeps independent counts per skill', () => {
    persistSkillUsageStat('a', 100);
    persistSkillUsageStat('b', 200);
    persistSkillUsageStat('a', 300);
    const loaded = loadPersistedSkillUsage();
    assert.equal(loaded.a.usageCount, 2);
    assert.equal(loaded.b.usageCount, 1);
    assert.equal(loaded.a.lastUsedAt, 300);
  });

  it('returns false and does not throw when no global-config path is resolvable', () => {
    delete process.env.HOME;
    delete process.env.USERPROFILE;
    assert.equal(persistSkillUsageStat('x', 1), false);
    assert.deepEqual(loadPersistedSkillUsage(), {});
  });
});
