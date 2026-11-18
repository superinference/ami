/**
 * U010 / w105 — config.ts parity additions (Claude Code knobs AMI was missing):
 *   - diffTool ('terminal' | 'auto') + resolveDiffTool        (CC utils/config.ts DiffTool)
 *   - defaultShell ('bash' | 'powershell') + resolveDefaultShell
 *                                                             (CC utils/shell/resolveDefaultShell.ts)
 *   - exampleFiles / exampleFilesGeneratedAt + getCachedExampleFiles + EXAMPLE_FILES_TTL_MS
 *                                                             (CC utils/exampleCommands.ts)
 *   - companyAnnouncements + pickCompanyAnnouncement          (CC LogoV2 companyAnnouncements)
 *   - allowUnsandboxedCommands + areUnsandboxedCommandsAllowed
 *                                                             (CC shouldUseSandbox areUnsandboxedCommandsAllowed)
 *
 * RED-GREEN discriminators (each fails if the resolver is weakened):
 *  - "stale cache returns none" fails if getCachedExampleFiles ignores the TTL.
 *  - "first startup is deterministic" fails if pickCompanyAnnouncement randomizes on startup 1.
 *  - "only an explicit false disables the override" fails if areUnsandboxedCommandsAllowed
 *    treats unset/malformed as disabled (would force-sandbox by default — a behavior change).
 *
 * Run: cd core/core && node --import tsx --test test/config-u010-w105.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  resolveDiffTool,
  resolveDefaultShell,
  getCachedExampleFiles,
  EXAMPLE_FILES_TTL_MS,
  pickCompanyAnnouncement,
  areUnsandboxedCommandsAllowed,
  mergeConfigs,
  validateProjectConfigValues,
  findUnknownConfigKeys,
  KNOWN_CONFIG_KEYS,
  getConfigJSONSchema,
  type ProjectConfig,
} from '../src/config';

// ---------------------------------------------------------------------------
// diffTool / resolveDiffTool
// ---------------------------------------------------------------------------
describe('resolveDiffTool', () => {
  it("defaults to 'auto' (unset / null / undefined)", () => {
    assert.equal(resolveDiffTool(undefined), 'auto');
    assert.equal(resolveDiffTool(null), 'auto');
    assert.equal(resolveDiffTool({}), 'auto');
  });
  it("returns 'terminal' only when explicitly set", () => {
    assert.equal(resolveDiffTool({ diffTool: 'terminal' }), 'terminal');
    assert.equal(resolveDiffTool({ diffTool: 'auto' }), 'auto');
  });
});

// ---------------------------------------------------------------------------
// defaultShell / resolveDefaultShell
// ---------------------------------------------------------------------------
describe('resolveDefaultShell', () => {
  it("defaults to 'bash' everywhere (no Windows auto-flip)", () => {
    assert.equal(resolveDefaultShell(undefined), 'bash');
    assert.equal(resolveDefaultShell(null), 'bash');
    assert.equal(resolveDefaultShell({}), 'bash');
  });
  it("returns 'powershell' only when explicitly selected", () => {
    assert.equal(resolveDefaultShell({ defaultShell: 'powershell' }), 'powershell');
    assert.equal(resolveDefaultShell({ defaultShell: 'bash' }), 'bash');
  });
});

// ---------------------------------------------------------------------------
// exampleFiles cache / getCachedExampleFiles
// ---------------------------------------------------------------------------
describe('getCachedExampleFiles', () => {
  it('returns [] when unset or empty', () => {
    assert.deepEqual(getCachedExampleFiles(undefined), []);
    assert.deepEqual(getCachedExampleFiles({}), []);
    assert.deepEqual(getCachedExampleFiles({ exampleFiles: [] }), []);
  });
  it('returns the cached files when fresh', () => {
    const now = 1_000_000_000_000;
    const cfg: ProjectConfig = { exampleFiles: ['a.ts', 'b.ts'], exampleFilesGeneratedAt: now - 1000 };
    assert.deepEqual(getCachedExampleFiles(cfg, now), ['a.ts', 'b.ts']);
  });
  it('stale cache returns none (older than the one-week TTL)', () => {
    const now = 1_000_000_000_000;
    const cfg: ProjectConfig = { exampleFiles: ['a.ts'], exampleFilesGeneratedAt: now - EXAMPLE_FILES_TTL_MS - 1 };
    assert.deepEqual(getCachedExampleFiles(cfg, now), []);
  });
  it('a never-generated cache (no timestamp) is treated as expired', () => {
    const now = 1_000_000_000_000;
    assert.deepEqual(getCachedExampleFiles({ exampleFiles: ['a.ts'] }, now), []);
  });
  it('TTL is exactly one week', () => {
    assert.equal(EXAMPLE_FILES_TTL_MS, 7 * 24 * 60 * 60 * 1000);
  });
});

// ---------------------------------------------------------------------------
// companyAnnouncements / pickCompanyAnnouncement
// ---------------------------------------------------------------------------
describe('pickCompanyAnnouncement', () => {
  it('returns undefined when none configured', () => {
    assert.equal(pickCompanyAnnouncement(undefined, 1), undefined);
    assert.equal(pickCompanyAnnouncement({}, 1), undefined);
    assert.equal(pickCompanyAnnouncement({ companyAnnouncements: [] }, 1), undefined);
  });
  it('first startup is deterministic (index 0)', () => {
    const cfg: ProjectConfig = { companyAnnouncements: ['hello', 'world', 'third'] };
    // Even with a rand that would pick a later index, startup 1 shows the first entry.
    assert.equal(pickCompanyAnnouncement(cfg, 1, () => 0.99), 'hello');
  });
  it('later startups pick at random (injected rand)', () => {
    const cfg: ProjectConfig = { companyAnnouncements: ['a', 'b', 'c'] };
    assert.equal(pickCompanyAnnouncement(cfg, 2, () => 0), 'a');
    assert.equal(pickCompanyAnnouncement(cfg, 5, () => 0.5), 'b');
    assert.equal(pickCompanyAnnouncement(cfg, 9, () => 0.99), 'c');
  });
});

// ---------------------------------------------------------------------------
// allowUnsandboxedCommands / areUnsandboxedCommandsAllowed
// ---------------------------------------------------------------------------
describe('areUnsandboxedCommandsAllowed', () => {
  it('defaults to true (unset preserves AMI behavior — override honored)', () => {
    assert.equal(areUnsandboxedCommandsAllowed(undefined), true);
    assert.equal(areUnsandboxedCommandsAllowed(null), true);
    assert.equal(areUnsandboxedCommandsAllowed({}), true);
    assert.equal(areUnsandboxedCommandsAllowed({ allowUnsandboxedCommands: true }), true);
  });
  it('only an explicit false disables the override (managed hard-disable)', () => {
    assert.equal(areUnsandboxedCommandsAllowed({ allowUnsandboxedCommands: false }), false);
  });
  it('a malformed value fails safe (open), like unset', () => {
    assert.equal(
      areUnsandboxedCommandsAllowed({ allowUnsandboxedCommands: 'no' as unknown as boolean }),
      true,
    );
  });
});

// ---------------------------------------------------------------------------
// Recognition / merge / validation / schema wiring (shared across the new knobs)
// ---------------------------------------------------------------------------
describe('w105 config knobs — recognition, merge, validation, schema', () => {
  it('are recognized keys (no spurious unknown-key warning)', () => {
    for (const k of ['diffTool', 'defaultShell', 'exampleFiles', 'exampleFilesGeneratedAt', 'companyAnnouncements', 'allowUnsandboxedCommands']) {
      assert.ok(KNOWN_CONFIG_KEYS.has(k), `${k} is a known config key`);
    }
    assert.deepEqual(
      findUnknownConfigKeys({
        diffTool: 'auto',
        defaultShell: 'bash',
        exampleFiles: ['a.ts'],
        exampleFilesGeneratedAt: 1,
        companyAnnouncements: ['hi'],
        allowUnsandboxedCommands: false,
      }),
      [],
    );
  });
  it('merge through with higher-priority-layer-wins semantics', () => {
    const merged = mergeConfigs(
      {},
      {},
      { diffTool: 'terminal', defaultShell: 'powershell' },
      { diffTool: 'auto', defaultShell: 'bash', allowUnsandboxedCommands: false },
    );
    assert.equal(merged.diffTool, 'terminal', 'higher-priority project layer wins');
    assert.equal(merged.defaultShell, 'powershell', 'higher-priority project layer wins');
    assert.equal(merged.allowUnsandboxedCommands, false, 'lower-layer value retained when not overridden');
  });
  it('merge through example-cache + announcements arrays', () => {
    const merged = mergeConfigs(
      {},
      {},
      { exampleFiles: ['x.ts'], exampleFilesGeneratedAt: 42 },
      { companyAnnouncements: ['note'] },
    );
    assert.deepEqual(merged.exampleFiles, ['x.ts']);
    assert.equal(merged.exampleFilesGeneratedAt, 42);
    assert.deepEqual(merged.companyAnnouncements, ['note']);
  });
  it('validate value types + enums (good values produce no issue)', () => {
    assert.deepEqual(
      validateProjectConfigValues({
        diffTool: 'terminal',
        defaultShell: 'powershell',
        exampleFiles: ['a.ts'],
        exampleFilesGeneratedAt: 1,
        companyAnnouncements: ['hi'],
        allowUnsandboxedCommands: false,
      }),
      [],
    );
  });
  it('flag an invalid diffTool / defaultShell enum value', () => {
    const d = validateProjectConfigValues({ diffTool: 'vscode' as unknown as 'auto' });
    assert.ok(d.some((i) => i.field === 'diffTool'), 'bad diffTool enum flagged');
    const s = validateProjectConfigValues({ defaultShell: 'fish' as unknown as 'bash' });
    assert.ok(s.some((i) => i.field === 'defaultShell'), 'bad defaultShell enum flagged');
  });
  it('flag a wrong base type for the array / number / boolean knobs', () => {
    const issues = validateProjectConfigValues({
      exampleFiles: 'a.ts' as unknown as string[],
      exampleFilesGeneratedAt: 'soon' as unknown as number,
      companyAnnouncements: 'hi' as unknown as string[],
      allowUnsandboxedCommands: 'no' as unknown as boolean,
    });
    for (const f of ['exampleFiles', 'exampleFilesGeneratedAt', 'companyAnnouncements', 'allowUnsandboxedCommands']) {
      assert.ok(issues.some((i) => i.field === f), `${f} wrong type flagged`);
    }
  });
  it('appear in the generated JSON schema', () => {
    const schema = getConfigJSONSchema();
    assert.deepEqual(schema.properties.diffTool?.enum, ['terminal', 'auto']);
    assert.deepEqual(schema.properties.defaultShell?.enum, ['bash', 'powershell']);
    assert.equal(schema.properties.exampleFiles?.type, 'array');
    assert.equal(schema.properties.exampleFilesGeneratedAt?.type, 'number');
    assert.equal(schema.properties.companyAnnouncements?.type, 'array');
    assert.equal(schema.properties.allowUnsandboxedCommands?.type, 'boolean');
  });
});
