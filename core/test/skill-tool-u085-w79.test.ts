// U085 (w79) — additive skill-tool parity building blocks:
//   * `requiresGitRepo` skill precondition — a worktree/PR-spawning skill refuses a
//     non-git cwd with an explanatory up-front message (parity: the reference's getIsGit()
//     guard in /batch), surfaced via the new `isGitRepo` core git helper.
//   * `requiresGitRepo` is a reviewed-safe declarative property (never forces the
//     permission gate).
//   * skill-tool `args` description documents the supported placeholder forms
//     ($ARGUMENTS, $ARGUMENTS[N], $N, $0).
//   * `recordSkillUsage` DEBOUNCED global-config write (parity: the reference's
//     SKILL_USAGE_DEBOUNCE_MS) — hot-repeat invocations skip the disk round-trip.

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

import { skillTool, SAFE_SKILL_PROPERTIES, skillHasOnlySafeProperties } from '../src/tools/skill';
import { isGitRepo } from '../src/utils/git';
import {
  recordSkillUsageDebounced,
  loadPersistedSkillUsage,
  SKILL_USAGE_DEBOUNCE_MS,
} from '../src/config';
import type { SkillDefinition } from '../src/skills';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp/u085-w79',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: SkillDefinition) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: (n: string) => (n === skill.name ? 'do the thing' : null),
    listSkills: () => [skill],
    getAgent: () => null,
    listAgents: () => [],
  } as any;
}

// ===========================================================================
// isGitRepo core helper
// ===========================================================================

describe('isGitRepo — git working-tree predicate', () => {
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-isgit-')); });
  afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  it('returns false for a non-git directory', () => {
    assert.equal(isGitRepo(tmp), false);
  });

  it('returns true inside an initialized repo', () => {
    execFileSync('git', ['init', '-q'], { cwd: tmp });
    assert.equal(isGitRepo(tmp), true);
  });

  it('fails closed for a nonexistent cwd', () => {
    assert.equal(isGitRepo(path.join(tmp, 'does-not-exist')), false);
  });
});

// ===========================================================================
// requiresGitRepo precondition in the skill tool
// ===========================================================================

describe('skillTool — requiresGitRepo precondition', () => {
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-skill-git-')); });
  afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  const gitSkill = (): SkillDefinition => ({
    name: 'batch', description: 'd', content: 'body', filePath: '<test>',
    context: 'inline', requiresGitRepo: true,
  });

  it('blocks the skill from a non-git cwd with an explanatory message', async () => {
    const res = await skillTool.execute(
      { skill: 'batch' },
      ctx({ cwd: tmp, _skillManager: manager(gitSkill()) }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /requires a git repository/i);
    assert.match(res.output, /git init/);
  });

  it('allows the skill once the cwd is a git repo', async () => {
    execFileSync('git', ['init', '-q'], { cwd: tmp });
    let activated: string | undefined;
    const res = await skillTool.execute(
      { skill: 'batch' },
      ctx({
        cwd: tmp,
        _skillManager: manager(gitSkill()),
        _engineSetActiveSkill: (n?: string) => { activated = n; },
      }),
    );
    assert.equal(res.isError, undefined);
    assert.match(res.output, /activated inline/);
    assert.equal(activated, 'batch');
  });

  it('does not gate a skill that omits requiresGitRepo', async () => {
    const plain: SkillDefinition = {
      name: 'explain', description: 'd', content: 'body', filePath: '<test>', context: 'inline',
    };
    const res = await skillTool.execute(
      { skill: 'explain' },
      ctx({ cwd: tmp, _skillManager: manager(plain) }),
    );
    assert.equal(res.isError, undefined);
  });
});

// ===========================================================================
// requiresGitRepo is reviewed-safe (does not force the permission gate)
// ===========================================================================

describe('requiresGitRepo is a reviewed-safe property', () => {
  it('is a member of SAFE_SKILL_PROPERTIES', () => {
    assert.ok(SAFE_SKILL_PROPERTIES.has('requiresGitRepo'));
  });

  it('a skill using only requiresGitRepo stays auto-run eligible', () => {
    const s: SkillDefinition = {
      name: 's', description: 'd', content: 'body', filePath: '<test>', requiresGitRepo: true,
    };
    assert.equal(skillHasOnlySafeProperties(s as unknown as Record<string, unknown>), true);
  });
});

// ===========================================================================
// skill-tool args placeholder documentation (gap: $ARGUMENTS[N] / $N / $0)
// ===========================================================================

describe('skillTool args description documents placeholder forms', () => {
  it('lists $ARGUMENTS, $ARGUMENTS[N], $N and $0', () => {
    const desc = (skillTool.inputSchema.properties as any).args.description as string;
    assert.match(desc, /\$ARGUMENTS\b/);
    assert.match(desc, /\$ARGUMENTS\[N\]/);
    assert.match(desc, /\$N\b/);
    assert.match(desc, /\$0\b/);
  });
});

// ===========================================================================
// recordSkillUsage — debounced global-config write
// ===========================================================================

describe('recordSkillUsageDebounced — debounced persistence', () => {
  let tmp: string;
  let origHome: string | undefined;
  let origUserProfile: string | undefined;

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-skillusage-w79-'));
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

  it('exposes a 60s debounce window', () => {
    assert.equal(SKILL_USAGE_DEBOUNCE_MS, 60_000);
  });

  it('writes the first call, skips a hot repeat, then writes again after the window', () => {
    // The debounce map is caller-owned (instance-scoped in SkillManager); a local map +
    // a unique skill name keep this case fully deterministic and isolated.
    const cache = new Map<string, number>();
    const name = `w79-debounce-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const t0 = 1_000_000;

    assert.equal(recordSkillUsageDebounced(name, cache, t0), true, 'first call persists');
    assert.deepEqual(loadPersistedSkillUsage()[name], { usageCount: 1, lastUsedAt: t0 });

    // Within the debounce window → skipped, no disk write, count unchanged.
    assert.equal(recordSkillUsageDebounced(name, cache, t0 + SKILL_USAGE_DEBOUNCE_MS - 1), false, 'hot repeat is debounced');
    assert.deepEqual(loadPersistedSkillUsage()[name], { usageCount: 1, lastUsedAt: t0 });

    // At/after the window → writes again, count increments.
    assert.equal(recordSkillUsageDebounced(name, cache, t0 + SKILL_USAGE_DEBOUNCE_MS), true, 'write resumes after the window');
    assert.deepEqual(loadPersistedSkillUsage()[name], { usageCount: 2, lastUsedAt: t0 + SKILL_USAGE_DEBOUNCE_MS });
  });

  it('a separate cache (independent session) is NOT debounced by another cache', () => {
    const name = `w79-indep-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const t0 = 2_000_000;
    assert.equal(recordSkillUsageDebounced(name, new Map(), t0), true);
    // A DIFFERENT owner (fresh map) writes again immediately — no cross-session suppression.
    assert.equal(recordSkillUsageDebounced(name, new Map(), t0 + 1), true);
    assert.equal(loadPersistedSkillUsage()[name]?.usageCount, 2);
  });

  it('ignores an empty skill name', () => {
    assert.equal(recordSkillUsageDebounced('', new Map(), 5_000), false);
  });
});
