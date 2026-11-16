// U023-w67 — skill/command metadata flags (parity with Claude Code's command
// `isSensitive` / `isHidden` / `userFacingName()` / `disableNonInteractive` /
// `supportsNonInteractive`) + the generic `wasGenerated` agent provenance flag.
// Pure-helper unit coverage lives here; the skill-tool end-to-end wiring (arg
// masking + non-interactive gate) is exercised against the real skillTool below.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  REDACTED_SKILL_ARGS,
  redactSkillArgs,
  isSkillHidden,
  skillUserFacingName,
  isSkillAvailableNonInteractive,
  parseBooleanFlag,
  parseBackgroundFlag,
  SAFE_SKILL_PROPERTIES,
} from '../src/skills';
import { skillTool } from '../src/tools/skill';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// redactSkillArgs — mask sensitive args
// ---------------------------------------------------------------------------

describe('redactSkillArgs — isSensitive argument masking', () => {
  it('masks a non-empty argument when the skill is sensitive', () => {
    assert.equal(redactSkillArgs({ isSensitive: true }, 'my-secret-token'), REDACTED_SKILL_ARGS);
    assert.equal(REDACTED_SKILL_ARGS, '***');
  });

  it('passes the argument through for a non-sensitive skill', () => {
    assert.equal(redactSkillArgs({ isSensitive: false }, 'hello'), 'hello');
    assert.equal(redactSkillArgs({}, 'hello'), 'hello');
  });

  it('never masks an empty / whitespace-only argument (nothing to hide)', () => {
    assert.equal(redactSkillArgs({ isSensitive: true }, ''), '');
    assert.equal(redactSkillArgs({ isSensitive: true }, '   '), '   ');
  });
});

// ---------------------------------------------------------------------------
// isSkillHidden
// ---------------------------------------------------------------------------

describe('isSkillHidden', () => {
  it('is true only for an explicit isHidden === true', () => {
    assert.equal(isSkillHidden({ isHidden: true }), true);
    assert.equal(isSkillHidden({ isHidden: false }), false);
    assert.equal(isSkillHidden({}), false);
  });
});

// ---------------------------------------------------------------------------
// skillUserFacingName
// ---------------------------------------------------------------------------

describe('skillUserFacingName', () => {
  it('prefers a non-empty trimmed userFacingName', () => {
    assert.equal(skillUserFacingName({ name: 'deploy', userFacingName: '  Deploy App  ' }), 'Deploy App');
  });

  it('falls back to the canonical name when userFacingName is absent/blank', () => {
    assert.equal(skillUserFacingName({ name: 'deploy' }), 'deploy');
    assert.equal(skillUserFacingName({ name: 'deploy', userFacingName: '   ' }), 'deploy');
  });
});

// ---------------------------------------------------------------------------
// isSkillAvailableNonInteractive — disableNonInteractive / supportsNonInteractive
// ---------------------------------------------------------------------------

describe('isSkillAvailableNonInteractive', () => {
  it('every skill is available in interactive mode', () => {
    assert.equal(isSkillAvailableNonInteractive({ disableNonInteractive: true }, false), true);
    assert.equal(isSkillAvailableNonInteractive({ supportsNonInteractive: false }, false), true);
  });

  it('disableNonInteractive hides the skill in a non-interactive run', () => {
    assert.equal(isSkillAvailableNonInteractive({ disableNonInteractive: true }, true), false);
  });

  it('supportsNonInteractive === false hides the skill in a non-interactive run', () => {
    assert.equal(isSkillAvailableNonInteractive({ supportsNonInteractive: false }, true), false);
  });

  it('supportsNonInteractive === true overrides disableNonInteractive', () => {
    assert.equal(
      isSkillAvailableNonInteractive({ disableNonInteractive: true, supportsNonInteractive: true }, true),
      true,
    );
  });

  it('a skill with neither flag is available everywhere', () => {
    assert.equal(isSkillAvailableNonInteractive({}, true), true);
  });
});

// ---------------------------------------------------------------------------
// parseBooleanFlag — tri-state (distinct from parseBackgroundFlag)
// ---------------------------------------------------------------------------

describe('parseBooleanFlag', () => {
  it('parses true / false / absent and preserves an explicit false', () => {
    assert.deepEqual(parseBooleanFlag('true'), { value: true, invalid: false });
    assert.deepEqual(parseBooleanFlag(true), { value: true, invalid: false });
    assert.deepEqual(parseBooleanFlag('false'), { value: false, invalid: false });
    assert.deepEqual(parseBooleanFlag(false), { value: false, invalid: false });
    assert.deepEqual(parseBooleanFlag(undefined), { value: undefined, invalid: false });
  });

  it('flags a garbled value as invalid', () => {
    assert.deepEqual(parseBooleanFlag('yes'), { value: undefined, invalid: true });
    assert.deepEqual(parseBooleanFlag(3), { value: undefined, invalid: true });
  });

  it('differs from parseBackgroundFlag: false is PRESERVED, not collapsed to undefined', () => {
    assert.equal(parseBooleanFlag('false').value, false);
    assert.equal(parseBackgroundFlag('false').value, undefined);
  });
});

// ---------------------------------------------------------------------------
// SAFE_SKILL_PROPERTIES — the new flags are reviewed-safe (won't force the gate)
// ---------------------------------------------------------------------------

describe('SAFE_SKILL_PROPERTIES includes the new metadata flags', () => {
  it('lists isSensitive / isHidden / userFacingName / non-interactive gates', () => {
    for (const k of ['isSensitive', 'isHidden', 'userFacingName', 'disableNonInteractive', 'supportsNonInteractive']) {
      assert.ok(SAFE_SKILL_PROPERTIES.has(k), `expected ${k} to be reviewed-safe`);
    }
  });
});

// ---------------------------------------------------------------------------
// End-to-end through the real skillTool
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    _engineAddSystemReminder: () => {},
    _engineSetActiveSkill: () => {},
    ...overrides,
  } as unknown as ToolContext;
}

function manager(skill: any) {
  return {
    getSkill: (n: string) => (n === skill.name ? skill : null),
    getSkillContent: () => 'do the thing',
    listSkills: () => [skill],
  } as any;
}

describe('skillTool — isSensitive masks args in the permission echo (E2E)', () => {
  function capturingHandler() {
    const calls: string[] = [];
    return {
      calls,
      handler: { showPrompt: async (tool: string, summary: string) => { calls.push(summary); return { action: 'allow_once' as const }; } },
    };
  }

  it('masks a sensitive skill\'s argument as *** in the echoed summary', async () => {
    const h = capturingHandler();
    const r = await skillTool.execute(
      { skill: 'login', args: 'super-secret-password' },
      ctx({
        _skillManager: manager({ name: 'login', context: 'inline', isSensitive: true }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'login', action: 'ask' }],
        _permissionPromptHandler: h.handler as any,
      }),
    );
    assert.ok(!r.isError);
    assert.equal(h.calls.length, 1);
    assert.match(h.calls[0], /skill: login \*\*\*/);
    assert.doesNotMatch(h.calls[0], /super-secret-password/);
  });

  it('a NON-sensitive skill still echoes its real argument (discriminates the fix)', async () => {
    const h = capturingHandler();
    await skillTool.execute(
      { skill: 'login', args: 'super-secret-password' },
      ctx({
        _skillManager: manager({ name: 'login', context: 'inline' }),
        _parentPermissionRules: [{ tool: 'skill', pattern: 'login', action: 'ask' }],
        _permissionPromptHandler: h.handler as any,
      }),
    );
    assert.match(h.calls[0], /skill: login super-secret-password/);
  });
});

describe('skillTool — non-interactive gating (E2E)', () => {
  it('refuses a disableNonInteractive skill in detached mode', async () => {
    const r = await skillTool.execute(
      { skill: 'wizard' },
      ctx({
        _skillManager: manager({ name: 'wizard', context: 'inline', disableNonInteractive: true }),
        detachedMode: true,
      }),
    );
    assert.ok(r.isError);
    assert.match(r.output, /not available in non-interactive/);
  });

  it('runs the same skill in interactive mode', async () => {
    const r = await skillTool.execute(
      { skill: 'wizard' },
      ctx({
        _skillManager: manager({ name: 'wizard', context: 'inline', disableNonInteractive: true }),
        detachedMode: false,
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
  });

  it('supportsNonInteractive === true lets a skill run while detached', async () => {
    const r = await skillTool.execute(
      { skill: 'wizard' },
      ctx({
        _skillManager: manager({ name: 'wizard', context: 'inline', disableNonInteractive: true, supportsNonInteractive: true }),
        detachedMode: true,
      }),
    );
    assert.ok(!r.isError);
    assert.match(r.output, /activated inline/);
  });
});
