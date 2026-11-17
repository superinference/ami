import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { isBashToolAllowRule, allowlistGrantsBash } from '../src/skills';

// ===========================================================================
// GAP U023 / gap [0] (medium): detect project-shipped bash-capable skills /
// slash commands as a workspace-trust signal (parity with Claude Code's
// TrustDialog `hasSkillsBash` / `hasSlashCommandBash`). These are the canonical
// pure core detectors the safe-mode settings scanner + the `/safe-mode audit`
// per-file workspace audit both delegate to, so "does this allowlist grant a
// shell" is decided in exactly one place.
//
// SAFE BY CONSTRUCTION: nothing is executed — only allowlist STRING matching is
// exercised. The strings are inert pattern-match fixtures.
// ===========================================================================

describe('isBashToolAllowRule — canonical bash-grant allowlist matcher', () => {
  it('matches the bare tool name in both cases', () => {
    assert.equal(isBashToolAllowRule('bash'), true);
    assert.equal(isBashToolAllowRule('Bash'), true);
    assert.equal(isBashToolAllowRule('  BASH  '), true); // trimmed + case-insensitive
  });

  it('matches a scoped rule (Bash(...)/bash(...))', () => {
    assert.equal(isBashToolAllowRule('Bash(git:*)'), true);
    assert.equal(isBashToolAllowRule('bash(npm:*)'), true);
    assert.equal(isBashToolAllowRule('Bash(curl:*)'), true);
  });

  it('matches the space-prefixed `bash <...>` form', () => {
    assert.equal(isBashToolAllowRule('bash --login'), true);
  });

  it('does NOT match a different tool whose name starts with "bash"-like text', () => {
    // `bashful` is a different tool — the boundary (space/paren/end) guards it.
    assert.equal(isBashToolAllowRule('bashful'), false);
    assert.equal(isBashToolAllowRule('bashrc_tool'), false);
  });

  it('does NOT match non-bash tools or category tokens', () => {
    assert.equal(isBashToolAllowRule('Read'), false);
    assert.equal(isBashToolAllowRule('Edit(~/.claude/settings.json)'), false);
    assert.equal(isBashToolAllowRule('read-only'), false);
    assert.equal(isBashToolAllowRule('mcp__playwright__click'), false);
  });

  it('returns false for a non-string / empty entry', () => {
    assert.equal(isBashToolAllowRule(undefined), false);
    assert.equal(isBashToolAllowRule(null), false);
    assert.equal(isBashToolAllowRule(42), false);
    assert.equal(isBashToolAllowRule(''), false);
    assert.equal(isBashToolAllowRule('   '), false);
  });
});

describe('allowlistGrantsBash — does a skill/command allowlist pre-grant bash', () => {
  it('true when ANY entry grants bash', () => {
    assert.equal(allowlistGrantsBash(['Read', 'Glob', 'Bash(curl:*)']), true);
    assert.equal(allowlistGrantsBash(['bash']), true);
  });

  it('false when no entry grants bash', () => {
    assert.equal(allowlistGrantsBash(['Read', 'Glob', 'Grep']), false);
    assert.equal(allowlistGrantsBash(['Edit(~/**)', 'mcp__playwright__*']), false);
  });

  it('false for an absent / empty allowlist (no explicit skill-scoped grant)', () => {
    assert.equal(allowlistGrantsBash(undefined), false);
    assert.equal(allowlistGrantsBash([]), false);
  });
});
