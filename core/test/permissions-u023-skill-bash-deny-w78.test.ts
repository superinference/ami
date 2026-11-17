// U023 (core/core/src/permissions.ts) — PermissionManager.isBashCommandDenied:
// the synchronous deny-half of hasPermissionsToUseTool that the SkillManager uses to
// gate a trusted skill body's live `!`cmd` injections (parity with Claude Code's
// promptShellExecution permission check). It must reuse the SAME canonical bash
// deny-matching check() uses — compound-command fanout, $(...) decomposition, and
// de-obfuscation — so a deny configured for the model's own bash calls is honored
// for skill-embedded commands too.
//
// SAFE BY CONSTRUCTION: no command is ever executed here — only rule MATCHING is
// exercised. The strings are inert pattern-match fixtures.
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { PermissionManager } from '../src/permissions';

describe('U023 — PermissionManager.isBashCommandDenied (skill injection deny gate)', () => {
  it('returns true when a bash deny rule matches the command', () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm *', action: 'deny' }]);
    assert.equal(pm.isBashCommandDenied('rm -rf build'), true);
  });

  it('returns false when no deny rule matches', () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm *', action: 'deny' }]);
    assert.equal(pm.isBashCommandDenied('git status'), false);
  });

  it('returns false with no rules at all', () => {
    const pm = new PermissionManager('ask', []);
    assert.equal(pm.isBashCommandDenied('rm -rf build'), false);
  });

  it('honors the deny across a COMPOUND command (not just the first sub-command)', () => {
    // matchBashPattern scans every sub-command of a chain, so a denied command
    // hidden after a benign one is still caught — matchPattern alone would miss it.
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm *', action: 'deny' }]);
    assert.equal(pm.isBashCommandDenied('cd /tmp && rm -rf x'), true);
  });

  it('a tool-wide bash deny (no pattern) denies every bash command', () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', action: 'deny' }]);
    assert.equal(pm.isBashCommandDenied('echo hi'), true);
  });

  it('a deny rule for a DIFFERENT tool does not apply to bash', () => {
    const pm = new PermissionManager('ask', [{ tool: 'file_edit', pattern: '*', action: 'deny' }]);
    assert.equal(pm.isBashCommandDenied('echo hi'), false);
  });

  it('a non-string or blank command is never denied (nothing to run)', () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', action: 'deny' }]);
    assert.equal(pm.isBashCommandDenied(''), false);
    assert.equal(pm.isBashCommandDenied('   '), false);
    assert.equal(pm.isBashCommandDenied(undefined as unknown as string), false);
  });

  it('an ALLOW rule does not deny — only deny rules gate the injection', () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm *', action: 'allow' }]);
    assert.equal(pm.isBashCommandDenied('rm -rf x'), false);
  });
});
