import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  containsShellSubstitution,
  type PermissionMode,
} from '../src/permissions.ts';

// ===========================================================================
// UNIT U003 — wave w61 parity gaps in core/core/src/permissions.ts
// ===========================================================================

// ---------------------------------------------------------------------------
// Gap: ps — detect BSD-style 'e' modifier (env exposure) in letter-only tokens
//   WITHOUT over-blocking UNIX `-e` (select all).
//   claudeRef references/claude/.../BashTool/readOnlyValidation.ts:L418-L429
//
// Before: ps config blocked the bare token 'e', and validateCommandFlags only
// inspects dash-prefixed args — so `ps -e` (safe UNIX select-all) was flag='e'
// and WRONGLY blocked, while the dangerous BSD `ps axe` token (no dash) was
// never inspected and auto-'safe' (exposes every process's environment).
// ---------------------------------------------------------------------------
describe('U003/w61: ps BSD `e` modifier vs UNIX -e', () => {
  const pm = new PermissionManager('ask');

  it('UNIX `ps -e` (select all) is safe — no longer over-blocked', () => {
    assert.equal(pm.classifyBashCommand('ps -e'), 'safe');
  });

  it('UNIX `ps -ef` is safe', () => {
    assert.equal(pm.classifyBashCommand('ps -ef'), 'safe');
  });

  it('BSD `ps axe` (environment modifier) is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('ps axe'), 'safe');
  });

  it('BSD `ps auxe` (environment modifier) is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('ps auxe'), 'safe');
  });

  it('BSD `ps aux` (no e modifier) stays safe', () => {
    assert.equal(pm.classifyBashCommand('ps aux'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// Gap: man/help flag allowlist — block `man -P <cmd>` (arbitrary pager exec)
//   and restrict `help` to bash-builtin help flags (help is often aliased to
//   man). Wires the implemented-but-unwired detectUnsafeHelpCommand into the
//   classifier.
//   claudeRef references/claude/.../BashTool/readOnlyValidation.ts:L304-L332
// ---------------------------------------------------------------------------
describe('U003/w61: man/help unsafe-flag gating', () => {
  const pm = new PermissionManager('ask');

  it('`man -P /bin/sh ls` (pager exec) is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('man -P /bin/sh ls'), 'safe');
  });

  it('`man -l ./evil.1` (local file) is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('man -l ./evil.1'), 'safe');
  });

  it('`man --html=firefox ls` is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('man --html=firefox ls'), 'safe');
  });

  it('plain `man ls` stays safe', () => {
    assert.equal(pm.classifyBashCommand('man ls'), 'safe');
  });

  it('`help -P /bin/sh` (aliased-to-man attack shape) is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('help -P /bin/sh'), 'safe');
  });

  it('`help -d echo` (allowed builtin help flag) stays safe', () => {
    assert.equal(pm.classifyBashCommand('help -d echo'), 'safe');
  });

  it('plain `help echo` stays safe', () => {
    assert.equal(pm.classifyBashCommand('help echo'), 'safe');
  });

  it('`git help --web` (opens a browser) is NOT auto-safe', () => {
    assert.notEqual(pm.classifyBashCommand('git help --web commit'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// Gap: reject `$` variable-expansion / special-parameter tokens on auto-approved
//   commands (parser-differential defense). containsShellSubstitution is now a
//   quote-state machine covering special parameters ($@ $* $# $? $! $$ $0-9 $-)
//   and correctly handling single-quote escape semantics.
//   claudeRef references/claude/.../BashTool/readOnlyValidation.ts:L1328-L1369,L1600-1669
// ---------------------------------------------------------------------------
describe('U003/w61: containsShellSubstitution special parameters', () => {
  it('still detects $VAR', () => {
    assert.equal(containsShellSubstitution('echo $HOME'), true);
  });
  it('still detects ${VAR}', () => {
    assert.equal(containsShellSubstitution('echo ${HOME}'), true);
  });
  it('still detects $(cmd)', () => {
    assert.equal(containsShellSubstitution('echo $(whoami)'), true);
  });
  it('now detects $? (exit status special parameter)', () => {
    assert.equal(containsShellSubstitution('echo $?'), true);
  });
  it('now detects $@ (all args)', () => {
    assert.equal(containsShellSubstitution('echo $@'), true);
  });
  it('now detects $# (arg count)', () => {
    assert.equal(containsShellSubstitution('echo $#'), true);
  });
  it('now detects $! (last bg pid)', () => {
    assert.equal(containsShellSubstitution('echo $!'), true);
  });
  it('now detects positional $1', () => {
    assert.equal(containsShellSubstitution('echo $1'), true);
  });
  it('still tolerates globs (no $)', () => {
    assert.equal(containsShellSubstitution('ls *.ts'), false);
    assert.equal(containsShellSubstitution('ls file?.ts'), false);
    assert.equal(containsShellSubstitution('ls [abc].ts'), false);
  });
  it('single-quoted $VAR is literal (no expansion)', () => {
    assert.equal(containsShellSubstitution("echo '$HOME'"), false);
    assert.equal(containsShellSubstitution("echo '$?'"), false);
  });
  it('quoted $VAR is tolerated (AMI for-loop perk — single AND double quotes)', () => {
    // AMI deliberately tolerates `$` inside quotes so the for-loop-with-glob perk
    // (`for f in *.ts; do cat "$f"; done`) stays auto-safe. Only UNQUOTED `$` flags.
    assert.equal(containsShellSubstitution('echo "$HOME"'), false);
    assert.equal(containsShellSubstitution('echo "$?"'), false);
  });
  it('a `$` not followed by an expansion char is not a substitution', () => {
    assert.equal(containsShellSubstitution('echo cost is 5$ total'), false);
  });
  it("single-quote-with-escape does not desync the tracker (ls '\\' then a $ outside)", () => {
    // In bash `\` is literal inside '...'; the closing quote still closes.
    assert.equal(containsShellSubstitution("ls '\\' $HOME"), true);
  });
  it('classifier: `echo $?` is no longer auto-safe', () => {
    const pm = new PermissionManager('ask');
    assert.notEqual(pm.classifyBashCommand('echo $?'), 'safe');
  });
});

// ---------------------------------------------------------------------------
// Gap: recognize `time` as a command wrapper so safety/permission analysis
//   unwraps to the INNER command (parity with Claude's `time` command spec).
//   claudeRef references/claude/.../utils/bash/specs/time.ts:L3-L11
// ---------------------------------------------------------------------------
describe('U003/w61: `time` command-wrapper unwrapping', () => {
  const pm = new PermissionManager('ask');

  it('`time ls` classifies as the inner safe command', () => {
    assert.equal(pm.classifyBashCommand('time ls'), 'safe');
  });

  it('`time cat file.txt` classifies as safe', () => {
    assert.equal(pm.classifyBashCommand('time cat file.txt'), 'safe');
  });

  it('`time -p ls` (POSIX flag) unwraps to the inner command', () => {
    assert.equal(pm.classifyBashCommand('time -p ls'), 'safe');
  });

  it('`time rm -rf x` still classifies destructive (inner command analyzed)', () => {
    assert.equal(pm.classifyBashCommand('time rm -rf x'), 'destructive');
  });
});

// ---------------------------------------------------------------------------
// Gap: ToolPermissionContext.prePlanMode — remember and restore the permission
//   mode a session was in before entering plan mode.
//   claudeRef references/claude/.../types/permissions.ts:L426-L441
// ---------------------------------------------------------------------------
describe('U003/w61: prePlanMode capture + restore', () => {
  it('captures the prior mode when entering plan', () => {
    const pm = new PermissionManager('acceptEdits');
    pm.setMode('plan');
    assert.equal(pm.getPrePlanMode(), 'acceptEdits');
    assert.equal(pm.getMode(), 'plan');
  });

  it('exitPlanMode restores the captured prior mode', () => {
    const pm = new PermissionManager('acceptEdits');
    pm.setMode('plan');
    const restored = pm.exitPlanMode();
    assert.equal(restored, 'acceptEdits');
    assert.equal(pm.getMode(), 'acceptEdits');
    assert.equal(pm.getPrePlanMode(), undefined);
  });

  it('does not overwrite prePlanMode on a plan→plan no-op', () => {
    const pm = new PermissionManager('ask');
    pm.setMode('plan');
    pm.setMode('plan');
    assert.equal(pm.getPrePlanMode(), 'ask');
  });

  it('clears prePlanMode when switching to an explicit non-plan mode', () => {
    const pm = new PermissionManager('ask');
    pm.setMode('plan');
    pm.setMode('acceptEdits');
    assert.equal(pm.getPrePlanMode(), undefined);
    assert.equal(pm.getMode(), 'acceptEdits');
  });

  it('exitPlanMode is a no-op (returns current mode) when not in plan', () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(pm.exitPlanMode(), 'acceptEdits');
    assert.equal(pm.getMode(), 'acceptEdits');
  });

  it('exitPlanMode falls back to ask when no prior mode was captured', () => {
    const pm = new PermissionManager('plan'); // constructed directly in plan
    const restored = pm.exitPlanMode();
    assert.equal(restored, 'ask');
    assert.equal(pm.getMode(), 'ask');
  });

  it('restoring a bypass mode blocked by managed policy falls back to ask', () => {
    const pm = new PermissionManager('auto-allow');
    pm.setMode('plan');
    assert.equal(pm.getPrePlanMode(), 'auto-allow');
    pm.setDisableBypass(true); // managed killswitch armed while in plan
    const restored = pm.exitPlanMode();
    assert.equal(restored, 'ask');
    assert.equal(pm.getMode(), 'ask');
  });
});

// A tiny compile-time guard that PermissionMode is importable/usable here.
const _modeCheck: PermissionMode = 'plan';
void _modeCheck;
