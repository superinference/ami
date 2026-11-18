import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  PermissionManager,
  bashCommandArgLeaksValue,
} from '../src/permissions';

// =============================================================================
// U-perm1 / w124 — permissions.ts parity gaps
// =============================================================================

// -----------------------------------------------------------------------------
// GAP: AST-based unsafe-compound (subshell / command-group) forced-ask.
//
// Claude forces behavior:'ask' (with no rule suggestions) whenever a command
// contains a subshell `( … )` or a brace command group `{ …; }`, because such a
// grouping can never be safely auto-allowed. AMI previously recursed into a
// subshell via extractBaseCommands and could classify a subshell whose inner
// command looked read-only as 'safe', letting acceptEdits / auto modes
// auto-allow it. The fix elevates any auto-'safe' grouped command to 'unsafe'
// (= prompt) using the existing analyzeCommandStructure AST, matching Claude's
// hasSubshell || hasCommandGroup forced-ask.
// -----------------------------------------------------------------------------
describe('U-perm1/w124 — subshell / command-group forced-ask (classifier)', () => {
  const pm = new PermissionManager('ask');

  it('a subshell whose inner command is read-only is NOT auto-safe (forced to a prompt)', () => {
    // RED on revert: these returned 'safe' before the forced-ask elevation.
    assert.equal(pm.classifyBashCommand('(cat foo.txt)'), 'unsafe');
    assert.equal(pm.classifyBashCommand('(ls)'), 'unsafe');
    assert.equal(pm.classifyBashCommand('(echo hi)'), 'unsafe');
  });

  it('a safe inner command inside a subshell with a git read prefix is still gated', () => {
    assert.equal(pm.classifyBashCommand('(git status)'), 'unsafe');
  });

  it('a destructive subshell keeps its STRONGER verdict (not downgraded to a mere prompt)', () => {
    assert.equal(pm.classifyBashCommand('(rm -rf /tmp/x)'), 'destructive');
  });

  it('brace EXPANSION {a,b} is NOT a command group and stays safe', () => {
    // Discriminates the command-group detector from brace expansion — a false
    // positive here would needlessly prompt on `echo {a,b}` / `cp a.{js,ts} d/`.
    assert.equal(pm.classifyBashCommand('echo {a,b}'), 'safe');
  });

  it('a plain (ungrouped) read-only command is unaffected', () => {
    assert.equal(pm.classifyBashCommand('ls -la'), 'safe');
    assert.equal(pm.classifyBashCommand('cat foo.txt'), 'safe');
  });

  it('integration: acceptEdits mode no longer auto-allows a safe-looking subshell', async () => {
    const ae = new PermissionManager('acceptEdits');
    // RED on revert: returned 'allow' (silent auto-execute) before the fix.
    assert.equal(await ae.check('bash', { command: '(cat foo.txt)' }), 'ask');
    // A plain safe command in acceptEdits is still auto-allowed (no regression).
    assert.equal(await ae.check('bash', { command: 'cat foo.txt' }), 'allow');
  });
});

// -----------------------------------------------------------------------------
// GAP: Strict ordering — extract heredoc bodies BEFORE joining line
// continuations (and before compound-splitting / arg-leak scanning).
//
// Joining `\<newline>` continuations first can shift a quoted-heredoc delimiter
// and swallow real post-heredoc statements, so the permission matcher reasons
// about the wrong token stream. AMI now strips heredoc bodies up front for both
// the rule-matcher (matchBashPattern) and the arg-leak scanner
// (bashCommandArgLeaksValue), preserving Claude's heredoc-before-join invariant
// uniformly. Observable effect: a `$SECRET`-looking token that is LITERAL text
// inside a quoted heredoc body is no longer mis-scanned as an argument value
// leak, so a legitimate `cat <<'E' … E` stays auto-approvable under an allow
// rule — while a REAL arg expansion outside the body is still caught.
// -----------------------------------------------------------------------------
describe('U-perm1/w124 — heredoc extraction before line-continuation join', () => {
  it('a $VAR inside a quoted heredoc body is NOT treated as an arg-value leak', () => {
    // RED on revert: the pre-fix join-first order scans the body token
    // `$ANTHROPIC_API_KEY` and returns true (a false positive).
    assert.equal(
      bashCommandArgLeaksValue("cat <<'E'\n$ANTHROPIC_API_KEY\nE"),
      false,
    );
  });

  it('a REAL arg expansion OUTSIDE the heredoc body is still a leak', () => {
    assert.equal(
      bashCommandArgLeaksValue("cat <<'E'\nbody\nE && echo $ANTHROPIC_API_KEY"),
      true,
    );
    assert.equal(bashCommandArgLeaksValue('echo $ANTHROPIC_API_KEY'), true);
  });

  it('integration: an allow rule auto-approves a heredoc whose body merely CONTAINS $VAR text', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'cat *', action: 'allow' },
    ]);
    // RED on revert: the body token forces a spurious 'ask' before the fix.
    assert.equal(
      await pm.check('bash', { command: "cat <<'E'\n$ANTHROPIC_API_KEY\nE" }),
      'allow',
    );
    // A genuine secret expansion in an argument still prompts (not auto-allowed).
    assert.equal(
      await pm.check('bash', { command: 'cat x && echo $ANTHROPIC_API_KEY' }),
      'ask',
    );
  });
});
