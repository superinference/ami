import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { validateBashSecurity, CHECK_IDS } from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 (wave w65) — core/core/src/tools/bash-security.ts parity hardening
//
// Three confirmed Claude-Code parity gaps against bashSecurity.ts:
//
//  Gap 2 (isSafeHeredoc, bashSecurity.ts L467-513) — before trusting a
//    `$(cat <<'EOF'…EOF)` heredoc substitution, Claude enforces three guards:
//    (1) the `$(` must be in ARGUMENT position (a command word must precede the
//        first `$(`), else the heredoc body becomes the command name and the
//        trailing text its arguments
//        (`$(cat <<'EOF'\nchmod\nEOF\n) 777 /etc/shadow` → `chmod 777 /etc/shadow`);
//    (2) the text remaining after stripping must match a strict safe charset;
//    (3) that remainder must recursively pass every validator (so a dangerous
//        command with a safe heredoc appended, e.g. `zmodload zsh/system $(…)`,
//        is still rejected). AMI previously only re-ran the command-substitution
//        check on the remainder.
//
//  Gap 3 (validateRedirections, bashSecurity.ts L875-903) — Claude emits two
//    distinct, consequence-explaining messages: input `<` "could read sensitive
//    files", output `>` "could write to arbitrary files". AMI collapsed both into
//    one terse "unquoted redirect operators detected" with no rationale.
//
//  Gap 4 (validateMidWordHash, bashSecurity.ts L1919-1962) — before-only `#`
//    match on a non-whitespace predecessor. Already implemented in AMI; locked
//    here as a regression guard.
//
// The heredoc path (Gap 2) is only reachable when validateNewlines is skipped
// (opt-in relaxed-detached mode); interactive mode blocks the multi-line command
// earlier with CHECK_IDS.NEWLINES. We therefore exercise Gap 2 through the
// relaxed-detached option, a real SWE-bench code path.
// ===========================================================================

const RELAXED = { detachedMode: true, relaxDetachedValidators: true } as const;

describe('U015/w65 Gap 2 — safe-heredoc substitution hardening', () => {
  it('blocks a heredoc substitution in command-name position with trailing args', () => {
    // The heredoc body `chmod` would become the command name, `777 /etc/shadow`
    // its arguments. No command word precedes the `$(`, so it is UNSAFE.
    const cmd = "$(cat <<'EOF'\nchmod\nEOF\n) 777 /etc/shadow";
    const r = validateBashSecurity(cmd, RELAXED);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.COMMAND_SUBSTITUTION);
    assert.match(r.message, /command-name position/);
  });

  it('allows a heredoc substitution in argument position (legitimate git commit)', () => {
    // A real command word (`git commit -m`) precedes the `$(`, the remainder is
    // safe-charset clean, and recursively validates — so it is allowed.
    const cmd = "git commit -m \"$(cat <<'EOF'\ncommit message\nEOF\n)\"";
    const r = validateBashSecurity(cmd, RELAXED);
    assert.equal(r.safe, true, r.message);
  });

  it('still rejects a dangerous command carrying an argument-position safe heredoc', () => {
    // The heredoc is in argument position (prefix `zmodload zsh/system`), so the
    // argument-position guard does not fire — but the dangerous `zmodload` must
    // still be rejected. In AMI every validator runs on the full command (no
    // early-allow short-circuit), so validateZshDangerousCommands catches it.
    const cmd = "zmodload zsh/system $(cat <<'EOF'\nx\nEOF\n)";
    const r = validateBashSecurity(cmd, RELAXED);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.ZSH_DANGEROUS_COMMANDS);
  });

  it('blocks when a shell metacharacter remains after stripping the heredoc', () => {
    // Argument position is satisfied (prefix `echo`), but the remainder carries a
    // `;` that could chain a command after the safe heredoc — fails safe-charset.
    const cmd = "echo $(cat <<'EOF'\ndata\nEOF\n) ; whoami";
    const r = validateBashSecurity(cmd, RELAXED);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.COMMAND_SUBSTITUTION);
    assert.match(r.message, /unsafe characters remain/);
  });
});

describe('U015/w65 Gap 3 — consequence-explaining redirection messages', () => {
  it('input redirection names the read-sensitive-files risk', () => {
    const r = validateBashSecurity('cat < /etc/passwd');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.REDIRECTIONS);
    assert.match(r.message, /input redirection \(<\)/);
    assert.match(r.message, /read sensitive files/);
  });

  it('output redirection names the write-arbitrary-files risk', () => {
    const r = validateBashSecurity('echo hi > /tmp/evil');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.REDIRECTIONS);
    assert.match(r.message, /output redirection \(>\)/);
    assert.match(r.message, /write to arbitrary files/);
  });

  it('append redirection also uses the output-redirection message', () => {
    const r = validateBashSecurity('echo hi >> /tmp/evil');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.REDIRECTIONS);
    assert.match(r.message, /output redirection \(>\)/);
  });
});

describe('U015/w65 Gap 4 — mid-word # before-only match (regression lock)', () => {
  it('blocks a trailing # after a word (echo foo#)', () => {
    const r = validateBashSecurity('echo foo#');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MID_WORD_HASH);
  });

  it('blocks a # immediately after a closing single quote (quote-preserving)', () => {
    const r = validateBashSecurity("echo 'x'#");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MID_WORD_HASH);
  });

  it('blocks a continuation-joined mid-word # (foo\\<newline>#bar)', () => {
    const r = validateBashSecurity('echo foo\\\n#bar');
    assert.equal(r.safe, false);
  });

  it('still allows a legitimate spaced comment and ${#var} length expansion', () => {
    assert.equal(validateBashSecurity('ls -la').safe, true);
    assert.equal(validateBashSecurity('echo "${#arr}"').safe, true);
  });
});
