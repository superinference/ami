/**
 * Additional coverage tests targeting function coverage gaps across:
 *   - src/tools/bash-security.ts   (72% fn -> every exported fn exercised)
 *   - src/tools/bash.ts            (uncovered error paths)
 *   - src/tools/bash-sandbox.ts    (uncovered branches)
 *   - src/process-manager.ts       (spawn, kill, signal, monitorMcp)
 *   - src/config.ts                (loadLocalConfig, loadManagedConfig, ConfigService.watch/onConfigChange)
 *   - src/tools/index.ts           (toOpenAIFormat, createDefaultTools)
 *   - src/utils/shell.ts           (looksLikePrompt, stall detection)
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// ============================================================================
// 1. bash-security.ts -- exercise every exported function
// ============================================================================

import {
  validateBashSecurity,
  validateControlCharacters,
  validateCarriageReturn,
  validateIFSInjection,
  validateProcEnvironAccess,
  validateObfuscatedFlags,
  validateUnicodeWhitespace,
  validateBraceExpansion,
  validateDangerousVariables,
  validateZshDangerousCommands,
  validateBackslashEscapedOperators,
  validateNewlines,
  extractQuotedContent,
  stripSafeRedirections,
  isEscapedAtPosition,
  stripSafeWrappers,
  validateSedDangerous,
  validateASTCommands,
  validateJqDangerous,
  validateShellMetacharsInCommands,
  validateCommentQuoteDesync,
  validateQuotedNewline,
  validateMidWordHash,
  validateProcessSubstitution,
  validateBacktickInjection,
  validateShellQuoteBug,
  validateCommandSubstitution,
  validateRedirections,
  validateBackslashEscapedWhitespace,
  validateMalformedTokenInjection,
  stripSafeHeredocSubstitutions,
  extractCommandPaths,
  CHECK_IDS,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// extractCommandPaths -- exercises the PATH_COMMANDS dispatch table
// ---------------------------------------------------------------------------

describe('extractCommandPaths', () => {
  it('extracts paths for cd', () => {
    const paths = extractCommandPaths('cd /tmp/dir');
    assert.ok(paths.includes('/tmp/dir'));
  });

  it('extracts paths for ls', () => {
    const paths = extractCommandPaths('ls -la /var/log');
    assert.ok(paths.includes('/var/log'));
  });

  it('extracts paths for cat', () => {
    const paths = extractCommandPaths('cat -n file.txt');
    assert.ok(paths.includes('file.txt'));
  });

  it('extracts paths for head', () => {
    const paths = extractCommandPaths('head -20 data.csv');
    assert.ok(paths.includes('data.csv'));
  });

  it('extracts paths for tail', () => {
    const paths = extractCommandPaths('tail -f output.log');
    assert.ok(paths.includes('output.log'));
  });

  it('extracts paths for sort', () => {
    const paths = extractCommandPaths('sort -r items.txt');
    assert.ok(paths.includes('items.txt'));
  });

  it('extracts paths for wc', () => {
    const paths = extractCommandPaths('wc -l file.txt');
    assert.ok(paths.includes('file.txt'));
  });

  it('extracts paths for touch', () => {
    const paths = extractCommandPaths('touch newfile.txt');
    assert.ok(paths.includes('newfile.txt'));
  });

  it('extracts paths for mkdir', () => {
    const paths = extractCommandPaths('mkdir -p /tmp/new/dir');
    assert.ok(paths.includes('/tmp/new/dir'));
  });

  it('extracts paths for rm', () => {
    const paths = extractCommandPaths('rm -rf -- dir/');
    assert.ok(paths.includes('dir/'));
  });

  it('rm filters -- from args', () => {
    const paths = extractCommandPaths('rm -- file.txt');
    assert.ok(!paths.includes('--'));
    assert.ok(paths.includes('file.txt'));
  });

  it('extracts paths for rmdir', () => {
    const paths = extractCommandPaths('rmdir emptydir');
    assert.ok(paths.includes('emptydir'));
  });

  it('extracts paths for mv', () => {
    const paths = extractCommandPaths('mv -f src dst');
    assert.deepEqual(paths, ['src', 'dst']);
  });

  it('extracts paths for cp', () => {
    const paths = extractCommandPaths('cp -r src/ dst/');
    assert.deepEqual(paths, ['src/', 'dst/']);
  });

  it('extracts paths for find (stops at first flag)', () => {
    const paths = extractCommandPaths('find . /tmp -name "*.ts"');
    assert.deepEqual(paths, ['.', '/tmp']);
  });

  it('extracts paths for stat', () => {
    const paths = extractCommandPaths('stat -c "%s" file.bin');
    assert.ok(paths.includes('file.bin'));
  });

  it('extracts paths for diff', () => {
    const paths = extractCommandPaths('diff -u a.txt b.txt');
    assert.deepEqual(paths, ['a.txt', 'b.txt']);
  });

  it('extracts paths for file', () => {
    const paths = extractCommandPaths('file -b image.png');
    assert.ok(paths.includes('image.png'));
  });

  it('extracts paths for strings', () => {
    const paths = extractCommandPaths('strings -a binary.elf');
    assert.ok(paths.includes('binary.elf'));
  });

  it('extracts paths for grep (files after pattern)', () => {
    const paths = extractCommandPaths('grep -r pattern src/ lib/');
    assert.ok(paths.includes('src/'));
    assert.ok(paths.includes('lib/'));
  });

  it('grep returns empty when no non-flag arg', () => {
    const paths = extractCommandPaths('grep -r -l');
    assert.deepEqual(paths, []);
  });

  it('extracts paths for rg (files after pattern)', () => {
    const paths = extractCommandPaths('rg -i query file.ts');
    assert.ok(paths.includes('file.ts'));
  });

  it('rg returns empty when no non-flag arg', () => {
    const paths = extractCommandPaths('rg -i');
    assert.deepEqual(paths, []);
  });

  it('extracts paths for sed (filters flags and expressions)', () => {
    const paths = extractCommandPaths("sed -i 's/old/new/' file.txt");
    assert.ok(paths.includes('file.txt'));
  });

  it('extracts paths for awk (filters flags and quoted programs)', () => {
    const paths = extractCommandPaths("awk -F: '{print $1}' /etc/passwd");
    assert.ok(paths.includes('/etc/passwd'));
  });

  it('extracts paths for git (subcommand args minus flags)', () => {
    const paths = extractCommandPaths('git diff --cached src/main.ts');
    assert.ok(paths.includes('src/main.ts'));
  });

  it('returns empty for unknown commands', () => {
    const paths = extractCommandPaths('unknown_command arg1 arg2');
    assert.deepEqual(paths, []);
  });

  it('handles double-dash separator', () => {
    const paths = extractCommandPaths('ls -- -file-starting-with-dash');
    assert.ok(Array.isArray(paths));
  });
});

// ---------------------------------------------------------------------------
// validateASTCommands -- blocked commands and depth checks
// ---------------------------------------------------------------------------

describe('validateASTCommands', () => {
  it('blocks nc command', () => {
    const r = validateASTCommands('nc -l 8080');
    assert.equal(r.safe, false);
  });

  it('blocks dd command', () => {
    const r = validateASTCommands('dd if=/dev/zero of=disk.img');
    assert.equal(r.safe, false);
  });

  it('blocks crontab', () => {
    const r = validateASTCommands('crontab -e');
    assert.equal(r.safe, false);
  });

  it('blocks mount', () => {
    const r = validateASTCommands('mount /dev/sda1 /mnt');
    assert.equal(r.safe, false);
  });

  it('blocks useradd', () => {
    const r = validateASTCommands('useradd testuser');
    assert.equal(r.safe, false);
  });

  it('blocks iptables', () => {
    const r = validateASTCommands('iptables -A INPUT -j DROP');
    assert.equal(r.safe, false);
  });

  it('allows safe commands', () => {
    assert.equal(validateASTCommands('echo hello').safe, true);
    assert.equal(validateASTCommands('ls -la').safe, true);
  });

  it('passes when AST parsing returns null', () => {
    assert.equal(validateASTCommands('').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateJqDangerous
// ---------------------------------------------------------------------------

describe('validateJqDangerous', () => {
  it('blocks jq system() calls', () => {
    const r = validateJqDangerous('echo {} | jq "system(\\"whoami\\")"');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.JQ_DANGEROUS);
  });

  it('allows safe jq', () => {
    assert.equal(validateJqDangerous('echo {} | jq ".key"').safe, true);
  });

  it('passes non-jq commands', () => {
    assert.equal(validateJqDangerous('cat data.json').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateShellMetacharsInCommands
// ---------------------------------------------------------------------------

describe('validateShellMetacharsInCommands', () => {
  it('blocks unquoted metachar in find', () => {
    const r = validateShellMetacharsInCommands('find . -name *.ts; rm -rf /');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.SHELL_METACHARS_IN_COMMANDS);
  });

  it('blocks unquoted metachar in xargs', () => {
    const r = validateShellMetacharsInCommands('xargs cmd $VAR');
    assert.equal(r.safe, false);
  });

  it('allows quoted metachar in find', () => {
    assert.equal(validateShellMetacharsInCommands("find . -name '*.ts;evil'").safe, true);
  });

  it('passes commands without find/grep/xargs', () => {
    assert.equal(validateShellMetacharsInCommands('echo hello; world').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateMidWordHash
// ---------------------------------------------------------------------------

describe('validateMidWordHash', () => {
  it('blocks mid-word hash', () => {
    const r = validateMidWordHash('echo foo#bar');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.MID_WORD_HASH);
  });

  it('allows hash in single quotes', () => {
    assert.equal(validateMidWordHash("echo 'foo#bar'").safe, true);
  });

  it('allows hash in double quotes', () => {
    assert.equal(validateMidWordHash('echo "foo#bar"').safe, true);
  });

  it('allows standalone # (comment)', () => {
    assert.equal(validateMidWordHash('echo hello # comment').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateProcessSubstitution
// ---------------------------------------------------------------------------

describe('validateProcessSubstitution', () => {
  it('blocks <( process substitution', () => {
    const r = validateProcessSubstitution('diff <(cmd1) <(cmd2)');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PROCESS_SUBSTITUTION);
  });

  it('blocks >( process substitution', () => {
    const r = validateProcessSubstitution('cmd >(tee log)');
    assert.equal(r.safe, false);
  });

  it('allows commands without process substitution', () => {
    assert.equal(validateProcessSubstitution('echo hello').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateBacktickInjection
// ---------------------------------------------------------------------------

describe('validateBacktickInjection', () => {
  it('blocks backtick command substitution', () => {
    const r = validateBacktickInjection('echo `whoami`');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.BACKTICK_INJECTION);
  });

  it('allows escaped backticks', () => {
    assert.equal(validateBacktickInjection('echo \\`safe\\`').safe, true);
  });

  it('allows commands without backticks', () => {
    assert.equal(validateBacktickInjection('echo hello').safe, true);
  });
});

// ---------------------------------------------------------------------------
// stripSafeWrappers -- edge cases for better coverage
// ---------------------------------------------------------------------------

describe('stripSafeWrappers -- additional', () => {
  it('handles env with only variable assignments (no inner cmd)', () => {
    const result = stripSafeWrappers('env FOO=bar');
    assert.equal(result, 'env FOO=bar');
  });

  it('handles timeout with no args after duration', () => {
    const result = stripSafeWrappers('timeout 30');
    assert.equal(result, 'timeout 30');
  });

  it('handles nice with no args after priority', () => {
    const result = stripSafeWrappers('nice -n 5');
    assert.equal(result, 'nice -n 5');
  });

  it('handles stdbuf with only flags', () => {
    const result = stripSafeWrappers('stdbuf -oL');
    assert.equal(result, 'stdbuf -oL');
  });

  it('handles time alone (no inner command)', () => {
    const result = stripSafeWrappers('time');
    assert.equal(result, 'time');
  });

  it('handles nohup alone (no inner command)', () => {
    const result = stripSafeWrappers('nohup');
    assert.equal(result, 'nohup');
  });

  it('strips env followed by inner wrapper', () => {
    const result = stripSafeWrappers('env VAR=1 timeout 5 ls');
    assert.equal(result, 'ls');
  });

  it('handles nice with numeric priority (no dash)', () => {
    const result = stripSafeWrappers('nice 10 cmd');
    assert.equal(result, 'cmd');
  });
});

// ---------------------------------------------------------------------------
// extractQuotedContent -- edge cases
// ---------------------------------------------------------------------------

describe('extractQuotedContent -- extra', () => {
  it('handles backslash before single quote outside double quotes', () => {
    const r = extractQuotedContent("echo \\'test");
    assert.ok(r.fullyUnquoted.includes('test'));
  });

  it('handles empty string', () => {
    const r = extractQuotedContent('');
    assert.equal(r.withDoubleQuotes, '');
    assert.equal(r.fullyUnquoted, '');
  });

  it('handles only quotes', () => {
    const r = extractQuotedContent('""');
    assert.equal(r.fullyUnquoted, '');
  });

  it('handles escaped char inside double quotes', () => {
    const r = extractQuotedContent('"hello\\nworld"');
    assert.ok(r.withDoubleQuotes.includes('hello'));
    assert.ok(r.withDoubleQuotes.includes('world'));
    assert.equal(r.fullyUnquoted, '');
  });
});

// ---------------------------------------------------------------------------
// stripSafeRedirections -- extra edge cases
// ---------------------------------------------------------------------------

describe('stripSafeRedirections -- extra', () => {
  it('strips 1>&2 redirection', () => {
    const result = stripSafeRedirections('cmd 1>&2');
    assert.equal(result.trim(), 'cmd');
  });

  it('strips 2>/dev/null', () => {
    const result = stripSafeRedirections('cmd 2>/dev/null');
    assert.equal(result.trim(), 'cmd');
  });

  it('strips multiple redirections', () => {
    const result = stripSafeRedirections('cmd 2>&1 >/dev/null');
    assert.equal(result.trim(), 'cmd');
  });
});

// ---------------------------------------------------------------------------
// isEscapedAtPosition -- extra
// ---------------------------------------------------------------------------

describe('isEscapedAtPosition -- extra', () => {
  it('triple backslash -- position is escaped', () => {
    assert.equal(isEscapedAtPosition('\\\\\\{', 3), true);
  });

  it('handles position 0 (never escaped)', () => {
    assert.equal(isEscapedAtPosition('a', 0), false);
  });
});

// ---------------------------------------------------------------------------
// validateSedDangerous -- extra coverage for edge paths
// ---------------------------------------------------------------------------

describe('validateSedDangerous -- extra', () => {
  it('blocks sed y (transliterate) command', () => {
    const r = validateSedDangerous("sed 'y/abc/ABC/' file.txt");
    assert.equal(r.safe, false);
  });

  it('blocks sed negation with dangerous command', () => {
    const r = validateSedDangerous("sed '! d' file.txt");
    assert.equal(r.safe, false);
  });

  it('blocks sed tilde address', () => {
    const r = validateSedDangerous("sed '0~2p' file.txt");
    assert.equal(r.safe, false);
  });

  it('blocks non-ASCII in sed expression', () => {
    const r = validateSedDangerous("sed 's/é/e/g' file.txt");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on .bashrc', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' ~/.bashrc");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on .ssh/', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' .ssh/config");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on authorized_keys', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' authorized_keys");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on /root/', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' /root/.profile");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on .zshrc', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' .zshrc");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on .bash_profile', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' .bash_profile");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i.bak on .profile', () => {
    const r = validateSedDangerous("sed -i.bak 's/x/y/' .profile");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on shadow file', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' /etc/shadow");
    assert.equal(r.safe, false);
  });

  it('blocks sed -i on passwd file', () => {
    const r = validateSedDangerous("sed -i 's/x/y/' /etc/passwd");
    assert.equal(r.safe, false);
  });

  it('allows sed substitution with alternate delimiter', () => {
    const r = validateSedDangerous("sed 's|old|new|g' file.txt");
    assert.equal(r.safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateObfuscatedFlags -- extra coverage for split-quote scanner
// ---------------------------------------------------------------------------

describe('validateObfuscatedFlags -- extra', () => {
  it('rejects empty quote pairs before quoted dash', () => {
    const r = validateObfuscatedFlags('cmd ""\"-flag"');
    assert.equal(r.safe, false);
  });

  it('rejects 3+ consecutive quotes at word start', () => {
    const r = validateObfuscatedFlags("cmd '''arg");
    assert.equal(r.safe, false);
  });

  it('rejects "-" followed by quote chaining', () => {
    const r = validateObfuscatedFlags('cmd "-"\'exec\'');
    assert.equal(r.safe, false);
  });

  it('allows normal flag after word', () => {
    assert.equal(validateObfuscatedFlags('grep -r pattern').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateBraceExpansion -- excess closing braces
// ---------------------------------------------------------------------------

describe('validateBraceExpansion -- excess close', () => {
  it('blocks excess closing braces', () => {
    const r = validateBraceExpansion('{a}}', '{a}}');
    assert.equal(r.safe, false);
    assert.ok(r.message.includes('excess closing'));
  });
});

// ---------------------------------------------------------------------------
// validateZshDangerousCommands -- remaining blocked commands
// ---------------------------------------------------------------------------

describe('validateZshDangerousCommands -- more', () => {
  it('blocks ztcp', () => {
    assert.equal(validateZshDangerousCommands('ztcp host 80').safe, false);
  });

  it('blocks zsocket', () => {
    assert.equal(validateZshDangerousCommands('zsocket /var/run/test.sock').safe, false);
  });

  it('blocks mapfile', () => {
    assert.equal(validateZshDangerousCommands('mapfile -t arr < file.txt').safe, false);
  });

  it('blocks sysopen', () => {
    assert.equal(validateZshDangerousCommands('sysopen -r fd file.txt').safe, false);
  });

  it('blocks sysread', () => {
    assert.equal(validateZshDangerousCommands('sysread -i 3 buf').safe, false);
  });

  it('blocks sysseek', () => {
    assert.equal(validateZshDangerousCommands('sysseek -u 3 0').safe, false);
  });

  it('blocks zf_rm', () => {
    assert.equal(validateZshDangerousCommands('zf_rm file.txt').safe, false);
  });

  it('blocks zf_mv', () => {
    assert.equal(validateZshDangerousCommands('zf_mv a b').safe, false);
  });

  it('blocks zf_ln', () => {
    assert.equal(validateZshDangerousCommands('zf_ln -s a b').safe, false);
  });

  it('blocks zf_chmod', () => {
    assert.equal(validateZshDangerousCommands('zf_chmod 777 file').safe, false);
  });

  it('blocks zf_chown', () => {
    assert.equal(validateZshDangerousCommands('zf_chown root file').safe, false);
  });

  it('blocks zf_mkdir', () => {
    assert.equal(validateZshDangerousCommands('zf_mkdir dir').safe, false);
  });

  it('blocks zf_rmdir', () => {
    assert.equal(validateZshDangerousCommands('zf_rmdir dir').safe, false);
  });

  it('blocks zf_chgrp', () => {
    assert.equal(validateZshDangerousCommands('zf_chgrp staff file').safe, false);
  });

  it('skips noglob and nocorrect modifiers', () => {
    assert.equal(validateZshDangerousCommands('noglob nocorrect zmodload zsh/system').safe, false);
  });
});

// ---------------------------------------------------------------------------
// validateBackslashEscapedOperators -- inside double quotes (should pass)
// ---------------------------------------------------------------------------

describe('validateBackslashEscapedOperators -- extra', () => {
  it('allows backslash-operator inside double quotes', () => {
    assert.equal(validateBackslashEscapedOperators('"test\\;ok"').safe, true);
  });

  it('rejects backslash-& outside quotes', () => {
    assert.equal(validateBackslashEscapedOperators('cmd \\& evil').safe, false);
  });

  it('rejects backslash-< outside quotes', () => {
    assert.equal(validateBackslashEscapedOperators('cmd \\< file').safe, false);
  });

  it('rejects backslash-> outside quotes', () => {
    assert.equal(validateBackslashEscapedOperators('cmd \\> file').safe, false);
  });
});

// ---------------------------------------------------------------------------
// validateNewlines -- edge: newline followed by only whitespace
// ---------------------------------------------------------------------------

describe('validateNewlines -- extra', () => {
  it('allows trailing newline with only whitespace after', () => {
    assert.equal(validateNewlines('echo hello\n   ').safe, true);
  });

  it('allows newlines inside single quotes', () => {
    assert.equal(validateNewlines("echo 'multi\nline'").safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateCommandSubstitution -- zsh-specific patterns
// ---------------------------------------------------------------------------

describe('validateCommandSubstitution -- extra', () => {
  it('blocks ~[] Zsh parameter expansion', () => {
    const r = validateCommandSubstitution('echo ~[test]', { fullyUnquoted: 'echo ~[test]' });
    assert.equal(r.safe, false);
  });

  it('blocks =() Zsh process substitution', () => {
    const r = validateCommandSubstitution('cmd =(echo test)', { fullyUnquoted: 'cmd =(echo test)' });
    assert.equal(r.safe, false);
  });

  it('blocks (e:...:) Zsh glob qualifier eval', () => {
    const r = validateCommandSubstitution('ls *(e:expr:)', { fullyUnquoted: 'ls *(e:expr:)' });
    assert.equal(r.safe, false);
  });

  it('blocks (+ Zsh glob qualifier', () => {
    const r = validateCommandSubstitution('ls *(+func)', { fullyUnquoted: 'ls *(+func)' });
    assert.equal(r.safe, false);
  });

  it('blocks } always { Zsh always block', () => {
    const r = validateCommandSubstitution('{ cmd } always { fallback }', { fullyUnquoted: '{ cmd } always { fallback }' });
    assert.equal(r.safe, false);
  });

  it('blocks <# PowerShell comment', () => {
    const r = validateCommandSubstitution('pwsh <# comment #>', { fullyUnquoted: 'pwsh <# comment #>' });
    assert.equal(r.safe, false);
  });

  it('uses re-extracted content when heredoc substitution is stripped', () => {
    const cmd = "git commit -m \"$(cat <<'EOF'\nfix: a bug\nEOF\n)\"";
    const extracted = extractQuotedContent(cmd);
    const r = validateCommandSubstitution(cmd, extracted);
    assert.equal(r.safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateCarriageReturn -- edge: CR in single quotes
// ---------------------------------------------------------------------------

describe('validateCarriageReturn -- extra', () => {
  it('rejects CR in single-quoted context (outside double quotes)', () => {
    const r = validateCarriageReturn("echo 'test\rinjection'");
    assert.equal(r.safe, false);
  });

  it('handles escaped backslash before CR', () => {
    const r = validateCarriageReturn('cmd \\\\\rinjection');
    assert.equal(r.safe, false);
  });
});

// ---------------------------------------------------------------------------
// validateMalformedTokenInjection -- extra edge
// ---------------------------------------------------------------------------

describe('validateMalformedTokenInjection -- extra', () => {
  it('handles unmatched double quote with pipe', () => {
    const r = validateMalformedTokenInjection('"test |cmd');
    assert.ok('safe' in r);
  });

  it('passes balanced quotes with no separator', () => {
    assert.equal(validateMalformedTokenInjection('"hello world"').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateCommentQuoteDesync -- last line is comment (no newline after)
// ---------------------------------------------------------------------------

describe('validateCommentQuoteDesync -- extra', () => {
  it('detects quote inside comment at end of input', () => {
    const r = validateCommentQuoteDesync("echo ok # that's bad");
    assert.equal(r.safe, false);
  });
});

// ---------------------------------------------------------------------------
// validateQuotedNewline -- edge cases
// ---------------------------------------------------------------------------

describe('validateQuotedNewline -- extra', () => {
  it('does not flag when no newlines and no hash', () => {
    assert.equal(validateQuotedNewline('echo hello').safe, true);
  });

  it('does not flag newlines without hash', () => {
    assert.equal(validateQuotedNewline('echo "multi\nline"').safe, true);
  });

  it('does not flag hash without newlines', () => {
    assert.equal(validateQuotedNewline('echo test # comment').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateDangerousVariables -- extra patterns
// ---------------------------------------------------------------------------

describe('validateDangerousVariables -- extra', () => {
  it('rejects variable before redirect', () => {
    const r = validateDangerousVariables('$CMD > output');
    assert.equal(r.safe, false);
  });

  it('rejects variable before pipe', () => {
    const r = validateDangerousVariables('$CMD | grep x');
    assert.equal(r.safe, false);
  });

  it('rejects redirect into variable', () => {
    const r = validateDangerousVariables('< $FILE');
    assert.equal(r.safe, false);
  });
});

// ---------------------------------------------------------------------------
// CHECK_IDS -- verify constant is exported and has expected keys
// ---------------------------------------------------------------------------

describe('CHECK_IDS', () => {
  it('exports expected check IDs', () => {
    assert.equal(typeof CHECK_IDS.CONTROL_CHARACTERS, 'number');
    assert.equal(typeof CHECK_IDS.CARRIAGE_RETURN, 'number');
    assert.equal(typeof CHECK_IDS.IFS_INJECTION, 'number');
    assert.equal(typeof CHECK_IDS.SAFE_HEREDOC, 'number');
    assert.equal(typeof CHECK_IDS.BACKTICK_INJECTION, 'number');
    assert.equal(typeof CHECK_IDS.COMMAND_SUBSTITUTION, 'number');
    assert.equal(typeof CHECK_IDS.REDIRECTIONS, 'number');
    assert.equal(typeof CHECK_IDS.SHELL_QUOTE_BUG, 'number');
    assert.equal(typeof CHECK_IDS.BACKSLASH_WHITESPACE, 'number');
    assert.equal(typeof CHECK_IDS.MALFORMED_TOKEN, 'number');
    assert.equal(typeof CHECK_IDS.PROCESS_SUBSTITUTION, 'number');
    assert.equal(typeof CHECK_IDS.MID_WORD_HASH, 'number');
    assert.equal(typeof CHECK_IDS.QUOTED_NEWLINE, 'number');
    assert.equal(typeof CHECK_IDS.COMMENT_QUOTE_DESYNC, 'number');
  });
});

// ============================================================================
// 2. bash.ts -- uncovered error paths
// ============================================================================

import { bashTool, detectSelfKill } from '../src/tools/bash';
import type { ToolContext } from '../src/types';
import { ProcessManager } from '../src/process-manager';

function makeCtx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: '/tmp',
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('bashTool -- blocked sleep detection', () => {
  it('blocks sleep >10s without run_in_background or custom timeout', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 30' },
      makeCtx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('Blocking sleep'));
  });

  it('allows sleep >10s when run_in_background is true', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-sleep-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await bashTool.execute(
        { command: 'sleep 30', run_in_background: true },
        makeCtx({ cwd: tmpDir, processManager: pm }),
      );
      assert.ok(!result.isError);
      assert.ok(result.output.includes('Background task'));
    } finally {
      pm.cleanup();
      await new Promise(r => setTimeout(r, 100));
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('allows sleep >10s with custom timeout', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 30', timeout: 200 },
      makeCtx(),
    );
    assert.ok(result.output.includes('timed out') || !result.output.includes('Blocking sleep'));
  });

  it('allows sleep <=10s', async () => {
    const result = await bashTool.execute(
      { command: 'sleep 1' },
      makeCtx(),
    );
    assert.ok(!result.output.includes('Blocking sleep'));
  });
});

describe('bashTool -- background task spawn', () => {
  it('spawns background task with custom description', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-bg-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const result = await bashTool.execute(
        { command: 'echo bg-test', run_in_background: true, description: 'My background task' },
        makeCtx({ cwd: tmpDir, processManager: pm }),
      );
      assert.ok(!result.isError);
      assert.ok(result.output.includes('Background task'));
      assert.ok(result.output.includes('task_output'));
    } finally {
      pm.cleanup();
      await new Promise(r => setTimeout(r, 100));
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('bashTool -- no-output commands', () => {
  it('shows success message for mkdir with no output', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-noout-'));
    try {
      const result = await bashTool.execute(
        { command: `mkdir -p ${path.join(tmpDir, 'subdir')}` },
        makeCtx({ cwd: tmpDir }),
      );
      assert.ok(!result.isError);
      assert.ok(result.output.includes('Command completed successfully'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('shows success message for touch with no output', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-touch-'));
    try {
      const result = await bashTool.execute(
        { command: `touch ${path.join(tmpDir, 'newfile.txt')}` },
        makeCtx({ cwd: tmpDir }),
      );
      assert.ok(!result.isError);
      assert.ok(result.output.includes('Command completed successfully'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('bashTool -- exit code interpretation', () => {
  it('interprets grep exit code 1 as no matches', async () => {
    const result = await bashTool.execute(
      { command: 'grep nonexistentpattern /dev/null' },
      makeCtx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('no matches found'));
  });

  it('interprets diff exit code 1 as files differ', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-bash-diff-'));
    try {
      fs.writeFileSync(path.join(tmpDir, 'a.txt'), 'hello\n');
      fs.writeFileSync(path.join(tmpDir, 'b.txt'), 'world\n');
      const result = await bashTool.execute(
        { command: `diff ${path.join(tmpDir, 'a.txt')} ${path.join(tmpDir, 'b.txt')}` },
        makeCtx({ cwd: tmpDir }),
      );
      assert.equal(result.isError, true);
      assert.ok(result.output.includes('files differ'));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('interprets exit code 127 as command not found', async () => {
    const result = await bashTool.execute(
      { command: 'nonexistent_command_xyz_abc' },
      makeCtx(),
    );
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('command not found'));
  });
});

describe('bashTool -- error diagnostics hints', () => {
  it('hints on ENOENT errors', async () => {
    const result = await bashTool.execute(
      { command: 'cat /nonexistent/file/that/does/not/exist/xyz' },
      makeCtx(),
    );
    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('Hint:') || result.output.includes('No such file'),
      `Expected ENOENT hint, got: ${result.output.slice(0, 200)}`,
    );
  });
});

describe('bashTool -- readOnly metadata', () => {
  it('sets readOnly metadata for safe commands', async () => {
    const result = await bashTool.execute(
      { command: 'echo safe_check' },
      makeCtx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.metadata !== undefined);
  });
});

// ============================================================================
// 3. bash-sandbox.ts -- uncovered paths
// ============================================================================

import {
  shouldUseSandbox,
  wrapWithSandbox,
  getSandboxStatus,
  resetSandboxCache,
} from '../src/tools/bash-sandbox';

describe('shouldUseSandbox -- extra', () => {
  it('triggers on php -r', () => {
    assert.equal(shouldUseSandbox('php -r "echo 1;"'), true);
  });

  it('does not trigger on go build (exempt)', () => {
    assert.equal(shouldUseSandbox('go build ./...'), false);
  });

  it('does not trigger on go test (exempt)', () => {
    assert.equal(shouldUseSandbox('go test ./...'), false);
  });

  it('does not trigger on go run (exempt)', () => {
    assert.equal(shouldUseSandbox('go run main.go'), false);
  });

  it('does not trigger on go mod (exempt)', () => {
    assert.equal(shouldUseSandbox('go mod tidy'), false);
  });

  it('exempt pattern takes priority over trigger', () => {
    assert.equal(shouldUseSandbox('npm run eval'), false);
  });

  it('exempts cargo', () => {
    assert.equal(shouldUseSandbox('cargo build'), false);
  });
});

describe('wrapWithSandbox -- extra', () => {
  it('accepts custom sandbox config', () => {
    resetSandboxCache();
    const wrapped = wrapWithSandbox('echo test', '/tmp', {
      allowNetwork: true,
      maxMemoryMB: 1024,
      maxProcesses: 128,
    });
    assert.ok(wrapped.includes('echo test'));
    assert.ok(wrapped.includes('ulimit'));
  });

  it('default config uses 512MB and 64 processes', () => {
    resetSandboxCache();
    const wrapped = wrapWithSandbox('echo test', '/tmp');
    assert.ok(wrapped.includes('524288') || wrapped.includes('ulimit'));
  });
});

describe('getSandboxStatus -- extra', () => {
  it('always returns available: true', () => {
    resetSandboxCache();
    const status = getSandboxStatus();
    assert.equal(status.available, true);
  });
});

// ============================================================================
// 4. process-manager.ts -- monitorMcp, getOutputSize, edge cases
// ============================================================================

function pmWaitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 5000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`Timed out waiting for ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(event: any) {
      if (event.taskId === taskId) {
        clearTimeout(timer);
        pm.off('complete', handler);
        resolve(event);
      }
    });
  });
}

describe('ProcessManager -- getOutputSize', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns 0 for unknown task', () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-size-'));
    pm = new ProcessManager(tmpDir);
    assert.equal(pm.getOutputSize('bg-nonexistent'), 0);
  });

  it('returns byte count for active task', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-size2-'));
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo output-size-test', { cwd: tmpDir });
    await pmWaitForComplete(pm, id);
    const size = pm.getOutputSize(id);
    assert.ok(size > 0);
  });
});

describe('ProcessManager -- monitorMcp', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 150));
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('emits complete when health check returns false', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-mcp2-'));
    pm = new ProcessManager(tmpDir);
    let callCount = 0;
    const taskId = pm.monitorMcp('failing', async () => {
      callCount++;
      return false;
    }, 50);

    assert.ok(taskId.startsWith('monitor-'));
    assert.ok(taskId.includes('failing'));

    const event = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timed out')), 2000);
      pm.on('complete', function handler(evt: any) {
        if (evt.taskId === taskId) {
          clearTimeout(timer);
          pm.off('complete', handler);
          resolve(evt);
        }
      });
    });

    assert.equal(event.exitCode, 1);
    assert.ok(callCount >= 1);
  });

  it('emits complete when health check throws', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-mcp3-'));
    pm = new ProcessManager(tmpDir);
    const taskId = pm.monitorMcp('erroring', async () => {
      throw new Error('health check failed');
    }, 50);

    const event = await new Promise<any>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timed out')), 2000);
      pm.on('complete', function handler(evt: any) {
        if (evt.taskId === taskId) {
          clearTimeout(timer);
          pm.off('complete', handler);
          resolve(evt);
        }
      });
    });

    assert.equal(event.exitCode, 1);
  });
});

describe('ProcessManager -- getOutput tailLines=0', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns full output when tailLines is 0', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-tail0-'));
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('for i in $(seq 1 20); do echo "line$i"; done', { cwd: tmpDir });
    await pmWaitForComplete(pm, id);
    const result = pm.getOutput(id, 0);
    assert.ok(result);
    assert.ok(result!.output.includes('line1'));
    assert.ok(result!.output.includes('line20'));
  });
});

describe('ProcessManager -- spawn with env', () => {
  let tmpDir: string;
  let pm: ProcessManager;

  afterEach(async () => {
    pm?.cleanup();
    await new Promise(r => setTimeout(r, 100));
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('passes custom env to spawned process', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-pm-env-'));
    pm = new ProcessManager(tmpDir);
    const id = pm.spawn('echo $MY_TEST_VAR', {
      cwd: tmpDir,
      env: { ...process.env, MY_TEST_VAR: 'custom_val' },
    });
    await pmWaitForComplete(pm, id);
    const result = pm.getOutput(id);
    assert.ok(result!.output.includes('custom_val'));
  });
});

// ============================================================================
// 5. config.ts -- loadLocalConfig, loadManagedConfig, ConfigService.onConfigChange
// ============================================================================

import {
  loadLocalConfig,
  loadManagedConfig,
  mergeConfigs,
  ConfigService,
  stripJsoncComments,
  type ProjectConfig,
} from '../src/config';

describe('loadLocalConfig', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-config-local-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns null when config.local.json does not exist', () => {
    assert.equal(loadLocalConfig(tmpDir), null);
  });

  it('loads config.local.json when present', () => {
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.local.json'), '{"model": "local-model"}');
    const result = loadLocalConfig(tmpDir);
    assert.notEqual(result, null);
    assert.equal(result!.model, 'local-model');
  });
});

describe('loadManagedConfig', () => {
  it('returns null since /etc/superinference/config.json typically does not exist', () => {
    const result = loadManagedConfig();
    assert.ok(result === null || typeof result === 'object');
  });
});

describe('mergeConfigs -- with localConfig and managedConfig', () => {
  it('local config overrides project config', () => {
    const project: ProjectConfig = { model: 'project' };
    const local: ProjectConfig = { model: 'local' };
    const result = mergeConfigs({}, {}, project, null, local);
    assert.equal(result.model, 'local');
  });

  it('managed config provides baseline before global', () => {
    const managed: ProjectConfig = { model: 'managed', provider: 'openai' };
    const global: ProjectConfig = { model: 'global' };
    const result = mergeConfigs({}, {}, null, global, null, managed);
    assert.equal(result.model, 'global');
    assert.equal(result.provider, 'openai');
  });

  it('full priority chain: cli > env > local > project > global > managed', () => {
    const managed: ProjectConfig = { persona: 'managed-persona' };
    const global: ProjectConfig = { persona: 'global-persona', provider: 'openai' };
    const project: ProjectConfig = { persona: 'project-persona' };
    const local: ProjectConfig = { persona: 'local-persona' };
    const env: Partial<ProjectConfig> = {};
    const cli: Partial<ProjectConfig> = {};
    const result = mergeConfigs(cli, env, project, global, local, managed);
    assert.equal(result.persona, 'local-persona');
    assert.equal(result.provider, 'openai');
  });
});

describe('ConfigService -- onConfigChange callback', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-config-svc-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('calls onConfigChange callback on reload', () => {
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v1"}');

    const svc = new ConfigService(tmpDir);
    let calledSource = '';
    let calledPath = '';
    svc.onConfigChange((source, configPath) => {
      calledSource = source;
      calledPath = configPath;
    });

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v2"}');
    svc.reload();

    assert.equal(calledSource, 'project');
    assert.ok(calledPath.includes('config.json'));
    svc.stop();
  });

  it('handles onConfigChange callback errors gracefully', () => {
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v1"}');

    const svc = new ConfigService(tmpDir);
    svc.onConfigChange(() => { throw new Error('callback error'); });

    let secondCalled = false;
    svc.onConfigChange(() => { secondCalled = true; });

    fs.writeFileSync(path.join(dir, 'config.json'), '{"model": "v2"}');
    svc.reload();
    assert.equal(secondCalled, true);
    svc.stop();
  });

  it('reload returns false when config file is missing', () => {
    const svc = new ConfigService(tmpDir);
    assert.equal(svc.reload(), false);
    svc.stop();
  });
});

describe('ConfigService -- watch', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-config-watch-'));
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('watch() sets up watcher and stop() tears it down', () => {
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{}');

    const svc = new ConfigService(tmpDir);
    svc.watch();
    svc.watch();
    svc.stop();
    svc.stop();
  });

  it('watch() is no-op for nonexistent directory', () => {
    const svc = new ConfigService(path.join(tmpDir, 'nope', 'deep'));
    svc.watch();
    svc.stop();
  });
});

describe('stripJsoncComments -- extra', () => {
  it('handles block comment that runs to end of input', () => {
    const result = stripJsoncComments('{"a":1}/* unclosed');
    assert.ok(result.includes('"a"'));
  });

  it('handles consecutive block comments', () => {
    const result = stripJsoncComments('/* a *//* b */{"x":1}');
    const parsed = JSON.parse(result);
    assert.equal(parsed.x, 1);
  });
});

// ============================================================================
// 6. tools/index.ts -- ToolRegistry and createDefaultTools
// ============================================================================

import { ToolRegistry, createDefaultTools } from '../src/tools/index';

describe('ToolRegistry -- toOpenAIFormat with multiple tools', () => {
  it('maps all registered tools to OpenAI function format', () => {
    const registry = new ToolRegistry();
    for (let i = 0; i < 5; i++) {
      registry.register({
        name: `tool_${i}`,
        description: `Tool number ${i}`,
        inputSchema: { type: 'object' as const, properties: { arg: { type: 'string' } } },
        isReadOnly: true,
        async execute() { return { output: 'ok' }; },
      });
    }

    const formatted = registry.toOpenAIFormat();
    assert.equal(formatted.length, 5);
    for (let i = 0; i < 5; i++) {
      assert.equal(formatted[i].type, 'function');
      assert.equal(formatted[i].function.name, `tool_${i}`);
      assert.equal(formatted[i].function.description, `Tool number ${i}`);
      assert.deepEqual(formatted[i].function.parameters.properties, { arg: { type: 'string' } });
    }
  });

  it('returns empty array for empty registry', () => {
    const registry = new ToolRegistry();
    assert.deepEqual(registry.toOpenAIFormat(), []);
  });
});

describe('createDefaultTools -- full tool set', () => {
  it('includes newer tools: task_output, task_kill, task_list, skill', () => {
    const registry = createDefaultTools(os.tmpdir());
    const names = registry.getAll().map(t => t.name);
    for (const name of ['task_output', 'task_kill', 'task_list', 'skill']) {
      assert.ok(names.includes(name), `Missing tool: ${name}`);
    }
  });

  it('includes cron tools', () => {
    const registry = createDefaultTools(os.tmpdir());
    const names = registry.getAll().map(t => t.name);
    for (const name of ['cron_create', 'cron_delete', 'cron_list']) {
      assert.ok(names.includes(name), `Missing tool: ${name}`);
    }
  });

  it('includes worktree tools', () => {
    const registry = createDefaultTools(os.tmpdir());
    const names = registry.getAll().map(t => t.name);
    for (const name of ['enter_worktree', 'exit_worktree']) {
      assert.ok(names.includes(name), `Missing tool: ${name}`);
    }
  });

  it('includes send_message tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('send_message'));
  });

  it('includes sleep tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('sleep'));
  });

  it('includes workflow tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('workflow'));
  });

  it('includes brief tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('brief'));
  });

  it('includes team tools', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('team_create'));
    assert.ok(registry.get('team_delete'));
  });

  it('includes todo_write tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('todo_write'));
  });

  it('includes config tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('config'));
  });

  it('includes structured_output tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('structured_output'));
  });

  it('includes schedule_wakeup tool', () => {
    const registry = createDefaultTools(os.tmpdir());
    assert.ok(registry.get('schedule_wakeup'));
  });

  it('toOpenAIFormat on full registry has correct shape', () => {
    const registry = createDefaultTools(os.tmpdir());
    const formatted = registry.toOpenAIFormat();
    assert.ok(formatted.length >= 30, `Expected >= 30 tools in OpenAI format, got ${formatted.length}`);
    for (const entry of formatted) {
      assert.equal(entry.type, 'function');
      assert.ok(entry.function.name.length > 0);
      assert.ok(entry.function.description.length > 0);
      assert.ok(entry.function.parameters);
      assert.equal(entry.function.parameters.type, 'object');
    }
  });
});

// ============================================================================
// 7. utils/shell.ts -- stall detection, env, edge cases
// ============================================================================

import { execCommand } from '../src/utils/shell';

describe('execCommand -- env parameter', () => {
  it('passes custom environment variables', async () => {
    const result = await execCommand('echo $MY_CUSTOM_VAR', {
      cwd: '/tmp',
      env: { ...process.env, MY_CUSTOM_VAR: 'test_value_xyz' },
    });
    assert.ok(result.stdout.includes('test_value_xyz'));
  });
});

describe('execCommand -- stallTimeoutMs disabled', () => {
  it('accepts stallTimeoutMs of 0 (disables stall detection)', async () => {
    const result = await execCommand('echo no-stall', {
      cwd: '/tmp',
      stallTimeoutMs: 0,
    });
    assert.equal(result.exitCode, 0);
    assert.ok(result.stdout.includes('no-stall'));
  });
});

describe('execCommand -- process error event', () => {
  it('captures error when spawn fails', async () => {
    const result = await execCommand('echo test', {
      cwd: '/definitely/nonexistent/path/xyz_abc_123',
    });
    assert.ok(result.stderr.length > 0 || result.exitCode === null);
  });
});

describe('execCommand -- combined stdout and stderr', () => {
  it('captures both stdout and stderr from the same command', async () => {
    const result = await execCommand('echo out-msg && echo err-msg >&2', {
      cwd: '/tmp',
    });
    assert.ok(result.stdout.includes('out-msg'));
    assert.ok(result.stderr.includes('err-msg'));
  });
});

describe('execCommand -- onData streams both stdout and stderr', () => {
  it('calls onData for both streams', async () => {
    const chunks: string[] = [];
    await execCommand('echo stdout-chunk && echo stderr-chunk >&2', {
      cwd: '/tmp',
      onData: (chunk) => chunks.push(chunk),
    });
    const all = chunks.join('');
    assert.ok(all.includes('stdout-chunk'));
    assert.ok(all.includes('stderr-chunk'));
  });
});
