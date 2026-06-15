import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
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
  CHECK_IDS,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// extractQuotedContent
// ---------------------------------------------------------------------------

describe('extractQuotedContent', () => {
  it('strips single-quoted content', () => {
    const r = extractQuotedContent("echo 'hello world'");
    assert.equal(r.fullyUnquoted, 'echo ');
  });

  it('strips double-quoted content', () => {
    const r = extractQuotedContent('echo "hello world"');
    assert.equal(r.fullyUnquoted, 'echo ');
    assert.equal(r.withDoubleQuotes, 'echo hello world');
  });

  it('handles escaped quotes', () => {
    const r = extractQuotedContent('echo \\"safe\\"');
    // Backslash-escaped quotes: backslash + char both pass through to fullyUnquoted
    assert.ok(r.fullyUnquoted.includes('\\'));
    assert.ok(r.fullyUnquoted.includes('safe'));
  });

  it('handles mixed quotes', () => {
    const r = extractQuotedContent(`echo 'a' "b" c`);
    // Spaces between quotes are preserved in unquoted output
    assert.ok(r.fullyUnquoted.includes('echo'));
    assert.ok(r.fullyUnquoted.includes('c'));
    assert.ok(!r.fullyUnquoted.includes('a'));
    assert.ok(!r.fullyUnquoted.includes('b'));
  });
});

// ---------------------------------------------------------------------------
// stripSafeRedirections
// ---------------------------------------------------------------------------

describe('stripSafeRedirections', () => {
  it('strips 2>&1', () => {
    assert.equal(stripSafeRedirections('cmd 2>&1'), 'cmd');
  });

  it('strips >/dev/null', () => {
    assert.equal(stripSafeRedirections('cmd >/dev/null'), 'cmd');
  });

  it('strips </dev/null', () => {
    assert.equal(stripSafeRedirections('cmd </dev/null'), 'cmd');
  });

  it('does not strip >/dev/nullo (boundary)', () => {
    const r = stripSafeRedirections('cmd >/dev/nullo');
    assert.ok(r.includes('/dev/nullo'));
  });
});

// ---------------------------------------------------------------------------
// isEscapedAtPosition
// ---------------------------------------------------------------------------

describe('isEscapedAtPosition', () => {
  it('detects single backslash escape', () => {
    assert.equal(isEscapedAtPosition('\\{', 1), true);
  });

  it('double backslash is not escaped', () => {
    assert.equal(isEscapedAtPosition('\\\\{', 2), false);
  });

  it('no backslash is not escaped', () => {
    assert.equal(isEscapedAtPosition('{', 0), false);
  });
});

// ---------------------------------------------------------------------------
// validateControlCharacters
// ---------------------------------------------------------------------------

describe('validateControlCharacters', () => {
  it('rejects null byte', () => {
    const r = validateControlCharacters('echo\x00hello');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.CONTROL_CHARACTERS);
  });

  it('rejects backspace', () => {
    assert.equal(validateControlCharacters('cmd\x08x').safe, false);
  });

  it('allows tab', () => {
    assert.equal(validateControlCharacters('echo\thello').safe, true);
  });

  it('allows newline (handled by other validator)', () => {
    assert.equal(validateControlCharacters('echo\nhello').safe, true);
  });

  it('allows normal commands', () => {
    assert.equal(validateControlCharacters('ls -la').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateCarriageReturn
// ---------------------------------------------------------------------------

describe('validateCarriageReturn', () => {
  it('rejects \\r outside double quotes', () => {
    const r = validateCarriageReturn('cmd\recho evil');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.CARRIAGE_RETURN);
  });

  it('allows \\r inside double quotes', () => {
    assert.equal(validateCarriageReturn('echo "hello\rworld"').safe, true);
  });

  it('allows commands without \\r', () => {
    assert.equal(validateCarriageReturn('echo hello').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateIFSInjection
// ---------------------------------------------------------------------------

describe('validateIFSInjection', () => {
  it('rejects $IFS', () => {
    const r = validateIFSInjection('cmd$IFSarg');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.IFS_INJECTION);
  });

  it('rejects ${IFS:0:1}', () => {
    assert.equal(validateIFSInjection('cmd${IFS:0:1}arg').safe, false);
  });

  it('rejects ${#IFS}', () => {
    assert.equal(validateIFSInjection('echo ${#IFS}').safe, false);
  });

  it('allows normal commands', () => {
    assert.equal(validateIFSInjection('echo hello world').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateProcEnvironAccess
// ---------------------------------------------------------------------------

describe('validateProcEnvironAccess', () => {
  it('rejects /proc/self/environ', () => {
    const r = validateProcEnvironAccess('cat /proc/self/environ');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PROC_ENVIRON_ACCESS);
  });

  it('rejects /proc/1/environ', () => {
    assert.equal(validateProcEnvironAccess('cat /proc/1/environ').safe, false);
  });

  it('allows /proc/self/status', () => {
    assert.equal(validateProcEnvironAccess('cat /proc/self/status').safe, true);
  });

  it('allows normal commands', () => {
    assert.equal(validateProcEnvironAccess('ls /proc').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateObfuscatedFlags
// ---------------------------------------------------------------------------

describe('validateObfuscatedFlags', () => {
  it("rejects ANSI-C quoting $'...'", () => {
    const r = validateObfuscatedFlags("find . $'-exec' rm {} +");
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.OBFUSCATED_FLAGS);
  });

  it('rejects locale quoting $"..."', () => {
    assert.equal(validateObfuscatedFlags('cmd $"-flag"').safe, false);
  });

  it("rejects empty quotes before dash: ''-flag", () => {
    assert.equal(validateObfuscatedFlags("cmd ''-flag").safe, false);
  });

  it('allows echo with ANSI-C (safe)', () => {
    assert.equal(validateObfuscatedFlags("echo $'hello'").safe, true);
  });

  it('rejects echo with ANSI-C when piped', () => {
    assert.equal(validateObfuscatedFlags("echo $'hello' | cmd").safe, false);
  });

  it('allows normal flags', () => {
    assert.equal(validateObfuscatedFlags('ls -la').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateUnicodeWhitespace
// ---------------------------------------------------------------------------

describe('validateUnicodeWhitespace', () => {
  it('rejects non-breaking space (\\u00A0)', () => {
    const r = validateUnicodeWhitespace('cmd arg');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.UNICODE_WHITESPACE);
  });

  it('rejects ideographic space (\\u3000)', () => {
    assert.equal(validateUnicodeWhitespace('cmd　arg').safe, false);
  });

  it('rejects BOM (\\uFEFF)', () => {
    assert.equal(validateUnicodeWhitespace('﻿echo hello').safe, false);
  });

  it('allows normal spaces', () => {
    assert.equal(validateUnicodeWhitespace('echo hello world').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateBraceExpansion
// ---------------------------------------------------------------------------

describe('validateBraceExpansion', () => {
  it('rejects comma-separated brace expansion', () => {
    const r = validateBraceExpansion('{a,b,c}', '{a,b,c}');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.BRACE_EXPANSION);
  });

  it('rejects sequence brace expansion', () => {
    assert.equal(validateBraceExpansion('{1..5}', '{1..5}').safe, false);
  });

  it('rejects quoted brace inside brace context', () => {
    // fullyUnquoted has {a}, original has quoted brace — should detect obfuscation
    assert.equal(validateBraceExpansion('{a}', "cmd {'{'}a}").safe, false);
  });

  it('allows escaped braces', () => {
    assert.equal(validateBraceExpansion('\\{a,b\\}', '\\{a,b\\}').safe, true);
  });

  it('allows single braces without expansion', () => {
    assert.equal(validateBraceExpansion('{test}', '{test}').safe, true);
  });

  it('allows no braces', () => {
    assert.equal(validateBraceExpansion('echo hello', 'echo hello').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateDangerousVariables
// ---------------------------------------------------------------------------

describe('validateDangerousVariables', () => {
  it('rejects variable piped to command', () => {
    const r = validateDangerousVariables('$HOME | cat');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DANGEROUS_VARIABLES);
  });

  it('rejects redirection with variable', () => {
    assert.equal(validateDangerousVariables('> $FILE').safe, false);
  });

  it('allows normal variable usage', () => {
    assert.equal(validateDangerousVariables('echo $HOME').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateZshDangerousCommands
// ---------------------------------------------------------------------------

describe('validateZshDangerousCommands', () => {
  it('rejects zmodload', () => {
    const r = validateZshDangerousCommands('zmodload zsh/system');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.ZSH_DANGEROUS_COMMANDS);
  });

  it('rejects emulate', () => {
    assert.equal(validateZshDangerousCommands('emulate -c evil').safe, false);
  });

  it('rejects zpty', () => {
    assert.equal(validateZshDangerousCommands('zpty -b name cmd').safe, false);
  });

  it('rejects syswrite', () => {
    assert.equal(validateZshDangerousCommands('syswrite -o 3 data').safe, false);
  });

  it('rejects fc -e', () => {
    assert.equal(validateZshDangerousCommands('fc -e vim').safe, false);
  });

  it('skips env var assignments to find base command', () => {
    assert.equal(validateZshDangerousCommands('FOO=bar zmodload zsh/system').safe, false);
  });

  it('skips precommand modifiers', () => {
    assert.equal(validateZshDangerousCommands('command builtin zmodload zsh/system').safe, false);
  });

  it('allows normal commands', () => {
    assert.equal(validateZshDangerousCommands('ls -la').safe, true);
  });

  it('allows fc without -e', () => {
    assert.equal(validateZshDangerousCommands('fc -l').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateBackslashEscapedOperators
// ---------------------------------------------------------------------------

describe('validateBackslashEscapedOperators', () => {
  it('rejects \\; outside quotes', () => {
    const r = validateBackslashEscapedOperators('cmd \\; evil');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.BACKSLASH_ESCAPED_OPERATORS);
  });

  it('rejects \\| outside quotes', () => {
    assert.equal(validateBackslashEscapedOperators('cmd \\| evil').safe, false);
  });

  it('allows operators inside single quotes', () => {
    assert.equal(validateBackslashEscapedOperators("echo 'test \\; ok'").safe, true);
  });

  it('allows normal commands', () => {
    assert.equal(validateBackslashEscapedOperators('ls -la').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateNewlines
// ---------------------------------------------------------------------------

describe('validateNewlines', () => {
  it('rejects unquoted newline followed by command', () => {
    const r = validateNewlines('echo hello\nrm -rf /');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.NEWLINES);
  });

  it('allows newlines inside quotes', () => {
    assert.equal(validateNewlines('echo "hello\nworld"').safe, true);
  });

  it('allows commands without newlines', () => {
    assert.equal(validateNewlines('echo hello').safe, true);
  });
});

// ---------------------------------------------------------------------------
// validateBashSecurity (orchestrator)
// ---------------------------------------------------------------------------

describe('validateBashSecurity', () => {
  it('passes safe commands', () => {
    assert.equal(validateBashSecurity('echo hello').safe, true);
    assert.equal(validateBashSecurity('ls -la /tmp').safe, true);
    assert.equal(validateBashSecurity('git status').safe, true);
    assert.equal(validateBashSecurity('npm test').safe, true);
    assert.equal(validateBashSecurity('cat file.txt | grep pattern').safe, true);
  });

  it('catches IFS injection', () => {
    assert.equal(validateBashSecurity('cmd$IFSarg').safe, false);
  });

  it('catches /proc/environ', () => {
    assert.equal(validateBashSecurity('cat /proc/self/environ').safe, false);
  });

  it('catches control characters', () => {
    assert.equal(validateBashSecurity('echo\x00test').safe, false);
  });

  it('catches Unicode whitespace', () => {
    assert.equal(validateBashSecurity('cmd arg').safe, false);
  });

  it('catches brace expansion', () => {
    const extracted = extractQuotedContent('{a,b}');
    assert.equal(validateBashSecurity('{a,b}').safe, false);
  });

  it('catches zsh dangerous commands', () => {
    assert.equal(validateBashSecurity('zmodload zsh/system').safe, false);
  });
});

// ---------------------------------------------------------------------------
// Integration with bashTool
// ---------------------------------------------------------------------------

describe('bash tool integration', () => {
  it('bashTool blocks IFS injection', async () => {
    const { bashTool } = require('../src/tools/bash');
    const ctx = { cwd: '/tmp', abortSignal: new AbortController().signal };
    const result = await bashTool.execute({ command: 'cmd$IFSarg' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('IFS'));
  });

  it('bashTool blocks /proc/environ', async () => {
    const { bashTool } = require('../src/tools/bash');
    const ctx = { cwd: '/tmp', abortSignal: new AbortController().signal };
    const result = await bashTool.execute({ command: 'cat /proc/self/environ' }, ctx);
    assert.ok(result.isError);
    assert.ok(result.output.includes('environ'));
  });

  it('bashTool allows normal commands', async () => {
    const { bashTool } = require('../src/tools/bash');
    const ctx = { cwd: '/tmp', abortSignal: new AbortController().signal };
    const result = await bashTool.execute({ command: 'echo safe_test_123' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('safe_test_123'));
  });
});

// ---------------------------------------------------------------------------
// stripSafeWrappers
// ---------------------------------------------------------------------------
describe('stripSafeWrappers', () => {
  it('strips timeout prefix', () => {
    assert.equal(stripSafeWrappers('timeout 30 ls -la'), 'ls -la');
  });

  it('strips timeout with flags', () => {
    assert.equal(stripSafeWrappers('timeout --signal=KILL 60 curl http://x'), 'curl http://x');
  });

  it('strips time prefix', () => {
    assert.equal(stripSafeWrappers('time make -j4'), 'make -j4');
  });

  it('strips nice prefix with priority', () => {
    assert.equal(stripSafeWrappers('nice -n 10 gcc main.c'), 'gcc main.c');
  });

  it('strips nohup prefix', () => {
    assert.equal(stripSafeWrappers('nohup node server.js'), 'node server.js');
  });

  it('strips stdbuf prefix with flags', () => {
    assert.equal(stripSafeWrappers('stdbuf -oL grep pattern file'), 'grep pattern file');
  });

  it('strips env with variable assignments', () => {
    assert.equal(stripSafeWrappers('env FOO=bar BAZ=qux ls'), 'ls');
  });

  it('strips chained wrappers', () => {
    assert.equal(stripSafeWrappers('nice -n 5 timeout 30 stdbuf -oL cmd arg'), 'cmd arg');
  });

  it('returns command as-is when no wrappers', () => {
    assert.equal(stripSafeWrappers('git status'), 'git status');
  });

  it('does not strip non-wrapper commands', () => {
    assert.equal(stripSafeWrappers('rm -rf /'), 'rm -rf /');
  });

  it('detects dangerous commands through wrappers', () => {
    const result = validateBashSecurity('timeout 10 zmodload zsh/system');
    assert.ok(!result.safe);
  });
});

// ---------------------------------------------------------------------------
// sed validation
// ---------------------------------------------------------------------------

describe('validateSedDangerous', () => {
  it('allows safe sed substitution', () => {
    const result = validateSedDangerous("sed 's/foo/bar/g' file.txt");
    assert.ok(result.safe);
  });

  it('allows sed line printing', () => {
    const result = validateSedDangerous("sed -n '5p' file.txt");
    assert.ok(result.safe);
  });

  it('allows sed delete lines', () => {
    const result = validateSedDangerous("sed '/^#/d' file.txt");
    assert.ok(result.safe);
  });

  it('blocks sed substitution with e flag', () => {
    const result = validateSedDangerous("sed 's/foo/whoami/e' file.txt");
    assert.ok(!result.safe);
    assert.equal(result.checkId, CHECK_IDS.SED_DANGEROUS);
    assert.ok(result.message.includes("'e' flag"));
  });

  it('blocks sed w command writing to file', () => {
    const result = validateSedDangerous("sed -n '/pattern/ w /tmp/output.txt' input.txt");
    assert.ok(!result.safe);
    assert.equal(result.checkId, CHECK_IDS.SED_DANGEROUS);
  });

  it('blocks sed R command', () => {
    const result = validateSedDangerous("sed '1 R /etc/passwd' file.txt");
    assert.ok(!result.safe);
  });

  it('blocks sed W command', () => {
    const result = validateSedDangerous("sed '1 W /tmp/output' file.txt");
    assert.ok(!result.safe);
  });

  it('blocks sed -i on sensitive paths', () => {
    const result = validateSedDangerous("sed -i 's/old/new/' /etc/hosts");
    assert.ok(!result.safe);
    assert.ok(result.message.includes('sensitive'));
  });

  it('blocks sed --in-place on .env', () => {
    const result = validateSedDangerous("sed --in-place 's/x/y/' .env");
    assert.ok(!result.safe);
  });

  it('allows sed -i on normal files', () => {
    const result = validateSedDangerous("sed -i 's/old/new/' src/main.ts");
    assert.ok(result.safe);
  });

  it('passes through non-sed commands', () => {
    const result = validateSedDangerous('grep pattern file.txt');
    assert.ok(result.safe);
  });

  it('integrates into validateBashSecurity', () => {
    const result = validateBashSecurity("sed 's/x/id/e' file.txt");
    assert.ok(!result.safe);
    assert.equal(result.checkId, CHECK_IDS.SED_DANGEROUS);
  });
});
