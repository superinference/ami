import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectHomoglyphCommandName,
  validateBashSecurity,
  CHECK_IDS,
} from '../src/tools/bash-security';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// U015 / w89 — Non-ASCII homoglyph command-name folding.
// Parity with Claude Code utils/powershell/parser.ts L872-886: a command NAME
// carrying non-ASCII look-alike letters resolves to the real command at runtime
// (OrdinalIgnoreCase / Cyrillic-Greek homoglyphs) while evading ASCII-anchored
// deny regexes, so it can never be auto-approved.

describe('detectHomoglyphCommandName (U015/w89)', () => {
  it('blocks a Latin small-letter-long-s homoglyph (U+017F ſ → s) in the command name', () => {
    // ſudo folds to sudo under OrdinalIgnoreCase.
    const r = detectHomoglyphCommandName('ſudo rm -rf /tmp/x');
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /non-ASCII/);
    assert.match(r.reason!, /U\+017F/);
  });

  it('blocks a Cyrillic homoglyph command name (Cyrillic es ѕ → s)', () => {
    const r = detectHomoglyphCommandName('ѕudo whoami');
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /U\+0455/);
  });

  it('blocks a dotless-i PowerShell cmdlet homoglyph (U+0131 ı)', () => {
    const r = detectHomoglyphCommandName('ınvoke-expression "payload"');
    assert.equal(r.blocked, true);
  });

  it('blocks a homoglyph in a later pipeline segment command name', () => {
    const r = detectHomoglyphCommandName('ls | ѕudo tee /etc/x');
    assert.equal(r.blocked, true);
  });

  it('blocks a quoted homoglyph command name', () => {
    const r = detectHomoglyphCommandName('"ѕudo" rm -rf /tmp/x');
    assert.equal(r.blocked, true);
  });

  it('does NOT flag a pure-ASCII command name', () => {
    assert.equal(detectHomoglyphCommandName('sudo rm -rf /tmp/x').blocked, false);
    assert.equal(detectHomoglyphCommandName('ls -la').blocked, false);
    assert.equal(detectHomoglyphCommandName('git status').blocked, false);
  });

  it('does NOT flag a non-ASCII character in an ARGUMENT / path (only the name matters)', () => {
    assert.equal(detectHomoglyphCommandName('cat café.txt').blocked, false);
    assert.equal(detectHomoglyphCommandName('echo "héllo wörld"').blocked, false);
    assert.equal(detectHomoglyphCommandName('grep motörhead file.txt').blocked, false);
  });

  it('skips leading env-var assignments and finds the real command name', () => {
    assert.equal(detectHomoglyphCommandName('FOO=bar ls -la').blocked, false);
    assert.equal(detectHomoglyphCommandName('FOO=bar ѕudo whoami').blocked, true);
    // non-ASCII in an assignment VALUE is an argument, not a command name
    assert.equal(detectHomoglyphCommandName('FOO=café ls').blocked, false);
  });

  it('skips a leading subshell/group marker to reach the command name', () => {
    assert.equal(detectHomoglyphCommandName('( ѕudo whoami )').blocked, true);
    assert.equal(detectHomoglyphCommandName('( ls -la )').blocked, false);
  });
});

describe('validateBashSecurity — homoglyph command names (U015/w89 integration)', () => {
  it('blocks a homoglyph sudo with checkId HOMOGLYPH_COMMAND_NAME', () => {
    const r = validateBashSecurity('ѕudo rm -rf /tmp/x');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.HOMOGLYPH_COMMAND_NAME);
  });

  it('blocks a homoglyph PowerShell cmdlet (ſtart-proceſſ)', () => {
    const r = validateBashSecurity('ſtart-proceſſ calc');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.HOMOGLYPH_COMMAND_NAME);
  });

  it('keeps a legitimate command with a non-ASCII filename ARGUMENT safe (no over-block)', () => {
    const r = validateBashSecurity('cat café.txt');
    assert.equal(r.safe, true);
  });

  it('keeps ordinary ASCII commands safe (no regression)', () => {
    assert.equal(validateBashSecurity('ls -la').safe, true);
    assert.equal(validateBashSecurity('git status').safe, true);
  });
});

describe('bashTool.execute — homoglyph command is blocked end-to-end (U015/w89 E2E)', () => {
  it('refuses to run a Cyrillic-homoglyph command and never executes it', async () => {
    // ѕudo folds to `sudo`; the ASCII \bsudo\b deny regex would otherwise miss it.
    const result = await bashTool.execute({ command: 'ѕudo whoami' }, ctx());
    assert.equal(result.isError, true);
    assert.match(result.output, /non-ASCII|homoglyph/);
    // The real command must not have executed (no username / no normal output).
    assert.doesNotMatch(result.output, /uid=|\broot\b/);
  });

  it('still runs an equivalent pure-ASCII safe command (control)', async () => {
    const result = await bashTool.execute({ command: 'echo homoglyph-control-ok' }, ctx());
    assert.equal(result.isError, false);
    assert.match(result.output, /homoglyph-control-ok/);
  });
});
