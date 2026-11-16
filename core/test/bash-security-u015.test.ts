import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateBashSecurity,
  detectPersistenceCommands,
  detectEvalLikeBuiltins,
  detectArithmeticConditionalInjection,
  commandTargetsSystemDir,
  detectCredentialPathWrite,
} from '../src/tools/bash-security';
import { getOutputRedirections } from '../src/tools/bash-parser';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ===========================================================================
// UNIT U015 — bash-security.ts parity gaps
// ===========================================================================

// ---------------------------------------------------------------------------
// Gap: Zsh/POSIX force-clobber redirect operator variants (`>!`, `2>!`, `>!file`)
// The canonical redirect scanner must consume the force-clobber `!` as part of
// the operator so the target path is captured clean — otherwise `>!file`
// yields the stray target `!file`, which path.resolve() treats as relative and
// which bypasses the system-dir / credential write guards.
// ---------------------------------------------------------------------------
describe('force-clobber redirect operator (>! / 2>!)', () => {
  it('captures a clean target for the spaced force-clobber form `>! path`', () => {
    const r = getOutputRedirections('echo x >! /usr/bin/evil');
    assert.deepEqual(r, [{ target: '/usr/bin/evil', operator: '>' }]);
  });

  it('captures a clean target for the no-space force-clobber form `>!path`', () => {
    const r = getOutputRedirections('echo x >!/usr/bin/evil');
    assert.deepEqual(r, [{ target: '/usr/bin/evil', operator: '>' }]);
  });

  it('strips the leading `!` so the system-dir write guard fires on `>!`', () => {
    // RED before fix: target captured as `!/usr/bin/evil` → commandTargetsSystemDir false.
    assert.equal(commandTargetsSystemDir('echo x >!/usr/bin/evil'), true);
  });

  it('handles the fd-prefixed force-clobber form `2>!` and fires the credential guard', () => {
    const r = getOutputRedirections('echo x 2>! ~/.ssh/authorized_keys');
    assert.deepEqual(r, [{ target: '~/.ssh/authorized_keys', operator: '>' }]);
    assert.equal(detectCredentialPathWrite('echo x 2>! ~/.aws/credentials').blocked, true);
  });

  it('still parses the noclobber override `>|` form (regression)', () => {
    assert.deepEqual(getOutputRedirections('echo ok >| out.txt'), [
      { target: 'out.txt', operator: '>' },
    ]);
  });

  it('still parses an ordinary `>` redirect (regression)', () => {
    assert.deepEqual(getOutputRedirections('echo ok > normal.txt'), [
      { target: 'normal.txt', operator: '>' },
    ]);
  });
});

// ---------------------------------------------------------------------------
// Gap: Arithmetic-expansion variable/subscript injection — `let`
// `let EXPR` evaluates its argument as bash arithmetic (identical evaluator to
// `$((…))`, which AMI already hard-denies), so it must be denied too.
// ---------------------------------------------------------------------------
describe('let arithmetic-evaluator denial', () => {
  it('blocks a bare `let` invocation', () => {
    const r = detectEvalLikeBuiltins('let z=1+1');
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /arithmetic/);
  });

  it('blocks `let` after a command separator (taint payload hidden in single quotes)', () => {
    const r = detectEvalLikeBuiltins("x='a[$(id)]'; let \"z=x\"");
    assert.equal(r.blocked, true);
  });

  it('is wired into validateBashSecurity', () => {
    assert.equal(validateBashSecurity('let count=count+1').safe, false);
  });

  it('does NOT misfire on `let` used as a plain argument', () => {
    assert.equal(detectEvalLikeBuiltins('echo let me go').blocked, false);
  });

  it('does NOT misfire on a word ending in `let`', () => {
    assert.equal(detectEvalLikeBuiltins('applet build').blocked, false);
  });
});

// ---------------------------------------------------------------------------
// Gap: Arithmetic-expansion injection via `[[ … -eq … ]]` comparison operands
// bash evaluates the operands of -eq/-ne/-lt/-gt/-le/-ge AS ARITHMETIC, so a
// non-numeric operand carrying a subscript / substitution — directly or via a
// same-line tainted variable — executes code the same way `$((…))` does.
// ---------------------------------------------------------------------------
describe('detectArithmeticConditionalInjection', () => {
  it('blocks a single-quote-hidden payload fed into an arithmetic comparison', () => {
    // RED before fix: `$(id)` hides in single quotes → command-sub extractor misses it.
    const r = detectArithmeticConditionalInjection("x='a[$(id)]'; [[ \"$x\" -eq 0 ]]");
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /arithmetic/);
  });

  it('blocks a direct array-subscript operand', () => {
    assert.equal(detectArithmeticConditionalInjection('[[ a[0] -eq 5 ]]').blocked, true);
  });

  it('allows a numeric arithmetic comparison', () => {
    assert.equal(detectArithmeticConditionalInjection('[[ 3 -lt 4 ]]').blocked, false);
  });

  it('allows a plain variable numeric comparison', () => {
    assert.equal(detectArithmeticConditionalInjection('[[ $n -eq 5 ]]').blocked, false);
  });

  it('allows a non-arithmetic string comparison with a variable', () => {
    assert.equal(detectArithmeticConditionalInjection('[[ "$name" == foo ]]').blocked, false);
  });

  it('is wired into validateBashSecurity', () => {
    assert.equal(validateBashSecurity("x='a[$(id)]'; [[ \"$x\" -eq 0 ]]").safe, false);
  });
});

// ---------------------------------------------------------------------------
// Gap: Scheduled-task flag detection (Windows persistence parity with cron/at)
// ---------------------------------------------------------------------------
describe('detectPersistenceCommands — Windows scheduled tasks', () => {
  it('blocks `schtasks /create`', () => {
    const r = detectPersistenceCommands('schtasks /create /tn evil /tr calc.exe');
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /schtasks/);
  });

  it('blocks the getopt-style `schtasks -change`', () => {
    assert.equal(detectPersistenceCommands('schtasks -change -tn x -tr y').blocked, true);
  });

  it('allows read-only `schtasks /query`', () => {
    assert.equal(detectPersistenceCommands('schtasks /query').blocked, false);
  });

  it('blocks Register-ScheduledTask', () => {
    assert.equal(detectPersistenceCommands('Register-ScheduledTask -TaskName evil').blocked, true);
  });

  it('blocks New-ScheduledTask and Set-ScheduledTask', () => {
    assert.equal(detectPersistenceCommands('New-ScheduledTask -Action a').blocked, true);
    assert.equal(detectPersistenceCommands('Set-ScheduledTask -TaskName x').blocked, true);
  });

  it('does NOT misfire on `schtasks` mentioned as an argument', () => {
    assert.equal(detectPersistenceCommands('echo schtasks docs').blocked, false);
  });

  it('is wired into validateBashSecurity', () => {
    assert.equal(validateBashSecurity('schtasks /create /tn evil /tr calc.exe').safe, false);
  });
});

// ---------------------------------------------------------------------------
// Gap: UTF-8 byte-accurate command-length limit (vs UTF-16 code-unit count)
// A BMP character is 1 UTF-16 code unit but up to 3 UTF-8 bytes, so `.length`
// under-reports the size by up to 3× and would let a long multibyte command
// slip past the ceiling.
// ---------------------------------------------------------------------------
describe('command-length limit measured in UTF-8 bytes', () => {
  it('blocks a multibyte command whose UTF-8 byte length exceeds the limit but whose .length does not', () => {
    const cjk = '中'.repeat(3400); // 3400 UTF-16 units (<10000) but 10200 UTF-8 bytes (>10000)
    assert.ok(cjk.length < 10_000);
    assert.ok(Buffer.byteLength('echo ' + cjk, 'utf8') > 10_000);
    const r = validateBashSecurity('echo ' + cjk);
    assert.equal(r.safe, false);
    assert.match(r.message!, /bytes/);
  });

  it('allows an ASCII command of the same character count (well under the byte limit)', () => {
    const ascii = 'a'.repeat(3400);
    // The redirect/metachar validators do not fire on a plain echo of letters.
    assert.equal(validateBashSecurity('echo ' + ascii).safe, true);
  });
});

// ===========================================================================
// E2E — through the real bash tool (validateBashSecurity is enforced in execute)
// ===========================================================================
describe('bashTool.execute — U015 guards block the command end to end', () => {
  it('blocks a `let` arithmetic-evaluator command', async () => {
    const r = await bashTool.execute({ command: 'let z=1+1' }, ctx());
    assert.ok(r.isError);
    assert.match(r.output, /arithmetic/);
  });

  it('blocks a schtasks persistence command', async () => {
    const r = await bashTool.execute(
      { command: 'schtasks /create /tn evil /tr calc.exe' },
      ctx(),
    );
    assert.ok(r.isError);
    assert.match(r.output, /schtasks/);
  });

  it('blocks an arithmetic-conditional injection command', async () => {
    const r = await bashTool.execute(
      { command: "x='a[$(id)]'; [[ \"$x\" -eq 0 ]]" },
      ctx(),
    );
    assert.ok(r.isError);
  });

  it('blocks a force-clobber write into a system dir', async () => {
    const r = await bashTool.execute({ command: 'echo x >!/usr/bin/evil' }, ctx());
    assert.ok(r.isError);
  });
});
