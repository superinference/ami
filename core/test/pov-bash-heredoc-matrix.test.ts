import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateBashSecurity,
  stripHeredocBodies,
  detectNuclearCommand,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// POV: EXHAUSTIVE guard for ui#26 (heredoc bodies are inert stdin DATA, not
// command separation / file redirects) + the un-bypassable destructive guards.
//
// These tests assert OBSERVABLE BEHAVIOR of the real functions:
//   - validateBashSecurity(cmd).safe            → true (allow) / false (block)
//   - stripHeredocBodies(cmd)                   → the transformed string
//   - detectNuclearCommand(cmd).blocked         → true (defense-in-depth)
// No source text is read; every expectation is derived from first principles
// about what the loosening is supposed to (and must NOT) do.
// ---------------------------------------------------------------------------

/** Assert a command is auto-approvable (passes every security validator). */
function allow(cmd: string, label: string): void {
  const r = validateBashSecurity(cmd);
  assert.equal(
    r.safe,
    true,
    `[allow:${label}] expected safe=true but blocked (checkId=${r.checkId}): ${r.message}`,
  );
  assert.equal(r.checkId, 0, `[allow:${label}] a safe result must carry checkId 0`);
  assert.equal(r.message, '', `[allow:${label}] a safe result must carry an empty message`);
}

/** Assert a command is NOT auto-approvable (some validator blocks it). */
function block(cmd: string, label: string): { safe: boolean; checkId: number; message: string } {
  const r = validateBashSecurity(cmd);
  assert.equal(
    r.safe,
    false,
    `[block:${label}] expected safe=false but it was allowed`,
  );
  assert.notEqual(r.checkId, 0, `[block:${label}] a blocked result must carry a non-zero checkId`);
  assert.ok(r.message.length > 0, `[block:${label}] a blocked result must carry a message`);
  return r;
}

describe('pov: heredoc loosening — ALLOW (ui#26)', () => {
  it('allows a plain heredoc (cat <<EOF … EOF)', () => {
    allow('cat <<EOF\nhello world\nEOF', 'plain');
  });

  it("allows a quoted-delimiter heredoc (<<'QUOTED')", () => {
    allow("cat <<'EOF'\nno $expansion here\nEOF", 'single-quoted');
    allow('cat <<"EOF"\nliteral body\nEOF', 'double-quoted');
  });

  it('allows the dash form with an indented closing delimiter (<<-DASH)', () => {
    allow('cat <<-EOF\n\tindented body line\n\tEOF', 'dash-indent');
  });

  it('allows a heredoc with a pipe on the opening line (cat <<EOF | grep x)', () => {
    allow('cat <<EOF | grep hello\nhello\nworld\nEOF', 'pipe-on-open');
  });

  it('allows a heredoc BODY containing < > (HTML/XML) across newlines', () => {
    allow('cat <<EOF\n<html>\n  <body>ok</body>\n</html>\nEOF', 'html-body');
    allow("cat <<'XML'\n<a href=\"x\">link</a>\n<b>bold</b>\nXML", 'xml-body');
  });

  it('allows comparison-like < > inside a heredoc body', () => {
    allow('cat <<EOF\nif 1 < 2 and 3 > 2 then ok\nEOF', 'cmp-body');
  });

  it("allows python3 - <<'PY' … PY", () => {
    allow("python3 - <<'PY'\nprint('hello')\nprint(1 + 2)\nPY", 'python-stdin');
  });

  it('allows a bare-delimiter dash heredoc piped onward', () => {
    allow('cat <<-DATA | sort\n\tbanana\n\tapple\n\tDATA', 'dash-pipe');
  });
});

describe('pov: heredoc loosening — BLOCK (must stay blocked)', () => {
  it('blocks plain newline command separation (cmd1\\ncmd2)', () => {
    const r = block('echo one\necho two', 'newline-sep');
    assert.match(r.message, /newline/i);
  });

  it('blocks a multi-statement newline script with no heredoc at all', () => {
    block('ls -la\nwhoami\nid', 'multi-newline');
  });

  it('blocks a REAL file redirect on a heredoc opening line (> /etc/passwd)', () => {
    // The heredoc body is inert, but the `> /etc/passwd` on the opening line is a
    // genuine file redirect and must still be caught after body stripping.
    block('cat <<EOF > /etc/passwd\nhello\nEOF', 'redirect-on-open-etc');
  });

  it('blocks a real file redirect to an ordinary path on a heredoc opening line', () => {
    const r = block('cat <<EOF > out.txt\nhello\nEOF', 'redirect-on-open-file');
    assert.match(r.message, /redirect/i);
  });

  it('blocks an input redirect from a sensitive file (< /etc/shadow)', () => {
    block('cat < /etc/shadow', 'input-redirect-shadow');
  });

  it('blocks an unterminated heredoc (no closing delimiter)', () => {
    // No closing delimiter → stripHeredocBodies leaves the raw newlines in place,
    // so the structural newline guard still fires.
    block('cat <<EOF\nhello\nthere', 'unterminated');
  });

  it('blocks an unterminated dash heredoc', () => {
    block('cat <<-EOF\n\thello\n\tnot-the-delim', 'unterminated-dash');
  });
});

describe('pov: defense-in-depth — destructive content inside an executable heredoc', () => {
  it('detectNuclearCommand catches rm -rf / inside bash <<EOF', () => {
    const r = detectNuclearCommand('bash <<EOF\nrm -rf /\nEOF');
    assert.equal(r.blocked, true, `expected blocked for rm -rf / in heredoc: ${JSON.stringify(r)}`);
    assert.match(r.reason ?? '', /rm -rf|filesystem/i);
  });

  it('detectNuclearCommand catches a fork bomb inside sh <<SH', () => {
    const r = detectNuclearCommand('sh <<SH\n:(){ :|:& };:\nSH');
    assert.equal(r.blocked, true, `expected blocked for fork bomb in heredoc: ${JSON.stringify(r)}`);
    assert.match(r.reason ?? '', /fork bomb/i);
  });

  it('detectNuclearCommand catches rm -rf $HOME inside a heredoc body', () => {
    const r = detectNuclearCommand('bash <<EOF\nrm -rf $HOME\nEOF');
    assert.equal(r.blocked, true);
  });

  it('detectNuclearCommand leaves an innocuous heredoc untouched', () => {
    const r = detectNuclearCommand('cat <<EOF\njust some text\nEOF');
    assert.equal(r.blocked, false);
  });
});

describe('pov: stripHeredocBodies — unit behavior', () => {
  it('removes the <<WORD operator AND the body+delimiter', () => {
    const out = stripHeredocBodies('cat <<EOF\nhello\nEOF');
    assert.equal(out, 'cat   __HEREDOC__');
    // Body text must be gone; placeholder present; no residual newline.
    assert.doesNotMatch(out, /hello/);
    assert.match(out, /__HEREDOC__/);
    assert.doesNotMatch(out, /\n/);
  });

  it('keeps the remainder of the opening line (trailing pipe)', () => {
    const out = stripHeredocBodies('cat <<EOF | grep x\nhello\nEOF');
    assert.match(out, /\| grep x/);
    assert.match(out, /__HEREDOC__/);
    assert.doesNotMatch(out, /hello/);
  });

  it('keeps a trailing redirect on the opening line (so redirect guard still sees it)', () => {
    const out = stripHeredocBodies('cat <<EOF > out.txt\nhello\nEOF');
    assert.match(out, />\s*out\.txt/);
    assert.doesNotMatch(out, /hello/);
  });

  it('strips the dash form with indented closing delimiter', () => {
    const out = stripHeredocBodies('cat <<-EOF\n\tindented\n\tEOF');
    assert.match(out, /__HEREDOC__/);
    assert.doesNotMatch(out, /indented/);
    assert.doesNotMatch(out, /\n/);
  });

  it('leaves a command with no heredoc completely untouched', () => {
    const src = 'echo hello world && ls -la';
    assert.equal(stripHeredocBodies(src), src);
  });

  it('leaves an unterminated heredoc untouched (raw newlines preserved)', () => {
    const src = 'cat <<EOF\nhello\nno-close';
    assert.equal(stripHeredocBodies(src), src);
    // Newlines must survive so the newline guard can still fire.
    assert.match(stripHeredocBodies(src), /\n/);
  });

  it('is idempotent on content it already transformed', () => {
    const once = stripHeredocBodies('cat <<EOF\nhello\nEOF');
    assert.equal(stripHeredocBodies(once), once);
  });

  it('strips multiple heredocs in one command', () => {
    const out = stripHeredocBodies('cat <<A\nfirst\nA\ncat <<B\nsecond\nB');
    assert.doesNotMatch(out, /first/);
    assert.doesNotMatch(out, /second/);
    const count = (out.match(/__HEREDOC__/g) ?? []).length;
    assert.equal(count, 2);
  });
});

describe('pov: property sweep — delimiter variants all allow + strip cleanly', () => {
  const delimiters = ['EOF', 'END', 'DATA', 'PY', 'SH', '_MARK', 'Delim1'];
  for (const d of delimiters) {
    it(`plain heredoc with delimiter ${d} is allowed and stripped`, () => {
      const cmd = `cat <<${d}\nline one\nline two\n${d}`;
      allow(cmd, `delim-${d}`);
      const out = stripHeredocBodies(cmd);
      assert.match(out, /__HEREDOC__/);
      assert.doesNotMatch(out, /line one/);
    });

    it(`quoted heredoc with delimiter ${d} is allowed and stripped`, () => {
      const cmd = `cat <<'${d}'\nbody <tag>\n${d}`;
      allow(cmd, `qdelim-${d}`);
      assert.doesNotMatch(stripHeredocBodies(cmd), /body <tag>/);
    });
  }
});
