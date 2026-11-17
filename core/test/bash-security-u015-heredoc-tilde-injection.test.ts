import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateBashSecurity,
  stripHeredocBodies,
  detectAssignmentTildeExpansion,
  CHECK_IDS,
} from '../src/tools/bash-security';

// ===========================================================================
// UNIT U015 — bash-security.ts parity gaps (heredoc quote-awareness,
// assignment-RHS tilde expansion, command-injection-check opt-out knob)
// ===========================================================================

// Gaps 20/23/24 — quoted-vs-unquoted heredoc distinction (quotedOnly semantics).
// A QUOTED heredoc delimiter (<<'EOF' / <<"EOF" / <<\EOF) makes the body LITERAL
// in bash: no $()/${}/`` expansion runs, so those tokens are inert text and must
// not trip the command-substitution guard. An UNQUOTED <<EOF body IS expanded and
// stays scanned.
describe('U015 heredoc quotedOnly distinction', () => {
  it('RED-GREEN (gap 24): a single-quoted heredoc body with $() is no longer a false positive', () => {
    // Before the fix this returned { safe: false, checkId: COMMAND_SUBSTITUTION }.
    const r = validateBashSecurity("cat <<'EOF'\n$(whoami)\nEOF");
    assert.equal(r.safe, true, 'quoted heredoc body is literal — $() must not block');
  });

  it('allows a double-quoted heredoc literal body containing $()', () => {
    assert.equal(validateBashSecurity('cat <<"EOF"\n$(id)\nEOF').safe, true);
  });

  it('allows a backslash-escaped heredoc literal body containing $()', () => {
    assert.equal(validateBashSecurity('cat <<\\EOF\n$(whoami)\nEOF').safe, true);
  });

  it('allows a literal $()/${} body in a quoted heredoc', () => {
    assert.equal(validateBashSecurity("cat <<'END'\nprice is $(100)\nEND").safe, true);
    assert.equal(validateBashSecurity("cat <<'END'\n${NOT_EXPANDED}\nEND").safe, true);
  });

  it('STILL blocks an UNQUOTED heredoc whose body bash DOES expand', () => {
    const r = validateBashSecurity('cat <<EOF\n$(whoami)\nEOF');
    assert.equal(r.safe, false, 'unquoted heredoc body is expanded — must stay blocked');
    assert.equal(r.checkId, CHECK_IDS.COMMAND_SUBSTITUTION);
  });

  it('stripHeredocBodies default strips every heredoc body; quotedOnly strips only quoted ones', () => {
    const cmd = "a <<'Q'\nq$(x)\nQ\nb <<U\nu$(y)\nU";
    // Default: both bodies replaced with the __HEREDOC__ sentinel.
    const def = stripHeredocBodies(cmd);
    assert.ok(!def.includes('q$(x)'), 'quoted body stripped by default');
    assert.ok(!def.includes('u$(y)'), 'unquoted body stripped by default');
    // quotedOnly: only the quoted (literal) body is stripped; the unquoted
    // (expanding) body is left visible for the substitution validators.
    const q = stripHeredocBodies(cmd, { quotedOnly: true });
    assert.ok(!q.includes('q$(x)'), 'quoted body stripped in quotedOnly mode');
    assert.ok(q.includes('u$(y)'), 'unquoted body preserved in quotedOnly mode');
  });

  it('stripHeredocBodies is a no-op when there is no heredoc', () => {
    assert.equal(stripHeredocBodies('echo hi', { quotedOnly: true }), 'echo hi');
  });

  it('does not regress a dangerous $(…heredoc…) substitution in command-name position', () => {
    // A quoted heredoc nested in a $() command-name position must still be rejected.
    const r = validateBashSecurity("$(cat <<'EOF'\nchmod\nEOF\n) 777 /etc/shadow");
    assert.equal(r.safe, false);
  });
});

// Gap 52 — tilde expansion in an assignment RHS (VAR=~/x used later as a bare path).
describe('U015 assignment-RHS tilde expansion (gap 52)', () => {
  it('blocks VAR=~/x (tilde at value start)', () => {
    const d = detectAssignmentTildeExpansion('VAR=~/x');
    assert.equal(d.blocked, true);
    assert.equal(validateBashSecurity('VAR=~/x').safe, false);
  });

  it('blocks a PATH-style list with ~ after a colon', () => {
    assert.equal(detectAssignmentTildeExpansion('PATH=~/bin:~/tools cmd').blocked, true);
    assert.equal(validateBashSecurity('VAR=foo:~/tools cmd').safe, false);
    assert.equal(validateBashSecurity('VAR=foo:~/tools cmd').checkId, CHECK_IDS.ASSIGNMENT_TILDE_EXPANSION);
  });

  it('blocks a declaring-builtin assignment (export VAR=~/x)', () => {
    assert.equal(detectAssignmentTildeExpansion('export LD_PRELOAD=~/evil.so').blocked, true);
  });

  it('ALLOWS a quoted tilde — literal to bash, no expansion', () => {
    assert.equal(detectAssignmentTildeExpansion("VAR='~/x'").blocked, false);
    assert.equal(detectAssignmentTildeExpansion('VAR="~/x"').blocked, false);
    assert.equal(validateBashSecurity("VAR='~/x' cat \"$VAR\"").safe, true);
  });

  it('does not flag a tilde outside an assignment value', () => {
    assert.equal(detectAssignmentTildeExpansion('VAR=abc echo hi').blocked, false);
    assert.equal(detectAssignmentTildeExpansion('a~b=1').blocked, false);
  });

  it('is a no-op when no tilde is present', () => {
    assert.equal(detectAssignmentTildeExpansion('VAR=1 cmd').blocked, false);
  });
});

// Gap 41 — command-injection-check opt-out env knob (fail-open escape hatch).
describe('U015 command-injection-check disable knob (gap 41)', () => {
  const SI = 'SUPERINFERENCE_DISABLE_COMMAND_INJECTION_CHECK';
  const CC = 'CLAUDE_CODE_DISABLE_COMMAND_INJECTION_CHECK';
  const prevSI = process.env[SI];
  const prevCC = process.env[CC];
  after(() => {
    if (prevSI === undefined) delete process.env[SI]; else process.env[SI] = prevSI;
    if (prevCC === undefined) delete process.env[CC]; else process.env[CC] = prevCC;
  });

  const INJECTION = 'echo $(rm -rf /tmp/x)';

  it('blocks the injection by default (knob unset)', () => {
    delete process.env[SI];
    delete process.env[CC];
    assert.equal(validateBashSecurity(INJECTION).safe, false);
  });

  it('fail-opens the whole injection gate when the SUPERINFERENCE knob is truthy', () => {
    delete process.env[CC];
    process.env[SI] = '1';
    assert.equal(validateBashSecurity(INJECTION).safe, true);
    process.env[SI] = 'true';
    assert.equal(validateBashSecurity(INJECTION).safe, true);
  });

  it('honors the cross-framework CLAUDE_CODE alias', () => {
    delete process.env[SI];
    process.env[CC] = 'yes';
    assert.equal(validateBashSecurity(INJECTION).safe, true);
  });

  it('a falsy knob value keeps the gate on', () => {
    process.env[SI] = '0';
    delete process.env[CC];
    assert.equal(validateBashSecurity(INJECTION).safe, false);
  });
});
