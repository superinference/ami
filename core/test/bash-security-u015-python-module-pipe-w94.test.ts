import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  detectBase64ShellPipe,
  validateBashSecurity,
} from '../src/tools/bash-security.ts';

// ---------------------------------------------------------------------------
// U015 (wave 94) — gap: isModule handling for `python -m <module>` piped to a
// shell interpreter (registry.ts isModule / specs isModule).
//
// The prior textual check (`/\bpython[23]?\s+(?:-\S+\s+)*-m\b/`) only matched the
// SPACED `-m <module>` token. Python also accepts the module flag FUSED into a
// short-flag cluster (`python -mhttp.server`, a valid invocation) and after a
// value-taking option whose value is a bare word (`python -W ignore -mevil`).
// Both forms emit the module's attacker-chosen stdout straight into a shell — the
// same code-to-shell vector as `python -c` — yet evaded the detector.
//
// RED on revert: restoring the `-m\b` regex lets the fused / flag-separated forms
// through (detectBase64ShellPipe → blocked:false, validateBashSecurity → safe).
// ---------------------------------------------------------------------------

describe('python -m module-to-shell pipe (U015 w94 isModule)', () => {
  it('still blocks the spaced form (regression lock)', () => {
    const r = detectBase64ShellPipe('python -m http.server | sh');
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /python -m module/);
  });

  it('blocks the FUSED short-flag form `-mMODULE` (was a bypass)', () => {
    const r = detectBase64ShellPipe('python -mhttp.server | sh');
    assert.equal(r.blocked, true, 'fused -mMODULE must be detected as a module invocation');
    assert.match(r.reason!, /python -m module/);
  });

  it('blocks `-mMODULE` reached after a value-taking flag (`-W ignore`)', () => {
    const r = detectBase64ShellPipe('python3 -W ignore -mevil | bash');
    assert.equal(r.blocked, true);
  });

  it('blocks the `-m=MODULE` and `--module=MODULE` equals forms', () => {
    assert.equal(detectBase64ShellPipe('python -m=evil | sh').blocked, true);
    assert.equal(detectBase64ShellPipe('python --module=evil | sh').blocked, true);
  });

  it('blocks a module pipe hidden behind a safe wrapper (`timeout 5 python -mX | sh`)', () => {
    const r = detectBase64ShellPipe('timeout 5 python -mhttp.server | sh');
    assert.equal(r.blocked, true);
  });

  it('does NOT flag `-m` that is an argument to a SCRIPT, not Python (`python app.py -m x`)', () => {
    // Python's `-m` only applies before a script file; once the script positional
    // appears, a later `-m` is the script's own argument.
    const r = detectBase64ShellPipe('python app.py -m deploy | sh');
    assert.equal(r.blocked, false);
  });

  it('does NOT flag a non-python command with a `-m` flag (`sort -m a b | sh`)', () => {
    assert.equal(detectBase64ShellPipe('sort -m a b | sh').blocked, false);
  });

  it('does NOT flag a plain python module run with no shell pipe', () => {
    assert.equal(detectBase64ShellPipe('python -mhttp.server').blocked, false);
  });

  it('E2E: validateBashSecurity rejects the fused module-to-shell pipe', () => {
    const v = validateBashSecurity('python -mhttp.server | sh');
    assert.equal(v.safe, false, 'fused python -m module piped to a shell must be blocked end-to-end');
  });

  it('E2E: validateBashSecurity still allows a benign python module run', () => {
    const v = validateBashSecurity('python -mhttp.server 8000');
    assert.equal(v.safe, true);
  });
});
