import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectCdSensitiveDesync } from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 — compound cwd-desync TOCTOU detector (detectCdSensitiveDesync).
//
// Parity with the reference's compoundCommandHasCd cwd-desync guard
// (PowerShellTool/modeValidation.ts L192-224, pathValidation.ts L1599-1617):
// once a statement changes directory, a later RELATIVE operand resolves against
// the changed cwd at runtime while the permission layer only saw the literal
// token. A `cd ~/.ssh && cat id_rsa` reads the private key even though `id_rsa`
// alone matches no credential-path rule.
//
// High-signal: only escalates when the cd TARGET itself is a credential or
// sensitive system directory, so ordinary `cd build && cat log` is untouched.
// ===========================================================================

describe('detectCdSensitiveDesync', () => {
  it('escalates cd-into-credential-dir then a relative file read', () => {
    for (const cmd of [
      'cd ~/.ssh && cat id_rsa',
      'cd ~/.ssh; cat id_rsa',
      'cd .ssh && cat id_rsa',
      'cd ~/.gnupg && cat secring.gpg',
      'cd ~/.aws && cat credentials',
      'cd ~/.kube && cat config',
      'cd ~/.ssh && grep key id_rsa',
      'cd ~/.ssh && tee authorized_keys',
      'cd ~/.ssh && cp id_rsa /tmp/out',
    ]) {
      const r = detectCdSensitiveDesync(cmd);
      assert.equal(r.escalate, true, `expected escalate for: ${cmd}`);
      assert.match(r.reason!, /cwd-desync TOCTOU/);
    }
  });

  it('escalates cd-into-sensitive-system-dir then a relative file read', () => {
    for (const cmd of [
      'cd /etc && cat shadow',
      'cd /etc && cat passwd',
      'cd /root && cat .bash_history',
      'cd /proc && cat 1/environ',
    ]) {
      assert.equal(detectCdSensitiveDesync(cmd).escalate, true, `expected escalate for: ${cmd}`);
    }
  });

  it('does not escalate for ordinary (non-sensitive) cd targets', () => {
    for (const cmd of [
      'cd /tmp && cat a',
      'cd build && cat log.txt',
      'cd src && sed -i s/a/b/ main.ts',
      'cd /usr/local && cat README',   // /usr not in the narrow sensitive-system set
      'cd /var/log && tail app.log',   // /var not in the narrow sensitive-system set
    ]) {
      assert.equal(detectCdSensitiveDesync(cmd).escalate, false, `unexpected escalate for: ${cmd}`);
    }
  });

  it('does not escalate without a real cwd-relative operand after the sensitive cd', () => {
    for (const cmd of [
      'cd ~/.ssh && ls',                 // no operand — bare listing
      'cd ~/.ssh && echo done',          // echo operands are not paths
      'cd ~/.ssh && cat /etc/hostname',  // absolute operand — cwd-independent
      'cd ~/.ssh && cat ~/other',        // home-relative operand — cwd-independent
      'cd ~/.ssh && cat $FILE',          // variable — not statically resolvable
    ]) {
      assert.equal(detectCdSensitiveDesync(cmd).escalate, false, `unexpected escalate for: ${cmd}`);
    }
  });

  it('is not fooled by a bare cd (→ $HOME) or by cd-ing back out', () => {
    assert.equal(detectCdSensitiveDesync('cd && cat id_rsa').escalate, false);          // bare cd → home
    assert.equal(detectCdSensitiveDesync('cd ~/.ssh && cd /tmp && cat a').escalate, false); // cwd changed again
  });

  it('requires a compound command (a single segment never desyncs)', () => {
    assert.equal(detectCdSensitiveDesync('cat id_rsa').escalate, false);
    assert.equal(detectCdSensitiveDesync('cd ~/.ssh').escalate, false);
  });

  it('only escalates for file-accessing base commands, not arbitrary ones', () => {
    // `make` runs in the sensitive dir but takes a target name, not a file path.
    assert.equal(detectCdSensitiveDesync('cd ~/.ssh && make all').escalate, false);
  });
});
