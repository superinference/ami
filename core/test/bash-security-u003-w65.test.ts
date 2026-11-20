import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { validateBashSecurity } from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U003 — wave w65: `srun` added to bash-security SAFE_WRAPPERS so the
// AST-blocked-command fallback scan unwraps `srun [opts] <cmd>` and sees the
// inner command. Mirrors the reference's srun wrapper spec (specs/srun.ts) and keeps
// bash-security's wrapper set aligned with permissions.ts SHELL_WRAPPERS.
// ===========================================================================
describe('U003/w65: srun wrapper in bash-security', () => {
  it('bare inner blocked command stays blocked (baseline)', () => {
    assert.equal(validateBashSecurity('mkfs /dev/sda').safe, false);
    assert.equal(validateBashSecurity('nc -l 1234').safe, false);
  });

  it('`srun mkfs /dev/sda` strips the wrapper and blocks the inner mkfs', () => {
    const r = validateBashSecurity('srun mkfs /dev/sda');
    assert.equal(r.safe, false);
    assert.match(r.message, /mkfs/);
  });

  it('`srun nc -l 1234` strips the wrapper and blocks the inner nc', () => {
    assert.equal(validateBashSecurity('srun nc -l 1234').safe, false);
  });

  // srun option-arity: -n/--ntasks/-N/--nodes consume a following count operand.
  it('`srun -n 4 mkfs /dev/sda` skips the -n count and blocks mkfs', () => {
    assert.equal(validateBashSecurity('srun -n 4 mkfs /dev/sda').safe, false);
  });

  it('`srun --nodes 2 mount /mnt` skips the --nodes count and blocks mount', () => {
    assert.equal(validateBashSecurity('srun --nodes 2 mount /mnt').safe, false);
  });

  it('`srun -N1 fdisk /dev/sda` (attached count) blocks the inner fdisk', () => {
    assert.equal(validateBashSecurity('srun -N1 fdisk /dev/sda').safe, false);
  });

  it('a safe inner command wrapped in srun stays safe', () => {
    assert.equal(validateBashSecurity('srun -n 4 echo hi').safe, true);
  });
});
