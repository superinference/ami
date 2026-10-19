import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { detectDangerousRm } from '../src/tools/bash-security';

describe('detectDangerousRm', () => {
  it('returns false for commands without rm', () => {
    assert.equal(detectDangerousRm('ls -la').dangerous, false);
    assert.equal(detectDangerousRm('echo hello').dangerous, false);
    assert.equal(detectDangerousRm('cp file1 file2').dangerous, false);
  });

  it('returns false for rm with literal paths', () => {
    assert.equal(detectDangerousRm('rm -rf /tmp/test').dangerous, false);
    assert.equal(detectDangerousRm('rm file.txt').dangerous, false);
    assert.equal(detectDangerousRm('rm -rf dist/').dangerous, false);
  });

  it('detects rm with unguarded variable', () => {
    const result = detectDangerousRm('rm -rf $DIR');
    assert.equal(result.dangerous, true);
    assert.equal(result.variable, 'DIR');
    assert.ok(result.suggestion!.includes('${DIR:?}'));
  });

  it('detects rm -rf with ${VAR} (no guard)', () => {
    const result = detectDangerousRm('rm -rf ${BUILD_DIR}');
    assert.equal(result.dangerous, true);
    assert.equal(result.variable, 'BUILD_DIR');
  });

  it('allows rm with ${VAR:?} guard', () => {
    const result = detectDangerousRm('rm -rf "${DIR:?}"');
    assert.equal(result.dangerous, false);
  });

  it('allows rm with ${VAR:-default} guard', () => {
    const result = detectDangerousRm('rm -rf "${DIR:-/tmp/safe}"');
    assert.equal(result.dangerous, false);
  });

  it('detects rm -f with unguarded variable', () => {
    const result = detectDangerousRm('rm -f $FILE');
    assert.equal(result.dangerous, true);
    assert.equal(result.variable, 'FILE');
  });

  it('detects rm -rfi with variable', () => {
    const result = detectDangerousRm('rm -rfi $OUTDIR/build');
    assert.equal(result.dangerous, true);
  });

  it('provides suggestion with guard syntax', () => {
    const result = detectDangerousRm('rm -rf $DEPLOY_DIR');
    assert.ok(result.suggestion);
    assert.ok(result.suggestion!.includes('${DEPLOY_DIR:?}'));
  });

  // --- Command substitution targets (Gap #23) ---

  it('detects rm -rf with $() command substitution', () => {
    const result = detectDangerousRm('rm -rf "$(pwd)"');
    assert.equal(result.dangerous, true);
    assert.ok(result.suggestion!.includes('command substitution'));
  });

  it('detects rm -rf with $() without quotes', () => {
    const result = detectDangerousRm('rm -rf $(echo /tmp)');
    assert.equal(result.dangerous, true);
  });

  it('detects rm -rf with backtick substitution', () => {
    const result = detectDangerousRm('rm -rf `pwd`');
    assert.equal(result.dangerous, true);
  });

  it('detects rm -rf with nested command substitution', () => {
    const result = detectDangerousRm('rm -rf "$(basename $(pwd))"');
    assert.equal(result.dangerous, true);
  });

  it('detects rm -f with command substitution', () => {
    const result = detectDangerousRm('rm -f $(find /tmp -name "*.log")');
    assert.equal(result.dangerous, true);
  });

  it('still allows rm -rf with literal paths (no regression)', () => {
    assert.equal(detectDangerousRm('rm -rf /tmp/test').dangerous, false);
    assert.equal(detectDangerousRm('rm -rf dist/').dangerous, false);
    assert.equal(detectDangerousRm('rm file.txt').dangerous, false);
  });

  it('detects rm targeting a backslash-only path', () => {
    const result = detectDangerousRm('rm -rf \\');
    assert.equal(result.dangerous, true);
    assert.ok(result.suggestion!.includes('backslash'));
  });

  it('detects rm -rf with backslash target and trailing space', () => {
    const result = detectDangerousRm('rm -rf \\ ');
    assert.equal(result.dangerous, true);
  });

  it('detects rm with PWD variable (working directory derived)', () => {
    const result = detectDangerousRm('rm -rf $PWD');
    assert.equal(result.dangerous, true);
    assert.equal(result.variable, 'PWD');
    assert.ok(result.suggestion!.includes('working directory'));
  });

  it('detects rm with OLDPWD variable', () => {
    const result = detectDangerousRm('rm -rf $OLDPWD');
    assert.equal(result.dangerous, true);
    assert.equal(result.variable, 'OLDPWD');
  });

  it('detects rm with variable followed by top-level directory /usr', () => {
    const result = detectDangerousRm('rm -rf $PREFIX/usr');
    assert.equal(result.dangerous, true);
    assert.ok(result.suggestion!.includes('/usr'));
  });

  it('detects rm with variable followed by top-level directory /etc', () => {
    const result = detectDangerousRm('rm -rf ${ROOT}/etc');
    assert.equal(result.dangerous, true);
    assert.ok(result.suggestion!.includes('/etc'));
  });

  it('detects rm with variable followed by /bin', () => {
    const result = detectDangerousRm('rm -rf $BASE/bin');
    assert.equal(result.dangerous, true);
  });

  it('detects rm with variable followed by /home', () => {
    const result = detectDangerousRm('rm -rf $BASE/home');
    assert.equal(result.dangerous, true);
  });

  it('allows rm with variable followed by non-top-level directory', () => {
    const result = detectDangerousRm('rm -rf $BASE/myapp');
    assert.equal(result.dangerous, true);
    assert.ok(!result.suggestion!.includes('top-level'));
  });
});
