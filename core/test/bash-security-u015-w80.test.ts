import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  validateDangerousRedirectTargets,
  validateBashSecurity,
  detectUnsafeWrapperFlags,
  CHECK_IDS,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// U015 / w80 — bash-security.ts parity additions
//
// 1. Dangerous (non-static) redirect-target validation that runs in EVERY mode,
//    closing the relaxed-detached hole where `> $VAR` / `> ~/x` / `> *.conf` /
//    `< $SECRET` targets were neither blocked nor path-validated.
//    (parity: the reference utils/bash/commands.ts hasDangerousExpansion)
// 2. Bounded-recursion safety cap on nested safe-wrapper prefixes.
//    (parity: the reference utils/bash/prefix.ts wrapperCount/recursionDepth caps)
// ---------------------------------------------------------------------------

describe('validateDangerousRedirectTargets (U015/w80)', () => {
  // ---- static targets stay allowed (essential to detached report workflows) ----
  for (const ok of [
    'echo data > report.txt',
    'echo data >> out/log.txt',
    'pytest > /tmp/results.txt 2>&1',
    'cat < notes.txt',
    'wc -l < ./input.csv',
    'echo hi > /dev/null',
    'echo plain text with no redirect',
  ]) {
    it(`allows static / no-op redirect: ${ok}`, () => {
      assert.equal(validateDangerousRedirectTargets(ok).safe, true);
    });
  }

  // ---- dangerous (non-static) output targets are flagged ----
  for (const bad of [
    'echo data > $OUTFILE',
    'echo data > ${DEST}',
    'echo data >> out/$LOG',
    'echo data > ~/notes.txt',
    'echo data > *.conf',
    'echo data > report-?.txt',
    'echo data > "$(mktemp)"',
    'echo data > `mktemp`',
    'echo data > {a,b}.txt',
  ]) {
    it(`flags dangerous output target: ${bad}`, () => {
      const r = validateDangerousRedirectTargets(bad);
      assert.equal(r.safe, false, bad);
      assert.equal(r.checkId, CHECK_IDS.DANGEROUS_REDIRECT_TARGET);
      assert.match(r.message ?? '', /output redirection target/);
    });
  }

  // ---- dangerous input targets are flagged too ----
  for (const bad of ['cat < $SECRET', 'wc -l < ~/.history', 'sort < *.list']) {
    it(`flags dangerous input target: ${bad}`, () => {
      const r = validateDangerousRedirectTargets(bad);
      assert.equal(r.safe, false, bad);
      assert.equal(r.checkId, CHECK_IDS.DANGEROUS_REDIRECT_TARGET);
      assert.match(r.message ?? '', /input redirection target/);
    });
  }
});

describe('validateBashSecurity — dangerous redirect in relaxed-detached mode (U015/w80)', () => {
  const relaxed = { detachedMode: true, relaxDetachedValidators: true } as const;

  it('relaxed-detached still allows a plain static output redirect', () => {
    // The whole point of relaxDetachedValidators: report / test-output capture.
    assert.equal(validateBashSecurity('pytest -q > results.txt', relaxed).safe, true);
  });

  it('relaxed-detached BLOCKS a variable-expansion output target (red-green anchor)', () => {
    // Without validateDangerousRedirectTargets this is `safe:true` in relaxed mode
    // because validateRedirections is skipped — this assertion fails on revert.
    const r = validateBashSecurity('echo pwned > $OUTFILE', relaxed);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DANGEROUS_REDIRECT_TARGET);
  });

  it('relaxed-detached BLOCKS a tilde output target', () => {
    const r = validateBashSecurity('echo pwned > ~/notes.txt', relaxed);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DANGEROUS_REDIRECT_TARGET);
  });

  it('relaxed-detached BLOCKS a glob output target', () => {
    const r = validateBashSecurity('echo pwned > *.conf', relaxed);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DANGEROUS_REDIRECT_TARGET);
  });

  it('relaxed-detached BLOCKS a variable input redirect', () => {
    const r = validateBashSecurity('cat < $SECRET', relaxed);
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DANGEROUS_REDIRECT_TARGET);
  });

  it('interactive (non-relaxed) still blocks ANY redirect (no regression)', () => {
    // validateRedirections fires first here; behavior is unchanged.
    const stat = validateBashSecurity('echo data > report.txt');
    assert.equal(stat.safe, false);
    const dyn = validateBashSecurity('echo data > $OUTFILE');
    assert.equal(dyn.safe, false);
  });
});

describe('safe-wrapper nesting cap (U015/w80)', () => {
  it('allows a realistic multi-wrapper chain', () => {
    assert.equal(
      detectUnsafeWrapperFlags('nohup timeout 5 nice -n5 stdbuf -oL ls').blocked,
      false,
    );
  });

  it('allows a chain right at the cap boundary', () => {
    const cmd = Array(10).fill('env').join(' ') + ' ls';
    assert.equal(detectUnsafeWrapperFlags(cmd).blocked, false);
  });

  it('blocks a pathological over-nested wrapper prefix hiding a command', () => {
    const cmd = Array(11).fill('env').join(' ') + ' ls';
    const r = detectUnsafeWrapperFlags(cmd);
    assert.equal(r.blocked, true);
    assert.match(r.reason ?? '', /excessive safe-wrapper nesting/);
  });

  it('does not flag a long wrapper run with nothing after it (hides nothing)', () => {
    const cmd = Array(20).fill('env').join(' ');
    assert.equal(detectUnsafeWrapperFlags(cmd).blocked, false);
  });
});
