import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  COMMAND_SPECS,
  getCommandSpec,
  buildWrapperValueFlagPattern,
  type CommandSpec,
} from '../src/tools/bash-command-specs.ts';
import {
  WRAPPER_VALUE_FLAGS,
  SRUN_VALUE_FLAGS,
  validateBashSecurity,
} from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U273 — declarative per-command spec registry (parity: the reference
// src/utils/bash/registry.ts + specs/*). A typed CommandSpec/Option vocabulary
// models each wrapper's option-argument arity; the security wrapper-unwrapping
// derives its value-flag matchers from it instead of hand-copied regex literals.
// ===========================================================================

describe('U273: bash-command-specs declarative registry', () => {
  describe('getCommandSpec', () => {
    it('resolves a known command to its spec', () => {
      const spec = getCommandSpec('srun');
      assert.equal(spec?.name, 'srun');
      assert.equal(spec?.description, 'Run a command on SLURM cluster nodes');
    });

    it('returns undefined for an unknown command', () => {
      assert.equal(getCommandSpec('definitely-not-a-spec'), undefined);
    });

    it('every registry entry is reachable by name', () => {
      for (const spec of COMMAND_SPECS) {
        assert.equal(getCommandSpec(spec.name), spec);
      }
    });
  });

  describe('typed argument / option vocabulary (parity fields)', () => {
    it('marks a wrapped command argument with isCommand', () => {
      const srun = getCommandSpec('srun') as CommandSpec;
      assert.equal((srun.args as { isCommand?: boolean }).isCommand, true);
    });

    it('models timeout as [duration (required), command (isCommand)]', () => {
      const timeout = getCommandSpec('timeout') as CommandSpec;
      const args = timeout.args as Array<{ name?: string; isOptional?: boolean; isCommand?: boolean }>;
      assert.equal(args[0].isOptional, false);
      assert.equal(args[1].isCommand, true);
    });

    it('models variadic/optional args (alias definition, pyright files)', () => {
      const alias = getCommandSpec('alias') as CommandSpec;
      const aArg = alias.args as { isVariadic?: boolean; isOptional?: boolean };
      assert.equal(aArg.isVariadic, true);
      assert.equal(aArg.isOptional, true);
      const pyright = getCommandSpec('pyright') as CommandSpec;
      const pArg = pyright.args as { isVariadic?: boolean; isOptional?: boolean };
      assert.equal(pArg.isVariadic, true);
      assert.equal(pArg.isOptional, true);
    });
  });

  describe('buildWrapperValueFlagPattern', () => {
    // The derived matcher must reproduce the previously hand-authored regex
    // literals EXACTLY — this locks the consolidation (no drift from the old
    // behavior) for the wrappers the security layer consumes.
    const expected: Record<string, string> = {
      srun: '^(-n|--ntasks|-N|--nodes)$',
      timeout: '^(-k|--kill-after|-s|--signal)$',
      stdbuf: '^(-i|--input|-o|--output|-e|--error)$',
      nice: '^(-n|--adjustment)$',
      env: '^(-u|--unset)$',
    };
    for (const [name, source] of Object.entries(expected)) {
      it(`derives ${name}'s value-flag pattern from the spec`, () => {
        assert.equal(buildWrapperValueFlagPattern(name)?.source, source);
      });
    }

    it('returns undefined for an unknown command', () => {
      assert.equal(buildWrapperValueFlagPattern('definitely-not-a-spec'), undefined);
    });

    it('returns undefined for a command with no options (nohup)', () => {
      assert.equal(buildWrapperValueFlagPattern('nohup'), undefined);
    });

    it('returns undefined when every option is value-less (time -p)', () => {
      assert.equal(buildWrapperValueFlagPattern('time'), undefined);
    });

    it('includes string-named value options and excludes value-less ones (pyright)', () => {
      const pat = buildWrapperValueFlagPattern('pyright');
      assert.ok(pat, 'pyright has value-taking options');
      // string-named value option (--createstub takes an IMPORT operand)
      assert.ok(pat!.test('--createstub'));
      assert.ok(pat!.test('--pythonpath'));
      // array-named value option (--project / -p)
      assert.ok(pat!.test('--project'));
      assert.ok(pat!.test('-p'));
      // value-less flags are excluded
      assert.ok(!pat!.test('--verbose'));
      assert.ok(!pat!.test('--outputjson'));
      assert.ok(!pat!.test('-h'));
    });
  });
});

describe('U273: bash-security consumes the declarative spec registry', () => {
  it('WRAPPER_VALUE_FLAGS entries are the spec-derived patterns', () => {
    for (const wrapper of ['timeout', 'stdbuf', 'nice', 'env'] as const) {
      assert.equal(
        WRAPPER_VALUE_FLAGS[wrapper].source,
        buildWrapperValueFlagPattern(wrapper)?.source,
      );
    }
  });

  it('SRUN_VALUE_FLAGS is the spec-derived srun pattern', () => {
    assert.equal(SRUN_VALUE_FLAGS.source, buildWrapperValueFlagPattern('srun')?.source);
    assert.ok(SRUN_VALUE_FLAGS.test('--ntasks'));
    assert.ok(SRUN_VALUE_FLAGS.test('-N'));
    assert.ok(!SRUN_VALUE_FLAGS.test('--mem'));
  });

  // Integration: the derived srun pattern drives real wrapper-unwrapping, so a
  // dangerous inner command behind srun's value options is still classified on
  // the INNER command (the count operand is not mistaken for the command).
  it('`srun -n 4 mkfs /dev/sda` unwraps the count operand and blocks mkfs', () => {
    const r = validateBashSecurity('srun -n 4 mkfs /dev/sda');
    assert.equal(r.safe, false);
    assert.match(r.message, /mkfs/);
  });

  it('`srun --nodes 2 mount /mnt` unwraps the count operand and blocks mount', () => {
    assert.equal(validateBashSecurity('srun --nodes 2 mount /mnt').safe, false);
  });

  it('a safe inner command behind srun value options stays safe', () => {
    assert.equal(validateBashSecurity('srun -n 4 echo hi').safe, true);
  });

  // The timeout value-flag consolidation still skips a spaced signal operand so
  // the wrapped command is reached (would mis-read `KILL` as the command without
  // the derived WRAPPER_VALUE_FLAGS.timeout pattern).
  it('`timeout -s KILL 5 mkfs /dev/sda` skips the signal operand and blocks mkfs', () => {
    assert.equal(validateBashSecurity('timeout -s KILL 5 mkfs /dev/sda').safe, false);
  });
});
