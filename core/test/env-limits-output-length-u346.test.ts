import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { AMI_ENV_INT_LIMITS, collectEnvLimitDiagnostics } from '../src/utils/env';
import { BASH_MAX_OUTPUT_UPPER_LIMIT } from '../src/tools/bash';
import { DEFAULT_TASK_MAX_CHARS, TASK_MAX_OUTPUT_UPPER_LIMIT } from '../src/tools/spill-output';

/**
 * U346 — parity with the reference's Doctor env-validation surface (Doctor.tsx
 * L142-161 + utils/envValidation.ts + utils/shell/outputLimits.ts): Claude's
 * `/doctor` validates the OUTPUT-LENGTH env knobs (BASH_MAX_OUTPUT_LENGTH /
 * TASK_MAX_OUTPUT_LENGTH) through its bounded-int validator and surfaces any
 * over-cap / invalid override. AMI already validates these knobs at the enforcing
 * call sites (resolveBashMaxOutput / resolveTaskMaxChars) but the `/doctor`
 * env-limits catalog ({@link AMI_ENV_INT_LIMITS}) omitted them, so a misconfigured
 * output cap was never REPORTED to the operator. This test locks the two knobs
 * into the catalog with bounds that match the canonical enforcing constants.
 */
describe('U346: output-length env knobs in the /doctor env-limits catalog', () => {
  const byName = (name: string) => AMI_ENV_INT_LIMITS.find((s) => s.name === name);

  it('includes BASH_MAX_OUTPUT_LENGTH with bounds matching the bash enforcer (no drift)', () => {
    const spec = byName('BASH_MAX_OUTPUT_LENGTH');
    assert.ok(spec, 'BASH_MAX_OUTPUT_LENGTH must be in AMI_ENV_INT_LIMITS');
    // Advisory bounds must match the real cap applied by resolveBashMaxOutput so
    // the /doctor warning agrees with the value the tool actually enforces.
    assert.equal(spec!.upperLimit, BASH_MAX_OUTPUT_UPPER_LIMIT);
    assert.equal(spec!.defaultValue, BASH_MAX_OUTPUT_UPPER_LIMIT);
    assert.equal(spec!.min, 1);
  });

  it('includes TASK_MAX_OUTPUT_LENGTH with bounds matching the task enforcer (no drift)', () => {
    const spec = byName('TASK_MAX_OUTPUT_LENGTH');
    assert.ok(spec, 'TASK_MAX_OUTPUT_LENGTH must be in AMI_ENV_INT_LIMITS');
    assert.equal(spec!.upperLimit, TASK_MAX_OUTPUT_UPPER_LIMIT);
    assert.equal(spec!.defaultValue, DEFAULT_TASK_MAX_CHARS);
    assert.equal(spec!.min, 1);
  });

  it('flags an over-cap BASH_MAX_OUTPUT_LENGTH as capped', () => {
    const diags = collectEnvLimitDiagnostics(undefined, {
      BASH_MAX_OUTPUT_LENGTH: String(BASH_MAX_OUTPUT_UPPER_LIMIT + 1),
    });
    const d = diags.find((x) => x.name === 'BASH_MAX_OUTPUT_LENGTH');
    assert.ok(d, 'an over-cap BASH_MAX_OUTPUT_LENGTH must be reported');
    assert.equal(d!.status, 'capped');
    assert.equal(d!.effective, BASH_MAX_OUTPUT_UPPER_LIMIT);
  });

  it('flags a non-numeric TASK_MAX_OUTPUT_LENGTH as invalid and reports the default', () => {
    const diags = collectEnvLimitDiagnostics(undefined, {
      TASK_MAX_OUTPUT_LENGTH: 'not-a-number',
    });
    const d = diags.find((x) => x.name === 'TASK_MAX_OUTPUT_LENGTH');
    assert.ok(d, 'a non-numeric TASK_MAX_OUTPUT_LENGTH must be reported');
    assert.equal(d!.status, 'invalid');
    assert.equal(d!.effective, DEFAULT_TASK_MAX_CHARS);
  });

  it('reports nothing for an in-range output-length override', () => {
    const diags = collectEnvLimitDiagnostics(undefined, {
      BASH_MAX_OUTPUT_LENGTH: '50000',
      TASK_MAX_OUTPUT_LENGTH: '50000',
    });
    assert.ok(!diags.some((x) => x.name === 'BASH_MAX_OUTPUT_LENGTH'));
    assert.ok(!diags.some((x) => x.name === 'TASK_MAX_OUTPUT_LENGTH'));
  });

  it('emits each output-length finding to the injected debug log (logForDebugging parity)', () => {
    const lines: string[] = [];
    collectEnvLimitDiagnostics(undefined, {
      BASH_MAX_OUTPUT_LENGTH: String(BASH_MAX_OUTPUT_UPPER_LIMIT + 10),
    }, { log: (line) => lines.push(line) });
    assert.ok(lines.some((l) => /BASH_MAX_OUTPUT_LENGTH: capped/.test(l)));
  });
});
