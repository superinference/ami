/**
 * Unit tests (U019/w108) for the subprocess-env ISOLATION introspection surface.
 * AMI strips credential-bearing vars from every child process spawned by the bash
 * tool (sanitizeEnvForSubprocess) and merges explicit /env overrides in.
 * describeSubprocessEnvIsolation() surfaces THAT policy so it is discoverable via
 * `ami --introspect` (introspection-on-every-change rule / parity with the reference
 * Code's tmuxSocket isolation-state getters).
 *
 * These tests also lock the introspection view to the REAL stripping behavior, so
 * the two can never silently drift apart.
 *
 * Run: cd core/core && node --import tsx --test test/subprocess-env-isolation-u019-w108.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  describeSubprocessEnvIsolation,
  sanitizeEnvForSubprocess,
} from '../src/tools/bash';

describe('describeSubprocessEnvIsolation', () => {
  const view = describeSubprocessEnvIsolation();

  it('advertises the exact credential vars that are actually stripped', () => {
    assert.ok(Array.isArray(view.strippedExact));
    for (const want of ['ANTHROPIC_API_KEY', 'OPENAI_API_KEY', 'GITHUB_TOKEN', 'AWS_SECRET_ACCESS_KEY']) {
      assert.ok(view.strippedExact.includes(want), `must advertise stripping ${want}`);
    }
    // The advertised exact list is sorted (stable, diffable manifest output).
    const sorted = [...view.strippedExact].sort();
    assert.deepEqual(view.strippedExact, sorted);
  });

  it('every advertised exact var is in fact stripped by sanitizeEnvForSubprocess', () => {
    const input: Record<string, string> = { PATH: '/usr/bin', HOME: '/home/x' };
    for (const name of view.strippedExact) input[name] = 'secret';
    const out = sanitizeEnvForSubprocess(input);
    for (const name of view.strippedExact) {
      assert.ok(!(name in out), `${name} is advertised as stripped but survived sanitization`);
    }
    // Benign vars must be kept (the isolation only strips credentials).
    assert.equal(out.PATH, '/usr/bin');
    assert.equal(out.HOME, '/home/x');
  });

  it('advertises the suffix patterns that also mark a var sensitive', () => {
    assert.ok(view.strippedSuffixPatterns.some(p => /_TOKEN/.test(p)));
    assert.ok(view.strippedSuffixPatterns.some(p => /_KEY/.test(p)));
    // A var matching an advertised suffix is really stripped.
    const out = sanitizeEnvForSubprocess({ MY_CUSTOM_TOKEN: 'x', PATH: '/bin' });
    assert.ok(!('MY_CUSTOM_TOKEN' in out));
  });

  it('reports the safe-var exclusions and the structural policy flags', () => {
    assert.ok(view.safeVarPatterns.length > 0);
    assert.equal(view.scrubsInputPrefixDuplicates, true);
    assert.equal(view.sessionEnvOverridesApplied, true);
    assert.equal(typeof view.note, 'string');
    assert.ok(view.note.length > 0);
    // INPUT_<NAME> duplicates (GitHub Actions) are really scrubbed as the base var.
    const out = sanitizeEnvForSubprocess({ INPUT_ANTHROPIC_API_KEY: 'x', PATH: '/bin' });
    assert.ok(!('INPUT_ANTHROPIC_API_KEY' in out), 'INPUT_ prefixed secret must be scrubbed');
  });
});
