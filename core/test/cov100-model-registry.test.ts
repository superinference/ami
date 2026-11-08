/**
 * Standalone cov-100 test for core/src/model-registry.ts.
 *
 *   cd core/core && node --import tsx --test test/cov100-model-registry.test.ts
 *
 * Targets the one branch arm the existing suite leaves open: the `EAI_AGAIN`
 * (DNS-again) alternative inside diagnoseModelDiscovery's connection-error
 * check (`code === 'ECONNREFUSED' || code === 'ENOTFOUND' || code ===
 * 'EAI_AGAIN'`). ECONNREFUSED (model-diagnostics.test.ts) and ENOTFOUND
 * (provider-cov.test.ts) are covered; the third arm is not.
 *
 * Core isolation: imports ONLY from ../src — no common/, cli/, or vscode/.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { diagnoseModelDiscovery } from '../src/model-registry';

describe('diagnoseModelDiscovery — EAI_AGAIN (transient DNS) branch', () => {
  it('classifies EAI_AGAIN as an unreachable endpoint and names the code', () => {
    // getaddrinfo EAI_AGAIN is Node's transient DNS-resolution failure. The
    // first two arms (ECONNREFUSED/ENOTFOUND) are false, so only the third arm
    // produces this message — exercising the otherwise-uncovered alternative.
    const err = Object.assign(new Error('getaddrinfo EAI_AGAIN models.example'), {
      code: 'EAI_AGAIN',
    });
    const msg = diagnoseModelDiscovery(err, 'openrouter');

    // Real behavior: the "could not reach" diagnostic naming provider + code.
    assert.match(msg, /Could not reach openrouter/);
    assert.match(msg, /\(EAI_AGAIN\)/);
    assert.match(msg, /Check the base URL and your network\./);
  });

  it('routes EAI_AGAIN to the same template as ECONNREFUSED / ENOTFOUND', () => {
    // All three network-code arms must yield the identical sentence shape with
    // only the code substituted — proving the EAI_AGAIN arm is not a divergent
    // fallthrough into the generic "Could not list models" message.
    const build = (code: string) =>
      diagnoseModelDiscovery(Object.assign(new Error('boom'), { code }), 'ollama');

    const again = build('EAI_AGAIN');
    const refused = build('ECONNREFUSED');
    const notfound = build('ENOTFOUND');

    assert.equal(again, 'Could not reach ollama (EAI_AGAIN). Check the base URL and your network.');
    assert.equal(refused, 'Could not reach ollama (ECONNREFUSED). Check the base URL and your network.');
    assert.equal(notfound, 'Could not reach ollama (ENOTFOUND). Check the base URL and your network.');

    // Same template, differing only by the parenthesized code.
    assert.equal(again.replace('EAI_AGAIN', 'X'), refused.replace('ECONNREFUSED', 'X'));
    assert.equal(again.replace('EAI_AGAIN', 'X'), notfound.replace('ENOTFOUND', 'X'));
  });

  it('does not treat EAI_AGAIN as the generic unknown-error fallback', () => {
    // Guard against regression where the EAI_AGAIN arm is dropped and the code
    // falls through to the generic `Could not list models for ... ` branch.
    const msg = diagnoseModelDiscovery(
      Object.assign(new Error('boom'), { code: 'EAI_AGAIN' }),
      'deepseek',
    );
    assert.doesNotMatch(msg, /Could not list models for/);
    assert.match(msg, /Could not reach deepseek/);
  });
});
