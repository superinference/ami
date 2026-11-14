import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  is1mContextDisabled,
  supportsOneMillionContextBeta,
  getContextWindow,
} from '../src/model-capabilities';

describe('1M-context kill-switch', () => {
  afterEach(() => { delete process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT; });

  it('is off by default', () => {
    assert.equal(is1mContextDisabled(), false);
    assert.equal(supportsOneMillionContextBeta('claude-sonnet-5'), true);
    assert.equal(getContextWindow('claude-sonnet-5'), 1_000_000);
  });

  for (const val of ['1', 'true', 'yes', 'TRUE']) {
    it(`honors SUPERINFERENCE_DISABLE_1M_CONTEXT=${val}`, () => {
      process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT = val;
      assert.equal(is1mContextDisabled(), true);
      // Beta-eligible model no longer advertises the 1M window...
      assert.equal(supportsOneMillionContextBeta('claude-sonnet-5'), false);
      // ...and its local budget falls back to the 200k table key.
      assert.equal(getContextWindow('claude-sonnet-4'), 200000);
    });
  }

  it('ignores an explicit [1m] id marker when disabled', () => {
    process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT = '1';
    // claude-sonnet-4[1m] would otherwise be forced to 1M by the marker.
    assert.equal(getContextWindow('claude-sonnet-4[1m]'), 200000);
  });

  it('treats an empty / falsey value as not-disabled', () => {
    process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT = '0';
    assert.equal(is1mContextDisabled(), false);
    process.env.SUPERINFERENCE_DISABLE_1M_CONTEXT = '';
    assert.equal(is1mContextDisabled(), false);
  });
});
