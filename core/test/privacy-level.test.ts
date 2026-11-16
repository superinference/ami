import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { getEssentialTrafficOnlyReason } from '../src/privacy-level';
import { getPrivacyLevel, isEssentialTrafficOnly, ESSENTIAL_TRAFFIC_VARS } from '../src/analytics';

describe('getEssentialTrafficOnlyReason', () => {
  it('returns null when nonessential traffic is not suppressed', () => {
    assert.equal(getEssentialTrafficOnlyReason({}), null);
    // no-telemetry is not essential-traffic → still unrestricted for egress
    assert.equal(getEssentialTrafficOnlyReason({ DISABLE_TELEMETRY: '1' }), null);
    assert.equal(getEssentialTrafficOnlyReason({ DO_NOT_TRACK: '1' }), null);
  });

  it('returns the AMI-native knob when it is set', () => {
    const env = { SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' };
    assert.equal(getEssentialTrafficOnlyReason(env), 'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC');
    // consistent with the canonical resolver
    assert.equal(isEssentialTrafficOnly(env), true);
    assert.equal(getPrivacyLevel(env), 'essential-traffic');
  });

  it('returns the Claude Code compat alias when only it is set', () => {
    assert.equal(
      getEssentialTrafficOnlyReason({ CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: 'yes' }),
      'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC',
    );
  });

  it('prefers the AMI-native knob when both are set (resolution order)', () => {
    assert.equal(
      getEssentialTrafficOnlyReason({
        SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
        CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
      }),
      'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
    );
  });

  it('treats falsy env values as unset (no restriction)', () => {
    for (const v of ['', '0', 'false', 'no', 'off']) {
      assert.equal(
        getEssentialTrafficOnlyReason({ SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: v }),
        null,
        `value ${JSON.stringify(v)} must be treated as unset`,
      );
    }
  });

  it('exposes the resolution-ordered var list, AMI-native first', () => {
    assert.deepEqual(
      [...ESSENTIAL_TRAFFIC_VARS],
      ['SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC', 'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC'],
    );
  });
});
