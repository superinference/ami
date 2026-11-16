import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  isEnvTruthy,
  getPrivacyLevel,
  isTelemetryDisabled,
  isEssentialTrafficOnly,
  getEssentialTrafficOnlyReason,
  getPrivacyPosture,
  PRIVACY_LEVELS,
  ESSENTIAL_TRAFFIC_VARS,
  type PrivacyLevel,
} from '../src/privacy-level';

describe('privacy-level canonical module', () => {
  it('re-exports the same resolver through ./analytics (single source of truth, no duplicate impl)', async () => {
    const analytics = await import('../src/analytics');
    assert.equal(analytics.getPrivacyLevel, getPrivacyLevel);
    assert.equal(analytics.isTelemetryDisabled, isTelemetryDisabled);
    assert.equal(analytics.isEssentialTrafficOnly, isEssentialTrafficOnly);
    assert.equal(analytics.getEssentialTrafficOnlyReason, getEssentialTrafficOnlyReason);
    assert.equal(analytics.getPrivacyPosture, getPrivacyPosture);
    assert.equal(analytics.isEnvTruthy, isEnvTruthy);
    assert.deepEqual([...analytics.PRIVACY_LEVELS], [...PRIVACY_LEVELS]);
    assert.deepEqual([...analytics.ESSENTIAL_TRAFFIC_VARS], [...ESSENTIAL_TRAFFIC_VARS]);
  });

  it('exposes the ordered tier list and essential-traffic vars', () => {
    assert.deepEqual([...PRIVACY_LEVELS], ['default', 'no-telemetry', 'essential-traffic']);
    assert.deepEqual([...ESSENTIAL_TRAFFIC_VARS], [
      'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
      'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC',
    ]);
  });
});

describe('isEnvTruthy', () => {
  it('treats set non-falsey values as true', () => {
    for (const v of ['1', 'true', 'yes', 'on', 'TRUE', ' x ']) assert.equal(isEnvTruthy(v), true, v);
  });
  it('treats unset/empty/falsey values as false', () => {
    for (const v of [undefined, '', '0', 'false', 'no', 'off', 'OFF', '  ']) {
      assert.equal(isEnvTruthy(v), false, String(v));
    }
  });
});

describe('getPrivacyLevel', () => {
  it('defaults to "default" when no opt-out var is set', () => {
    assert.equal(getPrivacyLevel({}), 'default');
    assert.equal(isTelemetryDisabled({}), false);
    assert.equal(isEssentialTrafficOnly({}), false);
  });

  it('DISABLE_TELEMETRY / DO_NOT_TRACK raise to no-telemetry', () => {
    assert.equal(getPrivacyLevel({ DISABLE_TELEMETRY: '1' }), 'no-telemetry');
    assert.equal(getPrivacyLevel({ DO_NOT_TRACK: '1' }), 'no-telemetry');
    assert.equal(isTelemetryDisabled({ DO_NOT_TRACK: '1' }), true);
    assert.equal(isEssentialTrafficOnly({ DISABLE_TELEMETRY: '1' }), false);
  });

  it('nonessential-traffic flag wins as the MOST restrictive signal', () => {
    const env = { DISABLE_TELEMETRY: '1', SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' };
    assert.equal(getPrivacyLevel(env), 'essential-traffic');
    assert.equal(isEssentialTrafficOnly(env), true);
    assert.equal(isTelemetryDisabled(env), true);
    assert.equal(getPrivacyLevel({ CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1' }), 'essential-traffic');
  });

  it('ignores falsey values of opt-out vars', () => {
    assert.equal(getPrivacyLevel({ DISABLE_TELEMETRY: '0', SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: 'false' }), 'default');
  });
});

describe('getEssentialTrafficOnlyReason', () => {
  it('returns null when nonessential traffic is allowed', () => {
    assert.equal(getEssentialTrafficOnlyReason({}), null);
    assert.equal(getEssentialTrafficOnlyReason({ DISABLE_TELEMETRY: '1', DO_NOT_TRACK: '1' }), null);
    assert.equal(getEssentialTrafficOnlyReason({ SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '0' }), null);
  });

  it('names the responsible env var (AMI-native first, then the Claude alias)', () => {
    assert.equal(
      getEssentialTrafficOnlyReason({ SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' }),
      'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
    );
    assert.equal(
      getEssentialTrafficOnlyReason({ CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: 'true' }),
      'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC',
    );
    assert.equal(
      getEssentialTrafficOnlyReason({
        SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
        CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1',
      }),
      'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
    );
  });

  it('is consistent with isEssentialTrafficOnly (reason iff restricted)', () => {
    for (const env of [
      {},
      { DISABLE_TELEMETRY: '1' },
      { SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' },
      { CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1' },
    ] as NodeJS.ProcessEnv[]) {
      assert.equal(getEssentialTrafficOnlyReason(env) !== null, isEssentialTrafficOnly(env));
    }
  });
});

describe('getPrivacyPosture (introspectable snapshot)', () => {
  it('reports a fully-open posture by default', () => {
    assert.deepEqual(getPrivacyPosture({}), {
      level: 'default',
      telemetryDisabled: false,
      essentialTrafficOnly: false,
      essentialTrafficOnlyReason: null,
    });
  });

  it('reports the no-telemetry posture', () => {
    assert.deepEqual(getPrivacyPosture({ DO_NOT_TRACK: '1' }), {
      level: 'no-telemetry',
      telemetryDisabled: true,
      essentialTrafficOnly: false,
      essentialTrafficOnlyReason: null,
    });
  });

  it('reports the essential-traffic posture with the responsible env var', () => {
    assert.deepEqual(getPrivacyPosture({ SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC: '1' }), {
      level: 'essential-traffic',
      telemetryDisabled: true,
      essentialTrafficOnly: true,
      essentialTrafficOnlyReason: 'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
    });
  });

  it('every reported level is a member of PRIVACY_LEVELS', () => {
    for (const env of [{}, { DISABLE_TELEMETRY: '1' }, { CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC: '1' }]) {
      const level: PrivacyLevel = getPrivacyPosture(env).level;
      assert.ok(PRIVACY_LEVELS.includes(level));
    }
  });
});
