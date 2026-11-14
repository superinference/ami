import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  isPinnedVariable,
  getPinCategory,
  CODE_LOADING_VARIABLES,
  CODE_LOADING_PREFIXES,
  isNetworkTrustEnvVar,
  NETWORK_TRUST_VARS,
  classifyUntrustedEnvVar,
} from '../src/env-protection';

// ---------------------------------------------------------------------------
// gap: code-loading pin set omits per-language module-loading vectors
// (PYTHONPATH/NODE_PATH/CLASSPATH/RUBYLIB/GOFLAGS/RUSTFLAGS/PERL5LIB/PERLLIB)
// ---------------------------------------------------------------------------
describe('CODE_LOADING_VARIABLES — per-language module/code-loading vectors', () => {
  for (const v of ['PYTHONPATH', 'NODE_PATH', 'CLASSPATH', 'RUBYLIB', 'GOFLAGS', 'RUSTFLAGS', 'PERL5LIB', 'PERLLIB']) {
    it(`pins ${v}`, () => {
      assert.equal(CODE_LOADING_VARIABLES.has(v), true);
      assert.equal(isPinnedVariable(v), true);
      assert.equal(getPinCategory(v), 'code_loading');
      assert.equal(isPinnedVariable(v.toLowerCase()), true);
    });
  }
});

// ---------------------------------------------------------------------------
// gap: prefix matching for the LD_*/DYLD_* code-loading family (binary hijack)
// ---------------------------------------------------------------------------
describe('LD_*/DYLD_* prefix pinning', () => {
  it('exposes the code-loading prefixes', () => {
    assert.deepEqual([...CODE_LOADING_PREFIXES], ['LD_', 'DYLD_']);
  });

  it('pins novel LD_* / DYLD_* vars not in the explicit set', () => {
    // Not enumerated in CODE_LOADING_VARIABLES, but caught by the prefix.
    for (const v of ['LD_PROFILE', 'LD_BIND_NOW', 'DYLD_VERSIONED_LIBRARY_PATH', 'DYLD_ROOT_PATH']) {
      assert.equal(CODE_LOADING_VARIABLES.has(v), false, `${v} should not be enumerated`);
      assert.equal(isPinnedVariable(v), true, `${v} should be pinned via prefix`);
      assert.equal(getPinCategory(v), 'code_loading');
    }
  });

  it('is case-insensitive on the prefix', () => {
    assert.equal(isPinnedVariable('ld_preload_extra'), true);
    assert.equal(getPinCategory('dyld_shared_region'), 'code_loading');
  });

  it('does not over-match unrelated names that merely contain LD/DYLD', () => {
    assert.equal(isPinnedVariable('BUILD_DIR'), false);
    assert.equal(isPinnedVariable('MYDYLD'), false);
    assert.equal(getPinCategory('OLD_VALUE'), null);
  });
});

// ---------------------------------------------------------------------------
// gap: network-redirect / TLS-trust env vars classified as dangerous
// ---------------------------------------------------------------------------
describe('isNetworkTrustEnvVar', () => {
  it('flags TLS-trust and proxy-redirect vars', () => {
    for (const v of [
      'NODE_TLS_REJECT_UNAUTHORIZED',
      'NODE_EXTRA_CA_CERTS',
      'HTTP_PROXY',
      'HTTPS_PROXY',
      'NO_PROXY',
      'ALL_PROXY',
    ]) {
      assert.equal(NETWORK_TRUST_VARS.has(v), true);
      assert.equal(isNetworkTrustEnvVar(v), true, `${v} should be network-trust`);
      assert.equal(isNetworkTrustEnvVar(v.toLowerCase()), true, `${v.toLowerCase()} should be network-trust`);
    }
  });

  it('flags OTEL OTLP endpoint exfiltration vars by prefix+suffix', () => {
    assert.equal(isNetworkTrustEnvVar('OTEL_EXPORTER_OTLP_ENDPOINT'), true);
    assert.equal(isNetworkTrustEnvVar('OTEL_EXPORTER_OTLP_LOGS_ENDPOINT'), true);
    assert.equal(isNetworkTrustEnvVar('OTEL_EXPORTER_OTLP_METRICS_ENDPOINT'), true);
    // An OTEL var that is not an endpoint is not network-trust.
    assert.equal(isNetworkTrustEnvVar('OTEL_EXPORTER_OTLP_PROTOCOL'), false);
  });

  it('does not flag safe/unrelated vars', () => {
    assert.equal(isNetworkTrustEnvVar('OPENAI_API_KEY'), false);
    assert.equal(isNetworkTrustEnvVar('SOME_ENDPOINT'), false);
    assert.equal(isNetworkTrustEnvVar('HOME'), false);
  });
});

// ---------------------------------------------------------------------------
// gap: single canonical dangerous-env classifier (classifyUntrustedEnvVar)
// ---------------------------------------------------------------------------
describe('classifyUntrustedEnvVar', () => {
  it('returns "pinned" for code-loading/policy/telemetry vars', () => {
    assert.equal(classifyUntrustedEnvVar('LD_PRELOAD'), 'pinned');
    assert.equal(classifyUntrustedEnvVar('PYTHONPATH'), 'pinned');
    assert.equal(classifyUntrustedEnvVar('DO_NOT_TRACK'), 'pinned');
  });

  it('returns "network-trust" for TLS/proxy/OTEL vars', () => {
    assert.equal(classifyUntrustedEnvVar('NODE_TLS_REJECT_UNAUTHORIZED'), 'network-trust');
    assert.equal(classifyUntrustedEnvVar('NODE_EXTRA_CA_CERTS'), 'network-trust');
    assert.equal(classifyUntrustedEnvVar('HTTPS_PROXY'), 'network-trust');
  });

  it('returns "provider-managed" for routing/credential vars ONLY when host owns routing', () => {
    assert.equal(classifyUntrustedEnvVar('ANTHROPIC_BASE_URL'), null);
    assert.equal(classifyUntrustedEnvVar('OPENAI_API_KEY'), null);
    assert.equal(classifyUntrustedEnvVar('ANTHROPIC_BASE_URL', { hostManagedProvider: true }), 'provider-managed');
    assert.equal(classifyUntrustedEnvVar('OPENAI_API_KEY', { hostManagedProvider: true }), 'provider-managed');
  });

  it('returns null for ordinary/safe vars', () => {
    assert.equal(classifyUntrustedEnvVar('MY_APP_FLAG'), null);
    assert.equal(classifyUntrustedEnvVar('DATABASE_URL'), null);
    assert.equal(classifyUntrustedEnvVar('DATABASE_URL', { hostManagedProvider: true }), null);
  });

  it('pinned takes precedence over network-trust / provider-managed', () => {
    // NODE_OPTIONS is pinned (code-loading); must classify as pinned, not otherwise.
    assert.equal(classifyUntrustedEnvVar('NODE_OPTIONS', { hostManagedProvider: true }), 'pinned');
  });
});
