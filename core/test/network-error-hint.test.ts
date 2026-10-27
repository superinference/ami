import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { classifyError, friendlyErrorMessage, networkErrorHint } from '../src/error-classifier';

// ---------------------------------------------------------------------------
// Richer connection-drop diagnostics — proxy/gateway named before VPN/firewall
// ---------------------------------------------------------------------------

describe('networkErrorHint — proxy/gateway-first diagnosis', () => {
  it('names proxy/gateway before VPN/firewall for a generic drop', () => {
    const hint = networkErrorHint('econnrefused 127.0.0.1:443');
    assert.ok(hint.includes('proxy/gateway'), 'hint must mention proxy/gateway');
    assert.ok(hint.includes('VPN/firewall'), 'hint must mention VPN/firewall');
    assert.ok(
      hint.indexOf('proxy/gateway') < hint.indexOf('VPN/firewall'),
      'proxy/gateway must be suggested before VPN/firewall',
    );
    // Still recognizable as a network error (preserves existing contract).
    assert.ok(hint.includes('Network error'));
  });

  it('gives a DNS-specific lead for enotfound / getaddrinfo / dns', () => {
    for (const raw of ['enotfound api.example.com', 'getaddrinfo failed', 'dns resolution error']) {
      const hint = networkErrorHint(raw);
      assert.ok(hint.includes('DNS lookup failed'), `DNS lead missing for: ${raw}`);
      assert.ok(hint.indexOf('proxy/gateway') < hint.indexOf('VPN/firewall'));
    }
  });

  it('gives a timeout-specific lead for etimedout / timed out', () => {
    for (const raw of ['etimedout connecting', 'connection timed out']) {
      const hint = networkErrorHint(raw);
      assert.ok(hint.includes('timed out'), `timeout lead missing for: ${raw}`);
      assert.ok(hint.indexOf('proxy/gateway') < hint.indexOf('VPN/firewall'));
    }
  });
});

describe('friendlyErrorMessage — enriched network_error', () => {
  it('emits the proxy/gateway-first hint for a classified network error', () => {
    const classified = classifyError('ECONNREFUSED 127.0.0.1:443');
    assert.equal(classified.category, 'network_error');
    const msg = friendlyErrorMessage(classified);
    // Discriminating: the old static message did NOT mention proxy/gateway.
    assert.ok(msg.includes('proxy/gateway'), 'enriched message must point at proxy/gateway');
    assert.ok(msg.indexOf('proxy/gateway') < msg.indexOf('VPN/firewall'));
    // Preserves the existing contract (recognizable, no raw error code).
    assert.ok(msg.includes('Network error'));
    assert.ok(!msg.includes('ECONNREFUSED'));
  });

  it('uses the DNS lead end-to-end for a name-resolution failure', () => {
    const classified = classifyError('getaddrinfo ENOTFOUND api.anthropic.com');
    assert.equal(classified.category, 'network_error');
    const msg = friendlyErrorMessage(classified);
    assert.ok(msg.includes('DNS lookup failed'));
  });
});
