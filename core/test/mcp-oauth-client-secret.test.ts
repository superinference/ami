import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { exchangeCode, refreshToken } from '../src/mcp/oauth';
import type { FetchLike } from '../src/mcp/oauth';

// ---------------------------------------------------------------------------
// H01: confidential-client support — a clientSecret, when supplied, is sent in
// the token/refresh form bodies; when absent, client_secret is never included
// (public PKCE client stays unchanged).
// ---------------------------------------------------------------------------

const NOW = 1_700_000_000_000;

function capture(): { fn: FetchLike; bodies: string[] } {
  const bodies: string[] = [];
  const fn: FetchLike = async (_url, init) => {
    bodies.push(String((init as any)?.body ?? ''));
    return { ok: true, status: 200, json: async () => ({ access_token: 'AT', token_type: 'Bearer' }) };
  };
  return { fn, bodies };
}

describe('OAuth confidential client secret (H01)', () => {
  it('includes client_secret in the authorization_code exchange when set', async () => {
    const { fn, bodies } = capture();
    await exchangeCode(fn, { tokenEndpoint: 'https://as/token', code: 'c', verifier: 'v', clientId: 'cid', redirectUri: 'r', resource: 'rs', clientSecret: 'SECRET' }, NOW);
    const form = new URLSearchParams(bodies[0]);
    assert.equal(form.get('client_secret'), 'SECRET');
    assert.equal(form.get('grant_type'), 'authorization_code');
  });

  it('includes client_secret in the refresh exchange when set', async () => {
    const { fn, bodies } = capture();
    await refreshToken(fn, { tokenEndpoint: 'https://as/token', refreshToken: 'RT', clientId: 'cid', resource: 'rs', clientSecret: 'SECRET' }, NOW);
    const form = new URLSearchParams(bodies[0]);
    assert.equal(form.get('client_secret'), 'SECRET');
    assert.equal(form.get('grant_type'), 'refresh_token');
  });

  it('omits client_secret for a public (PKCE-only) client', async () => {
    const { fn, bodies } = capture();
    await exchangeCode(fn, { tokenEndpoint: 'https://as/token', code: 'c', verifier: 'v', clientId: 'cid', redirectUri: 'r', resource: 'rs' }, NOW);
    assert.equal(new URLSearchParams(bodies[0]).has('client_secret'), false);
  });

  it('omits client_secret when the value is an empty string', async () => {
    const { fn, bodies } = capture();
    await refreshToken(fn, { tokenEndpoint: 'https://as/token', refreshToken: 'RT', clientId: 'cid', resource: 'rs', clientSecret: '' }, NOW);
    assert.equal(new URLSearchParams(bodies[0]).has('client_secret'), false);
  });
});
