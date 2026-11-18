/**
 * U010 / w106 — persistent `web_fetch` domain policy enforcement (parity gap [18]).
 *
 * The web_fetch tool reads the saved `webFetch` policy off the project config tier and
 * enforces it on EVERY call (distinct from the per-request allowed_domains/blocked_domains
 * params): blockedDomains always wins, allowedDomains is a baseline allow-list the request
 * can override with its own allowed_domains. All branches are PRE-NETWORK, so no socket is
 * opened (the probe host is 127.0.0.1:9 with _allowLocalhostForTesting).
 *
 * RED before the web-fetch.ts change: no saved policy is consulted, so a configured
 * blockedDomains host is NOT refused and a configured allowedDomains restriction is NOT
 * applied — the fetch falls through to the network instead of the policy error.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { webFetchTool } from '../src/tools/web-fetch';
import type { HttpGetResult } from '../src/tools/web-fetch';
import type { ToolContext } from '../src/types';

let tmp: string;

function writePolicy(policy: unknown): void {
  const dir = nodePath.join(tmp, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(nodePath.join(dir, 'config.json'), JSON.stringify({ webFetch: policy }));
}

// A stub HTTP getter so a "falls through to network" assertion never opens a real socket
// (and never incurs the retry/backoff delay of a refused connection). Returns a tiny
// text body — the policy branches we assert on run BEFORE this is ever called.
const stubHttpGet = async (url: string): Promise<HttpGetResult> => ({
  body: 'ok',
  statusCode: 200,
  contentType: 'text/plain',
  finalUrl: url,
});

function ctx(withNetwork = false): ToolContext {
  return {
    cwd: tmp,
    abortSignal: new AbortController().signal,
    _allowLocalhostForTesting: true,
    ...(withNetwork ? { _webFetchHttpGet: stubHttpGet } : {}),
  } as ToolContext;
}

beforeEach(() => {
  tmp = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'wf-policy-'));
});
afterEach(() => {
  try { fs.rmSync(tmp, { recursive: true, force: true }); } catch { /* best effort */ }
});

describe('web_fetch persistent domain policy — blockedDomains (gap [18])', () => {
  it('refuses a host matching the saved blockedDomains policy', async () => {
    writePolicy({ blockedDomains: ['127.0.0.1'] });
    const res = await webFetchTool.execute({ url: 'http://127.0.0.1:9/x' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /blocked by the web_fetch domain policy/);
  });

  it('deny wins: saved blockedDomains refuses even when the request allows the host', async () => {
    writePolicy({ blockedDomains: ['127.0.0.1'] });
    const res = await webFetchTool.execute(
      { url: 'http://127.0.0.1:9/x', allowed_domains: ['127.0.0.1'] },
      ctx(),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /blocked by the web_fetch domain policy/);
  });

  it('does not refuse a host that does not match the saved blockedDomains policy', async () => {
    writePolicy({ blockedDomains: ['example.com'] });
    const res = await webFetchTool.execute({ url: 'http://127.0.0.1:9/x' }, ctx(true));
    // It may still fail at the network layer, but NEVER via the domain-policy branch.
    assert.doesNotMatch(res.output, /web_fetch domain policy/);
  });
});

describe('web_fetch persistent domain policy — allowedDomains (gap [18])', () => {
  it('refuses a host not in the saved allowedDomains baseline', async () => {
    writePolicy({ allowedDomains: ['example.com'] });
    const res = await webFetchTool.execute({ url: 'http://127.0.0.1:9/x' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /not in the web_fetch allowed-domains policy/);
  });

  it('allows a host present in the saved allowedDomains baseline (falls through to network)', async () => {
    writePolicy({ allowedDomains: ['127.0.0.1'] });
    const res = await webFetchTool.execute({ url: 'http://127.0.0.1:9/x' }, ctx(true));
    assert.doesNotMatch(res.output, /allowed-domains policy/);
  });

  it('a per-request allowed_domains overrides the saved baseline allow-list', async () => {
    // Saved baseline would reject 127.0.0.1, but the request names it explicitly.
    writePolicy({ allowedDomains: ['example.com'] });
    const res = await webFetchTool.execute(
      { url: 'http://127.0.0.1:9/x', allowed_domains: ['127.0.0.1'] },
      ctx(true),
    );
    assert.doesNotMatch(res.output, /web_fetch allowed-domains policy/);
  });
});

describe('web_fetch persistent domain policy — no policy configured', () => {
  it('is a no-op when no webFetch config is present', async () => {
    const res = await webFetchTool.execute({ url: 'http://127.0.0.1:9/x' }, ctx(true));
    assert.doesNotMatch(res.output, /web_fetch domain policy/);
    assert.doesNotMatch(res.output, /web_fetch allowed-domains policy/);
  });
});
