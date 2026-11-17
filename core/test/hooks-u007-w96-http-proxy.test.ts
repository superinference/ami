/**
 * U007 — gap: "Route HTTP hook requests through the sandbox / env-var proxy"
 * (ref utils/hooks/execHttpHook.ts:L14-41,L174-217 + the low "Proxy config knobs
 * for HTTP hooks (HTTP_PROXY/HTTPS_PROXY/NO_PROXY) + maxRedirects:0").
 *
 * Claude Code's execHttpHook routes the POST through a configured proxy and, in
 * that case, SKIPS the local SSRF/pinned-DNS guard: the proxy performs DNS for
 * the target and enforces its own egress allowlist (applying the guard would
 * instead validate the proxy's own — possibly private — address and wrongly block
 * a corporate proxy). AMI's runHttpHook had NO proxy support: it always used the
 * raw http/https client with pinned-DNS SSRF validation, so a hook could not be
 * routed through an HTTP(S)_PROXY at all.
 *
 * This adds the env-var proxy branch, reusing the canonical provider proxy helpers
 * (resolveProxyForUrl + withProxyDispatcher — no second proxy implementation).
 *
 * RED (revert the proxy branch in runHttpHook / runHttpHookViaProxy): the proxied
 * fetch is never called and the request falls through to the SSRF-guarded raw path
 * — so `fetchCalled` stays false and the SSRF validator IS consulted, failing the
 * routing + ssrf-skip assertions; the fail-closed-via-proxy deny also disappears.
 */
import { describe, it, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import * as net from 'node:net';
import { HookManager } from '../src/hooks';

const tmpDirs: string[] = [];
function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-proxy-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ } }
});
function preCtx(toolName: string) {
  return { messages: [], turnCount: 0, toolName, toolInput: {} } as any; // eslint-disable-line @typescript-eslint/no-explicit-any
}
function makeManager(dir: string): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  return hm;
}

// Preserve + restore the proxy env around each test so we never leak a proxy
// setting into the rest of the suite (and so the default no-proxy case is clean).
const PROXY_KEYS = ['HTTP_PROXY', 'http_proxy', 'HTTPS_PROXY', 'https_proxy', 'NO_PROXY', 'no_proxy'] as const;
let savedEnv: Record<string, string | undefined> = {};
beforeEach(() => {
  savedEnv = {};
  for (const k of PROXY_KEYS) { savedEnv[k] = process.env[k]; delete process.env[k]; }
});
afterEach(() => {
  for (const k of PROXY_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
});

describe('runHttpHook env-var proxy routing (gap: route HTTP hooks through the proxy)', () => {
  it('routes through the proxied fetch and SKIPS the SSRF guard when HTTP_PROXY is set', async () => {
    process.env.HTTP_PROXY = 'http://127.0.0.1:3128';
    // timeout:1 keeps the RED path (proxy branch removed → raw network request to a
    // non-loopback host) failing fast instead of blocking on the 10-min default.
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'http://webhook.example/h', timeout: 1 } }]);
    const hm = makeManager(dir);

    let ssrfCalled = false;
    hm.setUrlSafetyValidator(async () => { ssrfCalled = true; return { resolvedIP: '93.184.216.34' } as any; }); // eslint-disable-line @typescript-eslint/no-explicit-any

    let fetchCalled = false;
    let seenUrl = '';
    let seenRedirect: RequestRedirect | undefined;
    hm.setProxiedHookFetch((async (url: any, init: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
      fetchCalled = true;
      seenUrl = String(url);
      seenRedirect = init?.redirect;
      return new Response('{}', { status: 200 });
    }) as typeof fetch);

    const decision = await hm.executePreToolUse(preCtx('bash'));

    assert.equal(fetchCalled, true, 'the proxied fetch MUST be used when a proxy env var resolves');
    assert.equal(seenUrl, 'http://webhook.example/h', 'the hook URL is POSTed through the proxied fetch');
    assert.equal(seenRedirect, 'manual', 'redirect:manual preserves the maxRedirects:0 SSRF-bypass guard');
    assert.equal(ssrfCalled, false, 'the SSRF/pinned-DNS guard is skipped on the proxied path (proxy does DNS)');
    assert.equal(decision.action, 'allow', 'a 2xx proxied webhook with no decision allows the action');
  });

  it('fail-closed (onFailure:block): a non-2xx proxied response DENIES', async () => {
    process.env.HTTPS_PROXY = 'http://127.0.0.1:3128';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'https://webhook.example/h', onFailure: 'block' } }]);
    const hm = makeManager(dir);
    hm.setUrlSafetyValidator(async () => { throw new Error('SSRF guard must not run on the proxied path'); });
    hm.setProxiedHookFetch((async () => new Response('nope', { status: 500 })) as typeof fetch);

    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'deny', 'a 5xx from the proxied webhook fail-closes to deny');
  });

  it('does NOT route through the proxy when NO_PROXY matches the target host', async () => {
    process.env.HTTP_PROXY = 'http://127.0.0.1:3128';
    process.env.NO_PROXY = 'webhook.example';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'http://webhook.example/h' } }]);
    const hm = makeManager(dir);
    let ssrfCalled = false;
    hm.setUrlSafetyValidator(async () => { ssrfCalled = true; return { error: 'blocked for test' }; });
    let fetchCalled = false;
    hm.setProxiedHookFetch((async () => { fetchCalled = true; return new Response('{}', { status: 200 }); }) as typeof fetch);

    await hm.executePreToolUse(preCtx('bash'));
    assert.equal(fetchCalled, false, 'a NO_PROXY match must take the direct (SSRF-guarded) path, not the proxy');
    assert.equal(ssrfCalled, true, 'the direct path consults the SSRF guard');
  });

  it('no proxy env set → direct SSRF-guarded path (default, unchanged)', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'http://webhook.example/h' } }]);
    const hm = makeManager(dir);
    let ssrfCalled = false;
    hm.setUrlSafetyValidator(async () => { ssrfCalled = true; return { error: 'blocked for test' }; });
    let fetchCalled = false;
    hm.setProxiedHookFetch((async () => { fetchCalled = true; return new Response('{}', { status: 200 }); }) as typeof fetch);

    await hm.executePreToolUse(preCtx('bash'));
    assert.equal(fetchCalled, false, 'with no proxy env the proxied fetch is never used');
    assert.equal(ssrfCalled, true, 'the direct path consults the SSRF guard');
  });
});

/**
 * E2E: a REAL local CONNECT forward proxy + a REAL target HTTP server, with the
 * genuine default transport (undici ProxyAgent via withProxyDispatcher) — no seam.
 * Proves the whole wired path: the hook's POST tunnels through the proxy to the
 * target and the target's response body flows back to the hook result.
 */
describe('runHttpHook proxy routing — real forward proxy (E2E)', () => {
  it('tunnels the hook POST through a live CONNECT proxy to the target', async () => {
    // Target origin server the proxy tunnels to.
    let targetGotBody = '';
    const target = http.createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on('data', c => chunks.push(c));
      req.on('end', () => {
        targetGotBody = Buffer.concat(chunks).toString();
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end('{}');
      });
    });
    await new Promise<void>(r => target.listen(0, '127.0.0.1', r));
    const targetPort = (target.address() as net.AddressInfo).port;

    // Minimal HTTP CONNECT forward proxy: pipe the client socket to the target.
    let proxySawConnect = false;
    const proxy = http.createServer();
    proxy.on('connect', (req, clientSocket, head) => {
      proxySawConnect = true;
      const [host, port] = req.url!.split(':');
      const serverSocket = net.connect(Number(port), host, () => {
        clientSocket.write('HTTP/1.1 200 Connection Established\r\n\r\n');
        serverSocket.write(head);
        serverSocket.pipe(clientSocket);
        clientSocket.pipe(serverSocket);
      });
      serverSocket.on('error', () => clientSocket.end());
      clientSocket.on('error', () => serverSocket.end());
    });
    await new Promise<void>(r => proxy.listen(0, '127.0.0.1', r));
    const proxyPort = (proxy.address() as net.AddressInfo).port;

    const savedHttp = process.env.HTTP_PROXY;
    process.env.HTTP_PROXY = `http://127.0.0.1:${proxyPort}`;
    try {
      const dir = tmpCwdWithHooks([{
        event: 'preToolUse',
        hook: { type: 'http', url: `http://127.0.0.1:${targetPort}/hook` },
      }]);
      const hm = makeManager(dir);
      // No setProxiedHookFetch → exercises the REAL withProxyDispatcher/undici path.
      const decision = await hm.executePreToolUse(preCtx('bash'));
      assert.equal(proxySawConnect, true, 'the request MUST tunnel through the CONNECT proxy');
      assert.ok(targetGotBody.length > 0, 'the target origin received the tunnelled hook POST');
      assert.equal(decision.action, 'allow', 'a 2xx tunnelled webhook allows the action');
    } finally {
      if (savedHttp === undefined) delete process.env.HTTP_PROXY; else process.env.HTTP_PROXY = savedHttp;
      await new Promise<void>(r => target.close(() => r()));
      await new Promise<void>(r => proxy.close(() => r()));
    }
  });
});
