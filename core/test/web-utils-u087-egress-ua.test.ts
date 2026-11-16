/**
 * U087 — web-utils additive parity features.
 *
 * Two independent, additive capabilities (Claude-parity, no restyle):
 *
 *  1. Egress-proxy allow-list block detection. A network egress proxy signals an
 *     allow-list refusal with a `403` + `X-Proxy-Error: blocked-by-allowlist`
 *     header; that is NOT the origin saying "Forbidden" (the request never left
 *     the boundary). `httpGet` must raise a distinct `EgressBlockedError` carrying
 *     the blocked host, and `web_fetch` must surface the actionable remedy.
 *     An ordinary origin `403` (no such header) is unaffected and flows through as
 *     a normal body (regression lock). Mirrors Claude's `EgressBlockedError`.
 *
 *  2. User-Agent product version from the single-source `AMI_VERSION` macro
 *     (core/src/version.ts) instead of a hardcoded "1.0" — a version bump flows
 *     through automatically (parity with Claude's `claude-code/${MACRO.VERSION}`).
 *
 * E2E through the real `httpGet` redirect/response loop (via the `_webHooks.rawGet`
 * seam, no network) and through the real `web_fetch` tool execute() path.
 */

import { describe, it, after } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import type * as http from 'node:http';
import type * as https from 'node:https';
import {
  httpGet,
  getUserAgent,
  DEFAULT_WEB_USER_AGENT,
  EgressBlockedError,
  EGRESS_PROXY_ERROR_HEADER,
  EGRESS_PROXY_BLOCKED_VALUE,
  _webHooks,
} from '../src/tools/web-utils';
import { AMI_VERSION } from '../src/version';
import { webFetchTool } from '../src/tools/web-fetch';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// rawGet seam: serve a single response with arbitrary status + headers + body.
// ---------------------------------------------------------------------------
type Hop = { status: number; headers?: Record<string, string>; body?: string };

function seamFor(hops: Record<string, Hop>): typeof _webHooks.rawGet {
  return ((
    _requester: typeof http | typeof https,
    url: string,
    _options: http.RequestOptions,
    callback: (res: http.IncomingMessage) => void,
  ) => {
    const hop = hops[url] ?? { status: 404 };
    const res = new EventEmitter() as unknown as http.IncomingMessage & { resume: () => void };
    res.resume = () => {};
    res.statusCode = hop.status;
    res.headers = { 'content-type': 'text/plain', ...(hop.headers ?? {}) };
    process.nextTick(() => {
      callback(res);
      process.nextTick(() => {
        if (hop.body !== undefined) res.emit('data', Buffer.from(hop.body));
        res.emit('end');
      });
    });
    const req = new EventEmitter() as unknown as http.ClientRequest;
    (req as unknown as { destroy: () => void }).destroy = () => {};
    return req;
  }) as typeof _webHooks.rawGet;
}

// ---------------------------------------------------------------------------
// 1. Egress-proxy allow-list block detection
// ---------------------------------------------------------------------------
describe('httpGet — egress-proxy allow-list block (U087)', () => {
  const origRawGet = _webHooks.rawGet;
  after(() => {
    _webHooks.rawGet = origRawGet;
  });

  it('rejects with EgressBlockedError on 403 + X-Proxy-Error: blocked-by-allowlist', async () => {
    _webHooks.rawGet = seamFor({
      'https://blocked.example/doc': {
        status: 403,
        headers: { [EGRESS_PROXY_ERROR_HEADER]: EGRESS_PROXY_BLOCKED_VALUE },
        body: 'proxy says no',
      },
    });
    await assert.rejects(
      httpGet('https://blocked.example/doc', new AbortController().signal),
      (err: unknown) => {
        assert.ok(err instanceof EgressBlockedError, 'expected EgressBlockedError');
        assert.equal(err.domain, 'blocked.example');
        return true;
      },
    );
  });

  it('lets an ordinary origin 403 (no proxy header) flow through as a normal body', async () => {
    _webHooks.rawGet = seamFor({
      'https://origin.example/private': { status: 403, body: 'Forbidden by origin' },
    });
    const r = await httpGet('https://origin.example/private', new AbortController().signal);
    assert.equal(r.statusCode, 403);
    assert.equal(r.body, 'Forbidden by origin');
  });

  it('ignores a 403 whose X-Proxy-Error carries a different value', async () => {
    _webHooks.rawGet = seamFor({
      'https://origin.example/other': {
        status: 403,
        headers: { [EGRESS_PROXY_ERROR_HEADER]: 'some-other-reason' },
        body: 'still a body',
      },
    });
    const r = await httpGet('https://origin.example/other', new AbortController().signal);
    assert.equal(r.statusCode, 403);
    assert.equal(r.body, 'still a body');
  });
});

describe('web_fetch — surfaces an egress-proxy block distinctly (U087 e2e)', () => {
  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return { cwd: process.cwd(), abortSignal: new AbortController().signal, ...overrides };
  }

  it('reports the allow-list remedy, not a generic origin error', async () => {
    const throwingGet = (async () => {
      throw new EgressBlockedError('blocked.example');
    }) as unknown as typeof httpGet;
    const res = await webFetchTool.execute(
      { url: 'https://blocked.example/x', prompt: 'summarize' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: throwingGet }),
    );
    assert.equal(res.isError, true);
    assert.match(res.output, /egress proxy/i);
    assert.match(res.output, /allow-list/i);
    assert.match(res.output, /blocked\.example/);
  });
});

// ---------------------------------------------------------------------------
// 2. User-Agent version from the single-source AMI_VERSION macro
// ---------------------------------------------------------------------------
describe('User-Agent embeds the real build version (U087)', () => {
  it('DEFAULT_WEB_USER_AGENT carries AMI_VERSION, not a hardcoded 1.0', () => {
    assert.ok(
      DEFAULT_WEB_USER_AGENT.includes(AMI_VERSION),
      `expected UA to embed AMI_VERSION ${AMI_VERSION}, got "${DEFAULT_WEB_USER_AGENT}"`,
    );
    assert.equal(DEFAULT_WEB_USER_AGENT, `SuperInference-AMI/${AMI_VERSION} (+https://superinference.org)`);
    assert.doesNotMatch(DEFAULT_WEB_USER_AGENT, /SuperInference-AMI\/1\.0 /);
  });

  it('getUserAgent returns the version-bearing UA when the override env is unset', () => {
    const prev = process.env.SUPERINFERENCE_WEB_USER_AGENT;
    delete process.env.SUPERINFERENCE_WEB_USER_AGENT;
    try {
      const ua = getUserAgent();
      assert.equal(ua, DEFAULT_WEB_USER_AGENT);
      assert.ok(ua.includes(AMI_VERSION));
    } finally {
      if (prev !== undefined) process.env.SUPERINFERENCE_WEB_USER_AGENT = prev;
    }
  });
});
