/**
 * POV: core web-fetch / SSRF guards — behavioral matrix.
 *
 * Everything here drives the REAL functions (webFetchTool.execute + the exported
 * web-utils helpers) and asserts on RESULTS. The network is NEVER touched:
 *  - SSRF-block cases return before any fetch (blocked hostnames need no DNS).
 *  - Allowed/success cases set `_allowLocalhostForTesting` (skips the live DNS in
 *    validateUrlSafety) and inject a mock HTTP getter via the `_webFetchHttpGet`
 *    seam, so no socket is ever opened.
 *  - validateUrlSafety is exercised through its `lookup` DI seam (synthetic DNS).
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as nodePath from 'path';
import { webFetchTool, summaryWindow, getCached, setCache } from '../src/tools/web-fetch';
import {
  isPrivateIP,
  isBlockedRedirectTarget,
  isValidIP,
  isMetadataEndpoint,
  stripHtml,
  validateUrlSafety,
  getUserAgent,
  DEFAULT_WEB_USER_AGENT,
  BLOCKED_HOSTNAMES,
  MAX_HTTP_BODY_BYTES,
  type HostLookupAll,
} from '../src/tools/web-utils';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type HttpGetResult = {
  body: string;
  bodyBuffer?: Buffer;
  statusCode: number;
  contentType: string;
  finalUrl: string;
  truncated?: boolean;
};

/** Build a mock HTTP getter for the `_webFetchHttpGet` seam. `finalUrl` defaults
 *  to the requested URL (same host → no redirect branch). Records calls. */
function mockGet(resp: Partial<HttpGetResult>, calls?: string[]) {
  return async (url: string): Promise<HttpGetResult> => {
    if (calls) calls.push(url);
    return {
      body: '',
      statusCode: 200,
      contentType: 'text/plain',
      finalUrl: url,
      ...resp,
    };
  };
}

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/** A lookup seam returning the given addresses (or an error). */
function lookupReturning(addrs: Array<{ address: string; family?: number }>): HostLookupAll {
  return ((_h, _o, cb) => cb(null, addrs.map(a => ({ address: a.address, family: a.family ?? 4 })))) as HostLookupAll;
}
function lookupFailing(): HostLookupAll {
  return ((_h, _o, cb) => cb(new Error('ENOTFOUND') as NodeJS.ErrnoException, [])) as HostLookupAll;
}

// Guard against a globally-set kill switch leaking into fetch tests.
let savedDisable: Array<[string, string | undefined]>;
before(() => {
  savedDisable = [
    ['CLAUDE_CODE_DISABLE_WEB_FETCH', process.env.CLAUDE_CODE_DISABLE_WEB_FETCH],
    ['SUPERINFERENCE_DISABLE_WEB_FETCH', process.env.SUPERINFERENCE_DISABLE_WEB_FETCH],
  ];
  delete process.env.CLAUDE_CODE_DISABLE_WEB_FETCH;
  delete process.env.SUPERINFERENCE_DISABLE_WEB_FETCH;
});
after(() => {
  for (const [k, v] of savedDisable) {
    if (v === undefined) delete process.env[k]; else process.env[k] = v;
  }
});

// ---------------------------------------------------------------------------
// Protocol / URL-shape guard (blocked vs allowed)
// ---------------------------------------------------------------------------

describe('web_fetch — protocol & URL-shape guard', () => {
  it('rejects empty / whitespace url', async () => {
    assert.equal((await webFetchTool.execute({ url: '' }, ctx())).isError, true);
    assert.equal((await webFetchTool.execute({ url: '   ' }, ctx())).isError, true);
  });

  it('rejects an unparseable URL', async () => {
    const r = await webFetchTool.execute({ url: 'http://  spaces in host' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /Invalid URL/);
  });

  for (const bad of ['ftp://example.com/f', 'file:///etc/passwd', 'data:text/plain,hi', 'gopher://x/1']) {
    it(`rejects non-http(s) protocol: ${bad}`, async () => {
      const r = await webFetchTool.execute({ url: bad }, ctx());
      assert.equal(r.isError, true);
      assert.match(r.output, /Unsupported protocol/);
    });
  }

  it('rejects an over-length URL (> 2000 chars)', async () => {
    const long = 'https://example.com/' + 'a'.repeat(2100);
    const r = await webFetchTool.execute({ url: long }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /maximum length/);
  });

  it('auto-upgrades http→https for non-local hosts (observable in sanitized URL)', async () => {
    const calls: string[] = [];
    const r = await webFetchTool.execute(
      { url: 'http://upgrade-me.example/page1' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'ok', contentType: 'text/plain' }, calls) }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('URL: https://upgrade-me.example/page1'), r.output);
    assert.ok(calls[0].startsWith('https://'), 'getter received the upgraded https URL');
  });
});

// ---------------------------------------------------------------------------
// SSRF guard — blocked hostnames (no DNS, returns before fetch)
// ---------------------------------------------------------------------------

describe('web_fetch — SSRF hostname block (no network)', () => {
  for (const host of ['localhost', '127.0.0.1', 'metadata.google.internal', '0.0.0.0']) {
    it(`blocks ${host}`, async () => {
      // A seam that throws if reached proves the block happens BEFORE any fetch.
      const r = await webFetchTool.execute(
        { url: `http://${host}/secret` },
        ctx({ _webFetchHttpGet: async () => { throw new Error('must not fetch'); } }),
      );
      assert.equal(r.isError, true);
      assert.match(r.output, /Blocked/);
    });
  }

  it('every BLOCKED_HOSTNAMES entry is rejected by validateUrlSafety with no lookup', async () => {
    const neverLookup: HostLookupAll = (() => { throw new Error('lookup must not run'); }) as HostLookupAll;
    for (const host of BLOCKED_HOSTNAMES) {
      const bracket = host.includes(':') ? `[${host}]` : host;
      const res = await validateUrlSafety(`http://${bracket}/`, neverLookup);
      assert.ok('error' in res, `${host} should be blocked`);
    }
  });
});

// ---------------------------------------------------------------------------
// SSRF guard — validateUrlSafety via DNS seam (private vs public)
// ---------------------------------------------------------------------------

describe('validateUrlSafety — resolved-IP matrix', () => {
  it('returns resolvedIP for a public address', async () => {
    const res = await validateUrlSafety('https://ok.example/', lookupReturning([{ address: '93.184.216.34' }]));
    assert.deepEqual(res, { resolvedIP: '93.184.216.34' });
  });

  it('blocks when the hostname resolves to a private IP (DNS-rebinding guard)', async () => {
    const res = await validateUrlSafety('https://sneaky.example/', lookupReturning([{ address: '10.1.2.3' }]));
    assert.ok('error' in res);
    assert.match((res as { error: string }).error, /private IP 10\.1\.2\.3/);
  });

  it('blocks when the hostname resolves to the cloud metadata IP', async () => {
    const res = await validateUrlSafety('https://evil.example/', lookupReturning([{ address: '169.254.169.254' }]));
    assert.ok('error' in res);
  });

  it('blocks when DNS yields no usable addresses', async () => {
    const res = await validateUrlSafety('https://empty.example/', lookupReturning([]));
    assert.ok('error' in res);
    assert.match((res as { error: string }).error, /DNS resolution failed/);
  });

  it('blocks when DNS yields only invalid addresses', async () => {
    const res = await validateUrlSafety('https://junk.example/', lookupReturning([{ address: 'undefined' }]));
    assert.ok('error' in res);
    assert.match((res as { error: string }).error, /DNS resolution failed/);
  });

  it('blocks when the DNS lookup errors out', async () => {
    const res = await validateUrlSafety('https://nxdomain.example/', lookupFailing());
    assert.ok('error' in res);
    assert.match((res as { error: string }).error, /DNS resolution failed/);
  });

  it('pins the FIRST usable address when several are returned', async () => {
    const res = await validateUrlSafety(
      'https://multi.example/',
      lookupReturning([{ address: '93.184.216.34' }, { address: '93.184.216.35' }]),
    );
    assert.deepEqual(res, { resolvedIP: '93.184.216.34' });
  });
});

// ---------------------------------------------------------------------------
// isPrivateIP matrix
// ---------------------------------------------------------------------------

describe('isPrivateIP — classification matrix', () => {
  const privateIPs = [
    '10.0.0.1', '10.255.255.255', '172.16.0.1', '172.31.255.1', '192.168.1.1',
    '127.0.0.1', '127.9.9.9', '169.254.0.5', '169.254.169.254', '0.0.0.0',
    '0.1.2.3', '100.64.0.1', '100.127.0.1', '::1', '::', 'fe80::1', 'fc00::1',
    'fd12::9', '::ffff:10.0.0.1', '::ffff:192.168.0.1',
  ];
  for (const ip of privateIPs) {
    it(`treats ${ip} as private`, () => assert.equal(isPrivateIP(ip), true));
  }

  const publicIPs = [
    '8.8.8.8', '1.1.1.1', '93.184.216.34', '172.15.0.1', '172.32.0.1',
    '192.169.0.1', '11.0.0.1', '100.63.0.1', '100.128.0.1', '169.253.0.1',
    '::ffff:8.8.8.8', '2001:4860:4860::8888',
  ];
  for (const ip of publicIPs) {
    it(`treats ${ip} as public`, () => assert.equal(isPrivateIP(ip), false));
  }
});

// ---------------------------------------------------------------------------
// isBlockedRedirectTarget / isValidIP / isMetadataEndpoint
// ---------------------------------------------------------------------------

describe('isBlockedRedirectTarget', () => {
  for (const u of [
    'http://localhost/x', 'http://127.0.0.1/x', 'http://10.1.2.3/x',
    'http://192.168.0.1/x', 'http://169.254.169.254/x', 'http://172.16.9.9/x',
    'http://0.0.0.0/x', 'http://[::1]/x', 'not even a url',
  ]) {
    it(`blocks ${u}`, () => assert.equal(isBlockedRedirectTarget(u), true));
  }
  for (const u of ['https://example.com/x', 'http://8.8.8.8/x', 'https://sub.example.org/']) {
    it(`allows ${u}`, () => assert.equal(isBlockedRedirectTarget(u), false));
  }
});

describe('isValidIP', () => {
  for (const ip of ['1.2.3.4', '255.255.255.255', 'fe80::1', '::1', '2001:db8::1']) {
    it(`accepts ${ip}`, () => assert.equal(isValidIP(ip), true));
  }
  for (const ip of ['', 'undefined', 'null', 'example.com', 'not-an-ip']) {
    it(`rejects ${JSON.stringify(ip)}`, () => assert.equal(isValidIP(ip), false));
  }
});

describe('isMetadataEndpoint', () => {
  for (const u of [
    'http://169.254.169.254/latest/', 'http://metadata.google.internal/x',
    'http://169.254.170.2/v2/', 'http://[fd00:ec2::254]/latest/',
  ]) {
    it(`detects ${u}`, () => assert.equal(isMetadataEndpoint(u), true));
  }
  for (const u of ['https://example.com', 'http://169.254.169.253/x', '', 'nonsense']) {
    it(`ignores ${JSON.stringify(u)}`, () => assert.equal(isMetadataEndpoint(u), false));
  }
});

// ---------------------------------------------------------------------------
// Domain filtering (allowed / blocked lists)
// ---------------------------------------------------------------------------

describe('web_fetch — domain filtering', () => {
  it('rejects a host absent from allowed_domains', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://example.com/x', allowed_domains: ['other.org'] },
      ctx({ _webFetchHttpGet: async () => { throw new Error('must not fetch'); } }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /not in the allowed domains/);
  });

  it('allows a subdomain of an allowed_domains entry', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://api.example.com/x', allowed_domains: ['example.com'] },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'allowed-body', contentType: 'text/plain' }) }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('allowed-body'));
  });

  it('rejects a host present in blocked_domains', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://bad.example.com/x', blocked_domains: ['example.com'] },
      ctx({ _webFetchHttpGet: async () => { throw new Error('must not fetch'); } }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /is blocked/);
  });
});

// ---------------------------------------------------------------------------
// Credential redaction in output
// ---------------------------------------------------------------------------

describe('web_fetch — credential redaction', () => {
  it('strips user:pass from the URL echoed back to the model', async () => {
    const r = await webFetchTool.execute(
      { url: 'http://alice:s3cr3t@creds.example/data' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'hi', contentType: 'text/plain' }) }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(!r.output.includes('s3cr3t'), 'password must not appear');
    assert.ok(!r.output.includes('alice'), 'username must not appear');
    assert.ok(r.output.includes('creds.example'), 'host is still shown');
  });

  it('strips credentials even on an error path', async () => {
    const r = await webFetchTool.execute(
      { url: 'http://bob:pw@err.example/x' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: async () => { throw new Error('boom'); } }),
    );
    assert.equal(r.isError, true);
    assert.ok(!r.output.includes('pw@'), r.output);
    assert.ok(!r.output.includes('bob'), r.output);
  });
});

// ---------------------------------------------------------------------------
// Content-type handling (html / plain / json / binary / pdf / redirect / 5xx)
// ---------------------------------------------------------------------------

describe('web_fetch — content-type handling', () => {
  it('converts HTML to markdown-ish text (tags stripped)', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://html.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: '<p>Hello <b>World</b></p>', contentType: 'text/html' }) }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('Hello'));
    assert.ok(r.output.includes('World'));
    assert.ok(!r.output.includes('<p>'), 'tags removed');
    assert.ok(r.output.includes('Content-Type: text/html'));
  });

  it('returns plain text unchanged', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://plain.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'raw plain text', contentType: 'text/plain' }) }),
    );
    assert.ok(r.output.includes('raw plain text'));
  });

  it('returns JSON bodies verbatim', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://json.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: '{"k":1}', contentType: 'application/json' }) }),
    );
    assert.ok(r.output.includes('{"k":1}'));
  });

  it('saves binary (octet-stream) to disk with bytes intact', async () => {
    const tmp = fs.mkdtempSync(nodePath.join(os.tmpdir(), 'pov-wf-bin-'));
    try {
      const bytes = Buffer.from([0x00, 0xff, 0x10, 0x89, 0x50]);
      const r = await webFetchTool.execute(
        { url: 'https://bin.example/a' },
        ctx({
          cwd: tmp,
          _allowLocalhostForTesting: true,
          _webFetchHttpGet: mockGet({ body: bytes.toString('binary'), bodyBuffer: bytes, contentType: 'application/octet-stream' }),
        }),
      );
      assert.equal(r.isError, undefined);
      const m = r.output.match(/saved to (\S+)\]/);
      assert.ok(m, r.output);
      assert.deepEqual([...fs.readFileSync(m![1])], [...bytes]);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('errors on a non-extractable PDF', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://pdf.example/doc' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'this is not really a pdf', contentType: 'application/pdf' }) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /PDF/);
  });

  it('detects a %PDF magic header even when content-type lies', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://magic.example/doc' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: '%PDF-1.4 broken', contentType: 'text/plain' }) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /PDF/);
  });

  it('flags a cross-host redirect instead of returning the body', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://orig.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'secret', contentType: 'text/plain', finalUrl: 'https://elsewhere.example/a' }) }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /Redirect detected/);
    assert.ok(!r.output.includes('secret'), 'redirected body is not surfaced');
  });

  it('treats HTTP 5xx as an error', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://err5.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'oops', statusCode: 503, contentType: 'text/plain' }) }),
    );
    assert.equal(r.isError, true);
    assert.match(r.output, /503/);
  });

  it('surfaces a 4xx body as a normal (non-error) result', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://err4.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'nope', statusCode: 404, contentType: 'text/plain' }) }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes('Status: 404'));
    assert.ok(r.output.includes('nope'));
  });
});

// ---------------------------------------------------------------------------
// Response size bounding
// ---------------------------------------------------------------------------

describe('web_fetch — response size bounding', () => {
  it('truncates bodies over the 50k response cap and reports unread chars + offset', async () => {
    const big = 'x'.repeat(60_000);
    const r = await webFetchTool.execute(
      { url: 'https://big.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: big, contentType: 'text/plain' }) }),
    );
    assert.equal(r.isError, undefined);
    assert.match(r.output, /characters unread/);
    assert.match(r.output, /offset=50000/);
  });

  it('appends a download-stopped notice when the getter reports truncation', async () => {
    const r = await webFetchTool.execute(
      { url: 'https://trunc.example/a' },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'partial body', contentType: 'text/plain', truncated: true }) }),
    );
    assert.equal(r.isError, undefined);
    assert.ok(r.output.includes(`Download stopped at ${MAX_HTTP_BODY_BYTES} bytes`), r.output);
  });

  it('honours an offset into the page and reports no-content past the end', async () => {
    const body = 'ABCDEFGHIJ'.repeat(10_000); // 100k chars
    const mid = await webFetchTool.execute(
      { url: 'https://off.example/a', offset: 1000 },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body, contentType: 'text/plain' }) }),
    );
    assert.equal(mid.isError, undefined);
    // endOffset = offset(1000) + min(contentAfterSlice, 50000) = 51000 (vs 50000 at offset 0)
    assert.match(mid.output, /offset=51000/);

    const past = await webFetchTool.execute(
      { url: 'https://off2.example/a', offset: 999_999 },
      ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body, contentType: 'text/plain' }) }),
    );
    assert.equal(past.isError, undefined);
    assert.match(past.output, /No content at offset/);
  });
});

// ---------------------------------------------------------------------------
// Kill switch
// ---------------------------------------------------------------------------

describe('web_fetch — operator kill switch', () => {
  for (const key of ['CLAUDE_CODE_DISABLE_WEB_FETCH', 'SUPERINFERENCE_DISABLE_WEB_FETCH']) {
    it(`${key}=1 disables fetching before any network use`, async () => {
      const prev = process.env[key];
      process.env[key] = '1';
      try {
        const r = await webFetchTool.execute(
          { url: 'https://example.com' },
          ctx({ _webFetchHttpGet: async () => { throw new Error('must not fetch'); } }),
        );
        assert.equal(r.isError, true);
        assert.match(r.output, /disabled/i);
      } finally {
        if (prev === undefined) delete process.env[key]; else process.env[key] = prev;
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Pure helpers: stripHtml, summaryWindow, getUserAgent, cache
// ---------------------------------------------------------------------------

describe('stripHtml', () => {
  it('drops script/style/head blocks entirely', () => {
    const out = stripHtml('<head><title>T</title></head><style>.a{}</style><script>evil()</script><p>Keep</p>');
    assert.ok(out.includes('Keep'));
    assert.ok(!out.includes('evil'));
    assert.ok(!out.includes('.a{}'));
    assert.ok(!out.includes('<title>'));
  });

  it('decodes common entities and collapses whitespace', () => {
    const out = stripHtml('<p>A   &amp;   B</p>');
    assert.ok(out.includes('A & B'));
  });

  it('turns block tags into line breaks', () => {
    const out = stripHtml('<div>one</div><div>two</div>');
    assert.ok(out.includes('one'));
    assert.ok(out.includes('two'));
    assert.ok(out.split('\n').length >= 2);
  });
});

describe('summaryWindow', () => {
  it('returns short text unchanged', () => {
    assert.equal(summaryWindow('short', 1000), 'short');
    assert.equal(summaryWindow('exactly-limit', 'exactly-limit'.length), 'exactly-limit');
  });

  it('keeps head and tail with a middle-omitted marker for long text', () => {
    const text = 'H'.repeat(1000) + 'M'.repeat(1000) + 'T'.repeat(1000);
    const out = summaryWindow(text, 1000);
    assert.ok(out.includes('middle omitted'));
    assert.ok(out.startsWith('H'));
    assert.ok(out.endsWith('T'));
    assert.ok(out.length < text.length);
  });
});

describe('getUserAgent', () => {
  it('returns the default UA when the override env is unset', () => {
    const prev = process.env.SUPERINFERENCE_WEB_USER_AGENT;
    delete process.env.SUPERINFERENCE_WEB_USER_AGENT;
    try {
      assert.equal(getUserAgent(), DEFAULT_WEB_USER_AGENT);
    } finally {
      if (prev !== undefined) process.env.SUPERINFERENCE_WEB_USER_AGENT = prev;
    }
  });

  it('honours the override env var', () => {
    const prev = process.env.SUPERINFERENCE_WEB_USER_AGENT;
    process.env.SUPERINFERENCE_WEB_USER_AGENT = 'MyBot/9';
    try {
      assert.equal(getUserAgent(), 'MyBot/9');
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_WEB_USER_AGENT; else process.env.SUPERINFERENCE_WEB_USER_AGENT = prev;
    }
  });
});

describe('web_fetch byte-limited cache', () => {
  it('round-trips a set/get and misses on an unknown key', () => {
    const key = `cache-key-${Date.now()}-${Math.random()}`;
    assert.equal(getCached(key), null);
    setCache(key, 'cached-value');
    assert.equal(getCached(key), 'cached-value');
  });

  it('overwrites an existing key on re-set', () => {
    const key = `cache-key-reset-${Date.now()}-${Math.random()}`;
    setCache(key, 'first');
    setCache(key, 'second');
    assert.equal(getCached(key), 'second');
  });

  it('a successful fetch is served from cache on an identical repeat (getter not re-called)', async () => {
    const url = `https://cacheme-${Date.now()}.example/a`;
    const calls: string[] = [];
    const context = () => ctx({ _allowLocalhostForTesting: true, _webFetchHttpGet: mockGet({ body: 'cached-page', contentType: 'text/plain' }, calls) });
    const first = await webFetchTool.execute({ url }, context());
    const second = await webFetchTool.execute({ url }, context());
    assert.equal(first.output, second.output);
    assert.equal(calls.length, 1, 'second call served from cache');
  });
});
