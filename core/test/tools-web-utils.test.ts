import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isPrivateIP,
  isBlockedRedirectTarget,
  validateUrlSafety,
  stripHtml,
  BLOCKED_HOSTNAMES,
} from '../src/tools/web-utils';

// ---------------------------------------------------------------------------
// BLOCKED_HOSTNAMES
// ---------------------------------------------------------------------------

describe('BLOCKED_HOSTNAMES', () => {
  it('contains localhost', () => {
    assert.ok(BLOCKED_HOSTNAMES.has('localhost'));
  });

  it('contains 127.0.0.1', () => {
    assert.ok(BLOCKED_HOSTNAMES.has('127.0.0.1'));
  });

  it('contains ::1', () => {
    assert.ok(BLOCKED_HOSTNAMES.has('::1'));
  });

  it('contains 0.0.0.0', () => {
    assert.ok(BLOCKED_HOSTNAMES.has('0.0.0.0'));
  });

  it('contains metadata.google.internal', () => {
    assert.ok(BLOCKED_HOSTNAMES.has('metadata.google.internal'));
  });
});

// ---------------------------------------------------------------------------
// isPrivateIP
// ---------------------------------------------------------------------------

describe('isPrivateIP', () => {
  // IPv4 private ranges
  it('10.0.0.1 is private', () => assert.ok(isPrivateIP('10.0.0.1')));
  it('10.255.255.255 is private', () => assert.ok(isPrivateIP('10.255.255.255')));
  it('172.16.0.1 is private', () => assert.ok(isPrivateIP('172.16.0.1')));
  it('172.31.255.255 is private', () => assert.ok(isPrivateIP('172.31.255.255')));
  it('192.168.0.1 is private', () => assert.ok(isPrivateIP('192.168.0.1')));
  it('192.168.255.255 is private', () => assert.ok(isPrivateIP('192.168.255.255')));

  // Loopback
  it('127.0.0.1 is private', () => assert.ok(isPrivateIP('127.0.0.1')));
  it('127.255.255.255 is private', () => assert.ok(isPrivateIP('127.255.255.255')));

  // Link-local
  it('169.254.0.1 is private', () => assert.ok(isPrivateIP('169.254.0.1')));
  it('169.254.169.254 is private (AWS metadata)', () => assert.ok(isPrivateIP('169.254.169.254')));

  // Carrier-grade NAT
  it('100.64.0.1 is private', () => assert.ok(isPrivateIP('100.64.0.1')));
  it('100.127.255.255 is private', () => assert.ok(isPrivateIP('100.127.255.255')));

  // IPv6 private
  it('::1 is private', () => assert.ok(isPrivateIP('::1')));
  it(':: is private', () => assert.ok(isPrivateIP('::')));
  it('0.0.0.0 is private', () => assert.ok(isPrivateIP('0.0.0.0')));
  it('fe80::1 is private (link-local)', () => assert.ok(isPrivateIP('fe80::1')));
  it('fc00::1 is private', () => assert.ok(isPrivateIP('fc00::1')));
  it('fd12::1 is private', () => assert.ok(isPrivateIP('fd12::1')));

  // IPv4-mapped IPv6
  it('::ffff:127.0.0.1 is private', () => assert.ok(isPrivateIP('::ffff:127.0.0.1')));
  it('::ffff:10.0.0.1 is private', () => assert.ok(isPrivateIP('::ffff:10.0.0.1')));

  // Public IPs
  it('8.8.8.8 is public', () => assert.ok(!isPrivateIP('8.8.8.8')));
  it('1.1.1.1 is public', () => assert.ok(!isPrivateIP('1.1.1.1')));
  it('93.184.216.34 is public', () => assert.ok(!isPrivateIP('93.184.216.34')));
  it('172.32.0.1 is public (outside 172.16-31 range)', () => assert.ok(!isPrivateIP('172.32.0.1')));
  it('172.15.0.1 is public (outside 172.16-31 range)', () => assert.ok(!isPrivateIP('172.15.0.1')));
  it('100.128.0.1 is public (outside carrier-grade NAT)', () => assert.ok(!isPrivateIP('100.128.0.1')));
});

// ---------------------------------------------------------------------------
// isBlockedRedirectTarget
// ---------------------------------------------------------------------------

describe('isBlockedRedirectTarget', () => {
  it('blocks localhost URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://localhost/path'));
  });

  it('blocks 127.0.0.1 URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://127.0.0.1/path'));
  });

  it('blocks 10.x URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://10.0.0.1/path'));
  });

  it('blocks 192.168.x URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://192.168.1.1/path'));
  });

  it('blocks 172.16-31.x URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://172.16.0.1/path'));
  });

  it('blocks ::1 URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://[::1]/path'));
  });

  it('blocks 0.0.0.0', () => {
    assert.ok(isBlockedRedirectTarget('http://0.0.0.0/path'));
  });

  it('blocks 169.254.x URLs', () => {
    assert.ok(isBlockedRedirectTarget('http://169.254.169.254/path'));
  });

  it('allows public URLs', () => {
    assert.ok(!isBlockedRedirectTarget('https://example.com/path'));
  });

  it('returns true for invalid URLs', () => {
    assert.ok(isBlockedRedirectTarget('not a url'));
  });
});

// ---------------------------------------------------------------------------
// validateUrlSafety
// ---------------------------------------------------------------------------

describe('validateUrlSafety', () => {
  it('blocks blocked hostnames', async () => {
    const result = await validateUrlSafety('http://localhost/path');
    assert.ok(result !== null);
    assert.ok(result!.includes('Blocked'));
  });

  it('blocks metadata.google.internal', async () => {
    const result = await validateUrlSafety('http://metadata.google.internal/v1/');
    assert.ok(result !== null);
    assert.ok(result!.includes('Blocked'));
  });

  it('returns null for public hostname or fails DNS gracefully', async () => {
    const result = await validateUrlSafety('https://example.com');
    // In environments with DNS, example.com resolves to a public IP → null.
    // In restricted/offline environments, DNS fails → a "Blocked: DNS resolution failed" message.
    if (result !== null) {
      assert.ok(result.includes('DNS resolution failed'),
        `Expected null or DNS failure, got: ${result}`);
    }
  });
});

// ---------------------------------------------------------------------------
// stripHtml
// ---------------------------------------------------------------------------

describe('stripHtml', () => {
  it('removes basic HTML tags', () => {
    assert.equal(stripHtml('<p>hello</p>'), 'hello');
  });

  it('removes script tags and their content', () => {
    const result = stripHtml('<div>text<script>alert("xss")</script></div>');
    assert.ok(!result.includes('alert'));
    assert.ok(!result.includes('script'));
    assert.ok(result.includes('text'));
  });

  it('removes style tags and their content', () => {
    const result = stripHtml('<style>.foo { color: red; }</style><p>content</p>');
    assert.ok(!result.includes('.foo'));
    assert.ok(!result.includes('color'));
    assert.ok(result.includes('content'));
  });

  it('removes head tags and their content', () => {
    const result = stripHtml('<head><title>Test</title></head><body>body</body>');
    assert.ok(!result.includes('title'));
    assert.ok(result.includes('body'));
  });

  it('removes HTML comments', () => {
    const result = stripHtml('before<!-- comment -->after');
    assert.ok(!result.includes('comment'));
    assert.ok(result.includes('before'));
    assert.ok(result.includes('after'));
  });

  it('converts <br> to newlines', () => {
    const result = stripHtml('line1<br>line2<br/>line3');
    assert.ok(result.includes('line1'));
    assert.ok(result.includes('line2'));
    assert.ok(result.includes('line3'));
  });

  it('converts block elements to newlines', () => {
    const result = stripHtml('<p>para1</p><p>para2</p>');
    const lines = result.split('\n').filter(Boolean);
    assert.ok(lines.some(l => l.includes('para1')));
    assert.ok(lines.some(l => l.includes('para2')));
  });

  it('decodes common HTML entities', () => {
    const result = stripHtml('&amp; &lt; &gt; &quot; &#39; &nbsp;');
    assert.ok(result.includes('&'));
    assert.ok(result.includes('<'));
    assert.ok(result.includes('>'));
    assert.ok(result.includes('"'));
    assert.ok(result.includes("'"));
  });

  it('collapses multiple whitespace within lines', () => {
    const result = stripHtml('<p>hello    world    test</p>');
    assert.ok(result.includes('hello world test'));
  });

  it('collapses 3+ consecutive blank lines into 2', () => {
    const result = stripHtml('a\n\n\n\n\nb');
    const maxConsecutiveBlank = result.split('\n').reduce((acc, line, i, arr) => {
      if (i === 0) return 0;
      if (line.trim() === '' && arr[i - 1].trim() === '') return acc + 1;
      return acc;
    }, 0);
    assert.ok(maxConsecutiveBlank <= 1, 'Should not have more than 2 consecutive blank lines');
  });

  it('returns empty string for empty input', () => {
    assert.equal(stripHtml(''), '');
  });

  it('handles nested tags', () => {
    const result = stripHtml('<div><span><a href="#">link</a></span></div>');
    assert.ok(result.includes('link'));
    assert.ok(!result.includes('<'));
  });

  it('handles heading tags', () => {
    const result = stripHtml('<h1>Title</h1><h2>Subtitle</h2><p>text</p>');
    assert.ok(result.includes('Title'));
    assert.ok(result.includes('Subtitle'));
    assert.ok(result.includes('text'));
  });
});
