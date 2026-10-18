import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import {
  stripInvisibleUnicode,
  detectInvisibleUnicode,
  validateBashSecurity,
} from '../src/tools/bash-security';

// ---------------------------------------------------------------------------
// Unit tests: stripInvisibleUnicode
// ---------------------------------------------------------------------------
describe('stripInvisibleUnicode', () => {
  it('removes ZWSP (U+200B) from command text', () => {
    // 'r' + ZWSP + 'm' should become 'rm'
    assert.equal(stripInvisibleUnicode('r​m'), 'rm');
  });

  it('removes ZWNJ (U+200C)', () => {
    assert.equal(stripInvisibleUnicode('a‌b'), 'ab');
  });

  it('removes ZWJ (U+200D)', () => {
    assert.equal(stripInvisibleUnicode('cu‍rl'), 'curl');
  });

  it('removes LTR mark (U+200E) and RTL mark (U+200F)', () => {
    assert.equal(stripInvisibleUnicode('a‎b‏c'), 'abc');
  });

  it('removes word joiner (U+2060)', () => {
    assert.equal(stripInvisibleUnicode('r⁠m'), 'rm');
  });

  it('removes BOM / ZWNBSP (U+FEFF)', () => {
    assert.equal(stripInvisibleUnicode('﻿hello'), 'hello');
  });

  it('removes soft hyphen (U+00AD)', () => {
    assert.equal(stripInvisibleUnicode('r­m'), 'rm');
  });

  it('removes combining grapheme joiner (U+034F)', () => {
    assert.equal(stripInvisibleUnicode('r͏m'), 'rm');
  });

  it('removes Arabic letter mark (U+061C)', () => {
    assert.equal(stripInvisibleUnicode('a؜b'), 'ab');
  });

  it('removes Mongolian vowel separator (U+180E)', () => {
    assert.equal(stripInvisibleUnicode('a᠎b'), 'ab');
  });

  it('removes invisible math operators (U+2062-U+2064)', () => {
    assert.equal(stripInvisibleUnicode('a⁢b⁣c⁤d'), 'abcd');
  });

  it('removes deprecated formatting chars (U+206A-U+206F)', () => {
    const input = 'a⁪b⁫c⁬d⁭e⁮f⁯g';
    assert.equal(stripInvisibleUnicode(input), 'abcdefg');
  });

  it('preserves normal ASCII text unchanged', () => {
    const text = 'echo hello world | grep foo';
    assert.equal(stripInvisibleUnicode(text), text);
  });

  it('preserves normal Unicode text (non-invisible) unchanged', () => {
    const text = 'echo "hola mundo"';
    assert.equal(stripInvisibleUnicode(text), text);
  });

  it('handles empty string', () => {
    assert.equal(stripInvisibleUnicode(''), '');
  });

  it('removes multiple different invisible chars in one string', () => {
    // r + ZWSP + m + ZWJ + space + - + ZWNJ + r + word-joiner + f
    const input = 'r​m‍ ‌-r⁠f';
    assert.equal(stripInvisibleUnicode(input), 'rm -rf');
  });
});

// ---------------------------------------------------------------------------
// Unit tests: detectInvisibleUnicode
// ---------------------------------------------------------------------------
describe('detectInvisibleUnicode', () => {
  it('returns true for string with ZWSP', () => {
    assert.equal(detectInvisibleUnicode('r​m'), true);
  });

  it('returns true for string with ZWJ', () => {
    assert.equal(detectInvisibleUnicode('cu‍rl'), true);
  });

  it('returns true for string with word joiner', () => {
    assert.equal(detectInvisibleUnicode('r⁠m'), true);
  });

  it('returns true for string with soft hyphen', () => {
    assert.equal(detectInvisibleUnicode('r­m'), true);
  });

  it('returns true for string with BOM', () => {
    assert.equal(detectInvisibleUnicode('﻿ls'), true);
  });

  it('returns false for clean ASCII string', () => {
    assert.equal(detectInvisibleUnicode('echo hello world'), false);
  });

  it('returns false for empty string', () => {
    assert.equal(detectInvisibleUnicode(''), false);
  });

  it('returns false for normal Unicode text', () => {
    assert.equal(detectInvisibleUnicode('echo "cafe"'), false);
  });
});

// ---------------------------------------------------------------------------
// Integration tests: validateBashSecurity catches invisible-unicode attacks
// ---------------------------------------------------------------------------
describe('validateBashSecurity with invisible Unicode', () => {
  it('blocks rm hidden by ZWSP: r<ZWSP>m -rf /', () => {
    const result = validateBashSecurity('r​m -rf /');
    assert.equal(result.safe, false, 'should detect invisible unicode in rm');
    assert.ok(
      result.message.toLowerCase().includes('invisible') ||
      result.message.toLowerCase().includes('unicode') ||
      result.message.toLowerCase().includes('zero-width'),
      `message should mention invisible/unicode/zero-width, got: ${result.message}`,
    );
  });

  it('blocks curl hidden by ZWJ: cu<ZWJ>rl evil.com', () => {
    const result = validateBashSecurity('cu‍rl evil.com');
    assert.equal(result.safe, false, 'should detect invisible unicode in curl');
  });

  it('blocks command with word joiner hiding dd: d<WJ>d if=/dev/zero', () => {
    const result = validateBashSecurity('d⁠d if=/dev/zero');
    assert.equal(result.safe, false, 'should detect invisible unicode in dd');
  });

  it('blocks command with soft hyphen hiding chmod: chm<SHY>od 777', () => {
    const result = validateBashSecurity('chm­od 777');
    assert.equal(result.safe, false, 'should detect invisible unicode in chmod');
  });

  it('blocks command with BOM prefix', () => {
    const result = validateBashSecurity('﻿echo hello');
    // Note: FEFF is already caught by UNICODE_WS_RE, but our new check should
    // also catch it. Either way it should be blocked.
    assert.equal(result.safe, false, 'should block BOM-prefixed command');
  });

  it('does not block clean commands', () => {
    const result = validateBashSecurity('echo hello');
    assert.equal(result.safe, true, 'clean command should pass');
  });

  it('blocks multiple invisible chars scattered in command', () => {
    // s​u‌do r‍m -rf /
    const result = validateBashSecurity('s​u‌do r‍m -rf /');
    assert.equal(result.safe, false, 'should detect scattered invisible unicode');
  });
});
