import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  sanitizeLogMessage,
  UILogGuard,
  DEFAULT_MAX_LOG_LENGTH,
  SECRET_REDACTION,
} from '../src/ui-log-guard';

// Parity / hardening tests for terminal-safety sanitization of UI log messages.
// Each block targets a specific real gap: ANSI escapes, OSC/DCS sequences,
// C0/C1 control chars, CR line overwrites, secret redaction, and length caps.

const ESC = '\u001B';

describe('sanitizeLogMessage — plain text passthrough', () => {
  it('leaves ordinary ASCII unchanged', () => {
    assert.equal(sanitizeLogMessage('hello world'), 'hello world');
  });

  it('preserves newlines by default', () => {
    assert.equal(sanitizeLogMessage('line1\nline2'), 'line1\nline2');
  });

  it('preserves tabs', () => {
    assert.equal(sanitizeLogMessage('a\tb'), 'a\tb');
  });

  it('preserves unicode / emoji', () => {
    assert.equal(sanitizeLogMessage('café ✅ 日本語'), 'café ✅ 日本語');
  });
});

describe('sanitizeLogMessage — ANSI / escape stripping (GAP: terminal injection)', () => {
  it('strips SGR color CSI sequences', () => {
    // RED on old code: raw \u001B[31m...\u001B[0m reaches the terminal.
    assert.equal(sanitizeLogMessage(`${ESC}[31mred${ESC}[0m`), 'red');
  });

  it('strips cursor-movement CSI sequences', () => {
    assert.equal(sanitizeLogMessage(`a${ESC}[2Jb${ESC}[Hc`), 'abc');
  });

  it('strips OSC window-title injection (BEL-terminated)', () => {
    assert.equal(sanitizeLogMessage(`${ESC}]0;pwned\u0007text`), 'text');
  });

  it('strips OSC hyperlink injection (ST-terminated)', () => {
    assert.equal(
      sanitizeLogMessage(`${ESC}]8;;http://evil${ESC}\\click${ESC}]8;;${ESC}\\`),
      'click',
    );
  });

  it('strips DCS sequences', () => {
    assert.equal(sanitizeLogMessage(`x${ESC}Pq#0;2;0;0;0y${ESC}\\z`), 'xz');
  });

  it('strips 8-bit CSI (0x9B) sequences', () => {
    assert.equal(sanitizeLogMessage('a\u009B31mb'), 'ab');
  });

  it('strips lone/unterminated OSC at end of string', () => {
    assert.equal(sanitizeLogMessage(`keep${ESC}]0;dangling`), 'keep');
  });

  it('does not keep sanitizeAnsi option disabled from stripping controls', () => {
    const out = sanitizeLogMessage(`${ESC}[31mX`, { stripAnsi: false });
    // With ANSI disabled the ESC char is still a C0 control that gets stripped.
    assert.equal(out, '[31mX');
  });
});

describe('sanitizeLogMessage — control chars (GAP: C0/C1 + CR overwrite)', () => {
  it('normalizes CRLF to LF', () => {
    assert.equal(sanitizeLogMessage('a\r\nb'), 'a\nb');
  });

  it('normalizes lone CR to LF (prevents line overwrite)', () => {
    // RED on old code: bare \r lets attacker repaint the current line.
    assert.equal(sanitizeLogMessage('visible\rHIDDEN'), 'visible\nHIDDEN');
  });

  it('strips NUL and other C0 controls', () => {
    assert.equal(sanitizeLogMessage('a\u0000b\u0007c\u0008d'), 'abcd');
  });

  it('strips DEL (0x7F)', () => {
    assert.equal(sanitizeLogMessage('a\u007Fb'), 'ab');
  });

  it('strips C1 controls (0x80-0x9F)', () => {
    assert.equal(sanitizeLogMessage('a\u0085b\u0090c'), 'abc');
  });

  it('collapses newlines to spaces when allowNewlines=false', () => {
    assert.equal(sanitizeLogMessage('a\nb', { allowNewlines: false }), 'a b');
  });

  it('can disable control stripping', () => {
    assert.equal(sanitizeLogMessage('a\u0000b', { stripControl: false }), 'a\u0000b');
  });
});

describe('sanitizeLogMessage — secret redaction (GAP: credential leak)', () => {
  it('redacts OpenAI-style sk- keys', () => {
    assert.equal(
      sanitizeLogMessage('key is sk-abcdefghijklmnop1234 end'),
      `key is ${SECRET_REDACTION} end`,
    );
  });

  it('redacts Anthropic sk-ant keys', () => {
    assert.ok(sanitizeLogMessage('sk-ant-api03-abcdefghijklmnop').includes(SECRET_REDACTION));
  });

  it('redacts GitHub tokens', () => {
    assert.ok(sanitizeLogMessage('ghp_0123456789abcdefghijklmnopqrstuvwx').includes(SECRET_REDACTION));
  });

  it('redacts AWS access key ids', () => {
    assert.equal(sanitizeLogMessage('AKIAIOSFODNN7EXAMPLE'), SECRET_REDACTION);
  });

  it('redacts Slack tokens', () => {
    assert.ok(sanitizeLogMessage('xoxb-123456789012-abcdefghijkl').includes(SECRET_REDACTION));
  });

  it('redacts JWTs', () => {
    const jwt = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdefghijklmno';
    assert.ok(sanitizeLogMessage(jwt).includes(SECRET_REDACTION));
  });

  it('redacts Bearer tokens but keeps the scheme label', () => {
    const out = sanitizeLogMessage('Authorization: Bearer abc123def456ghi789');
    assert.ok(out.includes('Bearer'));
    assert.ok(out.includes(SECRET_REDACTION));
    assert.ok(!out.includes('abc123def456ghi789'));
  });

  it('redacts key=value secret pairs but keeps the key', () => {
    const out = sanitizeLogMessage('api_key=supersecretvalue123');
    assert.ok(out.startsWith('api_key='));
    assert.ok(out.includes(SECRET_REDACTION));
    assert.ok(!out.includes('supersecretvalue123'));
  });

  it('redacts password: value', () => {
    const out = sanitizeLogMessage('password: hunter2hunter2');
    assert.ok(!out.includes('hunter2hunter2'));
  });

  it('does not mangle ordinary prose', () => {
    const msg = 'the secret to good code is tests';
    // "secret to" has no =/: so it is not treated as a key=value pair.
    assert.equal(sanitizeLogMessage(msg), msg);
  });

  it('can disable redaction', () => {
    const msg = 'sk-abcdefghijklmnop1234';
    assert.equal(sanitizeLogMessage(msg, { redactSecrets: false }), msg);
  });
});

describe('sanitizeLogMessage — length bounds (GAP: unbounded output)', () => {
  it('truncates beyond the default cap', () => {
    const big = 'x'.repeat(DEFAULT_MAX_LOG_LENGTH + 500);
    const out = sanitizeLogMessage(big);
    assert.ok(out.length <= DEFAULT_MAX_LOG_LENGTH);
    assert.ok(out.endsWith('[truncated]'));
  });

  it('respects a custom cap', () => {
    const out = sanitizeLogMessage('abcdefghij', { maxLength: 5 });
    assert.ok(out.length <= 5);
  });

  it('disables cap with non-positive maxLength', () => {
    const big = 'y'.repeat(DEFAULT_MAX_LOG_LENGTH + 10);
    assert.equal(sanitizeLogMessage(big, { maxLength: 0 }).length, big.length);
  });

  it('leaves short messages untouched', () => {
    assert.equal(sanitizeLogMessage('short', { maxLength: 100 }), 'short');
  });
});

describe('sanitizeLogMessage — defensive coercion', () => {
  it('coerces non-string input', () => {
    // @ts-expect-error runtime callers may pass non-strings
    assert.equal(sanitizeLogMessage(42), '42');
  });

  it('handles null/undefined as empty string', () => {
    // @ts-expect-error runtime safety
    assert.equal(sanitizeLogMessage(null), '');
    // @ts-expect-error runtime safety
    assert.equal(sanitizeLogMessage(undefined), '');
  });
});

describe('UILogGuard — sanitizes on addLog (GAP: raw message to UI)', () => {
  it('strips ANSI from stored message', () => {
    const guard = new UILogGuard();
    const entry = guard.addLog(`${ESC}[31mred${ESC}[0m`, 'user', 's');
    assert.equal(entry.message, 'red');
    assert.equal(guard.getLogs()[0].message, 'red');
  });

  it('normalizes CR in stored message', () => {
    const guard = new UILogGuard();
    const entry = guard.addLog('a\rb', 'user', 's');
    assert.equal(entry.message, 'a\nb');
  });

  it('redacts secrets in stored message', () => {
    const guard = new UILogGuard();
    const entry = guard.addLog('token sk-abcdefghijklmnop1234', 'engine', 'sys');
    assert.ok(entry.message.includes(SECRET_REDACTION));
  });

  it('sanitizes and single-lines the source label (no redaction of source)', () => {
    const guard = new UILogGuard();
    const entry = guard.addLog('msg', 'user', `plug${ESC}[31min\nname`);
    assert.equal(entry.message, 'msg');
    assert.equal(entry.source, 'plugin name');
  });

  it('leaves plain messages unchanged (existing behavior preserved)', () => {
    const guard = new UILogGuard();
    const entry = guard.addLog('hello', 'user', 'test-plugin');
    assert.equal(entry.message, 'hello');
    assert.equal(entry.source, 'test-plugin');
  });

  it('can opt out of sanitization', () => {
    const guard = new UILogGuard(50_000, false);
    const entry = guard.addLog(`${ESC}[31mred`, 'user', 's');
    assert.equal(entry.message, `${ESC}[31mred`);
  });

  it('accepts custom sanitize options', () => {
    const guard = new UILogGuard(50_000, { redactSecrets: false });
    const entry = guard.addLog('sk-abcdefghijklmnop1234', 'user', 's');
    assert.equal(entry.message, 'sk-abcdefghijklmnop1234');
  });

  it('getLogsBySource matches the sanitized source', () => {
    const guard = new UILogGuard();
    guard.addLog('a', 'user', 'alpha');
    assert.equal(guard.getLogsBySource('alpha').length, 1);
  });
});
