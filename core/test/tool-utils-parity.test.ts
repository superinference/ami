import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  scanForSecrets,
  redactSecrets,
  sanitizePathForDisplay,
  isBlockedPath,
} from '../src/tools/tool-utils';

// ---------------------------------------------------------------------------
// Parity hardening for shared tool helpers. Each block targets a REAL gap that
// was verified failing on the pre-fix source (see the comment per block for the
// discriminating value that fails on revert).
// ---------------------------------------------------------------------------

describe('tool-utils parity — Anthropic real key format (sk-ant-api03-…)', () => {
  // A genuine Anthropic key is `sk-ant-api03-<base64url>`: the body contains
  // `-`/`_`. The old `sk-ant-[a-zA-Z0-9]{20,}` run stopped after `api03` (5
  // chars) and never matched — so a leaked key sailed through both scan+redact.
  const realKey = 'sk-ant-api03-AbCdEf_gHiJkL-mNoPqRsTuVwXyZ0123456789AbCdEfGhIjKlMnOpQ';
  const line = `ANTHROPIC_API_KEY=${realKey}`;

  it('scanForSecrets detects a real sk-ant-api03 key (fails on revert: was [])', () => {
    assert.ok(scanForSecrets(line).length > 0);
  });

  it('redactSecrets fully removes the key body (fails on revert: key leaks verbatim)', () => {
    const out = redactSecrets(line);
    assert.ok(!out.includes(realKey), 'raw key must not survive');
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('tool-utils parity — GitHub fine-grained PAT (github_pat_)', () => {
  // Fine-grained PATs use `github_pat_` with an 82-char underscore-bearing body.
  // There was no dedicated pattern; scanForSecrets returned [].
  const pat = 'github_pat_11ABCDEFG0' + 'a'.repeat(72);

  it('scanForSecrets detects a github_pat_ token (fails on revert: was [])', () => {
    // Use a bare token with no KEY= prefix so only the token rule can catch it.
    assert.ok(scanForSecrets(pat).length > 0);
  });

  it('redactSecrets removes a bare github_pat_ token (fails on revert: leaks)', () => {
    const out = redactSecrets(`value ${pat} end`);
    assert.ok(!out.includes(pat));
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('tool-utils parity — Stripe secret/restricted keys', () => {
  // `sk_live_`/`rk_test_` are underscore-delimited, so the OpenAI `sk-` rule
  // never matched them and there was no dedicated redaction.
  const live = 'sk_live_' + 'A1b2C3d4E5f6G7h8I9j0K1l2';
  const restricted = 'rk_test_' + 'Z9y8X7w6V5u4T3s2R1q0P9o8';

  it('scanForSecrets detects sk_live_ (fails on revert: was [])', () => {
    assert.ok(scanForSecrets(`STRIPE_KEY=${live}`).length > 0);
  });

  it('redactSecrets removes a bare sk_live_ key (fails on revert: leaks verbatim)', () => {
    const out = redactSecrets(`STRIPE=${live}`);
    assert.ok(!out.includes(live));
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });

  it('redactSecrets removes rk_test_ restricted keys too', () => {
    const out = redactSecrets(`cfg ${restricted}`);
    assert.ok(!out.includes(restricted));
  });
});

describe('tool-utils parity — Google OAuth tokens (ya29.)', () => {
  const token = 'ya29.' + 'A0ARrdaBc9DeFgHiJkLmNoPqRsTuVwXyZ';

  it('scanForSecrets detects a ya29. token (fails on revert: was [])', () => {
    assert.ok(scanForSecrets(`refresh=${token}`).length > 0);
  });

  it('redactSecrets removes a bare ya29. token (fails on revert: leaks)', () => {
    const out = redactSecrets(`token ${token}`);
    assert.ok(!out.includes(token));
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('tool-utils parity — SendGrid API keys (SG.)', () => {
  const key = 'SG.' + 'a'.repeat(22) + '.' + 'b'.repeat(43);

  it('scanForSecrets detects a SendGrid key (fails on revert: was [])', () => {
    assert.ok(scanForSecrets(`SENDGRID=${key}`).length > 0);
  });

  it('redactSecrets removes a bare SendGrid key (fails on revert: leaks)', () => {
    const out = redactSecrets(`key ${key} x`);
    assert.ok(!out.includes(key));
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('tool-utils parity — sanitizePathForDisplay escapes terminal-injection bytes', () => {
  // ESC/BEL/backspace and other control bytes previously flowed through verbatim,
  // enabling ANSI terminal injection from an attacker-named path.
  it('escapes ESC (\\x1b) — fails on revert: raw ESC passes through', () => {
    const out = sanitizePathForDisplay('/tmp/\x1b[31mred');
    assert.ok(!out.includes('\x1b'), 'raw ESC must not survive');
    assert.ok(out.includes('\\x1b'));
  });

  it('escapes BEL (\\x07) and backspace (\\x08)', () => {
    const out = sanitizePathForDisplay('a\x07b\x08c');
    assert.ok(!/[\x07\x08]/.test(out));
    assert.equal(out, 'a\\x07b\\x08c');
  });

  it('escapes DEL (\\x7f)', () => {
    assert.equal(sanitizePathForDisplay('x\x7fy'), 'x\\x7fy');
  });

  it('still uses readable \\n/\\r/\\t and leaves plain paths untouched', () => {
    assert.equal(sanitizePathForDisplay('a\nb\rc\td'), 'a\\nb\\rc\\td');
    assert.equal(sanitizePathForDisplay('/home/u/my file.txt'), '/home/u/my file.txt');
  });
});

describe('tool-utils parity — blocks physical/kernel memory devices', () => {
  // Reading these can dump raw RAM or hang; they were previously allowed.
  for (const p of ['/dev/mem', '/dev/kmem', '/dev/port', '/proc/kcore']) {
    it(`blocks ${p} (fails on revert: returns false)`, () => {
      assert.equal(isBlockedPath(p), true);
    });
  }

  it('still allows an ordinary workspace path', () => {
    assert.equal(isBlockedPath('/home/user/project/file.ts'), false);
  });
});
