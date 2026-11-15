import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  estimateTokens,
  estimateTextTokens,
  utf8ByteLength,
  estimateImageTokens,
  estimateBlockTokens,
  estimateContentTokens,
  estimateMessageTokens,
  IMAGE_DOCUMENT_TOKEN_ESTIMATE,
} from '../src/token-estimator';

const OVERHEAD_PER_MESSAGE = 4;
const PIXELS_PER_IMAGE_TOKEN = 750;

// ---------------------------------------------------------------------------
// Fix 1 — byte-aware (UTF-8) text estimation.
//
// The old estimator counted text by UTF-16 code units (String.length), which
// under-counts multi-byte unicode: a CJK char is 1 code unit but 3 UTF-8 bytes,
// an astral emoji is 2 code units but 4 bytes. Claude's rough token count
// divides the BYTE length. These tests assert the byte-based values and the
// integration paths that now route through them.
// ---------------------------------------------------------------------------

describe('parity/fix1: utf8ByteLength', () => {
  it('equals String.length for pure ASCII', () => {
    assert.equal(utf8ByteLength('hello world'), 'hello world'.length);
    assert.equal(utf8ByteLength(''), 0);
  });

  it('counts CJK as 3 bytes/char and astral emoji as 4 bytes', () => {
    assert.equal(utf8ByteLength('你'), 3);
    assert.equal(utf8ByteLength('你好世界'), 12);
    assert.equal(utf8ByteLength('😀'), 4); // 4 UTF-8 bytes (but .length === 2)
    assert.equal('😀'.length, 2);
    assert.equal(utf8ByteLength('é'), 2); // U+00E9 → 2 bytes
  });
});

describe('parity/fix1: estimateTextTokens', () => {
  it('matches estimateTokens exactly for ASCII (additive, no regression)', () => {
    for (const s of ['', 'a', 'abcd', 'abcde', 'hello world', 'x'.repeat(401)]) {
      assert.equal(estimateTextTokens(s), estimateTokens(s), `ascii s.len=${s.length}`);
    }
  });

  it('counts multi-byte text by UTF-8 bytes, strictly above the code-unit estimate', () => {
    const cjk = '你'.repeat(100); // 100 code units, 300 UTF-8 bytes
    assert.equal(estimateTextTokens(cjk), 75); // ceil(300/4)
    assert.equal(estimateTokens(cjk), 25); // ceil(100/4) — the old under-count
    assert.ok(estimateTextTokens(cjk) > estimateTokens(cjk));
  });

  it('empty string is 0 and a non-positive ratio falls back to the default', () => {
    assert.equal(estimateTextTokens(''), 0);
    assert.equal(estimateTextTokens('abcd', 0), estimateTextTokens('abcd'));
    assert.equal(estimateTextTokens('abcd', -3), estimateTextTokens('abcd'));
  });
});

describe('parity/fix1: block/content/message text paths are byte-aware', () => {
  const cjk = '你'.repeat(100); // bytes 300 → 75 tokens (byte-aware) vs 25 (code units)

  it('text block counts by UTF-8 bytes', () => {
    assert.equal(estimateBlockTokens({ type: 'text', text: cjk }), 75);
  });

  it('raw string block counts by UTF-8 bytes', () => {
    assert.equal(estimateBlockTokens(cjk), 75);
  });

  it('thinking and redacted_thinking blocks count by UTF-8 bytes', () => {
    assert.equal(estimateBlockTokens({ type: 'thinking', thinking: cjk }), 75);
    assert.equal(estimateBlockTokens({ type: 'redacted_thinking', data: cjk }), 75);
  });

  it('unknown block type serializes and counts by UTF-8 bytes', () => {
    const block = { type: 'server_tool_use', note: cjk };
    assert.equal(estimateBlockTokens(block), estimateTextTokens(JSON.stringify(block)));
    // The serialized form contains the multi-byte payload, so it must exceed the
    // old code-unit count of the same serialization.
    assert.ok(estimateBlockTokens(block) > estimateTokens(JSON.stringify(block)));
  });

  it('estimateContentTokens string + object fallback are byte-aware', () => {
    assert.equal(estimateContentTokens(cjk), 75);
    const obj = { note: cjk };
    assert.equal(estimateContentTokens(obj), estimateTextTokens(JSON.stringify(obj)));
    assert.ok(estimateContentTokens(obj) > estimateTokens(JSON.stringify(obj)));
  });

  it('estimateMessageTokens carries the byte-aware text count + overhead', () => {
    assert.equal(
      estimateMessageTokens({ role: 'user', content: cjk }),
      75 + OVERHEAD_PER_MESSAGE,
    );
  });

  it('ASCII message estimates are unchanged (parity preserved)', () => {
    assert.equal(
      estimateMessageTokens({ role: 'user', content: 'hello world' }),
      estimateTokens('hello world') + OVERHEAD_PER_MESSAGE,
    );
  });
});

// ---------------------------------------------------------------------------
// Fix 2 — dimension-aware image token estimation.
//
// A flat IMAGE_DOCUMENT_TOKEN_ESTIMATE wildly over-counts small images: a 64x64
// icon costs a handful of tokens, not ~2000. Claude charges ≈ (w*h)/750 tokens.
// When real pixel dimensions are present on the block, use them (capped at the
// flat conservative ceiling); otherwise fall back to the flat estimate.
// ---------------------------------------------------------------------------

describe('parity/fix2: estimateImageTokens', () => {
  it('uses (width*height)/750 when both dimensions are valid', () => {
    assert.equal(estimateImageTokens(64, 64), Math.ceil((64 * 64) / PIXELS_PER_IMAGE_TOKEN)); // 6
    assert.equal(estimateImageTokens(100, 100), Math.ceil((100 * 100) / PIXELS_PER_IMAGE_TOKEN)); // 14
    // A small image must cost far less than the flat conservative estimate.
    assert.ok(estimateImageTokens(100, 100) < IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });

  it('caps a very large image at the conservative flat estimate', () => {
    assert.equal(estimateImageTokens(10000, 10000), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });

  it('falls back to the flat estimate for missing/invalid dimensions', () => {
    assert.equal(estimateImageTokens(), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
    assert.equal(estimateImageTokens(0, 100), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
    assert.equal(estimateImageTokens(-5, 10), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
    assert.equal(estimateImageTokens(Number.NaN, 10), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
    assert.equal(estimateImageTokens(Infinity, 10), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
    assert.equal(estimateImageTokens(100, undefined), IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });
});

describe('parity/fix2: estimateBlockTokens image is dimension-aware', () => {
  it('reads top-level width/height on an image block', () => {
    assert.equal(
      estimateBlockTokens({ type: 'image', width: 100, height: 100, source: {} }),
      Math.ceil((100 * 100) / PIXELS_PER_IMAGE_TOKEN),
    );
  });

  it('reads width/height nested under source', () => {
    assert.equal(
      estimateBlockTokens({ type: 'image', source: { width: 64, height: 64, data: 'x' } }),
      Math.ceil((64 * 64) / PIXELS_PER_IMAGE_TOKEN),
    );
  });

  it('a small dimensioned image counts far less than the flat estimate', () => {
    assert.ok(estimateBlockTokens({ type: 'image', width: 80, height: 80 }) < IMAGE_DOCUMENT_TOKEN_ESTIMATE);
  });

  it('an image WITHOUT dimensions keeps the flat estimate (parity preserved)', () => {
    assert.equal(
      estimateBlockTokens({ type: 'image', source: { data: 'base64data' } }),
      IMAGE_DOCUMENT_TOKEN_ESTIMATE,
    );
  });

  it('a document block is always the flat estimate regardless of stray w/h', () => {
    assert.equal(
      estimateBlockTokens({ type: 'document', width: 10, height: 10, source: { data: 'x' } }),
      IMAGE_DOCUMENT_TOKEN_ESTIMATE,
    );
  });
});
