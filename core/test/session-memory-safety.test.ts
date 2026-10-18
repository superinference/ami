import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateMemoryEntry,
  sanitizeMemoryEntry,
  MAX_MEMORY_ENTRIES,
  MAX_MEMORY_ENTRY_CHARS,
  SessionMemoryExtractor,
} from '../src/session-memory';
import type { SessionFact } from '../src/session-memory';

describe('validateMemoryEntry', () => {
  it('accepts normal text', () => {
    const result = validateMemoryEntry('User prefers TypeScript over JavaScript');
    assert.equal(result.valid, true);
    assert.equal(result.reason, undefined);
  });

  it('rejects empty string', () => {
    const result = validateMemoryEntry('');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'empty');
  });

  it('rejects whitespace-only string', () => {
    const result = validateMemoryEntry('   \n\t  ');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'empty');
  });

  it('rejects entry exceeding MAX_MEMORY_ENTRY_CHARS', () => {
    const long = 'a'.repeat(MAX_MEMORY_ENTRY_CHARS + 1);
    const result = validateMemoryEntry(long);
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'too_long');
  });

  it('accepts entry at exactly MAX_MEMORY_ENTRY_CHARS', () => {
    const exact = 'b'.repeat(MAX_MEMORY_ENTRY_CHARS);
    const result = validateMemoryEntry(exact);
    assert.equal(result.valid, true);
  });

  it('rejects <script> content', () => {
    const result = validateMemoryEntry('Inject <script>alert(1)</script> here');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });

  it('rejects javascript: protocol', () => {
    const result = validateMemoryEntry('click javascript:void(0) to test');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });

  it('rejects data:text/html content', () => {
    const result = validateMemoryEntry('visit data:text/html,<h1>hi</h1>');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });

  it('rejects eval() content', () => {
    const result = validateMemoryEntry('Use eval("code") to run dynamically');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });

  it('rejects Function() constructor', () => {
    const result = validateMemoryEntry('new Function("return 1")');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });

  it('rejects __proto__ manipulation', () => {
    const result = validateMemoryEntry('Set obj.__proto__ = null');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });

  it('rejects constructor.prototype pollution', () => {
    const result = validateMemoryEntry('obj.constructor.prototype.isAdmin = true');
    assert.equal(result.valid, false);
    assert.equal(result.reason, 'suspicious_content');
  });
});

describe('sanitizeMemoryEntry', () => {
  it('trims whitespace', () => {
    assert.equal(sanitizeMemoryEntry('  hello world  '), 'hello world');
  });

  it('truncates entries exceeding MAX_MEMORY_ENTRY_CHARS', () => {
    const long = 'x'.repeat(MAX_MEMORY_ENTRY_CHARS + 500);
    const result = sanitizeMemoryEntry(long);
    assert.ok(result.length <= MAX_MEMORY_ENTRY_CHARS);
    assert.ok(result.endsWith('... [truncated]'));
  });

  it('does not truncate entries within limit', () => {
    const short = 'short entry';
    assert.equal(sanitizeMemoryEntry(short), 'short entry');
  });

  it('strips HTML tags', () => {
    assert.equal(sanitizeMemoryEntry('hello <b>world</b>'), 'hello world');
    assert.equal(sanitizeMemoryEntry('<p>paragraph</p>'), 'paragraph');
  });

  it('strips nested HTML tags', () => {
    assert.equal(sanitizeMemoryEntry('<div><span>text</span></div>'), 'text');
  });
});

describe('constants', () => {
  it('MAX_MEMORY_ENTRIES is 100', () => {
    assert.equal(MAX_MEMORY_ENTRIES, 100);
  });

  it('MAX_MEMORY_ENTRY_CHARS is 2000', () => {
    assert.equal(MAX_MEMORY_ENTRY_CHARS, 2000);
  });
});

describe('SessionMemoryExtractor.mergeFacts — bounds enforcement', () => {
  const dummyProvider = { model: 'test', apiKey: 'x', baseUrl: 'http://localhost' };

  it('skips facts with empty text', () => {
    const extractor = new SessionMemoryExtractor(dummyProvider);
    const existing: SessionFact[] = [];
    const bad: SessionFact[] = [
      { fact: '', category: 'decision', confidence: 0.9 },
    ];
    const merged = extractor.mergeFacts(existing, bad);
    assert.equal(merged.length, 0);
  });

  it('skips facts with suspicious content', () => {
    const extractor = new SessionMemoryExtractor(dummyProvider);
    const merged = extractor.mergeFacts([], [
      { fact: '<script>alert(1)</script>', category: 'decision', confidence: 0.9 },
    ]);
    assert.equal(merged.length, 0);
  });

  it('sanitizes facts (strips HTML, trims)', () => {
    const extractor = new SessionMemoryExtractor(dummyProvider);
    const merged = extractor.mergeFacts([], [
      { fact: '  Use <b>TypeScript</b>  ', category: 'convention', confidence: 0.8 },
    ]);
    assert.equal(merged.length, 1);
    assert.equal(merged[0].fact, 'Use TypeScript');
  });

  it('caps merged list at MAX_MEMORY_ENTRIES, keeping highest confidence', () => {
    const extractor = new SessionMemoryExtractor(dummyProvider);
    const existing: SessionFact[] = Array.from({ length: MAX_MEMORY_ENTRIES }, (_, i) => ({
      fact: `fact-${i}`,
      category: 'decision' as const,
      confidence: 0.7 + (i / 1000),
    }));
    const extra: SessionFact[] = [
      { fact: 'new-high-confidence', category: 'decision', confidence: 0.99 },
    ];
    const merged = extractor.mergeFacts(existing, extra);
    assert.equal(merged.length, MAX_MEMORY_ENTRIES);
    assert.ok(merged.some(f => f.fact === 'new-high-confidence'));
  });
});
