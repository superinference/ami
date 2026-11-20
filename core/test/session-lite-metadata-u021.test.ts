import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  unescapeJsonString,
  extractJsonStringField,
  extractLastJsonStringField,
  LITE_READ_BUF_SIZE,
  INITIAL_ENRICH_COUNT,
  SKIP_PRECOMPACT_THRESHOLD,
} from '../src/session';
import type { Session } from '../src/session';

// ---------------------------------------------------------------------------
// U021 — bounded head/tail lite-metadata reads for the session list.
// Covers: unescapeJsonString / extractJsonStringField / extractLastJsonStringField
// (truncation-tolerant partial parse), the read-size constants, readSessionMetaLite,
// and list()'s progressive enrichment (full top-N + lite tail / oversize fallback).
// ---------------------------------------------------------------------------

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-lite-meta-'));
}

describe('partial-JSON field extractors (unescapeJsonString / extract*JsonStringField)', () => {
  it('unescapeJsonString decodes JSON escapes including \\uXXXX', () => {
    assert.equal(unescapeJsonString('a\\nb\\tc'), 'a\nb\tc');
    assert.equal(unescapeJsonString('quote=\\" slash=\\\\ fwd=\\/'), 'quote=" slash=\\ fwd=/');
    assert.equal(unescapeJsonString('smile=\\u0041'), 'smile=A');
    // fast path: a slice with no backslash is returned as-is (identity).
    assert.equal(unescapeJsonString('plain text'), 'plain text');
  });

  it('unescapeJsonString tolerates a truncated trailing escape (bounded read cut mid-escape)', () => {
    // A lone trailing backslash is dropped, not thrown.
    assert.equal(unescapeJsonString('abc\\'), 'abc');
    // A truncated \u (fewer than 4 hex digits) drops the backslash, keeps the 'u'.
    assert.equal(unescapeJsonString('x\\u00'), 'xu00');
  });

  it('extractJsonStringField reads the FIRST top-level string value for a key', () => {
    const text = '{"id": "abc", "title": "Hello \\"World\\"", "n": 42}';
    assert.equal(extractJsonStringField(text, 'id'), 'abc');
    assert.equal(extractJsonStringField(text, 'title'), 'Hello "World"');
    // A non-string value (number) is not returned as a string field.
    assert.equal(extractJsonStringField(text, 'n'), undefined);
    // A missing key yields undefined.
    assert.equal(extractJsonStringField(text, 'nope'), undefined);
  });

  it('extractJsonStringField skips a matching key that appears as a VALUE, not a key', () => {
    // The literal `"id"` appears inside a value string before the real key.
    const text = '{"note": "see \\"id\\" below", "id": "real-one"}';
    assert.equal(extractJsonStringField(text, 'id'), 'real-one');
  });

  it('extractLastJsonStringField reads the LAST occurrence (newest-wins tail field)', () => {
    const text = '{"updatedAt":"2024-01-01"} ... {"updatedAt":"2024-06-30"}';
    assert.equal(extractLastJsonStringField(text, 'updatedAt'), '2024-06-30');
    assert.equal(extractLastJsonStringField(text, 'absent'), undefined);
  });

  it('readJsonStringAt-style truncation: an unterminated final string yields the partial', () => {
    // Simulates a tail/head slice cut mid-value — best-effort partial, never a throw.
    assert.equal(extractJsonStringField('{"title": "unterminated val', 'title'), 'unterminated val');
  });

  it('constants match the reference parity values', () => {
    assert.equal(LITE_READ_BUF_SIZE, 65536);
    assert.equal(INITIAL_ENRICH_COUNT, 50);
    assert.equal(SKIP_PRECOMPACT_THRESHOLD, 5 * 1024 * 1024);
  });
});

describe('SessionManager.readSessionMetaLite — bounded head/tail metadata read', () => {
  let base: string;
  let dir: string;
  let mgr: SessionManager;

  beforeEach(() => {
    base = tmp();
    dir = path.join(base, 'sessions');
    fs.mkdirSync(dir, { recursive: true });
    mgr = new SessionManager(base, dir);
  });
  afterEach(() => fs.rmSync(base, { recursive: true, force: true }));

  it('recovers metadata from a saved session WITHOUT parsing the full transcript', () => {
    const s: Session = {
      id: 'sess-meta' as Session['id'],
      customTitle: 'My Custom Title',
      aiTitle: 'auto title',
      messages: [{ role: 'user', content: 'hello world' }],
      createdAt: '2024-01-01T00:00:00.000Z',
      updatedAt: '2024-01-01T00:00:00.000Z',
      provider: { model: 'm', baseUrl: 'http://x' },
      gitBranch: 'feature/lite',
      tag: 'bug',
    };
    mgr.save(s);
    const meta = mgr.readSessionMetaLite(path.join(dir, 'sess-meta.json'));
    assert.ok(meta);
    assert.equal(meta!.id, 'sess-meta');
    assert.equal(meta!.customTitle, 'My Custom Title');
    assert.equal(meta!.aiTitle, 'auto title');
    assert.equal(meta!.gitBranch, 'feature/lite');
    assert.equal(meta!.tag, 'bug');
    assert.equal(meta!.kind, 'interactive');
    assert.ok(typeof meta!.updatedAt === 'string' && meta!.updatedAt.length > 0);
  });

  it('metadata-first serialization keeps metadata readable past a huge messages array', () => {
    // A messages payload far larger than the lite buffer; because save() writes the
    // scalar metadata BEFORE `messages`, the bounded head read still finds the title.
    // Assistant content is kept inline (not blob-offloaded), so the on-disk JSON is
    // genuinely larger than the lite buffer.
    const big = 'x'.repeat(LITE_READ_BUF_SIZE * 3);
    const s: Session = {
      id: 'sess-big' as Session['id'],
      customTitle: 'Findable Title',
      messages: [{ role: 'assistant', content: big }],
      createdAt: '2024-02-02T00:00:00.000Z',
      updatedAt: '2024-02-02T00:00:00.000Z',
      provider: { model: 'm', baseUrl: 'http://x' },
    };
    mgr.save(s);
    const file = path.join(dir, 'sess-big.json');
    assert.ok(fs.statSync(file).size > LITE_READ_BUF_SIZE, 'precondition: file exceeds the lite buffer');
    const meta = mgr.readSessionMetaLite(file);
    assert.ok(meta);
    assert.equal(meta!.customTitle, 'Findable Title');
    assert.equal(meta!.id, 'sess-big');
  });

  it('does not mistake a scalar key nested inside a message for the top-level value', () => {
    // A message whose content JSON contains `"gitBranch"` must not shadow the real one.
    const s: Session = {
      id: 'sess-collide' as Session['id'],
      messages: [{ role: 'user', content: 'payload with "gitBranch": "evil" inside' }],
      createdAt: '2024-03-03T00:00:00.000Z',
      updatedAt: '2024-03-03T00:00:00.000Z',
      provider: { model: 'm', baseUrl: 'http://x' },
      gitBranch: 'main',
    };
    mgr.save(s);
    const meta = mgr.readSessionMetaLite(path.join(dir, 'sess-collide.json'));
    assert.equal(meta!.gitBranch, 'main');
  });

  it('detects a top-level isSidechain marker', () => {
    const s = {
      id: 'sess-side',
      isSidechain: true,
      messages: [{ role: 'user', content: 'sub' }],
      createdAt: '2024-04-04T00:00:00.000Z',
      updatedAt: '2024-04-04T00:00:00.000Z',
      provider: { model: 'm', baseUrl: 'http://x' },
    } as unknown as Session;
    mgr.save(s);
    const meta = mgr.readSessionMetaLite(path.join(dir, 'sess-side.json'));
    assert.equal(meta!.isSidechain, true);
  });

  it('returns null for a zero-byte or missing file', () => {
    const empty = path.join(dir, 'empty.json');
    fs.writeFileSync(empty, '');
    assert.equal(mgr.readSessionMetaLite(empty), null);
    assert.equal(mgr.readSessionMetaLite(path.join(dir, 'ghost.json')), null);
  });

  it('returns null when no recoverable top-level id exists', () => {
    const noId = path.join(dir, 'noid.json');
    fs.writeFileSync(noId, JSON.stringify({ messages: [], createdAt: 'x' }));
    assert.equal(mgr.readSessionMetaLite(noId), null);
  });
});

describe('SessionManager.list — progressive enrichment (full top-N + lite tail)', () => {
  let base: string;
  let dir: string;
  let mgr: SessionManager;

  beforeEach(() => {
    base = tmp();
    dir = path.join(base, 'sessions');
    fs.mkdirSync(dir, { recursive: true });
    mgr = new SessionManager(base, dir);
  });
  afterEach(() => fs.rmSync(base, { recursive: true, force: true }));

  function seed(id: string, dateSuffix: string, firstPrompt: string): void {
    const s: Session = {
      id: id as Session['id'],
      messages: [{ role: 'user', content: firstPrompt }],
      createdAt: `2024-01-01T00:00:${dateSuffix}.000Z`,
      updatedAt: `2024-01-01T00:00:${dateSuffix}.000Z`,
      provider: { model: 'm', baseUrl: 'http://x' },
    };
    mgr.save(s);
  }

  it('enriches the most-recent N and lite-reads the rest', () => {
    // Newest -> oldest by date: s3, s2, s1.
    seed('s1', '01', 'oldest prompt');
    seed('s2', '02', 'middle prompt');
    seed('s3', '03', 'newest prompt');

    const entries = mgr.list({ enrichCount: 1 });
    assert.equal(entries.length, 3);
    // The single enriched (newest) row has a real first-prompt preview.
    assert.equal(entries[0].id, 's3');
    assert.equal(entries[0].preview, 'newest prompt');
    assert.equal(entries[0].lite, undefined);
    assert.equal(entries[0].messageCount, 1);
    // The remaining rows are lite: no full parse, so messageCount is 0 and the
    // preview falls back to the summary/sentinel (no real first-prompt text).
    for (const e of entries.slice(1)) {
      assert.equal(e.lite, true);
      assert.equal(e.messageCount, 0);
      assert.notEqual(e.preview, 'middle prompt');
      assert.notEqual(e.preview, 'oldest prompt');
      assert.ok(typeof e.date === 'string' && e.date.length > 0);
    }
  });

  it('default enrichCount full-parses a small store (no regression for existing callers)', () => {
    seed('a', '01', 'prompt a');
    seed('b', '02', 'prompt b');
    const entries = mgr.list();
    assert.equal(entries.length, 2);
    for (const e of entries) {
      assert.equal(e.lite, undefined);
      assert.equal(e.messageCount, 1);
    }
    assert.equal(entries.find(e => e.id === 'a')!.preview, 'prompt a');
  });

  it('forces a lite row for a file at/above SKIP_PRECOMPACT_THRESHOLD even within the window', () => {
    seed('small', '01', 'tiny prompt');
    // A session whose on-disk size exceeds the precompact threshold.
    const huge = 'y'.repeat(SKIP_PRECOMPACT_THRESHOLD + 1024);
    const s: Session = {
      id: 'huge' as Session['id'],
      customTitle: 'Huge Session',
      // Assistant content stays inline (not offloaded) so the file truly exceeds the threshold.
      messages: [{ role: 'assistant', content: huge }],
      createdAt: '2024-01-01T00:00:09.000Z',
      updatedAt: '2024-01-01T00:00:09.000Z',
      provider: { model: 'm', baseUrl: 'http://x' },
    };
    mgr.save(s);
    assert.ok(fs.statSync(path.join(dir, 'huge.json')).size >= SKIP_PRECOMPACT_THRESHOLD);

    const entries = mgr.list(); // default enrichCount — both are within the window
    const hugeEntry = entries.find(e => e.id === 'huge')!;
    assert.equal(hugeEntry.lite, true, 'oversize file must be lite-read, not fully parsed');
    assert.equal(hugeEntry.title, 'Huge Session');
    const smallEntry = entries.find(e => e.id === 'small')!;
    assert.equal(smallEntry.lite, undefined);
    assert.equal(smallEntry.preview, 'tiny prompt');
  });

  it('lite rows still honor excludeId and sidechain filtering', () => {
    seed('keep', '03', 'keep me');
    seed('drop', '02', 'exclude me');
    const sub = {
      id: 'sub',
      isSidechain: true,
      messages: [{ role: 'user', content: 'sidechain' }],
      createdAt: '2024-01-01T00:00:01.000Z',
      updatedAt: '2024-01-01T00:00:01.000Z',
      provider: { model: 'm', baseUrl: 'http://x' },
    } as unknown as Session;
    mgr.save(sub);

    // enrichCount 0 -> every row is lite; excludeId + default sidechain filtering apply.
    const entries = mgr.list({ enrichCount: 0, excludeId: 'drop' });
    const ids = entries.map(e => String(e.id)).sort();
    assert.deepEqual(ids, ['keep']);
  });
});
