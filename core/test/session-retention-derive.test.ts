/**
 * Unit + integration coverage for two additive session.ts parity features:
 *
 *  1. Session retention cleanup (`SessionManager.cleanupOldSessions`) — parity with
 *     Claude's `cleanupPeriodDays`: delete transcripts older than N days, no-op when
 *     the window is unset / 0 / negative / non-finite, and remove the blob sidecar
 *     alongside the JSON.
 *  2. `contentToText` / deriveFirstPrompt hardening — parity with Claude's
 *     deriveFirstPrompt: a title/preview is derived from a multimodal (array)
 *     first user message by extracting its first text block, instead of collapsing
 *     to a generic placeholder.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { SessionManager, contentToText } from '../src/session';
import type { Session } from '../src/session';
import type { Message } from '../src/types';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-session-retain-'));
}

function makeSession(id: string, messages: Message[] = []): Session {
  return {
    id,
    messages,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'test-model', baseUrl: 'http://localhost' },
  };
}

const DAY_MS = 24 * 60 * 60 * 1000;

describe('contentToText (deriveFirstPrompt hardening)', () => {
  it('returns a plain string unchanged', () => {
    assert.equal(contentToText('hello world'), 'hello world');
  });

  it('extracts the first text block from an array (multimodal) content', () => {
    const content = [
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'x' } },
      { type: 'text', text: 'describe this screenshot' },
    ];
    assert.equal(contentToText(content), 'describe this screenshot');
  });

  it('returns the first text block when multiple are present', () => {
    const content = [
      { type: 'text', text: 'first' },
      { type: 'text', text: 'second' },
    ];
    assert.equal(contentToText(content), 'first');
  });

  it('returns "" for an array with no text block', () => {
    const content = [
      { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'x' } },
    ];
    assert.equal(contentToText(content), '');
  });

  it('uses a blob ref preview', () => {
    const ref = { __siSessionBlob: 'deadbeef', preview: 'the preview', length: 9000 };
    assert.equal(contentToText(ref), 'the preview');
  });

  it('returns "" for undefined / null / non-text values', () => {
    assert.equal(contentToText(undefined), '');
    assert.equal(contentToText(null), '');
    assert.equal(contentToText(42), '');
    assert.equal(contentToText({ not: 'content' }), '');
  });
});

describe('SessionManager.fork — title derivation from multimodal first message', () => {
  let tmpDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = makeTempDir();
    sm = new SessionManager(tmpDir);
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('derives the branch title from an array-content first user message', () => {
    const source = makeSession('src-multimodal', [
      {
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'x' } },
          { type: 'text', text: 'Refactor   the  parser\nmodule' },
        ],
      } as Message,
    ]);
    const forked = sm.fork(source);
    // Whitespace collapsed + "(Branch)" suffix — NOT the generic fallback.
    assert.equal(forked.customTitle, 'Refactor the parser module (Branch)');
  });

  it('falls back to a generic title when the first message has no text', () => {
    const source = makeSession('src-image-only', [
      {
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'x' } },
        ],
      } as Message,
    ]);
    const forked = sm.fork(source);
    assert.equal(forked.customTitle, 'Branched conversation (Branch)');
  });
});

describe('SessionManager.list — preview from multimodal first message', () => {
  let tmpDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = makeTempDir();
    sm = new SessionManager(tmpDir);
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('shows the text block in the picker preview for an array-content message', () => {
    sm.save(
      makeSession('mm-preview', [
        {
          role: 'user',
          content: [
            { type: 'image', source: { type: 'base64', media_type: 'image/png', data: 'x' } },
            { type: 'text', text: 'what does this diagram show?' },
          ],
        } as Message,
      ]),
    );
    const [entry] = sm.list();
    assert.equal(entry.preview, 'what does this diagram show?');
  });
});

describe('SessionManager.cleanupOldSessions (cleanupPeriodDays retention)', () => {
  let tmpDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = makeTempDir();
    sm = new SessionManager(tmpDir);
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  function backdate(id: string, days: number): void {
    const file = path.join(sm.getSessionDir(), `${id}.json`);
    const when = new Date(Date.now() - days * DAY_MS);
    fs.utimesSync(file, when, when);
  }

  it('is a no-op for undefined / 0 / negative / non-finite windows', () => {
    sm.save(makeSession('keep-1', [{ role: 'user', content: 'hi' }]));
    backdate('keep-1', 400);
    for (const bad of [undefined, 0, -5, NaN, Infinity] as const) {
      assert.equal(sm.cleanupOldSessions(bad), 0, `window=${String(bad)} should delete nothing`);
      assert.ok(sm.load('keep-1'), `window=${String(bad)} must keep the session`);
    }
  });

  it('deletes sessions older than the window and keeps fresh ones', () => {
    sm.save(makeSession('old-1', [{ role: 'user', content: 'ancient' }]));
    sm.save(makeSession('old-2', [{ role: 'user', content: 'also old' }]));
    sm.save(makeSession('fresh', [{ role: 'user', content: 'recent' }]));
    backdate('old-1', 40);
    backdate('old-2', 31);
    // fresh keeps its just-written mtime (0 days old)

    const removed = sm.cleanupOldSessions(30);
    assert.equal(removed, 2);
    assert.equal(sm.load('old-1'), null);
    assert.equal(sm.load('old-2'), null);
    assert.ok(sm.load('fresh'));
  });

  it('removes the content-addressed blob sidecar of a pruned session', () => {
    // A >5000-char message offloads to a blob sidecar dir; cleanup must remove it.
    const big = 'x'.repeat(6000);
    sm.save(makeSession('with-blob', [{ role: 'user', content: big }]));
    const blobDir = path.join(sm.getSessionDir(), 'blobs', 'with-blob');
    assert.ok(fs.existsSync(blobDir), 'precondition: blob sidecar exists');
    backdate('with-blob', 90);

    assert.equal(sm.cleanupOldSessions(30), 1);
    assert.equal(sm.load('with-blob'), null);
    assert.equal(fs.existsSync(blobDir), false, 'blob sidecar must be gone');
  });

  it('skips a .json entry whose stat fails (dangling symlink) and sweeps the rest', () => {
    // A dangling symlink named *.json is enumerated by listFiles() but statSync
    // (which follows symlinks) throws ENOENT — exercising the stat-race catch for
    // real, without mocking the frozen fs namespace. The sweep must continue and
    // still prune the genuinely-old session alongside it.
    sm.save(makeSession('old-real', [{ role: 'user', content: 'ancient' }]));
    backdate('old-real', 90);
    fs.symlinkSync(
      path.join(sm.getSessionDir(), 'nonexistent-target'),
      path.join(sm.getSessionDir(), 'dangling.json'),
    );

    assert.equal(sm.cleanupOldSessions(30), 1); // only the real old session removed
    assert.equal(sm.load('old-real'), null);
  });
});
