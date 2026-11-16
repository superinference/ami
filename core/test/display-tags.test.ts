import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { stripDisplayTags } from '../src/utils/display-tags';
import { SessionManager } from '../src/session';
import type { Session } from '../src/session';
import { generateTitle } from '../src/title-generator';
import type { ProviderConfig, StreamChunk } from '../src/types';

// ---------------------------------------------------------------------------
// Unit: stripDisplayTags — the three deliberate design choices
// ---------------------------------------------------------------------------

describe('stripDisplayTags', () => {
  it('strips an entire system-injected tag block — content and all', () => {
    const input =
      '<system-reminder>injected machine context here</system-reminder>\nFix the login bug';
    assert.equal(stripDisplayTags(input), 'Fix the login bug');
  });

  it('strips multi-line blocks including appended context-file sections', () => {
    const input =
      'Refactor the parser\n<context>\nfile: a.ts\nfile: b.ts\n</context>';
    assert.equal(stripDisplayTags(input), 'Refactor the parser');
  });

  it('strips ANY lowercase tag block (allowlist-free) — new wrapper kinds too', () => {
    const input =
      '<brand-new-notification kind="x">noise</brand-new-notification>\nkeep this';
    assert.equal(stripDisplayTags(input), 'keep this');
  });

  it('leaves user prose with uppercase JSX/HTML components untouched', () => {
    const input = 'fix the <Button> layout and the <DocTable> header';
    assert.equal(stripDisplayTags(input), input);
  });

  it('leaves <!DOCTYPE html> and other `!`-prefixed markup untouched', () => {
    const input = 'why does <!DOCTYPE html> appear at the top';
    assert.equal(stripDisplayTags(input), input);
  });

  it('leaves an unpaired angle bracket (x < y) untouched', () => {
    const input = 'the guard fails when x < y in the loop';
    assert.equal(stripDisplayTags(input), input);
  });

  it('keeps adjacent differently-named blocks separate via the backreference', () => {
    // Non-greedy body + \1 closing tag must not swallow the middle text between
    // two different tagged blocks.
    const input = '<a>one</a>KEEP ME<b>two</b>';
    assert.equal(stripDisplayTags(input), 'KEEP ME');
  });

  it('does NOT strip a lowercase tag that is never closed (no backref match)', () => {
    const input = 'text with <open> but no close';
    assert.equal(stripDisplayTags(input), input);
  });

  it('matches tags with attributes', () => {
    const input = '<ide_selection file="a.ts" line="5">sel</ide_selection>real question';
    assert.equal(stripDisplayTags(input), 'real question');
  });

  it('returns the original text when stripping would leave nothing', () => {
    const input = '<system-reminder>only machine context</system-reminder>';
    // Better to show something than an empty title.
    assert.equal(stripDisplayTags(input), input);
  });

  it('is a no-op for plain text', () => {
    assert.equal(stripDisplayTags('just a normal message'), 'just a normal message');
  });
});

// ---------------------------------------------------------------------------
// Integration: SessionManager preview strips injected blocks
// ---------------------------------------------------------------------------

function makeSession(id: string, messages: Session['messages'] = []): Session {
  return {
    id,
    messages,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'test-model', baseUrl: 'http://localhost' },
  };
}

describe('SessionManager.extractPreview (wired to stripDisplayTags)', () => {
  let tmpDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-display-tags-'));
    sm = new SessionManager(tmpDir);
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preview excludes system-injected tag blocks on the first user message', () => {
    sm.save(
      makeSession('wrapped', [
        {
          role: 'user',
          content:
            '<system-reminder>do not surface this</system-reminder>\nDeploy the service',
        },
      ]),
    );
    const entries = sm.list();
    assert.ok(entries[0].preview.includes('Deploy the service'));
    assert.ok(!entries[0].preview.includes('do not surface this'));
  });
});

// ---------------------------------------------------------------------------
// Integration: generateTitle fallback strips injected blocks
// ---------------------------------------------------------------------------

function emptyStream(): typeof import('../src/provider').streamChatCompletion {
  // Model produces nothing usable → generateTitle falls back to the raw message.
  return async function* (): AsyncGenerator<StreamChunk> {
    yield { type: 'done' } as StreamChunk;
  } as unknown as typeof import('../src/provider').streamChatCompletion;
}

describe('generateTitle (wired to stripDisplayTags)', () => {
  const provider: ProviderConfig = { model: 'm', baseUrl: 'http://localhost' };

  it('fallback title derives from the user prose, not the injected block', async () => {
    const title = await generateTitle(
      '<system-reminder>machine noise</system-reminder>\nAdd a retry helper',
      provider,
      undefined,
      emptyStream(),
    );
    assert.equal(title, 'Add a retry helper');
    assert.ok(!title.includes('machine noise'));
  });
});
