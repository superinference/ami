/**
 * Unit + integration coverage for the meaningful-first-prompt picker preview
 * (parity with the reference's getFirstMeaningfulUserMessageTextContent /
 * extractFirstPromptFromHead / getLogDisplayTitle):
 *
 *   - skip synthetic/ephemeral (`isMeta`, transcript-only) + sidechain turns
 *   - skip compaction-summary turns
 *   - iterate ALL text blocks of a multimodal message to find the real prompt
 *     behind leading `<ide_selection>`/`<ide_opened_file>` metadata
 *   - remember a `<command-name>` slash-command turn as a fallback label but keep
 *     scanning for a real prompt
 *   - format a `<bash-input>` turn as `! <cmd>`
 *   - drop lines matching the leading-lowercase-XML / `[Request interrupted…]`
 *     skip pattern
 *
 * Exercised both directly (`extractMeaningfulPreview`) and end-to-end through the
 * real `SessionManager.save()` → `list()` picker path.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { SessionManager, extractMeaningfulPreview, contentToTexts } from '../src/session';
import type { Session } from '../src/session';
import type { Message } from '../src/types';

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-session-preview-'));
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

describe('contentToTexts', () => {
  it('returns all text blocks of an array in order', () => {
    const content = [
      { type: 'text', text: 'alpha' },
      { type: 'image', source: {} },
      { type: 'text', text: 'beta' },
    ];
    assert.deepEqual(contentToTexts(content), ['alpha', 'beta']);
  });

  it('wraps a non-empty string, drops an empty one', () => {
    assert.deepEqual(contentToTexts('hi'), ['hi']);
    assert.deepEqual(contentToTexts(''), []);
  });

  it('returns a blob-ref preview and [] for non-content', () => {
    assert.deepEqual(contentToTexts({ __siSessionBlob: 'x', preview: 'p', length: 9 }), ['p']);
    assert.deepEqual(contentToTexts(undefined), []);
    assert.deepEqual(contentToTexts(42), []);
  });
});

describe('extractMeaningfulPreview', () => {
  it('returns "(no messages)" when there is no user turn', () => {
    assert.equal(extractMeaningfulPreview([]), '(no messages)');
    assert.equal(
      extractMeaningfulPreview([{ role: 'assistant', content: 'hi' } as Message]),
      '(no messages)',
    );
  });

  it('skips an isMeta user turn and returns the next real prompt', () => {
    const preview = extractMeaningfulPreview([
      { role: 'user', content: 'ephemeral nudge', isMeta: true } as Message,
      { role: 'user', content: 'the real question' } as Message,
    ]);
    assert.equal(preview, 'the real question');
  });

  it('skips a sidechain (subagent) user turn', () => {
    const preview = extractMeaningfulPreview([
      { role: 'user', content: 'subagent instruction', isSidechain: true } as Message,
      { role: 'user', content: 'main thread prompt' } as Message,
    ]);
    assert.equal(preview, 'main thread prompt');
  });

  it('skips a transcript-only user turn', () => {
    const preview = extractMeaningfulPreview([
      { role: 'user', content: 'verbose-only', isVisibleInTranscriptOnly: true } as Message,
      { role: 'user', content: 'actual prompt' } as Message,
    ]);
    assert.equal(preview, 'actual prompt');
  });

  it('skips a compaction-summary turn', () => {
    const preview = extractMeaningfulPreview([
      { role: 'user', content: '[Previous conversation summary] blah blah' } as Message,
      { role: 'user', content: 'post-compaction prompt' } as Message,
    ]);
    assert.equal(preview, 'post-compaction prompt');
  });

  it('iterates array text blocks to find the real prompt behind IDE metadata', () => {
    const preview = extractMeaningfulPreview([
      {
        role: 'user',
        content: [
          { type: 'text', text: '<ide_opened_file>src/a.ts</ide_opened_file>' },
          { type: 'text', text: 'please fix the parser' },
        ],
      } as Message,
    ]);
    assert.equal(preview, 'please fix the parser');
  });

  it('formats a bash-input turn as "! cmd"', () => {
    const preview = extractMeaningfulPreview([
      { role: 'user', content: '<bash-input>git status</bash-input>' } as Message,
    ]);
    assert.equal(preview, '! git status');
  });

  it('uses a slash-command name as fallback only when no real prompt follows', () => {
    const onlyCmd = extractMeaningfulPreview([
      { role: 'user', content: '<command-name>/clear</command-name>' } as Message,
    ]);
    assert.equal(onlyCmd, '/clear');

    const cmdThenPrompt = extractMeaningfulPreview([
      { role: 'user', content: '<command-name>/model</command-name>' } as Message,
      { role: 'user', content: 'now do the work' } as Message,
    ]);
    assert.equal(cmdThenPrompt, 'now do the work');
  });

  it('drops a turn starting with a lowercase XML tag or interrupt marker', () => {
    const preview = extractMeaningfulPreview([
      { role: 'user', content: '<task-notification>bg done</task-notification>' } as Message,
      { role: 'user', content: '[Request interrupted by user]' } as Message,
      { role: 'user', content: 'resumed instruction' } as Message,
    ]);
    assert.equal(preview, 'resumed instruction');
  });

  it('collapses whitespace and caps at 100 chars with an ellipsis', () => {
    const long = 'word '.repeat(60); // >100 chars, multiple spaces + implicit len
    const preview = extractMeaningfulPreview([
      { role: 'user', content: `line one\nline two ${long}` } as Message,
    ]);
    assert.ok(!preview.includes('\n'));
    assert.ok(preview.length <= 103);
    assert.ok(preview.endsWith('...'));
  });
});

describe('SessionManager.list — meaningful preview end-to-end', () => {
  let tmpDir: string;
  let sm: SessionManager;

  beforeEach(() => {
    tmpDir = makeTempDir();
    sm = new SessionManager(tmpDir);
  });
  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('shows the real prompt in the picker, skipping meta + IDE-metadata turns', () => {
    sm.save(
      makeSession('meaningful', [
        { role: 'user', content: 'injected context', isMeta: true } as Message,
        {
          role: 'user',
          content: [
            { type: 'text', text: '<ide_selection>lines 1-5</ide_selection>' },
            { type: 'text', text: 'summarise the selection' },
          ],
        } as Message,
      ]),
    );
    const [entry] = sm.list();
    assert.equal(entry.preview, 'summarise the selection');
  });

  it('labels a bash-only session as "! cmd" in the picker', () => {
    sm.save(
      makeSession('bash-only', [
        { role: 'user', content: '<bash-input>ls -la</bash-input>' } as Message,
      ]),
    );
    const [entry] = sm.list();
    assert.equal(entry.preview, '! ls -la');
  });
});
