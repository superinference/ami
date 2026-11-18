import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { stripDisplayTags, stripInjectedContextTags, extractTag } from '../src/utils/display-tags';
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
// Unit: stripInjectedContextTags — narrow allowlist, preserves user markup
// ---------------------------------------------------------------------------

describe('stripInjectedContextTags', () => {
  it('strips an injected <system-reminder> block but keeps the user prose', () => {
    const input =
      '<system-reminder>injected machine context</system-reminder>\nFix the login bug';
    assert.equal(stripInjectedContextTags(input), 'Fix the login bug');
  });

  it('strips a bare <system> block', () => {
    assert.equal(
      stripInjectedContextTags('<system>policy</system>\nreal ask'),
      'real ask',
    );
  });

  it('strips IDE context blocks (ide_opened_file / ide_selection)', () => {
    const input =
      '<ide_opened_file path="a.ts">x</ide_opened_file><ide_selection>sel</ide_selection>run the tests';
    assert.equal(stripInjectedContextTags(input), 'run the tests');
  });

  it('strips local slash-command marker families (command-* / local-command-*)', () => {
    const input =
      '<command-name>/clear</command-name><command-args></command-args><local-command-stdout>ok</local-command-stdout>deploy it';
    assert.equal(stripInjectedContextTags(input), 'deploy it');
  });

  it('PRESERVES user-typed lowercase HTML like <code>foo</code> (not in the allowlist)', () => {
    const input = 'use <code>foo</code> inside the <pre>block</pre>';
    assert.equal(stripInjectedContextTags(input), input);
  });

  it('does NOT strip a generic lowercase block outside the injected allowlist', () => {
    // stripDisplayTags WOULD strip this; the narrow stripper must not.
    const input = '<context>file: a.ts</context>\nrefactor';
    assert.equal(stripInjectedContextTags(input), input);
  });

  it('leaves uppercase JSX and unpaired brackets untouched', () => {
    assert.equal(
      stripInjectedContextTags('fix the <Button> when x < y'),
      'fix the <Button> when x < y',
    );
  });

  it('keeps adjacent injected blocks separate via the backreference', () => {
    const input = '<system>a</system>KEEP<system-reminder>b</system-reminder>';
    assert.equal(stripInjectedContextTags(input), 'KEEP');
  });

  it('returns EMPTY when the whole turn is injected context (caller can skip it)', () => {
    assert.equal(
      stripInjectedContextTags('<system-reminder>only noise</system-reminder>\n'),
      '',
    );
  });

  it('is a no-op for plain text', () => {
    assert.equal(stripInjectedContextTags('just a prompt'), 'just a prompt');
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

describe('extractTag (generic named-tag extraction primitive)', () => {
  it('extracts the inner content of a named tag block', () => {
    assert.equal(extractTag('<command-name>/clear</command-name>', 'command-name'), '/clear');
    assert.equal(
      extractTag('before <local-command-stdout>line1\nline2</local-command-stdout> after', 'local-command-stdout'),
      'line1\nline2',
    );
  });

  it('is case-insensitive and tolerates attributes on the opening tag', () => {
    assert.equal(extractTag('<CommandName>/x</CommandName>', 'commandname'), '/x');
    assert.equal(extractTag('<bash-input lang="sh">ls -la</bash-input>', 'bash-input'), 'ls -la');
  });

  it('returns null when the tag is absent or unmatched', () => {
    assert.equal(extractTag('plain text, no tags', 'command-name'), null);
    assert.equal(extractTag('<command-name>/x', 'command-name'), null); // no closing tag
  });

  it('returns null for empty/blank html or tagName', () => {
    assert.equal(extractTag('', 'tag'), null);
    assert.equal(extractTag('   ', 'tag'), null);
    assert.equal(extractTag('<tag>x</tag>', ''), null);
    assert.equal(extractTag('<tag>x</tag>', '   '), null);
  });

  it('returns null when the matched block has empty content', () => {
    assert.equal(extractTag('<tag></tag>', 'tag'), null);
  });

  it('escapes regex metacharacters in the tag name (literal match)', () => {
    assert.equal(extractTag('<a.b>x</a.b>', 'a.b'), 'x');
    assert.equal(extractTag('<aXb>x</aXb>', 'a.b'), null); // '.' must NOT match 'X'
  });

  it('is nesting-aware: the non-greedy body stops at the first close (parity with reference)', () => {
    const html = '<box><box>inner</box> outer</box>';
    assert.equal(extractTag(html, 'box'), '<box>inner');
  });

  it('skips a match whose preceding tags leave it at a non-zero nesting depth', () => {
    // A leading unmatched close tag puts the later block at depth -1 → skipped → null.
    assert.equal(extractTag('foo</box><box>bar</box>', 'box'), null);
  });

  it('matches the first depth-0 block when several siblings exist', () => {
    assert.equal(extractTag('<t>first</t><t>second</t>', 't'), 'first');
  });
});
