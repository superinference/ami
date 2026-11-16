import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  isExpectedMissing,
  stripHtmlComments,
  clampIndexEntry,
  MAX_INDEX_ENTRY_CHARS,
  computeIndexAppend,
  parseFrontmatter,
  parseFrontmatterList,
  resolveMemoryExcludes,
  resolveMemoryExtraGuidelines,
} from '../src/memory';

// ---------------------------------------------------------------------------
// U016-w66 — memory.ts parity batch. Each describe block maps to one gap:
//  - isExpectedMissing / errno-discriminated memory reads
//  - 0o700 dir / 0o600 file owner-only permissions
//  - YAML block-sequence frontmatter values
//  - block-level HTML comment stripping
//  - bounded index: per-entry char demotion
//  - AMI_MEMORY_EXCLUDES (claudeMdExcludes) instruction-file exclusion
//  - AMI_MEMORY_EXTRA_GUIDELINES (extraGuidelines) prompt injection
//
// The auto-memory store is redirected to an isolated temp dir via AMI_MEMORY_DIR
// so nothing touches the real ~/.ami store.
// ---------------------------------------------------------------------------

let cwdDir: string;
let memDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedExcludes = process.env.AMI_MEMORY_EXCLUDES;
const savedExtra = process.env.AMI_MEMORY_EXTRA_GUIDELINES;
const isPosix = process.platform !== 'win32';

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

beforeEach(() => {
  cwdDir = mkTmp('si-w66-cwd-');
  memDir = mkTmp('si-w66-store-');
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_MEMORY_EXCLUDES;
  delete process.env.AMI_MEMORY_EXTRA_GUIDELINES;
});

afterEach(() => {
  try { fs.rmSync(cwdDir, { recursive: true, force: true }); } catch { /* ignore */ }
  try { fs.rmSync(memDir, { recursive: true, force: true }); } catch { /* ignore */ }
  const restore = (k: string, v: string | undefined) => {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  };
  restore('AMI_MEMORY_DIR', savedMemEnv);
  restore('AMI_MEMORY_EXCLUDES', savedExcludes);
  restore('AMI_MEMORY_EXTRA_GUIDELINES', savedExtra);
});

// ---------------------------------------------------------------------------
// Gap: Error-class discrimination on memory reads (ENOENT = silent fallback)
// ---------------------------------------------------------------------------

describe('isExpectedMissing — errno classifier', () => {
  it('treats ENOENT / ENOTDIR as the expected-missing case', () => {
    assert.equal(isExpectedMissing({ code: 'ENOENT' }), true);
    assert.equal(isExpectedMissing({ code: 'ENOTDIR' }), true);
  });

  it('treats a genuine access/corruption error as NOT missing', () => {
    assert.equal(isExpectedMissing({ code: 'EACCES' }), false);
    assert.equal(isExpectedMissing({ code: 'EISDIR' }), false);
    assert.equal(isExpectedMissing({ code: 'EIO' }), false);
    assert.equal(isExpectedMissing({ code: 'EEXIST' }), false);
  });

  it('treats a code-less / non-object throw as NOT missing (surfaced, not swallowed)', () => {
    assert.equal(isExpectedMissing(new Error('boom')), false);
    assert.equal(isExpectedMissing('nope'), false);
    assert.equal(isExpectedMissing(undefined), false);
    assert.equal(isExpectedMissing({}), false);
  });
});

describe('loadSessionMemory — errno-discriminated read', () => {
  it('returns [] silently when the session file does not exist (ENOENT)', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.deepEqual(mgr.loadSessionMemory('does-not-exist'), []);
  });

  it('returns [] but does NOT crash when the session path is a directory (EISDIR, unexpected)', () => {
    const mgr = new MemoryManager(cwdDir);
    // Create a DIRECTORY where the session file is expected → readFileSync throws
    // EISDIR, the unexpected branch (logged) rather than the silent ENOENT path.
    const dirAsFile = path.join(memDir, 'session-eisdir.md');
    fs.mkdirSync(dirAsFile, { recursive: true });
    assert.deepEqual(mgr.loadSessionMemory('eisdir'), []);
  });

  it('round-trips saved facts (confirms the happy path still parses)', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveSessionMemory('sess1', [
      { fact: 'use pnpm', category: 'convention', confidence: 0.9 },
    ]);
    const loaded = mgr.loadSessionMemory('sess1');
    assert.equal(loaded.length, 1);
    assert.equal(loaded[0].fact, 'use pnpm');
  });
});

// ---------------------------------------------------------------------------
// Gap: Restrictive owner-only permissions (0o700 dir / 0o600 file)
// ---------------------------------------------------------------------------

describe('owner-only memory permissions (0o700 dir / 0o600 file)', () => {
  it('saveMemory writes a 0o600 file inside a 0o700 dir', { skip: !isPosix }, () => {
    const mgr = new MemoryManager(cwdDir);
    const p = mgr.saveMemory('cred-safe', 'body', 'desc', 'project');
    assert.equal(fs.statSync(memDir).mode & 0o777, 0o700, 'memory dir must be owner-only');
    assert.equal(fs.statSync(p).mode & 0o777, 0o600, 'memory file must be owner-only');
  });

  it('two-step index write (MEMORY.md) is also 0o600', { skip: !isPosix }, () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('topic', 'body', 'desc', 'project', 'private', true);
    const idx = path.join(memDir, 'MEMORY.md');
    assert.equal(fs.statSync(idx).mode & 0o777, 0o600);
  });

  it('saveSessionMemory writes a 0o600 file', { skip: !isPosix }, () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveSessionMemory('s', [{ fact: 'x', category: 'decision', confidence: 1 }]);
    const p = path.join(memDir, 'session-s.md');
    assert.equal(fs.statSync(p).mode & 0o777, 0o600);
  });
});

// ---------------------------------------------------------------------------
// Gap: YAML block-sequence (block-list) frontmatter values
// ---------------------------------------------------------------------------

describe('parseFrontmatter — YAML block-sequence values', () => {
  it('collects indented `- item` lists into a comma-joined scalar', () => {
    const raw = [
      '---',
      'name: s',
      'allowed-tools:',
      '  - Bash(gh:*)',
      '  - Edit',
      'arguments:',
      '  - file',
      '  - line',
      '---',
      'body',
    ].join('\n');
    const { frontmatter } = parseFrontmatter(raw);
    assert.equal(frontmatter['allowed-tools'], 'Bash(gh:*), Edit');
    assert.equal(frontmatter['arguments'], 'file, line');
    assert.deepEqual(parseFrontmatterList(frontmatter['allowed-tools']), ['Bash(gh:*)', 'Edit']);
  });

  it('an inline scalar value still wins over a following list', () => {
    const raw = ['---', 'key: inline', '  - not-a-list-item', '---', 'b'].join('\n');
    const { frontmatter } = parseFrontmatter(raw);
    assert.equal(frontmatter['key'], 'inline');
  });
});

// ---------------------------------------------------------------------------
// Gap: Block-level HTML comment stripping (preserve code blocks/spans)
// ---------------------------------------------------------------------------

describe('stripHtmlComments', () => {
  it('returns the input unchanged when there is no comment (fast path)', () => {
    const s = 'plain text, no comments';
    assert.equal(stripHtmlComments(s), s);
  });

  it('strips a block-level comment but keeps surrounding text', () => {
    const out = stripHtmlComments('before\n<!-- editorial note -->\nafter');
    assert.ok(!out.includes('editorial note'), 'comment removed');
    assert.ok(out.includes('before') && out.includes('after'));
  });

  it('preserves comment syntax inside a fenced code block', () => {
    const src = 'text\n```html\n<!-- kept in code -->\n```\nmore';
    const out = stripHtmlComments(src);
    assert.ok(out.includes('<!-- kept in code -->'), 'fenced code is preserved');
  });

  it('preserves a comment inside an inline code span', () => {
    const out = stripHtmlComments('use `<!-- x -->` as the marker');
    assert.ok(out.includes('`<!-- x -->`'), 'inline code span preserved');
  });

  it('leaves an unclosed comment in place (typo must not swallow the file)', () => {
    const src = 'keep this\n<!-- oops never closed\nand this too';
    assert.equal(stripHtmlComments(src), src);
  });

  it('is applied when loading project instructions', () => {
    fs.writeFileSync(
      path.join(cwdDir, 'AGENTS.md'),
      '# Rules\n<!-- TODO: internal note, do not show the model -->\nAlways run tests.',
      'utf-8',
    );
    const ctx = new MemoryManager(cwdDir).loadProjectInstructions();
    assert.ok(ctx.includes('Always run tests.'));
    assert.ok(!ctx.includes('internal note'), 'HTML comment stripped from instructions');
  });
});

// ---------------------------------------------------------------------------
// Gap: Bounded index — per-entry char demotion
// ---------------------------------------------------------------------------

describe('clampIndexEntry / computeIndexAppend — per-entry bound', () => {
  it('passes a short entry through unchanged', () => {
    const e = '- [t](t.md) — short';
    assert.equal(clampIndexEntry(e), e);
  });

  it('demotes an over-long entry with an ellipsis', () => {
    const e = '- [t](t.md) — ' + 'x'.repeat(400);
    const c = clampIndexEntry(e);
    assert.ok(c.length <= MAX_INDEX_ENTRY_CHARS, `clamped length ${c.length} <= ${MAX_INDEX_ENTRY_CHARS}`);
    assert.ok(c.endsWith('…'));
  });

  it('computeIndexAppend clamps on write and dedups on the clamped form', () => {
    const entry = '- [t](t.md) — ' + 'y'.repeat(400);
    const first = computeIndexAppend('', entry);
    assert.ok(first !== null);
    const clamped = clampIndexEntry(entry);
    assert.ok(first!.includes(clamped));
    // Re-appending the same (clamped) entry is a no-op (dedup).
    assert.equal(computeIndexAppend(first!, entry), null);
  });

  it('a long description is demoted in the on-disk MEMORY.md index', () => {
    const mgr = new MemoryManager(cwdDir);
    mgr.saveMemory('big', 'body', 'z'.repeat(400), 'project', 'private', true);
    const idx = fs.readFileSync(path.join(memDir, 'MEMORY.md'), 'utf-8');
    const pointer = idx.split('\n').find(l => l.startsWith('- [big]'))!;
    assert.ok(pointer.length <= MAX_INDEX_ENTRY_CHARS);
    assert.ok(pointer.endsWith('…'));
  });
});

// ---------------------------------------------------------------------------
// Gap: AMI_MEMORY_EXCLUDES (claudeMdExcludes) — instruction-file exclusion
// ---------------------------------------------------------------------------

describe('AMI_MEMORY_EXCLUDES — instruction-file exclusion', () => {
  it('parses comma/colon/semicolon-separated patterns', () => {
    assert.deepEqual(resolveMemoryExcludes({ AMI_MEMORY_EXCLUDES: ' a, b : c ; ' }), ['a', 'b', 'c']);
    assert.deepEqual(resolveMemoryExcludes({}), []);
    assert.deepEqual(resolveMemoryExcludes({ AMI_MEMORY_EXCLUDES: '  ' }), []);
  });

  it('skips a matched project instruction file; loads it otherwise', () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), 'secret project rules', 'utf-8');
    const mgr = new MemoryManager(cwdDir);
    // Without an exclude the file loads.
    assert.ok(mgr.loadProjectInstructions().includes('secret project rules'));
    // With a matching glob it is excluded.
    process.env.AMI_MEMORY_EXCLUDES = '**/AGENTS.md';
    assert.ok(!mgr.loadProjectInstructions().includes('secret project rules'));
  });

  it('excludes a .local.md variant too', () => {
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.local.md'), 'local-only notes', 'utf-8');
    const mgr = new MemoryManager(cwdDir);
    assert.ok(mgr.loadProjectInstructions().includes('local-only notes'));
    process.env.AMI_MEMORY_EXCLUDES = '**/CLAUDE.local.md';
    assert.ok(!mgr.loadProjectInstructions().includes('local-only notes'));
  });

  it('collectInstructionSources honors the exclude list', async () => {
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), 'proj rules body', 'utf-8');
    process.env.AMI_MEMORY_EXCLUDES = '**/AGENTS.md';
    const mgr = new MemoryManager(cwdDir);
    const sources = await mgr.collectInstructionSources();
    assert.ok(!sources.some(s => s.content.includes('proj rules body')));
  });
});

// ---------------------------------------------------------------------------
// Gap: AMI_MEMORY_EXTRA_GUIDELINES (extraGuidelines) prompt injection
// ---------------------------------------------------------------------------

describe('AMI_MEMORY_EXTRA_GUIDELINES — extra memory guidelines', () => {
  it('resolves + sanitizes the env value (empty when unset)', () => {
    assert.equal(resolveMemoryExtraGuidelines({}), '');
    assert.equal(resolveMemoryExtraGuidelines({ AMI_MEMORY_EXTRA_GUIDELINES: '  ' }), '');
    // System-tag neutralization: a hostile value cannot inject a real tag.
    const out = resolveMemoryExtraGuidelines({ AMI_MEMORY_EXTRA_GUIDELINES: '<system>evil</system> rule' });
    assert.ok(!out.includes('<system>'));
    assert.ok(out.includes('rule'));
  });

  it('appends the guidelines to the memory guidance block', () => {
    // A stored memory is required for the guidance block to render.
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(
      path.join(memDir, 'm.md'),
      '---\nname: m\ndescription: d\ntype: project\n---\n\nbody\n',
      'utf-8',
    );
    const mgr = new MemoryManager(cwdDir);
    assert.ok(!mgr.getMemoryContext().includes('Additional memory guidelines'));
    process.env.AMI_MEMORY_EXTRA_GUIDELINES = 'Never store PRD links.';
    const ctx = mgr.getMemoryContext();
    assert.ok(ctx.includes('### Additional memory guidelines'));
    assert.ok(ctx.includes('Never store PRD links.'));
  });
});
