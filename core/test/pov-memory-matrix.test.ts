import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  MEMORY_TYPES,
  matchesPaths,
  sanitizeMemoryContent,
  parseFrontmatterList,
  type MemoryType,
} from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

// ---------------------------------------------------------------------------
// Harness: isolate ALL journal/config/memory I/O under a temp SUPERINFERENCE_DATA_DIR
// so projectStateDir() resolves inside our sandbox, never the real ~/.ami.
// ---------------------------------------------------------------------------
let cwd: string;
let dataDir: string;
let savedDataEnv: string | undefined;
let savedCfgEnv: string | undefined;
let mgr: MemoryManager;

/** The actual on-disk dir saveMemory()/loadMemories() use. */
function memoryDir(): string {
  return projectStateDir(cwd, 'memory');
}

function writeRawMemory(filename: string, content: string): string {
  const dir = memoryDir();
  fs.mkdirSync(dir, { recursive: true });
  const p = path.join(dir, filename);
  fs.writeFileSync(p, content, 'utf-8');
  return p;
}

beforeEach(() => {
  cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memmatrix-cwd-'));
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memmatrix-data-'));
  savedDataEnv = process.env.SUPERINFERENCE_DATA_DIR;
  savedCfgEnv = process.env.CLAUDE_CONFIG_DIR;
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  // CLAUDE_CONFIG_DIR wins over SUPERINFERENCE_DATA_DIR in resolveConfigDir, so clear it.
  delete process.env.CLAUDE_CONFIG_DIR;
  mgr = new MemoryManager(cwd);
});

afterEach(() => {
  if (savedDataEnv === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
  else process.env.SUPERINFERENCE_DATA_DIR = savedDataEnv;
  if (savedCfgEnv === undefined) delete process.env.CLAUDE_CONFIG_DIR;
  else process.env.CLAUDE_CONFIG_DIR = savedCfgEnv;
  fs.rmSync(cwd, { recursive: true, force: true });
  fs.rmSync(dataDir, { recursive: true, force: true });
});

// ===========================================================================
// MEMORY_TYPES constant
// ===========================================================================
describe('MEMORY_TYPES', () => {
  it('is exactly the four documented categories, in order', () => {
    assert.deepEqual([...MEMORY_TYPES], ['user', 'feedback', 'project', 'reference']);
  });

  it('every member round-trips through saveMemory/loadMemories as memoryType', () => {
    for (const t of MEMORY_TYPES) {
      const m = new MemoryManager(cwd);
      m.saveMemory(`mem-${t}`, `body for ${t}`, `desc ${t}`, t as MemoryType);
    }
    const loaded = mgr.loadMemories();
    const byName = new Map(loaded.map(e => [e.name, e]));
    for (const t of MEMORY_TYPES) {
      assert.equal(byName.get(`mem-${t}`)?.memoryType, t);
    }
  });
});

// ===========================================================================
// saveMemory + loadMemories round-trip (add / list)
// ===========================================================================
describe('MemoryManager save/load round-trip', () => {
  it('persists name, content, description, and type', () => {
    mgr.saveMemory('Deploy Notes', 'ssh to the box and run make', 'how to deploy', 'reference');
    const loaded = mgr.loadMemories();
    assert.equal(loaded.length, 1);
    const e = loaded[0];
    assert.equal(e.name, 'Deploy Notes');
    assert.equal(e.description, 'how to deploy');
    assert.equal(e.memoryType, 'reference');
    assert.equal(e.type, 'memory');
    assert.ok(e.content.includes('ssh to the box and run make'));
    // file written under the sandboxed data dir, not the real home
    assert.ok(e.path.startsWith(dataDir));
  });

  it('defaults memoryType to project and description to the name', () => {
    mgr.saveMemory('solo', 'just a body');
    const e = mgr.loadMemories()[0];
    assert.equal(e.memoryType, 'project');
    assert.equal(e.description, 'solo');
  });

  it('sanitizes the filename: special chars collapse to a single underscore, lowercased', () => {
    mgr.saveMemory('My Fancy!! Name', 'content here');
    const files = fs.readdirSync(memoryDir()).filter(f => f.endsWith('.md'));
    assert.deepEqual(files, ['my_fancy_name.md']);
    // but the frontmatter name field keeps the human-readable original
    assert.equal(mgr.loadMemories()[0].name, 'My Fancy!! Name');
  });

  it('returns [] when the memory dir does not exist yet', () => {
    assert.deepEqual(mgr.loadMemories(), []);
  });

  it('sorts loaded memories newest-first by mtime', () => {
    mgr.saveMemory('oldest', 'a');
    mgr.saveMemory('middle', 'b');
    mgr.saveMemory('newest', 'c');
    const dir = memoryDir();
    const now = Date.now();
    fs.utimesSync(path.join(dir, 'oldest.md'), new Date(now - 30000), new Date(now - 30000));
    fs.utimesSync(path.join(dir, 'middle.md'), new Date(now - 15000), new Date(now - 15000));
    fs.utimesSync(path.join(dir, 'newest.md'), new Date(now), new Date(now));
    const names = mgr.loadMemories().map(e => e.name);
    assert.deepEqual(names, ['newest', 'middle', 'oldest']);
  });
});

// ===========================================================================
// update (overwrite) + delete
// ===========================================================================
describe('MemoryManager update + delete', () => {
  it('overwrites a memory with the same name (update) rather than duplicating', () => {
    mgr.saveMemory('topic', 'first version');
    mgr.saveMemory('topic', 'second version', 'now with desc', 'feedback');
    const loaded = mgr.loadMemories();
    assert.equal(loaded.length, 1);
    assert.ok(loaded[0].content.includes('second version'));
    assert.ok(!loaded[0].content.includes('first version'));
    assert.equal(loaded[0].memoryType, 'feedback');
    assert.equal(loaded[0].description, 'now with desc');
  });

  it('reflects on-disk deletion in the next list (delete)', () => {
    mgr.saveMemory('keep', 'x');
    mgr.saveMemory('remove', 'y');
    assert.equal(mgr.loadMemories().length, 2);
    fs.rmSync(path.join(memoryDir(), 'remove.md'));
    const names = mgr.loadMemories().map(e => e.name);
    assert.deepEqual(names, ['keep']);
  });
});

// ===========================================================================
// readMemory — case-insensitive lookup by name
// ===========================================================================
describe('MemoryManager.readMemory', () => {
  it('returns the sanitized body for a case-insensitive name match', () => {
    mgr.saveMemory('Secrets', 'the password is swordfish');
    const body = mgr.readMemory('secrets');
    assert.ok(body && body.includes('the password is swordfish'));
  });

  it('returns null when no memory matches', () => {
    mgr.saveMemory('present', 'x');
    assert.equal(mgr.readMemory('absent'), null);
  });

  it('sanitizes system tags in the returned body', () => {
    mgr.saveMemory('tagged', 'before <system-reminder>evil</system-reminder> after');
    const body = mgr.readMemory('tagged')!;
    assert.ok(!body.includes('<system-reminder>'));
    assert.ok(body.includes('＜system-reminder＞'));
  });
});

// ===========================================================================
// matchesPaths — glob / path matching true + false
// ===========================================================================
describe('matchesPaths', () => {
  it('matches a single-star glob against a flat filename but not across slashes', () => {
    assert.equal(matchesPaths(['src/*.ts'], 'src/index.ts'), true);
    assert.equal(matchesPaths(['src/*.ts'], 'src/nested/index.ts'), false);
  });

  it('matches a double-star glob across directory separators', () => {
    assert.equal(matchesPaths(['src/**/*.ts'], 'src/a/b/c.ts'), true);
    assert.equal(matchesPaths(['src/**/*.ts'], 'lib/a.ts'), false);
  });

  it('honors the ? single-character wildcard (not a slash)', () => {
    assert.equal(matchesPaths(['file?.ts'], 'file1.ts'), true);
    assert.equal(matchesPaths(['file?.ts'], 'file12.ts'), false);
    assert.equal(matchesPaths(['a?b'], 'a/b'), false);
  });

  it('escapes regex metacharacters in the glob (dot is literal)', () => {
    assert.equal(matchesPaths(['a.b'], 'a.b'), true);
    assert.equal(matchesPaths(['a.b'], 'axb'), false);
  });

  it('requires a full-string match, not a substring', () => {
    assert.equal(matchesPaths(['foo'], 'foobar'), false);
    assert.equal(matchesPaths(['foo'], 'foo'), true);
  });

  it('returns false for an empty glob list', () => {
    assert.equal(matchesPaths([], 'anything.ts'), false);
  });

  it('returns true if ANY glob in the list matches', () => {
    assert.equal(matchesPaths(['*.js', '*.ts'], 'main.ts'), true);
  });
});

// ===========================================================================
// parseFrontmatterList — supports the paths: frontmatter field
// ===========================================================================
describe('parseFrontmatterList', () => {
  it('splits a comma-separated list and trims entries', () => {
    assert.deepEqual(parseFrontmatterList('src/*.ts, lib/*.ts , x'), ['src/*.ts', 'lib/*.ts', 'x']);
  });

  it('drops empty segments and returns [] for empty input', () => {
    assert.deepEqual(parseFrontmatterList(''), []);
    assert.deepEqual(parseFrontmatterList('a,,b,'), ['a', 'b']);
  });
});

// ===========================================================================
// sanitizeMemoryContent — strips invisibles, neutralizes system tags
// ===========================================================================
describe('sanitizeMemoryContent', () => {
  it('strips zero-width / invisible characters', () => {
    const dirty = 'hel​lo‍world﻿';
    assert.equal(sanitizeMemoryContent(dirty), 'helloworld');
  });

  it('neutralizes opening and closing system/claude/anthropic tags to fullwidth brackets', () => {
    const out = sanitizeMemoryContent('<system>x</system> <claude>y</claude> <anthropic>z</anthropic>');
    assert.ok(!out.includes('<system>'));
    assert.ok(!out.includes('</claude>'));
    assert.ok(out.includes('＜system＞'));
    assert.ok(out.includes('＜/claude＞'));
    assert.ok(out.includes('＜anthropic＞'));
  });

  it('matches the cc[a-z]* tag family', () => {
    const out = sanitizeMemoryContent('<ccprompt>hi</ccprompt>');
    assert.ok(!out.includes('<ccprompt>'));
    assert.ok(out.includes('＜ccprompt＞'));
  });

  it('leaves ordinary content untouched', () => {
    const clean = 'a < b and c > d, plain text';
    assert.equal(sanitizeMemoryContent(clean), clean);
  });

  it('is idempotent on already-sanitized content', () => {
    const once = sanitizeMemoryContent('<system>x</system>');
    assert.equal(sanitizeMemoryContent(once), once);
  });
});

// ===========================================================================
// Stale-memory marking (ageDays >= 30 days)
// ===========================================================================
describe('loadMemories stale marking', () => {
  it('prepends a STALE banner and reports months-ago freshness for old memories', () => {
    mgr.saveMemory('ancient', 'original body text');
    const p = path.join(memoryDir(), 'ancient.md');
    const old = Date.now() - 40 * 24 * 60 * 60 * 1000;
    fs.utimesSync(p, new Date(old), new Date(old));
    const e = mgr.loadMemories()[0];
    assert.ok(e.content.startsWith('[STALE'));
    assert.ok(e.content.includes('original body text'));
    assert.equal(e.ageDays, 40);
    assert.equal(e.freshness, '1 months ago');
  });

  it('marks a fresh memory as today with no STALE banner', () => {
    mgr.saveMemory('fresh', 'brand new');
    const e = mgr.loadMemories()[0];
    assert.equal(e.ageDays, 0);
    assert.equal(e.freshness, 'today');
    assert.ok(!e.content.startsWith('[STALE'));
  });
});

// ===========================================================================
// Corrupt-store tolerance
// ===========================================================================
describe('loadMemories corrupt-store tolerance', () => {
  it('skips a .md path that is actually a directory (EISDIR) and keeps the good ones', () => {
    mgr.saveMemory('good', 'valid body');
    fs.mkdirSync(path.join(memoryDir(), 'trap.md'));
    const names = mgr.loadMemories().map(e => e.name);
    assert.deepEqual(names, ['good']);
  });

  it('ignores non-.md files entirely', () => {
    mgr.saveMemory('kept', 'x');
    writeRawMemory('notes.txt', 'ignored');
    writeRawMemory('data.json', '{}');
    assert.deepEqual(mgr.loadMemories().map(e => e.name), ['kept']);
  });

  it('tolerates a file with no frontmatter: name falls back to the filename, type undefined', () => {
    writeRawMemory('bare.md', 'just a body, no frontmatter at all');
    const e = mgr.loadMemories().find(m => m.name === 'bare')!;
    assert.ok(e);
    assert.equal(e.memoryType, undefined);
    assert.ok(e.content.includes('just a body'));
  });

  it('treats an unknown frontmatter type as undefined memoryType', () => {
    writeRawMemory('weird.md', '---\nname: weird\ntype: bogus-category\n---\nbody');
    const e = mgr.loadMemories().find(m => m.name === 'weird')!;
    assert.equal(e.memoryType, undefined);
  });

  it('tolerates an unterminated frontmatter block (no closing ---)', () => {
    writeRawMemory('half.md', '---\nname: half\ntype: project\nbody runs on without a terminator');
    const e = mgr.loadMemories().find(m => m.name === 'half')!;
    assert.ok(e);
    // No closing fence => whole raw is the body, name falls back to filename.
    assert.equal(e.name, 'half');
  });
});

// ===========================================================================
// MEMORY.md index CRUD (add / read / remove)
// ===========================================================================
describe('MemoryManager MEMORY.md index CRUD', () => {
  it('adds entries, reads them back, and dedupes identical adds', async () => {
    const e1 = '- [alpha](alpha.md) — first topic';
    const e2 = '- [beta](beta.md) — second topic';
    await mgr.addToMemoryIndex(cwd, e1);
    await mgr.addToMemoryIndex(cwd, e2);
    await mgr.addToMemoryIndex(cwd, e1); // duplicate — must not double-write
    const idx = await mgr.readMemoryIndex(cwd);
    assert.deepEqual(idx, [e1, e2]);
  });

  it('removes an entry by substring match', async () => {
    const e1 = '- [alpha](alpha.md) — first';
    const e2 = '- [beta](beta.md) — second';
    await mgr.addToMemoryIndex(cwd, e1);
    await mgr.addToMemoryIndex(cwd, e2);
    await mgr.removeFromMemoryIndex(cwd, 'alpha.md');
    const idx = await mgr.readMemoryIndex(cwd);
    assert.deepEqual(idx, [e2]);
  });

  it('readMemoryIndex returns [] when no index file exists', async () => {
    assert.deepEqual(await mgr.readMemoryIndex(cwd), []);
  });

  it('removeFromMemoryIndex on a missing file is a no-op (best effort)', async () => {
    await assert.doesNotReject(mgr.removeFromMemoryIndex(cwd, 'whatever'));
  });
});
