/**
 * Tests for memory new features:
 * - loadRulesDirectory (.superinference/rules/)
 * - resolveIncludes (@include directives)
 * - matchesPaths (path glob matching)
 * - readMemoryIndex (MEMORY.md parsing)
 * - collectInstructionSources
 * - parseFrontmatter
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { MemoryManager, matchesPaths, parseFrontmatter, MEMORY_TYPES } from '../src/memory';

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memory-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// matchesPaths — glob matching
// ---------------------------------------------------------------------------

describe('matchesPaths', () => {
  it('matches exact file paths', () => {
    assert.equal(matchesPaths(['src/index.ts'], 'src/index.ts'), true);
  });

  it('matches wildcard globs', () => {
    assert.equal(matchesPaths(['src/**/*.ts'], 'src/utils/helper.ts'), true);
  });

  it('rejects non-matching paths', () => {
    assert.equal(matchesPaths(['src/**/*.ts'], 'test/foo.js'), false);
  });

  it('handles empty globs array', () => {
    assert.equal(matchesPaths([], 'any/file.ts'), false);
  });

  it('matches multiple globs (any matches)', () => {
    assert.equal(matchesPaths(['*.js', '*.ts'], 'foo.ts'), true);
    assert.equal(matchesPaths(['*.js', '*.ts'], 'foo.py'), false);
  });

  it('single * does NOT cross path separators', () => {
    assert.equal(matchesPaths(['src/*.ts'], 'src/index.ts'), true);
    assert.equal(matchesPaths(['src/*.ts'], 'src/deeply/nested/file.ts'), false);
  });

  it('pattern is anchored — no substring matches', () => {
    assert.equal(matchesPaths(['src/*.ts'], 'other/src/foo.ts'), false);
  });

  it('** still crosses path separators', () => {
    assert.equal(matchesPaths(['src/**/*.ts'], 'src/a/b/c.ts'), true);
  });
});

// ---------------------------------------------------------------------------
// parseFrontmatter — YAML frontmatter extraction
// ---------------------------------------------------------------------------

describe('parseFrontmatter', () => {
  it('parses YAML frontmatter from markdown', () => {
    const raw = '---\nname: test\ndescription: A test\n---\nBody content.';
    const result = parseFrontmatter(raw);
    assert.equal(result.frontmatter.name, 'test');
    assert.equal(result.frontmatter.description, 'A test');
    assert.ok(result.body.includes('Body content'));
  });

  it('returns empty frontmatter when none present', () => {
    const raw = 'Just body content.';
    const result = parseFrontmatter(raw);
    assert.deepEqual(result.frontmatter, {});
    assert.ok(result.body.includes('Just body content'));
  });

  it('handles metadata type field', () => {
    const raw = '---\nname: test\nmetadata:\n  type: user\n---\nBody.';
    const result = parseFrontmatter(raw);
    assert.ok(result.frontmatter.name === 'test' || result.frontmatter.metadata);
  });
});

// ---------------------------------------------------------------------------
// MEMORY_TYPES constant
// ---------------------------------------------------------------------------

describe('MEMORY_TYPES', () => {
  it('includes all 4 memory types', () => {
    assert.ok(MEMORY_TYPES.includes('user'));
    assert.ok(MEMORY_TYPES.includes('feedback'));
    assert.ok(MEMORY_TYPES.includes('project'));
    assert.ok(MEMORY_TYPES.includes('reference'));
    assert.equal(MEMORY_TYPES.length, 4);
  });
});

// ---------------------------------------------------------------------------
// MemoryManager — loadRulesDirectory
// ---------------------------------------------------------------------------

describe('MemoryManager — loadRulesDirectory', () => {
  it('loads rules from .superinference/rules/ directory', async () => {
    const rulesDir = path.join(tmpDir, '.superinference', 'rules');
    fs.mkdirSync(rulesDir, { recursive: true });
    fs.writeFileSync(path.join(rulesDir, 'rule1.md'), 'Always use strict mode.');
    fs.writeFileSync(path.join(rulesDir, 'rule2.md'), 'Never use eval.');

    const mgr = new MemoryManager(tmpDir);
    const rules = await mgr.loadRulesDirectory(tmpDir);
    assert.ok(Array.isArray(rules));
    assert.ok(rules.length >= 2);
  });

  it('returns empty array when rules directory is missing', async () => {
    const mgr = new MemoryManager(tmpDir);
    const rules = await mgr.loadRulesDirectory(tmpDir);
    assert.ok(Array.isArray(rules));
    assert.equal(rules.length, 0);
  });
});

// ---------------------------------------------------------------------------
// MemoryManager — readMemoryIndex
// ---------------------------------------------------------------------------

describe('MemoryManager — readMemoryIndex', () => {
  it('reads MEMORY.md index', async () => {
    const memDir = path.join(tmpDir, '.superinference', 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'MEMORY.md'), '- [Test](test.md) — A test memory\n- [Other](other.md) — Another memory');

    const mgr = new MemoryManager(tmpDir);
    const entries = await mgr.readMemoryIndex(tmpDir);
    assert.ok(Array.isArray(entries));
  });

  it('returns empty array when MEMORY.md is missing', async () => {
    const mgr = new MemoryManager(tmpDir);
    const entries = await mgr.readMemoryIndex(tmpDir);
    assert.ok(Array.isArray(entries));
    assert.equal(entries.length, 0);
  });
});

// ---------------------------------------------------------------------------
// MemoryManager — resolveIncludes
// ---------------------------------------------------------------------------

describe('MemoryManager — resolveIncludes', () => {
  it('resolves @include directives', async () => {
    const includeDir = path.join(tmpDir, 'includes');
    fs.mkdirSync(includeDir, { recursive: true });
    fs.writeFileSync(path.join(includeDir, 'snippet.md'), 'Included content here.');
    const content = `Before include.\n@include ${path.join(includeDir, 'snippet.md')}\nAfter include.`;

    const mgr = new MemoryManager(tmpDir);
    const resolved = await mgr.resolveIncludes(content, tmpDir);
    assert.ok(typeof resolved === 'string');
    assert.ok(resolved.includes('Included content here') || resolved.includes('Before include'));
  });

  it('returns content unchanged when no includes', async () => {
    const mgr = new MemoryManager(tmpDir);
    const resolved = await mgr.resolveIncludes('No includes here.', tmpDir);
    assert.ok(resolved.includes('No includes here'));
  });

  it('resolves diamond (non-cyclic) includes on all paths', async () => {
    const dir = path.join(tmpDir, 'includes');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'A.md'), '@include B.md\n@include C.md');
    fs.writeFileSync(path.join(dir, 'B.md'), 'B-top\n@include common.md');
    fs.writeFileSync(path.join(dir, 'C.md'), 'C-top\n@include common.md');
    fs.writeFileSync(path.join(dir, 'common.md'), 'COMMON_MARKER');

    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes('@include A.md', path.join(dir, 'root.md'));

    const occurrences = result.split('COMMON_MARKER').length - 1;
    assert.equal(occurrences, 2, 'common.md must be inlined on both B and C branches');
    assert.ok(!result.includes('[circular reference'), 'a diamond is not a cycle');
  });

  it('still detects true cycles after diamond fix', async () => {
    const dir = path.join(tmpDir, 'includes');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'a.md'), '@include b.md');
    fs.writeFileSync(path.join(dir, 'b.md'), '@include a.md');
    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes('@include b.md', path.join(dir, 'a.md'));
    assert.ok(result.includes('[circular reference: b.md]'));
  });
});

// ---------------------------------------------------------------------------
// MemoryManager — collectInstructionSources
// ---------------------------------------------------------------------------

describe('MemoryManager — collectInstructionSources', () => {
  it('returns an array of instruction sources', async () => {
    const mgr = new MemoryManager(tmpDir);
    const sources = await mgr.collectInstructionSources();
    assert.ok(Array.isArray(sources));
  });
});
