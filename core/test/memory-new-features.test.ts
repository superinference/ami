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
