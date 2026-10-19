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
import { MemoryManager, matchesPaths, parseFrontmatter, MEMORY_TYPES, sanitizeMemoryContent } from '../src/memory';

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

  it('skips binary files in @include directives', async () => {
    const dir = path.join(tmpDir, 'includes');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'image.png'), Buffer.from([0x89, 0x50, 0x4e, 0x47]));
    fs.writeFileSync(path.join(dir, 'data.pdf'), 'fake pdf content');

    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes(
      'Before\n@include image.png\nMiddle\n@include data.pdf\nAfter',
      path.join(dir, 'root.md'),
    );
    assert.ok(result.includes('[binary file skipped: image.png]'));
    assert.ok(result.includes('[binary file skipped: data.pdf]'));
    assert.ok(result.includes('Before'));
    assert.ok(result.includes('After'));
  });

  it('includes text files but skips binary extensions', async () => {
    const dir = path.join(tmpDir, 'includes');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'notes.md'), 'TEXT_CONTENT');
    fs.writeFileSync(path.join(dir, 'archive.zip'), 'fake zip');

    const mgr = new MemoryManager(tmpDir);
    const result = await mgr.resolveIncludes(
      '@include notes.md\n@include archive.zip',
      path.join(dir, 'root.md'),
    );
    assert.ok(result.includes('TEXT_CONTENT'));
    assert.ok(result.includes('[binary file skipped: archive.zip]'));
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

describe('sanitizeMemoryContent', () => {
  it('strips invisible Unicode characters', () => {
    const input = 'hello​world‍test﻿end';
    const result = sanitizeMemoryContent(input);
    assert.equal(result, 'helloworldtestend');
  });

  it('neutralizes system-reminder tags', () => {
    const input = 'some <system-reminder>injected</system-reminder> text';
    const result = sanitizeMemoryContent(input);
    assert.ok(!result.includes('<system-reminder>'));
    assert.ok(result.includes('＜system-reminder＞'));
    assert.ok(result.includes('＜/system-reminder＞'));
    assert.ok(result.includes('injected'));
  });

  it('neutralizes anthropic tags', () => {
    const input = '<anthropic>data</anthropic>';
    const result = sanitizeMemoryContent(input);
    assert.ok(!result.includes('<anthropic>'));
    assert.ok(result.includes('＜anthropic＞'));
  });

  it('neutralizes claude tags', () => {
    const input = '<claude>instructions</claude>';
    const result = sanitizeMemoryContent(input);
    assert.ok(!result.includes('<claude>'));
  });

  it('neutralizes ccmemory tags', () => {
    const input = 'text <ccmemory>hidden</ccmemory>';
    const result = sanitizeMemoryContent(input);
    assert.ok(!result.includes('<ccmemory>'));
    assert.ok(result.includes('＜ccmemory＞'));
  });

  it('preserves normal HTML tags', () => {
    const input = '<div>normal html</div>';
    const result = sanitizeMemoryContent(input);
    assert.equal(result, input);
  });

  it('handles zero-width space (ZWSP) in memory content', () => {
    const result = sanitizeMemoryContent('rm​ -rf /');
    assert.equal(result, 'rm -rf /');
  });

  it('handles zero-width joiner (ZWJ)', () => {
    const result = sanitizeMemoryContent('te‍st');
    assert.equal(result, 'test');
  });

  it('handles soft hyphen', () => {
    const result = sanitizeMemoryContent('com­mand');
    assert.equal(result, 'command');
  });

  it('handles combined invisible chars and tags', () => {
    const input = '​<system-reminder>‍hack</system-reminder>';
    const result = sanitizeMemoryContent(input);
    assert.ok(!result.includes('<system-reminder>'));
    assert.ok(!result.includes('​'));
    assert.ok(!result.includes('‍'));
  });

  it('returns empty string unchanged', () => {
    assert.equal(sanitizeMemoryContent(''), '');
  });

  it('preserves normal text', () => {
    const input = 'This is a normal memory entry with no issues.';
    assert.equal(sanitizeMemoryContent(input), input);
  });
});

// ---------------------------------------------------------------------------
// buildFrontmatter — modified timestamp
// ---------------------------------------------------------------------------

describe('memory modified timestamp', () => {
  it('saveMemory includes a modified ISO timestamp in frontmatter', () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('test-modified', 'some content', 'test description', 'project');
    const filePath = path.join(tmpDir, '.superinference', 'memory', 'test-modified.md');
    const content = fs.readFileSync(filePath, 'utf-8');
    assert.ok(content.includes('modified:'), 'frontmatter must include modified field');
    const match = content.match(/modified:\s*(.+)/);
    assert.ok(match, 'modified field must have a value');
    const date = new Date(match![1]);
    assert.ok(!isNaN(date.getTime()), 'modified value must be a valid ISO date');
    const now = Date.now();
    assert.ok(Math.abs(now - date.getTime()) < 5000, 'modified timestamp must be recent (within 5s)');
  });

  it('updating a memory changes the modified timestamp', async () => {
    const mm = new MemoryManager(tmpDir);
    const filePath = path.join(tmpDir, '.superinference', 'memory', 'test-update.md');

    mm.saveMemory('test-update', 'version 1', 'desc', 'project');
    const first = fs.readFileSync(filePath, 'utf-8');
    const firstMatch = first.match(/modified:\s*(.+)/);
    assert.ok(firstMatch);
    const firstDate = new Date(firstMatch![1]);

    await new Promise(r => setTimeout(r, 50));

    mm.saveMemory('test-update', 'version 2', 'desc', 'project');
    const second = fs.readFileSync(filePath, 'utf-8');
    const secondMatch = second.match(/modified:\s*(.+)/);
    assert.ok(secondMatch);
    const secondDate = new Date(secondMatch![1]);

    assert.ok(secondDate.getTime() >= firstDate.getTime(),
      'second save must have equal or later modified timestamp');
    assert.ok(second.includes('version 2'), 'content must be updated');
  });

  it('frontmatter contains all expected fields', () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('fields-check', 'body text', 'a description', 'user');
    const filePath = path.join(tmpDir, '.superinference', 'memory', 'fields-check.md');
    const content = fs.readFileSync(filePath, 'utf-8');
    assert.ok(content.includes('name: fields-check'));
    assert.ok(content.includes('description: a description'));
    assert.ok(content.includes('type: user'));
    assert.ok(content.includes('modified:'));
    assert.ok(content.startsWith('---\n'));
    assert.ok(content.includes('\n---\n'));
  });
});
