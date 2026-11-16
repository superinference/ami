import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  parseFrontmatter,
  normalizeFrontmatterPaths,
  matchesPaths,
} from '../src/memory';
import { fsHooks, __resetLoggerForTest } from '../src/logger';

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-fm-'));
}
function rm(dir: string): void {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch {}
}

// ── Gap: line-anchored frontmatter delimiter matching ──────────────────────
describe('U016 — line-anchored frontmatter delimiter matching', () => {
  it('does NOT treat a run-together `---foo` as an opener', () => {
    // No end-of-line after the three dashes → not a valid frontmatter opener.
    const raw = '---foo\nname: x\n---\nbody';
    const { frontmatter, body } = parseFrontmatter(raw);
    assert.deepEqual(frontmatter, {});
    assert.equal(body, raw);
  });

  it('a `---` INSIDE a value does not prematurely close the block', () => {
    const raw = [
      '---',
      'name: test',
      'description: a value with --- dashes inline',
      'type: project',
      '---',
      'real body',
    ].join('\n');
    const { frontmatter, body } = parseFrontmatter(raw);
    // Later keys survive instead of leaking into the body.
    assert.equal(frontmatter.name, 'test');
    assert.equal(frontmatter.description, 'a value with --- dashes inline');
    assert.equal(frontmatter.type, 'project');
    assert.equal(body, 'real body');
  });

  it('still parses a well-formed block (closing `---` on its own line)', () => {
    const raw = '---\nname: ok\ntype: user\n---\n\nhello';
    const { frontmatter, body } = parseFrontmatter(raw);
    assert.equal(frontmatter.name, 'ok');
    assert.equal(frontmatter.type, 'user');
    assert.equal(body, 'hello');
  });

  it('tolerates CRLF line endings on the delimiters', () => {
    const raw = '---\r\nname: crlf\r\n---\r\nbody';
    const { frontmatter } = parseFrontmatter(raw);
    assert.equal(frontmatter.name, 'crlf');
  });
});

// ── Gap: diagnostic warning when frontmatter is malformed (with source path) ─
describe('U016 — diagnostic on unterminated frontmatter block', () => {
  const savedLevel = process.env.AMI_DEBUG_LOG_LEVEL;
  beforeEach(() => {
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
    __resetLoggerForTest();
  });
  afterEach(() => {
    mock.restoreAll();
    if (savedLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL;
    else process.env.AMI_DEBUG_LOG_LEVEL = savedLevel;
    __resetLoggerForTest();
  });

  it('logs a warn naming the source path when the block is never closed', () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: string) => {
      lines.push(String(line));
    });
    const raw = '---\nname: broken\nno closing delimiter here';
    const { frontmatter, body } = parseFrontmatter(raw, '/proj/skills/broken.md');
    assert.deepEqual(frontmatter, {});
    assert.equal(body, raw);
    const diag = lines.find(l => l.includes('Failed to parse frontmatter'));
    assert.ok(diag, 'expected a diagnostic log line');
    assert.ok(diag!.includes('/proj/skills/broken.md'), 'diagnostic names the source path');
    assert.ok(diag!.includes('[WARN]'), 'diagnostic is warn level');
  });

  it('does not log for a well-formed block', () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: string) => {
      lines.push(String(line));
    });
    parseFrontmatter('---\nname: ok\n---\nbody', '/proj/ok.md');
    assert.equal(lines.find(l => l.includes('Failed to parse frontmatter')), undefined);
  });
});

// ── Gap: frontmatter `paths` normalization (strip /**, all-** = unconditional) ─
describe('U016 — normalizeFrontmatterPaths', () => {
  it('returns null (unconditional) when every pattern is `**`', () => {
    assert.equal(normalizeFrontmatterPaths(['**']), null);
    assert.equal(normalizeFrontmatterPaths(['**', '**']), null);
  });

  it('treats a trailing `/**` as the directory itself for the all-** decision', () => {
    // `**/**` normalizes to `**` → unconditional.
    assert.equal(normalizeFrontmatterPaths(['**/**']), null);
  });

  it('returns null when the list is empty / all-blank', () => {
    assert.equal(normalizeFrontmatterPaths([]), null);
    assert.equal(normalizeFrontmatterPaths(['']), null);
  });

  it('keeps conditional patterns UNCHANGED so subtree matching still works', () => {
    const globs = normalizeFrontmatterPaths(['src/**']);
    assert.deepEqual(globs, ['src/**']);
    assert.ok(globs && matchesPaths(globs, 'src/foo.ts'));
  });

  it('a mix of ** and a real glob stays conditional', () => {
    assert.deepEqual(normalizeFrontmatterPaths(['**', 'docs/*.md']), ['**', 'docs/*.md']);
  });
});

// ── Integration: loadRulesForFile honors unconditional vs conditional paths ──
describe('U016 — loadRulesForFile path normalization (integration)', () => {
  let cwd: string;
  beforeEach(() => { cwd = tmp(); });
  afterEach(() => { rm(cwd); });

  function writeRule(name: string, frontmatter: string, body: string): void {
    const fp = path.join(cwd, '.ami', 'rules', name);
    fs.mkdirSync(path.dirname(fp), { recursive: true });
    fs.writeFileSync(fp, `---\n${frontmatter}\n---\n${body}\n`, 'utf-8');
  }

  it('loads a `paths: **` rule unconditionally for any file', async () => {
    writeRule('all.md', 'paths: **', 'ALWAYS APPLIES');
    const mgr = new MemoryManager(cwd);
    const sources = await mgr.loadRulesForFile(cwd, 'anything/else.py');
    assert.ok(sources.some(s => s.content.includes('ALWAYS APPLIES')));
  });

  it('a `paths: **/**` rule applies even to a top-level file (unconditional)', async () => {
    // Behavioral discriminator: the pre-fix matcher turns `**/**` into `^.*/.*$`,
    // which does NOT match a slash-less top-level path, so the rule would be
    // skipped. normalizeFrontmatterPaths collapses it to unconditional.
    writeRule('allsub.md', 'paths: **/**', 'DOUBLE STAR');
    const mgr = new MemoryManager(cwd);
    const sources = await mgr.loadRulesForFile(cwd, 'toplevel.py');
    assert.ok(sources.some(s => s.content.includes('DOUBLE STAR')));
  });

  it('a `paths: src/**` rule matches a file under src but not elsewhere', async () => {
    writeRule('src.md', 'paths: src/**', 'SRC RULE');
    const mgr = new MemoryManager(cwd);
    const match = await mgr.loadRulesForFile(cwd, 'src/deep/file.ts');
    assert.ok(match.some(s => s.content.includes('SRC RULE')), 'should match src subtree');
    const noMatch = await mgr.loadRulesForFile(cwd, 'lib/file.ts');
    assert.ok(!noMatch.some(s => s.content.includes('SRC RULE')), 'should not match lib');
  });
});
