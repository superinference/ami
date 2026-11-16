/**
 * U016 (wave 67) — memory.ts parity gaps:
 *  - real-YAML scalar quote-stripping + inline flow sequence parsing in parseFrontmatter
 *  - diagnostic (named, home-redacted) on malformed frontmatter
 *  - non-destructive saveMemory (name-collision disambiguation, no silent clobber)
 *  - defensive mkdir errno classification in the memory write path
 *  - PII-redacted (home-abbreviated) memory-file paths in diagnostics
 *  - resolveAtImports: #fragment stripping, escaped-space paths, inline-code-span skip
 *  - resolveIncludes: symlink-aware cycle detection
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-w67.test.ts
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  MemoryManager,
  parseFrontmatter,
  parseFrontmatterList,
  stripSurroundingQuotes,
  parseInlineFlowSequence,
} from '../src/memory';
import { fsHooks, __resetLoggerForTest } from '../src/logger';

function tmp(prefix = 'si-mem-w67-'): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}
function rm(dir: string): void {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ }
}

// ── stripSurroundingQuotes (unit) ──────────────────────────────────────────
describe('U016-w67 — stripSurroundingQuotes', () => {
  it('strips matching double quotes and unescapes \\" and \\\\', () => {
    assert.equal(stripSurroundingQuotes('"a: b"'), 'a: b');
    assert.equal(stripSurroundingQuotes('"say \\"hi\\""'), 'say "hi"');
    assert.equal(stripSurroundingQuotes('"c:\\\\path"'), 'c:\\path');
  });
  it('strips matching single quotes and unescapes YAML \'\' → \'', () => {
    assert.equal(stripSurroundingQuotes("'plain'"), 'plain');
    assert.equal(stripSurroundingQuotes("'it''s'"), "it's");
  });
  it('leaves an unquoted or mismatched value unchanged', () => {
    assert.equal(stripSurroundingQuotes('plain'), 'plain');
    assert.equal(stripSurroundingQuotes('"half'), '"half');
    assert.equal(stripSurroundingQuotes('x'), 'x');
    assert.equal(stripSurroundingQuotes(''), '');
  });
});

// ── parseInlineFlowSequence (unit) ─────────────────────────────────────────
describe('U016-w67 — parseInlineFlowSequence', () => {
  it('splits a simple flow sequence', () => {
    assert.deepEqual(parseInlineFlowSequence('[a, b, c]'), ['a', 'b', 'c']);
  });
  it('does not split on a comma inside quotes', () => {
    assert.deepEqual(parseInlineFlowSequence('["a, b", c]'), ['a, b', 'c']);
  });
  it('returns [] for an empty sequence and drops empty items', () => {
    assert.deepEqual(parseInlineFlowSequence('[]'), []);
    assert.deepEqual(parseInlineFlowSequence('[a, , b]'), ['a', 'b']);
  });
  it('returns null for a non-sequence or an unterminated bracket', () => {
    assert.equal(parseInlineFlowSequence('plain'), null);
    assert.equal(parseInlineFlowSequence('[a, b'), null);
  });
});

// ── parseFrontmatter: quote stripping + flow sequence ──────────────────────
describe('U016-w67 — parseFrontmatter quote stripping + flow sequences', () => {
  it('unquotes a quoted scalar so a YAML-injection-safe value round-trips', () => {
    // Discriminator: the pre-fix parser returned the value WITH its quotes.
    const { frontmatter } = parseFrontmatter('---\nname: "foo: bar"\ntype: user\n---\nbody');
    assert.equal(frontmatter.name, 'foo: bar');
    assert.equal(frontmatter.type, 'user');
  });

  it('parses an inline flow sequence into a comma-joined list consumers can split', () => {
    const { frontmatter } = parseFrontmatter('---\npaths: [src/*.ts, lib/*.ts]\n---\nbody');
    assert.equal(frontmatter.paths, 'src/*.ts, lib/*.ts');
    assert.deepEqual(parseFrontmatterList(frontmatter.paths), ['src/*.ts', 'lib/*.ts']);
  });

  it('unquotes each flow-sequence item and coexists with other scalar keys', () => {
    const { frontmatter } = parseFrontmatter('---\nname: rule\npaths: ["src/a.ts", "src/b.ts"]\n---\nx');
    assert.equal(frontmatter.name, 'rule');
    assert.deepEqual(parseFrontmatterList(frontmatter.paths), ['src/a.ts', 'src/b.ts']);
  });
});

// ── parseFrontmatter: diagnostic + PII redaction ───────────────────────────
describe('U016-w67 — malformed-frontmatter diagnostic is named + home-redacted', () => {
  const savedLevel = process.env.AMI_DEBUG_LOG_LEVEL;
  beforeEach(() => { process.env.AMI_DEBUG_LOG_LEVEL = 'debug'; __resetLoggerForTest(); });
  afterEach(() => {
    mock.restoreAll();
    if (savedLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL;
    else process.env.AMI_DEBUG_LOG_LEVEL = savedLevel;
    __resetLoggerForTest();
  });

  it('warns (named) on an unterminated inline flow sequence', () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: string) => { lines.push(String(line)); });
    const { frontmatter } = parseFrontmatter('---\npaths: [a, b\n---\nbody', '/proj/skills/bad.md');
    // Unterminated → kept as the raw literal, but surfaced.
    assert.equal(frontmatter.paths, '[a, b');
    const diag = lines.find(l => l.includes('unterminated flow sequence'));
    assert.ok(diag, 'expected a flow-sequence diagnostic');
    assert.ok(diag!.includes('/proj/skills/bad.md'));
    assert.ok(diag!.includes('[WARN]'));
  });

  it('abbreviates the user home directory in the diagnostic path (PII redaction)', () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: string) => { lines.push(String(line)); });
    const homePath = path.join(os.homedir(), 'notes', 'broken.md');
    parseFrontmatter('---\nname: broken\nno closing delimiter', homePath);
    const diag = lines.find(l => l.includes('Failed to parse frontmatter'));
    assert.ok(diag, 'expected a diagnostic');
    assert.ok(diag!.includes(`~${path.sep}notes${path.sep}broken.md`), diag);
    assert.ok(!diag!.includes(os.homedir()), 'raw home path must not leak');
  });
});

// ── saveMemory: non-destructive write ──────────────────────────────────────
describe('U016-w67 — saveMemory never clobbers an unrelated memory', () => {
  const saved = process.env.AMI_MEMORY_DIR;
  let dir: string;
  beforeEach(() => { dir = tmp(); process.env.AMI_MEMORY_DIR = dir; });
  afterEach(() => {
    if (saved === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = saved;
    rm(dir);
  });

  it('disambiguates two distinct names that normalize to the same filename', () => {
    const mm = new MemoryManager('/does/not/matter');
    const p1 = mm.saveMemory('Foo Bar', 'FIRST_BODY', 'first', 'project');
    const p2 = mm.saveMemory('Foo:Bar', 'SECOND_BODY', 'second', 'project');
    // Discriminator: the pre-fix writer overwrote foo_bar.md, destroying FIRST_BODY.
    assert.notEqual(p1, p2, 'colliding names must not share one file');
    assert.ok(fs.readFileSync(p1, 'utf-8').includes('FIRST_BODY'), 'first memory preserved');
    assert.ok(fs.readFileSync(p2, 'utf-8').includes('SECOND_BODY'), 'second memory written');
  });

  it('still overwrites in place when the SAME memory name is re-saved (update)', () => {
    const mm = new MemoryManager('/does/not/matter');
    const first = mm.saveMemory('My Note', 'V1', 'd', 'project');
    const second = mm.saveMemory('My  Note', 'V2', 'd', 'project'); // extra space collapses the same
    assert.equal(first, second, 'an update targets the same file');
    assert.ok(fs.readFileSync(second, 'utf-8').includes('V2'));
    assert.ok(!fs.readFileSync(second, 'utf-8').includes('V1'));
  });
});

// ── ensureMemoryDirMode: defensive mkdir errno classification ───────────────
describe('U016-w67 — memory write path classifies a failed mkdir', () => {
  const saved = process.env.AMI_MEMORY_DIR;
  const savedLevel = process.env.AMI_DEBUG_LOG_LEVEL;
  let base: string;
  let filePath: string;
  beforeEach(() => {
    base = tmp();
    filePath = path.join(base, 'memory-is-a-file');
    fs.writeFileSync(filePath, 'not a directory');
    process.env.AMI_MEMORY_DIR = filePath; // getMemoryDir returns a FILE
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
    __resetLoggerForTest();
  });
  afterEach(() => {
    mock.restoreAll();
    if (saved === undefined) delete process.env.AMI_MEMORY_DIR; else process.env.AMI_MEMORY_DIR = saved;
    if (savedLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL; else process.env.AMI_DEBUG_LOG_LEVEL = savedLevel;
    __resetLoggerForTest();
    rm(base);
  });

  it('logs a classified warn and rethrows when the memory dir cannot be created', () => {
    const lines: string[] = [];
    mock.method(fsHooks, 'appendFileSync', (_f: unknown, line: string) => { lines.push(String(line)); });
    const mm = new MemoryManager('/does/not/matter');
    assert.throws(() => mm.saveMemory('x', 'body', 'd', 'project'));
    const diag = lines.find(l => l.includes('failed to create memory directory'));
    assert.ok(diag, 'expected a mkdir-failure diagnostic');
    assert.ok(/EEXIST|ENOTDIR|EACCES|EPERM|EROFS/.test(diag!), `expected an errno code: ${diag}`);
  });
});

// ── resolveAtImports: fragment / escaped space / inline code ────────────────
describe('U016-w67 — resolveAtImports edge-case path parsing', () => {
  let cwd: string;
  const mm = () => new MemoryManager(cwd);
  const base = () => path.join(cwd, 'CLAUDE.md');
  beforeEach(() => { cwd = fs.realpathSync(tmp()); });
  afterEach(() => rm(cwd));

  it('strips a #fragment from an @-import and inlines the base file', () => {
    fs.writeFileSync(path.join(cwd, 'notes.md'), 'NOTES_BODY');
    const out = mm().resolveAtImports('see @notes.md#section here', base());
    assert.match(out, /NOTES_BODY/);
    assert.doesNotMatch(out, /#section/, 'fragment must not survive');
  });

  it('resolves an @-import whose path has an escaped space', () => {
    fs.writeFileSync(path.join(cwd, 'my file.md'), 'SPACED_BODY');
    const out = mm().resolveAtImports('load @my\\ file.md done', base());
    assert.match(out, /SPACED_BODY/);
  });

  it('does NOT inline an @-import inside an inline code span', () => {
    fs.writeFileSync(path.join(cwd, 'secret.md'), 'SECRET_BODY');
    const out = mm().resolveAtImports('use `@secret.md` literally', base());
    assert.doesNotMatch(out, /SECRET_BODY/, 'a code-span @path is literal');
    assert.match(out, /`@secret\.md`/, 'the code span survives verbatim');
  });
});

// ── resolveIncludes: symlink-aware cycle detection ─────────────────────────
describe('U016-w67 — resolveIncludes symlink-aware cycle detection', () => {
  let cwd: string;
  beforeEach(() => { cwd = fs.realpathSync(tmp()); });
  afterEach(() => rm(cwd));

  it('detects a cycle reached through a symlink alias', async () => {
    // real.md includes link.md; link.md is a symlink back to real.md → a cycle
    // only visible once paths are realpath-resolved. The lexical guard would
    // recurse real→link→real→link to the depth cap without a circular marker.
    const realFile = path.join(cwd, 'real.md');
    const linkFile = path.join(cwd, 'link.md');
    fs.writeFileSync(realFile, 'REAL_TOP\n@include link.md');
    try {
      fs.symlinkSync(realFile, linkFile);
    } catch {
      return; // platform without symlink support — skip
    }
    const res = await new MemoryManager(cwd).resolveIncludes('@include real.md', path.join(cwd, 'root.md'));
    assert.match(res, /\[circular reference: link\.md\]/);
    // Discriminator: realpath-keyed `seen` catches the alias on the FIRST hop, so
    // the body is inlined exactly once. The pre-fix lexical guard inlined link.md
    // one extra level first, duplicating REAL_TOP, before the self-reference tripped.
    const occurrences = res.split('REAL_TOP').length - 1;
    assert.equal(occurrences, 1, `body must inline once, got ${occurrences}`);
  });
});
