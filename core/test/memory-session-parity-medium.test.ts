/**
 * Claude-parity (medium): YAML frontmatter injection defense + MEMORY.md body
 * exclusion + bounded memory read; NDJSON-safe (U+2028/U+2029) serialization of
 * persisted session + transcript records.
 *
 * Run: cd core/core && node --import tsx --test test/memory-session-parity-medium.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { MemoryManager, parseFrontmatter } from '../src/memory';
import { projectStateDir, ndjsonSafeStringify } from '../src/reference-journal';
import { SessionManager, safeAppendTranscript } from '../src/session';
import type { Session } from '../src/session';

let tmpDir: string;
beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mem-sess-med-')); });
afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ } });

describe('YAML frontmatter injection defense', () => {
  it('a newline in a memory name cannot forge extra YAML keys', () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('legit\ntype: user\ninjected: evil', 'a desc', 'Some desc', 'project');
    // Read the raw file and parse its frontmatter — the type must remain 'project'.
    const memDir = projectStateDir(tmpDir, 'memory');
    const file = fs.readdirSync(memDir).find(f => f.endsWith('.md'))!;
    const raw = fs.readFileSync(path.join(memDir, file), 'utf-8');
    const { frontmatter } = parseFrontmatter(raw);
    assert.equal(frontmatter.type, 'project', 'injected type must not win');
    assert.equal(frontmatter.injected, undefined, 'no forged key may appear');
  });
  it('a description with a newline stays a single field', () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('x', 'body', 'line1\ntype: hacked', 'project');
    const memDir = projectStateDir(tmpDir, 'memory');
    const file = fs.readdirSync(memDir).find(f => f.endsWith('.md'))!;
    const { frontmatter } = parseFrontmatter(fs.readFileSync(path.join(memDir, file), 'utf-8'));
    assert.equal(frontmatter.type, 'project');
  });
});

describe('MEMORY.md excluded from the memory-body scan', () => {
  it('does not surface the index file as a memory', () => {
    const memDir = projectStateDir(tmpDir, 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    fs.writeFileSync(path.join(memDir, 'MEMORY.md'), '- [t](t.md) — index entry');
    fs.writeFileSync(path.join(memDir, 'real.md'), '---\nname: real\n---\nREAL_BODY');
    const names = new MemoryManager(tmpDir).loadMemories().map(m => m.name);
    assert.ok(names.includes('real'));
    assert.ok(!names.includes('MEMORY'), 'MEMORY.md must not be a memory body');
  });
});

describe('bounded memory read', () => {
  it('skips a memory file over the per-file byte cap', () => {
    const memDir = projectStateDir(tmpDir, 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const p = path.join(memDir, 'huge.md');
    fs.writeFileSync(p, '---\nname: huge\n---\nx');
    fs.truncateSync(p, 256 * 1024 + 1);
    const names = new MemoryManager(tmpDir).loadMemories().map(m => m.name);
    assert.ok(!names.includes('huge'), 'oversized memory file must be skipped');
  });
});

describe('NDJSON-safe serialization (U+2028/U+2029)', () => {
  it('ndjsonSafeStringify escapes the line separators', () => {
    const out = ndjsonSafeStringify({ a: 'x y z' });
    assert.ok(!out.includes(' ') && !out.includes(' '), 'raw separators must be escaped');
    assert.deepEqual(JSON.parse(out), { a: 'x y z' }, 'round-trips to the same value');
  });
  it('a saved session with U+2028 in content stays a single JSON document', () => {
    const mgr = new SessionManager(tmpDir, tmpDir);
    const session: Session = {
      id: 'sep' as Session['id'],
      messages: [{ role: 'user', content: 'before after' }],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      provider: { model: 'm', baseUrl: 'b' },
    };
    mgr.save(session);
    const raw = fs.readFileSync(path.join(tmpDir, 'sep.json'), 'utf-8');
    assert.ok(!raw.includes(' '), 'persisted JSON contains no raw U+2028');
    const loaded = mgr.load('sep');
    assert.equal((loaded!.messages[0] as { content: string }).content, 'before after');
  });
  it('a transcript append with U+2029 is one line', () => {
    const p = path.join(tmpDir, 't.jsonl');
    safeAppendTranscript(p, { type: 'x', timestamp: 1, text: 'a b' });
    const raw = fs.readFileSync(p, 'utf-8');
    assert.equal(raw.split('\n').filter(Boolean).length, 1, 'single record → single line');
    assert.ok(!raw.includes(' '));
  });
});
