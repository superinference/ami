/**
 * Claude-parity hardening for MemoryManager — new confinement + cap behaviors:
 *   - truncateEntrypointContent / MEMORY.md entrypoint size caps (H4)
 *   - resolveAtImports realpath confinement — a symlink INSIDE the workspace
 *     pointing OUT is blocked (lexical prefix check missed it) (H15/H16)
 *   - resolveIncludes workspace confinement for @include (H14)
 *   - team-memory symlink-escape containment (H6)
 *   - parent-walk stops at `.git` as a FILE (worktree/submodule), not only a dir (H19)
 *
 * Run: cd core/core && node --import tsx --test test/memory-parity-confinement.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  MemoryManager,
  truncateEntrypointContent,
  MAX_ENTRYPOINT_LINES,
  MAX_ENTRYPOINT_BYTES,
} from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

let PARENT: string;
let CWD: string;
beforeEach(() => {
  PARENT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'mem-par-')));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(PARENT, 'ws-')));
});
afterEach(() => { try { fs.rmSync(PARENT, { recursive: true, force: true }); } catch { /* ignore */ } });

// ---------------------------------------------------------------------------
describe('truncateEntrypointContent — MEMORY.md caps (H4)', () => {
  it('returns short content unchanged', () => {
    const c = '- [a](a.md)\n- [b](b.md)';
    assert.equal(truncateEntrypointContent(c), c);
  });
  it('caps at MAX_ENTRYPOINT_LINES and names the line cap', () => {
    const c = Array.from({ length: MAX_ENTRYPOINT_LINES + 50 }, (_, i) => `- [m${i}](m${i}.md)`).join('\n');
    const out = truncateEntrypointContent(c);
    const body = out.split('\n').filter(l => l.startsWith('- ['));
    assert.equal(body.length, MAX_ENTRYPOINT_LINES);
    assert.match(out, new RegExp(`${MAX_ENTRYPOINT_LINES}-line`));
  });
  it('caps at MAX_ENTRYPOINT_BYTES and names the byte cap', () => {
    // One very long line — under the line cap but over the byte cap.
    const c = '- [x](x.md) ' + 'y'.repeat(MAX_ENTRYPOINT_BYTES + 100);
    const out = truncateEntrypointContent(c);
    assert.ok(Buffer.byteLength(out.replace(/\n\n\[MEMORY.*$/s, ''), 'utf-8') <= MAX_ENTRYPOINT_BYTES);
    assert.match(out, new RegExp(`${MAX_ENTRYPOINT_BYTES}-byte`));
  });
});

describe('readMemoryIndex — applies the entrypoint cap (H4)', () => {
  it('never returns more than MAX_ENTRYPOINT_LINES index entries', async () => {
    const memDir = projectStateDir(CWD, 'memory');
    fs.mkdirSync(memDir, { recursive: true });
    const lines = Array.from({ length: MAX_ENTRYPOINT_LINES + 80 }, (_, i) => `- [t${i}](t${i}.md) — n`);
    fs.writeFileSync(path.join(memDir, 'MEMORY.md'), lines.join('\n'));
    const entries = await new MemoryManager(CWD).readMemoryIndex(CWD);
    assert.ok(entries.length <= MAX_ENTRYPOINT_LINES, `got ${entries.length}`);
  });
});

// ---------------------------------------------------------------------------
describe('resolveAtImports — realpath confinement (H15/H16)', () => {
  const base = () => path.join(CWD, 'CLAUDE.md');
  it('blocks a symlink INSIDE the workspace that points OUT', () => {
    fs.writeFileSync(path.join(PARENT, 'secret.md'), 'TOPSECRET_IMPORT');
    const link = path.join(CWD, 'link.md');
    try { fs.symlinkSync(path.join(PARENT, 'secret.md'), link); } catch { return; /* no symlink support */ }
    const out = new MemoryManager(CWD).resolveAtImports('load @./link.md', base());
    assert.doesNotMatch(out, /TOPSECRET_IMPORT/, 'symlinked-out content must NOT be inlined');
    assert.match(out, /import blocked — outside workspace/i);
  });
  it('still inlines a symlink that stays INSIDE the workspace', () => {
    fs.mkdirSync(path.join(CWD, 'sub'), { recursive: true });
    fs.writeFileSync(path.join(CWD, 'sub', 'real.md'), 'INSIDE_LINK_OK');
    const link = path.join(CWD, 'alias.md');
    try { fs.symlinkSync(path.join(CWD, 'sub', 'real.md'), link); } catch { return; }
    const out = new MemoryManager(CWD).resolveAtImports('@./alias.md', base());
    assert.match(out, /INSIDE_LINK_OK/);
  });
});

// ---------------------------------------------------------------------------
describe('resolveIncludes — workspace confinement (H14)', () => {
  it('blocks @include that escapes the workspace via ..', async () => {
    fs.writeFileSync(path.join(PARENT, 'outside.md'), 'OUTSIDE_INCLUDE');
    const mgr = new MemoryManager(CWD);
    const out = await mgr.resolveIncludes('@include ../outside.md', path.join(CWD, 'root.md'));
    assert.doesNotMatch(out, /OUTSIDE_INCLUDE/);
    assert.match(out, /include blocked — outside workspace/i);
  });
  it('blocks @include of an absolute path outside the workspace', async () => {
    const mgr = new MemoryManager(CWD);
    const out = await mgr.resolveIncludes('@include /etc/hostname', path.join(CWD, 'root.md'));
    assert.match(out, /include blocked — outside workspace/i);
  });
  it('blocks a symlink inside the workspace pointing out', async () => {
    fs.writeFileSync(path.join(PARENT, 'o.md'), 'SYM_OUT_INCLUDE');
    const link = path.join(CWD, 'o-link.md');
    try { fs.symlinkSync(path.join(PARENT, 'o.md'), link); } catch { return; }
    const mgr = new MemoryManager(CWD);
    const out = await mgr.resolveIncludes('@include o-link.md', path.join(CWD, 'root.md'));
    assert.doesNotMatch(out, /SYM_OUT_INCLUDE/);
    assert.match(out, /include blocked — outside workspace/i);
  });
  it('still inlines an in-workspace @include', async () => {
    fs.writeFileSync(path.join(CWD, 'in.md'), 'IN_INCLUDE_OK');
    const mgr = new MemoryManager(CWD);
    const out = await mgr.resolveIncludes('@include in.md', path.join(CWD, 'root.md'));
    assert.match(out, /IN_INCLUDE_OK/);
  });
});

// ---------------------------------------------------------------------------
describe('team-memory symlink-escape containment (H6)', () => {
  it('skips a team .md symlink that points outside the team dir', () => {
    const memDir = projectStateDir(CWD, 'memory');
    const teamDir = path.join(memDir, 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'ok.md'), 'TEAM_OK_CONTENT');
    fs.writeFileSync(path.join(PARENT, 'leak.md'), 'TEAM_LEAK_CONTENT');
    try { fs.symlinkSync(path.join(PARENT, 'leak.md'), path.join(teamDir, 'leak.md')); } catch { return; }
    const loaded = new MemoryManager(CWD).loadTeamMemories();
    const joined = loaded.join('\n');
    assert.match(joined, /TEAM_OK_CONTENT/, 'real in-dir team memory still loads');
    assert.doesNotMatch(joined, /TEAM_LEAK_CONTENT/, 'symlinked-out team memory is skipped');
  });
});

// ---------------------------------------------------------------------------
describe('parent-walk stops at `.git` FILE (worktree/submodule) (H19)', () => {
  it('does not load a parent project\'s CLAUDE.md past a `.git` file', () => {
    // grandparent is an outer project; child is a linked worktree (.git is a FILE).
    fs.writeFileSync(path.join(PARENT, 'CLAUDE.md'), 'GRANDPARENT_RULES');
    fs.writeFileSync(path.join(CWD, 'CLAUDE.md'), 'CHILD_RULES');
    fs.writeFileSync(path.join(CWD, '.git'), 'gitdir: /somewhere/.git/worktrees/ws\n');
    const out = new MemoryManager(CWD).loadProjectInstructions();
    assert.match(out, /CHILD_RULES/);
    assert.doesNotMatch(out, /GRANDPARENT_RULES/, 'must stop at the `.git` file boundary');
  });
});
