import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  MemoryManager,
  formatRelativeTime,
  formatRelativeTimeAgo,
  isTeamMemoryEnabled,
  MAX_MEMORY_CHARACTER_COUNT,
} from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

/**
 * U016 / wave-72 — relative-time formatting, memory-file classification,
 * team-memory op classification + enable knob, date-partitioned daily logs, and
 * EEXIST-safe create-if-absent. Parity with Claude Code's utils/format.ts +
 * utils/intl.ts (formatRelativeTime/Ago), utils/memoryFileDetection.ts
 * (isAutoManagedMemoryFile), utils/teamMemoryOps.ts (team-op classifier),
 * memdir/paths.ts (getAutoMemDailyLogPath) and commands/memory/memory.tsx
 * (wx create-if-absent).
 */

let tmpDir: string;
let memoryDir: string;
let prevMemDir: string | undefined;
let prevTeam: string | undefined;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memw72-'));
  memoryDir = projectStateDir(tmpDir, 'memory');
  fs.mkdirSync(memoryDir, { recursive: true });
  // Neutralize env overrides that would redirect the memory dir / enable team mem.
  prevMemDir = process.env.AMI_MEMORY_DIR;
  prevTeam = process.env.AMI_ENABLE_TEAM_MEMORY;
  delete process.env.AMI_MEMORY_DIR;
  delete process.env.AMI_ENABLE_TEAM_MEMORY;
});

afterEach(() => {
  if (prevMemDir === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = prevMemDir;
  if (prevTeam === undefined) delete process.env.AMI_ENABLE_TEAM_MEMORY;
  else process.env.AMI_ENABLE_TEAM_MEMORY = prevTeam;
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('formatRelativeTime / formatRelativeTimeAgo (U016/w72)', () => {
  const now = new Date('2026-01-15T12:00:00Z');

  it('narrow style: picks the largest matching interval, past uses "N<unit> ago"', () => {
    assert.equal(formatRelativeTime(new Date('2026-01-12T12:00:00Z'), { now }), '3d ago');
    assert.equal(formatRelativeTime(new Date('2026-01-15T10:00:00Z'), { now }), '2h ago');
    // 90 minutes ago → selects the HOUR unit (truncated), not 90 minutes.
    assert.equal(formatRelativeTime(new Date('2026-01-15T10:30:00Z'), { now }), '1h ago');
    assert.equal(formatRelativeTime(new Date('2025-11-15T12:00:00Z'), { now }), '2mo ago');
    assert.equal(formatRelativeTime(new Date('2024-01-15T12:00:00Z'), { now }), '2y ago');
  });

  it('narrow style: future uses "in N<unit>"', () => {
    assert.equal(formatRelativeTime(new Date('2026-01-18T12:00:00Z'), { now }), 'in 3d');
    assert.equal(formatRelativeTime(new Date('2026-01-15T13:00:00Z'), { now }), 'in 1h');
  });

  it('narrow style: sub-second gap degrades to a zero marker', () => {
    assert.equal(formatRelativeTime(new Date('2026-01-15T12:00:00Z'), { now }), '0s ago');
    assert.equal(formatRelativeTime(new Date('2026-01-15T11:59:59.500Z'), { now }), '0s ago');
    // A sub-second FUTURE gap truncates to 0 seconds, so it also renders "0s ago"
    // (matches the reference's `diffInSeconds <= 0` guard).
    assert.equal(formatRelativeTime(new Date('2026-01-15T12:00:00.500Z'), { now }), '0s ago');
  });

  it('long/short style defers to the localized Intl.RelativeTimeFormat', () => {
    // A matched interval (>= 1 day) always renders long + localized.
    assert.equal(formatRelativeTime(new Date('2026-01-12T12:00:00Z'), { now, style: 'long' }), '3 days ago');
    // Sub-second non-narrow uses the localized zero-second string, honoring numeric.
    assert.equal(
      formatRelativeTime(new Date('2026-01-15T12:00:00Z'), { now, style: 'long', numeric: 'auto' }),
      'now',
    );
  });

  it('formatRelativeTimeAgo forces numeric:always for past dates but not future', () => {
    // Past with numeric:auto still yields a definite "ago" (forced to always).
    assert.equal(
      formatRelativeTimeAgo(new Date('2026-01-14T12:00:00Z'), { now, style: 'long', numeric: 'auto' }),
      '1 day ago',
    );
    // Future falls through to the caller's options (narrow default here).
    assert.equal(formatRelativeTimeAgo(new Date('2026-01-18T12:00:00Z'), { now }), 'in 3d');
  });
});

describe('isTeamMemoryEnabled (U016/w72)', () => {
  it('is off by default and on with a truthy env value', () => {
    assert.equal(isTeamMemoryEnabled({}), false);
    assert.equal(isTeamMemoryEnabled({ AMI_ENABLE_TEAM_MEMORY: '1' }), true);
    assert.equal(isTeamMemoryEnabled({ AMI_ENABLE_TEAM_MEMORY: 'true' }), true);
    assert.equal(isTeamMemoryEnabled({ AMI_ENABLE_TEAM_MEMORY: '0' }), false);
    assert.equal(isTeamMemoryEnabled({ AMI_ENABLE_TEAM_MEMORY: '' }), false);
  });
});

describe('MemoryManager file classification (U016/w72)', () => {
  it('isUserManagedInstructionFile recognizes instruction + rule files, not memory files', () => {
    const mm = new MemoryManager(tmpDir);
    assert.equal(mm.isUserManagedInstructionFile(path.join(tmpDir, 'CLAUDE.md')), true);
    assert.equal(mm.isUserManagedInstructionFile(path.join(tmpDir, 'AGENTS.md')), true);
    assert.equal(mm.isUserManagedInstructionFile(path.join(tmpDir, 'CLAUDE.local.md')), true);
    assert.equal(mm.isUserManagedInstructionFile(path.join(tmpDir, '.ami', 'rules', 'style.md')), true);
    assert.equal(mm.isUserManagedInstructionFile(path.join(tmpDir, '.claude', 'rules', 'a', 'b.txt')), true);
    assert.equal(mm.isUserManagedInstructionFile(path.join(memoryDir, 'note.md')), false);
    assert.equal(mm.isUserManagedInstructionFile(path.join(tmpDir, 'README.md')), false);
  });

  it('isAutoManagedMemoryFile is true for memory-tree files and false for instruction files', () => {
    const mm = new MemoryManager(tmpDir);
    assert.equal(mm.isAutoManagedMemoryFile(path.join(memoryDir, 'topic.md')), true);
    assert.equal(mm.isAutoManagedMemoryFile(path.join(mm.getTeamMemoryDir(), 'shared.md')), true);
    assert.equal(mm.isAutoManagedMemoryFile(path.join(tmpDir, 'CLAUDE.md')), false);
    // A file outside the memory tree is not managed memory.
    assert.equal(mm.isAutoManagedMemoryFile(path.join(tmpDir, 'src', 'index.ts')), false);
    // A non-.md file inside the memory tree is not a managed MEMORY FILE.
    assert.equal(mm.isAutoManagedMemoryFile(path.join(memoryDir, 'data.json')), false);
  });
});

describe('MemoryManager.classifyTeamMemoryToolUse (U016/w72)', () => {
  it('is inert when team memory is disabled (default)', () => {
    const mm = new MemoryManager(tmpDir);
    const teamFile = path.join(mm.getTeamMemoryDir(), 'shared.md');
    assert.equal(mm.classifyTeamMemoryToolUse('file_edit', { file_path: teamFile }, {}), null);
  });

  it('classifies write/edit and search/read ops against team files when enabled', () => {
    const mm = new MemoryManager(tmpDir);
    const env = { AMI_ENABLE_TEAM_MEMORY: '1' };
    const teamFile = path.join(mm.getTeamMemoryDir(), 'shared.md');
    const teamDir = mm.getTeamMemoryDir();

    assert.equal(mm.classifyTeamMemoryToolUse('file_edit', { file_path: teamFile }, env), 'write');
    assert.equal(mm.classifyTeamMemoryToolUse('file_read', { file_path: teamFile }, env), 'search');
    assert.equal(mm.classifyTeamMemoryToolUse('grep', { path: teamDir }, env), 'search');
    assert.equal(mm.classifyTeamMemoryToolUse('glob', { path: teamDir }, env), 'search');
  });

  it('returns null for non-team paths, unknown tools, and missing input', () => {
    const mm = new MemoryManager(tmpDir);
    const env = { AMI_ENABLE_TEAM_MEMORY: '1' };
    assert.equal(mm.classifyTeamMemoryToolUse('file_edit', { file_path: path.join(tmpDir, 'x.ts') }, env), null);
    assert.equal(mm.classifyTeamMemoryToolUse('bash', { path: mm.getTeamMemoryDir() }, env), null);
    assert.equal(mm.classifyTeamMemoryToolUse('file_read', undefined, env), null);
    assert.equal(mm.classifyTeamMemoryToolUse('file_edit', {}, env), null);
  });
});

describe('MemoryManager daily memory log (U016/w72)', () => {
  it('getDailyMemoryLogPath builds a zero-padded logs/YYYY/MM/YYYY-MM-DD.md path', () => {
    const mm = new MemoryManager(tmpDir);
    const p = mm.getDailyMemoryLogPath(new Date('2026-03-07T09:00:00'));
    assert.equal(p, path.join(mm.getMemoryDir(), 'logs', '2026', '03', '2026-03-07.md'));
  });

  it('appendDailyMemoryLog creates the tree, appends line-separated entries, and redacts credentials', () => {
    const mm = new MemoryManager(tmpDir);
    const day = new Date('2026-03-07T09:00:00');
    const first = mm.appendDailyMemoryLog('ran the build', day);
    mm.appendDailyMemoryLog('fixed a flaky test', day);
    mm.appendDailyMemoryLog('token is sk-ant-api03-SECRETKEY1234567890abcdefghij', day);

    assert.equal(first, mm.getDailyMemoryLogPath(day));
    const body = fs.readFileSync(first, 'utf-8');
    const lines = body.split('\n').filter(Boolean);
    assert.equal(lines[0], 'ran the build');
    assert.equal(lines[1], 'fixed a flaky test');
    assert.ok(!body.includes('SECRETKEY1234567890'), 'a credential in a daily log must be redacted');
  });
});

describe('MemoryManager create-if-absent (EEXIST-safe) (U016/w72)', () => {
  it('createMemoryFileIfAbsent creates when absent and preserves existing content', () => {
    const mm = new MemoryManager(tmpDir);
    const target = path.join(memoryDir, 'new-area', 'NOTES.md');
    assert.equal(mm.createMemoryFileIfAbsent(target), true);
    assert.equal(fs.readFileSync(target, 'utf-8'), '');

    // Populate, then a second create-if-absent must NOT clobber it.
    fs.writeFileSync(target, 'existing content', 'utf-8');
    assert.equal(mm.createMemoryFileIfAbsent(target), false);
    assert.equal(fs.readFileSync(target, 'utf-8'), 'existing content');
  });

  it('addToMemoryIndex creates MEMORY.md and appends without clobbering (E2E)', async () => {
    const mm = new MemoryManager(tmpDir);
    const indexPath = path.join(projectStateDir(tmpDir, 'memory'), 'MEMORY.md');
    await mm.addToMemoryIndex(tmpDir, '- [Auth](auth.md) — JWT approach');
    assert.ok(fs.existsSync(indexPath));
    await mm.addToMemoryIndex(tmpDir, '- [Deploy](deploy.md) — GitHub Actions');
    const entries = await mm.readMemoryIndex(tmpDir);
    assert.ok(entries.some(e => e.includes('Auth')));
    assert.ok(entries.some(e => e.includes('Deploy')));
  });
});

describe('MemoryManager.buildMemoryDiagnostics enrichment (U016/w72)', () => {
  it('tags each diagnostic with managed classification and a relative mtime', () => {
    const mm = new MemoryManager(tmpDir);
    const big = 'x'.repeat(MAX_MEMORY_CHARACTER_COUNT + 100);
    // Oversized MANAGED memory file + oversized USER-managed instruction file.
    fs.writeFileSync(path.join(memoryDir, 'bloat.md'), big, 'utf-8');
    fs.writeFileSync(path.join(tmpDir, 'CLAUDE.md'), big, 'utf-8');

    const diags = mm.buildMemoryDiagnostics();
    const memDiag = diags.find(d => d.file.endsWith('bloat.md'));
    const claudeDiag = diags.find(d => d.file.endsWith('CLAUDE.md'));
    assert.ok(memDiag, 'expected a diagnostic for the oversized memory file');
    assert.ok(claudeDiag, 'expected a diagnostic for the oversized CLAUDE.md');

    assert.equal(memDiag!.managed, true, 'memory-tree file is AMI-managed');
    assert.equal(claudeDiag!.managed, false, 'CLAUDE.md is user-managed');
    assert.match(memDiag!.modifiedAgo ?? '', /ago$|in /, 'modifiedAgo should be a relative-time label');
    assert.match(claudeDiag!.modifiedAgo ?? '', /ago$|in /);
  });
});
