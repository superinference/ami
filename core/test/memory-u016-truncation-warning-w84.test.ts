/**
 * U016 / wave w84 — visible memory-index truncation warnings + errno-aware
 * rules-directory reads. Closes the following confirmed parity gaps:
 *
 *   - "Visible truncation/cap warning when the memory index is clipped"
 *     (Claude Code memdir/memdir.ts `truncateEntrypointContent`): AMI silently
 *     capped loaded memories at MAX_MEMORY_FILES (slice, comment only). Now
 *     `getMemoryContext` surfaces a Memory Health Warning naming how many topic
 *     files exist vs. how many were loaded, so the model can self-correct an
 *     overgrown index. Backed by `loadMemoriesWithMeta().totalFound` (pre-cap).
 *
 *   - "rules-directory readdir distinguishes expected vs unexpected errors"
 *     (Claude Code the reference's markdown-memory module `processMdRules`): `loadRulesDirectory` /
 *     `loadRulesForFile` wrapped the whole readdir in `catch {}` → a transient
 *     EACCES/EIO was indistinguishable from a missing dir. Now an unexpected
 *     errno is surfaced via the memory logger (ENOENT/ENOTDIR stay silent).
 *
 *   - "MEMORY.md entrypoint truncation to line AND byte caps with a warning"
 *     (lock test for the already-implemented `truncateEntrypointContent`): the
 *     warning must NAME which cap (line vs byte) fired.
 *
 * Red-green: with the getMemoryContext warning reverted, the cap-note assertions
 * fail (no "topic files exist" note); with the errno change reverted, the EACCES
 * log assertion fails (silent swallow); with truncateEntrypointContent's warning
 * reverted the cap-name assertions fail.
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-truncation-warning-w84.test.ts
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  MemoryManager,
  truncateEntrypointContent,
  MAX_MEMORY_FILES,
  MAX_ENTRYPOINT_LINES,
  MAX_ENTRYPOINT_BYTES,
} from '../src/memory';
import * as logger from '../src/logger';

let ROOT: string;
let CWD: string;
let memDir: string;
const savedEnv: Record<string, string | undefined> = {};

function stashEnv(...keys: string[]): void {
  for (const k of keys) savedEnv[k] = process.env[k];
}
function restoreEnv(): void {
  for (const [k, v] of Object.entries(savedEnv)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

beforeEach(() => {
  ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-w84tw-')));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'ws-')));
  memDir = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'mem-')));
  stashEnv('AMI_MEMORY_DIR', 'SUPERINFERENCE_DATA_DIR', 'AMI_DISABLE_AUTO_MEMORY');
  process.env.AMI_MEMORY_DIR = memDir;
  process.env.SUPERINFERENCE_DATA_DIR = fs.mkdtempSync(path.join(ROOT, 'data-'));
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  restoreEnv();
  try { fs.rmSync(ROOT, { recursive: true, force: true }); } catch { /* ignore */ }
});

function writeMemories(n: number, bodyBytes = 16): void {
  const body = 'x'.repeat(bodyBytes);
  for (let i = 0; i < n; i++) {
    const name = `m${String(i).padStart(4, '0')}.md`;
    fs.writeFileSync(path.join(memDir, name), `---\nname: Memory ${i}\n---\n${body}\n`, 'utf-8');
  }
}

// ---------------------------------------------------------------------------
describe('gap: visible warning when the stored-memory index is clipped at MAX_MEMORY_FILES', () => {
  it('loadMemoriesWithMeta reports the pre-cap total and caps the entries', () => {
    writeMemories(MAX_MEMORY_FILES + 5);
    const { entries, totalFound } = new MemoryManager(CWD).loadMemoriesWithMeta();
    assert.equal(totalFound, MAX_MEMORY_FILES + 5, 'totalFound is the pre-cap count');
    assert.equal(entries.length, MAX_MEMORY_FILES, 'entries are capped at MAX_MEMORY_FILES');
  });

  it('loadMemories stays the capped entry list (delegates to the meta reader)', () => {
    writeMemories(MAX_MEMORY_FILES + 5);
    const mem = new MemoryManager(CWD).loadMemories();
    assert.equal(mem.length, MAX_MEMORY_FILES);
  });

  it('getMemoryContext surfaces a Memory Health Warning naming the clipped count', () => {
    writeMemories(MAX_MEMORY_FILES + 5);
    const ctx = new MemoryManager(CWD).getMemoryContext();
    assert.match(ctx, /## Memory Health Warning/);
    assert.match(ctx, new RegExp(`Only the ${MAX_MEMORY_FILES} most recent memories are loaded`));
    assert.match(ctx, new RegExp(`\\(${MAX_MEMORY_FILES + 5} topic files exist\\)`));
    assert.match(ctx, /the oldest 5 are NOT in context/);
  });

  it('does NOT emit the clipped warning when under the cap', () => {
    writeMemories(3);
    const mgr = new MemoryManager(CWD);
    const { totalFound, entries } = mgr.loadMemoriesWithMeta();
    assert.equal(totalFound, 3);
    assert.equal(entries.length, 3);
    const ctx = mgr.getMemoryContext();
    assert.doesNotMatch(ctx, /topic files exist/);
  });
});

// ---------------------------------------------------------------------------
describe('gap: rules-directory readdir distinguishes expected vs unexpected errors', () => {
  // Capture emitted log lines via the logger's mockable fsHooks sink (the logger
  // namespace is a frozen ESM import and must not be monkeypatched directly).
  let logLines: string[];
  const savedLogLevel = process.env.AMI_DEBUG_LOG_LEVEL;
  const savedDisableLog = process.env.AMI_DISABLE_DEBUG_LOG;
  const savedToStderr = process.env.AMI_DEBUG_TO_STDERR;

  beforeEach(() => {
    logLines = [];
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug'; // ensure warn passes the verbosity gate
    delete process.env.AMI_DISABLE_DEBUG_LOG;
    delete process.env.AMI_DEBUG_TO_STDERR;
    mock.method(logger.fsHooks, 'appendFileSync', (_f: unknown, line: unknown) => {
      logLines.push(String(line));
    });
  });

  afterEach(() => {
    mock.restoreAll();
    if (savedLogLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL;
    else process.env.AMI_DEBUG_LOG_LEVEL = savedLogLevel;
    if (savedDisableLog === undefined) delete process.env.AMI_DISABLE_DEBUG_LOG;
    else process.env.AMI_DISABLE_DEBUG_LOG = savedDisableLog;
    if (savedToStderr === undefined) delete process.env.AMI_DEBUG_TO_STDERR;
    else process.env.AMI_DEBUG_TO_STDERR = savedToStderr;
  });

  function errno(code: string): NodeJS.ErrnoException {
    const e = new Error(code) as NodeJS.ErrnoException;
    e.code = code;
    return e;
  }

  it('loadRulesDirectory surfaces an unexpected EACCES readdir failure (not a silent skip)', async () => {
    const mgr = new MemoryManager(CWD);
    (mgr as unknown as { _readdir: () => Promise<never> })._readdir = async () => { throw errno('EACCES'); };
    const rules = await mgr.loadRulesDirectory(CWD);
    assert.deepEqual(rules, [], 'still falls back to empty rules (never throws)');
    const warn = logLines.find(l => /\[memory\]/.test(l) && /read failed/.test(l));
    assert.ok(warn, 'expected an unexpected-errno diagnostic to be logged');
    assert.match(warn!, /EACCES/);
    assert.match(warn!, /\[WARN\]/);
  });

  it('loadRulesDirectory stays silent for an expected ENOENT (missing rules dir)', async () => {
    const mgr = new MemoryManager(CWD);
    (mgr as unknown as { _readdir: () => Promise<never> })._readdir = async () => { throw errno('ENOENT'); };
    const rules = await mgr.loadRulesDirectory(CWD);
    assert.deepEqual(rules, []);
    assert.equal(logLines.find(l => /read failed/.test(l)), undefined, 'ENOENT must not log');
  });

  it('loadRulesForFile surfaces an unexpected EIO readdir failure', async () => {
    const mgr = new MemoryManager(CWD);
    (mgr as unknown as { _readdir: () => Promise<never> })._readdir = async () => { throw errno('EIO'); };
    const sources = await mgr.loadRulesForFile(CWD, path.join(CWD, 'src/x.ts'));
    assert.deepEqual(sources, []);
    const warn = logLines.find(l => /\[memory\]/.test(l) && /read failed/.test(l));
    assert.ok(warn, 'expected an unexpected-errno diagnostic to be logged');
    assert.match(warn!, /EIO/);
  });

  it('loadRulesForFile stays silent for an expected ENOTDIR', async () => {
    const mgr = new MemoryManager(CWD);
    (mgr as unknown as { _readdir: () => Promise<never> })._readdir = async () => { throw errno('ENOTDIR'); };
    const sources = await mgr.loadRulesForFile(CWD, path.join(CWD, 'src/x.ts'));
    assert.deepEqual(sources, []);
    assert.equal(logLines.find(l => /read failed/.test(l)), undefined, 'ENOTDIR must not log');
  });
});

// ---------------------------------------------------------------------------
describe('gap (lock): truncateEntrypointContent names which cap fired', () => {
  it('appends a line-cap warning naming the line limit', () => {
    const raw = Array.from({ length: MAX_ENTRYPOINT_LINES + 50 }, (_, i) => `- [t${i}](t${i}.md)`).join('\n');
    const out = truncateEntrypointContent(raw);
    assert.match(out, new RegExp(`truncated to the ${MAX_ENTRYPOINT_LINES}-line`));
    assert.doesNotMatch(out, /t249\]/, 'tail entries past the line cap are dropped');
  });

  it('appends a byte-cap warning naming the byte limit for a few very long lines', () => {
    // Under the LINE cap but over the BYTE cap (long lines are the byte-cap's target).
    const raw = Array.from({ length: 5 }, () => 'x'.repeat(MAX_ENTRYPOINT_BYTES)).join('\n');
    const out = truncateEntrypointContent(raw);
    assert.match(out, new RegExp(`truncated to the ${MAX_ENTRYPOINT_BYTES}-byte`));
  });

  it('returns content unchanged when within both caps', () => {
    const raw = '- [a](a.md)\n- [b](b.md)';
    assert.equal(truncateEntrypointContent(raw), raw);
  });
});
