/**
 * U016 (wave 82) — memory.ts parity gap: SESSION-MEMORY INTROSPECTION.
 *
 * Claude Code surfaces the session-memory notes file path (`getSessionMemoryPath`,
 * services/SessionMemory/sessionMemoryUtils.ts) and emits observable introspection
 * events on both the WRITE path (`tengu_session_memory_file_read` content-length
 * tracking, sessionMemory.ts:L228-230) and the LOAD path
 * (`logEvent('tengu_session_memory_loaded', { content_length })`,
 * sessionMemoryUtils.ts:L110-126) so the reinjection of persisted session memory
 * into the conversation is observable.
 *
 * AMI's `MemoryManager` previously saved/loaded session facts with no path accessor
 * and no introspection signal. This adds:
 *   - `getSessionMemoryPath(sessionId)` — the single canonical path accessor shared
 *     by save + load (and usable by a `/memory` surface).
 *   - a no-PII `session_memory_saved` diagnostic (content_length + fact_count).
 *   - a no-PII `session_memory_loaded` diagnostic (content_length + fact_count),
 *     emitted ONLY on a successful read.
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-session-introspection-w82.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { MemoryManager } from '../src/memory';
import type { SessionFact } from '../src/session-memory';

let cwdDir: string;
let memDir: string;
let diagFile: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedDiag = process.env.AMI_DIAGNOSTICS_FILE;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function readEvents(event: string): Array<Record<string, unknown>> {
  if (!fs.existsSync(diagFile)) return [];
  return fs
    .readFileSync(diagFile, 'utf-8')
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l) as Record<string, unknown>)
    .filter((r) => r.event === event);
}

const FACTS: SessionFact[] = [
  { fact: 'use pnpm not npm', category: 'user_preference', confidence: 0.9 },
  { fact: 'fixed the race in engine.ts', category: 'error_fix', confidence: 0.8 },
];

beforeEach(() => {
  cwdDir = mkTmp('si-mem-sess82-cwd-');
  memDir = mkTmp('si-mem-sess82-store-');
  diagFile = path.join(mkTmp('si-mem-sess82-diag-'), 'diag.jsonl');
  process.env.AMI_MEMORY_DIR = memDir;
  process.env.AMI_DIAGNOSTICS_FILE = diagFile;
});

afterEach(() => {
  for (const d of [cwdDir, memDir, path.dirname(diagFile)]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
  if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = savedMemEnv;
  if (savedDiag === undefined) delete process.env.AMI_DIAGNOSTICS_FILE;
  else process.env.AMI_DIAGNOSTICS_FILE = savedDiag;
});

describe('U016-w82 — session-memory introspection', () => {
  it('getSessionMemoryPath points into the memory dir and sanitizes the id', () => {
    const mm = new MemoryManager(cwdDir);
    const p = mm.getSessionMemoryPath('abc-123');
    assert.equal(p, path.join(mm.getMemoryDir(), 'session-abc-123.md'));
    // A hostile id cannot escape the memory directory.
    const hostile = mm.getSessionMemoryPath('../../etc/passwd');
    assert.equal(path.dirname(hostile), mm.getMemoryDir());
    assert.ok(path.basename(hostile).startsWith('session-'));
    assert.ok(!hostile.includes('..'));
  });

  it('save then load round-trips via the same canonical path', () => {
    const mm = new MemoryManager(cwdDir);
    mm.saveSessionMemory('s1', FACTS);
    assert.ok(fs.existsSync(mm.getSessionMemoryPath('s1')), 'file written at the accessor path');
    const loaded = mm.loadSessionMemory('s1');
    assert.equal(loaded.length, 2);
    assert.deepEqual(loaded.map((f) => f.category).sort(), ['error_fix', 'user_preference']);
  });

  it('emits a no-PII session_memory_saved diagnostic with counts (never fact text)', () => {
    const mm = new MemoryManager(cwdDir);
    mm.saveSessionMemory('s2', FACTS);
    const events = readEvents('session_memory_saved');
    assert.equal(events.length, 1, 'exactly one save diagnostic');
    const data = events[0]!.data as Record<string, unknown>;
    assert.equal(data.fact_count, 2);
    assert.ok(typeof data.content_length === 'number' && (data.content_length as number) > 0);
    // No fact text leaks into the no-PII channel.
    const raw = fs.readFileSync(diagFile, 'utf-8');
    assert.ok(!raw.includes('use pnpm not npm'));
    assert.ok(!raw.includes('race in engine.ts'));
  });

  it('emits session_memory_loaded ONLY on a successful read', () => {
    const mm = new MemoryManager(cwdDir);
    // No file yet → load returns [] and emits NO load event (ENOENT fallback).
    assert.deepEqual(mm.loadSessionMemory('missing'), []);
    assert.equal(readEvents('session_memory_loaded').length, 0, 'no event on ENOENT fallback');

    mm.saveSessionMemory('s3', FACTS);
    const loaded = mm.loadSessionMemory('s3');
    assert.equal(loaded.length, 2);
    const events = readEvents('session_memory_loaded');
    assert.equal(events.length, 1, 'one load event after a real read');
    const data = events[0]!.data as Record<string, unknown>;
    assert.equal(data.fact_count, 2);
    assert.ok((data.content_length as number) > 0);
  });

  it('is silent on the diagnostics channel when it is unconfigured', () => {
    delete process.env.AMI_DIAGNOSTICS_FILE;
    const mm = new MemoryManager(cwdDir);
    mm.saveSessionMemory('s4', FACTS);
    mm.loadSessionMemory('s4');
    assert.equal(fs.existsSync(diagFile), false, 'no diagnostics file written when channel off');
  });
});
