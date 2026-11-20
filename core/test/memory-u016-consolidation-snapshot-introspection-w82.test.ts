/**
 * U016 (wave 82) — parity gaps:
 *   1. CONSOLIDATION prompt — PRIORITIZED signal gathering that names the dated
 *      daily-log stream (`logs/YYYY/MM/YYYY-MM-DD.md`) as the primary, cheapest
 *      source before the expensive JSONL transcript grep (parity with the reference's
 *      services/autoDream/consolidationPrompt.ts:L31-40 priority-ordered gather).
 *   2. AGENT-MEMORY SNAPSHOT lifecycle introspection — a no-PII `agent_memory_snapshot`
 *      diagnostic ({ action, scope }) emitted when the drift check decides to
 *      initialize / prompt-update, and when a sync marker is saved (parity with
 *      Claude Code's snapshot-sync `logForDebugging` lines,
 *      tools/AgentTool/agentMemorySnapshot.ts:L74-76,L154-159,L169-171).
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-consolidation-snapshot-introspection-w82.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { buildConsolidationPrompt } from '../src/memory-consolidation';
import {
  checkAgentMemorySnapshot,
  writeAgentSnapshotMeta,
  saveAgentSyncedMeta,
} from '../src/agent-memory-snapshot';
import { getAgentMemoryDir } from '../src/memory';

describe('U016-w82 — consolidation prompt prioritized daily-log stream', () => {
  it('names the dated daily-log path as a prioritized signal before the transcript grep', () => {
    const p = buildConsolidationPrompt('/mem/root', '/t/dir');
    // The dated daily-log stream is surfaced (rooted at the memory dir).
    assert.ok(p.includes('/mem/root/logs/YYYY/MM/YYYY-MM-DD.md'), 'daily-log path present');
    assert.ok(/priority order/i.test(p), 'gather is described as priority-ordered');
    // Priority order: the daily-log stream must be introduced BEFORE the JSONL grep.
    const logIdx = p.indexOf('/mem/root/logs/YYYY/MM/YYYY-MM-DD.md');
    const grepIdx = p.indexOf("--include=\"*.jsonl\"");
    assert.ok(logIdx > -1 && grepIdx > -1 && logIdx < grepIdx, 'daily logs come before transcript grep');
    // Still embeds the transcript dir (regression lock on the existing knob).
    assert.ok(p.includes('/t/dir'));
  });
});

describe('U016-w82 — agent-memory snapshot lifecycle introspection', () => {
  const AGENT = 'my-agent';
  const SCOPE = 'project' as const;
  let tmpDir: string;
  let diagFile: string;
  const savedDiag = process.env.AMI_DIAGNOSTICS_FILE;

  function readActions(): string[] {
    if (!fs.existsSync(diagFile)) return [];
    return fs
      .readFileSync(diagFile, 'utf-8')
      .split('\n')
      .filter(Boolean)
      .map((l) => JSON.parse(l) as Record<string, unknown>)
      .filter((r) => r.event === 'agent_memory_snapshot')
      .map((r) => (r.data as Record<string, unknown>).action as string);
  }

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mem-snap82-'));
    fs.mkdirSync(path.join(tmpDir, '.git'), { recursive: true });
    diagFile = path.join(tmpDir, 'diag.jsonl');
    process.env.AMI_DIAGNOSTICS_FILE = diagFile;
  });

  afterEach(() => {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ }
    if (savedDiag === undefined) delete process.env.AMI_DIAGNOSTICS_FILE;
    else process.env.AMI_DIAGNOSTICS_FILE = savedDiag;
  });

  it('emits NO diagnostic when there is nothing to do (action none)', () => {
    assert.deepEqual(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir), { action: 'none' });
    assert.deepEqual(readActions(), []);
  });

  it('emits an initialize diagnostic when a snapshot exists but no local memory', () => {
    writeAgentSnapshotMeta(AGENT, tmpDir, '2026-01-02T00:00:00Z');
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'initialize');
    assert.deepEqual(readActions(), ['initialize']);
  });

  it('emits a prompt-update diagnostic when the snapshot is newer than the synced marker', () => {
    writeAgentSnapshotMeta(AGENT, tmpDir, '2026-02-01T00:00:00Z');
    const localDir = getAgentMemoryDir(AGENT, SCOPE, tmpDir);
    fs.mkdirSync(localDir, { recursive: true });
    fs.writeFileSync(path.join(localDir, 'x.md'), '# notes');
    saveAgentSyncedMeta(AGENT, SCOPE, tmpDir, '2026-01-01T00:00:00Z');
    // saveAgentSyncedMeta emits 'synced'; the check then emits 'prompt-update'.
    assert.equal(checkAgentMemorySnapshot(AGENT, SCOPE, tmpDir).action, 'prompt-update');
    const actions = readActions();
    assert.ok(actions.includes('synced'), 'sync marker emitted a synced diagnostic');
    assert.ok(actions.includes('prompt-update'), 'drift emitted a prompt-update diagnostic');
  });

  it('saveAgentSyncedMeta emits a no-PII synced diagnostic (scope only, no agent path)', () => {
    saveAgentSyncedMeta(AGENT, SCOPE, tmpDir, '2026-03-03T00:00:00Z');
    assert.deepEqual(readActions(), ['synced']);
    const raw = fs.readFileSync(diagFile, 'utf-8');
    assert.ok(!raw.includes(tmpDir), 'no filesystem path leaks into the no-PII channel');
  });
});
