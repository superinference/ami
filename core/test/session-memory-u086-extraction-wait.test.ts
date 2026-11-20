/**
 * session-memory-u086-extraction-wait — unit coverage for the extraction-wait /
 * staleness coordination primitives (U086 gap: EXTRACTION_WAIT_TIMEOUT_MS /
 * EXTRACTION_STALE_THRESHOLD_MS + markExtractionStarted/Completed +
 * waitForSessionMemoryExtraction).
 *
 * Parity: the reference's sessionMemoryUtils.ts (L12-13, L74-106) tracks extractionStartedAt
 * and exposes a wait helper that blocks up to a timeout and bails on a stale pass, so
 * a consumer (compaction/shutdown) can coordinate with an in-flight extraction without
 * hanging. These tests drive the wait/stale/timeout branches with an injected clock +
 * sleep seam (no real timers) and prove extractFacts marks the in-flight window.
 *
 * ESM-safe: no source-text reads, deterministic clock, temp config dir.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'node:os';
import * as fs from 'node:fs';
import * as path from 'node:path';

import {
  SessionMemoryExtractor,
  EXTRACTION_WAIT_TIMEOUT_MS,
  EXTRACTION_STALE_THRESHOLD_MS,
  DEFAULT_SESSION_MEMORY_CONFIG,
} from '../src/session-memory';
import type { ProviderConfig, StreamChunk } from '../src/types';

const provider: ProviderConfig = { model: 'test', apiKey: 'x', baseUrl: 'http://localhost' };

function contentChunk(text: string): StreamChunk {
  return { type: 'content_delta', text } as unknown as StreamChunk;
}

describe('U086 extraction-wait constants', () => {
  it('exports the reference-parity defaults (15s wait, 60s stale)', () => {
    assert.equal(EXTRACTION_WAIT_TIMEOUT_MS, 15_000);
    assert.equal(EXTRACTION_STALE_THRESHOLD_MS, 60_000);
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.extractionWaitTimeoutMs, 15_000);
    assert.equal(DEFAULT_SESSION_MEMORY_CONFIG.extractionStaleThresholdMs, 60_000);
  });

  it('honors per-instance overrides of the wait/stale knobs', () => {
    const ex = new SessionMemoryExtractor(provider, undefined, {
      extractionWaitTimeoutMs: 1,
      extractionStaleThresholdMs: 2,
    });
    assert.equal(ex.config.extractionWaitTimeoutMs, 1);
    assert.equal(ex.config.extractionStaleThresholdMs, 2);
  });
});

describe('U086 markExtractionStarted / Completed / isExtractionInProgress', () => {
  it('tracks the in-flight flag across start and completion', () => {
    const ex = new SessionMemoryExtractor(provider);
    assert.equal(ex.isExtractionInProgress(), false);
    ex.markExtractionStarted();
    assert.equal(ex.isExtractionInProgress(), true);
    ex.markExtractionCompleted();
    assert.equal(ex.isExtractionInProgress(), false);
  });
});

describe('U086 waitForSessionMemoryExtraction', () => {
  it('returns immediately when no extraction is in flight (never sleeps)', async () => {
    const ex = new SessionMemoryExtractor(provider);
    let slept = 0;
    ex._sleep = async () => { slept++; };
    await ex.waitForSessionMemoryExtraction();
    assert.equal(slept, 0);
  });

  it('blocks while a pass is in flight and resolves once it completes', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex.markExtractionStarted();
    let slept = 0;
    // Simulate the pass finishing during the first poll.
    ex._sleep = async () => { slept++; ex.markExtractionCompleted(); };
    await ex.waitForSessionMemoryExtraction();
    assert.equal(slept, 1);
    assert.equal(ex.isExtractionInProgress(), false);
  });

  it('bails out without blocking when the in-flight pass is STALE', async () => {
    let nowMs = 1_000;
    const ex = new SessionMemoryExtractor(provider);
    ex._now = () => new Date(nowMs);
    let slept = 0;
    ex._sleep = async () => { slept++; };
    ex.markExtractionStarted(); // startedAt = 1000
    // Advance the clock past the stale threshold (60s) before waiting.
    nowMs += EXTRACTION_STALE_THRESHOLD_MS + 1;
    await ex.waitForSessionMemoryExtraction();
    assert.equal(slept, 0, 'stale pass must not be waited on');
    // Flag stays set (we did not complete it) — the wait just stopped blocking.
    assert.equal(ex.isExtractionInProgress(), true);
  });

  it('bails out after the wait timeout when the pass never completes (not stale)', async () => {
    let nowMs = 1_000;
    const ex = new SessionMemoryExtractor(provider);
    ex._now = () => new Date(nowMs);
    let slept = 0;
    // Each poll advances the clock past the wait timeout but stays below the stale
    // threshold, so the TIMEOUT branch (not the stale branch) ends the wait.
    ex._sleep = async () => { slept++; nowMs += EXTRACTION_WAIT_TIMEOUT_MS + 1; };
    ex.markExtractionStarted(); // startedAt = 1000
    await ex.waitForSessionMemoryExtraction();
    assert.equal(slept, 1, 'should poll once, then bail on timeout');
    assert.ok(nowMs - 1_000 > EXTRACTION_WAIT_TIMEOUT_MS);
    assert.ok(nowMs - 1_000 < EXTRACTION_STALE_THRESHOLD_MS);
  });
});

describe('U086 extractFacts in-flight marking', () => {
  function tmpConfigDir(): string {
    return fs.mkdtempSync(path.join(os.tmpdir(), 'si-u086-'));
  }

  it('marks the in-flight window during extraction and clears it after success', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._configDir = tmpConfigDir();
    let seenInFlight = false;
    ex._streamFn = (async function* () {
      seenInFlight = ex.isExtractionInProgress();
      yield contentChunk('[]');
    }) as unknown as SessionMemoryExtractor['_streamFn'];

    assert.equal(ex.isExtractionInProgress(), false);
    await ex.extractFacts([{ toolName: 'bash', output: 'x', isError: false }], []);
    assert.equal(seenInFlight, true, 'flag must be set while streaming');
    assert.equal(ex.isExtractionInProgress(), false, 'flag must clear after completion');
  });

  it('clears the in-flight flag even when the stream emits an error chunk', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._configDir = tmpConfigDir();
    ex._streamFn = (async function* () {
      yield { type: 'error', error: 'boom' } as unknown as StreamChunk;
    }) as unknown as SessionMemoryExtractor['_streamFn'];
    const facts = await ex.extractFacts([{ toolName: 'bash', output: 'x', isError: false }], []);
    assert.deepEqual(facts, []);
    assert.equal(ex.isExtractionInProgress(), false);
  });

  it('clears the in-flight flag when the stream throws', async () => {
    const ex = new SessionMemoryExtractor(provider);
    ex._configDir = tmpConfigDir();
    ex._streamFn = (async function* () {
      yield contentChunk('partial');
      throw new Error('stream failure');
    }) as unknown as SessionMemoryExtractor['_streamFn'];
    const facts = await ex.extractFacts([{ toolName: 'bash', output: 'x', isError: false }], []);
    assert.deepEqual(facts, []);
    assert.equal(ex.isExtractionInProgress(), false);
  });
});
