/**
 * U083 — unit: session-scoped cost persistence (session-id boundary check) + render-perf
 * (FPS) metrics persisted alongside the serialized cost state.
 *
 * PARITY (the reference's getStoredSessionCosts / saveCurrentSessionCosts):
 *  - getStoredSessionCosts returns undefined when projectConfig.lastSessionId !== the
 *    resuming sessionId — another session's spend is never restored into the wrong session.
 *    AMI mirrors this with an embedded `sessionId` in the serialized cost state and the
 *    guarded `deserializeCostStateForSession(json, sessionId)` reader.
 *  - saveCurrentSessionCosts(getFpsMetrics()) persists lastFpsAverage/lastFpsLow1Pct next
 *    to the costs. AMI mirrors this with an optional `fpsMetrics` field on {@link CostState}
 *    that round-trips through serialize/deserialize.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  serializeCostState,
  deserializeCostState,
  deserializeCostStateForSession,
  type CostState,
} from '../src/cost-tracker';

describe('U083: cost state session-id scoping', () => {
  it('round-trips an embedded sessionId through serialize/deserialize', () => {
    const state: CostState = {
      totalCost: 1.5, totalInputTokens: 100, totalOutputTokens: 40, requestCount: 3,
      sessionId: 'sess-abc',
    };
    const back = deserializeCostState(serializeCostState(state));
    assert.ok(back);
    assert.equal(back!.sessionId, 'sess-abc');
  });

  it('omits sessionId from the payload when absent or empty', () => {
    const noId = serializeCostState({ totalCost: 1, totalInputTokens: 1, totalOutputTokens: 1, requestCount: 1 });
    assert.equal(JSON.parse(noId).sessionId, undefined);
    const emptyId = serializeCostState({ totalCost: 1, totalInputTokens: 1, totalOutputTokens: 1, requestCount: 1, sessionId: '' });
    assert.equal(JSON.parse(emptyId).sessionId, undefined);
    // An unscoped state deserializes with no sessionId field.
    assert.equal(deserializeCostState(noId)!.sessionId, undefined);
  });

  it('deserializeCostStateForSession restores a MATCHING session', () => {
    const json = serializeCostState({
      totalCost: 2, totalInputTokens: 10, totalOutputTokens: 5, requestCount: 2, sessionId: 'sess-1',
    });
    const got = deserializeCostStateForSession(json, 'sess-1');
    assert.ok(got);
    assert.equal(got!.totalCost, 2);
    assert.equal(got!.sessionId, 'sess-1');
  });

  it('REJECTS (null) a state whose embedded sessionId does not match (boundary check)', () => {
    const json = serializeCostState({
      totalCost: 99, totalInputTokens: 10, totalOutputTokens: 5, requestCount: 2, sessionId: 'sess-OTHER',
    });
    assert.equal(deserializeCostStateForSession(json, 'sess-1'), null,
      "another session's spend must not restore into this session");
  });

  it('ALLOWS an unscoped state (no embedded sessionId) — file identity already scopes it', () => {
    const json = serializeCostState({ totalCost: 3, totalInputTokens: 1, totalOutputTokens: 1, requestCount: 1 });
    const got = deserializeCostStateForSession(json, 'sess-1');
    assert.ok(got, 'a legacy/unscoped state is session-agnostic and restorable');
    assert.equal(got!.totalCost, 3);
  });

  it('returns null for malformed JSON through the guarded reader', () => {
    assert.equal(deserializeCostStateForSession('not json', 'sess-1'), null);
    assert.equal(deserializeCostStateForSession('', 'sess-1'), null);
  });
});

describe('U083: render-performance (FPS) metrics persisted with costs', () => {
  const base = { totalCost: 1, totalInputTokens: 1, totalOutputTokens: 1, requestCount: 1 };

  it('round-trips fpsMetrics through serialize/deserialize', () => {
    const json = serializeCostState({ ...base, fpsMetrics: { averageFps: 59.9, low1PctFps: 31.2 } });
    const back = deserializeCostState(json);
    assert.ok(back?.fpsMetrics);
    assert.equal(back!.fpsMetrics!.averageFps, 59.9);
    assert.equal(back!.fpsMetrics!.low1PctFps, 31.2);
  });

  it('omits fpsMetrics when absent', () => {
    const json = serializeCostState(base);
    assert.equal(JSON.parse(json).fpsMetrics, undefined);
    assert.equal(deserializeCostState(json)!.fpsMetrics, undefined);
  });

  it('drops an invalid fpsMetrics on serialize (non-finite / negative)', () => {
    for (const bad of [
      { averageFps: NaN, low1PctFps: 10 },
      { averageFps: 10, low1PctFps: Infinity },
      { averageFps: -1, low1PctFps: 10 },
      { averageFps: 10, low1PctFps: -5 },
    ]) {
      const json = serializeCostState({ ...base, fpsMetrics: bad as { averageFps: number; low1PctFps: number } });
      assert.equal(JSON.parse(json).fpsMetrics, undefined, `invalid ${JSON.stringify(bad)} must be dropped`);
    }
  });

  it('drops a malformed fpsMetrics on deserialize', () => {
    const raw = JSON.stringify({ ...base, version: 1, fpsMetrics: { averageFps: 'fast', low1PctFps: 10 } });
    assert.equal(deserializeCostState(raw)!.fpsMetrics, undefined);
    const rawNeg = JSON.stringify({ ...base, version: 1, fpsMetrics: { averageFps: -2, low1PctFps: 10 } });
    assert.equal(deserializeCostState(rawNeg)!.fpsMetrics, undefined);
  });

  it('persists sessionId AND fpsMetrics together', () => {
    const json = serializeCostState({ ...base, sessionId: 'sess-1', fpsMetrics: { averageFps: 60, low1PctFps: 30 } });
    const got = deserializeCostStateForSession(json, 'sess-1');
    assert.ok(got?.fpsMetrics);
    assert.equal(got!.fpsMetrics!.averageFps, 60);
  });
});
