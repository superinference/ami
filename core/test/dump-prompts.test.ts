// ===========================================================================
// dump-prompts.test.ts
//
// Unit coverage for the per-session, deduplicated prompt-dump entry schema
// (core/core/src/dump-prompts.ts), mirroring Claude Code's createDumpPromptsFetch
// DumpState bookkeeping (services/api/dumpPrompts.ts):
//   - initFingerprint: cheap model|toolNames|systemLen change proxy (string vs
//     array system, missing tools/system).
//   - buildRequestDumpEntries: init once, system_update on drift, message per new
//     user turn; messageCountSeen advances; assistant turns skipped.
//   - the per-key DumpState registry (getOrCreateDumpState / clearDumpState /
//     clearAllDumpState) keeps each session independent.
// ===========================================================================

import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  createDumpState,
  initFingerprint,
  buildRequestDumpEntries,
  getOrCreateDumpState,
  clearDumpState,
  clearAllDumpState,
} from '../src/dump-prompts';

afterEach(() => clearAllDumpState());

describe('createDumpState', () => {
  it('returns a fresh uninitialized state', () => {
    assert.deepEqual(createDumpState(), {
      initialized: false,
      messageCountSeen: 0,
      lastInitFingerprint: '',
    });
  });
});

describe('initFingerprint', () => {
  it('combines model, comma-joined tool names, and system length (string system)', () => {
    assert.equal(
      initFingerprint({ model: 'm', tools: [{ name: 'a' }, { name: 'b' }], system: 'hello' }),
      'm|a,b|5',
    );
  });

  it('sums text lengths for an array (Anthropic-shape) system', () => {
    assert.equal(
      initFingerprint({ model: 'm', system: [{ text: 'ab' }, { text: 'cde' }] }),
      'm||5',
    );
  });

  it('treats missing tools/system as empty / zero length', () => {
    assert.equal(initFingerprint({ model: 'm' }), 'm||0');
  });

  it('tolerates a nameless tool and a non-array/non-string system', () => {
    assert.equal(initFingerprint({ model: 'm', tools: [{}], system: 42 }), 'm||0');
  });
});

describe('buildRequestDumpEntries', () => {
  it('emits an init (messages stripped) + a message entry on the first request', () => {
    const state = createDumpState();
    const lines = buildRequestDumpEntries(
      state,
      { model: 'm', system: 's', messages: [{ role: 'user', content: 'hi' }] },
      'TS',
    );
    const parsed = lines.map((l) => JSON.parse(l) as { type: string; timestamp: string; data: Record<string, unknown> });
    assert.deepEqual(parsed.map((p) => p.type), ['init', 'message']);
    assert.equal(parsed[0].timestamp, 'TS');
    assert.equal(parsed[0].data.model, 'm');
    assert.equal(parsed[0].data.messages, undefined);
    assert.equal((parsed[1].data as { content: string }).content, 'hi');
    assert.equal(state.initialized, true);
    assert.equal(state.messageCountSeen, 1);
  });

  it('emits NOTHING on a repeat request with identical init and no new messages', () => {
    const state = createDumpState();
    const req = { model: 'm', system: 's', messages: [{ role: 'user', content: 'hi' }] };
    buildRequestDumpEntries(state, req, 'T1');
    const lines = buildRequestDumpEntries(state, req, 'T2');
    assert.deepEqual(lines, []);
  });

  it('emits only a new message entry when init is unchanged but a user turn is added', () => {
    const state = createDumpState();
    buildRequestDumpEntries(state, { model: 'm', system: 's', messages: [{ role: 'user', content: 'u1' }] }, 'T1');
    const lines = buildRequestDumpEntries(
      state,
      { model: 'm', system: 's', messages: [{ role: 'user', content: 'u1' }, { role: 'assistant', content: 'a1' }, { role: 'user', content: 'u2' }] },
      'T2',
    );
    const parsed = lines.map((l) => JSON.parse(l) as { type: string; data: { content?: string } });
    assert.deepEqual(parsed.map((p) => p.type), ['message']);
    assert.equal(parsed[0].data.content, 'u2');
    assert.equal(state.messageCountSeen, 3);
  });

  it('emits system_update (not a second init) when the init blob drifts', () => {
    const state = createDumpState();
    buildRequestDumpEntries(state, { model: 'm', system: 'short', messages: [] }, 'T1');
    const lines = buildRequestDumpEntries(state, { model: 'm', system: 'a much longer system prompt', messages: [] }, 'T2');
    const parsed = lines.map((l) => JSON.parse(l) as { type: string });
    assert.deepEqual(parsed.map((p) => p.type), ['system_update']);
  });

  it('short-circuits on a matching fingerprint (same model|tools|sysLen) without a system_update', () => {
    const state = createDumpState();
    // Same fingerprint (model|tools|sysLen all identical) but different content:
    // the cheap fingerprint gate means no system_update is emitted (acceptable
    // dedup trade-off — matches the reference's fingerprint-first short-circuit).
    buildRequestDumpEntries(state, { model: 'm', system: 'aaaa', messages: [] }, 'T1');
    const lines = buildRequestDumpEntries(state, { model: 'm', system: 'bbbb', messages: [] }, 'T2');
    assert.deepEqual(lines, []);
  });

  it('skips assistant messages — only user turns become message entries', () => {
    const state = createDumpState();
    const lines = buildRequestDumpEntries(
      state,
      { model: 'm', system: 's', messages: [{ role: 'assistant', content: 'a0' }, { role: 'user', content: 'u0' }] },
      'T1',
    );
    const msgs = lines.map((l) => JSON.parse(l) as { type: string; data: { content?: string } }).filter((p) => p.type === 'message');
    assert.equal(msgs.length, 1);
    assert.equal(msgs[0].data.content, 'u0');
    assert.equal(state.messageCountSeen, 2);
  });

  it('defaults messages to empty when the field is absent', () => {
    const state = createDumpState();
    const lines = buildRequestDumpEntries(state, { model: 'm', system: 's' }, 'T1');
    assert.deepEqual(lines.map((l) => (JSON.parse(l) as { type: string }).type), ['init']);
    assert.equal(state.messageCountSeen, 0);
  });
});

describe('DumpState registry', () => {
  it('returns the same instance for a key and keeps sessions independent', () => {
    const a1 = getOrCreateDumpState('A');
    const a2 = getOrCreateDumpState('A');
    const b = getOrCreateDumpState('B');
    assert.equal(a1, a2);
    assert.notEqual(a1, b);

    buildRequestDumpEntries(a1, { model: 'm', system: 's', messages: [{ role: 'user', content: 'x' }] }, 'T1');
    assert.equal(getOrCreateDumpState('A').initialized, true);
    assert.equal(getOrCreateDumpState('B').initialized, false);
  });

  it('clearDumpState drops one session (next use re-emits init)', () => {
    const a = getOrCreateDumpState('A');
    a.initialized = true;
    clearDumpState('A');
    assert.equal(getOrCreateDumpState('A').initialized, false);
  });

  it('clearAllDumpState drops every session', () => {
    getOrCreateDumpState('A').initialized = true;
    getOrCreateDumpState('B').initialized = true;
    clearAllDumpState();
    assert.equal(getOrCreateDumpState('A').initialized, false);
    assert.equal(getOrCreateDumpState('B').initialized, false);
  });
});
