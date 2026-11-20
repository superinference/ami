/**
 * U020 — toolUseSummaryLabelEnabled() opt-in flag (core unit tests).
 *
 * Parity: the reference gates the tool-use summary generator behind
 * `CLAUDE_CODE_EMIT_TOOL_USE_SUMMARIES` (query/config.ts). AMI honors that AND the
 * AMI-native `SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES`; default OFF (the extra
 * cheap-model call per tool batch is opt-in).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { toolUseSummaryLabelEnabled } from '../src/provider';

const KEYS = ['SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES', 'CLAUDE_CODE_EMIT_TOOL_USE_SUMMARIES'] as const;
const saved: Record<string, string | undefined> = {};

beforeEach(() => { for (const k of KEYS) { saved[k] = process.env[k]; delete process.env[k]; } });
afterEach(() => {
  for (const k of KEYS) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
});

describe('U020 toolUseSummaryLabelEnabled', () => {
  it('is OFF by default', () => {
    assert.equal(toolUseSummaryLabelEnabled(), false);
  });
  it('is ON for the AMI-native knob', () => {
    process.env.SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES = '1';
    assert.equal(toolUseSummaryLabelEnabled(), true);
  });
  it('is ON for the Claude-compat knob', () => {
    process.env.CLAUDE_CODE_EMIT_TOOL_USE_SUMMARIES = 'true';
    assert.equal(toolUseSummaryLabelEnabled(), true);
  });
  it('treats a falsy value as OFF', () => {
    process.env.SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES = 'false';
    assert.equal(toolUseSummaryLabelEnabled(), false);
    process.env.SUPERINFERENCE_EMIT_TOOL_USE_SUMMARIES = '0';
    assert.equal(toolUseSummaryLabelEnabled(), false);
  });
});
