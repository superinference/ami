import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import { briefTool } from '../src/tools/brief';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/tmp' } as ToolContext;

describe('briefTool', () => {
  it('has correct name and schema', () => {
    assert.equal(briefTool.name, 'brief');
    assert.ok(briefTool.isReadOnly);
    assert.ok(briefTool.inputSchema.properties.topic);
  });

  it('execute returns prompt with default topic when none given', async () => {
    const result = await briefTool.execute!({}, ctx);
    assert.ok(result.output.includes('current conversation'));
    assert.ok(result.output.includes('one sentence'));
  });

  it('execute includes topic when provided', async () => {
    const result = await briefTool.execute!({ topic: 'authentication flow' }, ctx);
    assert.ok(result.output.includes('authentication flow'));
    assert.ok(result.output.includes('one sentence'));
  });

  it('execute with empty string topic uses default', async () => {
    const result = await briefTool.execute!({ topic: '' }, ctx);
    assert.ok(result.output.includes('current conversation'));
  });

  it('does not return isError', async () => {
    const result = await briefTool.execute!({}, ctx);
    assert.equal(result.isError, undefined);
  });

  it("proactive status changes the prompt wording", async () => {
    const result = await briefTool.execute!({ topic: 'the deploy', status: 'proactive' }, ctx);
    assert.ok(result.output.includes('Proactively surface'));
    assert.ok(result.output.includes('the deploy'));
  });

  it('normal status yields the summarize wording', async () => {
    const result = await briefTool.execute!({ topic: 't', status: 'normal' }, ctx);
    assert.ok(result.output.includes('Summarize "t"'));
  });

  it('rejects an invalid status with the valid options', async () => {
    const result = await briefTool.execute!({ status: 'loud' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('normal, proactive'));
  });

  it('rejects unknown parameters', async () => {
    const result = await briefTool.execute!({ topics: 'typo' }, ctx);
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('unknown parameter'));
  });

  it('falls back to default for a truthy non-string topic (no coercion)', async () => {
    // A number is truthy, so `(input.topic as string) || default` would coerce it into the
    // prompt. The typeof guard must reject it and use the default instead.
    const result = await briefTool.execute!({ topic: 42 as unknown as string }, ctx);
    assert.ok(result.output.includes('current conversation'));
    assert.ok(!result.output.includes('42'));
  });

  it('caps an over-long topic to the maximum length', async () => {
    const topic = 'a'.repeat(500);
    const result = await briefTool.execute!({ topic }, ctx);
    // Capped at 300 chars: the 300-char run is present, the 301-char run is not.
    assert.ok(result.output.includes('a'.repeat(300)));
    assert.ok(!result.output.includes('a'.repeat(301)));
  });

  // --- Gap A: proactive/normal status surfaced as a structured discriminator ---
  describe('status discriminator in result.metadata', () => {
    it('tags a proactive brief with metadata.proactive === true', async () => {
      const result = await briefTool.execute!({ topic: 'x', status: 'proactive' }, ctx);
      assert.equal(result.isError, undefined);
      assert.deepEqual(result.metadata, { status: 'proactive', proactive: true });
    });

    it('tags a normal brief with metadata.proactive === false', async () => {
      const result = await briefTool.execute!({ topic: 'x', status: 'normal' }, ctx);
      assert.deepEqual(result.metadata, { status: 'normal', proactive: false });
    });

    it('defaults the discriminator to normal when status is omitted', async () => {
      const result = await briefTool.execute!({}, ctx);
      assert.deepEqual(result.metadata, { status: 'normal', proactive: false });
    });
  });

  // --- Gap B: validated config knob with safe default fallback ---
  describe('AMI_BRIEF_CONFIG validated config (safe-parse fallback)', () => {
    const KEY = 'AMI_BRIEF_CONFIG';
    const saved = process.env[KEY];
    afterEach(() => {
      if (saved === undefined) delete process.env[KEY];
      else process.env[KEY] = saved;
    });

    it('uses the hardcoded defaults when unset', async () => {
      delete process.env[KEY];
      const result = await briefTool.execute!({}, ctx);
      assert.ok(result.output.includes('current conversation'));
    });

    it('honors a valid config: custom cap truncates the topic', async () => {
      process.env[KEY] = JSON.stringify({ maxTopicLen: 5, defaultTopic: 'current conversation' });
      const result = await briefTool.execute!({ topic: 'abcdefghij' }, ctx);
      assert.ok(result.output.includes('"abcde"'));
      assert.ok(!result.output.includes('abcdef'));
    });

    it('honors a valid config: custom default topic', async () => {
      process.env[KEY] = JSON.stringify({ maxTopicLen: 300, defaultTopic: 'the whole session' });
      const result = await briefTool.execute!({}, ctx);
      assert.ok(result.output.includes('the whole session'));
      assert.ok(!result.output.includes('current conversation'));
    });

    it('falls back ENTIRELY to defaults on non-JSON config (never partial)', async () => {
      process.env[KEY] = 'not json {{{';
      const result = await briefTool.execute!({ topic: 'a'.repeat(500) }, ctx);
      // default cap 300 restored, default topic restored
      assert.ok(result.output.includes('a'.repeat(300)));
      assert.ok(!result.output.includes('a'.repeat(301)));
    });

    it('falls back on a malformed field: zero/negative/NaN cap drops the whole object', async () => {
      process.env[KEY] = JSON.stringify({ maxTopicLen: 0, defaultTopic: 'x' });
      const result = await briefTool.execute!({ topic: 'a'.repeat(500) }, ctx);
      // bad maxTopicLen → whole config reverts to defaults (cap 300, topic default)
      assert.ok(result.output.includes('a'.repeat(300)));
      assert.ok(!result.output.includes('a'.repeat(301)));
    });

    it('falls back when maxTopicLen is a non-integer', async () => {
      process.env[KEY] = JSON.stringify({ maxTopicLen: 10.5, defaultTopic: 'x' });
      const result = await briefTool.execute!({}, ctx);
      assert.ok(result.output.includes('current conversation'));
    });

    it('falls back when defaultTopic is missing or empty', async () => {
      process.env[KEY] = JSON.stringify({ maxTopicLen: 50, defaultTopic: '' });
      const result = await briefTool.execute!({}, ctx);
      assert.ok(result.output.includes('current conversation'));
    });

    it('falls back when the config is a JSON array, not an object', async () => {
      process.env[KEY] = JSON.stringify([1, 2, 3]);
      const result = await briefTool.execute!({}, ctx);
      assert.ok(result.output.includes('current conversation'));
    });

    it('falls back when the config is a JSON null', async () => {
      process.env[KEY] = JSON.stringify(null);
      const result = await briefTool.execute!({}, ctx);
      assert.ok(result.output.includes('current conversation'));
    });
  });
});
