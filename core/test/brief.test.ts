import { describe, it } from 'node:test';
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
});
