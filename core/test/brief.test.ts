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
});
