import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { syntheticOutputTool } from '../src/tools/synthetic-output';

describe('syntheticOutputTool', () => {
  it('has correct name and metadata', () => {
    assert.equal(syntheticOutputTool.name, 'structured_output');
    assert.ok(syntheticOutputTool.isReadOnly);
    assert.deepEqual(syntheticOutputTool.inputSchema.required, ['data']);
  });

  it('serializes simple object', async () => {
    const result = await syntheticOutputTool.execute!({ data: { key: 'value' } }, {} as any);
    assert.ok(!result.isError);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.key, 'value');
  });

  it('serializes nested object', async () => {
    const result = await syntheticOutputTool.execute!({ data: { a: { b: [1, 2, 3] } } }, {} as any);
    assert.ok(!result.isError);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed.a.b, [1, 2, 3]);
  });

  it('handles schema_name parameter', async () => {
    const result = await syntheticOutputTool.execute!({ data: { x: 1 }, schema_name: 'test' }, {} as any);
    assert.ok(!result.isError);
    const parsed = JSON.parse(result.output);
    assert.equal(parsed.x, 1);
  });

  it('handles null data', async () => {
    const result = await syntheticOutputTool.execute!({ data: null }, {} as any);
    assert.ok(!result.isError);
    assert.equal(result.output, 'null');
  });

  it('handles array data', async () => {
    const result = await syntheticOutputTool.execute!({ data: [1, 2, 3] }, {} as any);
    assert.ok(!result.isError);
    const parsed = JSON.parse(result.output);
    assert.deepEqual(parsed, [1, 2, 3]);
  });
});
