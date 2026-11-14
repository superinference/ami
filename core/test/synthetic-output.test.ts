import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { syntheticOutputTool, validateAgainstSchema, MAX_RESULT_SIZE_CHARS } from '../src/tools/synthetic-output';

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

  it('description instructs calling exactly once at the end', () => {
    assert.match(syntheticOutputTool.description, /exactly once at the end/);
  });

  describe('schema validation', () => {
    it('accepts data matching the provided schema', async () => {
      const schema = { type: 'object', required: ['name'], properties: { name: { type: 'string' } } };
      const result = await syntheticOutputTool.execute!({ data: { name: 'ok' }, schema }, {} as any);
      assert.ok(!result.isError);
    });

    it('rejects data missing a required property with a descriptive error', async () => {
      const schema = { type: 'object', required: ['name'], properties: { name: { type: 'string' } } };
      const result = await syntheticOutputTool.execute!({ data: {}, schema }, {} as any);
      assert.equal(result.isError, true);
      assert.match(result.output, /data\.name is required/);
    });

    it('rejects a type mismatch', async () => {
      const schema = { type: 'object', properties: { n: { type: 'number' } } };
      const result = await syntheticOutputTool.execute!({ data: { n: 'str' }, schema }, {} as any);
      assert.equal(result.isError, true);
      assert.match(result.output, /data\.n should be number but got string/);
    });

    it('ignores a non-object schema', async () => {
      const result = await syntheticOutputTool.execute!({ data: { x: 1 }, schema: 'not-a-schema' }, {} as any);
      assert.ok(!result.isError);
    });
  });

  describe('result-size cap', () => {
    it('truncates and flags output larger than the cap', async () => {
      const big = 'x'.repeat(MAX_RESULT_SIZE_CHARS + 100);
      const result = await syntheticOutputTool.execute!({ data: { big } }, {} as any);
      assert.equal(result.isError, true);
      assert.match(result.output, /truncated at/);
    });
  });

  it('errors when data is not serializable (function value)', async () => {
    const result = await syntheticOutputTool.execute!({ data: (() => 1) as any }, {} as any);
    assert.equal(result.isError, true);
    assert.match(result.output, /not serializable/);
  });

  it('errors on a circular structure', async () => {
    const circ: any = {};
    circ.self = circ;
    const result = await syntheticOutputTool.execute!({ data: circ }, {} as any);
    assert.equal(result.isError, true);
    assert.match(result.output, /Error serializing/);
  });

  describe('validateAgainstSchema unit', () => {
    it('validates enum, const, integer, array items, additionalProperties', () => {
      assert.deepEqual(validateAgainstSchema('a', { enum: ['a', 'b'] }), []);
      assert.ok(validateAgainstSchema('z', { enum: ['a', 'b'] }).length > 0);
      assert.deepEqual(validateAgainstSchema(5, { const: 5 }), []);
      assert.ok(validateAgainstSchema(6, { const: 5 }).length > 0);
      assert.deepEqual(validateAgainstSchema(3, { type: 'integer' }), []);
      assert.ok(validateAgainstSchema(3.5, { type: 'integer' }).length > 0);
      assert.deepEqual(validateAgainstSchema([1, 2], { type: 'array', items: { type: 'number' }, minItems: 1, maxItems: 3 }), []);
      assert.ok(validateAgainstSchema([], { type: 'array', minItems: 1 }).length > 0);
      assert.ok(validateAgainstSchema([1, 2, 3, 4], { type: 'array', maxItems: 2 }).length > 0);
      assert.ok(validateAgainstSchema(['x'], { type: 'array', items: { type: 'number' } }).length > 0);
      assert.ok(
        validateAgainstSchema({ a: 1, b: 2 }, { type: 'object', additionalProperties: false, properties: { a: {} } }).length > 0,
      );
      assert.deepEqual(
        validateAgainstSchema({ a: 1, b: 2 }, { type: 'object', additionalProperties: { type: 'number' }, properties: { a: {} } }),
        [],
      );
      assert.ok(
        validateAgainstSchema({ a: 1, b: 'x' }, { type: 'object', additionalProperties: { type: 'number' }, properties: { a: {} } }).length > 0,
      );
      assert.deepEqual(validateAgainstSchema(null, { type: ['string', 'null'] }), []);
    });
  });
});
