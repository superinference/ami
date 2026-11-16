import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  syntheticOutputTool,
  createSyntheticOutputTool,
  validateAgainstSchema,
  MAX_RESULT_SIZE_CHARS,
  ndjsonSafeStringify,
} from '../src/tools/synthetic-output';

const LS = '\u2028'; // LINE SEPARATOR
const PS = '\u2029'; // PARAGRAPH SEPARATOR

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

  describe('NDJSON-safe serialization (U+2028 / U+2029)', () => {
    it('escapes line/paragraph separators in the tool output so no raw JS line terminator leaks', async () => {
      const result = await syntheticOutputTool.execute!({ data: { note: `a${LS}b${PS}c` } }, {} as any);
      assert.ok(!result.isError);
      // The raw separators must NOT appear verbatim in the emitted record.
      assert.ok(!result.output.includes(LS), 'raw U+2028 must not leak into output');
      assert.ok(!result.output.includes(PS), 'raw U+2029 must not leak into output');
      // They must be present as \u-escapes instead.
      assert.ok(result.output.includes('\\u2028'));
      assert.ok(result.output.includes('\\u2029'));
      // And the record must still parse back to the exact original value.
      assert.equal(JSON.parse(result.output).note, `a${LS}b${PS}c`);
    });

    it('ndjsonSafeStringify keeps the output parse-identical while removing raw separators', () => {
      const value = { s: `x${LS}${PS}y`, arr: [LS, PS] };
      const out = ndjsonSafeStringify(value, 2);
      assert.ok(!out.includes(LS));
      assert.ok(!out.includes(PS));
      assert.deepEqual(JSON.parse(out), value);
    });

    it('ndjsonSafeStringify returns undefined for non-serializable input (passthrough)', () => {
      assert.equal(ndjsonSafeStringify(undefined), undefined);
    });
  });

  describe('createSyntheticOutputTool factory (schema-bound variant)', () => {
    const schema = {
      type: 'object',
      required: ['name', 'count'],
      additionalProperties: false,
      properties: { name: { type: 'string' }, count: { type: 'integer' } },
    };

    it('returns a tool whose inputSchema is the caller-supplied JSON schema', () => {
      const created = createSyntheticOutputTool(schema);
      assert.ok('tool' in created);
      if (!('tool' in created)) return;
      // The model sees the real required fields / types, not the generic wrapper.
      assert.equal(created.tool.name, 'structured_output');
      assert.ok(created.tool.isReadOnly);
      assert.deepEqual(created.tool.inputSchema as unknown, schema);
    });

    it('validates the call input against the bound schema and serializes on success', async () => {
      const created = createSyntheticOutputTool(schema);
      assert.ok('tool' in created);
      if (!('tool' in created)) return;
      const result = await created.tool.execute({ name: 'ok', count: 2 }, {} as any);
      assert.ok(!result.isError);
      assert.deepEqual(JSON.parse(result.output), { name: 'ok', count: 2 });
    });

    it('rejects a non-conforming call input with a descriptive per-path error', async () => {
      const created = createSyntheticOutputTool(schema);
      assert.ok('tool' in created);
      if (!('tool' in created)) return;
      const result = await created.tool.execute({ count: 1.5, extra: true } as any, {} as any);
      assert.equal(result.isError, true);
      assert.match(result.output, /Output does not match required schema/);
      assert.match(result.output, /data\.name is required/);
      assert.match(result.output, /data\.count should be integer/);
      assert.match(result.output, /data\.extra is not an allowed property/);
    });

    it('enforces the result-size cap on the schema-bound variant', async () => {
      const strSchema = { type: 'object', properties: { big: { type: 'string' } } };
      const created = createSyntheticOutputTool(strSchema);
      assert.ok('tool' in created);
      if (!('tool' in created)) return;
      const result = await created.tool.execute({ big: 'x'.repeat(MAX_RESULT_SIZE_CHARS + 100) }, {} as any);
      assert.equal(result.isError, true);
      assert.match(result.output, /truncated at/);
    });

    it('returns {error} with a diagnostic when the supplied schema is itself malformed', () => {
      const bad = createSyntheticOutputTool({ type: 'nonsense' });
      assert.ok('error' in bad);
      if (!('error' in bad)) return;
      assert.match(bad.error, /nonsense|type/i);

      const badProps = createSyntheticOutputTool({ type: 'object', properties: 'not-an-object' as unknown as Record<string, unknown> });
      assert.ok('error' in badProps);

      const badRequired = createSyntheticOutputTool({ type: 'object', required: [1, 2] as unknown as string[] });
      assert.ok('error' in badRequired);
    });

    it('memoizes the compiled result by schema object identity (WeakMap cache)', () => {
      const s = { type: 'object', properties: { a: { type: 'string' } } };
      const first = createSyntheticOutputTool(s);
      const second = createSyntheticOutputTool(s);
      // Same object reference -> exact same cached result (no recompile).
      assert.strictEqual(first, second);
      // A different (even deep-equal) object -> a distinct result.
      const other = createSyntheticOutputTool({ type: 'object', properties: { a: { type: 'string' } } });
      assert.notStrictEqual(first, other);
    });
  });

  describe('syntheticOutputTool delegates to the schema-bound factory', () => {
    it('rejects a malformed caller schema instead of silently accepting output', async () => {
      const result = await syntheticOutputTool.execute!(
        { data: { x: 1 }, schema: { type: 'nonsense' } },
        {} as any,
      );
      assert.equal(result.isError, true);
      assert.match(result.output, /schema is invalid/i);
    });
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
