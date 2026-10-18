import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateMcpToolSchema,
  sanitizeMcpToolSchema,
  MAX_MCP_TOOLS_PER_SERVER,
  MAX_MCP_TOOL_NAME_LENGTH,
} from '../src/mcp/manager';

describe('MCP schema validation constants', () => {
  it('MAX_MCP_TOOLS_PER_SERVER is 200', () => {
    assert.equal(MAX_MCP_TOOLS_PER_SERVER, 200);
  });

  it('MAX_MCP_TOOL_NAME_LENGTH is 128', () => {
    assert.equal(MAX_MCP_TOOL_NAME_LENGTH, 128);
  });
});

describe('validateMcpToolSchema', () => {
  it('accepts a valid schema with properties and required', () => {
    const schema = {
      type: 'object',
      properties: { name: { type: 'string' } },
      required: ['name'],
    };
    assert.deepStrictEqual(validateMcpToolSchema(schema), { valid: true });
  });

  it('accepts a minimal valid schema (type: object only)', () => {
    assert.deepStrictEqual(validateMcpToolSchema({ type: 'object' }), { valid: true });
  });

  it('accepts schema with empty properties', () => {
    assert.deepStrictEqual(
      validateMcpToolSchema({ type: 'object', properties: {} }),
      { valid: true },
    );
  });

  it('rejects null', () => {
    const result = validateMcpToolSchema(null);
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('null'));
  });

  it('rejects undefined', () => {
    const result = validateMcpToolSchema(undefined);
    assert.equal(result.valid, false);
  });

  it('rejects a string', () => {
    const result = validateMcpToolSchema('not an object');
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('non-array object'));
  });

  it('rejects an array', () => {
    const result = validateMcpToolSchema([{ type: 'object' }]);
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('non-array object'));
  });

  it('rejects missing type field', () => {
    const result = validateMcpToolSchema({ properties: {} });
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes("'object'"));
  });

  it('rejects wrong type (not "object")', () => {
    const result = validateMcpToolSchema({ type: 'string' });
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes("'string'"));
  });

  it('rejects non-object properties', () => {
    const result = validateMcpToolSchema({ type: 'object', properties: 'bad' });
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('properties'));
  });

  it('rejects array properties', () => {
    const result = validateMcpToolSchema({ type: 'object', properties: [] });
    assert.equal(result.valid, false);
  });

  it('rejects null properties', () => {
    const result = validateMcpToolSchema({ type: 'object', properties: null });
    assert.equal(result.valid, false);
  });

  it('rejects non-array required', () => {
    const result = validateMcpToolSchema({ type: 'object', required: 'name' });
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('array'));
  });

  it('rejects non-string items in required', () => {
    const result = validateMcpToolSchema({ type: 'object', required: [42] });
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes('strings'));
  });

  it('rejects required keys not in properties', () => {
    const result = validateMcpToolSchema({
      type: 'object',
      properties: { a: { type: 'string' } },
      required: ['a', 'b'],
    });
    assert.equal(result.valid, false);
    assert.ok(result.error!.includes("'b'"));
  });

  it('allows required without properties (no cross-check)', () => {
    const result = validateMcpToolSchema({
      type: 'object',
      required: ['x'],
    });
    assert.deepStrictEqual(result, { valid: true });
  });
});

describe('sanitizeMcpToolSchema', () => {
  it('passes a valid schema through', () => {
    const schema = {
      type: 'object',
      properties: { name: { type: 'string' } },
      required: ['name'],
    };
    const result = sanitizeMcpToolSchema(schema);
    assert.deepStrictEqual(result, schema);
  });

  it('returns empty schema for invalid input (null)', () => {
    const result = sanitizeMcpToolSchema(null);
    assert.deepStrictEqual(result, { type: 'object', properties: {} });
  });

  it('returns empty schema for non-object input', () => {
    const result = sanitizeMcpToolSchema('bad');
    assert.deepStrictEqual(result, { type: 'object', properties: {} });
  });

  it('returns empty schema when type is wrong', () => {
    const result = sanitizeMcpToolSchema({ type: 'array' });
    assert.deepStrictEqual(result, { type: 'object', properties: {} });
  });

  it('strips unknown top-level keys', () => {
    const schema = {
      type: 'object',
      properties: { a: { type: 'number' } },
      unknownKey: 'should be stripped',
      anotherBad: 123,
    };
    const result = sanitizeMcpToolSchema(schema) as Record<string, unknown>;
    assert.equal(result.type, 'object');
    assert.ok('properties' in result);
    assert.equal('unknownKey' in result, false);
    assert.equal('anotherBad' in result, false);
  });

  it('preserves allowed keys like additionalProperties', () => {
    const schema = {
      type: 'object',
      properties: {},
      additionalProperties: false,
      description: 'a tool',
    };
    const result = sanitizeMcpToolSchema(schema) as Record<string, unknown>;
    assert.equal(result.additionalProperties, false);
    assert.equal(result.description, 'a tool');
  });

  it('preserves allOf/anyOf/oneOf composition keys', () => {
    const schema = {
      type: 'object',
      allOf: [{ properties: { a: { type: 'string' } } }],
    };
    const result = sanitizeMcpToolSchema(schema) as Record<string, unknown>;
    assert.ok(Array.isArray(result.allOf));
  });
});
