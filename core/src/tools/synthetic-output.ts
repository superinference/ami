import { ToolDefinition, ToolResult } from '../types';

/** Hard cap on the serialized structured-output result size. */
export const MAX_RESULT_SIZE_CHARS = 100_000;

type JsonSchema = {
  type?: string | string[];
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  enum?: unknown[];
  const?: unknown;
  additionalProperties?: boolean | JsonSchema;
  minItems?: number;
  maxItems?: number;
};

function typeOf(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/**
 * Minimal, dependency-free JSON-schema validator covering the common subset the
 * model is asked to produce (type, properties, required, items, enum, const,
 * additionalProperties, min/maxItems). Returns a list of human-readable
 * mismatch messages ([] when valid). Mirrors Claude's Ajv-backed validation of
 * SyntheticOutputTool output without pulling in a new dependency.
 */
export function validateAgainstSchema(data: unknown, schema: JsonSchema, pathPrefix = 'data'): string[] {
  const errors: string[] = [];

  if (schema.const !== undefined && JSON.stringify(data) !== JSON.stringify(schema.const)) {
    errors.push(`${pathPrefix} must equal ${JSON.stringify(schema.const)}`);
  }

  if (schema.enum && !schema.enum.some((e) => JSON.stringify(e) === JSON.stringify(data))) {
    errors.push(`${pathPrefix} must be one of ${schema.enum.map((e) => JSON.stringify(e)).join(', ')}`);
  }

  if (schema.type) {
    const expected = Array.isArray(schema.type) ? schema.type : [schema.type];
    const actual = typeOf(data);
    const ok = expected.some((t) => (t === 'integer' ? actual === 'number' && Number.isInteger(data as number) : actual === t));
    if (!ok) {
      errors.push(`${pathPrefix} should be ${expected.join(' | ')} but got ${actual}`);
      return errors; // further checks assume the base type matched
    }
  }

  if (typeOf(data) === 'object') {
    const obj = data as Record<string, unknown>;
    for (const key of schema.required ?? []) {
      if (!(key in obj)) errors.push(`${pathPrefix}.${key} is required`);
    }
    if (schema.properties) {
      for (const [key, sub] of Object.entries(schema.properties)) {
        if (key in obj) errors.push(...validateAgainstSchema(obj[key], sub, `${pathPrefix}.${key}`));
      }
    }
    if (schema.additionalProperties === false && schema.properties) {
      for (const key of Object.keys(obj)) {
        if (!(key in schema.properties)) errors.push(`${pathPrefix}.${key} is not an allowed property`);
      }
    } else if (schema.additionalProperties && typeof schema.additionalProperties === 'object') {
      const known = schema.properties ?? {};
      for (const key of Object.keys(obj)) {
        if (!(key in known)) {
          errors.push(...validateAgainstSchema(obj[key], schema.additionalProperties, `${pathPrefix}.${key}`));
        }
      }
    }
  }

  if (typeOf(data) === 'array') {
    const arr = data as unknown[];
    if (schema.minItems !== undefined && arr.length < schema.minItems) {
      errors.push(`${pathPrefix} must have at least ${schema.minItems} item(s)`);
    }
    if (schema.maxItems !== undefined && arr.length > schema.maxItems) {
      errors.push(`${pathPrefix} must have at most ${schema.maxItems} item(s)`);
    }
    if (schema.items) {
      arr.forEach((item, i) => errors.push(...validateAgainstSchema(item, schema.items!, `${pathPrefix}[${i}]`)));
    }
  }

  return errors;
}

export const syntheticOutputTool: ToolDefinition = {
  name: 'structured_output',
  description:
    'Return structured JSON output. Use this to produce machine-parseable results. ' +
    'Pass the result in `data`, and optionally a JSON `schema` the data must satisfy — ' +
    'if provided, the data is validated against it and rejected with a descriptive error on mismatch. ' +
    'You MUST call this tool exactly once at the end of your response when structured output is requested.',
  inputSchema: {
    type: 'object',
    properties: {
      schema_name: { type: 'string', description: 'Name/label for the output schema.' },
      schema: { type: 'object', description: 'Optional JSON schema the data must satisfy (type/properties/required/items/enum).' },
      data: { type: 'object', description: 'The structured JSON data to return.' },
    },
    required: ['data'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>): Promise<ToolResult> {
    const data = input.data;
    const schema = input.schema;

    if (schema && typeof schema === 'object' && !Array.isArray(schema)) {
      const errors = validateAgainstSchema(data, schema as JsonSchema);
      if (errors.length > 0) {
        return {
          output: `Structured output does not match the schema:\n- ${errors.join('\n- ')}`,
          isError: true,
        };
      }
    }

    let serialized: string;
    try {
      serialized = JSON.stringify(data, null, 2);
    } catch (err) {
      return { output: `Error serializing structured output: ${err}`, isError: true };
    }
    if (serialized === undefined) {
      return { output: 'Error: data is not serializable to JSON.', isError: true };
    }
    if (serialized.length > MAX_RESULT_SIZE_CHARS) {
      return {
        output:
          serialized.slice(0, MAX_RESULT_SIZE_CHARS) +
          `\n\n[... structured output truncated at ${MAX_RESULT_SIZE_CHARS} chars (was ${serialized.length}) ...]`,
        isError: true,
      };
    }
    return { output: serialized };
  },
};
