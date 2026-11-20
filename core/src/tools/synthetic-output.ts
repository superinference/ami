import { ToolDefinition, ToolResult } from '../types';

/** Hard cap on the serialized structured-output result size. */
export const MAX_RESULT_SIZE_CHARS = 100_000;

export type JsonSchema = {
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

/**
 * Escape the two Unicode code points that `JSON.stringify` emits verbatim but
 * that ECMA-262 (section 11.3) treats as line terminators: U+2028 (LINE
 * SEPARATOR) and U+2029 (PARAGRAPH SEPARATOR). A model-supplied `data` value
 * carrying either char would otherwise embed a raw JS line break inside the
 * serialized record, so any NDJSON / line-split consumer (a `tail`, a log
 * shipper, the Agent SDK transport) would cut the payload mid-string and
 * silently drop it. Replacing them with their `\uXXXX` escapes keeps the output
 * parse-identical while making it unbreakable by line-terminator-aware
 * receivers. Mirrors the reference's `ndjsonSafeStringify`.
 */
export function ndjsonSafeStringify(value: unknown, space?: number): string {
  const serialized = JSON.stringify(value, null, space);
  if (serialized === undefined) return serialized as unknown as string;
  return serialized.replace(/\u2028/g, '\\u2028').replace(/\u2029/g, '\\u2029');
}

function typeOf(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/**
 * Minimal, dependency-free JSON-schema validator covering the common subset the
 * model is asked to produce (type, properties, required, items, enum, const,
 * additionalProperties, min/maxItems). Returns a list of human-readable
 * mismatch messages ([] when valid). Mirrors the reference's Ajv-backed validation of
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

/** The JSON Schema primitive types this validator understands. */
const KNOWN_TYPES = new Set(['string', 'number', 'integer', 'boolean', 'object', 'array', 'null']);

/**
 * Validate that a caller-supplied value is itself a well-formed JSON schema for
 * the subset {@link validateAgainstSchema} enforces. Returns a human-readable
 * diagnostic (mirroring Ajv's `errorsText`) for the FIRST problem, or `null`
 * when the schema is usable. A structurally broken schema (e.g. a typo'd `type`,
 * `properties` that is not an object) must be rejected BEFORE it is used —
 * otherwise `validateAgainstSchema` would silently accept any output against it,
 * voiding the structured-output guarantee. Mirrors the reference's `ajv.validateSchema`
 * gate in `createSyntheticOutputTool`.
 */
export function validateSchemaShape(schema: unknown, path = 'schema'): string | null {
  if (typeof schema !== 'object' || schema === null || Array.isArray(schema)) {
    return `${path} must be a JSON Schema object`;
  }
  const s = schema as Record<string, unknown>;

  if ('type' in s) {
    const types = Array.isArray(s.type) ? s.type : [s.type];
    if (types.length === 0) return `${path}.type must name at least one type`;
    for (const t of types) {
      if (typeof t !== 'string' || !KNOWN_TYPES.has(t)) {
        return `${path}.type has unknown type "${String(t)}" (expected ${[...KNOWN_TYPES].join(' | ')})`;
      }
    }
  }
  if ('enum' in s && !Array.isArray(s.enum)) return `${path}.enum must be an array`;
  if ('required' in s) {
    if (!Array.isArray(s.required) || s.required.some((k) => typeof k !== 'string')) {
      return `${path}.required must be an array of strings`;
    }
  }
  if ('minItems' in s && typeof s.minItems !== 'number') return `${path}.minItems must be a number`;
  if ('maxItems' in s && typeof s.maxItems !== 'number') return `${path}.maxItems must be a number`;
  if ('properties' in s) {
    if (typeof s.properties !== 'object' || s.properties === null || Array.isArray(s.properties)) {
      return `${path}.properties must be an object`;
    }
    for (const [key, sub] of Object.entries(s.properties)) {
      const err = validateSchemaShape(sub, `${path}.properties.${key}`);
      if (err) return err;
    }
  }
  if ('items' in s) {
    const err = validateSchemaShape(s.items, `${path}.items`);
    if (err) return err;
  }
  if ('additionalProperties' in s && typeof s.additionalProperties !== 'boolean') {
    const err = validateSchemaShape(s.additionalProperties, `${path}.additionalProperties`);
    if (err) return err;
  }
  return null;
}

/**
 * Serialize a structured-output payload with line-terminator-safe stringify and
 * enforce the hard {@link MAX_RESULT_SIZE_CHARS} boundary. Shared by the generic
 * `structured_output` tool and every schema-bound variant produced by
 * {@link createSyntheticOutputTool} so the size cap / NDJSON safety live in ONE
 * place.
 */
export function serializeStructuredOutput(data: unknown): ToolResult {
  let serialized: string;
  try {
    serialized = ndjsonSafeStringify(data, 2);
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
}

/** Result of {@link createSyntheticOutputTool}: a schema-bound tool, or a diagnostic. */
export type CreateSyntheticOutputResult = { tool: ToolDefinition } | { error: string };

/**
 * Identity-keyed cache of compiled schema-bound tools. A workflow that calls the
 * agent with the SAME schema object reference many times (30-80×) reuses the one
 * validated/bound tool instead of re-walking well-formedness on every call.
 * Keyed by object identity (WeakMap) exactly as the reference's `toolCache`.
 */
const toolCache = new WeakMap<object, CreateSyntheticOutputResult>();

/**
 * Build a `structured_output` tool variant bound to a caller-supplied JSON
 * schema. The returned tool advertises that schema as its own `inputSchema` (so
 * the model sees the real required fields/types natively) and, on each call,
 * validates its input against the schema — returning an `isError` result that
 * lists every per-path mismatch, or the serialized output on success. Returns
 * `{ error }` (never a tool) when the supplied schema is itself malformed.
 *
 * Mirrors the reference's `createSyntheticOutputTool`: a per-schema tool whose call
 * validates-then-returns, with the compiled result memoized by schema identity.
 */
export function createSyntheticOutputTool(jsonSchema: Record<string, unknown>): CreateSyntheticOutputResult {
  const cached = toolCache.get(jsonSchema);
  if (cached) return cached;

  const shapeError = validateSchemaShape(jsonSchema);
  let result: CreateSyntheticOutputResult;
  if (shapeError) {
    result = { error: shapeError };
  } else {
    result = {
      tool: {
        ...syntheticOutputTool,
        // Expose the caller schema as the tool's real input contract so the
        // provider constrains generated arguments to it.
        inputSchema: jsonSchema as unknown as ToolDefinition['inputSchema'],
        async execute(input: Record<string, unknown>): Promise<ToolResult> {
          const errors = validateAgainstSchema(input, jsonSchema as JsonSchema);
          if (errors.length > 0) {
            return {
              output: `Output does not match required schema:\n- ${errors.join('\n- ')}`,
              isError: true,
            };
          }
          return serializeStructuredOutput(input);
        },
      },
    };
  }

  toolCache.set(jsonSchema, result);
  return result;
}

/** Name of the dedicated verification-verdict variant (see {@link createVerificationOutputTool}). */
export const VERIFICATION_OUTPUT_TOOL_NAME = 'verification_output';

/**
 * Instructional prompt wording for the verification-result tool. A verification
 * (stop/plan) hook must return a single pass/fail verdict with exactly one call,
 * so the tool is framed to force that — mirroring the reference's
 * `createStructuredOutputTool().prompt()` ("Use this tool to return your
 * verification result. You MUST call this tool exactly once at the end of your
 * response."). Kept as one exported constant so the hook path and the tool share
 * identical wording.
 */
export const VERIFICATION_OUTPUT_PROMPT =
  'Use this tool to return your verification result. You MUST call this tool exactly once at the end of your response.';

/**
 * Canonical `{ ok, reason }` verification schema (the single source of the verdict
 * contract shared by the verification tool and the AI-hook verdict parser): `ok`
 * is a required boolean, `reason` an optional string explaining a failure, and no
 * other properties are allowed. Mirrors the reference's `hookResponseSchema` /
 * `createStructuredOutputTool` `inputJSONSchema` ({ ok, reason, required:['ok'],
 * additionalProperties:false }).
 */
export const VERIFICATION_OUTPUT_SCHEMA: Record<string, unknown> = {
  type: 'object',
  properties: {
    ok: { type: 'boolean', description: 'Whether the condition was met' },
    reason: { type: 'string', description: 'Reason, if the condition was not met' },
  },
  required: ['ok'],
  additionalProperties: false,
};

/**
 * Build the reusable verification-result tool: a schema-bound {@link
 * syntheticOutputTool} variant whose input contract is {@link
 * VERIFICATION_OUTPUT_SCHEMA} and whose description is the verification-specific
 * {@link VERIFICATION_OUTPUT_PROMPT}. Reused by agent/prompt (AI-driven) hooks and
 * background verification so a verification agent advertises a clearly-worded tool
 * that forces a one-call structured `{ ok, reason }` verdict instead of free prose.
 *
 * It delegates to {@link createSyntheticOutputTool} (reusing its validate-then-
 * serialize `execute`, size cap and identity cache — no duplicated logic) and only
 * overrides the model-facing `name`/`description`/`searchHint`. Mirrors the reference
 * Code's `createStructuredOutputTool`, which clones `SyntheticOutputTool` and
 * overrides its `prompt()` with the verification wording plus the `{ ok, reason }`
 * schema.
 */
export function createVerificationOutputTool(): ToolDefinition {
  const created = createSyntheticOutputTool(VERIFICATION_OUTPUT_SCHEMA);
  // VERIFICATION_OUTPUT_SCHEMA is a hand-authored, well-formed constant, so the
  // shape validator never rejects it; the error arm is unreachable in practice but
  // kept honest (no non-null assertion masking a future malformed edit).
  if ('error' in created) {
    throw new Error(`verification output schema is invalid: ${created.error}`);
  }
  return {
    ...created.tool,
    name: VERIFICATION_OUTPUT_TOOL_NAME,
    description: VERIFICATION_OUTPUT_PROMPT,
    searchHint: 'return a structured pass/fail verification verdict',
  };
}

/**
 * Extract a `{ ok, reason }` verdict from a verification tool call's input (the
 * `tool_use_start` `input` of {@link createVerificationOutputTool}). Returns the
 * verdict when `ok` is a boolean, else `undefined` (caller falls back to text
 * parsing). Shared so the hook loop reads a structured tool verdict identically to
 * how free-text `{ok,reason}` output is parsed.
 */
export function parseVerificationToolInput(input: unknown): { ok: boolean; reason?: string } | undefined {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return undefined;
  const obj = input as Record<string, unknown>;
  if (typeof obj.ok !== 'boolean') return undefined;
  return { ok: obj.ok, reason: typeof obj.reason === 'string' ? obj.reason : undefined };
}

export const syntheticOutputTool: ToolDefinition = {
  name: 'structured_output',
  description:
    'Return structured JSON output. Use this to produce machine-parseable results. ' +
    'Pass the result in `data`, and optionally a JSON `schema` the data must satisfy — ' +
    'if provided, the data is validated against it and rejected with a descriptive error on mismatch. ' +
    'You MUST call this tool exactly once at the end of your response when structured output is requested.',
  // Discovery synonyms for `tool_search` (terms NOT already in the name/description)
  // so a deferred/forgotten structured-output capability is findable by intent.
  // Parity: the reference's `SyntheticOutputTool.searchHint`.
  searchHint: 'return the final response as structured JSON',
  // Cap the serialized result the SAME way the tool's own serializer does, so the
  // engine's generic spill-to-disk path (tool-executor) honors the identical
  // boundary instead of the larger engine-global default. Single source of the cap
  // is {@link MAX_RESULT_SIZE_CHARS}. Parity: the reference's per-tool `maxResultSizeChars`.
  maxResultSizeChars: MAX_RESULT_SIZE_CHARS,
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
  async execute(input: Record<string, unknown>, context): Promise<ToolResult> {
    const data = input.data;
    const schema = input.schema;

    // A per-call `schema` binds this invocation to a caller-supplied contract.
    // Route it through the schema-bound factory so a malformed schema is
    // rejected up front (instead of silently accepting any output) and the
    // compiled validator is reused across repeated same-schema calls.
    if (schema && typeof schema === 'object' && !Array.isArray(schema)) {
      const created = createSyntheticOutputTool(schema as Record<string, unknown>);
      if ('error' in created) {
        return { output: `Structured output schema is invalid: ${created.error}`, isError: true };
      }
      return created.tool.execute(data as Record<string, unknown>, context);
    }

    return serializeStructuredOutput(data);
  },
};
