// Split from workflow-runtime.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { parseEnvInt } from '../utils/env';
import { parseDataLiteral } from './workflow-runtime-parse-data-literal';
import type { WorkflowMeta } from './workflow-runtime-parse-data-literal';

export function parseWorkflowMeta(script: string): WorkflowMeta | null {
  const headerMatch = script.match(/export\s+const\s+meta\s*=\s*\{/);
  if (!headerMatch) return null;

  // Extract the meta object literal using brace counting
  let depth = 0;
  let inString = false;
  let stringChar = '';
  // A non-global .match() always populates .index when it matches.
  const braceStart = headerMatch.index! + headerMatch[0].length - 1;
  let braceEnd = braceStart;
  for (let i = braceStart; i < script.length; i++) {
    const ch = script[i];
    if (inString) {
      if (ch === stringChar && script[i - 1] !== '\\') inString = false;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') { inString = true; stringChar = ch; continue; }
    if (ch === '{') depth++;
    if (ch === '}') { depth--; if (depth === 0) { braceEnd = i; break; } }
  }
  if (depth !== 0) return null;

  const metaLiteral = script.slice(braceStart, braceEnd + 1);

  try {
    // Parse the metadata as DATA — never execute it (see parseDataLiteral). A
    // literal that is not a plain object, or that contains any non-data
    // construct, is rejected.
    const parsed = parseDataLiteral(metaLiteral);
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    const obj = parsed as Record<string, unknown>;
    return {
      name: String(obj.name ?? ''),
      description: String(obj.description ?? ''),
      phases: Array.isArray(obj.phases) ? (obj.phases as WorkflowMeta['phases']) : undefined,
      whenToUse: obj.whenToUse ? String(obj.whenToUse) : undefined,
    };
  } catch {
    return null;
  }
}

/**
 * [FUNCTIONAL PARITY] Validate a (possibly JSON-string) agent output against a
 * JSON-Schema-like descriptor — parity with the reference schema-forced
 * StructuredOutput: an `agent(prompt, { schema })` call must return output that
 * conforms to the schema. The runtime parses + validates it (and, in
 * {@link WorkflowRuntime}, retries the agent on mismatch).
 *
 * Returns the parsed/validated value on success, or `undefined` when the value
 * does not satisfy the schema (the signal to retry or fail). Validation is
 * intentionally shallow — type of the root plus `required` top-level keys —
 * matching the lightweight guarantees the workflow runtime makes without pulling
 * in a full JSON-Schema engine.
 */
export function validateStructuredOutput(value: unknown, schema: Record<string, unknown>): unknown | undefined {
  let parsed: unknown = value;
  if (typeof value === 'string') {
    // Best-effort parse: a string that isn't JSON stays the raw string, so a
    // `string`/absent-descriptor schema still accepts it while object/array/
    // number/boolean schemas reject it on the per-type check below.
    try { parsed = JSON.parse(value); } catch { parsed = value; }
  }
  const declaredType = typeof schema.type === 'string' ? (schema.type as string) : undefined;
  const expectsObject = declaredType === 'object' || (declaredType === undefined && (schema.properties !== undefined || schema.required !== undefined));
  if (expectsObject) {
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
    const obj = parsed as Record<string, unknown>;
    const required = Array.isArray(schema.required) ? (schema.required as unknown[]) : [];
    for (const key of required) {
      if (typeof key === 'string' && !(key in obj)) return undefined;
    }
    return parsed;
  }
  if (declaredType === 'array') {
    return Array.isArray(parsed) ? parsed : undefined;
  }
  if (declaredType === 'string') {
    // For a string schema the raw (unparsed) value is the structured output.
    return typeof value === 'string' ? value : undefined;
  }
  if (declaredType === 'number' || declaredType === 'integer') {
    return typeof parsed === 'number' && Number.isFinite(parsed) ? parsed : undefined;
  }
  if (declaredType === 'boolean') {
    return typeof parsed === 'boolean' ? parsed : undefined;
  }
  // Unknown/absent descriptor: accept the parsed value as-is.
  return parsed;
}

/** Max attempts for a schema-forced agent() call (1 try + retries). */
export function schemaMaxAttempts(): number {
  return Math.max(1, Math.min(10, parseEnvInt(process.env.AMI_WORKFLOW_SCHEMA_MAX_ATTEMPTS, 3)));
}
