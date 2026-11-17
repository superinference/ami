/**
 * Shared infrastructure for classifier-based permission systems (building block).
 *
 * Parity: Claude Code's `utils/permissions/classifierShared.ts`
 * (references/claude/claude-code-main/src/utils/permissions/classifierShared.ts:L30-L39)
 * provides the common extract-tool-use + schema-validate-or-null primitives used by both
 * its LLM-driven permission classifiers:
 * - `bashClassifier.ts` (semantic Bash command matching)
 * - `yoloClassifier.ts` (YOLO-mode security classification)
 *
 * This is the self-contained, dependency-free primitive half of that feature: it extracts a
 * named tool call from a provider's structured ({@link ToolCallContent}) output and parses it
 * against a zod schema, returning the typed data or `null` on ANY failure (missing block,
 * malformed JSON arguments, or a schema-validation miss). The management/invocation layer — an
 * LLM semantic Bash/YOLO permission classifier — lives in common/ (it needs the provider +
 * engine); this layer is a pure parser, exactly like the sibling {@link parsePermissionPromptToolResult}.
 *
 * AMI adaptation vs the reference: Claude's helper works on `BetaContentBlock` tool-use blocks
 * whose `input` is already a decoded object. AMI's provider abstraction surfaces tool calls as
 * {@link ToolCallContent} (`{ id, type: 'function', function: { name, arguments } }`) where
 * `arguments` is a JSON *string*. So AMI's {@link parseClassifierResponse} additionally decodes
 * the JSON-string arguments (fail-closed to `null` on a decode error) before schema validation.
 */
import type { z } from 'zod/v4';
import type { ToolCallContent } from '../types';

/**
 * Extract a tool call from a provider's structured output by tool (function) name.
 *
 * Returns the first matching {@link ToolCallContent}, or `null` when no tool call with that
 * name is present. Parity: reference `extractToolUseBlock(content, toolName)`.
 */
export function extractToolCallBlock(
  toolCalls: readonly ToolCallContent[],
  toolName: string,
): ToolCallContent | null {
  const block = toolCalls.find((b) => b.type === 'function' && b.function.name === toolName);
  return block ?? null;
}

/**
 * Parse and validate a classifier's structured response against a zod schema.
 *
 * Decodes the tool call's JSON-string `arguments` (an empty string is treated as `{}`), then
 * runs `schema.safeParse` on the result. Returns the typed, schema-validated data, or `null`
 * if the arguments are not valid JSON or do not satisfy the schema — so a classifier consumer
 * can fail-closed on malformed model output instead of trusting an unchecked shape.
 *
 * Parity: reference `parseClassifierResponse<T>(toolUseBlock, schema)` →
 * `schema.safeParse(toolUseBlock.input)` → data | null.
 */
export function parseClassifierResponse<T extends z.ZodTypeAny>(
  toolCall: ToolCallContent,
  schema: T,
): z.infer<T> | null {
  let input: unknown;
  try {
    const raw = toolCall.function.arguments;
    input = raw === '' ? {} : JSON.parse(raw);
  } catch {
    return null;
  }
  const parseResult = schema.safeParse(input);
  if (!parseResult.success) {
    return null;
  }
  return parseResult.data;
}

/**
 * Convenience: extract the named tool call AND validate it in one step.
 *
 * Returns the typed, schema-validated classifier output, or `null` when the tool call is
 * absent or its arguments fail JSON/schema validation. This is the shape an LLM semantic
 * permission classifier (common/) actually consumes: one call from a provider's
 * `toolCalls` array to a trusted, typed decision. Composed from the two reference primitives
 * above so there is a single canonical parse path (no duplicated extract/validate logic).
 */
export function extractAndParseClassifierResponse<T extends z.ZodTypeAny>(
  toolCalls: readonly ToolCallContent[],
  toolName: string,
  schema: T,
): z.infer<T> | null {
  const block = extractToolCallBlock(toolCalls, toolName);
  if (block === null) {
    return null;
  }
  return parseClassifierResponse(block, schema);
}
