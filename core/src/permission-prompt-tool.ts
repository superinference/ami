/**
 * Delegated permission-prompt tool result parsing (building block).
 *
 * Parity: Claude Code's `--permission-prompt-tool <name>` routes every non-interactive
 * ("ask") permission decision through a named tool, whose JSON result is validated against
 * `permissionToolOutputSchema` and normalized into an allow/deny decision
 * (references/claude/claude-code-main/src/utils/permissions/PermissionPromptToolResultSchema.ts).
 *
 * This is the self-contained, dependency-free validator half of that feature: it parses the
 * tool's raw JSON output into a {@link PermissionPromptToolDecision}, tolerating the same SDK
 * quirks the reference does (an empty `updatedInput: {}` means "use the original input"), and
 * returns a typed {ok,error} result so the orchestration layer (common/tool-executor) can
 * fail-closed (deny) on any malformed output instead of crashing. The management/invocation
 * layer lives in common/ (it needs the tool registry + engine); this layer is a pure parser.
 */
import type { PermissionUpdate } from './permissions';

/** A permission decision returned by a delegated permission-prompt tool. */
export type PermissionPromptToolDecision =
  | {
      behavior: 'allow';
      /** When present+non-empty, the (possibly rewritten) input the tool should run with. */
      updatedInput?: Record<string, unknown>;
      /** Durable permission rules to persist alongside the allow (reference parity). */
      updatedPermissions?: PermissionUpdate[];
    }
  | {
      behavior: 'deny';
      /** Operator/model-facing denial reason (required). */
      message: string;
      /** When true, the denial is a hard kill-switch that aborts the rest of the turn. */
      interrupt?: boolean;
    };

/** Result of {@link parsePermissionPromptToolResult}: a typed decision or a reason it failed. */
export type ParsePermissionPromptToolResult =
  | { ok: true; decision: PermissionPromptToolDecision }
  | { ok: false; error: string };

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

/**
 * The payload a delegated permission-prompt tool is invoked with (reference `inputSchema`:
 * tool_name / input / tool_use_id). Exposed so the invocation layer builds the exact shape.
 */
export interface PermissionPromptToolInput {
  tool_name: string;
  input: Record<string, unknown>;
  tool_use_id?: string;
}

/**
 * Build the invocation payload for a delegated permission-prompt tool from the pending
 * tool call. Mirrors the reference `permissionPromptTool.call({ tool_name, input, tool_use_id })`.
 */
export function buildPermissionPromptToolInput(
  toolName: string,
  input: Record<string, unknown>,
  toolUseId?: string,
): PermissionPromptToolInput {
  const payload: PermissionPromptToolInput = { tool_name: toolName, input };
  if (toolUseId !== undefined) payload.tool_use_id = toolUseId;
  return payload;
}

/**
 * Parse+validate a delegated permission-prompt tool's raw JSON output into a decision.
 *
 * Accepts the reference union:
 *   - `{ behavior: 'allow', updatedInput?: object, updatedPermissions?: array }`
 *     (an empty `updatedInput: {}` is treated as "use the original input" → omitted)
 *   - `{ behavior: 'deny', message: string, interrupt?: boolean }`
 *
 * Returns `{ ok: false, error }` for any other shape so the caller can fail-closed (deny)
 * rather than throw mid-turn.
 */
export function parsePermissionPromptToolResult(raw: unknown): ParsePermissionPromptToolResult {
  if (!isPlainObject(raw)) {
    return { ok: false, error: 'result is not a JSON object' };
  }
  const behavior = raw.behavior;
  if (behavior === 'allow') {
    let updatedInput: Record<string, unknown> | undefined;
    if (raw.updatedInput !== undefined) {
      if (!isPlainObject(raw.updatedInput)) {
        return { ok: false, error: 'allow.updatedInput must be an object' };
      }
      // An empty object means "use the original input" (reference parity) — omit it.
      if (Object.keys(raw.updatedInput).length > 0) {
        updatedInput = raw.updatedInput;
      }
    }
    let updatedPermissions: PermissionUpdate[] | undefined;
    if (raw.updatedPermissions !== undefined) {
      if (!Array.isArray(raw.updatedPermissions)) {
        return { ok: false, error: 'allow.updatedPermissions must be an array' };
      }
      updatedPermissions = raw.updatedPermissions as PermissionUpdate[];
    }
    const decision: PermissionPromptToolDecision = { behavior: 'allow' };
    if (updatedInput) decision.updatedInput = updatedInput;
    if (updatedPermissions) decision.updatedPermissions = updatedPermissions;
    return { ok: true, decision };
  }
  if (behavior === 'deny') {
    if (typeof raw.message !== 'string') {
      return { ok: false, error: 'deny.message must be a string' };
    }
    const decision: PermissionPromptToolDecision = { behavior: 'deny', message: raw.message };
    if (raw.interrupt === true) decision.interrupt = true;
    return { ok: true, decision };
  }
  return { ok: false, error: `behavior must be "allow" or "deny" (got ${JSON.stringify(behavior)})` };
}
