import { ToolDefinition, ToolResult } from '../types';

/**
 * Environment-gated SYNTHETIC permission-flow test tool.
 *
 * Parity: the reference's `TestingPermissionTool`
 * (src/tools/testing/TestingPermissionTool.tsx) — a side-effect-free, test-only
 * tool whose SOLE purpose is to deterministically force the permission dialog so
 * an end-to-end test can exercise the ask → allow/deny path without mutating
 * files or running bash. the reference's tool combines two behaviors:
 *   - `isEnabled()` returns true only in a `test` build (invisible in production);
 *   - `checkPermissions()` UNCONDITIONALLY returns `{behavior:'ask', message:'Run test?'}`,
 *     with `isReadOnly()`/`isConcurrencySafe()` true and a no-op `call()`.
 *
 * AMI's permission system is centralized (there is no per-tool `checkPermissions`),
 * so parity is achieved functionally rather than field-for-field:
 *   - env gating is expressed through {@link ToolDefinition.isEnabled} +
 *     {@link isTestingPermissionEnabled} (opt-in, OFF by default) so the tool is
 *     dropped from the advertised pool in production, exactly like the reference's build gate;
 *   - the unconditional "always ask" is enforced in the centralized
 *     {@link import('../permissions').PermissionManager.check} via the
 *     {@link TESTING_PERMISSION_TOOL_NAME} guard (every mode asks; deny-all and an
 *     explicit user deny rule still win).
 *
 * INTENTIONAL DIVERGENCE (labeled): the reference declares this tool `isReadOnly()===true`,
 * but in AMI a read-only + concurrency-safe tool takes the executor's no-prompt
 * read-only fast path and would never reach the permission prompt. To keep the
 * SAME functional behavior (the call is routed through the permission-asking path),
 * AMI declares it {@link ToolDefinition.isReadOnly} = `false`. The tool is still
 * genuinely side-effect-free (its `execute` is a no-op that returns a fixed string).
 */

/** Model/UI-facing name of the synthetic permission-flow test tool (snake_case per AMI convention). */
export const TESTING_PERMISSION_TOOL_NAME = 'testing_permission';

/**
 * Opt-in env var that makes the synthetic permission test tool visible. OFF by
 * default so the tool is invisible in production and never pollutes the normal
 * advertised pool — the functional equivalent of the reference's build-time
 * `isEnabled()==='test'` gate. Accepts `1`/`true` (case-insensitive).
 */
export const TESTING_PERMISSION_ENV = 'AMI_TESTING_PERMISSION';

/** Whether the synthetic always-ask permission test tool is enabled (test-only). */
export function isTestingPermissionEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env[TESTING_PERMISSION_ENV];
  if (raw === undefined) return false;
  const v = raw.trim().toLowerCase();
  return v === '1' || v === 'true';
}

export const testingPermissionTool: ToolDefinition = {
  name: TESTING_PERMISSION_TOOL_NAME,
  description:
    'Test-only tool that always asks for permission before executing. Used for deterministic ' +
    'end-to-end testing of the permission ask → allow/deny flow. Has no side effects.',
  // Discovery synonyms for `tool_search` (terms NOT already in name/description).
  searchHint: 'deterministic always-ask permission flow test fixture',
  inputSchema: {
    type: 'object',
    properties: {},
    additionalProperties: false,
  },
  // See the INTENTIONAL DIVERGENCE note above: declared non-read-only so the call
  // is routed through AMI's permission-asking path (a read-only tool would take the
  // no-prompt fast path). The centralized permission guard then forces the ask.
  isReadOnly: false,
  // Runtime availability gate — dropped from the advertised pool unless the
  // test-only opt-in env var is set (parity: the reference's `isEnabled()==='test'`).
  isEnabled: () => isTestingPermissionEnabled(),
  async execute(): Promise<ToolResult> {
    // No-op, side-effect-free body (parity: the reference's `call()` returning a fixed
    // success string). The point of the tool is the permission prompt, not the work.
    return { output: `${TESTING_PERMISSION_TOOL_NAME} executed successfully` };
  },
};
