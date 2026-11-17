import * as vm from 'node:vm';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { isEnvTruthy } from '../privacy-level';
import { bashTool } from './bash';
import { fileReadTool } from './file-read';
import { fileEditTool } from './file-edit';
import { globTool } from './glob';
import { grepTool } from './grep';
import { notebookEditTool } from './notebook-edit';
import { taskTool } from './task';

/**
 * REPL (code-interpreter) tool — parity with Claude Code's `REPLTool`
 * (`src/tools/REPLTool/`). Lets the model orchestrate MANY primitive tool
 * invocations programmatically inside ONE sandboxed code block (loops,
 * conditionals, intermediate variables) instead of emitting a separate
 * `tool_use` per call. The primitive tools are injected as `async` functions
 * into a `node:vm` context; each call runs the SAME `ToolDefinition.execute`
 * (and therefore the same per-tool security/sandbox guards) as a direct call.
 *
 * AMI superset divergence (labelled): REPL mode is OPT-IN here (default OFF)
 * rather than Claude's ant-default-on, because the primitives it exposes run
 * inside the VM without re-prompting — so it is gated behind an explicit
 * operator env flag. Everything else mirrors the reference: the curated
 * primitive-tool set, the "hide the primitives from direct tool-calling when
 * REPL mode is on" filtering, the `getAllBaseTools`-bypass (the set is an
 * explicit ordered literal so search tools like grep/glob are never dropped),
 * and the lazy memoized getter that defers the TDZ-prone tool imports to call
 * time.
 */

/** Model-facing name of the REPL tool. */
export const REPL_TOOL_NAME = 'repl';
/** AMI-native env flag that enables REPL mode (opt-in; default OFF). */
export const REPL_MODE_ENV = 'AMI_REPL';
/** Claude-compatible spelling accepted alongside {@link REPL_MODE_ENV}. */
export const REPL_MODE_ENV_COMPAT = 'CLAUDE_REPL_MODE';
/** Default wall-clock budget for one REPL code block. */
export const DEFAULT_REPL_TIMEOUT_MS = 120_000;
/** Hard upper bound on a per-call REPL timeout. */
export const MAX_REPL_TIMEOUT_MS = 600_000;

/**
 * Whether REPL mode is enabled. Opt-in via {@link REPL_MODE_ENV} (or the
 * Claude-compatible {@link REPL_MODE_ENV_COMPAT}) using the standard AMI
 * falsey-token vocabulary. Default OFF — see the module header for why.
 */
export function isReplModeEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env[REPL_MODE_ENV]) || isEnvTruthy(env[REPL_MODE_ENV_COMPAT]);
}

let _primitiveTools: readonly ToolDefinition[] | undefined;

/**
 * Curated set of primitive tools hidden from direct model use when REPL mode is
 * on (see {@link getReplOnlyToolNames}) but still callable INSIDE the REPL VM
 * context. Exported so display/classification code can render virtual messages
 * for these tools even when they are absent from the advertised tool list.
 *
 * Lazy getter with `??=` memoization: the import chain index.ts → repl-tool.ts →
 * task.ts → index.ts is a cycle, so referencing the imported tool bindings at
 * module-eval time would hit "Cannot access before initialization" (TDZ).
 * Deferring to call time avoids it. Built from an explicit ordered literal
 * rather than the full registry so the search tools (grep/glob) are always
 * present — matching Claude's deliberate `getAllBaseTools()` bypass.
 */
export function getReplPrimitiveTools(): readonly ToolDefinition[] {
  return (_primitiveTools ??= [
    fileReadTool,
    fileEditTool,
    globTool,
    grepTool,
    bashTool,
    notebookEditTool,
    taskTool,
  ]);
}

let _replOnlyNames: ReadonlySet<string> | undefined;

/**
 * Names of the tools that are REPL-only when REPL mode is enabled. Derived from
 * {@link getReplPrimitiveTools} (single source of truth, so the two can never
 * drift) and memoized. Mirrors Claude's `REPL_ONLY_TOOLS` set.
 */
export function getReplOnlyToolNames(): ReadonlySet<string> {
  return (_replOnlyNames ??= new Set(getReplPrimitiveTools().map((t) => t.name)));
}

/**
 * Advertise-time filter: when REPL mode is enabled AND the REPL tool is present
 * in the pool, strip the REPL-only primitive tools so the model is forced to
 * orchestrate them through the REPL VM. When REPL mode is off, or the REPL tool
 * is not in the pool, the list is returned unchanged. Mirrors the
 * `isReplModeEnabled() && replEnabled` branch of Claude's `getTools`.
 */
export function filterReplOnlyTools<T extends { name: string }>(
  tools: readonly T[],
  env: NodeJS.ProcessEnv = process.env,
): T[] {
  if (!isReplModeEnabled(env)) return [...tools];
  const hasRepl = tools.some((t) => t.name === REPL_TOOL_NAME);
  if (!hasRepl) return [...tools];
  const only = getReplOnlyToolNames();
  return tools.filter((t) => !only.has(t.name));
}

/**
 * Extract a human message from a thrown value. An `Error` thrown INSIDE the
 * `node:vm` context belongs to a different realm, so `err instanceof Error` is
 * false in this realm — read its `.message` directly before falling back to
 * `String()` so a VM-thrown error still reports cleanly.
 */
function extractErrorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (err !== null && typeof err === 'object' && typeof (err as { message?: unknown }).message === 'string') {
    return (err as { message: string }).message;
  }
  return String(err);
}

/** Safely stringify a REPL log argument / return value for the text transcript. */
function stringifyReplValue(value: unknown): string {
  if (typeof value === 'string') return value;
  if (value === undefined) return 'undefined';
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

/**
 * Race a REPL code block's (async) completion against a wall-clock timeout and
 * the tool's abort signal. `vm`'s own `timeout` option only bounds SYNCHRONOUS
 * evaluation, so async tool orchestration needs an explicit race.
 */
function awaitReplResult(
  pending: Promise<unknown>,
  timeoutMs: number,
  signal: AbortSignal | undefined,
): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    let settled = false;
    const finish = (fn: (v: unknown) => void, value: unknown): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (signal) signal.removeEventListener('abort', onAbort);
      fn(value);
    };
    const onAbort = (): void => finish(reject as (v: unknown) => void, new Error('REPL execution aborted'));
    const timer = setTimeout(
      () => finish(reject as (v: unknown) => void, new Error(`REPL execution exceeded ${timeoutMs}ms`)),
      timeoutMs,
    );
    if (signal) {
      if (signal.aborted) {
        finish(reject as (v: unknown) => void, new Error('REPL execution aborted'));
        return;
      }
      signal.addEventListener('abort', onAbort);
    }
    pending.then(
      (v) => finish(resolve, v),
      (e) => finish(reject as (v: unknown) => void, e),
    );
  });
}

/**
 * Execute one REPL code block: contextify a `node:vm` sandbox exposing each
 * primitive tool as an `async` function (bound to the caller's {@link ToolContext}),
 * capture `console` output and the awaited return value. Separated from the tool
 * wrapper so it is unit-testable in isolation.
 */
export async function runReplCode(
  code: string,
  context: ToolContext,
  timeoutMs: number,
): Promise<ToolResult> {
  const logs: string[] = [];
  const record = (...args: unknown[]): void => {
    logs.push(args.map(stringifyReplValue).join(' '));
  };
  const toolApi: Record<string, (toolInput?: Record<string, unknown>) => Promise<ToolResult>> = {};
  for (const tool of getReplPrimitiveTools()) {
    toolApi[tool.name] = (toolInput: Record<string, unknown> = {}): Promise<ToolResult> =>
      tool.execute(toolInput ?? {}, context);
  }
  const sandbox: Record<string, unknown> = {
    console: { log: record, error: record, warn: record, info: record },
    tools: toolApi,
    ...toolApi,
  };

  // Wrap in an async IIFE so the model's code may `await` the injected tools and
  // optionally `return` a final value.
  const wrapped = `(async () => {\n${code}\n})()`;
  let value: unknown;
  try {
    const pending = Promise.resolve(vm.runInNewContext(wrapped, sandbox, { filename: 'repl.js' }));
    value = await awaitReplResult(pending, timeoutMs, context.abortSignal);
  } catch (err) {
    const message = extractErrorMessage(err);
    const captured = logs.length > 0 ? `${logs.join('\n')}\n` : '';
    return { output: `${captured}REPL error: ${message}`, isError: true };
  }

  const parts: string[] = [];
  if (logs.length > 0) parts.push(logs.join('\n'));
  if (value !== undefined) parts.push(`=> ${stringifyReplValue(value)}`);
  return { output: parts.length > 0 ? parts.join('\n') : '(no output)' };
}

export const replTool: ToolDefinition = {
  name: REPL_TOOL_NAME,
  description:
    'Run a block of JavaScript to orchestrate multiple tool calls programmatically in one step. ' +
    'The primitive tools (file_read, file_edit, glob, grep, bash, notebook_edit, task) are available ' +
    'as async functions you call with their normal input object, e.g. ' +
    '`const r = await grep({ pattern: "TODO", path: "src" }); console.log(r.output);`. ' +
    'Use loops/conditionals to batch work instead of emitting one tool call at a time. ' +
    'console.log output and the block\'s return value are reported back. ' +
    'Only enabled when REPL mode is on (AMI_REPL); while on, those primitive tools are hidden from ' +
    'direct tool-calling and must be driven from here.',
  inputSchema: {
    type: 'object',
    properties: {
      code: {
        type: 'string',
        description:
          'JavaScript to execute. Runs as an async function body, so you may use await and an optional return.',
      },
      timeout_ms: {
        type: 'number',
        description: `Optional wall-clock budget in ms (default ${DEFAULT_REPL_TIMEOUT_MS}, max ${MAX_REPL_TIMEOUT_MS}).`,
      },
    },
    required: ['code'],
  },
  isReadOnly: false,
  isConcurrencySafe: false,
  isEnabled: () => isReplModeEnabled(),

  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const code = input.code;
    if (typeof code !== 'string' || code.trim() === '') {
      return { output: 'Error: code must be a non-empty string.', isError: true };
    }
    let timeoutMs = DEFAULT_REPL_TIMEOUT_MS;
    if (typeof input.timeout_ms === 'number' && Number.isFinite(input.timeout_ms) && input.timeout_ms > 0) {
      timeoutMs = Math.min(Math.round(input.timeout_ms), MAX_REPL_TIMEOUT_MS);
    }
    return runReplCode(code, context, timeoutMs);
  },
};
