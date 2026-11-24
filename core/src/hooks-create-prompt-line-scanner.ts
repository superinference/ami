// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { HOOK_MATCHER_FIELD } from './hooks-async-rewake-delivery-message';
import type { PromptRequestOption } from './hooks-with-frontmatter-hook-plugin-root';

/** True when a hook's `matcher` applies to the given non-tool event context
 *  (matches the event's fieldToMatch value), or when there is no matcher / no
 *  matchable field. Shared by every non-tool event registration. */
export function matcherAppliesToCtx(event: string, matcher: string | undefined, ctx: Record<string, unknown>): boolean {
  if (!matcher) return true;
  const field = HOOK_MATCHER_FIELD[event];
  if (field === undefined) return true;
  return matchesPattern(matcher, String(ctx[field] ?? ''));
}

/** A hook's request for the user to choose one of `options` (the reference's
 *  `PromptRequest`). `prompt` is the correlation id echoed back in the
 *  `PromptResponse.prompt_response`. */
export interface PromptRequest {
  prompt: string;
  message: string;
  options: PromptRequestOption[];
}

/** The engine's answer to a {@link PromptRequest} (the reference's
 *  `PromptResponse`): the chosen option `key` plus the originating request id. */
export interface PromptResponse {
  prompt_response: string;
  selected: string;
}

/** Interactive prompt resolver: given a hook's {@link PromptRequest}, resolve to
 *  the user's chosen option key. Rejecting (or throwing) signals the user
 *  cancelled — the engine then closes the hook's stdin so it does not hang
 *  (the reference's `requestPrompt` contract + `app:interrupt` abort). */
export type PromptRequestHandler = (request: PromptRequest) => Promise<string>;

/**
 * Validate an arbitrary parsed value as a {@link PromptRequest} (the reference's
 * `promptRequestSchema.safeParse`). Returns the typed request on success, or
 * `undefined` when the shape does not match — so an async-output line
 * (`{async:true}`), a plain log line, or a decision JSON is never mistaken for a
 * prompt request. `description` is optional per option.
 */
export function validatePromptRequest(value: unknown): PromptRequest | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const v = value as Record<string, unknown>;
  if (typeof v.prompt !== 'string' || v.prompt.length === 0) return undefined;
  if (typeof v.message !== 'string') return undefined;
  if (!Array.isArray(v.options)) return undefined;
  const options: PromptRequestOption[] = [];
  for (const raw of v.options) {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
    const o = raw as Record<string, unknown>;
    if (typeof o.key !== 'string' || typeof o.label !== 'string') return undefined;
    if (o.description !== undefined && typeof o.description !== 'string') return undefined;
    options.push({ key: o.key, label: o.label, ...(o.description !== undefined ? { description: o.description } : {}) });
  }
  return { prompt: v.prompt, message: v.message, options };
}

/**
 * Parse a single stdout line as a {@link PromptRequest} (the reference's
 * per-line scan). Blank lines, non-JSON lines, and JSON that is not a valid
 * prompt request all return `undefined` — only a well-formed request line is
 * recognized, leaving ordinary hook output untouched.
 */
export function parsePromptRequestLine(line: string): PromptRequest | undefined {
  const trimmed = line.trim();
  if (trimmed.length === 0) return undefined;
  let parsed: unknown;
  try { parsed = JSON.parse(trimmed); } catch { return undefined; }
  return validatePromptRequest(parsed);
}

/** Whether `selected` is one of the keys the request actually offered (the reference
 *  Code answers with a key drawn from `options`). Guards against a resolver
 *  returning a key the hook never presented. */
export function isValidPromptSelection(request: PromptRequest, selected: string): boolean {
  return request.options.some(o => o.key === selected);
}

/** Build the {@link PromptResponse} for a resolved request (the reference's
 *  `{prompt_response: request.prompt, selected}`). */
export function buildPromptResponse(request: PromptRequest, selected: string): PromptResponse {
  return { prompt_response: request.prompt, selected };
}

/** Serialize a {@link PromptResponse} for writing to the hook's stdin — JSON
 *  plus the trailing newline the line-oriented hook reads (the reference writes
 *  `jsonStringify(response) + '\n'`). */
export function serializePromptResponse(response: PromptResponse): string {
  return JSON.stringify(response) + '\n';
}

/** A streaming scanner over a hook's stdout (the reference's line-buffer +
 *  `processedPromptLines` set). Returned by {@link createPromptLineScanner}. */
export interface PromptLineScanner {
  /** Feed a raw stdout chunk; complete lines are scanned for prompt requests. */
  feed(chunk: string): void;
  /** The exact trimmed request lines already handled — stripped from the final
   *  captured stdout by content match (no index tracking → no index drift). */
  readonly processedLines: ReadonlySet<string>;
  /** Remove every handled prompt-request line from `fullStdout`, preserving all
   *  ordinary output lines (and their order). */
  stripProcessed(fullStdout: string): string;
}

/**
 * Create a streaming prompt-request scanner (the reference's stdout line-scan).
 * Each complete line (split on `\n`; the trailing partial line is buffered until
 * completed) is tested with {@link parsePromptRequestLine}; a match is recorded
 * in `processedLines` and handed to `onRequest` synchronously, in the order the
 * hook asked. The caller serializes any async side effects (resolving +
 * answering) so responses are written back in request order (the reference's
 * `promptChain`).
 */
export function createPromptLineScanner(onRequest: (req: PromptRequest, rawLine: string) => void): PromptLineScanner {
  const processed = new Set<string>();
  let buffer = '';
  return {
    feed(chunk: string): void {
      buffer += chunk;
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? ''; // last element is an incomplete line
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed.length === 0) continue;
        const req = parsePromptRequestLine(trimmed);
        if (!req) continue;
        processed.add(trimmed);
        onRequest(req, trimmed);
      }
    },
    processedLines: processed,
    stripProcessed(fullStdout: string): string {
      if (processed.size === 0) return fullStdout;
      return fullStdout
        .split('\n')
        .filter(l => !processed.has(l.trim()))
        .join('\n');
    },
  };
}

/**
 * Mutable out-param threaded through the hook runners to carry a hook's execution
 * result back to {@link HookManager.fireAsyncHook}. `success` drives the
 * `once:true` self-removal gate (see `runHookByType`); the optional `exitCode` /
 * `stdout` / `stderr` are captured by the command path so an async hook's
 * structured completion record (`hook_response` EngineEvent) reports the exit code
 * and SEPARATELY-captured streams rather than one merged blob. Parity: the reference
 * Code's hook outcome passed to `finalizeHook`/`emitHookResponse`.
 */
export interface HookOutcome {
  success: boolean;
  exitCode?: number;
  stdout?: string;
  stderr?: string;
}

// ---------------------------------------------------------------------------
// Pattern matching utility
// ---------------------------------------------------------------------------

export function matchesPattern(pattern: string, value: string): boolean {
  if (!pattern || pattern === '*') return true;
  // the reference semantics (matchesPattern): a matcher made only of simple token
  // chars (alphanumeric/underscore, optionally pipe-separated) is an EXACT match
  // — NOT a substring/regex — so `Edit` scopes to the `Edit` tool alone and never
  // leaks onto `EditNotebook`. Only a matcher containing regex metacharacters is
  // treated as a regex (invalid-regex fallback handled below).
  if (/^[A-Za-z0-9_|]+$/.test(pattern)) {
    if (pattern.includes('|')) return pattern.split('|').some(p => p === value);
    return pattern === value;
  }
  // A matcher with regex metacharacters is a regex; an invalid one falls back to
  // literal equality (AMI's long-standing behavior — safer than silently matching
  // nothing when a user wrote a plain string that happens to contain a metachar).
  try { return new RegExp(pattern).test(value); } catch { return pattern === value; }
}

/** Pull the content string a `Tool(content)` matcher checks against the tool
 *  input (the command for bash, the path for file tools, the url for web). */
export function hookInputContent(toolInput: Record<string, unknown>): string {
  const candidate =
    toolInput.command ?? toolInput.file_path ?? toolInput.path ?? toolInput.url ??
    toolInput.pattern ?? toolInput.query;
  if (typeof candidate === 'string') return candidate;
  return Object.values(toolInput).filter(v => typeof v === 'string').join(' ');
}

export const TOOL_IF_RE = /^([A-Za-z_][A-Za-z0-9_]*)\(([\s\S]*)\)$/;
