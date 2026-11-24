// Split from workflow-runtime.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { formatDuration } from '../conversation-summary';
import { type WorkUnit, type PriorRun } from '../task';
import type { AgentOptions } from './workflow-runtime';

/**
 * Opt-in cache of completed agent() results so a re-run/resume of a workflow
 * skips agents that already finished (B2). Keyed on a stable hash of the prompt
 * and the semantic agent options.
 */
export interface WorkflowAgentCache {
  get(key: string): unknown | undefined;
  set(key: string, value: unknown): void;
}

/** Stable cache key for an agent call — prompt + semantic opts (label/phase,
 *  which are display-only, are excluded so a relabel does not bust the cache).
 *  `maxTurns` is a semantic cap on the agent's work, so it participates in the
 *  key — two calls that differ only by their turn budget are not conflated. */
export function workflowAgentCacheKey(prompt: string, opts?: { schema?: unknown; model?: string; isolation?: string; agentType?: string; maxTurns?: number; permissionMode?: string; tools?: string[]; disallowedTools?: string[]; effort?: string | number }): string {
  const norm = {
    prompt,
    schema: opts?.schema ?? null,
    model: opts?.model ?? null,
    isolation: opts?.isolation ?? null,
    agentType: opts?.agentType ?? null,
    maxTurns: opts?.maxTurns ?? null,
    permissionMode: opts?.permissionMode ?? null,
    // The per-agent capability surface (tool allow/deny) and reasoning effort are
    // semantic inputs: two otherwise-identical calls scoped to different tools or
    // run at a different effort are NOT the same work, so they key separately.
    // Allow/deny lists are sorted so order-only differences do not bust the cache.
    tools: Array.isArray(opts?.tools) ? [...opts.tools].sort() : null,
    disallowedTools: Array.isArray(opts?.disallowedTools) ? [...opts.disallowedTools].sort() : null,
    effort: opts?.effort ?? null,
  };
  return crypto.createHash('sha256').update(JSON.stringify(norm)).digest('hex');
}

/** Disk-backed {@link WorkflowAgentCache}: one JSON file per key under `dir`. */
export class DiskWorkflowCache implements WorkflowAgentCache {
  constructor(private readonly dir: string) {}
  private file(key: string): string { return path.join(this.dir, `${key}.json`); }
  get(key: string): unknown | undefined {
    try { return JSON.parse(fs.readFileSync(this.file(key), 'utf8')).value; } catch { return undefined; }
  }
  set(key: string, value: unknown): void {
    try { fs.mkdirSync(this.dir, { recursive: true }); fs.writeFileSync(this.file(key), JSON.stringify({ value })); } catch { /* best-effort */ }
  }
}

export interface WorkflowMeta {
  name: string;
  description: string;
  phases?: Array<{ title: string; detail?: string; model?: string }>;
  whenToUse?: string;
}

export interface AgentResult {
  text: string;
  toolUseCount: number;
  tokenCount: number;
}

/**
 * [FUNCTIONAL PARITY] Per-agent work stats a handler reports back to the runtime
 * so the `agent-complete` event can carry a completion affordance (parity with
 * the reference AgentTool UI `Done (N tool uses · M tokens · Xs)`). The handler
 * (which alone sees the sub-engine's token/tool-use stream) calls this with the
 * latest counts; the runtime adds the measured wall-clock duration itself.
 */
export type WorkflowAgentReporter = (stats: { toolUseCount?: number; tokenCount?: number }) => void;

/**
 * [FUNCTIONAL PARITY] Format a spawned agent's completion affordance the way the
 * reference AgentTool UI does: `Done (N tool uses · M tokens · Xs)`. Singular
 * "1 tool use", the token count rendered with thousands separators, and the
 * duration via the shared {@link formatDuration}. Omits a stat whose value is
 * unknown (undefined), so a cached short-circuit (no handler run) still yields a
 * well-formed message.
 */
export function formatAgentCompletion(stats: { toolUseCount?: number; tokenCount?: number; durationMs?: number }): string {
  const parts: string[] = [];
  if (typeof stats.toolUseCount === 'number') {
    parts.push(stats.toolUseCount === 1 ? '1 tool use' : `${stats.toolUseCount} tool uses`);
  }
  if (typeof stats.tokenCount === 'number') {
    parts.push(`${stats.tokenCount.toLocaleString('en-US')} tokens`);
  }
  if (typeof stats.durationMs === 'number') {
    parts.push(formatDuration(stats.durationMs));
  }
  return parts.length > 0 ? `Done (${parts.join(' · ')})` : 'Done';
}

/**
 * [FUNCTIONAL PARITY] Resolve a tool allow/deny scope by NAME against the full
 * tool set (parity with the reference agent `tools` / `disallowedTools` fields):
 *  - `allow` is an allowlist: only tools whose name is listed survive. `'*'` in
 *    the allowlist means "all tools" (no allow-filter). Any allow name not in
 *    `all` is collected in `invalid` so the caller can reject the scope rather
 *    than silently dropping a typo'd tool.
 *  - `disallow` is applied AFTER the allowlist and removes those tools by name
 *    (deny wins over allow, defense-in-depth).
 * A single shared resolver so the per-call `agent(p, { tools })` scope and a
 * named agentType's declared scope go through ONE code path (no duplication).
 */
export function resolveToolScope<T extends { name: string }>(
  all: T[],
  allow?: string[],
  disallow?: string[],
): { tools: T[]; invalid: string[] } {
  let tools = all;
  const invalid: string[] = [];
  if (allow && allow.length > 0 && !allow.includes('*')) {
    const known = new Set(all.map(t => t.name));
    for (const name of allow) if (!known.has(name)) invalid.push(name);
    const allowSet = new Set(allow);
    tools = all.filter(t => allowSet.has(t.name));
  }
  if (disallow && disallow.length > 0) {
    const denySet = new Set(disallow);
    tools = tools.filter(t => !denySet.has(t.name));
  }
  return { tools, invalid };
}

/**
 * [FUNCTIONAL PARITY] Normalize a per-agent `model` value (parity with the
 * reference loader's model transform + `'inherit'` sentinel). A blank/whitespace
 * value or the explicit case-insensitive `'inherit'` sentinel means "fall back
 * to the parent/default model" and returns `undefined`; any other value is
 * returned trimmed. So a workflow can say `agent(p, { model: 'inherit' })` to
 * intentionally inherit, and a stray-whitespace model never reaches the handler.
 */
export function normalizeAgentModel(model?: string): string | undefined {
  if (typeof model !== 'string') return undefined;
  const trimmed = model.trim();
  if (trimmed === '' || trimmed.toLowerCase() === 'inherit') return undefined;
  return trimmed;
}

/**
 * The injected per-agent worker. Receives the stable {@link WorkflowRuntime}
 * agent id (the same number surfaced on `agent-start`/`agent-complete` as
 * `agentNumber`) as a third argument so the consumer can correlate the spawned
 * work (e.g. a process-manager task) back to the live agent and drive per-agent
 * {@link WorkflowRuntime.retryAgent}/{@link WorkflowRuntime.skipAgent} recovery.
 */
export type AgentHandler = (prompt: string, opts?: AgentOptions, agentId?: number, report?: WorkflowAgentReporter) => Promise<unknown>;

export interface WorkflowBudget {
  total: number | null;
  spent(): number;
  remaining(): number;
}

export type WorkflowResolver = (nameOrRef: string | { scriptPath: string }) => string | null;

/** A work unit for {@link WorkflowContext.wave}: a {@link WorkUnit} plus the
 *  caller's own fields (passed through untouched to the `run` callback). */
export type WaveUnit = WorkUnit & Record<string, unknown>;

/** Durable per-run manifest stored in the resume cache for {@link WorkflowContext.wave}. */
export interface WaveManifest extends PriorRun {
  /** Map of completed unit id → the result the unit produced. */
  results: Record<string, unknown>;
}

export interface WorkflowContext {
  agent: AgentHandler;
  parallel: <T>(thunks: Array<() => Promise<T>>) => Promise<Array<T | null>>;
  pipeline: <T, S1, S2 = S1, S3 = S2>(
    items: T[],
    ...stages: Array<(prev: any, item: T, index: number) => Promise<any>>
  ) => Promise<any[]>;
  /**
   * Fan out file-disjoint work units wave-by-wave and integrate serially.
   * Units declaring the files they touch (and optional deps) are partitioned so
   * each wave is internally file-disjoint and runs in parallel (bounded by the
   * same concurrency cap as `parallel()`); successive waves run after the prior
   * one integrates. When a resume cache is configured, the longest unchanged
   * prefix of a prior run (keyed by a stable run id) is replayed from cache and
   * only new/edited units are re-run. Resolves to results in the input order.
   */
  wave: <T>(
    units: WaveUnit[],
    run: (unit: WaveUnit) => Promise<T>,
  ) => Promise<Array<T | null>>;
  phase: (title: string) => void;
  log: (message: string) => void;
  args: unknown;
  budget: WorkflowBudget;
  saveCheckpoint: (key: string, data: unknown) => void;
  loadCheckpoint: (key: string) => unknown | undefined;
  workflow: (nameOrRef: string | { scriptPath: string }, args?: unknown) => Promise<unknown>;
}

export const MAX_AGENTS = 1000;

export const MAX_PIPELINE_ITEMS = 4096;

/**
 * Strip the `export const meta = { ... }` block from a workflow script using
 * brace-counting so nested objects (e.g. phases with detail objects) are handled
 * correctly, unlike the previous lazy regex approach.
 */
export function stripMeta(script: string): string {
  const match = script.match(/export\s+const\s+meta\s*=\s*\{/);
  if (!match) return script;
  // A non-global .match() always populates .index when it matches.
  const start = match.index!;
  let depth = 0;
  let inString = false;
  let stringChar = '';
  let end = start + match[0].length;
  for (let i = end - 1; i < script.length; i++) {
    const ch = script[i];
    if (inString) {
      if (ch === stringChar && script[i - 1] !== '\\') inString = false;
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') { inString = true; stringChar = ch; continue; }
    if (ch === '{') depth++;
    if (ch === '}') { depth--; if (depth === 0) { end = i + 1; break; } }
  }
  return script.slice(0, start) + script.slice(end);
}

/**
 * [FUNCTIONAL PARITY] Sliding-window concurrency scheduler (parity with the
 * reference `all(generators, concurrencyCap)` in utils/generators): start at most
 * `cap` workers, and launch the next item ONLY when a running one finishes, so at
 * no point are more than `cap` promises materialized — regardless of what the
 * worker does. This bounds ALL work (not just `agent()` calls, which the separate
 * per-agent semaphore caps), so a large `parallel()`/`pipeline()` batch no longer
 * eagerly spins up thousands of simultaneous promises.
 *
 * Results are written back by index, so the returned array preserves input order
 * even though items complete out of order. `onLaunch` runs (awaited) just before
 * each worker is invoked, in ascending launch order, so a caller can gate/space
 * launches (e.g. the prompt-cache stagger).
 */
export async function mapWithConcurrency<T, R>(
  items: T[],
  cap: number,
  worker: (item: T, index: number) => Promise<R>,
  onLaunch?: (index: number) => void | Promise<void>,
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIndex = 0;
  const windowSize = Math.max(1, Math.min(cap, items.length));

  const runner = async (): Promise<void> => {
    for (;;) {
      const index = nextIndex++;
      if (index >= items.length) return;
      if (onLaunch) await onLaunch(index);
      results[index] = await worker(items[index]!, index);
    }
  };

  const runners: Array<Promise<void>> = [];
  for (let i = 0; i < windowSize; i++) runners.push(runner());
  await Promise.all(runners);
  return results;
}

/**
 * Parse a restricted JavaScript *data* literal — object / array / string /
 * number / `true` / `false` / `null` — WITHOUT executing it. Throws on any
 * identifier, call, operator, template interpolation, or other non-data
 * construct, and on malformed input.
 *
 * Workflow metadata is untrusted content discovered on disk. The previous
 * `new Function('return (' + literal + ')')` approach EXECUTED that content at
 * mere discovery/listing time — before {@link validateWorkflowScript} ever ran —
 * so a crafted `meta` block could run arbitrary code just by being parsed. This
 * parser accepts only pure data (parity with the reference loader, which
 * validates parsed metadata as data and never evals it).
 */
export function parseDataLiteral(src: string): unknown {
  let i = 0;
  const n = src.length;
  const err = (msg: string): never => { throw new Error(`parseDataLiteral: ${msg} at ${i}`); };

  const skipWs = (): void => {
    while (i < n) {
      const c = src[i]!;
      if (c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v') { i++; continue; }
      break;
    }
  };

  const parseString = (quote: string): string => {
    let out = '';
    while (i < n) {
      const c = src[i++]!;
      if (c === '\\') {
        const e = src[i++];
        if (e === undefined) return err('unterminated escape');
        const map: Record<string, string> = { n: '\n', t: '\t', r: '\r', b: '\b', f: '\f', v: '\v', '0': '\0' };
        out += map[e] ?? e;
        continue;
      }
      if (quote === '`' && c === '$' && src[i] === '{') return err('template interpolation not allowed');
      if (c === quote) return out;
      out += c;
    }
    return err('unterminated string');
  };

  const parseNumber = (): number => {
    const start = i;
    if (src[i] === '-') i++;
    while (i < n && src[i]! >= '0' && src[i]! <= '9') i++;
    if (src[i] === '.') { i++; while (i < n && src[i]! >= '0' && src[i]! <= '9') i++; }
    if (src[i] === 'e' || src[i] === 'E') {
      i++;
      if (src[i] === '+' || src[i] === '-') i++;
      while (i < n && src[i]! >= '0' && src[i]! <= '9') i++;
    }
    const text = src.slice(start, i);
    const num = Number(text);
    if (text === '' || text === '-' || !Number.isFinite(num)) return err('invalid number');
    return num;
  };

  const parseValue = (): unknown => {
    skipWs();
    const c = src[i];
    if (c === undefined) return err('unexpected end of input');
    if (c === '{') return parseObject();
    if (c === '[') return parseArray();
    if (c === '"' || c === "'" || c === '`') { i++; return parseString(c); }
    if (c === '-' || c === '.' || (c >= '0' && c <= '9')) return parseNumber();
    if (src.startsWith('true', i)) { i += 4; return true; }
    if (src.startsWith('false', i)) { i += 5; return false; }
    if (src.startsWith('null', i)) { i += 4; return null; }
    return err(`unexpected token '${c}'`);
  };

  const parseKey = (): string => {
    skipWs();
    const c = src[i];
    if (c === '"' || c === "'" || c === '`') { i++; return parseString(c); }
    const start = i;
    while (i < n && /[A-Za-z0-9_$]/.test(src[i]!)) i++;
    if (i === start) return err('expected object key');
    return src.slice(start, i);
  };

  const parseObject = (): Record<string, unknown> => {
    i++; // consume '{'
    const obj: Record<string, unknown> = {};
    for (;;) {
      skipWs();
      if (src[i] === '}') { i++; return obj; } // empty object or trailing comma
      const key = parseKey();
      skipWs();
      if (src[i] !== ':') return err("expected ':'");
      i++;
      obj[key] = parseValue();
      skipWs();
      const d = src[i];
      if (d === ',') { i++; continue; }
      if (d === '}') { i++; return obj; }
      return err("expected ',' or '}'");
    }
  };

  const parseArray = (): unknown[] => {
    i++; // consume '['
    const arr: unknown[] = [];
    for (;;) {
      skipWs();
      if (src[i] === ']') { i++; return arr; } // empty array or trailing comma
      arr.push(parseValue());
      skipWs();
      const d = src[i];
      if (d === ',') { i++; continue; }
      if (d === ']') { i++; return arr; }
      return err("expected ',' or ']'");
    }
  };

  const value = parseValue();
  skipWs();
  if (i !== n) return err('trailing characters after value');
  return value;
}
