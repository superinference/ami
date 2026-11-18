/**
 * Environment-variable utilities: numeric/boolean coercion, `.env` file parsing
 * and loading, and env-var waterfall resolution.
 *
 * These are intentionally dependency-free and side-effect-free (except the thin
 * {@link loadDotenv} filesystem wrapper, and an OPTIONAL injected `log` callback
 * on {@link validateBoundedEnvInt}/{@link collectEnvLimitDiagnostics} — no logger
 * is imported here), so they are safe to import from any layer. Credential
 * redaction lives in `credential-guard` and tool-argument boolean coercion lives
 * in `utils/coerce`; this module does NOT duplicate them.
 */

import { existsSync, readFileSync } from 'node:fs';
import * as os from 'node:os';

/**
 * The binfmt_misc interop handler WSL registers so Windows `.exe`s can run from
 * inside the distro — the single most reliable on-disk WSL marker (present on
 * every WSL1/WSL2 distro, absent on native Linux), independent of whether the
 * interop env vars happen to be exported in the current shell. Parity with Claude
 * Code's `isWslEnvironment` (`utils/env.ts`), which keys solely off this file.
 */
const WSL_INTEROP_MARKER = '/proc/sys/fs/binfmt_misc/WSLInterop';

/**
 * True when running under Windows Subsystem for Linux — detected via the
 * WSL-specific env vars (`WSL_DISTRO_NAME`/`WSL_INTEROP`), the presence of the
 * WSL interop binfmt marker ({@link WSL_INTEROP_MARKER}, the reliable file-system
 * signal Claude Code's `isWslEnvironment` uses — catches a WSL shell that did not
 * export the interop env vars), or the `microsoft`/`wsl` marker in the kernel
 * release string. The single canonical WSL detector shared by every consumer
 * (ripgrep timeout tuning, cross-OS lock-reclaim guards, the Windows-npm PATH
 * diagnostic) so the check can never drift between call sites (§0b.6 — no
 * duplicated platform probe). The on-disk probe is injectable so the file-signal
 * branch is unit-testable without a real WSL host.
 */
export function isWSL(
  env: NodeJS.ProcessEnv = process.env,
  fileExists: (p: string) => boolean = existsSync,
): boolean {
  if (env.WSL_DISTRO_NAME || env.WSL_INTEROP) return true;
  try { if (fileExists(WSL_INTEROP_MARKER)) return true; } catch { /* probe failed → fall through */ }
  try { return /microsoft|wsl/i.test(os.release()); } catch { return false; }
}

/**
 * Coerce an env-var string to a finite truncated integer, or `null` when it is
 * undefined/empty/non-finite. The single parse used by {@link parseEnvInt} and
 * {@link validateBoundedEnvInt} so the accepted numeric grammar (plain integers,
 * scientific notation, `_` digit separators) can never drift between the two.
 */
function coerceEnvInt(envVar: string | undefined): number | null {
  if (!envVar) return null;
  const cleaned = envVar.replace(/_/g, '');
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

/** Inclusive bounds for {@link parseEnvInt}. An out-of-range value falls back. */
export interface EnvIntBounds {
  /** Inclusive lower bound; a parsed value below it returns the fallback. */
  min?: number;
  /** Inclusive upper bound; a parsed value above it returns the fallback. */
  max?: number;
}

/**
 * Parse an environment variable as an integer, accepting:
 * - Plain integers: "42", "1000"
 * - Scientific notation: "1e6", "5e3"
 * - Digit separators: "64_000", "1_000_000"
 *
 * Returns the parsed integer, or the fallback for undefined/empty/non-finite
 * input. With optional `bounds`, a value outside `[min, max]` (inclusive) is
 * REJECTED back to the fallback — so a misconfigured negative/zero/absurd
 * override (e.g. a `0` concurrency that would deadlock, or a huge timeout from a
 * typo) can never take effect. See {@link validateBoundedEnvInt} for the richer
 * clamp-and-report variant that distinguishes "invalid" from "capped".
 */
export function parseEnvInt(envVar: string | undefined, fallback: number, bounds?: EnvIntBounds): number {
  const n = coerceEnvInt(envVar);
  if (n === null) return fallback;
  if (bounds?.min !== undefined && n < bounds.min) return fallback;
  if (bounds?.max !== undefined && n > bounds.max) return fallback;
  return n;
}

/** Outcome classification for {@link validateBoundedEnvInt}. */
export type EnvIntStatus = 'valid' | 'capped' | 'invalid';

/**
 * Structured result of validating a bounded integer env var: the value actually
 * used (`effective`), its `status`, and a human-readable `message` whenever the
 * raw value was rejected or clamped (so a caller can surface the misconfiguration
 * instead of silently discarding it).
 */
export interface BoundedEnvIntResult {
  effective: number;
  status: EnvIntStatus;
  message?: string;
}

/**
 * Validate a numeric env override against a lower bound and an upper cap, telling
 * the caller WHAT happened so a mistyped or over-limit value is surfaced rather
 * than silently swallowed:
 *
 * - unset/empty            → `{ effective: defaultValue, status: 'valid' }`
 * - non-numeric or `< min` → `status: 'invalid'`, `effective` is the default
 * - `> upperLimit`         → `status: 'capped'`,  `effective` is `upperLimit`
 * - otherwise              → `status: 'valid'`,   `effective` is the parsed value
 *
 * `min` defaults to `1` (the common "must be positive" case); pass `min: 0` where
 * zero is legitimately meaningful (e.g. a depth/concurrency "unlimited" sentinel).
 * Unlike {@link parseEnvInt} with bounds (which rejects an over-cap value back to
 * the default), this CLAMPS an over-cap value to `upperLimit` and reports it.
 *
 * Pass `log` to emit the diagnostic `message` whenever the value is rejected
 * (`invalid`) or clamped (`capped`) — parity with Claude Code's
 * `validateBoundedIntEnvVar`, which calls `logForDebugging` on those paths so a
 * misconfigured override surfaces to the debug log, not only to the return value.
 * The callback is injected (rather than importing a logger here) so this module
 * stays dependency-free; the real consumer wires the core debug logger.
 */
export function validateBoundedEnvInt(
  name: string,
  value: string | undefined,
  defaultValue: number,
  upperLimit: number,
  options: { min?: number; log?: (line: string) => void } = {},
): BoundedEnvIntResult {
  if (value === undefined || value === '') {
    return { effective: defaultValue, status: 'valid' };
  }
  const min = options.min ?? 1;
  const n = coerceEnvInt(value);
  if (n === null || n < min) {
    const result: BoundedEnvIntResult = {
      effective: defaultValue,
      status: 'invalid',
      message: `${name}: invalid value "${value}" (using default ${defaultValue})`,
    };
    options.log?.(result.message!);
    return result;
  }
  if (n > upperLimit) {
    const result: BoundedEnvIntResult = {
      effective: upperLimit,
      status: 'capped',
      message: `${name}: capped from ${n} to ${upperLimit}`,
    };
    options.log?.(result.message!);
    return result;
  }
  return { effective: n, status: 'valid' };
}

/** A numeric env var paired with its default + recommended bounds, for diagnostics. */
export interface EnvLimitSpec {
  name: string;
  defaultValue: number;
  upperLimit: number;
  /** Inclusive lower bound; defaults to 1 (must be positive). */
  min?: number;
}

/** A non-valid env-limit finding produced by {@link collectEnvLimitDiagnostics}. */
export interface EnvLimitDiagnostic extends BoundedEnvIntResult {
  name: string;
  message: string;
}

/**
 * Canonical catalog of AMI's numeric env overrides and their recommended bounds.
 * Consumed by the `/doctor` "env-limits" check so a misconfigured concurrency,
 * timeout, or subagent limit is reported to the user. Advisory: it flags values
 * outside the recommended range even where the consuming call site does not hard
 * enforce the bound.
 */
export const AMI_ENV_INT_LIMITS: readonly EnvLimitSpec[] = [
  { name: 'SUPERINFERENCE_MAX_TOOL_CONCURRENCY', defaultValue: 10, upperLimit: 1000, min: 1 },
  { name: 'SUPERINFERENCE_PARALLEL_TOOL_TIMEOUT', defaultValue: 120_000, upperLimit: 3_600_000, min: 1 },
  { name: 'SUPERINFERENCE_SEQUENTIAL_TOOL_TIMEOUT', defaultValue: 600_000, upperLimit: 3_600_000, min: 1 },
  { name: 'CLAUDE_CODE_MAX_CONCURRENT_SUBAGENTS', defaultValue: 20, upperLimit: 1000, min: 1 },
  { name: 'CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION', defaultValue: 200, upperLimit: 100_000, min: 1 },
  { name: 'CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH', defaultValue: 0, upperLimit: 100, min: 0 },
  // Output-length caps (parity: Claude Code's Doctor env-validation surface,
  // Doctor.tsx L142-161 — BASH_MAX_OUTPUT_LENGTH / TASK_MAX_OUTPUT_LENGTH). These
  // are the knobs the bash/task tools already clamp at their enforcing call sites
  // (resolveBashMaxOutput → BASH_MAX_OUTPUT_UPPER_LIMIT; resolveTaskMaxChars →
  // TASK_MAX_OUTPUT_UPPER_LIMIT, default DEFAULT_TASK_MAX_CHARS); surfacing them
  // here so a misconfigured output cap is REPORTED by /doctor, not only silently
  // clamped. Bounds kept in lock-step with those constants by a drift test.
  { name: 'BASH_MAX_OUTPUT_LENGTH', defaultValue: 200_000, upperLimit: 200_000, min: 1 },
  { name: 'TASK_MAX_OUTPUT_LENGTH', defaultValue: 32_000, upperLimit: 160_000, min: 1 },
];

/**
 * Run {@link validateBoundedEnvInt} over a list of specs and return only the
 * entries that are `invalid` or `capped` (each with a message). An empty result
 * means every numeric override is within range. Defaults to the live process env
 * and the {@link AMI_ENV_INT_LIMITS} catalog.
 *
 * Pass `options.log` to also emit each non-valid finding's message to the debug
 * logger (parity with Claude Code's per-validation `logForDebugging` call); the
 * callback is injected so this module stays dependency-free.
 */
export function collectEnvLimitDiagnostics(
  specs: readonly EnvLimitSpec[] = AMI_ENV_INT_LIMITS,
  env: Record<string, string | undefined> = process.env,
  options: { log?: (line: string) => void } = {},
): EnvLimitDiagnostic[] {
  const out: EnvLimitDiagnostic[] = [];
  for (const spec of specs) {
    const r = validateBoundedEnvInt(spec.name, env[spec.name], spec.defaultValue, spec.upperLimit, {
      min: spec.min,
      log: options.log,
    });
    if (r.status !== 'valid' && r.message) {
      out.push({ name: spec.name, effective: r.effective, status: r.status, message: r.message });
    }
  }
  return out;
}

/**
 * Parse an environment variable as a floating-point number, accepting the same
 * digit-separator and scientific-notation forms as {@link parseEnvInt} but
 * WITHOUT truncation. Returns the fallback for undefined/empty/non-finite input.
 */
export function parseEnvFloat(envVar: string | undefined, fallback: number): number {
  if (!envVar) return fallback;
  const cleaned = envVar.replace(/_/g, '').trim();
  if (cleaned === '') return fallback;
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return fallback;
  return n;
}

const ENV_TRUE = new Set(['1', 'true', 'yes', 'on', 'y', 't', 'enable', 'enabled']);
const ENV_FALSE = new Set(['0', 'false', 'no', 'off', 'n', 'f', 'disable', 'disabled', '']);

/**
 * Parse an environment variable as a boolean using the conventional set of
 * truthy/falsy spellings (case- and whitespace-insensitive):
 *
 * - true:  `1`, `true`, `yes`, `on`, `y`, `t`, `enable`, `enabled`
 * - false: `0`, `false`, `no`, `off`, `n`, `f`, `disable`, `disabled`, `""`
 *
 * Unrecognised values (and `undefined`) return the fallback. This is distinct
 * from `utils/coerce`'s `coerceBool`, which is for JSON tool-call arguments and
 * only accepts the literal string `"true"`.
 */
export function parseEnvBool(envVar: string | undefined, fallback = false): boolean {
  if (envVar === undefined) return fallback;
  const v = envVar.trim().toLowerCase();
  if (ENV_TRUE.has(v)) return true;
  if (ENV_FALSE.has(v)) return false;
  return fallback;
}

/**
 * The Claude-parity "explicit opt-out" falsy vocabulary for {@link isEnvDefinedFalsy}
 * — a strict subset of {@link parseEnvBool}'s falsy tokens. An empty string is
 * deliberately NOT in this set: a variable set to `""` reads as *unset*, not as an
 * explicit disable, so a default-ON knob stays on. Mirrors `envUtils.isEnvDefinedFalsy`.
 */
const ENV_DEFINED_FALSY = new Set(['0', 'false', 'no', 'off']);

/**
 * Three-state env-flag check: `true` ONLY when a variable is *explicitly set to a
 * falsy word* (`0`/`false`/`no`/`off`, case- and whitespace-insensitive), and
 * `false` for everything else — including unset (`undefined`), an empty string,
 * and arbitrary non-falsy text. This is the distinction a plain truthy check
 * cannot express: it lets a caller tell "the user turned this OFF" apart from "the
 * user did not set it", so a default-ON capability only flips off on an explicit
 * opt-out. A `boolean` is accepted for parity with the reference signature
 * (already-coerced config values): `false` → defined-falsy.
 *
 * Canonical home for the helper every layer shares (the MCP IDE auto-connect
 * override, the CLI update-check / syntax-highlight / personality opt-outs) — a
 * single definition instead of a per-site re-derivation (§0b.6). Mirrors Claude
 * Code's `envUtils.isEnvDefinedFalsy`, the hard-opt-out companion to a truthy check.
 */
export function isEnvDefinedFalsy(value: string | boolean | undefined): boolean {
  if (value === undefined) return false;
  if (typeof value === 'boolean') return !value;
  return ENV_DEFINED_FALSY.has(value.trim().toLowerCase());
}

/**
 * Resolve the first *set and non-empty* value among a list of candidate env var
 * names — the classic env waterfall (e.g. `AMI_TOKEN` → `API_TOKEN` → `TOKEN`).
 *
 * By default a variable that is set but empty (`""`) is skipped and resolution
 * continues to the next candidate; pass `{ allowEmpty: true }` to accept the
 * first *set* value even when empty. Returns `undefined` when none match.
 */
export function resolveEnvVar(
  names: readonly string[],
  env: Record<string, string | undefined> = process.env,
  options: { allowEmpty?: boolean } = {},
): string | undefined {
  for (const name of names) {
    const v = env[name];
    if (v === undefined) continue;
    if (!options.allowEmpty && v === '') continue;
    return v;
  }
  return undefined;
}

export interface DotenvParseOptions {
  /** Expand `${VAR}` / `$VAR` / `${VAR:-default}` references. Default `true`. */
  expand?: boolean;
  /**
   * Extra variables consulted first during expansion, before `process.env` and
   * the values parsed so far from the same source.
   */
  env?: Record<string, string | undefined>;
}

/** Resolve a `${...}` expression (optionally `name:-default`) during expansion. */
function lookupVar(
  expr: string,
  parsed: Record<string, string>,
  baseEnv: Record<string, string | undefined>,
): string {
  const di = expr.indexOf(':-');
  const name = di === -1 ? expr : expr.slice(0, di);
  const def = di === -1 ? undefined : expr.slice(di + 2);
  // Precedence: explicit base env → process.env → values parsed so far.
  for (const source of [baseEnv, process.env, parsed]) {
    const v = source[name];
    if (v !== undefined) return v; // set-but-empty counts as "set"
  }
  return def ?? '';
}

/** Interpolate `${VAR}`, `${VAR:-default}`, and bare `$VAR` references. */
function interpolate(
  value: string,
  parsed: Record<string, string>,
  baseEnv: Record<string, string | undefined>,
): string {
  let out = '';
  for (let i = 0; i < value.length; i++) {
    const c = value[i];
    // `\$` is a literal dollar sign, never expanded.
    if (c === '\\' && value[i + 1] === '$') {
      out += '$';
      i++;
      continue;
    }
    if (c === '$') {
      if (value[i + 1] === '{') {
        const end = value.indexOf('}', i + 2);
        if (end !== -1) {
          out += lookupVar(value.slice(i + 2, end), parsed, baseEnv);
          i = end;
          continue;
        }
      }
      const m = /^[A-Za-z_][A-Za-z0-9_]*/.exec(value.slice(i + 1));
      if (m) {
        out += lookupVar(m[0], parsed, baseEnv);
        i += m[0].length;
        continue;
      }
    }
    out += c;
  }
  return out;
}

/** Translate backslash escapes allowed inside double-quoted `.env` values. */
function unescapeDoubleQuoted(s: string): string {
  let out = '';
  for (let i = 0; i < s.length; i++) {
    if (s[i] !== '\\' || i === s.length - 1) {
      out += s[i];
      continue;
    }
    const next = s[i + 1];
    if (next === '$') {
      // Preserve `\$` so a later consumer still sees the escape marker is gone
      // but expansion already ran; emit a literal `$`.
      out += '$';
    } else {
      const map: Record<string, string> = {
        n: '\n', r: '\r', t: '\t', f: '\f', b: '\b', v: '\v',
        '\\': '\\', '"': '"', '`': '`', "'": "'", '0': '\0',
      };
      out += map[next] ?? next;
    }
    i++;
  }
  return out;
}

/** Find the index of the closing quote, honouring `\"` escapes inside `"`. */
function findClosingQuote(s: string, q: string): number {
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '\\' && q === '"') {
      i++;
      continue;
    }
    if (s[i] === q) return i;
  }
  return -1;
}

/** Strip an unquoted inline comment: a `#` at start or preceded by whitespace. */
function stripInlineComment(s: string): string {
  for (let i = 0; i < s.length; i++) {
    if (s[i] === '#' && (i === 0 || /\s/.test(s[i - 1]))) {
      return s.slice(0, i);
    }
  }
  return s;
}

/**
 * Parse `.env`-format text into a key/value record, matching the conventions of
 * the standard dotenv ecosystem (and Claude Code's loader):
 *
 * - Blank lines and `#` comment lines are ignored.
 * - An optional `export ` prefix is stripped (`export FOO=bar`).
 * - Keys must match `[A-Za-z_][A-Za-z0-9_.]*`; malformed lines are skipped.
 * - Single-quoted values are literal (no escapes, no expansion).
 * - Backtick-quoted values are literal but may span multiple lines.
 * - Double-quoted values process `\n`/`\t`/… escapes, may span multiple lines,
 *   and are variable-expanded (unless `expand: false`).
 * - Unquoted values are trimmed, have inline `#` comments removed, and are
 *   variable-expanded.
 * - Later duplicate keys override earlier ones.
 *
 * Pure function: never touches `process.env`. Use {@link loadDotenv} to read a
 * file and/or apply the result to the process environment.
 */
export function parseDotenv(src: string, options: DotenvParseOptions = {}): Record<string, string> {
  const expand = options.expand !== false;
  const baseEnv = options.env ?? {};
  const out: Record<string, string> = {};
  const text = src.replace(/\r\n?/g, '\n');
  const lines = text.split('\n');

  for (let i = 0; i < lines.length; i++) {
    let line = lines[i];
    if (i === 0) line = line.replace(/^\uFEFF/, ''); // strip BOM
    const trimmed = line.replace(/^\s+/, '');
    if (trimmed === '' || trimmed.startsWith('#')) continue;

    const body = trimmed.replace(/^export\s+/, '');
    const eq = body.indexOf('=');
    if (eq === -1) continue;
    const key = body.slice(0, eq).trim();
    if (!/^[A-Za-z_][A-Za-z0-9_.]*$/.test(key)) continue;

    let rest = body.slice(eq + 1).replace(/^\s+/, '');
    const q = rest[0];
    let value: string;

    if (q === '"' || q === "'" || q === '`') {
      let collected = rest.slice(1);
      while (findClosingQuote(collected, q) === -1 && i + 1 < lines.length) {
        i++;
        collected += '\n' + lines[i];
      }
      const close = findClosingQuote(collected, q);
      const inner = close === -1 ? collected : collected.slice(0, close);
      if (q === '"') {
        value = expand ? interpolate(inner, out, baseEnv) : inner;
        value = unescapeDoubleQuoted(value);
      } else {
        value = inner; // single / backtick: literal
      }
    } else {
      value = stripInlineComment(rest).trim();
      if (expand) value = interpolate(value, out, baseEnv);
    }
    out[key] = value;
  }
  return out;
}

export interface LoadDotenvOptions extends DotenvParseOptions {
  /** Apply parsed values to `process.env`. Default `false` (parse only). */
  apply?: boolean;
  /** When applying, overwrite variables already present. Default `false`. */
  override?: boolean;
}

/**
 * Read and parse a `.env` file. Returns the parsed record (empty when the file
 * does not exist or cannot be read). With `apply: true` the values are written
 * into `process.env`; existing variables are preserved unless `override: true`,
 * matching dotenv's "real environment wins" precedence.
 */
export function loadDotenv(filePath: string, options: LoadDotenvOptions = {}): Record<string, string> {
  let content: string;
  try {
    content = readFileSync(filePath, 'utf8');
  } catch {
    return {};
  }
  const parsed = parseDotenv(content, options);
  if (options.apply) {
    for (const [k, v] of Object.entries(parsed)) {
      if (options.override || process.env[k] === undefined) {
        process.env[k] = v;
      }
    }
  }
  return parsed;
}
