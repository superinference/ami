/**
 * Environment-variable utilities: numeric/boolean coercion, `.env` file parsing
 * and loading, and env-var waterfall resolution.
 *
 * These are intentionally dependency-free and side-effect-free (except the thin
 * {@link loadDotenv} filesystem wrapper), so they are safe to import from any
 * layer. Credential redaction lives in `credential-guard` and tool-argument
 * boolean coercion lives in `utils/coerce`; this module does NOT duplicate them.
 */

import { readFileSync } from 'node:fs';

/**
 * Parse an environment variable as an integer, accepting:
 * - Plain integers: "42", "1000"
 * - Scientific notation: "1e6", "5e3"
 * - Digit separators: "64_000", "1_000_000"
 * Returns the parsed integer or the fallback value.
 */
export function parseEnvInt(envVar: string | undefined, fallback: number): number {
  if (!envVar) return fallback;
  const cleaned = envVar.replace(/_/g, '');
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return fallback;
  return Math.trunc(n);
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
