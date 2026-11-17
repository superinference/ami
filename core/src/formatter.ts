import { execSync } from 'child_process';
import * as path from 'path';

/** @public Formatter configuration for library consumers to define custom formatters. */
export interface FormatterConfig {
  extensions: string[];
  command: string;
  args: string[];
  check?: string;
}

const BUILT_IN_FORMATTERS: FormatterConfig[] = [
  { extensions: ['.ts', '.tsx', '.js', '.jsx', '.mjs', '.cjs'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.py', '.pyw'], command: 'ruff', args: ['format'], check: 'ruff --version' },
  { extensions: ['.go'], command: 'gofmt', args: ['-w'], check: 'gofmt --help' },
  { extensions: ['.rs'], command: 'rustfmt', args: [], check: 'rustfmt --version' },
  { extensions: ['.java'], command: 'google-java-format', args: ['-i'], check: 'google-java-format --version' },
  { extensions: ['.rb'], command: 'rubocop', args: ['-a', '--fail-level', 'fatal'], check: 'rubocop --version' },
  { extensions: ['.php'], command: 'php-cs-fixer', args: ['fix'], check: 'php-cs-fixer --version' },
  { extensions: ['.swift'], command: 'swift-format', args: ['format', '-i'], check: 'swift-format --version' },
  { extensions: ['.kt', '.kts'], command: 'ktlint', args: ['-F'], check: 'ktlint --version' },
  { extensions: ['.css', '.scss', '.less'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.json'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.yaml', '.yml'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.md'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.html', '.htm'], command: 'npx', args: ['prettier', '--write'], check: 'npx prettier --version' },
  { extensions: ['.c', '.cpp', '.h', '.hpp'], command: 'clang-format', args: ['-i'], check: 'clang-format --version' },
  { extensions: ['.sh', '.bash'], command: 'shfmt', args: ['-w'], check: 'shfmt --version' },
  { extensions: ['.lua'], command: 'stylua', args: [], check: 'stylua --version' },
  { extensions: ['.zig'], command: 'zig', args: ['fmt'], check: 'zig version' },
  { extensions: ['.dart'], command: 'dart', args: ['format'], check: 'dart --version' },
  { extensions: ['.ex', '.exs'], command: 'mix', args: ['format'], check: 'mix --version' },
];

const formatterCache = new Map<string, boolean>();

/**
 * Custom formatters registered by library consumers via {@link registerFormatter}.
 * These take precedence over {@link BUILT_IN_FORMATTERS}; the most recently
 * registered wins for a given extension (prepended on registration).
 */
const customFormatters: FormatterConfig[] = [];

/**
 * Mockable exec seam (ESM-safe). Tests replace `execHooks.execSync` via
 * `mock.method` to simulate formatter availability/success/failure
 * deterministically. Defaults to the real child_process.execSync.
 */
export const execHooks = { execSync };

/** Clear the formatter-availability memo (test seam; harmless in production). */
export function __clearFormatterCache(): void {
  formatterCache.clear();
}

/**
 * Normalize a file extension for lookup: trims surrounding whitespace,
 * lowercases, collapses any leading dots to exactly one, and returns `''` for
 * empty or non-string input. So `'TS'`, `'.ts'`, `'..TS '`, `'ts'` all
 * normalize to `'.ts'`.
 *
 * ADDITIVE (new helper): previously there was no way to canonicalize an
 * extension, so callers holding a bare `'ts'` or an uppercase `'.TS'` could not
 * look up the matching formatter.
 * @public
 */
export function normalizeExtension(ext: string): string {
  if (typeof ext !== 'string') return '';
  const trimmed = ext.trim().toLowerCase().replace(/^\.+/, '');
  return trimmed === '' ? '' : `.${trimmed}`;
}

/**
 * Lowercased extension of a file path, guarded against non-string/empty input.
 * Returns `''` instead of throwing (plain `path.extname(undefined)` throws
 * `ERR_INVALID_ARG_TYPE`).
 */
function extensionOfPath(filePath: string): string {
  if (typeof filePath !== 'string' || filePath === '') return '';
  return path.extname(filePath).toLowerCase();
}

/**
 * Resolve the formatter config that handles `ext` (with or without a leading
 * dot, any case). Custom registered formatters take precedence over built-ins.
 * Returns `undefined` when nothing handles it or input is invalid.
 *
 * ADDITIVE (new helper + custom-formatter precedence).
 * @public
 */
export function getFormatterForExtension(ext: string): FormatterConfig | undefined {
  const norm = normalizeExtension(ext);
  if (norm === '') return undefined;
  return customFormatters.find(f => f.extensions.includes(norm))
    ?? BUILT_IN_FORMATTERS.find(f => f.extensions.includes(norm));
}

/**
 * True when some built-in or registered formatter claims this file's extension,
 * regardless of whether the underlying tool is installed. Safe for any input
 * (returns `false` for invalid/empty paths and extension-less files such as
 * `Makefile` instead of throwing).
 *
 * ADDITIVE (new query helper).
 * @public
 */
export function isFormattableFile(filePath: string): boolean {
  const ext = extensionOfPath(filePath);
  return ext !== '' && getFormatterForExtension(ext) !== undefined;
}

/**
 * Register a custom formatter for library consumers (wires the previously
 * dead `@public FormatterConfig` interface — there was no way to contribute a
 * formatter). Validates shape, normalizes extensions, and gives the entry
 * precedence over built-ins (and earlier customs) for its extensions.
 *
 * ADDITIVE (functional: {@link formatFile} and {@link getAvailableFormatters}
 * now consult registered customs; with none registered, behavior is unchanged).
 * Throws `TypeError` on an invalid config (not an object, empty command, or no
 * valid extension) so misconfiguration surfaces at registration time.
 * @public
 */
export function registerFormatter(config: FormatterConfig): void {
  if (!config || typeof config !== 'object') {
    throw new TypeError('registerFormatter: config must be an object');
  }
  if (typeof config.command !== 'string' || config.command.trim() === '') {
    throw new TypeError('registerFormatter: command must be a non-empty string');
  }
  if (!Array.isArray(config.extensions)) {
    throw new TypeError('registerFormatter: extensions must be an array');
  }
  const extensions = Array.from(
    new Set(config.extensions.map(normalizeExtension).filter(e => e !== '')),
  );
  if (extensions.length === 0) {
    throw new TypeError('registerFormatter: at least one valid extension is required');
  }
  const args = Array.isArray(config.args) ? config.args.map(a => String(a)) : [];
  const normalized: FormatterConfig = { extensions, command: config.command, args };
  if (typeof config.check === 'string' && config.check.trim() !== '') {
    normalized.check = config.check;
  }
  customFormatters.unshift(normalized);
}

/** Remove all registered custom formatters (lifecycle reset + test seam). @public */
export function clearCustomFormatters(): void {
  customFormatters.length = 0;
}

/**
 * Defensive snapshot of registered custom formatters (in precedence order).
 * @public
 */
export function listRegisteredFormatters(): FormatterConfig[] {
  return customFormatters.map(f => ({
    ...f,
    extensions: [...f.extensions],
    args: [...f.args],
  }));
}

/**
 * True when the formatter's configured `check` command (a `<tool> --version` /
 * `--help` presence probe) exits 0; memoized per `check` string.
 *
 * Capability-depth rationale (deliberately a shallow presence probe, NOT a
 * deep operation probe): a deep "probe the real operation" check is only
 * warranted when a tool's version probe can pass while the gated operation
 * still fails with *no fallback* (e.g. a CLI whose version prints fine but
 * whose API is disabled by a preference toggle). That does not hold for code
 * formatters: `--version`/`--help` passing means the binary is installed and
 * runnable, and {@link formatFile} already degrades gracefully at use time —
 * it wraps the real invocation in try/catch and returns
 * `{ formatted: false, error }` instead of throwing — so there is a fallback
 * and no silent late failure. Probing the real operation would also require
 * running a mutating formatter (writing files / per-tool stdin plumbing) as a
 * side effect, which is worse than the current cheap check. Hence version-depth
 * is the correct and sufficient probe here.
 */
export function isFormatterAvailable(config: FormatterConfig): boolean {
  if (!config || !config.check) return false;
  const key = config.check;
  if (formatterCache.has(key)) return formatterCache.get(key)!;
  try {
    execHooks.execSync(config.check, { timeout: 5000, stdio: 'pipe' });
    formatterCache.set(key, true);
    return true;
  } catch {
    formatterCache.set(key, false);
    return false;
  }
}

export function formatFile(filePath: string, cwd: string): { formatted: boolean; error?: string } {
  const ext = extensionOfPath(filePath);
  if (ext === '') return { formatted: false };
  const config = getFormatterForExtension(ext);
  if (!config) return { formatted: false };
  if (!isFormatterAvailable(config)) return { formatted: false };
  try {
    execHooks.execSync(`${config.command} ${config.args.join(' ')} '${filePath.replace(/'/g, "'\\''")}'`, {
      cwd, timeout: 15000, stdio: 'pipe',
    });
    return { formatted: true };
  } catch (err) {
    return { formatted: false, error: err instanceof Error ? err.message : String(err) };
  }
}

export function getAvailableFormatters(_cwd: string): string[] {
  return [...customFormatters, ...BUILT_IN_FORMATTERS]
    .filter(f => isFormatterAvailable(f))
    .map(f => `${f.extensions.join(', ')} → ${f.command} ${f.args.join(' ')}`);
}
