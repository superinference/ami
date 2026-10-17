import * as path from 'path';
import * as fs from 'fs';
import type { ToolResult } from '../types';

/**
 * Validate that a required string input field is present and non-empty.
 * Returns a ToolResult error if invalid, or null if valid.
 */
export function validateRequiredString(
  value: unknown,
  fieldName: string,
): ToolResult | null {
  if (!value || (typeof value === 'string' && value.trim().length === 0)) {
    return { output: `Error: ${fieldName} must not be empty.`, isError: true };
  }
  return null;
}

/**
 * Resolve a search path relative to cwd.
 * If inputPath is provided, resolves it absolutely; otherwise returns cwd.
 */
export function resolveSearchPath(
  inputPath: string | undefined,
  cwd: string,
): { resolved: string; error?: string } {
  const resolved = inputPath
    ? path.isAbsolute(inputPath)
      ? inputPath
      : path.resolve(cwd, inputPath)
    : cwd;
  const abs = path.resolve(resolved);
  const cwdAbs = path.resolve(cwd);
  if (abs !== cwdAbs && !abs.startsWith(cwdAbs + path.sep)) {
    return { resolved, error: `Error: path "${inputPath}" is outside the workspace directory.` };
  }
  return { resolved };
}

export interface ToolDescriptionContext {
  cwd: string;
  projectName?: string;
  hasGit?: boolean;
  hasPackageJson?: boolean;
}

/**
 * Resolve a file path relative to cwd and validate it's inside the workspace.
 * Returns the resolved path or a ToolResult error.
 */
export function resolveFilePath(
  filePath: string,
  cwd: string,
): { resolved: string; error?: ToolResult } {
  const resolved = path.isAbsolute(filePath)
    ? filePath
    : path.resolve(cwd, filePath);
  const cwdAbs = path.resolve(cwd);
  if (!path.resolve(resolved).startsWith(cwdAbs + path.sep) &&
      path.resolve(resolved) !== cwdAbs) {
    return { resolved, error: { output: `Error: path "${filePath}" is outside the workspace directory.`, isError: true } };
  }
  try {
    const real = fs.realpathSync(resolved);
    if (!real.startsWith(cwdAbs + path.sep) && real !== cwdAbs) {
      return { resolved, error: { output: `Error: path "${filePath}" resolves outside the workspace via symlink.`, isError: true } };
    }
  } catch {
    // File doesn't exist yet — but check if parent dir resolves outside workspace via symlink
    const parentDir = path.dirname(resolved);
    try {
      const realParent = fs.realpathSync(parentDir);
      if (!realParent.startsWith(cwdAbs + path.sep) && realParent !== cwdAbs) {
        return { resolved, error: { output: `Error: path "${filePath}" resolves outside the workspace via symlink.`, isError: true } };
      }
    } catch {
      // Parent doesn't exist either — logical path check above is sufficient
    }
  }
  return { resolved };
}

export type FileEncoding = 'utf-8' | 'utf-8-bom' | 'utf-16le' | 'utf-16be';

/** Detect a text file's encoding from its leading BOM bytes (UTF-8 otherwise). */
export function detectEncoding(buf: Buffer): FileEncoding {
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) return 'utf-8-bom';
  if (buf.length >= 2 && buf[0] === 0xFF && buf[1] === 0xFE) return 'utf-16le';
  if (buf.length >= 2 && buf[0] === 0xFE && buf[1] === 0xFF) return 'utf-16be';
  return 'utf-8';
}

/** Decode a buffer to text, stripping any BOM and honoring UTF-16 byte order. */
export function decodeBuffer(buf: Buffer): { text: string; encoding: FileEncoding } {
  const encoding = detectEncoding(buf);
  if (encoding === 'utf-8-bom') return { text: buf.subarray(3).toString('utf-8'), encoding };
  if (encoding === 'utf-16le') return { text: buf.subarray(2).toString('utf16le'), encoding };
  if (encoding === 'utf-16be') {
    const body = Buffer.from(buf.subarray(2));
    body.swap16();
    return { text: body.toString('utf16le'), encoding };
  }
  return { text: buf.toString('utf-8'), encoding };
}

/** Re-encode text to a buffer in the given encoding, re-prepending any BOM. */
export function encodeText(text: string, encoding: FileEncoding): Buffer {
  if (encoding === 'utf-8-bom') return Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(text, 'utf-8')]);
  if (encoding === 'utf-16le') return Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from(text, 'utf16le')]);
  if (encoding === 'utf-16be') {
    const body = Buffer.from(text, 'utf16le');
    body.swap16();
    return Buffer.concat([Buffer.from([0xFE, 0xFF]), body]);
  }
  return Buffer.from(text, 'utf-8');
}

/**
 * Fail-loud guard for search tools: confirm the resolved target is readable.
 * Returns an isError ToolResult when the path is missing or unreadable, else
 * null. This lets grep/glob distinguish "genuinely zero matches" from "the
 * search could not run" — otherwise an agent that greps a mistyped or
 * permission-denied path gets "No matches found" and wrongly concludes the
 * symbol is absent.
 */
export function assertReadableTarget(resolved: string): ToolResult | null {
  try {
    fs.accessSync(resolved, fs.constants.R_OK);
    return null;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return { output: `Error: search failed — cannot read "${resolved}": ${msg}`, isError: true };
  }
}

/**
 * Validate a required pattern string and resolve a search path in one call.
 * Returns { pattern, resolved } on success, or { error } on validation failure.
 */
export function validatePatternAndPath(
  pattern: unknown,
  searchPath: string | undefined,
  cwd: string,
): { pattern: string; resolved: string; error?: undefined } | { error: ToolResult; pattern?: undefined; resolved?: undefined } {
  const invalid = validateRequiredString(pattern, 'pattern');
  if (invalid) return { error: invalid };
  const { resolved, error: pathError } = resolveSearchPath(searchPath, cwd);
  if (pathError) return { error: { output: pathError, isError: true } };
  return { pattern: pattern as string, resolved };
}

/**
 * Extract and validate a required 'query' string from tool input.
 * Returns { query } on success, or { error } on validation failure.
 */
export function extractQuery(input: Record<string, unknown>): { query: string; error?: undefined } | { error: ToolResult; query?: undefined } {
  const invalid = validateRequiredString(input.query, 'query');
  if (invalid) return { error: invalid };
  return { query: input.query as string };
}

export function buildToolDescriptionContext(cwd: string): ToolDescriptionContext {
  const ctx: ToolDescriptionContext = { cwd, projectName: path.basename(cwd) };
  try { ctx.hasGit = fs.existsSync(path.join(cwd, '.git')); } catch { ctx.hasGit = false; }
  try { ctx.hasPackageJson = fs.existsSync(path.join(cwd, 'package.json')); } catch { ctx.hasPackageJson = false; }
  return ctx;
}

export function renderToolDescription(
  template: string,
  context: ToolDescriptionContext,
): string {
  return template
    .replace(/\{\{cwd\}\}/g, context.cwd)
    .replace(/\{\{projectName\}\}/g, context.projectName ?? '')
    .replace(/\{\{#hasGit\}\}([\s\S]*?)\{\{\/hasGit\}\}/g, context.hasGit ? '$1' : '')
    .replace(/\{\{#hasPackageJson\}\}([\s\S]*?)\{\{\/hasPackageJson\}\}/g, context.hasPackageJson ? '$1' : '');
}

// ---------------------------------------------------------------------------
// Secret scanner — checks content for leaked credentials before writes
// ---------------------------------------------------------------------------

const SECRET_PATTERNS = [
  /(?:^|[^a-zA-Z0-9])(?:sk-[a-zA-Z0-9]{20,})(?:[^a-zA-Z0-9]|$)/,   // OpenAI
  /(?:^|[^a-zA-Z0-9])(?:sk-ant-[a-zA-Z0-9]{20,})/,                   // Anthropic
  /(?:^|[^a-zA-Z0-9])(?:ghp_[a-zA-Z0-9]{36})/,                       // GitHub
  /(?:^|[^a-zA-Z0-9])(?:gsk_[a-zA-Z0-9]{20,})/,                      // Groq
  /(?:^|[^a-zA-Z0-9])(?:AKIA[A-Z0-9]{16})/,                          // AWS
  /(?:^|[^a-zA-Z0-9])(?:AIza[a-zA-Z0-9_-]{35})/,                     // Google
  /(?:password|secret|token|api_key)\s*[:=]\s*['"][^'"]{8,}['"]/i,
];

export function scanForSecrets(content: string): string[] {
  const found: string[] = [];
  for (const pattern of SECRET_PATTERNS) {
    const match = content.match(pattern);
    if (match) found.push(match[0].slice(0, 20) + '...');
  }
  return found;
}

/**
 * Replace detected secret tokens with a stable marker, preserving surrounding
 * boundary characters. Used before writing shared transcripts (/export) so API
 * keys and credentials never land in a file the user might paste or publish.
 */
export function redactSecrets(content: string): string {
  let out = content;
  // Token patterns (first 6 of SECRET_PATTERNS) capture a single boundary char
  // then the secret; redact just the secret run.
  const tokenRes = [
    /(sk-ant-[a-zA-Z0-9-]{20,})/g,
    /(sk-[a-zA-Z0-9]{20,})/g,
    /(ghp_[a-zA-Z0-9]{36})/g,
    /(gsk_[a-zA-Z0-9]{20,})/g,
    /(AKIA[A-Z0-9]{16})/g,
    /(AIza[a-zA-Z0-9_-]{35})/g,
  ];
  for (const re of tokenRes) out = out.replace(re, '[REDACTED-SECRET]');
  // key: "value" / token = 'value' style — redact the quoted value only.
  out = out.replace(/((?:password|secret|token|api_key)\s*[:=]\s*['"])[^'"]{8,}(['"])/gi, '$1[REDACTED-SECRET]$2');
  return out;
}

/**
 * Scan only lines that this edit introduces. A file that already contains
 * `Password = "placeholder"` must stay editable; a newly added secret must not.
 */
export function scanAddedSecrets(before: string, after: string): string[] {
  const beforeCounts = new Map<string, number>();
  for (const line of normalizeToLf(before).split('\n')) {
    beforeCounts.set(line, (beforeCounts.get(line) ?? 0) + 1);
  }
  const added: string[] = [];
  for (const line of normalizeToLf(after).split('\n')) {
    const remaining = beforeCounts.get(line) ?? 0;
    if (remaining > 0) beforeCounts.set(line, remaining - 1);
    else added.push(line);
  }
  if (added.length === 0) return [];
  return scanForSecrets(added.join('\n'));
}

// ---------------------------------------------------------------------------
// CRLF-aware line ending helpers (shared by file-edit)
// ---------------------------------------------------------------------------

export function detectLineEnding(content: string): '\r\n' | '\n' {
  const crlf = (content.match(/\r\n/g) || []).length;
  const lf = (content.match(/(?<!\r)\n/g) || []).length;
  return crlf > lf ? '\r\n' : '\n';
}

export function normalizeToLf(text: string): string {
  return text.replace(/\r\n/g, '\n');
}

export function convertToLineEnding(text: string, ending: '\r\n' | '\n'): string {
  const normalized = normalizeToLf(text);
  if (ending === '\r\n') return normalized.replace(/\n/g, '\r\n');
  return normalized;
}
