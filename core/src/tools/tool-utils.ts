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
    // File doesn't exist yet — no symlink to follow; the logical path check above is sufficient
  }
  return { resolved };
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

// ---------------------------------------------------------------------------
// CRLF-aware line ending helpers (shared by file-edit and file-write)
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
