import * as path from 'path';
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
): string {
  return inputPath
    ? path.isAbsolute(inputPath)
      ? inputPath
      : path.resolve(cwd, inputPath)
    : cwd;
}
