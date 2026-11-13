import * as path from 'path';

/**
 * Throw if `filePath` is not contained within `baseDir` (path-traversal guard). Shared by the
 * session store and the reference journal; lives in its own module so neither has to import the
 * other (avoids a session ↔ reference-journal import cycle).
 */
export function assertPathWithin(filePath: string, baseDir: string): void {
  const resolved = path.resolve(filePath);
  const resolvedBase = path.resolve(baseDir);
  if (!resolved.startsWith(resolvedBase + path.sep) && resolved !== resolvedBase) {
    throw new Error(`Path traversal blocked: ${filePath} escapes ${baseDir}`);
  }
}
