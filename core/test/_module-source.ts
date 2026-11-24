/**
 * Source text of a MODULE: the file itself plus every sibling chunk that
 * scripts/split-module.mjs moved code into (chunks start with the split marker).
 *
 * Gates and tests that inspect source text must read modules through this, so a
 * move-only split (verified by scripts/verify-split.mjs) cannot hide code from them.
 */
import * as fs from 'node:fs';
import * as path from 'node:path';

export function splitChunksOf(file: string): string[] {
  const dir = path.dirname(file);
  const marker = `// Split from ${path.basename(file)} by scripts/split-module.mjs`;
  let names: string[] = [];
  try { names = fs.readdirSync(dir); } catch { return []; }
  return names
    .filter((n) => /\.tsx?$/.test(n) && n !== path.basename(file))
    .map((n) => path.join(dir, n))
    .filter((p) => { try { return fs.readFileSync(p, 'utf8').startsWith(marker); } catch { return false; } })
    .sort();
}

export function readModuleSource(file: string): string {
  return [file, ...splitChunksOf(file)].map((p) => fs.readFileSync(p, 'utf8')).join('\n');
}
