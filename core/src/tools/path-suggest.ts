import * as fs from 'fs';
import * as path from 'path';

export function levenshteinDistance(a: string, b: string): number {
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;

  const matrix: number[][] = [];
  for (let i = 0; i <= a.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= b.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= a.length; i++) {
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      matrix[i][j] = Math.min(
        matrix[i - 1][j] + 1,
        matrix[i][j - 1] + 1,
        matrix[i - 1][j - 1] + cost,
      );
    }
  }
  return matrix[a.length][b.length];
}

export function suggestSimilarPaths(
  missingPath: string,
  cwd: string,
  maxResults = 3,
  maxDistance = 3,
): string[] {
  const abs = path.isAbsolute(missingPath) ? missingPath : path.resolve(cwd, missingPath);

  if (fs.existsSync(abs)) return [];

  const dir = path.dirname(abs);
  const base = path.basename(abs);

  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return [];
  }

  const scored: Array<{ name: string; distance: number }> = [];
  for (const entry of entries) {
    const dist = levenshteinDistance(base.toLowerCase(), entry.toLowerCase());
    if (dist <= maxDistance && dist > 0) {
      scored.push({ name: entry, distance: dist });
    }
  }

  scored.sort((a, b) => a.distance - b.distance);
  return scored.slice(0, maxResults).map(s => path.join(dir, s.name));
}
