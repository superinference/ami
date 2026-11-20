/**
 * Scalable fuzzy file-PATH index (pure-TypeScript port of the reference's native
 * file-index, which wraps nucleo — https://github.com/helix-editor/nucleo).
 *
 * Unlike the per-call subsequence scorer in {@link ./workspace-indexer} (which
 * ranks symbol NAMES), this indexes whole file PATHS once into parallel typed
 * arrays and then answers many `search()` queries cheaply:
 *   - a per-path a–z presence bitmap gives O(1) rejection of any path missing a
 *     needle letter (no scan at all for the common "no match" case);
 *   - a fused `indexOf` scan finds greedy-earliest match positions AND
 *     accumulates consecutive/gap terms in one pass;
 *   - a gap-bound early-reject skips the charCode-heavy boundary pass when the
 *     best-possible score can't beat the current top-k threshold;
 *   - a top-k sorted array (binary insertion) avoids an O(n log n) sort of all
 *     matches when only `limit` are wanted.
 *
 * Scoring constants and the boundary/camel classifier are shared with the symbol
 * scorer via {@link ./fuzzy-score} (one canonical "match quality" definition).
 *
 * Key API:
 *   new FileIndex()
 *   .loadFromFileList(fileList: string[]): void            — dedupe + index paths
 *   .loadFromFileListAsync(fileList): { queryable, done }  — chunked, yields to the loop
 *   .search(query: string, limit: number): SearchResult[]
 *
 * Score semantics: lower = better. Score is position-in-results / result-count,
 * so the best match is 0.0. Paths containing "test" get a 1.05× penalty (capped
 * at 1.0) so non-test files rank slightly higher.
 */

import {
  SCORE_MATCH,
  BONUS_BOUNDARY,
  BONUS_CONSECUTIVE,
  BONUS_FIRST_CHAR,
  PENALTY_GAP_START,
  PENALTY_GAP_EXTENSION,
  FUZZY_MAX_QUERY_LEN,
  boundaryBonus,
} from './fuzzy-score';

export type SearchResult = {
  path: string;
  score: number;
};

const TOP_LEVEL_CACHE_LIMIT = 100;

/**
 * Tunable cadence for the chunked async build (mirrors
 * {@link ./workspace-indexer}'s `indexBuildConfig`). The async build yields to
 * the event loop on a WALL-CLOCK budget (`chunkMs`) rather than a fixed count,
 * checking the clock only every `chunkCheckInterval` items to amortize the
 * `performance.now()` overhead. Exposed as a mutable object so the cadence is
 * deterministically testable (drop `chunkMs` to 0 to force a yield each chunk).
 */
export const fileIndexBuildConfig: { chunkMs: number; chunkCheckInterval: number } = {
  chunkMs: 4,
  chunkCheckInterval: 256,
};

// Reusable buffer: records where each needle char matched during the indexOf scan.
const posBuf = new Int32Array(FUZZY_MAX_QUERY_LEN);

export class FileIndex {
  private paths: string[] = [];
  private lowerPaths: string[] = [];
  private charBits: Int32Array = new Int32Array(0);
  private pathLens: Uint16Array = new Uint16Array(0);
  private topLevelCache: SearchResult[] | null = null;
  // During async build, tracks how many paths have bitmap/lowerPath filled.
  // search() uses this to search the ready prefix while build continues.
  private readyCount = 0;

  /**
   * Load paths from an array of strings (the main entry point — ripgrep / the
   * workspace indexer collects files, we just search them). Deduplicates and
   * drops empty strings (matches the Rust HashSet behavior).
   */
  loadFromFileList(fileList: string[]): void {
    this.buildIndex(dedupe(fileList));
  }

  /**
   * Async variant: yields to the event loop on a time budget so a huge index
   * (270k+ files) never blocks the main thread for long. Identical result to
   * {@link loadFromFileList}. Returns `{ queryable, done }`:
   *   - `queryable` resolves as soon as the first chunk is indexed (search then
   *     returns partial results over the ready prefix);
   *   - `done` resolves when the entire index is built.
   */
  loadFromFileListAsync(fileList: string[]): {
    queryable: Promise<void>;
    done: Promise<void>;
  } {
    let markQueryable: () => void = () => {};
    const queryable = new Promise<void>(resolve => {
      markQueryable = resolve;
    });
    const done = this.buildAsync(fileList, markQueryable);
    return { queryable, done };
  }

  private async buildAsync(
    fileList: string[],
    markQueryable: () => void,
  ): Promise<void> {
    const { chunkMs, chunkCheckInterval } = fileIndexBuildConfig;
    const mask = chunkCheckInterval - 1;
    const seen = new Set<string>();
    const paths: string[] = [];
    let chunkStart = performance.now();
    for (let i = 0; i < fileList.length; i++) {
      const line = fileList[i]!;
      if (line.length > 0 && !seen.has(line)) {
        seen.add(line);
        paths.push(line);
      }
      if ((i & mask) === mask && performance.now() - chunkStart > chunkMs) {
        await yieldToEventLoop();
        chunkStart = performance.now();
      }
    }

    this.resetArrays(paths);

    chunkStart = performance.now();
    let firstChunk = true;
    for (let i = 0; i < paths.length; i++) {
      this.indexPath(i);
      if ((i & mask) === mask && performance.now() - chunkStart > chunkMs) {
        this.readyCount = i + 1;
        if (firstChunk) {
          markQueryable();
          firstChunk = false;
        }
        await yieldToEventLoop();
        chunkStart = performance.now();
      }
    }
    this.readyCount = paths.length;
    markQueryable();
  }

  private buildIndex(paths: string[]): void {
    this.resetArrays(paths);
    for (let i = 0; i < paths.length; i++) {
      this.indexPath(i);
    }
    this.readyCount = paths.length;
  }

  private resetArrays(paths: string[]): void {
    const n = paths.length;
    this.paths = paths;
    this.lowerPaths = new Array(n);
    this.charBits = new Int32Array(n);
    this.pathLens = new Uint16Array(n);
    this.readyCount = 0;
    this.topLevelCache = computeTopLevelEntries(paths, TOP_LEVEL_CACHE_LIMIT);
  }

  // Precompute per path: lowercase string, an a–z presence bitmap, and length.
  // The bitmap gives O(1) rejection of any path missing a needle letter.
  private indexPath(i: number): void {
    const lp = this.paths[i]!.toLowerCase();
    this.lowerPaths[i] = lp;
    const len = lp.length;
    this.pathLens[i] = len;
    let bits = 0;
    for (let j = 0; j < len; j++) {
      const c = lp.charCodeAt(j);
      if (c >= 97 && c <= 122) bits |= 1 << (c - 97);
    }
    this.charBits[i] = bits;
  }

  /**
   * Search for files matching `query`, returning the top `limit` results sorted
   * best-first. An empty query returns the cached top-level entries; a
   * non-positive `limit` returns nothing. Smart case: an all-lowercase query
   * matches case-insensitively, any uppercase makes the match case-sensitive.
   */
  search(query: string, limit: number): SearchResult[] {
    if (limit <= 0) return [];
    if (query.length === 0) {
      if (this.topLevelCache) {
        return this.topLevelCache.slice(0, limit);
      }
      return [];
    }

    const caseSensitive = query !== query.toLowerCase();
    const needle = caseSensitive ? query : query.toLowerCase();
    const nLen = Math.min(needle.length, FUZZY_MAX_QUERY_LEN);
    const needleChars: string[] = new Array(nLen);
    let needleBitmap = 0;
    for (let j = 0; j < nLen; j++) {
      const ch = needle.charAt(j);
      needleChars[j] = ch;
      const cc = ch.charCodeAt(0);
      if (cc >= 97 && cc <= 122) needleBitmap |= 1 << (cc - 97);
    }

    // Upper bound on score (every match earns the max boundary bonus). Used to
    // reject paths whose gap penalties alone sink them below the top-k
    // threshold, before the charCode-heavy boundary pass.
    const scoreCeiling =
      nLen * (SCORE_MATCH + BONUS_BOUNDARY) + BONUS_FIRST_CHAR + 32;

    // Top-k: a sorted-ascending array of the best `limit` matches.
    const topK: { path: string; fuzzScore: number }[] = [];
    let threshold = -Infinity;

    const { paths, lowerPaths, charBits, pathLens, readyCount } = this;

    outer: for (let i = 0; i < readyCount; i++) {
      // O(1) bitmap reject: the path must contain every a–z letter in the needle.
      if ((charBits[i]! & needleBitmap) !== needleBitmap) continue;

      const haystack = caseSensitive ? paths[i]! : lowerPaths[i]!;

      // Fused indexOf scan: find greedy-earliest positions AND accumulate the
      // gap/consecutive terms inline, so we score directly from these positions.
      let pos = haystack.indexOf(needleChars[0]!);
      if (pos === -1) continue;
      posBuf[0] = pos;
      let gapPenalty = 0;
      let consecBonus = 0;
      let prev = pos;
      for (let j = 1; j < nLen; j++) {
        pos = haystack.indexOf(needleChars[j]!, prev + 1);
        if (pos === -1) continue outer;
        posBuf[j] = pos;
        const gap = pos - prev - 1;
        if (gap === 0) consecBonus += BONUS_CONSECUTIVE;
        else gapPenalty += PENALTY_GAP_START + gap * PENALTY_GAP_EXTENSION;
        prev = pos;
      }

      // Gap-bound reject: skip the boundary pass when the best case can't win.
      if (
        topK.length === limit &&
        scoreCeiling + consecBonus - gapPenalty <= threshold
      ) {
        continue;
      }

      // Boundary/camelCase scoring from the char before each match position.
      const path = paths[i]!;
      const hLen = pathLens[i]!;
      let score = nLen * SCORE_MATCH + consecBonus - gapPenalty;
      score += scoreBonusAt(path, posBuf[0]!, true);
      for (let j = 1; j < nLen; j++) {
        score += scoreBonusAt(path, posBuf[j]!, false);
      }
      score += Math.max(0, 32 - (hLen >> 2));

      if (topK.length < limit) {
        topK.push({ path, fuzzScore: score });
        if (topK.length === limit) {
          topK.sort((a, b) => a.fuzzScore - b.fuzzScore);
          threshold = topK[0]!.fuzzScore;
        }
      } else if (score > threshold) {
        let lo = 0;
        let hi = topK.length;
        while (lo < hi) {
          const mid = (lo + hi) >> 1;
          if (topK[mid]!.fuzzScore < score) lo = mid + 1;
          else hi = mid;
        }
        topK.splice(lo, 0, { path, fuzzScore: score });
        topK.shift();
        threshold = topK[0]!.fuzzScore;
      }
    }

    // topK is ascending; reverse to descending (best first).
    topK.sort((a, b) => b.fuzzScore - a.fuzzScore);

    const matchCount = topK.length;
    const denom = Math.max(matchCount, 1);
    const results: SearchResult[] = new Array(matchCount);

    for (let i = 0; i < matchCount; i++) {
      const path = topK[i]!.path;
      const positionScore = i / denom;
      const finalScore = path.includes('test')
        ? Math.min(positionScore * 1.05, 1.0)
        : positionScore;
      results[i] = { path, score: finalScore };
    }

    return results;
  }
}

/** Dedupe + drop empty strings, preserving first-seen order (Rust HashSet parity). */
function dedupe(fileList: string[]): string[] {
  const seen = new Set<string>();
  const paths: string[] = [];
  for (const line of fileList) {
    if (line.length > 0 && !seen.has(line)) {
      seen.add(line);
      paths.push(line);
    }
  }
  return paths;
}

/**
 * Boundary/camelCase bonus for a match at `pos` in the original-case `path`.
 * `first` enables the start-of-string bonus (only for needle[0]).
 */
function scoreBonusAt(path: string, pos: number, first: boolean): number {
  if (pos === 0) return first ? BONUS_FIRST_CHAR : 0;
  return boundaryBonus(path, pos);
}

/** Yield a macrotask so the async build doesn't starve the event loop. */
export function yieldToEventLoop(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve));
}

/**
 * Extract unique top-level path segments, sorted by (length asc, then alpha asc).
 * Handles both Unix (`/`) and Windows (`\`) separators. Mirrors
 * FileIndex::compute_top_level_entries in the Rust lib — these are the results
 * an empty query returns (the "top level of the tree" affordance).
 */
function computeTopLevelEntries(
  paths: string[],
  limit: number,
): SearchResult[] {
  const topLevel = new Set<string>();

  for (const p of paths) {
    let end = p.length;
    for (let i = 0; i < p.length; i++) {
      const c = p.charCodeAt(i);
      if (c === 47 || c === 92) {
        end = i;
        break;
      }
    }
    const segment = p.slice(0, end);
    if (segment.length > 0) {
      topLevel.add(segment);
      if (topLevel.size >= limit) break;
    }
  }

  const sorted = Array.from(topLevel);
  sorted.sort((a, b) => {
    const lenDiff = a.length - b.length;
    if (lenDiff !== 0) return lenDiff;
    return a < b ? -1 : a > b ? 1 : 0;
  });

  return sorted.slice(0, limit).map(path => ({ path, score: 0.0 }));
}

export default FileIndex;
