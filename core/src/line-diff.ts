/**
 * Positionally-accurate line diff stats (insertions / deletions) between two
 * text contents — the canonical, dependency-free LCS line diff shared across
 * layers.
 *
 * FUNCTIONAL: this is an LCS (longest-common-subsequence) line diff, matching
 * Claude Code's `diffLines`-based diff-stat computation — deletions are the
 * original lines NOT on the LCS, insertions the current lines NOT on it. A
 * naive per-line frequency delta (multiset diff) is coarser: it cancels
 * reordered identical lines to 0/0 (an LCS diff correctly reports the moves as
 * del+add) and cannot tell an in-place edit from an add+delete. For the common
 * cases (pure additions, pure deletions, in-place edits, duplicate add/remove)
 * both agree, so the stats only ever become MORE accurate.
 *
 * This lives in core (the lowest layer) so BOTH the checkpoint manager
 * (common/src/checkpoint.ts) and the transcript metrics extractor
 * (core/src/conversation-summary.ts) use one implementation — no duplicate
 * diff helper per site.
 */
export function computeLineDiff(original: string, current: string): { linesAdded: number; linesRemoved: number } {
  const a = original ? original.split('\n') : [];
  const b = current ? current.split('\n') : [];

  // Trim the common prefix/suffix: those lines are unchanged by definition, so
  // excluding them shrinks the O(n*m) LCS DP (often to nothing for a localized
  // edit) without altering the counts. The suffix trim is bounded by `start` on
  // BOTH sides so prefix and suffix can never overlap and double-count a line.
  let start = 0;
  while (start < a.length && start < b.length && a[start] === b[start]) start++;
  let endA = a.length;
  let endB = b.length;
  while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) { endA--; endB--; }

  const aMid = a.slice(start, endA);
  const bMid = b.slice(start, endB);
  const n = aMid.length;
  const m = bMid.length;

  // Space-optimized LCS length over the trimmed middles (two rolling rows,
  // O(n*m) time, O(m) space). When either middle is empty the LCS is 0 and
  // every remaining line on the other side is a pure insertion/deletion.
  let lcs = 0;
  if (n > 0 && m > 0) {
    let prev = new Array<number>(m + 1).fill(0);
    let curr = new Array<number>(m + 1).fill(0);
    for (let i = 1; i <= n; i++) {
      for (let j = 1; j <= m; j++) {
        if (aMid[i - 1] === bMid[j - 1]) curr[j] = prev[j - 1] + 1;
        else curr[j] = prev[j] >= curr[j - 1] ? prev[j] : curr[j - 1];
      }
      [prev, curr] = [curr, prev];
    }
    lcs = prev[m];
  }

  return { linesAdded: m - lcs, linesRemoved: n - lcs };
}
