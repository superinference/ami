// Split from cost-tracker.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { FpsMetrics } from './profiler';
import { MAX_LCS_CELLS, splitDiffLines } from './cost-tracker-cache-diag-request';
import type { DiffOp } from './cost-tracker-cache-diag-request';

/** LCS-based line diff of two already prefix/suffix-trimmed segments. */
export function lcsDiff(a: string[], b: string[]): DiffOp[] {
  const n = a.length;
  const m = b.length;
  // dp[i][j] = LCS length of a[i:] and b[j:].
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array<number>(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const out: DiffOp[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) { out.push({ t: '=', line: a[i] }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { out.push({ t: '-', line: a[i] }); i++; }
    else { out.push({ t: '+', line: b[j] }); j++; }
  }
  while (i < n) out.push({ t: '-', line: a[i++] });
  while (j < m) out.push({ t: '+', line: b[j++] });
  return out;
}

/** Diff two line arrays into ops, trimming common prefix/suffix first (cheap for a small change). */
export function diffOps(a: string[], b: string[]): DiffOp[] {
  let start = 0;
  while (start < a.length && start < b.length && a[start] === b[start]) start++;
  let endA = a.length;
  let endB = b.length;
  while (endA > start && endB > start && a[endA - 1] === b[endB - 1]) { endA--; endB--; }
  const midA = a.slice(start, endA);
  const midB = b.slice(start, endB);
  const ops: DiffOp[] = [];
  for (let k = 0; k < start; k++) ops.push({ t: '=', line: a[k] });
  if (midA.length * midB.length > MAX_LCS_CELLS) {
    // Too large for an O(n·m) LCS — emit a coarse (correct, if noisy) diff.
    for (const line of midA) ops.push({ t: '-', line });
    for (const line of midB) ops.push({ t: '+', line });
  } else {
    ops.push(...lcsDiff(midA, midB));
  }
  for (let k = endA; k < a.length; k++) ops.push({ t: '=', line: a[k] });
  return ops;
}

/** Number of unchanged context lines kept around each change hunk. */
export const DIFF_CONTEXT = 3;

/**
 * Produce a standard unified diff (`--- / +++` header, `@@ -a,b +c,d @@` hunks with
 * {@link DIFF_CONTEXT} lines of context) of `oldStr`→`newStr`. A self-contained
 * replacement for the `diff` package's createPatch (no third-party dependency), used
 * to serialize a prompt-cache-break's before/after state. Returns '' when identical.
 */
export function createUnifiedPatch(
  fileName: string,
  oldStr: string,
  newStr: string,
  oldHeader = 'before',
  newHeader = 'after',
): string {
  const ops = diffOps(splitDiffLines(oldStr), splitDiffLines(newStr));
  if (!ops.some(o => o.t !== '=')) return '';

  // Group ops into hunks separated by runs of > 2·context unchanged lines.
  type Hunk = { oldStart: number; oldLines: number; newStart: number; newLines: number; rows: string[] };
  const hunks: Hunk[] = [];
  let oldLine = 1;
  let newLine = 1;
  let current: Hunk | null = null;
  let trailingContext = 0;

  const flush = (): void => { if (current) { hunks.push(current); current = null; trailingContext = 0; } };

  for (let idx = 0; idx < ops.length; idx++) {
    const op = ops[idx];
    if (op.t === '=') {
      if (current) {
        // Look ahead: is another change within 2·context lines? If so keep the gap as context.
        let nextChange = -1;
        for (let k = idx; k < ops.length; k++) { if (ops[k].t !== '=') { nextChange = k; break; } }
        const gap = nextChange === -1 ? Infinity : nextChange - idx;
        if (gap > DIFF_CONTEXT * 2) {
          // Close the hunk with up to DIFF_CONTEXT trailing context lines.
          for (let c = 0; c < DIFF_CONTEXT && idx + c < ops.length && ops[idx + c].t === '='; c++) {
            current.rows.push(` ${ops[idx + c].line}`);
            current.oldLines++; current.newLines++;
          }
          oldLine += gap;
          newLine += gap;
          flush();
          idx += gap - 1;
          continue;
        }
        current.rows.push(` ${op.line}`);
        current.oldLines++; current.newLines++;
        trailingContext++;
      }
      oldLine++; newLine++;
    } else {
      if (!current) {
        // Open a hunk with up to DIFF_CONTEXT leading context lines from just before idx.
        const lead: string[] = [];
        let li = idx - 1;
        while (li >= 0 && ops[li].t === '=' && lead.length < DIFF_CONTEXT) { lead.unshift(` ${ops[li].line}`); li--; }
        current = {
          oldStart: oldLine - lead.length,
          newStart: newLine - lead.length,
          oldLines: lead.length,
          newLines: lead.length,
          rows: lead,
        };
      }
      trailingContext = 0;
      if (op.t === '-') { current.rows.push(`-${op.line}`); current.oldLines++; oldLine++; }
      else { current.rows.push(`+${op.line}`); current.newLines++; newLine++; }
    }
  }
  if (current) {
    // Drop excess trailing context beyond DIFF_CONTEXT accumulated at the tail.
    const extra = Math.max(0, trailingContext - DIFF_CONTEXT);
    if (extra > 0) {
      current.rows.splice(current.rows.length - extra, extra);
      current.oldLines -= extra; current.newLines -= extra;
    }
    flush();
  }

  const headerLines = [`--- ${fileName}\t${oldHeader}`, `+++ ${fileName}\t${newHeader}`];
  const body = hunks.map(h => {
    const head = `@@ -${h.oldStart},${h.oldLines} +${h.newStart},${h.newLines} @@`;
    return [head, ...h.rows].join('\n');
  });
  return `${headerLines.join('\n')}\n${body.join('\n')}\n`;
}

// ---------------------------------------------------------------------------
// Cost tracking safety — bounds checking, serialization for persistence
// ---------------------------------------------------------------------------

export const MAX_REASONABLE_COST_USD = 1000;

export const MAX_REASONABLE_TOKENS = 100_000_000;

export function validateCostEntry(
  cost: number,
  inputTokens: number,
  outputTokens: number,
): { valid: boolean; reason?: string } {
  if (cost < 0) return { valid: false, reason: 'Negative cost' };
  if (inputTokens < 0) return { valid: false, reason: 'Negative input token count' };
  if (outputTokens < 0) return { valid: false, reason: 'Negative output token count' };
  const totalTokens = inputTokens + outputTokens;
  if (cost > MAX_REASONABLE_COST_USD) {
    return { valid: true, reason: `Cost $${cost.toFixed(2)} exceeds $${MAX_REASONABLE_COST_USD} threshold` };
  }
  if (totalTokens > MAX_REASONABLE_TOKENS) {
    return { valid: true, reason: `Token count ${totalTokens} exceeds ${MAX_REASONABLE_TOKENS} threshold` };
  }
  return { valid: true };
}

export interface CostState {
  totalCost: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  requestCount: number;
  /**
   * The session this cost state belongs to. Serialized so a resume can enforce a
   * session-id boundary check (parity: the reference's getStoredSessionCosts, which returns
   * undefined when projectConfig.lastSessionId !== the resuming sessionId) — another
   * session's spend can never be restored into the wrong session. Omitted for an unscoped
   * (session-agnostic) state.
   */
  sessionId?: string;
  /**
   * Optional TUI render-performance snapshot captured at save time and persisted alongside
   * the session's costs (parity: the reference's saveCurrentSessionCosts(getFpsMetrics()) →
   * lastFpsAverage/lastFpsLow1Pct). Absent for non-interactive / unsampled sessions.
   */
  fpsMetrics?: FpsMetrics;
}

/** The serialized on-disk shape of a {@link CostState}, including optional scoping fields. */
export interface SerializedCostState {
  totalCost: number;
  totalInputTokens: number;
  totalOutputTokens: number;
  requestCount: number;
  version: 1;
  sessionId?: string;
  fpsMetrics?: FpsMetrics;
}

/** True when an {@link FpsMetrics}-shaped value carries two finite, non-negative numbers. */
export function isValidFpsMetrics(v: unknown): v is FpsMetrics {
  return (
    typeof v === 'object' && v !== null &&
    Number.isFinite((v as FpsMetrics).averageFps) &&
    Number.isFinite((v as FpsMetrics).low1PctFps) &&
    (v as FpsMetrics).averageFps >= 0 &&
    (v as FpsMetrics).low1PctFps >= 0
  );
}

export function serializeCostState(tracker: CostState): string {
  const out: SerializedCostState = {
    totalCost: tracker.totalCost,
    totalInputTokens: tracker.totalInputTokens,
    totalOutputTokens: tracker.totalOutputTokens,
    requestCount: tracker.requestCount,
    version: 1,
  };
  if (typeof tracker.sessionId === 'string' && tracker.sessionId) {
    out.sessionId = tracker.sessionId;
  }
  if (isValidFpsMetrics(tracker.fpsMetrics)) {
    out.fpsMetrics = { averageFps: tracker.fpsMetrics.averageFps, low1PctFps: tracker.fpsMetrics.low1PctFps };
  }
  return JSON.stringify(out);
}

export function deserializeCostState(json: string): CostState | null {
  try {
    const parsed = JSON.parse(json);
    if (
      typeof parsed !== 'object' || parsed === null ||
      typeof parsed.totalCost !== 'number' ||
      typeof parsed.totalInputTokens !== 'number' ||
      typeof parsed.totalOutputTokens !== 'number' ||
      typeof parsed.requestCount !== 'number'
    ) {
      return null;
    }
    if (parsed.totalCost < 0 || parsed.totalInputTokens < 0 ||
        parsed.totalOutputTokens < 0 || parsed.requestCount < 0) {
      return null;
    }
    const state: CostState = {
      totalCost: parsed.totalCost,
      totalInputTokens: parsed.totalInputTokens,
      totalOutputTokens: parsed.totalOutputTokens,
      requestCount: parsed.requestCount,
    };
    if (typeof parsed.sessionId === 'string' && parsed.sessionId) {
      state.sessionId = parsed.sessionId;
    }
    if (isValidFpsMetrics(parsed.fpsMetrics)) {
      state.fpsMetrics = { averageFps: parsed.fpsMetrics.averageFps, low1PctFps: parsed.fpsMetrics.low1PctFps };
    }
    return state;
  } catch {
    return null;
  }
}

/**
 * Deserialize a persisted cost state ONLY when it belongs to `sessionId` — a session-id
 * boundary check mirroring the reference's getStoredSessionCosts (which returns undefined when
 * projectConfig.lastSessionId !== the resuming sessionId). A state carrying a DIFFERENT
 * embedded sessionId (e.g. a copied or corrupted session file) is rejected (`null`) so
 * another session's spend can never be restored into this one. A state with NO embedded
 * sessionId is treated as unscoped and allowed — the cost state lives in the session's own
 * file, so file identity already scopes it. Malformed JSON → `null` (via deserializeCostState).
 */
export function deserializeCostStateForSession(json: string, sessionId: string): CostState | null {
  const state = deserializeCostState(json);
  if (!state) return null;
  if (state.sessionId !== undefined && state.sessionId !== sessionId) return null;
  return state;
}
