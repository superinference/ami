// Repetition / anti-paralysis detection.
//
// Two complementary capabilities live here:
//   1. `detectRepetition(text, options?)` — detects runaway *output* loops in a
//      single blob of text (consecutive identical lines + repeated inline
//      phrases) and reports where to truncate.
//   2. `ToolCallRepetitionTracker` — detects runaway *tool-call* loops across a
//      sequence of tool invocations (identical tool + input repeated), the
//      anti-infinite-loop guardrail for the agentic engine.

export interface RepetitionOptions {
  /** Minimum text length before any detection runs. Default 200. */
  minChars?: number;
  /** Minimum number of lines before the consecutive-line strategy runs. Default 12. */
  minLines?: number;
  /** Consecutive identical lines required to flag. Default 10. */
  maxConsecutive?: number;
  /** Shortest inline phrase (chars) considered. Default 20. */
  minPhraseLen?: number;
  /** Longest inline phrase (chars) considered. Default 80. */
  maxPhraseLen?: number;
  /** Phrase-length step for the inline scan. Default 10. */
  phraseStep?: number;
  /** Non-overlapping phrase repeats required to flag. Default 5. */
  minPhraseRepeats?: number;
  /** Trailing window (chars) scanned for inline phrases. Default 4000. */
  searchWindow?: number;
  /**
   * Phrases whose distinct-character count is <= this are treated as
   * low-entropy decoration (borders/rules) and skipped. Default 2.
   */
  maxEntropyForSkip?: number;
  /**
   * When true, lines are compared after trimming trailing whitespace, so lines
   * that differ only by invisible trailing spaces/tabs still count as repeats.
   * Default false (exact comparison — backward compatible).
   */
  normalizeWhitespace?: boolean;
}

const DEFAULTS: Required<RepetitionOptions> = {
  minChars: 200,
  minLines: 12,
  maxConsecutive: 10,
  minPhraseLen: 20,
  maxPhraseLen: 80,
  phraseStep: 10,
  minPhraseRepeats: 5,
  searchWindow: 4000,
  maxEntropyForSkip: 2,
  normalizeWhitespace: false,
};

export interface RepetitionResult {
  isRepetitive: boolean;
  truncateAt: number;
}

export function detectRepetition(text: string, options?: RepetitionOptions): RepetitionResult {
  const opt: Required<RepetitionOptions> = { ...DEFAULTS, ...(options ?? {}) };

  if (text.length < opt.minChars) return { isRepetitive: false, truncateAt: -1 };

  // Strategy 1: consecutive identical lines.
  const lines = text.split('\n');
  // Optional whitespace normalization: compare trailing-trimmed forms while
  // still measuring truncation offsets against the original text.
  const cmp = opt.normalizeWhitespace
    ? lines.map((l) => l.replace(/[ \t]+$/g, ''))
    : lines;
  if (lines.length >= opt.minLines && opt.maxConsecutive >= 1) {
    let consecutiveCount = 1;
    for (let i = 1; i < lines.length; i++) {
      if (cmp[i] === cmp[i - 1] && cmp[i].trim().length > 0) {
        consecutiveCount++;
        if (consecutiveCount >= opt.maxConsecutive) {
          const startLine = i - consecutiveCount + 1;
          const truncateAt = lines.slice(0, startLine + 1).join('\n').length;
          return { isRepetitive: true, truncateAt };
        }
      } else {
        consecutiveCount = 1;
      }
    }
  }

  // Strategy 2: repeated inline phrase detection (catches same-line loops).
  const minPhraseLen = opt.minPhraseLen;
  const minRepeats = opt.minPhraseRepeats;
  if (minRepeats >= 1 && minPhraseLen >= 1 && opt.maxPhraseLen >= minPhraseLen) {
    const searchWindow = Math.min(text.length, opt.searchWindow);
    const tail = text.slice(-searchWindow);

    for (let phraseLen = minPhraseLen; phraseLen <= opt.maxPhraseLen; phraseLen += opt.phraseStep) {
      const phrase = tail.slice(-phraseLen);
      if (phrase.trim().length < minPhraseLen) continue;
      // Skip low-entropy phrases (e.g. border chars ─────, ═════, -----) whose
      // overlapping matches would false-positive on decorative lines.
      if (new Set(phrase.trim()).size <= opt.maxEntropyForSkip) continue;
      let count = 0;
      let pos = 0;
      while ((pos = tail.indexOf(phrase, pos)) !== -1) {
        count++;
        pos += phrase.length;
        if (count >= minRepeats) {
          const firstOccurrence = text.indexOf(phrase);
          return { isRepetitive: true, truncateAt: firstOccurrence + phrase.length };
        }
      }
    }
  }

  return { isRepetitive: false, truncateAt: -1 };
}

// ---------------------------------------------------------------------------
// Tool-call repetition tracking (anti-infinite-loop guardrail)
// ---------------------------------------------------------------------------

/**
 * Deterministic, dependency-free FNV-1a hash -> base36. Keeps per-call
 * signatures compact regardless of input size so the tracker never retains the
 * full (possibly huge) tool input.
 */
function fnv1a(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) {
    h ^= s.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(36);
}

/**
 * Stable JSON stringify: object keys are emitted in sorted order (recursively),
 * so tool inputs that are structurally identical but key-reordered hash to the
 * same signature. Arrays preserve order (semantically significant).
 */
function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return String(value);
  if (typeof value !== 'object') return JSON.stringify(value) ?? String(value);
  if (Array.isArray(value)) {
    return '[' + value.map((v) => stableStringify(v)).join(',') + ']';
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringify(obj[k])).join(',') + '}';
}

/** Build the canonical signature for a (toolName, input) pair. */
export function toolCallSignature(toolName: string, input: unknown): string {
  return fnv1a(toolName + '\u0000' + stableStringify(input));
}

export interface ToolCallRepetitionOptions {
  /**
   * Number of identical calls (consecutive, or within the window when
   * `windowSize` is set) required to flag a loop. Default 3. Values < 1 are
   * clamped to 1.
   */
  threshold?: number;
  /**
   * When > 0, counts identical signatures among the most recent `windowSize`
   * calls (sliding window) rather than requiring them to be strictly
   * consecutive. Default 0 (strict consecutive counting). Values < 0 are
   * treated as 0.
   */
  windowSize?: number;
}

export interface ToolCallRepetitionResult {
  /** True once the identical-call count reaches the threshold. */
  isLooping: boolean;
  /** How many times this signature has repeated (consecutive, or in-window). */
  count: number;
  /** The signature of the call just recorded. */
  signature: string;
  /** The effective threshold used. */
  threshold: number;
}

/**
 * Detects runaway tool-call loops: the same tool invoked with the same input
 * over and over (the classic agent anti-paralysis failure). Hash-based so it
 * never retains the full input. Making a *different* call resets the consecutive
 * counter (progress), so a legitimately-repeated-but-interleaved call never
 * trips the strict mode.
 */
export class ToolCallRepetitionTracker {
  private readonly threshold: number;
  private readonly windowSize: number;
  private lastSignature: string | null = null;
  private consecutive = 0;
  private readonly window: string[] = [];

  constructor(options?: ToolCallRepetitionOptions) {
    this.threshold = Math.max(1, Math.floor(options?.threshold ?? 3));
    this.windowSize = Math.max(0, Math.floor(options?.windowSize ?? 0));
  }

  /** Record a tool call and report whether it constitutes a loop. */
  record(toolName: string, input: unknown): ToolCallRepetitionResult {
    const signature = toolCallSignature(toolName, input);

    // Consecutive counting (reset-on-progress).
    if (signature === this.lastSignature) {
      this.consecutive += 1;
    } else {
      this.consecutive = 1;
      this.lastSignature = signature;
    }

    let count = this.consecutive;
    if (this.windowSize > 0) {
      this.window.push(signature);
      while (this.window.length > this.windowSize) this.window.shift();
      count = this.window.reduce((n, s) => (s === signature ? n + 1 : n), 0);
    }

    return {
      isLooping: count >= this.threshold,
      count,
      signature,
      threshold: this.threshold,
    };
  }

  /** Current consecutive-identical count (0 before any call). */
  get consecutiveCount(): number {
    return this.consecutive;
  }

  /** Clear all state (e.g. after the loop is broken or a new turn begins). */
  reset(): void {
    this.lastSignature = null;
    this.consecutive = 0;
    this.window.length = 0;
  }
}
