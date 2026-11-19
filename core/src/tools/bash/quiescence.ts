/**
 * Quiescence / blocked-on-input state machine for the bash headless path.
 *
 * AMI's bash tool today waits out the whole timeout when a command silently
 * stops to ask a question ("Password:", "(y/n)"), because it only has raw bytes
 * and a hard deadline. Orca's `tui-idle-evidence` ranks timing + tail evidence
 * into a verdict so the UI can say *why* it stopped. This is the same idea, in
 * AMI terms and scoped to the bash tool:
 *
 *   - `streaming`        — output is still arriving (not yet quiet)
 *   - `blocked-on-input` — output went quiet AND the rendered tail looks like an
 *                          interactive prompt (so the model should answer / send
 *                          input, not keep polling to the deadline)
 *   - `quiescent`        — output went quiet with no prompt (likely a slow
 *                          computation; keep waiting) OR the process exited
 *
 * The blocked tail is the tier-0 signal (`readTailBlockedReason` in Orca):
 * a known prompt pattern in the LAST non-empty line dominates every timing tier.
 */

export type BashBlockedReason =
  | 'password'
  | 'passphrase'
  | 'sudo-password'
  | 'yes-no'
  | 'confirm'
  | 'overwrite'
  | 'press-key'
  | 'generic-prompt';

export type BashQuiescenceState = 'streaming' | 'quiescent' | 'blocked-on-input';

export type BashQuiescenceVerdict = {
  state: BashQuiescenceState;
  /** Set only when `state === 'blocked-on-input'`. */
  reason?: BashBlockedReason;
};

type BlockedRule = { reason: BashBlockedReason; re: RegExp };

/**
 * Prompt patterns matched against the trimmed LAST non-empty line. Ordered
 * most-specific first so `sudo` password wins over a generic `password:`.
 */
const BLOCKED_RULES: readonly BlockedRule[] = [
  { reason: 'sudo-password', re: /\[sudo\]\s+password\s+for\b|^password\s+for\b/i },
  { reason: 'passphrase', re: /\bpass(?:[- ]?)phrase\b.*:?\s*$|enter\s+passphrase/i },
  { reason: 'password', re: /\bpassword\b\s*:?\s*$|enter\s+password/i },
  { reason: 'yes-no', re: /\(\s*y(?:es)?\s*\/\s*n(?:o)?\s*\)\s*\??\s*$|\[\s*y\/n\s*\]\s*\??\s*$|\[\s*y\/n\s*,\s*default[^\]]*\]\s*$/i },
  // press-key precedes confirm: "Press Enter to proceed" must not fall to the
  // generic confirm `proceed` match.
  { reason: 'press-key', re: /\bpress\s+(any\s+key|enter|return|\[enter\])\b/i },
  { reason: 'overwrite', re: /\boverwrite\b.*\?\s*$|already exists.*\?\s*$/i },
  { reason: 'confirm', re: /\b(are you sure|do you want to continue|proceed\??|continue\?)\s*\??\s*$/i },
];

/** The last line that still has visible content, trimmed. */
export function lastNonEmptyLine(text: string): string {
  const lines = text.split('\n');
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const trimmed = lines[i].trimEnd();
    if (trimmed.trim() !== '') {
      return trimmed;
    }
  }
  return '';
}

/**
 * Classify the tail of a rendered screen as a blocking prompt, or null. The
 * input should be the RENDERED screen (so a `\r`-spinner prompt is already
 * collapsed to its final line), not raw bytes.
 */
export function detectBashBlockedPrompt(renderedTail: string): BashBlockedReason | null {
  const line = lastNonEmptyLine(renderedTail);
  if (line === '') {
    return null;
  }
  for (const rule of BLOCKED_RULES) {
    if (rule.re.test(line)) {
      return rule.reason;
    }
  }
  // Generic fallback: a line that ends in a colon or question-mark prompt with
  // no trailing newline, and reads like a question (contains a '?' or a known
  // interrogative), with no shell-prompt punctuation ($ or #) ending it.
  if (/[:?]\s*$/.test(line) && /\b(enter|choose|select|type|input|which|what|how|confirm)\b/i.test(line)) {
    return 'generic-prompt';
  }
  return null;
}

export type QuiescenceInput = {
  /** Rendered screen (collapsed), used only for the blocked-tail scan. */
  renderedTail: string;
  /** Milliseconds since the last output byte was observed. */
  msSinceLastOutput: number;
  /** Quiet threshold: below this, output is still considered streaming. */
  quietThresholdMs: number;
  /** The process already exited (settles to quiescent regardless of timing). */
  finished?: boolean;
};

/**
 * The single ranking point (mirrors Orca's one `evaluateTuiIdle`): the blocked
 * tail is tier 0 and dominates, then timing decides streaming vs quiescent.
 */
export function classifyBashQuiescence(input: QuiescenceInput): BashQuiescenceVerdict {
  if (input.finished) {
    // A finished command cannot be blocked on input; the tail is just output.
    return { state: 'quiescent' };
  }
  const stillStreaming = input.msSinceLastOutput < input.quietThresholdMs;
  const blocked = detectBashBlockedPrompt(input.renderedTail);
  if (blocked) {
    // A prompt in the tail dominates even a just-arrived byte: the byte IS the
    // prompt being drawn, so surface it immediately rather than waiting out the
    // quiet window and then the deadline.
    return { state: 'blocked-on-input', reason: blocked };
  }
  return { state: stillStreaming ? 'streaming' : 'quiescent' };
}

/**
 * Stateful tracker: records output arrival times and evaluates on demand, so a
 * caller polling a live PTY need not thread timestamps through itself.
 */
export class BashQuiescenceTracker {
  private lastOutputAt: number | null = null;

  constructor(private readonly quietThresholdMs: number) {}

  /** Record that output bytes arrived at `now` (defaults to Date.now()). */
  onOutput(now: number = Date.now()): void {
    this.lastOutputAt = now;
  }

  get hasOutputClock(): boolean {
    return this.lastOutputAt !== null;
  }

  evaluate(renderedTail: string, now: number = Date.now(), finished = false): BashQuiescenceVerdict {
    // No output clock yet: nothing has streamed, so defer to the prompt scan
    // with an "infinitely quiet" window (an adopted/empty stream is not busy).
    const msSinceLastOutput =
      this.lastOutputAt === null ? Number.POSITIVE_INFINITY : now - this.lastOutputAt;
    return classifyBashQuiescence({
      renderedTail,
      msSinceLastOutput,
      quietThresholdMs: this.quietThresholdMs,
      finished,
    });
  }
}
