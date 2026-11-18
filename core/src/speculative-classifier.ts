/**
 * Speculative bash-command safety classifier — the interactive ASK-path
 * grace-race optimization.
 *
 * PARITY: Claude Code's speculative classifier
 * (`tools/BashTool/bashPermissions.ts` → `peekSpeculativeClassifierCheck` /
 * `consumeSpeculativeClassifierCheck`, raced against a short grace window in
 * `hooks/useCanUseTool.tsx` L127-157).
 *
 * The mechanism: the moment a bash command needs a permission decision the engine
 * can PRIME an async classifier check for it (e.g. a model-backed "does this match
 * a known-safe prompt rule?" call). That check runs in the background while the
 * deterministic permission engine resolves. If the deterministic path would
 * otherwise PROMPT the user, the engine PEEKs the primed check and races it against
 * a short grace window:
 *
 *  - a HIGH-confidence match that resolves within the grace period auto-approves
 *    the command (the primed result is CONSUMED) and NO prompt is shown;
 *  - a miss, a low-confidence result, a timeout, or a thrown classifier error all
 *    fall through to the normal interactive prompt.
 *
 * It can therefore only ever turn a would-be PROMPT into an ALLOW — it never denies
 * and never loosens a deny/hardline (those resolve long before the ASK path is
 * reached), so it strictly reduces friction. A deterministic `allow`/`deny` is
 * never affected: the engine only consults it on the `ask` branch.
 *
 * Architecturally this module owns the pure primitives (cache + grace race +
 * decision). The engine (common layer) owns the orchestration: it calls
 * {@link SpeculativeClassifierCache.prime} when a bash command arrives and
 * {@link SpeculativeClassifierCache.resolveApproval} right before showing a prompt
 * — mirroring how Claude performs the race in the UI/engine layer rather than
 * inside the pure permission check.
 */

/** Confidence a speculative classifier attaches to its match verdict. Only a
 *  `high`-confidence match is allowed to auto-approve a would-be prompt. */
export type SpeculativeConfidence = 'high' | 'low';

/** A speculative classifier's verdict on a single bash command. */
export interface SpeculativeClassifierResult {
  /** Whether the command matched a known-safe prompt/allow rule. */
  matches: boolean;
  /** How confident the classifier is in {@link matches}. */
  confidence: SpeculativeConfidence;
  /** The human-readable rule/description that matched (shown as the allow reason). */
  matchedDescription?: string;
}

/**
 * The injectable async check a {@link SpeculativeClassifierCache} runs per command.
 * The engine supplies a concrete (e.g. model-backed) implementation; it must
 * resolve to a {@link SpeculativeClassifierResult}. A rejection is treated as a
 * miss (the command falls through to the prompt).
 */
export type SpeculativeBashClassifier = (
  command: string,
) => Promise<SpeculativeClassifierResult>;

/** Default grace window (ms) the engine waits for a primed check before prompting.
 *  Short by design: the user should never perceive a stall before the prompt. */
export const DEFAULT_SPECULATIVE_GRACE_MS = 200;

/** Default time (ms) a primed-but-unconsumed check stays cached before it is
 *  purged (so a command the model proposed but never ran cannot auto-approve a
 *  stale, much-later invocation of the same text). */
export const DEFAULT_SPECULATIVE_TTL_MS = 30_000;

/** True only for a definitive high-confidence match — the sole shape allowed to
 *  auto-approve a would-be prompt. Mirrors Claude's
 *  `result.matches && result.confidence === "high"` gate. */
export function isHighConfidenceMatch(
  result: SpeculativeClassifierResult | undefined | null,
): boolean {
  return !!result && result.matches === true && result.confidence === 'high';
}

/** Outcome of {@link raceWithGracePeriod}: the awaited value, or a timeout. */
export type GraceRaceOutcome<T> =
  | { type: 'result'; result: T }
  | { type: 'timeout' };

/**
 * Race `promise` against a `graceMs` timer. Resolves `{ type: 'result', result }`
 * if the promise settles first, or `{ type: 'timeout' }` if the grace window
 * elapses first. The timer is always cleared (never leaks) and a rejection of
 * `promise` propagates to the caller (the cache treats that as a miss). Mirrors
 * the `Promise.race([check, graceTimer])` in Claude's `useCanUseTool`.
 */
export async function raceWithGracePeriod<T>(
  promise: Promise<T>,
  graceMs: number,
): Promise<GraceRaceOutcome<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<GraceRaceOutcome<T>>((resolve) => {
    timer = setTimeout(() => resolve({ type: 'timeout' }), Math.max(0, graceMs));
  });
  try {
    return await Promise.race([
      promise.then((result): GraceRaceOutcome<T> => ({ type: 'result', result })),
      timeout,
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

interface CacheEntry {
  promise: Promise<SpeculativeClassifierResult>;
  expiresAt: number;
}

/** Construction options for a {@link SpeculativeClassifierCache}. */
export interface SpeculativeClassifierCacheOptions {
  /** The async check run per primed command. */
  classifier: SpeculativeBashClassifier;
  /** How long a primed-but-unconsumed entry survives (default
   *  {@link DEFAULT_SPECULATIVE_TTL_MS}). */
  ttlMs?: number;
  /** Injectable clock (ms) — for deterministic tests. Defaults to `Date.now`. */
  now?: () => number;
}

/**
 * A per-command cache of in-flight speculative classifier checks. The engine
 * {@link prime}s a command the moment it needs a decision (dedup: re-priming a
 * live entry is a no-op), then {@link resolveApproval}s it right before prompting.
 * Entries expire after a TTL so a stale primed result can never auto-approve a
 * much-later re-run of the same command text. Mirrors Claude's module-level
 * speculative-check map (`bashPermissions.ts`).
 */
export class SpeculativeClassifierCache {
  private readonly classifier: SpeculativeBashClassifier;
  private readonly ttlMs: number;
  private readonly now: () => number;
  private readonly entries = new Map<string, CacheEntry>();

  constructor(options: SpeculativeClassifierCacheOptions) {
    this.classifier = options.classifier;
    this.ttlMs = options.ttlMs ?? DEFAULT_SPECULATIVE_TTL_MS;
    this.now = options.now ?? Date.now;
  }

  /**
   * Start (or reuse) a speculative check for `command`. Re-priming a command whose
   * check is still live is a no-op (one in-flight check per command). A detached
   * no-op `.catch` is attached so a classifier rejection can never surface as an
   * unhandled promise rejection while the entry sits in the cache.
   */
  prime(command: string): void {
    this.purgeExpired();
    if (this.entries.has(command)) return;
    const promise = this.classifier(command);
    promise.catch(() => { /* swallowed here; resolveApproval re-reads + handles it */ });
    this.entries.set(command, { promise, expiresAt: this.now() + this.ttlMs });
  }

  /** The live primed check for `command`, or `undefined` if none (or expired). */
  peek(command: string): Promise<SpeculativeClassifierResult> | undefined {
    this.purgeExpired();
    return this.entries.get(command)?.promise;
  }

  /** Drop the cached check for `command` (called after it auto-approves). */
  consume(command: string): void {
    this.entries.delete(command);
  }

  /** Drop every cached check (e.g. on session reset). */
  clear(): void {
    this.entries.clear();
  }

  /** Number of live (non-expired) cached checks. */
  size(): number {
    this.purgeExpired();
    return this.entries.size;
  }

  /**
   * Peek the primed check for `command`, race it against `graceMs`, and return the
   * matched rule description (auto-approve) ONLY when a high-confidence match
   * settles within the grace window — consuming the entry so it cannot approve a
   * later invocation. Returns `null` (fall through to the prompt) when there is no
   * primed check, the grace window elapses first, the result is not a
   * high-confidence match, or the classifier rejected. An empty-string description
   * is preserved (a match with no description still auto-approves).
   */
  async resolveApproval(
    command: string,
    graceMs: number = DEFAULT_SPECULATIVE_GRACE_MS,
  ): Promise<string | null> {
    const pending = this.peek(command);
    if (!pending) return null;
    let outcome: GraceRaceOutcome<SpeculativeClassifierResult>;
    try {
      outcome = await raceWithGracePeriod(pending, graceMs);
    } catch {
      // Classifier rejected within the grace window — treat as a miss.
      return null;
    }
    if (outcome.type !== 'result') return null;
    if (!isHighConfidenceMatch(outcome.result)) return null;
    this.consume(command);
    return outcome.result.matchedDescription ?? '';
  }

  /** Drop entries whose TTL has elapsed. */
  private purgeExpired(): void {
    const t = this.now();
    for (const [command, entry] of this.entries) {
      if (entry.expiresAt <= t) this.entries.delete(command);
    }
  }
}
