/**
 * Speculative bash-command safety classifier — the interactive ASK-path
 * grace-race optimization.
 *
 * PARITY: the reference's speculative classifier
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
 * — mirroring how the reference performs the race in the UI/engine layer rather than
 * inside the pure permission check.
 */

/** Confidence a speculative classifier attaches to its match verdict. Only a
 *  `high`-confidence match is allowed to auto-approve a would-be prompt. */
export const SPECULATIVE_CONFIDENCES = ['high', 'low'] as const;
export type SpeculativeConfidence = (typeof SPECULATIVE_CONFIDENCES)[number];

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
 *  auto-approve a would-be prompt. Mirrors the reference's
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
 * the `Promise.race([check, graceTimer])` in the reference's `useCanUseTool`.
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
 * much-later re-run of the same command text. Mirrors the reference's module-level
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

  /**
   * The WHILE-OPEN variant of {@link resolveApproval}: a primed check that MISSED
   * the pre-prompt grace window keeps running in the background while the permission
   * dialog is on screen. This awaits it to COMPLETION (no grace cutoff — the prompt
   * is already shown, so the goal is to dismiss it the moment the check confirms
   * safety, not to avoid a perceived stall) and auto-approves the OPEN prompt ONLY
   * when (a) the result is a high-confidence match AND (b) the {@link latch} still
   * permits an auto-resolution — i.e. the user has not interacted with the dialog
   * and nothing else (a manual answer, abort, or auto-dismiss) has resolved it.
   *
   * On a win it claims the latch (resolve-once), consumes the entry, and returns the
   * matched description (empty string for a match with no description) so the caller
   * can tear down the dialog and allow the call. Returns `null` — leaving the prompt
   * for the user — when there is no primed check, the classifier missed / was
   * low-confidence / rejected, the user latched the dialog open, or the prompt was
   * already resolved. Mirrors the reference's `executeAsyncClassifierCheck` `onAllow`
   * (`interactiveHandler.ts`): `shouldContinue: () => !isResolved() && !userInteracted`.
   */
  async resolveWhileOpen(
    command: string,
    latch: PendingPromptLatch,
  ): Promise<string | null> {
    const pending = this.peek(command);
    if (!pending) return null;
    // Cheap pre-await guard: if the user already latched the dialog open (or it was
    // resolved) there is nothing to race for.
    if (!latch.canAutoResolve()) return null;
    let result: SpeculativeClassifierResult;
    try {
      result = await pending;
    } catch {
      // Classifier rejected — treat as a miss, leave the prompt for the user.
      return null;
    }
    // Re-check AFTER the await: the user may have interacted, or the prompt may have
    // been answered/aborted, while the classifier was still running.
    if (!latch.canAutoResolve()) return null;
    if (!isHighConfidenceMatch(result)) return null;
    const description = result.matchedDescription ?? '';
    // claimAutoApprove is the resolve-once gate: it can still lose to a
    // simultaneous manual answer that flipped `resolved` between the guard above
    // and here, in which case we must NOT consume/approve.
    if (!latch.claimAutoApprove(description)) return null;
    this.consume(command);
    return description;
  }

  /** Drop entries whose TTL has elapsed. */
  private purgeExpired(): void {
    const t = this.now();
    for (const [command, entry] of this.entries) {
      if (entry.expiresAt <= t) this.entries.delete(command);
    }
  }
}

/** Default grace window (ms) during which keypresses do NOT latch an open prompt,
 *  so an accidental keystroke the instant the dialog appears cannot cancel an
 *  in-flight auto-approval. Mirrors the reference's `GRACE_PERIOD_MS` (interactiveHandler). */
export const DEFAULT_PROMPT_INTERACTION_GRACE_MS = 200;

/** Construction options for a {@link PendingPromptLatch}. */
export interface PendingPromptLatchOptions {
  /** Grace window (ms) before a user interaction latches the prompt open
   *  (default {@link DEFAULT_PROMPT_INTERACTION_GRACE_MS}). */
  gracePeriodMs?: number;
  /** Injectable clock (ms) — for deterministic tests. Defaults to `Date.now`. */
  now?: () => number;
}

/**
 * Resolve-once + user-interaction latch for an interactive permission dialog whose
 * primed speculative check is still racing in the background WHILE the dialog is on
 * screen (see {@link SpeculativeClassifierCache.resolveWhileOpen}).
 *
 * PARITY: the reference's `interactiveHandler.ts` — the `createResolveOnce` guard plus
 * the `userInteracted` flag and `GRACE_PERIOD_MS` in `onUserInteraction`. Three
 * actors race to resolve one open dialog:
 *
 *  1. the BACKGROUND classifier (via {@link claimAutoApprove}) — may auto-approve and
 *     dismiss the dialog, but ONLY while {@link canAutoResolve} (no interaction, not
 *     yet resolved);
 *  2. the USER (via {@link claimManualResolve}) — a pressed answer, which also
 *     {@link onUserInteraction}-latches the dialog so an in-flight classifier can no
 *     longer race-dismiss a prompt the user is actively engaging with;
 *  3. a terminal teardown — abort / auto-dismiss (also via {@link claimManualResolve}).
 *
 * Exactly one claim wins (resolve-once). The three `classifier*` getters mirror the
 * fields the reference's `ToolUseConfirm` carries (`classifierCheckInProgress`,
 * `classifierAutoApproved`, `classifierMatchedRule`) so a UI can dim the options and
 * show the auto-approved ✓ transition. This is a pure state machine with an
 * injectable clock — no timers, no I/O — so the race is fully unit-testable.
 */
export class PendingPromptLatch {
  private readonly graceMs: number;
  private readonly clock: () => number;
  private readonly startedAt: number;
  private _userInteracted = false;
  private _resolved = false;
  private _classifierCheckInProgress: boolean;
  private _classifierAutoApproved = false;
  private _classifierMatchedRule: string | undefined;

  constructor(options: PendingPromptLatchOptions = {}) {
    this.graceMs = Math.max(0, options.gracePeriodMs ?? DEFAULT_PROMPT_INTERACTION_GRACE_MS);
    this.clock = options.now ?? Date.now;
    this.startedAt = this.clock();
    // A dialog opens with its background check still running (the pre-prompt grace
    // window already elapsed without a decision, else no prompt would be shown).
    this._classifierCheckInProgress = true;
  }

  /** Whether the user has latched the dialog open by interacting with it. */
  get userInteracted(): boolean { return this._userInteracted; }
  /** Whether the dialog has been resolved by any actor (resolve-once latch). */
  get resolved(): boolean { return this._resolved; }
  /** Whether a background classifier check is still eligible to auto-approve. */
  get classifierCheckInProgress(): boolean { return this._classifierCheckInProgress; }
  /** Whether the dialog was auto-approved by the background classifier. */
  get classifierAutoApproved(): boolean { return this._classifierAutoApproved; }
  /** The rule/description the auto-approval matched (for the ✓ transition line). */
  get classifierMatchedRule(): string | undefined { return this._classifierMatchedRule; }

  /**
   * Record a user interaction (arrow keys / tab / typing). Ignored within the grace
   * period so an accidental keypress the instant the dialog renders cannot cancel the
   * in-flight auto-approval (the reference's `GRACE_PERIOD_MS`). Past the grace period it
   * LATCHES the dialog open — a background auto-approval can no longer race-dismiss it
   * — and clears the "classifier running" indicator. A no-op once the dialog is
   * already resolved or already latched. Returns true iff this call latched.
   */
  onUserInteraction(): boolean {
    if (this._resolved || this._userInteracted) return false;
    if (this.clock() - this.startedAt < this.graceMs) return false;
    this._userInteracted = true;
    this._classifierCheckInProgress = false;
    return true;
  }

  /** The background classifier may still auto-resolve: not resolved AND not latched. */
  canAutoResolve(): boolean {
    return !this._resolved && !this._userInteracted;
  }

  /**
   * Claim the dialog for a background-classifier auto-approval. Succeeds (resolve-once)
   * only while {@link canAutoResolve}; records the auto-approved transition + matched
   * rule and clears the in-progress indicator. Returns false if the user already
   * interacted or the dialog was already resolved (the classifier lost the race).
   */
  claimAutoApprove(matchedRule?: string): boolean {
    if (!this.canAutoResolve()) return false;
    this._resolved = true;
    this._classifierAutoApproved = true;
    this._classifierMatchedRule = matchedRule;
    this._classifierCheckInProgress = false;
    return true;
  }

  /**
   * Claim the dialog for a terminal NON-classifier resolution — the user answered, the
   * turn aborted, or an auto-dismiss fired. Resolve-once; also latches out any pending
   * auto-approval and clears the in-progress indicator. Returns false if the dialog was
   * already resolved (e.g. the classifier auto-approved first), so the caller knows its
   * answer lost the race and must not double-resolve.
   */
  claimManualResolve(): boolean {
    if (this._resolved) return false;
    this._resolved = true;
    this._userInteracted = true;
    this._classifierCheckInProgress = false;
    return true;
  }
}
