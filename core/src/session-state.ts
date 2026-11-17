/**
 * Authoritative external SESSION STATE surface (pure, platform-free).
 *
 * A tri-state session machine plus the queryable external metadata that lets an
 * out-of-process consumer (cross-agent dashboard, status line, push notification)
 * answer "what is this session doing and what, if anything, does it need?" without
 * scanning the event stream.
 *
 * Parity targets (Claude Code `utils/sessionState.ts`, `entrypoints/sdk/coreSchemas.ts`):
 *  - tri-state `SessionState` ('idle' | 'running' | 'requires_action')
 *  - `RequiresActionDetails` descriptor of what a blocked session is waiting on
 *  - `SessionExternalMetadata` queryable fields (pending_action, post_turn_summary, …)
 *  - pending-action clearing logic (RFC-7396 null on the next non-blocked transition)
 *  - `SDKPostTurnSummaryMessage` structured post-turn status summary
 */

// ---------------------------------------------------------------------------
// Tri-state session machine
// ---------------------------------------------------------------------------

export const SESSION_STATES = ['idle', 'running', 'requires_action'] as const;
export type SessionState = (typeof SESSION_STATES)[number];

/** Narrow an arbitrary value to a known session state. */
export function isSessionState(v: unknown): v is SessionState {
  return typeof v === 'string' && (SESSION_STATES as readonly string[]).includes(v);
}

/**
 * Context carried with a `requires_action` transition so downstream surfaces can
 * show WHAT the session is blocked on, not merely that it is blocked. Parity:
 * Claude's `RequiresActionDetails`.
 */
export interface RequiresActionDetails {
  /** The tool awaiting a decision, e.g. `bash`, `file_edit`. */
  toolName: string;
  /** Human-readable summary, e.g. "Editing src/foo.ts", "Running npm test". */
  actionDescription: string;
  /** Id of the tool_use block awaiting action. */
  toolUseId: string;
  /** Correlating permission/request id. */
  requestId: string;
  /** Raw tool input so a frontend can parse question options / plan content
   *  without scanning the event stream. */
  input?: Record<string, unknown>;
}

/**
 * Queryable external metadata mirrored onto the session for out-of-process
 * consumers. `null` on a field is a meaningful RFC-7396 "clear this" signal.
 * Parity: Claude's `SessionExternalMetadata`.
 */
export interface SessionExternalMetadata {
  permissionMode?: string | null;
  isUltraplanMode?: boolean | null;
  model?: string | null;
  /** What a `requires_action` session is waiting on; `null` once unblocked. */
  pendingAction?: RequiresActionDetails | null;
  /** Structured end-of-turn summary (see {@link PostTurnStatusSummary}). */
  postTurnSummary?: PostTurnStatusSummary | null;
  /** Mid-turn progress line surfaced before the post-turn summary arrives;
   *  `null` at idle so the next turn does not show stale progress. */
  taskSummary?: string | null;
}

/**
 * A pure session-state machine. Each {@link transition} returns the metadata
 * patch a caller should mirror onto the session (and emit to listeners), applying
 * the same clearing rules Claude uses: entering `requires_action` with details sets
 * `pendingAction`; leaving it clears `pendingAction` to `null`; reaching `idle`
 * clears `taskSummary` to `null`. A patch is only returned when something actually
 * changed, so a no-op transition yields `null`.
 */
export class SessionStateMachine {
  private _state: SessionState = 'idle';
  private _hasPendingAction = false;

  get state(): SessionState {
    return this._state;
  }

  /**
   * Move to `next`. Returns the metadata patch to apply/emit, or `null` when the
   * transition produced no metadata change.
   */
  transition(next: SessionState, details?: RequiresActionDetails): SessionExternalMetadata | null {
    this._state = next;
    const patch: SessionExternalMetadata = {};

    if (next === 'requires_action' && details) {
      this._hasPendingAction = true;
      patch.pendingAction = details;
    } else if (this._hasPendingAction) {
      this._hasPendingAction = false;
      patch.pendingAction = null;
    }

    if (next === 'idle') {
      patch.taskSummary = null;
    }

    return Object.keys(patch).length > 0 ? patch : null;
  }

  /** Reset to the initial idle state (e.g. on a `/clear`). */
  reset(): void {
    this._state = 'idle';
    this._hasPendingAction = false;
  }
}

// ---------------------------------------------------------------------------
// Structured post-turn status summary (parity: SDKPostTurnSummaryMessageSchema)
// ---------------------------------------------------------------------------

export const STATUS_CATEGORIES = ['blocked', 'waiting', 'completed', 'review_ready', 'failed'] as const;
export type StatusCategory = (typeof STATUS_CATEGORIES)[number];

/** Narrow an arbitrary value to a known status category. */
export function isStatusCategory(v: unknown): v is StatusCategory {
  return typeof v === 'string' && (STATUS_CATEGORIES as readonly string[]).includes(v);
}

/**
 * Structured "what just happened and what it needs" summary emitted after an
 * assistant turn, so a host/dashboard can show status without parsing the
 * transcript. Parity: Claude's `SDKPostTurnSummaryMessage`.
 */
export interface PostTurnStatusSummary {
  /** Uuid of the assistant message this summarizes. */
  summarizesUuid: string;
  statusCategory: StatusCategory;
  /** Short free-text qualifier for the category. */
  statusDetail: string;
  isNoteworthy: boolean;
  title: string;
  description: string;
  /** What the agent just did. */
  recentAction: string;
  /** What is needed next (empty when nothing is required). */
  needsAction: string;
  /** URLs of artifacts produced this turn (PRs, files, previews). */
  artifactUrls: string[];
}

/**
 * Validate a candidate post-turn summary. Returns the list of problems (empty when
 * valid) so a caller can reject a malformed emit rather than publishing partial
 * state to an external consumer.
 */
export function validatePostTurnStatus(summary: Partial<PostTurnStatusSummary> | null | undefined): string[] {
  const errors: string[] = [];
  if (!summary || typeof summary !== 'object') {
    return ['summary must be an object'];
  }
  if (typeof summary.summarizesUuid !== 'string' || summary.summarizesUuid.length === 0) {
    errors.push('summarizesUuid is required');
  }
  if (!isStatusCategory(summary.statusCategory)) {
    errors.push(`statusCategory must be one of: ${STATUS_CATEGORIES.join(', ')}`);
  }
  if (typeof summary.title !== 'string' || summary.title.length === 0) {
    errors.push('title is required');
  }
  if (summary.isNoteworthy !== undefined && typeof summary.isNoteworthy !== 'boolean') {
    errors.push('isNoteworthy must be a boolean');
  }
  if (summary.artifactUrls !== undefined) {
    if (!Array.isArray(summary.artifactUrls) || summary.artifactUrls.some((u) => typeof u !== 'string')) {
      errors.push('artifactUrls must be an array of strings');
    }
  }
  return errors;
}
