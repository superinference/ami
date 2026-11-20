/**
 * Background subagent live-turn retention + phase tracking (PARITY with the reference
 * Code's DreamTask live-display state, src/tasks/DreamTask/DreamTask.ts).
 *
 * the reference keeps a bounded ring buffer of a forked agent's most recent assistant
 * turns for live display (`MAX_TURNS = 30`, `turns.slice(-(MAX_TURNS-1)).concat(turn)`)
 * and a two-value `DreamPhase` (`'starting' | 'updating'`) flipped from "started
 * but idle" to "actively mutating files" on the first file-mutating (`Edit`/`Write`)
 * tool_use. AMI tracked a sub-agent's recent TOOL trail (`recentActivities`) and a
 * coarse running/completed status, but had neither a bounded per-turn display buffer
 * nor a sub-phase distinguishing an agent that has merely started from one that is
 * already changing files. This module adds both as one cohesive, framework-agnostic
 * tracker that the subagent event loop drives and surfaces in its completion/stall
 * introspection footer.
 */

/**
 * Canonical set of file-mutating tool names (single source of truth). Shared with
 * {@link extractFilesModified} in conversation-summary.ts so the "did this agent
 * change files?" predicate is defined exactly once (no semantic duplication).
 */
export const FILE_MUTATING_TOOLS: ReadonlySet<string> = new Set([
  'file_edit',
  'file_write',
  'notebook_edit',
  'multi_edit',
]);

/**
 * Cross-framework tool-name aliases (PARITY): the reference emits `Edit`/`Write`/
 * `MultiEdit`/`NotebookEdit` tool names; AMI's canonical names are the snake_case
 * forms above. Normalizing here lets the phase flip correctly whether the event
 * stream carries AMI or the reference tool names.
 */
const CLAUDE_MUTATING_ALIASES: ReadonlySet<string> = new Set([
  'Edit',
  'Write',
  'MultiEdit',
  'NotebookEdit',
]);

/**
 * True when a tool call mutates files (so a background agent's phase should flip
 * from 'starting' to 'updating'). Accepts both AMI canonical and the reference tool
 * names. Mirrors the reference's "first Edit/Write tool_use" phase trigger.
 */
export function isFileMutatingTool(toolName: string): boolean {
  return FILE_MUTATING_TOOLS.has(toolName) || CLAUDE_MUTATING_ALIASES.has(toolName);
}

/**
 * Default live-display turn-retention cap — parity with the reference's
 * `MAX_TURNS = 30` in DreamTask.ts. Only the N most recent assistant turns are
 * kept in memory for live display of a long-running background agent.
 */
export const DEFAULT_LIVE_TURN_RETENTION = 30;

/**
 * Resolve the live-turn retention cap (PARITY env-waterfall, mirroring
 * {@link resolveTaskWatchDebounceMs}): an explicit argument wins, then the
 * `AMI_LIVE_TURN_RETENTION` knob, then the Claude-Code-compatible
 * `CLAUDE_CODE_LIVE_TURN_RETENTION`, falling back to the default. A non-positive,
 * non-integer, or unparseable value is ignored (falls through to the next source)
 * so a bad env value can never disable retention or arm a zero/negative buffer.
 */
export function resolveLiveTurnRetention(explicit?: number): number {
  const pick = (v: unknown): number | null => {
    if (v === undefined || v === null || v === '') return null;
    const n = typeof v === 'number' ? v : Number(String(v).trim());
    return Number.isInteger(n) && n > 0 ? n : null;
  };
  return (
    pick(explicit) ??
    pick(process.env.AMI_LIVE_TURN_RETENTION) ??
    pick(process.env.CLAUDE_CODE_LIVE_TURN_RETENTION) ??
    DEFAULT_LIVE_TURN_RETENTION
  );
}

/** A background agent's sub-phase (parity: the reference's `DreamPhase`). */
export const BACKGROUND_LIVE_PHASES = ['starting', 'updating'] as const;
export type BackgroundLivePhase = (typeof BACKGROUND_LIVE_PHASES)[number];

/**
 * A single retained assistant turn, with tool uses collapsed to a count (parity:
 * the reference's `DreamTurn = { text, toolUseCount }`). The prompt is never included.
 */
export interface LiveTurn {
  text: string;
  toolUseCount: number;
}

/**
 * Bounded live-turn + phase tracker for a background/forked subagent.
 *
 * Drive it from the subagent event loop: {@link recordToolUse} on every
 * `tool_use_start` and {@link commitTurn} on every `turn_complete`. It keeps only
 * the most recent {@link retention} turns (ring buffer) and flips {@link phase}
 * from 'starting' to 'updating' the first time a file-mutating tool runs.
 */
export class BackgroundLiveTurns {
  readonly retention: number;
  private _phase: BackgroundLivePhase = 'starting';
  private _turns: LiveTurn[] = [];
  private _currentToolUseCount = 0;

  constructor(retention: number = DEFAULT_LIVE_TURN_RETENTION) {
    // A caller may pass an already-resolved value; clamp defensively so an invalid
    // direct construction can never produce a zero/negative or fractional buffer.
    this.retention =
      Number.isInteger(retention) && retention > 0 ? retention : DEFAULT_LIVE_TURN_RETENTION;
  }

  get phase(): BackgroundLivePhase {
    return this._phase;
  }

  /** The retained turns, oldest first, bounded to {@link retention}. */
  get turns(): readonly LiveTurn[] {
    return this._turns;
  }

  /**
   * Record a tool_use for the turn in progress: increment the current turn's
   * tool-use count and flip the phase to 'updating' the first time a file-mutating
   * tool runs (parity: the reference flips on the first Edit/Write tool_use).
   */
  recordToolUse(toolName: string): void {
    this._currentToolUseCount++;
    if (this._phase === 'starting' && isFileMutatingTool(toolName)) {
      this._phase = 'updating';
    }
  }

  /**
   * Commit the in-progress assistant turn into the bounded ring buffer and reset
   * the per-turn tool counter. A pure no-op turn (empty text AND no tool uses) is
   * skipped entirely, mirroring the reference's `addDreamTurn` early-return so an empty
   * turn never evicts a meaningful one from the retention window.
   */
  commitTurn(text: string): void {
    const trimmedCount = this._currentToolUseCount;
    this._currentToolUseCount = 0;
    const safeText = typeof text === 'string' ? text : '';
    if (safeText === '' && trimmedCount === 0) return;
    this._turns.push({ text: safeText, toolUseCount: trimmedCount });
    if (this._turns.length > this.retention) {
      this._turns = this._turns.slice(this._turns.length - this.retention);
    }
  }

  /** The most recent retained turn, or undefined when none has been committed. */
  lastTurn(): LiveTurn | undefined {
    return this._turns[this._turns.length - 1];
  }

  /**
   * One-line introspection clause for a completion/stall footer, e.g.
   * `phase: updating, 3/30 turns retained`. Surfaces both the sub-phase and how
   * much of the bounded buffer is in use so a terminated/capped agent's live state
   * is observable instead of opaque.
   */
  describe(): string {
    return `phase: ${this._phase}, ${this._turns.length}/${this.retention} turns retained`;
  }
}
