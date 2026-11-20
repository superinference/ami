// ===========================================================================
// dump-prompts.ts
//
// Per-session, deduplicated prompt-dump entry schema for the AMI_DUMP_PROMPTS
// transport capture (see provider.ts withPromptDump). Mirrors the reference's
// createDumpPromptsFetch DumpState bookkeeping (services/api/dumpPrompts.ts):
//
//   - A per-agent/per-session keyed DumpState (initialized / messageCountSeen /
//     lastInitDataHash / lastInitFingerprint) so each session's dump stays
//     deduplicated independently.
//   - A STRUCTURED entry schema — `init` (first request), `system_update`
//     (the system prompt / tool schemas / model params drifted mid-session),
//     and `message` (each NEW user message) — instead of re-dumping the whole
//     multi-MB request body every turn.
//   - A cheap `model|toolNames|systemLen` fingerprint computed BEFORE the
//     expensive stringify+sha256, so an unchanged prompt/tool set skips the
//     ~hundreds-of-ms serialization entirely and emits nothing.
//
// Pure, side-effect-free entry builders (plus a small per-key registry) kept in
// their own module so the schema is unit-testable in isolation and the fetch
// wrapper only has to call buildRequestDumpEntries().
// ===========================================================================

/** Per-session dump bookkeeping, tracked so each session dedups independently. */
export interface DumpState {
  /** Whether the first `init` entry has been emitted for this session. */
  initialized: boolean;
  /** How many messages of the conversation have already been dumped. */
  messageCountSeen: number;
  /**
   * Cheap change-detection proxy — `model|toolNames|systemLen`. Lets a turn skip
   * the expensive stringify of the (multi-MB) init blob when the prompt/tool set
   * is structurally identical to the previous call; a change flips init→system_update.
   */
  lastInitFingerprint: string;
}

/** A structured dump entry: the session's non-message init blob or one user message. */
export const DUMP_ENTRY_TYPES = ['init', 'system_update', 'message'] as const;
export type DumpEntryType = (typeof DUMP_ENTRY_TYPES)[number];

/** A fresh, uninitialized {@link DumpState}. */
export function createDumpState(): DumpState {
  return { initialized: false, messageCountSeen: 0, lastInitFingerprint: '' };
}

/**
 * Cheap fingerprint of the init-relevant parts of a request body:
 * `model | comma-joined tool names | total system length`. System may be a
 * plain string or an array of `{ text }` content blocks (Anthropic shape); both
 * are reduced to a character count. Computed before any stringify so an
 * unchanged prompt/tool set is detected without serializing the whole body.
 */
export function initFingerprint(req: Record<string, unknown>): string {
  const tools = req.tools as Array<{ name?: string }> | undefined;
  const system = req.system as unknown[] | string | undefined;
  const sysLen =
    typeof system === 'string'
      ? system.length
      : Array.isArray(system)
        ? system.reduce((n: number, b) => n + ((b as { text?: string }).text?.length ?? 0), 0)
        : 0;
  const toolNames = tools?.map((t) => t.name ?? '').join(',') ?? '';
  return `${String(req.model)}|${toolNames}|${sysLen}`;
}

/** Serialize the init-relevant part of a request body (everything except the
 *  per-turn `messages`, which are dumped separately as `message` entries). */
function serializeInitData(req: Record<string, unknown>): string {
  const initData: Record<string, unknown> = { ...req };
  delete initData.messages;
  return JSON.stringify(initData);
}

/**
 * Build the deduplicated JSONL dump lines for one outgoing request, MUTATING
 * `state` to track what has been emitted. Returns the lines to append (possibly
 * empty):
 *   - `init` the first time a session is seen,
 *   - `system_update` when the init blob (system/tools/params) changed — detected
 *     by the cheap `model|toolNames|systemLen` fingerprint, which lets an unchanged
 *     turn skip the (multi-MB) stringify entirely,
 *   - `message` for each NEW user message since the last call (assistant
 *     messages are captured via the `response` entry, so they're skipped here).
 */
export function buildRequestDumpEntries(
  state: DumpState,
  req: Record<string, unknown>,
  ts: string,
): string[] {
  const entries: string[] = [];
  const messages = (req.messages ?? []) as Array<{ role?: string }>;

  // Cheap fingerprint first: system+tools don't change between most turns, so an
  // unchanged shape short-circuits before the expensive stringify of the init blob.
  const fingerprint = initFingerprint(req);
  const tsStr = JSON.stringify(ts);
  if (!state.initialized) {
    state.initialized = true;
    state.lastInitFingerprint = fingerprint;
    entries.push(`{"type":"init","timestamp":${tsStr},"data":${serializeInitData(req)}}`);
  } else if (fingerprint !== state.lastInitFingerprint) {
    state.lastInitFingerprint = fingerprint;
    entries.push(`{"type":"system_update","timestamp":${tsStr},"data":${serializeInitData(req)}}`);
  }

  // Only NEW user messages (assistant messages are captured in the response entry).
  for (const msg of messages.slice(state.messageCountSeen)) {
    if (msg.role === 'user') {
      entries.push(JSON.stringify({ type: 'message', timestamp: ts, data: msg }));
    }
  }
  state.messageCountSeen = messages.length;

  return entries;
}

// ---------------------------------------------------------------------------
// Per-key registry — one DumpState per agent/session id.
// ---------------------------------------------------------------------------

const dumpStates = new Map<string, DumpState>();

/** Get (creating on first use) the {@link DumpState} for a session/agent key. */
export function getOrCreateDumpState(key: string): DumpState {
  let state = dumpStates.get(key);
  if (!state) {
    state = createDumpState();
    dumpStates.set(key, state);
  }
  return state;
}

/** Drop one session's dump bookkeeping (next request re-emits `init`). */
export function clearDumpState(key: string): void {
  dumpStates.delete(key);
}

/** Drop ALL sessions' dump bookkeeping. */
export function clearAllDumpState(): void {
  dumpStates.clear();
}
