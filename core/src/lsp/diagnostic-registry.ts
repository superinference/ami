/**
 * LSP diagnostic registry — pending async delivery + cross-turn deduplication.
 *
 * Mirrors Claude Code's `LSPDiagnosticRegistry` (references
 * services/lsp/LSPDiagnosticRegistry.ts). This is a DIFFERENT service from the
 * baseline/diff {@link DiagnosticTracker} in `diagnostic-tracking.ts` (which
 * mirrors Claude's `diagnosticTracking.ts`): the tracker surfaces the delta a
 * single edit introduced, while THIS registry collects diagnostics pushed
 * asynchronously by language servers (`textDocument/publishDiagnostics`) and
 * delivers each distinct diagnostic to the conversation AT MOST ONCE across the
 * whole session.
 *
 * Flow (parity with Claude's AsyncHookRegistry-style pattern):
 *   1. a server publishes diagnostics → the LSP client calls
 *      {@link LSPDiagnosticRegistry.registerPendingLSPDiagnostic};
 *   2. a drain ({@link LSPDiagnosticRegistry.checkForLSPDiagnostics}, wired to the
 *      `diagnostics` tool's `only_new` mode) collects every unsent pending entry,
 *      dedupes it (within-batch AND against already-delivered keys), volume-limits
 *      it, marks the drained entries `attachmentSent`, records the delivered keys,
 *      and returns the new-this-session diagnostics;
 *   3. an edit to a file invalidates its delivered set via
 *      {@link LSPDiagnosticRegistry.clearDeliveredDiagnosticsForFile} so a fresh
 *      diagnostic for that file is shown again even if it matches a prior one.
 *
 * All dedup identity goes through the ONE canonical {@link createDiagnosticKey}
 * and all volume limiting through the ONE canonical {@link capDiagnosticVolumeReport}
 * (both in `diagnostic-tracking.ts`) — this module adds no parallel dedup/cap logic.
 */
import { randomUUID } from 'crypto';
import { log, logError } from '../logger';
import {
  capDiagnosticVolumeReport,
  createDiagnosticKey,
  normalizeDiagnosticPath,
  type DiagnosticFile,
} from './diagnostic-tracking';

/**
 * Max distinct file URIs whose delivered-diagnostic key set the registry retains
 * for cross-turn dedup. Mirrors Claude Code's `MAX_DELIVERED_FILES = 500`
 * (LSPDiagnosticRegistry.ts L46) — an LRU bound that "prevents unbounded memory
 * growth in long sessions" that touch thousands of distinct files.
 */
export const MAX_DELIVERED_FILES = 500;

/**
 * A pending diagnostic notification, exactly Claude Code's `PendingLSPDiagnostic`
 * record schema (LSPDiagnosticRegistry.ts L12-21): the originating server, its
 * diagnostic files, the receive timestamp, and whether its attachment has already
 * been delivered to the conversation.
 */
export interface PendingLSPDiagnostic {
  /** Name of the LSP server that sent the diagnostics. */
  serverName: string;
  /** Diagnostic files carried by this notification. */
  files: DiagnosticFile[];
  /** Epoch ms when the diagnostic was received. */
  timestamp: number;
  /** True once this entry's diagnostics have been delivered to the conversation. */
  attachmentSent: boolean;
}

/** Input shape for {@link LSPDiagnosticRegistry.registerPendingLSPDiagnostic}. */
export interface RegisterPendingInput {
  serverName: string;
  files: DiagnosticFile[];
}

/** A single drained delivery: the contributing server name(s) + deduped files. */
export interface DeliveredLSPDiagnostics {
  serverName: string;
  files: DiagnosticFile[];
}

/**
 * Pending-diagnostic registry + cross-turn delivered-key cache. Instance-scoped
 * (rather than Claude's module globals) so tests exercise it in isolation and a
 * session reset simply drops the instance; the process-wide consumer path uses the
 * {@link getLSPDiagnosticRegistry} singleton.
 */
export class LSPDiagnosticRegistry {
  /** UUID → pending notification (UUID keys handle rapid concurrent registrations). */
  private readonly pending = new Map<string, PendingLSPDiagnostic>();

  /**
   * normalized-path(file URI) → set of delivered diagnostic keys. A plain Map used
   * as an LRU: a re-insert deletes-then-sets the key so it becomes most-recently
   * used; once the retained count exceeds {@link maxFiles} the least-recently-used
   * entries are evicted (Map preserves insertion order). Keyed by the NORMALIZED
   * path (not the raw URI) so a `file://` URI from the server and a filesystem path
   * from an edit hook compare equal — a robustness superset over Claude's raw-URI
   * keying, which only matches when callers pass the exact same URI string.
   */
  private readonly delivered = new Map<string, Set<string>>();

  private readonly maxFiles: number;

  constructor(maxFiles: number = MAX_DELIVERED_FILES) {
    // A non-finite / non-positive cap would disable the bound (or wedge eviction);
    // clamp to at least one tracked file.
    this.maxFiles =
      Number.isFinite(maxFiles) && maxFiles >= 1 ? Math.floor(maxFiles) : MAX_DELIVERED_FILES;
  }

  /** Count of pending (undelivered) notifications — monitoring accessor. */
  getPendingLSPDiagnosticCount(): number {
    return this.pending.size;
  }

  /** Count of distinct files whose delivered-key set is retained — introspection. */
  get deliveredFileCount(): number {
    return this.delivered.size;
  }

  /**
   * Register diagnostics pushed by a server; delivered (deduped) on the next
   * {@link checkForLSPDiagnostics}. A UUID key guarantees uniqueness even under
   * rapid back-to-back registrations for the same file.
   */
  registerPendingLSPDiagnostic({ serverName, files }: RegisterPendingInput): void {
    const diagnosticId = randomUUID();
    log(
      'lsp',
      `LSP Diagnostics: registering ${files.length} diagnostic file(s) from ${serverName} (${diagnosticId})`,
      undefined,
      'verbose',
    );
    this.pending.set(diagnosticId, {
      serverName,
      files,
      timestamp: Date.now(),
      attachmentSent: false,
    });
  }

  /** Read (without recency touch) the delivered-key set for a normalized path. */
  private deliveredKeysFor(normPath: string): Set<string> {
    return this.delivered.get(normPath) ?? new Set<string>();
  }

  /** Record one delivered key under a normalized path, refreshing LRU recency + bounding memory. */
  private recordDelivered(normPath: string, key: string): void {
    const existing = this.delivered.get(normPath);
    const set = existing ?? new Set<string>();
    if (existing) this.delivered.delete(normPath);
    set.add(key);
    this.delivered.set(normPath, set);
    while (this.delivered.size > this.maxFiles) {
      const lru = this.delivered.keys().next().value;
      if (lru === undefined) break;
      this.delivered.delete(lru);
    }
  }

  /**
   * Deduplicate across all pending files: collapse within-batch duplicates (by
   * content key) AND drop diagnostics already delivered in a prior drain. Grouped
   * by normalized file path, output keeps each file's first-seen original URI.
   *
   * Fail-open (parity, L156-178): a per-diagnostic key computation that throws is
   * logged (message truncated) and the diagnostic is STILL included — information
   * is never lost to a serialization failure.
   */
  private deduplicate(allFiles: DiagnosticFile[]): DiagnosticFile[] {
    const seenByPath = new Map<string, Set<string>>();
    const order: string[] = [];
    const outByPath = new Map<string, DiagnosticFile>();

    for (const file of allFiles) {
      const normPath = normalizeDiagnosticPath(file.uri);
      let seen = seenByPath.get(normPath);
      let out = outByPath.get(normPath);
      if (!seen || !out) {
        seen = new Set<string>();
        out = { uri: file.uri, diagnostics: [] };
        seenByPath.set(normPath, seen);
        outByPath.set(normPath, out);
        order.push(normPath);
      }
      const previouslyDelivered = this.deliveredKeysFor(normPath);
      for (const diag of file.diagnostics) {
        try {
          const key = createDiagnosticKey(diag);
          if (seen.has(key) || previouslyDelivered.has(key)) continue;
          seen.add(key);
          out.diagnostics.push(diag);
        } catch (err) {
          const detail = err instanceof Error ? err.message : String(err);
          const snippet = (diag.message ?? '').slice(0, 100) || '<no message>';
          logError('lsp', `Failed to dedup diagnostic in ${file.uri}: ${detail}. Message: ${snippet}`);
          // Keep the diagnostic rather than drop it on a key failure.
          out.diagnostics.push(diag);
        }
      }
    }
    return order.map((p) => outByPath.get(p)!).filter((f) => f.diagnostics.length > 0);
  }

  /**
   * Drain every unsent pending notification: dedup (within-batch + cross-turn),
   * volume-limit, mark drained entries `attachmentSent` (then evict them), record
   * the delivered keys, and return the new-this-session diagnostics.
   *
   * Returns `[]` when there is nothing new to deliver. The whole dedup pass is
   * wrapped so a dedup failure falls back to the undeduped files rather than losing
   * diagnostics (parity, L220-227); delivered-key tracking is likewise fail-open
   * (L297-310).
   */
  checkForLSPDiagnostics(): DeliveredLSPDiagnostics[] {
    const allFiles: DiagnosticFile[] = [];
    const serverNames = new Set<string>();
    const toMark: string[] = [];
    for (const [id, entry] of this.pending) {
      if (!entry.attachmentSent) {
        allFiles.push(...entry.files);
        serverNames.add(entry.serverName);
        toMark.push(id);
      }
    }
    if (allFiles.length === 0) return [];

    let deduped: DiagnosticFile[];
    try {
      deduped = this.deduplicate(allFiles);
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      logError('lsp', `Failed to dedup LSP diagnostics: ${detail}`);
      deduped = allFiles;
    }

    // Mark drained entries sent AFTER a successful (or fallen-back) dedup, then
    // drop them — their keys live in the delivered cache for cross-turn dedup now.
    for (const id of toMark) {
      const entry = this.pending.get(id);
      if (entry) entry.attachmentSent = true;
    }
    for (const [id, entry] of this.pending) {
      if (entry.attachmentSent) this.pending.delete(id);
    }

    // Volume-limit through the ONE canonical capper (sorts severity-first so errors
    // survive, caps per-file then total, drops emptied files, reports the removed count).
    const { files: capped, removed } = capDiagnosticVolumeReport(deduped);
    if (removed > 0) {
      log('lsp', `LSP Diagnostics: volume limiting removed ${removed} diagnostic(s)`, undefined, 'verbose');
    }
    if (capped.length === 0) return [];

    // Record delivered keys (fail-open) so the next drain filters these out.
    for (const file of capped) {
      const normPath = normalizeDiagnosticPath(file.uri);
      for (const diag of file.diagnostics) {
        try {
          this.recordDelivered(normPath, createDiagnosticKey(diag));
        } catch (err) {
          const detail = err instanceof Error ? err.message : String(err);
          const snippet = (diag.message ?? '').slice(0, 100) || '<no message>';
          logError('lsp', `Failed to track delivered diagnostic in ${file.uri}: ${detail}. Message: ${snippet}`);
        }
      }
    }

    return [{ serverName: [...serverNames].join(', '), files: capped }];
  }

  /**
   * Invalidate the delivered-key set for a file (by path OR URI) so its next
   * diagnostics are shown again even if identical to previously delivered ones.
   * Called when the file is edited. Parity with `clearDeliveredDiagnosticsForFile`.
   */
  clearDeliveredDiagnosticsForFile(fileUriOrPath: string): void {
    const normPath = normalizeDiagnosticPath(fileUriOrPath);
    if (this.delivered.delete(normPath)) {
      log('lsp', `LSP Diagnostics: cleared delivered set for ${fileUriOrPath}`, undefined, 'verbose');
    }
  }

  /**
   * Clear pending diagnostics only (keeps the cross-turn delivered cache, which is
   * invalidated per-file on edit / on full reset). Parity with `clearAllLSPDiagnostics`.
   */
  clearAllLSPDiagnostics(): void {
    this.pending.clear();
  }

  /** Reset ALL state (pending + delivered). Parity with `resetAllLSPDiagnosticState`. */
  resetAllLSPDiagnosticState(): void {
    this.pending.clear();
    this.delivered.clear();
  }
}

let singleton: LSPDiagnosticRegistry | null = null;

/** Process-wide registry singleton used by the client ingestion + tool drain wiring. */
export function getLSPDiagnosticRegistry(): LSPDiagnosticRegistry {
  if (!singleton) singleton = new LSPDiagnosticRegistry();
  return singleton;
}

/** Drop the singleton (test isolation). */
export function resetLSPDiagnosticRegistry(): void {
  singleton = null;
}
