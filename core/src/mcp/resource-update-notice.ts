/**
 * MCP resource-update provenance notices.
 *
 * An MCP server can push `notifications/resources/updated` (a subscribed resource
 * changed) and `notifications/resources/list_changed` (the available-resource set
 * changed). {@link McpClient} re-emits these as `resource-updated` / `resources-changed`
 * and {@link McpManager} re-emits them with the originating server name, but nothing
 * surfaced them to the user — so a server could silently invalidate/mutate cached
 * resource content the model then acts on, with no audit trail of WHICH server
 * changed WHAT.
 *
 * This module turns those low-level manager events into user-facing provenance
 * notices (server attribution + target + optional reason) and buffers them so the
 * engine can drain and yield them on its event stream exactly once each — the same
 * durable buffer→drain pattern the background-task completion notices use, so an
 * update that fired while no API call was in flight is still surfaced, never lost.
 *
 * Parity: Claude Code's `UserResourceUpdateMessage` (server-attributed resource
 * refresh line with an optional reason).
 */

/** A single resource-update provenance notice (server attribution + target). */
export interface McpResourceUpdateNotice {
  /** The MCP server that pushed the update. */
  server: string;
  /** The changed resource URI. Empty string for a whole-list change. */
  uri: string;
  /** Why the notice fired, when distinguishable (`'list_changed'` for a
   *  resources-list change). Omitted for a plain single-resource update. */
  reason?: string;
}

/**
 * Format a resource-update notice as a one-line provenance string with explicit
 * server attribution and an optional ` · reason` suffix. Mirrors Claude's
 * refresh-arrow line. A whole-list change (empty `uri`) renders the server plus
 * the reason only.
 */
export function formatResourceUpdateNotice(notice: McpResourceUpdateNotice): string {
  const target = notice.uri.trim();
  const head = target.length > 0
    ? `MCP resource updated — ${notice.server}: ${target}`
    : `MCP resources changed — ${notice.server}`;
  return notice.reason ? `${head} · ${notice.reason}` : head;
}

/** The minimal event-source shape this buffer attaches to — satisfied by
 *  `McpManager` (an `EventEmitter`) without importing it (keeps this a
 *  self-contained building block with no manager dependency cycle). */
export interface McpResourceEventSource {
  on(event: 'resource-updated', listener: (server: string, uri: string) => void): unknown;
  on(event: 'resources-changed', listener: (server: string) => void): unknown;
}

/** A drainable buffer of pending resource-update notices. */
export interface McpResourceNoticeBuffer {
  /** Return every buffered notice (in arrival order) and clear the buffer, so
   *  each update is surfaced exactly once. */
  drain(): McpResourceUpdateNotice[];
  /** Number of pending notices (primarily for tests/introspection). */
  readonly size: number;
}

/**
 * Attach to an MCP manager's resource events and buffer each as a provenance
 * notice for later draining. `resource-updated` (server, uri) becomes a
 * single-resource notice; `resources-changed` (server) becomes a list-change
 * notice (`reason: 'list_changed'`, empty `uri`).
 */
export function createMcpResourceNoticeBuffer(source: McpResourceEventSource): McpResourceNoticeBuffer {
  const pending: McpResourceUpdateNotice[] = [];
  source.on('resource-updated', (server: string, uri: string) => {
    pending.push({ server, uri: typeof uri === 'string' ? uri : '' });
  });
  source.on('resources-changed', (server: string) => {
    pending.push({ server, uri: '', reason: 'list_changed' });
  });
  return {
    drain(): McpResourceUpdateNotice[] {
      if (pending.length === 0) return [];
      return pending.splice(0, pending.length);
    },
    get size(): number {
      return pending.length;
    },
  };
}
