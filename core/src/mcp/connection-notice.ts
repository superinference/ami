/**
 * MCP connection-change provenance notices.
 *
 * An MCP server can drop mid-session (its stdio child exits, a TCP connection
 * half-opens, auth expires) and later reconnect, or exhaust its reconnect budget.
 * {@link McpManager} already re-emits these as `server-state-changed` (server, state)
 * and `reconnect-failed` (server, attempts) events, but nothing surfaced them to the
 * user — so a tool the model depends on could silently go away (its calls start
 * failing) with no visible signal of WHY.
 *
 * This module turns those low-level manager events into user-facing connection-change
 * notices (`'lost'` / `'recovered'` / `'failed'`, with server attribution) and buffers
 * them so the engine can drain and yield them on its event stream exactly once each —
 * the same durable buffer→drain pattern the resource-update and background-task
 * notices use, so a drop that fired while no API call was in flight is still surfaced.
 *
 * Only USER-RELEVANT transitions are buffered (no noise on the initial connect / the
 * transient connecting/pending churn):
 *  - a server that was `ready` leaves `ready` → `'lost'` (once per drop);
 *  - a lost server returns to `ready` → `'recovered'` (once, only after a real drop);
 *  - `reconnect-failed` → `'failed'`.
 *
 * Parity: the reference's IDE/MCP connection status indicator
 * (`hooks/notifs/useIDEStatusIndicator`).
 */

import { plural } from '../utils/string';

/** The user-facing classification of an MCP connection change. */
export type McpConnectionState = 'lost' | 'recovered' | 'failed';

/** A single connection-change provenance notice (server attribution + state). */
export interface McpConnectionChange {
  /** The MCP server whose connection changed. */
  server: string;
  /** Normalized user-facing transition. */
  state: McpConnectionState;
  /** Extra context when distinguishable: the raw manager state for a `'lost'`
   *  notice, or the attempt count for a `'failed'` notice. Omitted otherwise. */
  detail?: string;
}

/**
 * Format a connection-change notice as a one-line provenance string with explicit
 * server attribution and an optional ` (detail)` suffix. Mirrors the reference's status
 * indicator line.
 */
export function formatMcpConnectionChange(change: McpConnectionChange): string {
  const head =
    change.state === 'lost'
      ? `MCP server connection lost — ${change.server}`
      : change.state === 'recovered'
        ? `MCP server reconnected — ${change.server}`
        : `MCP server reconnection failed — ${change.server}`;
  const detail = change.detail?.trim();
  return detail ? `${head} (${detail})` : head;
}

/** The minimal event-source shape this buffer attaches to — satisfied by
 *  `McpManager` (an `EventEmitter`) without importing it (keeps this a
 *  self-contained building block with no manager dependency cycle). */
export interface McpConnectionEventSource {
  on(event: 'server-state-changed', listener: (server: string, state: string) => void): unknown;
  on(event: 'reconnect-failed', listener: (server: string, attempts: number) => void): unknown;
}

/** A drainable buffer of pending connection-change notices. */
export interface McpConnectionNoticeBuffer {
  /** Return every buffered notice (in arrival order) and clear the buffer, so each
   *  change is surfaced exactly once. */
  drain(): McpConnectionChange[];
  /** Number of pending notices (primarily for tests/introspection). */
  readonly size: number;
}

/** The manager state that counts as a live, usable connection. */
const CONNECTED_STATE = 'ready';

/**
 * Attach to an MCP manager's connection events and buffer each USER-RELEVANT
 * transition as a provenance notice for later draining. Tracks the last state and
 * current lost/healthy status PER server so only genuine drops, recoveries, and
 * reconnect failures are buffered — never the initial connect or transient churn.
 */
export function createMcpConnectionNoticeBuffer(source: McpConnectionEventSource): McpConnectionNoticeBuffer {
  const pending: McpConnectionChange[] = [];
  const lastState = new Map<string, string>();
  /** Servers currently in a dropped state (so recovery fires once, and a drop is
   *  not re-buffered while already down). */
  const lost = new Set<string>();

  source.on('server-state-changed', (server: string, state: string) => {
    const prev = lastState.get(server);
    lastState.set(server, state);
    if (state === CONNECTED_STATE) {
      // A return to ready is only a user-facing RECOVERY if the server had actually
      // dropped — never on the first connect.
      if (lost.has(server)) {
        lost.delete(server);
        pending.push({ server, state: 'recovered' });
      }
      return;
    }
    // Any move OUT of ready (to a non-ready state) is a drop — but only once, and
    // only when we had actually observed it ready before.
    if (prev === CONNECTED_STATE && !lost.has(server)) {
      lost.add(server);
      pending.push({ server, state: 'lost', detail: state });
    }
  });

  source.on('reconnect-failed', (server: string, attempts: number) => {
    lost.add(server);
    pending.push({ server, state: 'failed', detail: `after ${attempts} ${plural(attempts, 'attempt')}` });
  });

  return {
    drain(): McpConnectionChange[] {
      if (pending.length === 0) return [];
      return pending.splice(0, pending.length);
    },
    get size(): number {
      return pending.length;
    },
  };
}
