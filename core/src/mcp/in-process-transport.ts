/**
 * in-process-transport.ts — a bidirectional, in-process MCP transport pair.
 *
 * Parity with the reference's `services/mcp/InProcessTransport.ts` (the linked
 * client/server transport pair) and `SdkControlTransport.ts` (in-process / SDK
 * control transport): two linked {@link InProcessTransport} objects where `send()`
 * on one delivers to the other's `onmessage`, so an MCP server and client can run
 * in the SAME process with zero subprocess/socket overhead. This enables lightweight
 * in-process MCP hosting and subprocess-free deterministic testing of the MCP wire.
 *
 * Three algorithmic guarantees mirror the reference exactly:
 *   1. `send()` delivers via `queueMicrotask` (NOT synchronously), deliberately
 *      breaking the call stack so a synchronous request→response round-trip between
 *      the two linked ends never overflows stack depth (InProcessTransport.ts:L32-34).
 *   2. `close()` propagates SYMMETRICALLY and exactly-once: it marks itself closed,
 *      fires its own `onclose`, then flips the peer's closed flag and fires the
 *      peer's `onclose` — but only if the peer had not already closed, so closing
 *      either end tears both down once (InProcessTransport.ts:L37-48).
 *   3. `send()` after close throws `'Transport is closed'`, a closed-state guard
 *      (InProcessTransport.ts:L27-29, SdkControlTransport.ts:L75-77/L121-123).
 */

/**
 * The minimal transport surface an {@link McpClient} drives for an in-process MCP
 * server: lifecycle (`start`/`close`), outbound `send`, and inbound callbacks
 * (`onmessage`/`onclose`/`onerror`). Intentionally a structural subset of the MCP
 * SDK `Transport` interface so a linked pair can bridge a client and a server with
 * no process boundary. `closed` lets a caller guard a send without catching a throw.
 */
export interface InProcessTransport {
  /** Delivered a message sent by the peer end. */
  onmessage?: (message: unknown) => void;
  /** Fired exactly once when either end closes the pair. */
  onclose?: () => void;
  /** Fired when a delivery callback throws (never crashes the microtask). */
  onerror?: (error: Error) => void;
  /** Whether this end has been closed. */
  readonly closed: boolean;
  /** Begin accepting messages. A no-op for the in-process pair (no handshake). */
  start(): Promise<void>;
  /** Deliver `message` to the peer's `onmessage` on a fresh microtask. Throws when closed. */
  send(message: unknown): Promise<void>;
  /** Close this end and (exactly once) the peer. Idempotent. */
  close(): Promise<void>;
}

class LinkedTransport implements InProcessTransport {
  onmessage?: (message: unknown) => void;
  onclose?: () => void;
  onerror?: (error: Error) => void;
  private _closed = false;
  /** The linked peer; set by {@link createLinkedTransportPair} once both exist. */
  peer!: LinkedTransport;

  get closed(): boolean {
    return this._closed;
  }

  async start(): Promise<void> {
    // No handshake for an in-process pair — the peers are already linked.
  }

  async send(message: unknown): Promise<void> {
    if (this._closed) {
      throw new Error('Transport is closed');
    }
    // Break the call stack (queueMicrotask) so a synchronous request→response cycle
    // across the two ends cannot overflow the stack (reference L32-34).
    queueMicrotask(() => {
      // The peer may have closed between send() and this microtask — drop silently.
      if (this.peer._closed) return;
      try {
        this.peer.onmessage?.(message);
      } catch (err) {
        this.peer.onerror?.(err instanceof Error ? err : new Error(String(err)));
      }
    });
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    this.onclose?.();
    // Symmetric teardown: flip the peer closed + fire its onclose exactly once.
    if (!this.peer._closed) {
      this.peer._closed = true;
      this.peer.onclose?.();
    }
  }
}

/**
 * Create a linked pair of in-process transports. A `send()` on either end is
 * delivered (on a fresh microtask) to the OTHER end's `onmessage`; closing either
 * end tears both down exactly once. Returns `[a, b]`; conventionally `a` is the
 * client end and `b` the server end, but the pair is symmetric.
 */
export function createLinkedTransportPair(): [InProcessTransport, InProcessTransport] {
  const a = new LinkedTransport();
  const b = new LinkedTransport();
  a.peer = b;
  b.peer = a;
  return [a, b];
}
