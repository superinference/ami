/**
 * MCP tool-polling scheduler + provenance notices.
 *
 * Besides pushing `notifications/resources/updated` (handled by
 * {@link ./resource-update-notice}), an MCP integration can surface a *second*
 * inbound update kind to the model: a periodically re-invoked ("polled") tool whose
 * output has changed since the last poll. Claude Code renders this through the same
 * `UserResourceUpdateMessage` component as a distinct
 * `<mcp-polling-update type="tool" server="…" tool="…"><reason>…</reason>` block.
 *
 * AMI had no polling concept at all. This module adds it as a self-contained core
 * building block:
 *  - {@link McpToolPoller} periodically calls a registered tool, serializes its
 *    result, and fires {@link McpToolPollingUpdate} only when the output *changes*
 *    from the previous poll (no spam on an unchanged tool, no emission on the first
 *    baseline poll). Timers are injectable so the scheduler is deterministically
 *    testable; the default uses an unref'd `setInterval` so it never keeps the
 *    process alive.
 *  - {@link createMcpToolPollingNoticeBuffer} buffers the manager's re-emitted
 *    `tool-polling-update` events with the same durable buffer→drain contract the
 *    resource-update notices use, so an update that fired while no API call was in
 *    flight is surfaced exactly once.
 *  - {@link formatToolPollingNotice} renders the one-line provenance string.
 *
 * Parity: Claude Code's `UserResourceUpdateMessage` polling branch
 * (`components/messages/UserResourceUpdateMessage.tsx`).
 */

/** A per-server poll registration: which tool to poll, how often, and with what
 *  arguments. The owning server name is supplied at {@link McpToolPoller.register}
 *  time (one registration call per server). */
export interface McpToolPollSpec {
  /** The bare (server-local) tool name to poll. */
  tool: string;
  /** Poll cadence in milliseconds. Non-positive / non-finite values are ignored. */
  intervalMs: number;
  /** Optional arguments passed to every poll invocation of the tool. */
  args?: Record<string, unknown>;
}

/** A detected tool-polling change (the tool's output differs from the last poll). */
export interface McpToolPollingUpdate {
  /** The MCP server the polled tool belongs to. */
  server: string;
  /** The bare tool name whose output changed. */
  tool: string;
  /** Why the update fired, when distinguishable. */
  reason?: string;
}

/** A single buffered tool-polling provenance notice. */
export interface McpToolPollingNotice {
  server: string;
  tool: string;
  reason?: string;
}

/** The reason string stamped on a change-detected polling update. */
export const TOOL_POLLING_CHANGED_REASON = 'tool output changed';

/**
 * Format a tool-polling notice as a one-line provenance string with explicit
 * server attribution and an optional ` · reason` suffix — the polling counterpart
 * of `formatResourceUpdateNotice`.
 */
export function formatToolPollingNotice(notice: McpToolPollingNotice): string {
  const head = `MCP tool polled — ${notice.server}: ${notice.tool}`;
  return notice.reason ? `${head} · ${notice.reason}` : head;
}

/** The minimal event-source shape the notice buffer attaches to — satisfied by
 *  `McpManager` (an `EventEmitter`) without importing it. */
export interface McpToolPollingEventSource {
  on(
    event: 'tool-polling-update',
    listener: (server: string, tool: string, reason?: string) => void,
  ): unknown;
}

/** A drainable buffer of pending tool-polling notices. */
export interface McpToolPollingNoticeBuffer {
  /** Return every buffered notice (in arrival order) and clear the buffer, so each
   *  update is surfaced exactly once. */
  drain(): McpToolPollingNotice[];
  /** Number of pending notices (primarily for tests/introspection). */
  readonly size: number;
}

/**
 * Attach to an MCP manager's `tool-polling-update` event and buffer each as a
 * provenance notice for later draining — the durable buffer→drain pattern the
 * resource-update notices use.
 */
export function createMcpToolPollingNoticeBuffer(
  source: McpToolPollingEventSource,
): McpToolPollingNoticeBuffer {
  const pending: McpToolPollingNotice[] = [];
  source.on('tool-polling-update', (server: string, tool: string, reason?: string) => {
    pending.push({ server, tool, ...(reason ? { reason } : {}) });
  });
  return {
    drain(): McpToolPollingNotice[] {
      if (pending.length === 0) return [];
      return pending.splice(0, pending.length);
    },
    get size(): number {
      return pending.length;
    },
  };
}

/** An opaque timer handle so the scheduler's timer source is injectable. */
export type PollTimerHandle = unknown;

/** Dependencies for {@link McpToolPoller}. */
export interface McpToolPollerDeps {
  /** Invoke an MCP tool and resolve its (opaque) result. */
  call(server: string, tool: string, args?: Record<string, unknown>): Promise<unknown>;
  /** Fired once per detected output change. */
  onUpdate(update: McpToolPollingUpdate): void;
  /** Called when a poll invocation throws (best-effort; the baseline is left
   *  unchanged so the next successful poll can still detect a change). */
  onError?(server: string, tool: string, err: unknown): void;
  /** Stable serialization used for change detection. Defaults to `JSON.stringify`
   *  with a non-serializable fallback to `String(result)`. */
  serialize?(result: unknown): string;
  /** Injectable interval timer (defaults to an unref'd `setInterval`). */
  setTimer?(fn: () => void, ms: number): PollTimerHandle;
  /** Injectable timer canceller (defaults to `clearInterval`). */
  clearTimer?(handle: PollTimerHandle): void;
}

function defaultSerialize(result: unknown): string {
  try {
    return JSON.stringify(result) ?? 'undefined';
  } catch {
    return String(result);
  }
}

function defaultSetTimer(fn: () => void, ms: number): PollTimerHandle {
  const handle = setInterval(fn, ms);
  // Never keep the process alive on account of a background poll.
  (handle as { unref?: () => void }).unref?.();
  return handle;
}

interface PollEntry {
  server: string;
  spec: McpToolPollSpec;
  timer: PollTimerHandle;
  /** Serialized previous result; undefined until the first successful poll. */
  last?: string;
  /** Guard against overlapping async polls of the same tool. */
  inFlight: boolean;
}

/**
 * A periodic MCP-tool poller. Registers one or more poll specs per server, runs
 * each on its own interval, and fires {@link McpToolPollerDeps.onUpdate} only when a
 * tool's serialized output differs from the previous poll. The first successful poll
 * of each tool establishes a baseline (no emission); subsequent changes emit.
 */
export class McpToolPoller {
  private readonly deps: Required<Pick<McpToolPollerDeps, 'call' | 'onUpdate'>> &
    McpToolPollerDeps;
  private readonly serialize: (result: unknown) => string;
  private readonly setTimer: (fn: () => void, ms: number) => PollTimerHandle;
  private readonly clearTimer: (handle: PollTimerHandle) => void;
  /** Keyed by `${server}\u0000${tool}`. */
  private readonly entries = new Map<string, PollEntry>();

  constructor(deps: McpToolPollerDeps) {
    this.deps = deps;
    this.serialize = deps.serialize ?? defaultSerialize;
    this.setTimer = deps.setTimer ?? defaultSetTimer;
    this.clearTimer = deps.clearTimer ?? ((handle: PollTimerHandle) => clearInterval(handle as ReturnType<typeof setInterval>));
  }

  private static key(server: string, tool: string): string {
    return `${server}\u0000${tool}`;
  }

  /** Number of currently scheduled (server, tool) polls. */
  get size(): number {
    return this.entries.size;
  }

  /**
   * Register a server's poll specs, replacing any previously-registered specs for
   * the SAME server (so a reconnect re-registers cleanly without duplicating
   * timers). Specs with a non-positive / non-finite interval are skipped.
   */
  register(server: string, specs: readonly McpToolPollSpec[] | undefined): void {
    this.unregister(server);
    if (!specs || specs.length === 0) return;
    for (const spec of specs) {
      if (!spec || typeof spec.tool !== 'string' || spec.tool.length === 0) continue;
      if (!Number.isFinite(spec.intervalMs) || spec.intervalMs <= 0) continue;
      const key = McpToolPoller.key(server, spec.tool);
      const entry: PollEntry = {
        server,
        spec,
        timer: this.setTimer(() => void this.poll(server, spec.tool), spec.intervalMs),
        inFlight: false,
      };
      this.entries.set(key, entry);
    }
  }

  /** Stop and drop all polls for a server. */
  unregister(server: string): void {
    for (const [key, entry] of this.entries) {
      if (entry.server === server) {
        this.clearTimer(entry.timer);
        this.entries.delete(key);
      }
    }
  }

  /** Stop and drop every scheduled poll. */
  stopAll(): void {
    for (const entry of this.entries.values()) this.clearTimer(entry.timer);
    this.entries.clear();
  }

  /**
   * Run a single poll cycle for one (server, tool): call the tool, serialize its
   * result, and fire `onUpdate` iff the output changed from the previous poll. The
   * first successful poll only establishes the baseline. Overlapping polls of the
   * same tool are skipped. Exposed (not private) so a scheduler-free consumer or a
   * test can drive one cycle deterministically.
   */
  async poll(server: string, tool: string): Promise<void> {
    const entry = this.entries.get(McpToolPoller.key(server, tool));
    if (!entry || entry.inFlight) return;
    entry.inFlight = true;
    try {
      const result = await this.deps.call(server, tool, entry.spec.args);
      const serialized = this.serialize(result);
      const hadBaseline = entry.last !== undefined;
      const changed = hadBaseline && serialized !== entry.last;
      entry.last = serialized;
      if (changed) {
        this.deps.onUpdate({ server, tool, reason: TOOL_POLLING_CHANGED_REASON });
      }
    } catch (err) {
      this.deps.onError?.(server, tool, err);
    } finally {
      entry.inFlight = false;
    }
  }
}
