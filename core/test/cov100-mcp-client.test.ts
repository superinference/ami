import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { McpClient } from '../src/mcp/client';

// ---------------------------------------------------------------------------
// Targets the best-effort `catch {}` inside McpClient.sendCancelled (the empty
// catch arm reached only when writing the `notifications/cancelled` message
// throws AFTER the request itself was written). The existing abort tests use a
// real writable stdin, so the server receives the cancel and the write
// succeeds — they never exercise the swallow-on-write-failure branch.
//
// sendCancelled is invoked from sendRequest's abort handler: when a pending
// stdio request is aborted it best-effort notifies the server, and if that
// write fails (connection already gone) the failure MUST be swallowed so the
// abort still rejects cleanly with 'MCP request aborted' and nothing escapes.
// ---------------------------------------------------------------------------

/** Make a McpClient look connected over stdio with a controllable fake stdin. */
function readyClientWithStdin(stdin: Record<string, unknown>): McpClient {
  const client = new McpClient({ command: 'echo' });
  // stdio transport (default) with sseUrl null routes sendRequestAuto -> sendRequest.
  (client as any)._state = 'ready';
  (client as any).process = { stdin };
  return client;
}

describe('McpClient.sendCancelled — best-effort swallow of cancel-write failures', () => {
  it('swallows a stdin.write() throw while sending notifications/cancelled on abort', async () => {
    const writes: string[] = [];
    let call = 0;
    // First write (the tools/call request) succeeds so the request is actually
    // in flight; the second write (notifications/cancelled) throws, exercising
    // the catch arm that must NOT propagate.
    const stdin = {
      writable: true,
      write: (data: string) => {
        call++;
        writes.push(data);
        if (call >= 2) throw new Error('EPIPE: broken pipe');
        return true;
      },
    };
    const client = readyClientWithStdin(stdin);

    const ac = new AbortController();
    const callPromise = client.callTool('slow-tool', { x: 1 }, { signal: ac.signal });

    // The initial request write happened synchronously in the Promise executor.
    assert.equal(call, 1, 'the request itself was written first');
    const firstMsg = JSON.parse(writes[0]);
    assert.equal(firstMsg.method, 'tools/call');
    assert.equal(firstMsg.params.name, 'slow-tool');

    // Abort -> abortHandler -> sendCancelled -> writeMessage -> stdin.write throws.
    // The throw is swallowed; the call still rejects with the abort error, not EPIPE.
    ac.abort();

    await assert.rejects(callPromise, /MCP request aborted/);

    // The cancel notification write WAS attempted (and threw, then swallowed).
    assert.equal(call, 2, 'a second write (notifications/cancelled) was attempted');
    const cancelMsg = JSON.parse(writes[1]);
    assert.equal(cancelMsg.method, 'notifications/cancelled');
    assert.equal(cancelMsg.params.reason, 'client aborted');
  });

  it('is a no-op (no write, no throw) when stdin is not writable', () => {
    // process?.stdin?.writable is falsy -> writeMessage is never attempted and
    // the method returns cleanly. Covers the guarded false branch.
    let wrote = false;
    const client = readyClientWithStdin({ writable: false, write: () => { wrote = true; return true; } });
    assert.doesNotThrow(() => (client as any).sendCancelled(42, 'client aborted'));
    assert.equal(wrote, false);
  });

  it('is a no-op when there is no process at all', () => {
    const client = new McpClient({ command: 'echo' });
    (client as any)._state = 'ready';
    (client as any).process = null;
    assert.doesNotThrow(() => (client as any).sendCancelled(7, 'gone'));
  });
});
