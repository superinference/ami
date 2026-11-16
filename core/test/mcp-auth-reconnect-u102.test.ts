/**
 * U102 — the MCP `authenticate` pseudo-tool must drive a REAL reconnect.
 *
 * Parity bug (red-green): the tool (src/tools/mcp-auth.ts) calls
 * `mcpManager.reconnect?.(serverName)`, but `McpManager` exposed **no**
 * `reconnect` method — so against the real manager the optional call was a
 * silent no-op and the tool reported a false "already connected" success
 * without ever attempting a connection or running the OAuth consent leg. The
 * existing mcp-auth parity tests only pass because they mock a `reconnect`
 * method on `_mcpManager`; production wiring (`engine.ts` → `_mcpManager:
 * this.mcpManager`) had nothing to call.
 *
 * Fix: add a real `McpManager.reconnect(name)` that invalidates the server's
 * cached tool schemas (auth-cache invalidation + post-auth tool-swap) and then
 * re-establishes the session (restart a ready server, connect a pending/errored
 * one), propagating failures so the tool surfaces them instead of claiming
 * success.
 *
 * Each test discriminates the fix from the pre-fix behavior (fails on revert).
 */
import { describe, it, mock } from 'node:test';
import assert from 'node:assert/strict';
import { McpManager } from '../src/mcp/manager';
import { createMcpAuthTool } from '../src/tools/mcp-auth';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

describe('mcp-auth U102 — real McpManager.reconnect wiring', () => {
  it('McpManager exposes a reconnect(name) the authenticate tool depends on', () => {
    const mgr = new McpManager();
    // RED on revert: without the method `typeof` is 'undefined'.
    assert.equal(typeof mgr.reconnect, 'function');
    mgr.disconnectAll();
  });

  it('reconnect invalidates the server tool cache before re-establishing the session', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('srv', { command: '_nonexistent_cmd_u102' });
    // Spy that passes through to the real (private) cache invalidator.
    const spy = mock.method(mgr as unknown as { invalidateToolCache: (n: string) => void }, 'invalidateToolCache');
    // The bad command fails to spawn; we only assert the cache was invalidated.
    await assert.rejects(() => mgr.reconnect('srv'));
    assert.ok(spy.mock.callCount() >= 1, 'reconnect must invalidate the tool cache');
    assert.deepEqual(spy.mock.calls[0].arguments, ['srv']);
    mgr.disconnectAll();
  });

  it('the authenticate tool actually attempts a reconnect against the real manager (no silent no-op success)', async () => {
    const mgr = new McpManager();
    mgr.addPendingServer('srv', { command: '_nonexistent_cmd_u102' });
    // transport:'http' passes the OAuth-transport guard so execute() reaches the
    // reconnect leg; the underlying pending server is a (bad) stdio command.
    const tool = createMcpAuthTool('srv', { transport: 'http' });
    const r = await tool.execute({}, ctx({ _mcpManager: mgr } as Partial<ToolContext>));
    // RED on revert: with no manager.reconnect method the optional call is a
    // no-op, the server never connects, and the tool falsely reports
    // "already connected — no re-authentication was needed" (isError falsy).
    // With the fix, reconnect runs, the bad command fails to spawn, and the
    // failure surfaces as a real error result.
    assert.equal(r.isError, true);
    assert.equal(r.metadata?.status, 'error');
    assert.ok(!/already connected/i.test(r.output), 'must not falsely claim success');
    mgr.disconnectAll();
  });
});
