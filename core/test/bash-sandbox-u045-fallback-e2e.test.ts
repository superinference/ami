import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool, _bashHooks } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// UNIT U045 (continuation) — END-TO-END through the real bash tool: when the
// host sandbox degrades to resource-limits-only while a network/filesystem
// carve-out was requested, the degradation warning must reach the command
// output so the lost protection is not silent (Claude parity: degraded-mode
// "Cannot block …" notices surfaced to the user).
//
// RED on revert: removing the `sandboxFallbackAdvisory` wiring in bash.ts drops
// the `[Warning: …]` line and the first assertion fails.
// ---------------------------------------------------------------------------

describe('bashTool – host sandbox weaker-fallback advisory (e2e)', () => {
  afterEach(() => mock.restoreAll());

  const NOTE = 'resource-limits-only — allowedDomains NOT enforced';

  it('appends the degradation warning when the sandbox fell back with a carve-out', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => true);
    // Run the raw command (do not actually namespace-wrap) so the echo really runs.
    mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string) => cmd);
    // Force the degraded state deterministically (host may actually have unshare).
    mock.method(_bashHooks, 'describeSandboxFallbackWarning', () => NOTE);

    const res = await bashTool.execute(
      { command: 'echo u045-e2e' },
      ctx({ sandbox: { mode: 'off', network: { allowedHosts: ['example.com'] } } }),
    );
    assert.equal(res.isError, false);
    assert.match(res.output, /u045-e2e/);
    assert.ok(
      res.output.includes(`[Warning: ${NOTE}]`),
      'expected the sandbox fallback advisory in the output',
    );
  });

  it('adds no advisory when the backend is not degraded (helper returns null)', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => true);
    mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string) => cmd);
    const spy = mock.method(_bashHooks, 'describeSandboxFallbackWarning', () => null);

    const res = await bashTool.execute(
      { command: 'echo u045-clean' },
      ctx({ sandbox: { mode: 'off', network: { allowedHosts: ['example.com'] } } }),
    );
    assert.equal(res.isError, false);
    assert.doesNotMatch(res.output, /\[Warning:/);
    // Discriminator: the advisory helper WAS consulted for a sandboxed command.
    assert.ok(spy.mock.callCount() >= 1);
  });

  it('never consults the advisory when the command is not sandboxed', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => false);
    const spy = mock.method(_bashHooks, 'describeSandboxFallbackWarning', () => NOTE);

    const res = await bashTool.execute(
      { command: 'echo u045-unsandboxed' },
      ctx({ sandbox: { mode: 'off', network: { allowedHosts: ['example.com'] } } }),
    );
    assert.equal(res.isError, false);
    assert.doesNotMatch(res.output, /\[Warning:/);
    assert.equal(spy.mock.callCount(), 0);
  });
});
