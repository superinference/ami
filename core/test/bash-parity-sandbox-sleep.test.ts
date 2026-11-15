import { describe, it, afterEach } from 'node:test';
import { mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool, _bashHooks, detectBlockedSleep, diagnoseError } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// dangerouslyDisableSandbox — string-serialized booleans must coerce, matching
// run_in_background. Providers often serialize JSON booleans as strings; a strict
// `=== true` would silently keep the sandbox on for the string "true".
// RED on revert: with `=== true`, the string "true" leaves disableSandbox=false,
// so wrapWithSandbox IS called and the first assertion (callCount 0) fails.
// ---------------------------------------------------------------------------

describe('bashTool – dangerouslyDisableSandbox coercion', () => {
  afterEach(() => mock.restoreAll());

  it('string "true" disables the non-container sandbox (no wrap)', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => true);
    const wrapSpy = mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string) => cmd);
    await bashTool.execute({ command: 'echo coerce-a', dangerouslyDisableSandbox: 'true' }, ctx());
    assert.equal(wrapSpy.mock.callCount(), 0, 'string "true" must disable the sandbox');
  });

  it('boolean true disables the non-container sandbox (no wrap)', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => true);
    const wrapSpy = mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string) => cmd);
    await bashTool.execute({ command: 'echo coerce-b', dangerouslyDisableSandbox: true }, ctx());
    assert.equal(wrapSpy.mock.callCount(), 0);
  });

  it('discriminator: a sandbox-eligible command WITHOUT the flag is wrapped', async () => {
    // Proves the test above actually distinguishes the fix: without the override a
    // sandbox-eligible command is wrapped exactly once. Under the old `=== true`
    // code, passing "true" would also hit this path (callCount 1) — the red arm.
    mock.method(_bashHooks, 'shouldUseSandbox', () => true);
    const wrapSpy = mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string) => cmd);
    await bashTool.execute({ command: 'echo coerce-c' }, ctx());
    assert.equal(wrapSpy.mock.callCount(), 1);
  });

  it('string "false" keeps the sandbox on', async () => {
    mock.method(_bashHooks, 'shouldUseSandbox', () => true);
    const wrapSpy = mock.method(_bashHooks, 'wrapWithSandbox', (cmd: string) => cmd);
    await bashTool.execute({ command: 'echo coerce-d', dangerouslyDisableSandbox: 'false' }, ctx());
    assert.equal(wrapSpy.mock.callCount(), 1, 'string "false" must NOT disable the sandbox');
  });
});

// ---------------------------------------------------------------------------
// Blocking-sleep guard inside compound foreground commands. The suffix/decimal
// grammar is already covered elsewhere; this locks the compound-segment scan.
// ---------------------------------------------------------------------------

describe('detectBlockedSleep – compound commands', () => {
  it('catches a blocking sleep after && / ; / ||', () => {
    assert.ok(detectBlockedSleep('echo done && sleep 600', false, false));
    assert.ok(detectBlockedSleep('build; sleep 300', false, false));
    assert.ok(detectBlockedSleep('a || sleep 2m', false, false));
  });

  it('does NOT flag a backgrounded sleep (trailing &)', () => {
    assert.equal(detectBlockedSleep('sleep 600 &', false, false), null);
  });

  it('does NOT flag a short compound sleep', () => {
    assert.equal(detectBlockedSleep('echo hi && sleep 2', false, false), null);
  });

  it('still honors run_in_background / custom timeout opt-out', () => {
    assert.equal(detectBlockedSleep('x && sleep 600', true, false), null);
    assert.equal(detectBlockedSleep('x && sleep 600', false, true), null);
  });
});

// ---------------------------------------------------------------------------
// Sandbox-restriction failure hint: an EPERM / "Operation not permitted" error
// should steer the model to retry with dangerouslyDisableSandbox.
// ---------------------------------------------------------------------------

describe('diagnoseError – sandbox restriction hint', () => {
  it('suggests dangerouslyDisableSandbox on "Operation not permitted"', () => {
    const hint = diagnoseError('mount: Operation not permitted');
    assert.ok(hint && /dangerouslyDisableSandbox/.test(hint), hint ?? 'null');
  });

  it('suggests dangerouslyDisableSandbox on an EPERM error', () => {
    const hint = diagnoseError('Error: EPERM: operation not permitted, open');
    assert.ok(hint && /sandbox/i.test(hint), hint ?? 'null');
  });
});
