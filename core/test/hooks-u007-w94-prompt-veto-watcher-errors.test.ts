/**
 * U007 (w94) — hook-layer parity gaps against the reference's hook system:
 *
 *  - UserPromptSubmit VETO wording (processUserInput.ts:L193-L224,
 *    utils/hooks.ts:L1936-L1939): a UserPromptSubmit hook can BLOCK a prompt
 *    (`blockingError` → "UserPromptSubmit operation blocked by hook:\n<err>",
 *    then the engine erases the input and appends "Original prompt: …") or STOP
 *    it (`preventContinuation` → "Operation stopped by hook: <reason>", or the
 *    bare "Operation stopped by hook" with no reason). These two pure formatters
 *    produce the exact user-facing strings the engine surfaces; the engine honor
 *    (short-circuit the turn) is covered by the common-layer e2e test.
 *
 *  - FileChanged / CwdChanged watcher-hook failures (fileChangedWatcher.ts:L99-L105):
 *    a throwing watcher hook fires from a fire-and-forget fs-watch / cwd-change
 *    call site, so it previously only `coreLog`-ed internally and was invisible.
 *    It now ALSO emits the visible `hookError` event every other hook failure
 *    uses, so a misconfigured watcher hook is observable instead of failing
 *    silently forever.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  HookManager,
  formatUserPromptSubmitBlockedMessage,
  formatUserPromptStoppedMessage,
  type FileChangeContext,
  type CwdChangedContext,
} from '../src/hooks';

describe('U007 w94 — UserPromptSubmit veto message formatters (unit)', () => {
  it('formatUserPromptSubmitBlockedMessage mirrors the reference wording', () => {
    assert.equal(
      formatUserPromptSubmitBlockedMessage('policy violation'),
      'UserPromptSubmit operation blocked by hook:\npolicy violation',
    );
  });

  it('formatUserPromptStoppedMessage includes a non-empty reason', () => {
    assert.equal(
      formatUserPromptStoppedMessage('rate limited'),
      'Operation stopped by hook: rate limited',
    );
  });

  it('formatUserPromptStoppedMessage falls back to the bare message with no reason', () => {
    assert.equal(formatUserPromptStoppedMessage(), 'Operation stopped by hook');
    assert.equal(formatUserPromptStoppedMessage(''), 'Operation stopped by hook');
    // Whitespace-only reason is treated as empty (trimmed), so the bare form wins.
    assert.equal(formatUserPromptStoppedMessage('   '), 'Operation stopped by hook');
  });
});

describe('U007 w94 — watcher-hook failures surface on the hookError channel (unit)', () => {
  const fileCtx: FileChangeContext = { filePath: '/tmp/x', event: 'change' };
  const cwdCtx: CwdChangedContext = { oldCwd: '/a', newCwd: '/b' };

  it('a throwing FileChanged hook emits a visible hookError (was silent)', async () => {
    const hm = new HookManager();
    const errors: Array<{ hookType: string; hook: string; error: string }> = [];
    hm.on('hookError', (e: { hookType: string; hook: string; error: string }) => errors.push(e));
    hm.onFileChanged(async () => { throw new Error('boom-file'); });

    await hm.executeFileChanged(fileCtx);

    assert.equal(errors.length, 1, 'exactly one hookError emitted');
    assert.equal(errors[0].hook, 'FileChanged hook');
    assert.equal(errors[0].hookType, 'command');
    assert.match(errors[0].error, /boom-file/);
  });

  it('a throwing CwdChanged hook emits a visible hookError', async () => {
    const hm = new HookManager();
    const errors: Array<{ hook: string; error: string }> = [];
    hm.on('hookError', (e: { hook: string; error: string }) => errors.push(e));
    hm.onCwdChanged(async () => { throw new Error('boom-cwd'); });

    await hm.executeCwdChanged(cwdCtx);

    assert.equal(errors.length, 1, 'exactly one hookError emitted');
    assert.equal(errors[0].hook, 'CwdChanged hook');
    assert.match(errors[0].error, /boom-cwd/);
  });

  it('a successful FileChanged hook emits NO hookError', async () => {
    const hm = new HookManager();
    const errors: unknown[] = [];
    hm.on('hookError', (e: unknown) => errors.push(e));
    hm.onFileChanged(async () => { /* ok */ });

    await hm.executeFileChanged(fileCtx);

    assert.equal(errors.length, 0, 'no hookError on success');
  });

  it('a no-op CwdChanged (oldCwd === newCwd) runs no hooks and emits nothing', async () => {
    const hm = new HookManager();
    const errors: unknown[] = [];
    let ran = false;
    hm.on('hookError', (e: unknown) => errors.push(e));
    hm.onCwdChanged(async () => { ran = true; throw new Error('should-not-run'); });

    await hm.executeCwdChanged({ oldCwd: '/same', newCwd: '/same' });

    assert.equal(ran, false, 'hook must not run when cwd did not change');
    assert.equal(errors.length, 0, 'no hookError for a no-op cwd change');
  });
});
