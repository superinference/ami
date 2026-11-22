/**
 * Cover the PRODUCTION default bodies on `tmuxSocketHooks` (probe / kill /
 * registerTeardown / onDebug / getuid). The main U509 suite stubs those seams
 * via `mock.method`, so the real arrows never run there — this file does not
 * mock them.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  tmuxSocketHooks,
  __resetTmuxSocketStateForTest,
} from '../src/utils/tmux-socket';

after(() => {
  __resetTmuxSocketStateForTest();
  delete process.env.AMI_SHELL_DEBUG;
});

describe('tmuxSocketHooks — production defaults', () => {
  it('probeTmuxAvailable runs real tmux -V (or returns false) without throwing', () => {
    assert.equal(typeof tmuxSocketHooks.probeTmuxAvailable(), 'boolean');
  });

  it('killAmiTmuxServerSync is best-effort on a missing socket', () => {
    assert.doesNotThrow(() =>
      tmuxSocketHooks.killAmiTmuxServerSync('/tmp/ami-tmux-nonexistent-socket-for-cov'),
    );
  });

  it('registerTeardown attaches via process.once(exit)', () => {
    const fn = () => {};
    const before = process.listenerCount('exit');
    tmuxSocketHooks.registerTeardown(fn);
    assert.equal(process.listenerCount('exit'), before + 1);
    process.removeListener('exit', fn);
  });

  it('onDebug is silent by default and writes when shell debug is on', () => {
    const chunks: string[] = [];
    const orig = process.stderr.write.bind(process.stderr);
    (process.stderr.write as unknown) = ((chunk: string | Uint8Array, ...args: unknown[]) => {
      chunks.push(String(chunk));
      return Reflect.apply(orig, process.stderr, [chunk, ...args] as never);
    }) as typeof process.stderr.write;
    try {
      delete process.env.AMI_SHELL_DEBUG;
      tmuxSocketHooks.onDebug('silent');
      assert.equal(chunks.some((c) => c.includes('[tmux-socket] silent')), false);
      process.env.AMI_SHELL_DEBUG = '1';
      tmuxSocketHooks.onDebug('visible');
      assert.ok(chunks.some((c) => c.includes('[tmux-socket] visible')));
    } finally {
      process.stderr.write = orig;
      delete process.env.AMI_SHELL_DEBUG;
    }
  });

  it('getuid returns a number', () => {
    assert.equal(typeof tmuxSocketHooks.getuid(), 'number');
  });
});
