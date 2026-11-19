/**
 * bash/headless-output — the bridge that folds a bash command's control-heavy
 * stdout/stderr into the final RENDERED screen before the model sees it (Orca
 * "headless terminal emulator for the bash tool" capability).
 *
 * REGRESSION LOCK: these tests encode the two invariants the bash tool relies on.
 * If a future change breaks either, the bash tool silently regresses to leaking
 * raw ANSI soup (progress bars as hundreds of superimposed redraws) or, worse,
 * corrupts ordinary plain-log output:
 *   1. In `auto` mode, plain `\n`-delimited output is returned BYTE-FOR-BYTE
 *      (never re-rendered) and `used === false`.
 *   2. Control-heavy output (CR overwrite / cursor-motion / alt-screen) IS
 *      collapsed to the final screen and `used === true`.
 *   3. `off` is a hard kill switch (raw bytes, no OSC scan); `always` forces the
 *      fold even for plain text.
 *
 * CORE ISOLATION: imports only from ../src/tools/bash/*. No common/cli/vscode.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  BASH_HEADLESS_RENDER_ENV,
  resolveHeadlessRenderMode,
  foldBashControlOutput,
  shouldUseHeadlessRender,
} from '../src/tools/bash/headless-output';

describe('resolveHeadlessRenderMode', () => {
  it('defaults to auto when unset', () => {
    assert.equal(resolveHeadlessRenderMode({}), 'auto');
  });

  it('maps the explicit auto value', () => {
    assert.equal(resolveHeadlessRenderMode({ [BASH_HEADLESS_RENDER_ENV]: 'auto' }), 'auto');
  });

  it('maps the always synonyms', () => {
    for (const v of ['always', 'on', '1', 'true', 'ALWAYS', ' On ']) {
      assert.equal(
        resolveHeadlessRenderMode({ [BASH_HEADLESS_RENDER_ENV]: v }),
        'always',
        `expected "${v}" -> always`,
      );
    }
  });

  it('maps the off synonyms', () => {
    for (const v of ['off', '0', 'false', 'never', 'OFF', ' never ']) {
      assert.equal(
        resolveHeadlessRenderMode({ [BASH_HEADLESS_RENDER_ENV]: v }),
        'off',
        `expected "${v}" -> off`,
      );
    }
  });

  it('falls back to auto for an unrecognized value (never silently on/off)', () => {
    assert.equal(resolveHeadlessRenderMode({ [BASH_HEADLESS_RENDER_ENV]: 'garbage' }), 'auto');
    assert.equal(resolveHeadlessRenderMode({ [BASH_HEADLESS_RENDER_ENV]: '' }), 'auto');
  });

  it('reads process.env by default', () => {
    const saved = process.env[BASH_HEADLESS_RENDER_ENV];
    try {
      process.env[BASH_HEADLESS_RENDER_ENV] = 'always';
      assert.equal(resolveHeadlessRenderMode(), 'always');
      delete process.env[BASH_HEADLESS_RENDER_ENV];
      assert.equal(resolveHeadlessRenderMode(), 'auto');
    } finally {
      if (saved === undefined) delete process.env[BASH_HEADLESS_RENDER_ENV];
      else process.env[BASH_HEADLESS_RENDER_ENV] = saved;
    }
  });
});

describe('foldBashControlOutput — auto mode (heuristic-gated)', () => {
  it('REGRESSION LOCK: plain line output passes through byte-for-byte, used=false', () => {
    const stdout = 'line one\nline two\nline three\n';
    const stderr = 'warning: deprecated\n';
    const fold = foldBashControlOutput(stdout, stderr, 'auto');
    assert.equal(fold.stdout, stdout, 'plain stdout must not be re-rendered');
    assert.equal(fold.stderr, stderr, 'plain stderr must not be re-rendered');
    assert.equal(fold.used, false);
    assert.equal(fold.alternateScreen, false);
    assert.equal(fold.cwd, null);
    assert.equal(fold.title, null);
  });

  it('REGRESSION LOCK: a \\r progress bar collapses to its final line', () => {
    const raw = 'Downloading: 10%\rDownloading: 55%\rDownloading: 100%\n';
    const fold = foldBashControlOutput(raw, '', 'auto');
    assert.equal(fold.used, true);
    assert.equal(fold.stdout, 'Downloading: 100%');
    // The superimposed redraws must be gone — not a trace of the 10%/55% frames.
    assert.doesNotMatch(fold.stdout, /10%|55%/);
  });

  it('collapses a control-heavy stderr stream independently', () => {
    const err = 'fetch 1/3\rfetch 2/3\rfetch 3/3\n';
    const fold = foldBashControlOutput('plain stdout\n', err, 'auto');
    assert.equal(fold.used, true);
    assert.equal(fold.stdout, 'plain stdout\n', 'plain stdout stays raw even when stderr folds');
    assert.equal(fold.stderr, 'fetch 3/3');
  });

  it('flattens a curses alternate-screen frame and reports alternateScreen', () => {
    // Enter alt screen and draw a frame (the captured snapshot of a TUI still on
    // the alternate buffer). alternateScreen reflects the final buffer state.
    const raw = '\x1b[?1049hmenu redraw junk\rFINAL FRAME';
    const fold = foldBashControlOutput(raw, '', 'auto');
    assert.equal(fold.used, true);
    assert.equal(fold.alternateScreen, true);
  });

  it('surfaces OSC 7 cwd and OSC title from the control stream', () => {
    const raw = '\x1b]7;file://host/tmp/work\x07\x1b]0;my-title\x07progress\rdone\n';
    const fold = foldBashControlOutput(raw, '', 'auto');
    assert.equal(fold.used, true);
    assert.equal(fold.cwd, '/tmp/work');
    assert.equal(fold.title, 'my-title');
  });
});

describe('foldBashControlOutput — always / off gating', () => {
  it('always mode folds even plain text (used=true)', () => {
    const fold = foldBashControlOutput('a\nb\n', '', 'always');
    assert.equal(fold.used, true);
    assert.equal(fold.stdout, 'a\nb');
  });

  it('REGRESSION LOCK: off is a true no-op kill switch (raw bytes, no OSC scan)', () => {
    const raw = 'spin 1\rspin 2\rspin 3\n';
    const fold = foldBashControlOutput(raw, 'err\rerr2\n', 'off');
    assert.equal(fold.used, false);
    assert.equal(fold.stdout, raw, 'off must leave stdout exactly as captured');
    assert.equal(fold.stderr, 'err\rerr2\n');
    assert.equal(fold.cwd, null);
    assert.equal(fold.title, null);
    assert.equal(fold.alternateScreen, false);
  });

  it('defaults the mode arg to the resolved env mode', () => {
    const saved = process.env[BASH_HEADLESS_RENDER_ENV];
    try {
      process.env[BASH_HEADLESS_RENDER_ENV] = 'off';
      const fold = foldBashControlOutput('x\ry\n', '');
      assert.equal(fold.used, false);
      assert.equal(fold.stdout, 'x\ry\n');
    } finally {
      if (saved === undefined) delete process.env[BASH_HEADLESS_RENDER_ENV];
      else process.env[BASH_HEADLESS_RENDER_ENV] = saved;
    }
  });
});

describe('shouldUseHeadlessRender re-export', () => {
  it('is the same heuristic source of truth', () => {
    assert.equal(shouldUseHeadlessRender('plain\nlog\n'), false);
    assert.equal(shouldUseHeadlessRender('a\rb'), true);
  });
});
