import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  renderHeadlessTerminal,
  shouldUseHeadlessRender,
  describeBashHeadlessRender,
  HEADLESS_QUIET_THRESHOLD_MS,
} from '../src/tools/bash/headless-render';

describe('shouldUseHeadlessRender (heuristic gate)', () => {
  it('returns false for plain newline-delimited output', () => {
    assert.equal(shouldUseHeadlessRender('line 1\nline 2\nline 3\n'), false);
  });
  it('returns true for carriage-return overwrites (progress bar)', () => {
    assert.equal(shouldUseHeadlessRender('Downloading 10%\rDownloading 90%'), true);
  });
  it('does not trip on a plain \\r\\n sequence alone', () => {
    assert.equal(shouldUseHeadlessRender('a\r\nb\r\n'), false);
  });
  it('returns true for CSI cursor-motion / erase', () => {
    assert.equal(shouldUseHeadlessRender('spin\x1b[2K\x1b[1Gspin'), true);
  });
  it('returns true for alternate-screen enter', () => {
    assert.equal(shouldUseHeadlessRender('\x1b[?1049hTUI'), true);
  });
});

describe('renderHeadlessTerminal', () => {
  it('collapses a \\r progress bar to its final frame when auto-gated', () => {
    const raw = 'Progress:  0%\rProgress: 42%\rProgress: 100%\n';
    const r = renderHeadlessTerminal(raw);
    assert.equal(r.used, true);
    assert.equal(r.screen, 'Progress: 100%');
    assert.equal(r.alternateScreen, false);
  });

  it('leaves plain line output untouched (heuristic skips it) in auto mode', () => {
    const raw = 'first\nsecond\nthird\n';
    const r = renderHeadlessTerminal(raw);
    assert.equal(r.used, false);
    assert.equal(r.screen, raw);
    assert.equal(r.plainText, raw);
  });

  it('mode=always forces a render on plain output', () => {
    const raw = 'first\nsecond\n';
    const r = renderHeadlessTerminal(raw, { mode: 'always' });
    assert.equal(r.used, true);
    assert.equal(r.screen, 'first\nsecond');
  });

  it('mode=off never renders even on control-heavy output', () => {
    const raw = 'a\rb\rc';
    const r = renderHeadlessTerminal(raw, { mode: 'off' });
    assert.equal(r.used, false);
    assert.equal(r.screen, raw);
  });

  it('still scans OSC 7 cwd + OSC 2 title when the grid render is skipped', () => {
    const raw = 'plain\n\x1b]7;file:///srv/app\x07\x1b]2;my-title\x07';
    const r = renderHeadlessTerminal(raw);
    assert.equal(r.used, false);
    assert.equal(r.cwd, '/srv/app');
    assert.equal(r.title, 'my-title');
  });

  it('scans cwd/title during a rendered pass too', () => {
    const raw = 'step\rdone\x1b]7;file:///w\x07';
    const r = renderHeadlessTerminal(raw);
    assert.equal(r.used, true);
    assert.equal(r.cwd, '/w');
  });

  it('flattens a curses TUI from the alternate screen', () => {
    const raw =
      'launching\n\x1b[?1049h\x1b[2J\x1b[1;1Hmenu v1\x1b[2J\x1b[1;1Hmenu v2\x1b[?1049l';
    const r = renderHeadlessTerminal(raw, { cols: 20, rows: 4 });
    assert.equal(r.used, true);
    // Alt screen was exited, so the primary buffer (shell) is what remains.
    assert.match(r.screen, /launching/);
    assert.doesNotMatch(r.plainText, /menu v/);
  });

  it('reports blocked-on-input when the rendered quiet tail is a prompt', () => {
    const raw = 'cloning\rfetching\n[sudo] password for me:';
    const r = renderHeadlessTerminal(raw, { msSinceLastOutput: 1000 });
    assert.equal(r.quiescence.state, 'blocked-on-input');
    assert.equal(r.quiescence.reason, 'sudo-password');
  });

  it('reports streaming when output is recent', () => {
    const raw = 'tick\rtock';
    const r = renderHeadlessTerminal(raw, { msSinceLastOutput: 10 });
    assert.equal(r.quiescence.state, 'streaming');
  });

  it('reports quiescent when quiet with a non-prompt tail (not rendered)', () => {
    const raw = 'crunching numbers\n';
    const r = renderHeadlessTerminal(raw, { msSinceLastOutput: 9999 });
    assert.equal(r.used, false);
    assert.equal(r.quiescence.state, 'quiescent');
  });

  it('defaults msSinceLastOutput to infinity (quiescent) on a plain tail', () => {
    const r = renderHeadlessTerminal('all done\n');
    assert.equal(r.quiescence.state, 'quiescent');
  });

  it('honors a custom quietThresholdMs', () => {
    const raw = 'progress\rprogress2';
    const fast = renderHeadlessTerminal(raw, { msSinceLastOutput: 50, quietThresholdMs: 20 });
    assert.equal(fast.quiescence.state, 'quiescent');
  });
});

describe('describeBashHeadlessRender (introspect descriptor)', () => {
  it('reports the capability metadata', () => {
    const d = describeBashHeadlessRender();
    assert.equal(d.supported, true);
    assert.deepEqual(d.modes, ['auto', 'always', 'off']);
    assert.equal(d.defaultMode, 'auto');
    assert.equal(d.quietThresholdMs, HEADLESS_QUIET_THRESHOLD_MS);
    assert.ok(d.heuristicSignals.includes('carriage-return-overwrite'));
    assert.ok(d.sideBandMetadata.includes('osc7-cwd'));
    assert.ok(d.blockedReasons.includes('sudo-password'));
    assert.deepEqual(d.quiescenceStates, ['streaming', 'quiescent', 'blocked-on-input']);
  });
});
