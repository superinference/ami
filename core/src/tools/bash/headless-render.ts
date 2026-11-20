/**
 * Headless render entry point for the bash tool.
 *
 * Ties the three building blocks together into the capability described by the
 * Orca gap:
 *   1. {@link HeadlessTerminalGrid} — collapse `\r` overwrites / cursor motion /
 *      erases / alt-screen into the final rendered screen + plain-text view.
 *   2. {@link scanOscCwdTitle} — free OSC 7 cwd + OSC 0/2 title scan.
 *   3. {@link classifyBashQuiescence} — quiescence / blocked-on-input verdict.
 *
 * The render is OPT-IN per call and additionally GATED by a cheap heuristic:
 * plain line-oriented output (the overwhelming common case) is left untouched so
 * the model keeps the exact bytes, and only "terminal-control-heavy" output
 * (progress bars, spinners, curses TUIs) is flattened.
 */

import { HeadlessTerminalGrid, type HeadlessGridOptions } from './headless-grid';
import { scanOscCwdTitle } from './osc7-scan';
import {
  classifyBashQuiescence,
  type BashBlockedReason,
  type BashQuiescenceState,
} from './quiescence';

/** Default quiet window before a blocked-tail scan can declare quiescence. */
export const HEADLESS_QUIET_THRESHOLD_MS = 500;

export const HEADLESS_RENDER_MODES = ['auto', 'always', 'off'] as const;
export type HeadlessRenderMode = (typeof HEADLESS_RENDER_MODES)[number];

export type HeadlessRenderOptions = HeadlessGridOptions & {
  /**
   * Gating mode:
   *  - `auto` (default): render only when the heuristic detects control-heavy
   *    output.
   *  - `always`: force the headless render regardless of heuristic.
   *  - `off`: never render (return the raw bytes as the screen/plainText).
   */
  mode?: HeadlessRenderMode;
  /** Process already exited (feeds the quiescence verdict). */
  finished?: boolean;
  /** ms since the last output byte (feeds the quiescence verdict). */
  msSinceLastOutput?: number;
  /** Quiet threshold override. */
  quietThresholdMs?: number;
};

export type HeadlessRenderResult = {
  /** Whether the headless grid render was actually applied. */
  used: boolean;
  /** The final rendered screen (control codes applied), or raw when not used. */
  screen: string;
  /** Full transcript (scrollback + screen) with control codes applied, or raw. */
  plainText: string;
  /** Last OSC 7 cwd seen, or null. */
  cwd: string | null;
  /** Last OSC 0/1/2 title seen, or null. */
  title: string | null;
  /** Whether the output used the alternate screen (a curses TUI). */
  alternateScreen: boolean;
  /** Quiescence / blocked verdict over the rendered tail. */
  quiescence: { state: BashQuiescenceState; reason?: BashBlockedReason };
};

// --- heuristic -------------------------------------------------------------

/** A carriage return NOT immediately followed by a newline (progress overwrite). */
const CR_OVERWRITE_RE = /\r(?!\n)/;
/** CSI cursor-motion / erase finals that imply in-place repainting. */
const CURSOR_CONTROL_RE = /\x1b\[[0-9;?]*[ABCDEFGHJKdfsu]/;
/** Alternate screen enter (curses TUI). */
const ALT_SCREEN_RE = /\x1b\[\?(?:47|1047|1049)h/;

/**
 * Cheap scan: does this output benefit from a headless render? True when it
 * contains carriage-return overwrites, cursor-motion/erase sequences, or enters
 * the alternate screen. Plain `\n`-delimited logs return false.
 */
export function shouldUseHeadlessRender(raw: string): boolean {
  return CR_OVERWRITE_RE.test(raw) || CURSOR_CONTROL_RE.test(raw) || ALT_SCREEN_RE.test(raw);
}

// --- render ----------------------------------------------------------------

export function renderHeadlessTerminal(
  raw: string,
  opts: HeadlessRenderOptions = {},
): HeadlessRenderResult {
  const mode: HeadlessRenderMode = opts.mode ?? 'auto';
  const quietThresholdMs = opts.quietThresholdMs ?? HEADLESS_QUIET_THRESHOLD_MS;
  const use = mode === 'always' || (mode === 'auto' && shouldUseHeadlessRender(raw));

  // The OSC scan is cheap and useful even when the grid render is skipped.
  const osc = scanOscCwdTitle(raw);

  if (!use) {
    const quiescence = classifyBashQuiescence({
      renderedTail: raw,
      msSinceLastOutput: opts.msSinceLastOutput ?? Number.POSITIVE_INFINITY,
      quietThresholdMs,
      finished: opts.finished,
    });
    return {
      used: false,
      screen: raw,
      plainText: raw,
      cwd: osc.cwd,
      title: osc.title,
      alternateScreen: false,
      quiescence,
    };
  }

  const grid = new HeadlessTerminalGrid({
    cols: opts.cols,
    rows: opts.rows,
    scrollback: opts.scrollback,
  });
  grid.write(raw);
  const screen = grid.renderScreen();
  const plainText = grid.getPlainText();
  const quiescence = classifyBashQuiescence({
    renderedTail: screen,
    msSinceLastOutput: opts.msSinceLastOutput ?? Number.POSITIVE_INFINITY,
    quietThresholdMs,
    finished: opts.finished,
  });
  return {
    used: true,
    screen,
    plainText,
    cwd: osc.cwd,
    title: osc.title,
    alternateScreen: grid.isAlternateScreen,
    quiescence,
  };
}

// --- introspection ---------------------------------------------------------

/**
 * Capability descriptor for `ami --introspect` so the headless-render feature is
 * discoverable (parity with the other bash capability descriptors in the
 * manifest). Reported-only: describes the gating, what it collapses, and the
 * blocked-reason vocabulary.
 */
export function describeBashHeadlessRender(): {
  supported: true;
  /** Process-env kill switch that selects the gating mode. */
  envVar: string;
  modes: HeadlessRenderMode[];
  defaultMode: HeadlessRenderMode;
  quietThresholdMs: number;
  heuristicSignals: string[];
  collapses: string[];
  sideBandMetadata: string[];
  blockedReasons: BashBlockedReason[];
  quiescenceStates: BashQuiescenceState[];
} {
  return {
    supported: true,
    envVar: 'AMI_BASH_HEADLESS_RENDER',
    modes: ['auto', 'always', 'off'],
    defaultMode: 'auto',
    quietThresholdMs: HEADLESS_QUIET_THRESHOLD_MS,
    heuristicSignals: [
      'carriage-return-overwrite',
      'csi-cursor-motion-or-erase',
      'alternate-screen-enter',
    ],
    collapses: [
      'progress-bar \\r overwrites',
      'spinner repaints',
      'cursor-addressed redraws',
      'curses TUI alternate-screen frames',
    ],
    sideBandMetadata: ['osc7-cwd', 'osc0/1/2-title'],
    blockedReasons: [
      'password',
      'passphrase',
      'sudo-password',
      'yes-no',
      'confirm',
      'overwrite',
      'press-key',
      'generic-prompt',
    ],
    quiescenceStates: ['streaming', 'quiescent', 'blocked-on-input'],
  };
}
