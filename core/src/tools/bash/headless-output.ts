/**
 * Bridge between the headless terminal renderer and the bash tool's output
 * presentation path.
 *
 * AMI's bash tool historically returned the raw captured bytes, so a `\r`-driven
 * progress bar reached the model as hundreds of superimposed redraws ("ANSI
 * soup") and a curses TUI reached it as a scrollback of partial repaints. This
 * module folds that control-heavy output into the final RENDERED screen before
 * the model sees it — the Orca "headless terminal emulator for the bash tool"
 * capability, implemented with AMI's dependency-free {@link renderHeadlessTerminal}
 * instead of `@xterm/headless` + `SerializeAddon`.
 *
 * The fold is GATED two ways:
 *   1. A process-env kill switch (`AMI_BASH_HEADLESS_RENDER` = auto|always|off).
 *   2. Per-stream, by the cheap `shouldUseHeadlessRender` heuristic (only CR
 *      overwrites / cursor-motion CSI / alt-screen output is flattened; plain
 *      `\n`-delimited logs pass through byte-for-byte).
 *
 * REGRESSION LOCK: the whole point is that plain line-oriented output is NEVER
 * altered (`auto` mode + heuristic) and control-heavy output IS collapsed. If a
 * future change makes `foldBashControlOutput` mutate plain logs, or stop
 * collapsing a `\r` progress bar, the bash tool silently regresses to leaking
 * ANSI soup / corrupting ordinary output. `bash-headless-output.test.ts` and the
 * `bash-headless-integration` reproducer must stay green to prove it does not.
 */

import {
  renderHeadlessTerminal,
  shouldUseHeadlessRender,
  type HeadlessRenderMode,
} from './headless-render';

/** Env var that overrides the headless-render gating mode for the bash tool. */
export const BASH_HEADLESS_RENDER_ENV = 'AMI_BASH_HEADLESS_RENDER';

/**
 * Resolve the gating mode from the process env.
 *
 * - unset / `auto` / anything unrecognized → `auto` (heuristic-gated; the safe
 *   default so a bad value can never silently disable or force the feature).
 * - `always` / `on` / `1` / `true` → `always` (force the render).
 * - `off` / `0` / `false` / `never` → `off` (hard kill switch — raw bytes).
 */
export function resolveHeadlessRenderMode(
  env: NodeJS.ProcessEnv = process.env,
): HeadlessRenderMode {
  const raw = env[BASH_HEADLESS_RENDER_ENV];
  if (raw === undefined) {
    return 'auto';
  }
  switch (raw.trim().toLowerCase()) {
    case 'always':
    case 'on':
    case '1':
    case 'true':
      return 'always';
    case 'off':
    case '0':
    case 'false':
    case 'never':
      return 'off';
    default:
      // Includes the explicit `auto` and every unrecognized value.
      return 'auto';
  }
}

export type BashHeadlessFold = {
  /** stdout after the fold (identical to input when the fold did not apply). */
  stdout: string;
  /** stderr after the fold (identical to input when the fold did not apply). */
  stderr: string;
  /** True when at least one stream was actually re-rendered. */
  used: boolean;
  /** True when either stream entered the alternate screen (a curses TUI). */
  alternateScreen: boolean;
  /** Last OSC 7 cwd seen across the two streams, or null. */
  cwd: string | null;
  /** Last OSC 0/1/2 title seen across the two streams, or null. */
  title: string | null;
};

const NO_FOLD = (stdout: string, stderr: string): BashHeadlessFold => ({
  stdout,
  stderr,
  used: false,
  alternateScreen: false,
  cwd: null,
  title: null,
});

/**
 * Fold a bash command's stdout/stderr into the rendered screen when the mode and
 * heuristic allow it. Returns the (possibly unchanged) text plus the side-band
 * metadata (alt-screen, OSC 7 cwd, OSC title) the full transcript would have
 * shown a human.
 *
 * `off` is a true no-op (no render, no OSC scan) so the kill switch is cheap and
 * leaves the bytes exactly as captured.
 */
export function foldBashControlOutput(
  stdout: string,
  stderr: string,
  mode: HeadlessRenderMode = resolveHeadlessRenderMode(),
): BashHeadlessFold {
  if (mode === 'off') {
    return NO_FOLD(stdout, stderr);
  }
  const out = renderHeadlessTerminal(stdout, { mode });
  const err = renderHeadlessTerminal(stderr, { mode });
  return {
    stdout: out.plainText,
    stderr: err.plainText,
    used: out.used || err.used,
    alternateScreen: out.alternateScreen || err.alternateScreen,
    cwd: out.cwd ?? err.cwd,
    title: out.title ?? err.title,
  };
}

/**
 * Re-export of the heuristic so a caller that wants to decide WITHOUT rendering
 * (e.g. a cheap "is this control-heavy" check) shares the one source of truth.
 */
export { shouldUseHeadlessRender };
