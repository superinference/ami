// Canonical SCRATCHPAD directory resolution + containment (AGENTS.md No-Duplication
// Rule). AMI already advertises an operator-sanctioned scratchpad to the model
// (common/src/system-prompt.ts `buildScratchpadSection`, keyed on
// `SUPERINFERENCE_SCRATCHPAD_DIR`) as a dir "writable without a permission prompt".
// Until now NOTHING in the permission layer actually honored that promise: a
// scratchpad dir OUTSIDE the launch workspace hit the normal out-of-workspace
// prompt/deny on read and an ask on write. This module is the single source of
// truth for the scratchpad env knob + path resolution + containment so the system
// prompt and the permission carve-out agree on exactly one directory.
//
// Parity: Claude Code's `isScratchpadEnabled` / `getScratchpadDir` / `isScratchpadPath`
// (utils/permissions/filesystem.ts L293-424) — a per-session temp dir auto-allowed
// for read+write. AMI's superset: the dir is OPERATOR-configured (via
// `SUPERINFERENCE_SCRATCHPAD_DIR`), so it can point at any mounted scratch volume /
// size-limited tmpfs rather than being forced under the system temp root; the
// agent-temp-dir knob itself (`AMI_TMPDIR` / `CLAUDE_CODE_TMPDIR`) is already honored
// everywhere via `resolveTmpBase` (utils/shell.ts).

import * as path from 'node:path';

/** The operator env knob naming the sanctioned scratchpad directory. The same var
 *  drives the system-prompt scratchpad section, so prompt + permission carve-out
 *  stay on ONE directory (no second knob, no drift). */
export const SCRATCHPAD_DIR_ENV = 'SUPERINFERENCE_SCRATCHPAD_DIR';

/**
 * The absolute, normalized scratchpad directory the operator configured, or `null`
 * when the feature is off (env unset / blank). Resolved against `process.cwd()` so a
 * relative value is anchored deterministically, and normalized (trailing separators
 * stripped) so it composes cleanly with the containment prefix check.
 */
export function resolveScratchpadDir(env: NodeJS.ProcessEnv = process.env): string | null {
  const raw = (env[SCRATCHPAD_DIR_ENV] || '').trim();
  if (!raw) return null;
  // path.resolve collapses `.`/`..` and makes the value absolute; strip any trailing
  // separator so the `startsWith(dir + sep)` containment test below is exact.
  const abs = path.resolve(raw);
  return abs.replace(/[/\\]+$/, '') || abs;
}

/**
 * Whether the scratchpad carve-out is active (a dir is configured). AMI's equivalent
 * of Claude's `isScratchpadEnabled()` feature gate — here the gate IS "did the
 * operator point us at a scratchpad dir".
 */
export function isScratchpadEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return resolveScratchpadDir(env) !== null;
}

/**
 * True when `absolutePath` is the scratchpad dir itself or a descendant of it.
 *
 * SECURITY: the candidate is `path.normalize`d first so a traversal payload
 * (`<scratchpad>/../../etc/passwd`) that textually starts with the scratchpad
 * prefix but escapes it is collapsed to its real target BEFORE the prefix test and
 * therefore does NOT match — mirroring Claude's `normalize()`-before-`startsWith`
 * scratchpad check (filesystem.ts L419-424). Callers should pass an already
 * symlink-resolved path (the permission layer resolves via `resolveForCheck`); the
 * normalize here defends the lexical layer regardless.
 */
export function isScratchpadPath(absolutePath: string, env: NodeJS.ProcessEnv = process.env): boolean {
  const dir = resolveScratchpadDir(env);
  if (dir === null) return false;
  const normalized = path.normalize(absolutePath).replace(/[/\\]+$/, '') || path.normalize(absolutePath);
  return normalized === dir || normalized.startsWith(dir + path.sep);
}

/** Read-only introspection sub-manifest for `ami --introspect` (repo
 *  introspection-on-every-change invariant): the env knob, whether it is active,
 *  and the resolved dir. Never throws; `dir` is `null` when disabled. */
export interface ScratchpadReport {
  /** The env var that enables + locates the scratchpad. */
  envVar: string;
  /** Whether a scratchpad dir is currently configured. */
  enabled: boolean;
  /** The resolved absolute scratchpad dir, or `null` when disabled. */
  dir: string | null;
}

/** Build the {@link ScratchpadReport} for the feature manifest. */
export function describeScratchpad(env: NodeJS.ProcessEnv = process.env): ScratchpadReport {
  const dir = resolveScratchpadDir(env);
  return { envVar: SCRATCHPAD_DIR_ENV, enabled: dir !== null, dir };
}
