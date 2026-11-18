/**
 * Wrapper-aware permission-rule prefix derivation.
 *
 * When the user chooses "allow always" for a wrapped command (`nice -n5 git log`,
 * `sudo apt-get install foo`, `timeout 5 npm test`), the suggested permission rule
 * should target the REAL inner command — not the wrapper — while still recording
 * the wrapper so the grant stays narrowly scoped (a `nice git log` rule does not
 * auto-allow a bare `git log`, and vice-versa).
 *
 * This module unwraps a chain of argv-executing wrapper commands to expose the
 * inner command plus the ordered wrapper names, so {@link suggestAllowPrefix} can
 * compose `wrapper … inner-prefix`. Mirrors Claude Code's
 * `getCommandPrefixStatic`/`handleWrapper` (utils/bash/prefix.ts): the recursion
 * is HARD-BOUNDED by two independent counters — `wrapperCount` (nested wrappers)
 * and `recursionDepth` (general recursion) — and fails CLOSED (returns null) on a
 * pathologically nested command so a malicious `nice sudo env nice … cmd` cannot
 * blow past the per-wrapper operand parsers to produce a bogus over-broad rule.
 *
 * Distinct in purpose from bash-security's `stripSafeWrappers`, which DISCARDS
 * wrappers to re-classify the inner command's safety; this preserves the wrapper
 * NAMES and ORDER to build a composed suggestion. The per-wrapper operand
 * knowledge for the overlapping wrappers is reused from the single-source-of-truth
 * {@link WRAPPER_VALUE_FLAGS} so the two cannot drift.
 */

import { SAFE_WRAPPERS, WRAPPER_VALUE_FLAGS } from './bash-security';

/** Claude's prefix.ts bounds: reject deeper than these (fail-closed). */
const MAX_WRAPPER_COUNT = 2;
const MAX_RECURSION_DEPTH = 10;

/**
 * Argv-executing wrappers recognised for prefix composition. Reuses the
 * security-layer {@link SAFE_WRAPPERS} set and ADDS the prefix-only wrappers that
 * are deliberately NOT safe to strip for classification (privilege escalation and
 * fan-out runners) but ARE safe to COMPOSE into a scoped rule — a `sudo git
 * commit` rule is strictly narrower than `git commit`, never broader. Mirrors
 * Claude's WRAPPER_COMMANDS (nice/sudo/env/xargs/timeout/watch/…).
 */
export const WRAPPER_PREFIX_COMMANDS: ReadonlySet<string> = new Set<string>([
  ...SAFE_WRAPPERS,
  'sudo', 'doas', 'xargs', 'watch',
]);

/**
 * Non-fused (spaced) value-taking operand flags for the prefix-only wrappers that
 * {@link WRAPPER_VALUE_FLAGS} does not cover. A flag matched here consumes the
 * FOLLOWING token as its operand so the wrapped command is located past it.
 */
const EXTRA_OPERAND_FLAGS: Record<string, RegExp> = {
  sudo: /^(-u|--user|-g|--group|-h|--host|-p|--prompt|-C|--close-from|-r|--role|-t|--type|-U|--other-user)$/,
  doas: /^(-u|-C)$/,
  xargs: /^(-I|-i|-n|-P|-L|-s|-d|-a|-E|-e)$/,
  watch: /^(-n|--interval)$/,
};

/** Strip a leading path (`/usr/bin/sudo` → `sudo`) so an absolute wrapper matches. */
function bareName(token: string): string {
  return token.replace(/^.*\//, '');
}

/** Does `flag` (a spaced, non-`=` flag) consume the next token as a value for `wrapper`? */
function takesSpacedValue(wrapper: string, flag: string): boolean {
  if (flag.includes('=')) return false;
  if (WRAPPER_VALUE_FLAGS[wrapper]?.test(flag)) return true;
  return EXTRA_OPERAND_FLAGS[wrapper]?.test(flag) ?? false;
}

/**
 * Skip one wrapper's own operands, returning the index of the first inner-command
 * token (or the tokens length if the wrapper hides nothing). Returns `null` when
 * the operands cannot be parsed cleanly enough to trust the resulting rule
 * (fail-closed — e.g. `env -S` argv-splitting, which could smuggle a different
 * command into the suggested prefix).
 */
function skipWrapperOperands(tokens: string[], start: number, wrapper: string): number | null {
  let i = start;
  if (wrapper === 'env') {
    // Only VAR= assignments and a known-safe flag subset keep the inner command
    // where we expect it; -S/--split-string, -C/--chdir, -P/--default-path and any
    // other flag can relocate/replace the command, so bail (fail-closed).
    while (i < tokens.length) {
      const t = tokens[i]!;
      if (/^[A-Za-z_]\w*=/.test(t)) { i++; continue; }
      if (!t.startsWith('-')) break;
      if (/^(-i|--ignore-environment|-0|--null|-v|--debug)$/.test(t)) { i++; continue; }
      if (WRAPPER_VALUE_FLAGS.env!.test(t)) { i += 2; continue; } // -u NAME
      if (/^(-u|--unset)=/.test(t)) { i++; continue; }
      return null; // unrecognised env flag — cannot safely derive the inner command
    }
    return i;
  }
  while (i < tokens.length && tokens[i]!.startsWith('-')) {
    const flag = tokens[i]!;
    i++;
    if (takesSpacedValue(wrapper, flag) && i < tokens.length && !tokens[i]!.startsWith('-')) {
      i++; // consume the flag's spaced value operand
    }
  }
  // Positional operand that is NOT the command: timeout's DURATION and nice's bare
  // numeric priority precede the wrapped command.
  // Duration shapes: `5`, `30s`, `1.5m`. A single `[0-9.]*` run (no nested
  // quantifier) keeps the check linear — this only decides whether to skip a
  // positional operand, so loose acceptance (e.g. `5.5.5`) is harmless.
  if (wrapper === 'timeout' && i < tokens.length && /^[0-9][0-9.]*[smhd]?$/i.test(tokens[i]!)) {
    i++;
  } else if (wrapper === 'nice' && i < tokens.length && /^[0-9]+$/.test(tokens[i]!)) {
    i++;
  }
  return i;
}

/** Result of {@link unwrapWrapperChain}. */
export interface UnwrappedCommand {
  /** Ordered wrapper names stripped from the front (e.g. `['nice', 'sudo']`). */
  wrappers: string[];
  /** The inner command string with all wrappers (and their operands) removed. */
  inner: string;
}

/**
 * Unwrap a chain of argv-executing wrappers, returning the ordered wrapper names
 * and the inner command. Returns `null` when the command is NOT wrapped (so the
 * caller uses its normal single/two-word derivation), when nothing follows the
 * wrapper chain, or when the chain exceeds the bounded recursion caps (fail-closed
 * on obfuscated nesting) or cannot be parsed cleanly.
 */
export function unwrapWrapperChain(command: string): UnwrappedCommand | null {
  let tokens = command.trim().split(/\s+/).filter(Boolean);
  const wrappers: string[] = [];
  let wrapperCount = 0;
  let recursionDepth = 0;
  while (tokens.length > 0 && WRAPPER_PREFIX_COMMANDS.has(bareName(tokens[0]!))) {
    wrapperCount++;
    recursionDepth++;
    if (wrapperCount > MAX_WRAPPER_COUNT || recursionDepth > MAX_RECURSION_DEPTH) return null;
    const wrapper = bareName(tokens[0]!);
    const innerStart = skipWrapperOperands(tokens, 1, wrapper);
    if (innerStart === null) return null;
    if (innerStart >= tokens.length) return null; // bare wrapper hides nothing
    wrappers.push(wrapper);
    tokens = tokens.slice(innerStart);
  }
  if (wrappers.length === 0) return null;
  return { wrappers, inner: tokens.join(' ') };
}
