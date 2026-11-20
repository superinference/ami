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
 * compose `wrapper … inner-prefix`. Mirrors the reference's
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

/** the reference's prefix.ts bounds: reject deeper than these (fail-closed). */
const MAX_WRAPPER_COUNT = 2;
const MAX_RECURSION_DEPTH = 10;

/**
 * Argv-executing wrappers recognised for prefix composition. Reuses the
 * security-layer {@link SAFE_WRAPPERS} set and ADDS the prefix-only wrappers that
 * are deliberately NOT safe to strip for classification (privilege escalation and
 * fan-out runners) but ARE safe to COMPOSE into a scoped rule — a `sudo git
 * commit` rule is strictly narrower than `git commit`, never broader. Mirrors
 * the reference's WRAPPER_COMMANDS (nice/sudo/env/xargs/timeout/watch/…).
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

/**
 * Quote-aware argv tokenizer — word-integrity over the approval boundary.
 *
 * A naive `split(/\s+/)` shatters a quoted operand that contains whitespace
 * (`-u 'admin user'` → `-u`, `'admin`, `user'`), which makes the wrapper-operand
 * walk in {@link skipWrapperOperands} consume only the first fragment and leak the
 * rest (`user'`) into the "inner" command — so the composed allow-rule no longer
 * corresponds to what the user actually ran (`sudo -u 'admin user' git push`
 * silently becomes the bogus `sudo user' git` rule). Grouping a quoted span into a
 * SINGLE token keeps each argv element exactly one prefix word: a single-quoted
 * argv element can never be silently promoted into multiple prefix words, so a
 * persisted rule can never match more than the invocation that was approved.
 *
 * Quote characters are PRESERVED in the emitted token (not stripped): downstream
 * subcommand-shape matching ({@link import('../permissions').getSimpleCommandPrefix})
 * rejects a token carrying a quote, so a genuinely quoted subcommand
 * (`git 'push origin'`) still fails closed to the exact command rather than
 * over-matching `git push origin --force`. An unterminated quote fails closed
 * (returns null) rather than guessing a split. Mirrors the reference's post-buildPrefix
 * word-integrity / over-approval boundary check (utils/powershell/staticPrefix.ts
 * L89-140): a prefix word that does not map to one real argv element is rejected.
 */
export function tokenizeArgvQuoteAware(command: string): string[] | null {
  const tokens: string[] = [];
  let cur = '';
  let inToken = false;
  let quote: string | null = null;
  for (let i = 0; i < command.length; i++) {
    const ch = command[i]!;
    if (quote) {
      cur += ch;
      // A backslash escapes the next char inside double quotes (POSIX); single
      // quotes are literal, so no escaping applies there.
      if (ch === '\\' && quote === '"' && i + 1 < command.length) {
        cur += command[++i]!;
        continue;
      }
      if (ch === quote) quote = null;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      cur += ch;
      inToken = true;
      continue;
    }
    if (ch === ' ' || ch === '\t' || ch === '\n' || ch === '\r' || ch === '\f' || ch === '\v') {
      if (inToken) {
        tokens.push(cur);
        cur = '';
        inToken = false;
      }
      continue;
    }
    cur += ch;
    inToken = true;
  }
  // An unterminated quote means the command cannot be split with integrity —
  // fail closed so no over-broad rule is derived from a guessed split.
  if (quote !== null) return null;
  if (inToken) tokens.push(cur);
  return tokens;
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
  // Quote-aware tokenization keeps every quoted argv element a single token so a
  // wrapper's quoted operand (`sudo -u 'admin user' …`) cannot leak a fragment
  // into the inner command and produce an over-broad composed rule. An
  // unterminated quote fails closed (null).
  let tokens = tokenizeArgvQuoteAware(command);
  if (tokens === null) return null;
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

/**
 * Compound-prefix bare-root drop guard.
 *
 * When word-aligned longest-common-prefix collapse of a group of compound-command
 * prefixes sharing a root degenerates to a SINGLE word (`git add` + `git commit`
 * → `git`, `git fetch` + `git worktree add` → `git`), that collapsed root must NOT
 * be emitted as an "allow always" permission rule when the root is subcommand
 * aware: a bare `git` rule would auto-allow `git push --force` forever. In that
 * case the caller DROPS the whole group rather than suggest the too-broad root (or
 * N un-collapsed rules). A multi-word LCP (`npm run`) is always safe to keep — it
 * is narrowly scoped — so this returns false for it.
 *
 * Mirrors the reference's `getCompoundCommandPrefixesStatic` collapse guard
 * (utils/powershell/staticPrefix.ts L271-280): when `lcpWordCount <= 1` and the
 * root declares subcommands / has a `DEPTH_RULES` entry, the group is dropped.
 *
 * `isSubcommandAwareRoot` is INJECTED — the subcommand-aware-root oracle
 * (`SUBCOMMAND_AWARE_ROOTS` / the command-spec table) lives in the permission
 * layer, which imports this module; taking it as a parameter keeps this primitive
 * dependency-free (no import cycle) and unit-testable in isolation.
 */
export function isBareSubcommandAwareRoot(
  collapsedPrefix: string,
  isSubcommandAwareRoot: (root: string) => boolean,
): boolean {
  const trimmed = collapsedPrefix.trim();
  // Empty or multi-word prefixes are never a bare root: a scoped `cmd subcommand`
  // (or `npm run`) rule is safe to keep.
  if (trimmed === '' || trimmed.includes(' ')) return false;
  // Root matching is CASE-INSENSITIVE: a word-aligned LCP preserves the first
  // string's casing (`Git` for `Git add`+`git commit`) while the oracle's
  // subcommand-aware set is lowercase, so a mixed-case `Git` must still be
  // recognised as the bare `git` root and dropped. Mirrors the reference's
  // `getCommandSpec(rootLower)` lookup (utils/powershell/staticPrefix.ts L273).
  return isSubcommandAwareRoot(trimmed.toLowerCase());
}

/**
 * Word-aligned longest common prefix of a group of command prefixes, compared
 * CASE-INSENSITIVELY while emitting the FIRST prefix's original casing. Never
 * chops mid-word; returns at least the first prefix's first word so a non-empty
 * group never collapses to `''`.
 *
 * Case-insensitivity is required for PowerShell (which is case-insensitive:
 * `Git` === `git`, `Get-Process` === `get-process`) and is harmless for the
 * case-sensitive bash path (lowercased tokens compare identically). e.g.
 * `['npm run test', 'npm run lint']` → `npm run`; `['Git status', 'git log']`
 * → `Git`. Mirrors the reference's `wordAlignedLCP`
 * (utils/powershell/staticPrefix.ts L288-316).
 */
export function wordAlignedCommonPrefix(prefixes: readonly string[]): string {
  if (prefixes.length === 0) return '';
  if (prefixes.length === 1) return prefixes[0]!;
  const firstWords = prefixes[0]!.split(' ');
  let common = firstWords.length;
  for (let i = 1; i < prefixes.length; i++) {
    const other = prefixes[i]!.split(' ');
    let shared = 0;
    while (
      shared < common &&
      shared < other.length &&
      other[shared]!.toLowerCase() === firstWords[shared]!.toLowerCase()
    ) {
      shared++;
    }
    common = shared;
    if (common === 0) break;
  }
  return firstWords.slice(0, Math.max(1, common)).join(' ');
}

/**
 * Collapse a flat list of per-subcommand scoped prefixes (one per mutating
 * subcommand of a compound command) into the minimal set of "allow always"
 * rules. Prefixes are grouped by their root word (first token, keyed
 * CASE-INSENSITIVELY so `Git`/`git` share a group); each group is collapsed to
 * its {@link wordAlignedCommonPrefix}; and a group whose collapsed prefix is a
 * bare subcommand-aware root ({@link isBareSubcommandAwareRoot}) is DROPPED
 * rather than emitted, so a `git fetch`+`git worktree add` chain never suggests a
 * too-broad `git:*` rule. A multi-word LCP (`npm run`) is narrowly scoped and
 * kept.
 *
 * This is the single canonical compound-prefix collapser — the permission layer
 * ({@link import('../permissions').getCompoundCommandPrefixes}) derives the
 * per-subcommand prefixes, then delegates the grouping/LCP/drop to this helper so
 * the collapse logic lives in exactly one place. Mirrors the reference's
 * `getCompoundCommandPrefixesStatic` group/collapse/drop
 * (utils/powershell/staticPrefix.ts L246-284).
 */
export function collapseCompoundPrefixes(
  prefixes: readonly string[],
  isSubcommandAwareRoot: (root: string) => boolean,
): string[] {
  const groups = new Map<string, string[]>();
  for (const prefix of prefixes) {
    const key = prefix.split(' ')[0]!.toLowerCase();
    const group = groups.get(key);
    if (group) group.push(prefix);
    else groups.set(key, [prefix]);
  }
  const collapsed: string[] = [];
  for (const group of groups.values()) {
    const lcp = wordAlignedCommonPrefix(group);
    if (isBareSubcommandAwareRoot(lcp, isSubcommandAwareRoot)) continue;
    collapsed.push(lcp);
  }
  return collapsed;
}
