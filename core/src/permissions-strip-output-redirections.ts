// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { stripHeredocBodies } from './tools/bash-security';
import { stripQuotedSections } from './permissions-contains-shell-substitution';
import { MAX_COMMAND_PARSE_DEPTH, findMatchingParen, isNormalizedCdCommand, unwrapShellControlStructures } from './permissions-get-paths-for-permission-check';

/**
 * Detect that `cwd` is (or has been manipulated to look like) a BARE git repo, so
 * that running any git command from it would make git treat `cwd` as the git dir
 * and execute attacker-planted hooks (`hooks/pre-commit`, …) — a sandbox-escape
 * vector. Git's `is_git_directory()` (setup.c) treats a directory as a git dir when
 * it holds a valid `HEAD` ref plus `objects/` and `refs/`. The attack: plant
 * `HEAD` + `objects/` + `refs/` + `hooks/pre-commit` in `cwd` and remove/corrupt
 * `.git/HEAD` so git falls back to cwd discovery and runs the hook.
 *
 * Returns true when `cwd` has bare-repo indicators WITHOUT a valid `.git/HEAD`:
 * - `.git` is a regular FILE → worktree/submodule gitdir reference → SAFE (false).
 * - `.git` is a directory with a regular-file `HEAD` → normal repo → SAFE (false).
 * - otherwise, flag if any of `HEAD` (file), `objects/` (dir), `refs/` (dir) exist
 *   directly in `cwd`.
 *
 * Each indicator is stat'd under its own try/catch so one stat error can't mask
 * another. Pure + synchronous (a permission-eval boundary check). Mirrors the reference's
 * `isCurrentDirectoryBareGitRepo` (utils/git.ts). `cwd` defaults to the process cwd.
 */
export function isDisguisedBareGitRepo(cwd: string = process.cwd()): boolean {
  const gitPath = path.join(cwd, '.git');
  try {
    const stats = fs.statSync(gitPath);
    if (stats.isFile()) return false; // worktree/submodule gitdir reference
    if (stats.isDirectory()) {
      try {
        // An attacker creating .git/HEAD as a DIRECTORY passes a bare statSync but
        // git rejects a non-file HEAD and falls back to cwd discovery — so only a
        // regular-file HEAD proves a normal repo.
        if (fs.statSync(path.join(gitPath, 'HEAD')).isFile()) return false;
      } catch { /* .git exists but no HEAD — fall through to bare-repo check */ }
    }
  } catch { /* no .git — fall through to bare-repo indicator check */ }

  // No valid .git/HEAD — flag if cwd itself carries bare-repo indicators.
  try { if (fs.statSync(path.join(cwd, 'HEAD')).isFile()) return true; } catch { /* none */ }
  try { if (fs.statSync(path.join(cwd, 'objects')).isDirectory()) return true; } catch { /* none */ }
  try { if (fs.statSync(path.join(cwd, 'refs')).isDirectory()) return true; } catch { /* none */ }
  return false;
}

/**
 * Identity ("no-op") `cd` targets that resolve to the CURRENT working directory,
 * so a leading `cd <target>` changes nothing. Models routinely prepend `cd $PWD`
 * (or `cd .`) before the real command; that segment must not be treated as a real
 * directory change by the cwd-desync / multiple-cd guards. Covers the portable
 * identity forms (`.`, `$PWD`, `${PWD}`, quoted variants, `$(pwd)`, `` `pwd` ``)
 * and the Windows `%CD%` spelling.
 */
export const NOOP_CWD_TARGET_RE =
  /^(?:\.|\$PWD|\$\{PWD\}|"\$PWD"|'\$PWD'|"\$\{PWD\}"|\$\(pwd\)|`pwd`|%CD%)$/;

/**
 * Strip a SINGLE leading no-op `cd <cwd>` subcommand from a (possibly compound)
 * command, returning the remainder. A model-prepended identity cd (`cd $PWD &&
 * real-cmd`, `cd . ; real-cmd`) is a no-op, so counting it toward the multiple-cd
 * guard or the cd+redirect desync guard would spuriously escalate an otherwise
 * benign command to an explicit prompt. Only an IDENTITY target is peeled — a
 * `cd` into a real directory shifts the resolution base for later relative paths
 * and is never stripped. Mirrors the reference's BashTool prefix normalization, which
 * removes a leading `cd <cwd>` subcommand before counting cd commands / running
 * per-subcommand permission evaluation.
 */
export function stripLeadingNoopCd(command: string): string {
  const m = command.match(/^\s*([^\n;&|]+?)\s*(?:&&|;|\|\||\||&)\s*([\s\S]+)$/);
  if (!m) return command;
  const first = m[1]!.trim();
  const rest = m[2]!;
  const toks = first.split(/\s+/).filter(Boolean);
  const base = (toks[0] ?? '').toLowerCase();
  if (base !== 'cd' && base !== 'chdir' && base !== 'pushd' && base !== 'set-location') {
    return command;
  }
  // The segment must be exactly `cd <single-identity-target>` (no flags, no extra
  // operands) so `cd . file` or `cd $PWD extra` is left intact for scrutiny.
  const operands = toks.slice(1);
  if (operands.length !== 1 || !NOOP_CWD_TARGET_RE.test(operands[0]!)) return command;
  return rest;
}

/**
 * Detect a compound command that changes directory (`cd`/`pushd`/`chdir`/
 * PowerShell `Set-Location`) AND then performs a file-writing output redirection
 * (`>`/`>>` to a file, not an fd-dup like `2>&1`). The validator classifies the
 * redirect target against the launch cwd, but at runtime it resolves against the
 * NEW cwd — a path-resolution/TOCTOU desync that can write outside the workspace
 * (`cd /outside && echo payload > config`). the reference requires manual approval for
 * this shape (pathValidation.ts cd+write guard, PowerShell Set-Location desync).
 */
export function commandHasCdWriteDesync(command: string): boolean {
  // Drop a model-prepended identity `cd <cwd>` (a no-op) so it can't be mistaken
  // for a real directory change before a redirect.
  const unwrapped = unwrapShellControlStructures(stripLeadingNoopCd(command));
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let hasCd = false;
  let hasRedirect = false;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (isNormalizedCdCommand(cmd)) hasCd = true;
    // A file redirect: optional leading fd, `>`/`>>`, then a target that is not
    // an fd-dup (`&1`). Quoted sections are stripped so `echo "a > b"` is ignored.
    const unquoted = stripQuotedSections(seg);
    if (/\d*>>?\s*(?![&\s])\S/.test(unquoted)) hasRedirect = true;
  }
  return hasCd && hasRedirect;
}

/**
 * Count the directory-change segments (`cd`/`pushd`/`chdir`/PowerShell
 * `Set-Location`) in a (possibly compound) command. More than one directory
 * change in a single command is an unusual navigation pattern that defeats
 * cwd-relative path validation and must require approval (the reference's multi-cd
 * guard, bashCommandHelpers.ts).
 */
export function countDirectoryChanges(command: string): number {
  // A model-prepended identity `cd <cwd>` is a no-op and must not count toward the
  // multiple-cd guard (which escalates any command with more than one real cd).
  const unwrapped = unwrapShellControlStructures(stripLeadingNoopCd(command));
  const segments = unwrapped.split(/\s*(?:\|\||&&|[|;])\s*/).filter(s => s.trim());
  let count = 0;
  for (const seg of segments) {
    const tokens = seg.trim().split(/\s+/);
    let i = 0;
    while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i]!)) i++;
    const cmd = tokens[i];
    if (isNormalizedCdCommand(cmd)) count++;
  }
  return count;
}

/**
 * Resolve a FILE permission-rule pattern to its `(relativePattern, root)` pair,
 * mirroring the reference's `patternWithRoot`
 * (references/claude/claude-code-main/src/utils/permissions/filesystem.ts L852-917).
 *
 * The pattern's leading sigil chooses the directory it is anchored to, so the SAME
 * rule string resolves to the SAME file regardless of which settings tier it came
 * from. AMI previously matched file rules as raw globs against the literal path
 * string, so a root-relative rule (`//etc/**`), a home-relative rule (`~/.ssh/**`)
 * or a project-relative rule (`/src/**`) silently never matched a real absolute
 * path — AMI even generates `//abs/**` suggestions via {@link createReadRuleSuggestion}
 * that could not match. This restores per-root resolution:
 *  - `//x`  → root `/` (filesystem-root-relative); one leading slash is dropped.
 *  - `~/x`  → root = the NFC-normalized home directory.
 *  - `/x`   → root = `sourceRoot` (the directory the rule's settings tier lives in,
 *             i.e. the project root for project/local settings).
 *  - bare `x` / `./x` → root `null` (root-agnostic); a leading `./` is stripped so
 *    `./.env` matches `.env`.
 */
export function patternWithRoot(
  pattern: string,
  sourceRoot: string,
  home: string = os.homedir(),
): { relativePattern: string; root: string | null } {
  if (pattern.startsWith('//')) {
    return { relativePattern: pattern.slice(1), root: '/' };
  }
  if (pattern.startsWith('~/')) {
    return { relativePattern: pattern.slice(1), root: home.normalize('NFC') };
  }
  if (pattern.startsWith('/')) {
    return { relativePattern: pattern, root: sourceRoot };
  }
  const normalized = pattern.startsWith('./') ? pattern.slice(2) : pattern;
  return { relativePattern: normalized, root: null };
}

/**
 * Normalize line continuations (backslash + newline) in a bash command.
 * Bash treats `\<newline>` as a line join — stripping it reveals the
 * actual command that will execute, preventing pattern-bypass via
 * multi-line splitting.
 */
export function normalizeLineContinuations(cmd: string): string {
  // A `\<newline>` is a true line-continuation ONLY when the backslash run
  // immediately before the newline is ODD (the last backslash escapes the
  // newline). With an EVEN run the backslashes pair up as literal escapes and
  // the newline is a REAL command separator that must be KEPT — otherwise
  // `echo \\<newline>rm -rf /` would be mis-joined into one command and the
  // second statement could slip past an end-anchored pattern. Mirrors the reference's
  // odd/even backslash-count discrimination (utils/bash/commands.ts L96-139).
  let out = '';
  for (let i = 0; i < cmd.length; i++) {
    if (cmd[i] === '\n') {
      let bs = 0;
      let j = out.length - 1;
      while (j >= 0 && out[j] === '\\') { bs++; j--; }
      if (bs % 2 === 1) {
        out = out.slice(0, out.length - 1); // drop the escaping backslash, join lines
        continue; // and drop the newline
      }
      out += '\n'; // even run (or none) — real separator, keep it
      continue;
    }
    out += cmd[i];
  }
  return out;
}

/**
 * Prepare a raw command for compound-splitting / rule-matching in the SECURITY
 * ordering the reference relies on: extract (strip) heredoc BODIES *before* joining
 * `\<newline>` line-continuations, then join. Order matters for correctness AND
 * security — joining continuations first can shift a heredoc's closing delimiter
 * (`x\<newline>DELIM` → `xDELIM`) so that a real post-heredoc statement such as
 * `> /etc/passwd` gets swallowed INTO the heredoc body and never reaches the
 * sub-command matcher, letting an `allow` rule auto-approve a command whose
 * dangerous tail it never saw. {@link stripHeredocBodies} removes those bodies up
 * front (matching the reference's `extractHeredocs`-before-join invariant in
 * utils/bash/commands.ts L642-683), so the continuation-join and the subsequent
 * {@link splitCompoundCommand} only ever operate on the real COMMAND text.
 *
 * This is the single canonical "normalize a command for matching" entry point —
 * every permission-matching/compound-split consumer (rule matching, arg-leak
 * scanning) uses it so the heredoc-before-join guarantee holds uniformly.
 */
export function normalizeCommandForMatching(command: string): string {
  return normalizeLineContinuations(stripHeredocBodies(command));
}

export const COMPOUND_SPLIT_RE = /\s*(?:\|\||&&|[;|])\s*/;

/**
 * Split a compound bash command into individual sub-commands.
 * First unwraps shell control structures (for/while/if), then splits
 * on &&, ||, ;, and | operators.
 */
export function splitCompoundCommand(cmd: string): string[] {
  const unwrapped = unwrapShellControlStructures(cmd);
  return unwrapped.split(COMPOUND_SPLIT_RE).map(s => s.trim()).filter(s => s.length > 0);
}

/**
 * Cap on how many sub-commands a single compound command may fan out into before
 * the per-subcommand safety analysis (classification + rule matching) is run.
 * Above the cap we FAIL CLOSED — the command is treated as unsafe and an allow
 * rule cannot auto-approve it, so it falls back to a prompt. A pathologically
 * wide `a && b && c && …` chain otherwise runs the full validator set per segment
 * and can starve the event loop (the reference's MAX_SUBCOMMANDS_FOR_SECURITY_CHECK,
 * CC-643). Fifty is generous — legitimate user commands do not split that wide.
 */
export const MAX_SUBCOMMANDS_FOR_SECURITY_CHECK = 50;

/**
 * Extract the inner command string(s) from `$(...)` command substitutions and
 * `` `...` `` backticks, recursively (nested substitutions included).
 *
 * Used ONLY for deny/ask matching (M1): a DENIED command must not be able to hide
 * inside a substitution (e.g. `echo $(rm -rf x)` or `` echo `rm -rf x` ``). Allow
 * matching never consults this — decomposition can only ADD denies, never broaden
 * an allow.
 */
export function extractCommandSubstitutions(command: string): string[] {
  const results: string[] = [];
  const walk = (s: string, depth: number): void => {
    // Bounded recursion: a deeply-nested substitution chain must not blow the
    // stack or hang deny-matching (DoS). Mirrors the reference's recursionDepth>10 guard.
    if (depth > MAX_COMMAND_PARSE_DEPTH) return;
    for (let i = 0; i < s.length; i++) {
      if (s[i] === '$' && i + 1 < s.length && s[i + 1] === '(') {
        const end = findMatchingParen(s, i + 2);
        if (end !== -1) {
          const inner = s.slice(i + 2, end);
          results.push(inner);
          walk(inner, depth + 1); // nested $(...) / backticks
          i = end;
        }
      } else if (s[i] === '`') {
        const end = s.indexOf('`', i + 1);
        if (end !== -1) {
          const inner = s.slice(i + 1, end);
          results.push(inner);
          walk(inner, depth + 1);
          i = end;
        }
      }
    }
  };
  walk(command, 0);
  return results;
}

// ---------------------------------------------------------------------------
// Scoped command-prefix derivation for "allow always" permission rules
// ---------------------------------------------------------------------------

export const ENV_VAR_ASSIGN_RE = /^[A-Za-z_][A-Za-z0-9_]*=/;

// Subcommand shape: lowercase alnum, hyphen-separated (e.g. `commit`, `rev-parse`).
// The `[a-z0-9]*` run and the `-[a-z0-9]+` groups start with disjoint chars
// (alnum vs `-`), so there is no ambiguous overlap / ReDoS backtracking.
// eslint-disable-next-line security/detect-unsafe-regex
export const SUBCOMMAND_SHAPE_RE = /^[a-z][a-z0-9]*(-[a-z0-9]+)*$/;

/**
 * Minimal command-spec model for prefix extraction. For a subcommand-aware CLI
 * it declares the global option flags that CONSUME the following token as their
 * value (so `git -C /repo status` / `kubectl -n kube-system get pods` skip the
 * flag+value to reach the real subcommand) and an optional `depth` — how many
 * words the scoped prefix should extend to (default 2). Mirrors the reference's fig-spec
 * `options[].args` "does this flag take a value" (utils/shell/specPrefix.ts) and
 * its `DEPTH_RULES` per-command default depth table (utils/powershell/staticPrefix.ts).
 *
 * This is a static, parser-free analogue of the reference's dynamically-imported fig
 * specs — the specs those builds cannot load at runtime are exactly the ones
 * the reference hardcodes in DEPTH_RULES, so AMI hardcodes the same shallow table here.
 */
export interface BashCommandSpec {
  /** Global flags (long or short) that consume the next token as their value. */
  valueFlags: ReadonlySet<string>;
  /** Scoped-prefix depth in words (command + subcommands). Defaults to 2. */
  depth?: number;
  /**
   * Per-FIRST-SUBCOMMAND depth overrides, keyed by the lowercased first subcommand
   * word. A deep subcommand tree under one group needs a larger prefix than the
   * command's default `depth` to reach a meaningful leaf (e.g. `gcloud compute
   * instances describe` needs depth 6, while `gcloud config list` is fine at 4).
   * When the first subcommand after the command matches a key here, that value
   * REPLACES `depth` for this invocation. Mirrors the reference's `DEPTH_RULES` COMPOUND
   * keys (`${command} ${firstSubcommand}` → depth), e.g. `gcloud compute`:6,
   * `gcloud beta`:6 (utils/shell/specPrefix.ts DEPTH_RULES / staticPrefix.ts
   * calculateDepth, which consults the `${command} ${sub}` key before the bare
   * command key). Keyed on the first subcommand only (not arbitrary pairs) because
   * that is the sole compound form the reference's table uses.
   */
  subcommandDepth?: Readonly<Record<string, number>>;
}

/** Does `flag` consume the following token as its value, per `spec`? */
export function flagTakesValue(flag: string, spec: BashCommandSpec | undefined): boolean {
  // Strip an inline `--flag=value` form — that carries its own value.
  if (flag.includes('=')) return false;
  return spec?.valueFlags.has(flag) ?? false;
}

/**
 * Advance past leading global flags (and the values `spec` says they consume)
 * starting at `tokens[start]`, returning the index of the first non-flag token —
 * the real subcommand. `git -C /repo status` → the index of `status`;
 * `docker -D ps` → the index of `ps`. This is the canonical spec-driven
 * flag-value skip shared by prefix suggestion (getSimpleCommandPrefix), base
 * extraction (extractBaseCommands) and classification (allSegmentsKnown + the
 * docker read-only check), so a global flag before the subcommand can no longer
 * defeat SAFE_PREFIXES / DOCKER_SAFE_SUBCOMMANDS matching. Mirrors the reference's
 * findFirstSubcommand + flagTakesArg (utils/shell/specPrefix.ts L40-137).
 */
export function skipGlobalFlags(tokens: string[], start: number, spec: BashCommandSpec | undefined): number {
  let i = start;
  while (i < tokens.length && tokens[i]!.startsWith('-')) {
    if (flagTakesValue(tokens[i]!, spec)) i++;
    i++;
  }
  return i;
}

/**
 * Strip shell output/input redirection clauses (`> out.txt`, `>>log`, `2>&1`,
 * `&> both`, `< in`, `<<<here`) from a command so a redirect *target filename*
 * never leaks into a derived permission-rule prefix (turning `npm test > out.txt`
 * into a bogus `Bash(out.txt)` rule). Quote-aware: a `>` inside single/double
 * quotes is preserved. Mirrors the reference's `commandTransform` hook on
 * `generateShellSuggestionsLabel` (shellPermissionHelpers: "Bash strips output
 * redirections so filenames don't show as commands"). Best-effort for display /
 * rule derivation only — it never participates in a security decision.
 */
export function stripOutputRedirections(command: string): string {
  let out = '';
  let i = 0;
  let quote: string | null = null;
  const n = command.length;
  while (i < n) {
    const ch = command[i]!;
    if (quote) {
      out += ch;
      if (ch === quote && command[i - 1] !== '\\') quote = null;
      i++;
      continue;
    }
    if (ch === '"' || ch === "'") { quote = ch; out += ch; i++; continue; }
    // Optional leading file-descriptor digits (`2>`, `1>>`), then an operator.
    let j = i;
    while (j < n && command[j]! >= '0' && command[j]! <= '9') j++;
    // `>&`/`<&` and `&>` must be tried before the plain `>>?`/`<<?<?` so
    // `2>&1` is consumed as one operator (not `>` + leftover `&1`).
    const op = /^(?:>&|<&|&>>?|>>?|<<?<?)/.exec(command.slice(j));
    // Require a real redirection: either fd digits were consumed (`2>`), or the
    // char itself is a redirection lead (`<`/`>`/`&>`). A lone `&`/`&&` (control
    // operator) and digits that are just an argument are left untouched.
    if (op && (j > i || ch === '<' || ch === '>' || (ch === '&' && op[0].startsWith('&')))) {
      i = j + op[0].length;
      while (i < n && /\s/.test(command[i]!)) i++;          // skip whitespace
      while (i < n && !/[\s|;&<>()]/.test(command[i]!)) i++; // skip target token
      if (out && !out.endsWith(' ')) out += ' ';
      continue;
    }
    out += ch;
    i++;
  }
  return out.replace(/\s+/g, ' ').trim();
}
