// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

// ---------------------------------------------------------------------------
// Permission verdict hold-over
// ---------------------------------------------------------------------------

export interface PermissionVerdict {
  /**
   * The outcome of a single permission evaluator.
   *
   * `passthrough` is a FOURTH, NEUTRAL outcome distinct from a terminal
   * `allow`/`deny`/`ask`: a layered/composable evaluator can observe a tool call
   * and DECLINE to decide ("no opinion — defer to the next evaluator"), without
   * pinning the result to `allow`. It is the identity element of the
   * {@link mostRestrictiveVerdict} lattice — a `passthrough` never overrides a
   * real verdict and is always overridden by one, so a chain of evaluators can
   * abstain until one of them actually decides. Mirrors the reference's
   * `PermissionResult.behavior: 'passthrough'` (types/permissions.ts) and
   * consolidates with the {@link HookVerdict} `passthrough` state in
   * `hook-protection.ts` (one canonical abstain vocabulary across the engine).
   */
  decision: 'allow' | 'deny' | 'ask' | 'passthrough';
  rule?: string;
  source?: string;
  reason?: string;
  /**
   * Which classifier produced this verdict, when it came from one (vs a static
   * rule): `'bash'` for the deterministic bash command classifier, `'auto-mode'`
   * for the injected autonomous-safety classifier. Lets a UI explain HOW a tool
   * use was gated/approved — parity with the reference's `ClassifierApproval.classifier`
   * discriminant (utils/classifierApprovals.ts). Absent for static-rule verdicts.
   */
  classifier?: 'bash' | 'auto-mode';
  /**
   * For a `'bash'`-classifier verdict, the specific matched rule/pattern the
   * classifier decided under (parity: `ClassifierApproval.matchedRule`), so a
   * reviewer can answer "which rule approved this command". Absent otherwise.
   */
  matchedRule?: string;
}

export function verdictRank(decision: string): number {
  switch (decision) {
    case 'deny': return 3;
    case 'modify': return 2;
    case 'ask': return 2;
    case 'allow': return 1;
    // `passthrough` (abstain / "no opinion") is the lattice's neutral element:
    // strictly below `allow`, so it never holds over a real verdict and is always
    // overridden by one. An unknown decision shares this floor (fail-neutral).
    case 'passthrough': return 0;
    default: return 0;
  }
}

export function holdsOver(held: PermissionVerdict, answer: PermissionVerdict): boolean {
  const heldRank = verdictRank(held.decision);
  const answerRank = verdictRank(answer.decision);
  if (answerRank > heldRank) return false;
  if (heldRank > answerRank) return true;
  const heldHasAttribution = held.rule !== undefined || held.source !== undefined;
  const answerHasAttribution = answer.rule !== undefined || answer.source !== undefined;
  return heldHasAttribution && !answerHasAttribution;
}

/**
 * Combine a chain of layered permission evaluators into one decision under the
 * strictest-wins lattice. `passthrough` (abstain) is the neutral element: it never
 * holds over a real verdict and is always overridden by one, so a chain of
 * evaluators can defer until one actually decides. When EVERY evaluator abstains
 * (all `passthrough`) the result is itself `passthrough` — "no layer had an
 * opinion", which the caller resolves against its own default (parity with
 * the reference's passthrough falling through to the normal permission flow). An empty
 * chain is treated as abstain for the same reason.
 */
export function mostRestrictiveVerdict(verdicts: PermissionVerdict[]): PermissionVerdict {
  if (verdicts.length === 0) return { decision: 'passthrough' };
  let best = verdicts[0];
  for (let i = 1; i < verdicts.length; i++) {
    if (holdsOver(verdicts[i], best)) {
      best = verdicts[i];
    }
  }
  return best;
}

/**
 * Per-flag argument-type classification (mirrors the reference's `FlagArgType`,
 * readOnlyCommandValidation.ts). It tells the flag validator exactly how many
 * tokens a flag consumes and how to validate the value, closing the parser
 * differential where a flag's VALUE (or a bundled arg-taking flag's next token)
 * is mis-read as a positional/target.
 */
export type FlagArgType =
  | 'none'   // takes no argument (e.g. --hidden, -H)
  | 'number' // integer argument (e.g. --max-depth 3)
  | 'string' // any string argument (e.g. --type file)
  | 'char'   // a single character (e.g. a delimiter)
  | '{}'     // the literal "{}" only
  | 'EOF';   // the literal "EOF" only

/** Flag-shaped token detector, matching the reference's FLAG_PATTERN. */
export const FLAG_PATTERN = /^-[a-zA-Z0-9_-]/;

/**
 * Validate a flag's argument value against its declared {@link FlagArgType}.
 * Mirrors the reference's `validateFlagArgument`. 'none' always returns false —
 * it must never be called for a flag that takes no argument.
 */
export function validateFlagArgument(value: string, argType: FlagArgType): boolean {
  switch (argType) {
    case 'none': return false;
    case 'number': return /^\d+$/.test(value);
    case 'string': return true;
    case 'char': return value.length === 1;
    case '{}': return value === '{}';
    case 'EOF': return value === 'EOF';
    default: return false;
  }
}

/**
 * Arg-type map for `fd`/`fdfind` (shared). Depth flags are numeric, type/
 * extension/exclude/glob/search-path flags are strings, the rest are no-arg.
 * Modeled on the reference's FD_SAFE_FLAGS so the validator consumes the right number
 * of tokens per flag and validates each value.
 */
export const FD_ARG_TYPES: Record<string, FlagArgType> = {
  '-H': 'none', '--hidden': 'none', '-I': 'none', '--no-ignore': 'none',
  '-d': 'number', '--max-depth': 'number',
  '-t': 'string', '--type': 'string',
  '-e': 'string', '--extension': 'string',
  '-E': 'string', '--exclude': 'string',
  '-g': 'string', '-p': 'string',
};

/**
 * Arg-type map for `base64`. Decode/mode flags take no argument; the line-length
 * flags are numeric; `-i`/`--input` names a file to READ (never a write). Mirrors
 * the reference's base64 safeFlags (readOnlyValidation.ts). macOS base64 ignores POSIX
 * `--`, hence respectsDoubleDash:false on the entry below.
 */
export const BASE64_ARG_TYPES: Record<string, FlagArgType> = {
  '-d': 'none', '-D': 'none', '--decode': 'none',
  '-b': 'number', '--break': 'number', '-w': 'number', '--wrap': 'number',
  '-i': 'string', '--input': 'string',
  '--ignore-garbage': 'none',
  '-h': 'none', '--help': 'none', '--version': 'none',
};

/**
 * Arg-type map shared by the checksum commands (`sha256sum`/`sha1sum`/`md5sum`).
 * Every flag only selects an input mode, a verification mode, or an output
 * format — none write anything. Mirrors the reference's checksum safeFlags.
 */
export const CHECKSUM_ARG_TYPES: Record<string, FlagArgType> = {
  '-b': 'none', '--binary': 'none', '-t': 'none', '--text': 'none',
  '-c': 'none', '--check': 'none', '--ignore-missing': 'none',
  '--quiet': 'none', '--status': 'none', '--strict': 'none',
  '-w': 'none', '--warn': 'none',
  '--tag': 'none', '-z': 'none', '--zero': 'none',
  '--help': 'none', '--version': 'none',
};

/**
 * Arg-type map for `grep`. Pattern/file flags take a string value, the rest are
 * no-arg mode selectors. Strict: any flag outside this set (notably the
 * preprocessor/exec-adjacent flags blocked below) falls through to a prompt.
 * Mirrors the reference's grep safeFlags.
 */
export const GREP_ARG_TYPES: Record<string, FlagArgType> = {
  '-e': 'string', '--regexp': 'string', '-f': 'string', '--file': 'string',
  '-F': 'none', '--fixed-strings': 'none', '-G': 'none', '--basic-regexp': 'none',
  '-E': 'none', '--extended-regexp': 'none', '-P': 'none', '--perl-regexp': 'none',
  '-i': 'none', '--ignore-case': 'none', '-v': 'none', '--invert-match': 'none',
  '-n': 'none', '--line-number': 'none', '-c': 'none', '--count': 'none',
  '-l': 'none', '--files-with-matches': 'none', '-L': 'none', '--files-without-match': 'none',
  '-o': 'none', '--only-matching': 'none', '-w': 'none', '--word-regexp': 'none',
  '-x': 'none', '--line-regexp': 'none', '-r': 'none', '-R': 'none', '--recursive': 'none',
  '-h': 'none', '--no-filename': 'none', '-H': 'none', '--with-filename': 'none',
  '-A': 'number', '--after-context': 'number', '-B': 'number', '--before-context': 'number',
  '-C': 'number', '--context': 'number', '-m': 'number', '--max-count': 'number',
  '-s': 'none', '--no-messages': 'none', '-a': 'none', '--text': 'none',
  '-q': 'none', '--quiet': 'none', '--color': 'string', '--colour': 'string',
  '--include': 'string', '--exclude': 'string', '--exclude-dir': 'string',
  '-V': 'none', '--version': 'none',
};

/**
 * Arg-type map for `pyright`. Project/version/level flags take a string value;
 * stats/verbosity/version flags are no-arg. Strict: unknown flags prompt. The
 * long-running `--watch`/`-w` is blocked via dangerousArgs below. pyright treats
 * `--` as a FILE path (not end-of-options), so respectsDoubleDash:false keeps the
 * validator scanning past it. Mirrors the reference's PYRIGHT_READ_ONLY_COMMANDS.
 */
export const PYRIGHT_ARG_TYPES: Record<string, FlagArgType> = {
  '--outputjson': 'none', '--project': 'string', '-p': 'string',
  '--pythonversion': 'string', '--pythonplatform': 'string',
  '--typeshedpath': 'string', '--venvpath': 'string', '--level': 'string',
  '--stats': 'none', '--verbose': 'none', '--version': 'none',
  '--dependencies': 'none', '--warnings': 'none',
};

/**
 * Flag policy for a {@link SAFE_COMMANDS} entry (U003 fail-closed model).
 *  - `allowed`/`blocked` — the read-only / never-safe flag sets.
 *  - `strict` — reject any flag outside `allowed` (fall through to a prompt).
 *  - `argTypes` — getopt-accurate per-flag value typing (see {@link validateTypedCommandFlags}).
 *  - `dangerousArgs` — custom per-command danger predicate.
 *  - `respectsDoubleDash` — whether POSIX `--` ends option scanning (default true).
 *  - `allowAllFlags` — the command's ENTIRE flag surface is read-only, so every flag
 *    except those in `blocked` is accepted. This is the EXPLICIT opt-in that the reference's
 *    CommandConfig model requires (readOnlyValidation.ts L39-56): a SAFE command must
 *    now declare its flag policy, and a command with no entry is positional-only
 *    (fail-closed), rather than the old fail-OPEN where a missing entry waved every
 *    flag through. Marks genuinely all-flags-safe read utilities (ls/cat/df/…) so the
 *    inverted default never regresses them.
 */
export type SafeCommandFlagDef = {
  allowed: Set<string>;
  blocked: Set<string>;
  strict?: boolean;
  argTypes?: Record<string, FlagArgType>;
  dangerousArgs?: (args: string[]) => boolean;
  respectsDoubleDash?: boolean;
  allowAllFlags?: boolean;
};

/**
 * Sensitive config/credential path fragments that must still prompt even under
 * the blanket-allow modes (yolo/auto-allow and bypassPermissions). A write or
 * edit to any of these is a self-reconfig / credential-exposure vector.
 */
export const SENSITIVE_SAFETY_PATHS = [
  '.git/', '.ami/', '.bashrc', '.zshrc', '.profile', '.gitconfig',
  '.vscode/', '.ssh/', '.gnupg/', '.aws/', '.config/',
];

/**
 * Case-fold a path for a security boundary comparison, but ONLY on case-insensitive
 * filesystems (macOS/Windows), where `/Users/x/.SSH/id_rsa` and `/Users/x/.ssh/id_rsa`
 * name the SAME file yet differ byte-for-byte. On a case-sensitive FS (Linux) the
 * casings are genuinely distinct files, so this is a no-op to avoid wrongly merging
 * them. Mirrors the reference's normalizeCaseForComparison (filesystem.ts L81-92), closing
 * the fail-open where a differently-cased sensitive path slips a containment/safety
 * guard on macOS/Windows.
 */
export function normalizeCaseForComparison(p: string): string {
  return (process.platform === 'darwin' || process.platform === 'win32')
    ? p.toLowerCase()
    : p;
}

/**
 * A path token that survives permission validation as a LITERAL string but is
 * expanded differently by the shell at execution time — a TOCTOU gap where the
 * boundary check and the actual filesystem target disagree:
 *   - `shell-expansion`: `$VAR` / `${VAR}` / `$(cmd)` (Unix), `%VAR%` (Windows),
 *     or a leading `=` (zsh equals-expansion, e.g. `=rg` → `/usr/bin/rg`).
 *   - `tilde-variant`:   an unsupported `~` form (`~user`, `~+`, `~-`, `~N`) that
 *     `expandPath` leaves for the shell (it resolves only bare `~`, `~/`, `~\`).
 *   - `network-path`:    a UNC / network-share path (`//server`, `\\server`) or a
 *     WebDAV credential-leak marker (`@SSL@`, `DavWWWRoot`). Resolving one can leak
 *     NTLM/host credentials to a remote server, and its real target lives off the
 *     local filesystem where the workspace/forbidden guards cannot reason about it.
 *   - `command-substitution`: a backtick (`` ` ``) escape embedded in the path —
 *     the shell runs the backticked text as a command at execution time, so the
 *     validated literal and the real target diverge (a TOCTOU gap `$(...)` already
 *     covers under `shell-expansion`, but backtick substitution has no `$`).
 */
export const PATH_EXPANSION_HAZARDS = ['shell-expansion', 'tilde-variant', 'network-path', 'command-substitution'] as const;

export type PathExpansionHazard = (typeof PATH_EXPANSION_HAZARDS)[number];

/**
 * Classify a raw (un-expanded) path token for shell-expansion / tilde-variant
 * TOCTOU hazards. Mirrors the reference's pathValidation blanket-reject (pathValidation.ts
 * L393-L436): such a path is never silently auto-allowed — it is kept literal here
 * but the shell rewrites it at run time, so the permission decision and the real
 * target can diverge. Returns the hazard kind, or `null` when the path is inert.
 *
 * Tilde variants are detected on the RAW token because AMI's {@link expandPath}
 * resolves an unsupported `~user`/`~+` against cwd (→ `/cwd/~user`), so the `~`
 * no longer leads the expanded string.
 */
export function detectPathExpansionHazard(rawPath: string): PathExpansionHazard | null {
  const trimmed = rawPath.trim();
  if (trimmed.startsWith('~') && trimmed !== '~'
      && !trimmed.startsWith('~/') && !trimmed.startsWith('~\\')) {
    return 'tilde-variant';
  }
  // UNC / network-share prefix (`//server`, `\\server`) or a WebDAV credential-leak
  // marker (`@SSL@`, `DavWWWRoot`). A normal POSIX path begins with a SINGLE slash,
  // so a leading double slash/backslash is the UNC shape the reference blocks before
  // resolution (pathValidation.ts L1098-L1114) — resolving it can leak NTLM/host
  // credentials to a remote server and its target lives off the local filesystem.
  if (/^(?:\/\/|\\\\)/.test(trimmed) || /@SSL@|DavWWWRoot/i.test(trimmed)) {
    return 'network-path';
  }
  // A backtick escape runs the backticked text as a command at execution time
  // (`/project/`whoami`/x`), so the validated literal and the real target diverge.
  // `$(...)` substitution is already caught below by the `$` test.
  if (trimmed.includes('`')) {
    return 'command-substitution';
  }
  if (trimmed.includes('$') || trimmed.includes('%') || trimmed.startsWith('=')) {
    return 'shell-expansion';
  }
  return null;
}

/**
 * Best-effort strip of the shell-expansion / command-substitution constructs that
 * make a path {@link detectPathExpansionHazard unvalidatable}, yielding a plain
 * LITERAL "guess" of what the path might resolve to. Used ONLY to run a
 * deny-side (forbidden-path) re-check over an obscured path before it is routed to
 * a prompt — never to grant access. Mirrors the reference's `checkDenyRuleForGuessedPath`
 * (PowerShellTool/pathValidation.ts L984-L1008): an obscured path must not be able
 * to DOWNGRADE a hard credential/system deny into a mere "ask" by hiding the
 * forbidden segment behind a backtick / `$(…)` / `${VAR}` / `$VAR` / `%VAR%` /
 * leading-`=` construct — which, in an auto-allow session, an "ask" verdict would
 * then silently grant.
 *
 * Removes (in order): backtick-delimited runs, `$(…)` substitutions, `${…}` and
 * `$VAR` variable refs, `%VAR%` (Windows) refs, and a single leading `=`
 * (zsh equals-expansion). Returns the collapsed remainder. Pure, no I/O. This is a
 * guess heuristic, never a security boundary — the authoritative gate is still the
 * hazard→prompt fall-through for anything the guess does not catch.
 */
export function stripPathObscuring(rawPath: string): string {
  return rawPath
    // Backtick command substitution: `` `whoami` `` → removed entirely.
    .replace(/`[^`]*`/g, '')
    // `$(…)` substitution (single level is enough for a best-effort guess).
    .replace(/\$\([^)]*\)/g, '')
    // `${VAR}` references.
    .replace(/\$\{[^}]*\}/g, '')
    // Bare `$VAR` references.
    .replace(/\$[A-Za-z_][A-Za-z0-9_]*/g, '')
    // Windows `%VAR%` references.
    .replace(/%[^%]*%/g, '')
    // zsh equals-expansion: a single leading `=`.
    .replace(/^=/, '');
}

/**
 * Glob metacharacters (`*`, `?`, `[`, `]`) in a filesystem-path token. Parity:
 * the reference's GLOB_PATTERN_REGEX (pathValidation.ts L50). A path token carrying
 * one of these is a glob pattern whose runtime filesystem expansion cannot be
 * statically validated.
 */
export const GLOB_PATTERN_REGEX = /[*?[\]]/;

/** True if `p` contains a glob metacharacter (`* ? [ ]`). */
export function containsGlobPattern(p: string): boolean {
  return GLOB_PATTERN_REGEX.test(p);
}

/**
 * True if `p` contains a `..` path-traversal segment. Parity: the reference's
 * containsPathTraversal (utils/path.ts L133). A traversal glob such as
 * `/project/[star]/../../etc/shadow` is resolved-and-revalidated rather than
 * blindly gated, because collapsing the `..` segments pins down a concrete target
 * the forbidden/workspace guards can then inspect.
 */
export function containsPathTraversal(p: string): boolean {
  return /(?:^|[\\/])\.\.(?:[\\/]|$)/.test(p);
}

/**
 * Whether a read-path token is a glob that cannot be statically validated and must
 * therefore be forced to manual approval. Parity: the reference's pathValidation glob
 * handling (pathValidation.ts L1199-L1241, "finding #15"): a plain read glob's
 * pre-glob base is the only segment that gets realpath'd, so a symlink MATCHED
 * inside the expansion (e.g. `/project/[star]/passwd` with `/project/link → /etc`)
 * is never examined while the literal glob string still prefix-matches the
 * workspace. A traversal glob is excluded here (returns false) so it falls through
 * to normal resolve-and-revalidate, where its `..`-collapsed target is checked.
 */
export function isUnvalidatableReadGlob(rawPath: string): boolean {
  return containsGlobPattern(rawPath) && !containsPathTraversal(rawPath);
}

/**
 * Human-readable, user-facing reason for why an expansion-hazard path requires
 * manual approval, worded to match the reference's distinct messages so the user
 * understands why an otherwise-valid-looking path was gated.
 */
export function pathExpansionHazardReason(hazard: PathExpansionHazard): string {
  switch (hazard) {
    case 'tilde-variant':
      return 'Tilde expansion variants (~user, ~+, ~-) in paths require manual approval';
    case 'network-path':
      return 'UNC / remote network-share paths require manual approval (they can leak '
        + 'credentials to a remote server and resolve outside the local filesystem)';
    case 'command-substitution':
      return 'Backtick command substitution in paths requires manual approval';
    case 'shell-expansion':
    default:
      return 'Shell expansion syntax in paths requires manual approval';
  }
}
