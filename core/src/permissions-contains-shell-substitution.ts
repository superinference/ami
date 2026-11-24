// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { FLAG_PATTERN, validateFlagArgument } from './permissions-permission-verdict';
import type { SafeCommandFlagDef } from './permissions-permission-verdict';

/**
 * Getopt-accurate flag walk for a command whose flags carry {@link FlagArgType}
 * metadata. Mirrors the reference's `validateFlags`:
 *  - `--flag=value` validates the inline value (and rejects a value on a 'none' flag);
 *  - an arg-taking flag consumes the NEXT token as its value (rejecting a missing
 *    arg, a dash-led 'string' value — type confusion — and an invalid typed value);
 *  - a bundled short cluster (e.g. `-He`) is only safe when EVERY member is a
 *    known no-arg flag — a bundled arg-taking flag would consume the next token in
 *    GNU getopt, a differential the validator cannot model, so it is rejected.
 * Blocked flags always win. Unknown flags fall back to the allowed/strict rules.
 */
export function validateTypedCommandFlags(
  flagDef: SafeCommandFlagDef,
  args: string[],
): boolean {
  const argTypes = flagDef.argTypes!;
  let i = 0;
  while (i < args.length) {
    const token = args[i]!;
    // POSIX `--` end-of-options: stop treating later tokens as flags (unless the
    // command ignores `--`). Keeps the typed walker aligned with the real binary.
    if (token === '--' && flagDef.respectsDoubleDash !== false) break;
    if (!FLAG_PATTERN.test(token)) { i++; continue; } // positional
    const hasEquals = token.includes('=');
    const flag = token.split('=')[0]!;
    const inlineValue = token.slice(flag.length + 1);
    const bare = flag.replace(/^--?/, '');
    if (flagDef.blocked.has(token) || flagDef.blocked.has(flag) || flagDef.blocked.has(bare)) return false;

    const argType = argTypes[flag];
    if (argType === undefined) {
      // A short cluster like `-He` — safe only if every member is a known no-arg
      // flag. Any arg-taking or unknown member means we cannot safely model the
      // token consumption, so reject (conservative, matches the reference).
      if (/^-[a-zA-Z0-9]{2,}$/.test(flag)) {
        for (const ch of flag.slice(1)) {
          if (argTypes['-' + ch] !== 'none') return false;
        }
        i++; continue;
      }
      // Unknown single flag: honor the strict allowlist like the untyped path.
      if (flagDef.strict && !flagDef.allowed.has(flag)) return false;
      i++; continue;
    }

    if (argType === 'none') {
      if (hasEquals) return false; // a no-arg flag given `=value`
      i++; continue;
    }

    // Arg-taking flag: resolve its value (inline after `=` or the next token).
    let value: string;
    if (hasEquals) {
      value = inlineValue;
      i++;
    } else {
      const next = args[i + 1];
      if (next === undefined || (next.length > 1 && FLAG_PATTERN.test(next))) return false; // missing arg
      value = next;
      i += 2;
    }
    if (argType === 'string' && value.startsWith('-')) return false; // type confusion
    if (!validateFlagArgument(value, argType)) return false;
  }
  return true;
}

/**
 * Check whether a jq/yq expression contains dangerous constructs.
 * Blocks `-f`, `--rawfile`, `--slurpfile`, `-L` flags and `system()` calls.
 */
export function isJqDangerous(args: string[]): boolean {
  const dangerousFlags = ['-f', '--rawfile', '--slurpfile', '-L', '--library-path', '--from-file'];
  for (const arg of args) {
    if (dangerousFlags.includes(arg)) return true;
  }
  // Check for system() call in jq expression
  const expr = args.find(a => !a.startsWith('-'));
  if (expr && /\bsystem\b/.test(expr)) return true;
  return false;
}

/**
 * Check whether a python3 -e/-c one-liner is read-only.
 * Blocks imports of os, subprocess, shutil which can mutate state.
 */
export function isScriptOneLinerSafe(lang: string, code: string): boolean {
  if (lang === 'python3' || lang === 'python') {
    return !/\bimport\s+(os|subprocess|shutil)\b/.test(code) &&
           !/\bfrom\s+(os|subprocess|shutil)\b/.test(code);
  }
  if (lang === 'node') {
    return !/\brequire\s*\(\s*['"](?:child_process|fs)['"]\s*\)/.test(code) &&
           !/\bimport\b.*\bfrom\s+['"](?:child_process|fs)['"]/.test(code);
  }
  if (lang === 'ruby') {
    return !/\bsystem\b/.test(code) && !/\bexec\b/.test(code) &&
           !/`/.test(code) && !/\bIO\.popen\b/.test(code);
  }
  if (lang === 'perl') {
    return !/\bsystem\b/.test(code) && !/\bexec\b/.test(code) &&
           !/`/.test(code) && !/\bqx\b/.test(code);
  }
  return false;
}

/**
 * Strip quoted sections from a command to analyze unquoted content.
 */
export function stripQuotedSections(command: string): string {
  // Remove double-quoted sections
  let result = command.replace(/"[^"]*"/g, '');
  // Remove single-quoted sections
  result = result.replace(/'[^']*'/g, '');
  return result;
}

/**
 * True when a command cannot be tokenized by a real shell because a quote is left
 * open — an UNBALANCED single or double quote. A single-pass quote-state walker with
 * bash escape semantics: `\` escapes the next char OUTSIDE single quotes (inside
 * `'…'` a backslash is literal and cannot escape the closing quote); double quotes
 * are inert inside single quotes and vice-versa. If the scan ends still inside a
 * quote, the shell's own tokenizer would error — so the naive whitespace/regex
 * splitting AMI's classifier uses would see a DIFFERENT token stream than the shell.
 *
 * The permission classifier calls this to FAIL CLOSED: an unparseable command is
 * classified `'unsafe'` (→ an explicit prompt) rather than being split with naive
 * regexes and possibly auto-allowed. Mirrors the reference's "parse first, return
 * passthrough / require-permission when parsing fails" gate (BashTool
 * readOnlyValidation.ts: tryParseShellCommand → requires permission on failure).
 */
export function hasUnbalancedQuotes(command: string): boolean {
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i]!;
    if (escaped) { escaped = false; continue; }
    // A backslash escapes the next char everywhere EXCEPT inside single quotes,
    // where it is a literal (bash: single quotes have no escape mechanism).
    if (c === '\\' && !inSingle) { escaped = true; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
  }
  return inSingle || inDouble;
}

/**
 * Check whether a command contains unquoted variable expansions or glob
 * patterns that could be exploited for injection.
 *
 * Uses a single-pass quote-state walker (NOT a regex over quote-stripped text) so
 * single-quote escape semantics are correct — in bash `\` is a literal no-op inside
 * single quotes, so a stray `'\'` cannot desync the tracker and drop a later
 * expansion. It also covers EVERY `$`-expansion shape: a following variable-name
 * char, a `${...}`/`$(...)` substitution start, OR a special parameter
 * (`$@ $* $# $? $! $$ $- $0-$9`) that the old `\$[A-Za-z_{(]` regex missed — plus
 * the unquoted glob metacharacters `* ? [` (filename expansion / injection vectors).
 * Mirrors the reference's containsUnquotedExpansion char-walk (readOnlyValidation.ts).
 */
export function containsUnquotedExpansion(command: string): boolean {
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i]!;
    if (escaped) { escaped = false; continue; }
    if (c === '\\' && !inSingle) { escaped = true; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
    if (inSingle || inDouble) continue; // quoted content is inert
    if (c === '$') {
      const next = command[i + 1];
      // variable-name char, `${`/`$(` substitution, or a special parameter.
      if (next && /[A-Za-z_@*#?!$0-9({-]/.test(next)) return true;
    }
    // Unquoted glob metacharacters (`*`, `?`, bracket expression `[`).
    if (c === '*' || c === '?' || c === '[') return true;
  }
  return false;
}

/**
 * Check whether a command contains unquoted shell substitutions ($VAR,
 * ${VAR}, $(cmd)) but tolerate glob patterns (* ? [...]).
 * Used for safe read-only commands where globs are harmless filename expansion.
 */
export function containsShellSubstitution(command: string): boolean {
  // A quote-tracking scan (not a regex over quote-stripped text) so single-quote
  // escape semantics are correct AND every `$`-expansion shape is covered. In
  // bash a `\` is a NO-OP inside single quotes (literal), so escapes are only
  // honored OUTSIDE single quotes — otherwise `'\'` desyncs the tracker and the
  // scanner stays "in a quote" for the rest of the command, missing later
  // expansions (the reference's containsUnquotedExpansion single-quote guard).
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i]!;
    if (escaped) { escaped = false; continue; }
    if (c === '\\' && !inSingle) { escaped = true; continue; }
    if (c === "'" && !inDouble) { inSingle = !inSingle; continue; }
    if (c === '"' && !inSingle) { inDouble = !inDouble; continue; }
    if (inSingle || inDouble) continue; // quoted — AMI tolerates `$VAR`/`"$f"` here
    // UNQUOTED `$`: match a following variable-name char, a special parameter
    // ($@ $* $# $? $! $$ $0-9 $-), or the start of a ${...} / $(...) substitution.
    // AMI deliberately tolerates `$` inside quotes (single AND double) so the
    // for-loop-with-glob perk (`for f in *.ts; do cat "$f"; done`) stays auto-safe
    // — an intentional superset divergence from the reference; this adds the previously
    // missed UNQUOTED special-parameter shapes ($@/$*/$?/…) to the parser-
    // differential defense without regressing that perk.
    if (c === '$') {
      const next = command[i + 1];
      if (next && /[A-Za-z_@*#?!$0-9({-]/.test(next)) return true;
    }
  }
  return false;
}

/**
 * Typed safe-flag allowlists for the read-only docker subcommands that still take
 * flags. A subcommand match alone is NOT enough to auto-allow `docker logs` /
 * `docker inspect`: an unrecognized flag can smuggle behavior (and `--format` /
 * `-f` meanings differ per subcommand), so each known flag is pinned to its
 * arity — `'none'` (a boolean switch) or `'string'` (consumes one value). Any
 * flag NOT in the map falls through to a prompt (conservative). Subcommands absent
 * here (`ps`, `images`, `stats`, …) keep their existing name-only auto-allow.
 * Mirrors the reference's `DOCKER_READ_ONLY_COMMANDS` safeFlags
 * (utils/shell/readOnlyCommandValidation.ts).
 */
export const DOCKER_SAFE_SUBCOMMAND_FLAGS: Record<string, Record<string, 'none' | 'string'>> = {
  logs: {
    '--follow': 'none', '-f': 'none',
    '--tail': 'string', '-n': 'string',
    '--timestamps': 'none', '-t': 'none',
    '--since': 'string', '--until': 'string',
    '--details': 'none',
  },
  inspect: {
    '--format': 'string', '-f': 'string',
    '--type': 'string',
    '--size': 'none', '-s': 'none',
  },
};

/**
 * True when every flag passed to a read-only docker subcommand is in its typed
 * safe-flag allowlist (positional args — container names/IDs — are always fine).
 * A `--flag=value` form carries its own value; a `'string'` flag in `--flag value`
 * form consumes the following token. Returns true for subcommands with no
 * flag constraints. See {@link DOCKER_SAFE_SUBCOMMAND_FLAGS}.
 */
export function dockerReadOnlyFlagsSafe(sub: string, args: string[]): boolean {
  const allow = DOCKER_SAFE_SUBCOMMAND_FLAGS[sub];
  if (!allow) return true;
  for (let i = 0; i < args.length; i++) {
    const tok = args[i]!;
    if (!tok.startsWith('-')) continue; // positional (container/id) — allowed
    const eq = tok.indexOf('=');
    const name = eq >= 0 ? tok.slice(0, eq) : tok;
    const kind = allow[name];
    if (kind === undefined) return false; // unknown flag → not auto-safe
    if (kind === 'string' && eq < 0) i++; // consumes the next token as its value
  }
  return true;
}

/** SQL statements that are destructive. */
export const DESTRUCTIVE_SQL_PATTERNS = [
  /\bDROP\s+TABLE\b/i,
  /\bDROP\s+DATABASE\b/i,
  // `DROP SCHEMA public CASCADE` destroys an entire schema and everything in it;
  // the reference's SQL pattern covers DROP of TABLE/DATABASE/SCHEMA alike
  // (destructiveCommandWarning.ts L71-74). Without this arm a schema drop is not
  // classified destructive by the SQL branch.
  /\bDROP\s+SCHEMA\b/i,
  /\bDELETE\s+FROM\b/i,
  /\bTRUNCATE\s+TABLE\b/i,
  /\bTRUNCATE\b/i,
];

/**
 * Hardline patterns — blocked unconditionally, even in auto-allow mode.
 * These prevent catastrophic system damage regardless of settings.
 */
export const HARDLINE_PATTERNS: Array<{ pattern: RegExp; description: string }> = [
  // Match `rm … /` targeting the filesystem root, including a trailing token/
  // redirect (`rm -rf / 2>/dev/null`) and the catastrophic glob (`rm -rf /*`).
  // The arg run is `[^;&|]*` so it cannot span a command separator and
  // false-positive on a later bare `/` (e.g. `rm x.txt && ls /`).
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/(?=\s|$|[;&|><*])/,  description: 'rm -r on filesystem root' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/(home|etc|usr|var|bin|sbin|boot|lib|lib64|Volumes|mnt|media)\b/, description: 'rm -r on system directory' },
  { pattern: /\brm\s+(-[a-zA-Z]*r[a-zA-Z]*\s+|[^;&|]*\s+)\/private\/(etc|var|tmp|home)\b/, description: 'rm -r on macOS system directory (/private/*)' },
  { pattern: /\bmkfs\b/, description: 'filesystem format' },
  { pattern: /\bdd\b.*\bof\s*=\s*\/dev\/[sh]d/, description: 'dd to block device' },
  { pattern: /:\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;\s*:/, description: 'fork bomb' },
  { pattern: /\b(shutdown|reboot|halt|poweroff)\b/, description: 'system power control' },
  { pattern: /\binit\s+[06]\b/, description: 'init level change' },
  { pattern: /\bsystemctl\s+(poweroff|reboot|halt)\b/, description: 'systemctl power control' },
  { pattern: />\s*\/dev\/[sh]d/, description: 'redirect to block device' },
  { pattern: /\bchmod\b.{0,20}777\s+\//, description: 'chmod 777 on system path' },
  { pattern: /\bcurl\b.*\|\s*(ba|z|da|fi|k|c|tc)?sh\b/, description: 'pipe curl to shell' },
  { pattern: /\bwget\b.*\|\s*(ba|z|da|fi|k|c|tc)?sh\b/, description: 'pipe wget to shell' },
  { pattern: /\bpkill\b.*\bnode\b/, description: 'pkill node would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bkillall\b.*\bnode\b/, description: 'killall node would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bpkill\b.*\btsx\b/, description: 'pkill tsx would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bkillall\b.*\btsx\b/, description: 'killall tsx would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\bpkill\b.*\bsuperinference\b/i, description: 'would kill AMI — use kill <pid> to target a specific process' },
  { pattern: /\b(curl|wget|fetch|nc|ncat|http|https)\b[^\n]*(?:\b(?:169\.254\.169\.254|metadata\.google\.internal|metadata\.google\.com|100\.100\.100\.200|instance-data)\b|fd00:ec2::254)/i, description: 'cloud instance-metadata credential fetch (SSRF/credential-exfil vector)' },
  // PowerShell power-control / disk-format cmdlets — the Windows equivalents of
  // the POSIX shutdown/reboot/mkfs hardlines above (AMI runs PowerShell on Windows).
  { pattern: /\b(Stop-Computer|Restart-Computer)\b/i, description: 'PowerShell power control' },
  { pattern: /\bClear-RecycleBin\b/i, description: 'PowerShell empties the recycle bin (irreversible)' },
  { pattern: /\b(Format-Volume|Clear-Disk)\b/i, description: 'PowerShell disk format / wipe' },
  // Same-UID heap/credential scraping vectors. A prompt-injected command running
  // as AMI's own UID can attach a debugger/tracer to the agent (or a peer) process
  // and read provider API keys (ANTHROPIC_API_KEY / Bearer tokens) straight out of
  // its heap, or read them via /proc/<pid>/mem. the reference closes this by calling
  // prctl(PR_SET_DUMPABLE,0) via Bun FFI (upstreamproxy.ts); AMI runs on the Node
  // runtime (no libc FFI), so it blocks the attach vectors unconditionally at the
  // permission boundary instead — the hardline block overrides every mode.
  { pattern: /\bgdb\b[^\n]*\s--?p(?:id)?\b/, description: 'gdb attach to a live process (in-memory credential scraping vector)' },
  { pattern: /\blldb\b[^\n]*\s(?:-p|--pid|--attach-pid|--attach-name|-n)\b/, description: 'lldb attach to a live process (in-memory credential scraping vector)' },
  { pattern: /\b(?:strace|ltrace)\b[^\n]*\s-p\b/, description: 'strace/ltrace attach to a live process (in-memory credential scraping vector)' },
  { pattern: /\/proc\/(?:\d+|self|\$\{?[\w}$]*)\/mem\b/, description: 'reading /proc/<pid>/mem (process-memory / credential scraping vector)' },
  { pattern: /\bprocess_vm_readv\b/, description: 'process_vm_readv cross-process memory read (credential scraping vector)' },
];

/** Serializable snapshot of the built-in (default) permission patterns. */
export interface DefaultPermissionPatterns {
  /** Bash commands auto-classified SAFE (read-only) — never prompt. */
  autoAllowBashCommands: string[];
  /** Unconditionally BLOCKED command patterns (hardline deny — interrupts). */
  hardlineDenied: Array<{ pattern: string; description: string }>;
  /** Commands classified DESTRUCTIVE (irreversible) — require confirmation. */
  destructiveCommands: string[];
  /** SQL statement patterns classified DESTRUCTIVE. */
  destructiveSqlPatterns: string[];
  /** Bash base-commands `acceptEdits` mode auto-approves without a prompt. */
  acceptEditsAutoAllowedCommands: string[];
  /** PowerShell write cmdlets `acceptEdits` mode auto-approves without a prompt. */
  acceptEditsAutoAllowedPowerShellCmdlets: string[];
}

/**
 * Expand two documented evasions of the unconditional hardline guard so the
 * patterns still fire: (a) a catastrophic base built via a bare assignment and
 * referenced by variable (`c=rm; $c -rf /`), and (b) a quoted payload inside
 * `sh -c '...'` / `bash -c "..."` / `eval '...'` where the trailing quote
 * defeats the end-anchored patterns. Returns the original command augmented with
 * the normalized/unwrapped forms (newline-joined) so pattern matching sees both.
 */
export const HARDLINE_BASES = 'rm|dd|mkfs|shutdown|reboot|halt|poweroff|chmod|curl|wget|pkill|killall';

export function normalizeForHardline(command: string): string {
  let out = command;

  // (a) Substitute variable references with their assigned values. Covers both a
  // bare dangerous base (`name=rm`) and a full quoted command (`c='rm -rf /'`),
  // so `eval "$c"` is normalized to the real command.
  // Optional assignment-prefix keywords (export/declare/typeset/readonly/local)
  // must not let `export c=rm; $c -rf /` bypass the variable-indirection guard.
  const ASSIGN_PREFIX = `(?:(?:export|declare|typeset|readonly|local)\\s+)?`;
  const assigns = new Map<string, string>();
  const baseAssignRe = new RegExp(
    `(?:^|;|&&|\\|\\||\\n)\\s*${ASSIGN_PREFIX}([A-Za-z_][A-Za-z0-9_]*)=(?:\\/[^\\s"';|&]*\\/)?(${HARDLINE_BASES})(?=\\s|;|&|\\||$)`,
    'g',
  );
  let a: RegExpExecArray | null;
  while ((a = baseAssignRe.exec(command)) !== null) assigns.set(a[1], a[2]);
  const quotedAssignRe = new RegExp(
    `(?:^|;|&&|\\|\\||\\n)\\s*${ASSIGN_PREFIX}([A-Za-z_][A-Za-z0-9_]*)=(['"])([\\s\\S]*?)\\2`,
    'g',
  );
  let q: RegExpExecArray | null;
  while ((q = quotedAssignRe.exec(command)) !== null) assigns.set(q[1], q[3]);
  // Multiple space-separated assignments on one statement, e.g.
  // `export a=1 c=rm` — scan every name=BASE pair within each statement
  // (prefixed by export/declare/… or a bare assignment run). Split first to
  // keep the per-pair regex simple and ReDoS-safe.
  const pairRe = new RegExp(`\\b([A-Za-z_][A-Za-z0-9_]*)=(?:\\/[^\\s"';|&]*\\/)?(${HARDLINE_BASES})(?=\\s|$|[|&])`, 'g');
  for (const rawSeg of command.split(/[;\n]|&&|\|\|/)) {
    const seg = rawSeg.replace(/^\s*(?:export|declare|typeset|readonly|local)\s+/, '');
    let pr: RegExpExecArray | null;
    pairRe.lastIndex = 0;
    while ((pr = pairRe.exec(seg)) !== null) assigns.set(pr[1], pr[2]);
  }
  if (assigns.size) {
    out = out.replace(/\$\{?([A-Za-z_][A-Za-z0-9_]*)\}?/g, (full, name) =>
      assigns.has(name) ? assigns.get(name)! : full);
  }

  // (b) Expose command-substitution-built bases: $(echo rm)/`which rm` -> rm.
  out = out.replace(/\$\(\s*(?:echo|which|command\s+-v)\s+([^)]*)\)/g, '$1');
  out = out.replace(/`\s*(?:echo|which|command\s+-v)\s+([^`]*)`/g, '$1');

  // (c) Recursively unwrap sh/bash/eval -c quoted payloads (handles nesting),
  // appending each inner script as its own line so end-anchored patterns match.
  for (let depth = 0; depth < 5; depth++) {
    const wrapRe = /\b(?:sh|bash|zsh|dash|ksh|eval)\b[^'"]*(['"])([\s\S]*?)\1/g;
    const inners: string[] = [];
    let w: RegExpExecArray | null;
    while ((w = wrapRe.exec(out)) !== null) {
      if (w[2] && !out.includes('\n' + w[2])) inners.push(w[2]);
    }
    if (!inners.length) break;
    out += '\n' + inners.join('\n');
  }
  return out;
}
