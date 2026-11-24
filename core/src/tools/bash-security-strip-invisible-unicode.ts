// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

export interface SecurityCheckResult {
  safe: boolean;
  checkId: number;
  message: string;
}

export const CHECK_IDS = {
  CONTROL_CHARACTERS: 17,
  CARRIAGE_RETURN: 7,
  IFS_INJECTION: 11,
  PROC_ENVIRON_ACCESS: 13,
  OBFUSCATED_FLAGS: 4,
  UNICODE_WHITESPACE: 18,
  BRACE_EXPANSION: 16,
  DANGEROUS_VARIABLES: 6,
  ZSH_DANGEROUS_COMMANDS: 20,
  BACKSLASH_ESCAPED_OPERATORS: 21,
  NEWLINES: 1,
  SED_DANGEROUS: 22,
  AST_HIDDEN_COMMAND: 23,
  JQ_DANGEROUS: 24,
  SHELL_METACHARS_IN_COMMANDS: 25,
  COMMENT_QUOTE_DESYNC: 26,
  QUOTED_NEWLINE: 27,
  MID_WORD_HASH: 28,
  PROCESS_SUBSTITUTION: 29,
  BACKTICK_INJECTION: 30,
  COMMAND_SUBSTITUTION: 31,
  REDIRECTIONS: 32,
  SHELL_QUOTE_BUG: 33,
  BACKSLASH_WHITESPACE: 34,
  MALFORMED_TOKEN: 35,
  SAFE_HEREDOC: 36,
  INVISIBLE_UNICODE: 37,
  COMMAND_TOO_LONG: 38,
  DEV_TCP_UDP_REDIRECT: 39,
  GIT_CREDENTIAL_THEFT: 40,
  DNS_EXFILTRATION: 41,
  PERSISTENCE_COMMANDS: 42,
  DANGEROUS_DD: 43,
  BASE64_SHELL_PIPE: 44,
  PRIVILEGE_ESCALATION: 45,
  CONTAINER_ESCAPE: 46,
  PACKAGE_MANAGER_HIJACK: 47,
  NETWORK_MANIPULATION: 48,
  DATA_EXFILTRATION: 49,
  ESCAPE_OBFUSCATION: 50,
  UNSAFE_HELP_COMMAND: 51,
  ALIAS_REDEFINITION: 52,
  SOURCE_EXECUTION: 53,
  FILE_COMMAND_ARBITRARY_READ: 54,
  SHELL_STARTUP_WRITE: 55,
  DANGLING_OPERATOR: 56,
  SELF_PROCESS_KILL: 57,
  GIT_WORKTREE_ESCAPE: 58,
  GIT_INTERNAL_WRITE: 59,
  POWERSHELL_EXECUTION: 60,
  EVAL_LIKE_BUILTIN: 61,
  GIT_CONFIG_RCE: 62,
  INCOMPLETE_COMMAND: 63,
  ARCHIVE_TOOL_EXECUTION: 64,
  PARSE_ABORTED: 65,
  SUBCOMMAND_FANOUT: 66,
  PS4_TRACE_RCE: 67,
  ARITHMETIC_CONDITIONAL_INJECTION: 68,
  DESTRUCTIVE_TMUX: 69,
  UNC_PATH: 70,
  WRAPPER_FLAG_EVASION: 71,
  DANGEROUS_DECLARE: 72,
  VALIDATOR_EXCEPTION: 73,
  TILDE_EXPANSION_VARIANT: 74,
  DANGEROUS_REDIRECT_TARGET: 75,
  ASSIGNMENT_TILDE_EXPANSION: 76,
  HOMOGLYPH_COMMAND_NAME: 77,
} as const;

// eslint-disable-next-line no-control-regex
export const CONTROL_CHAR_RE = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/;

/**
 * Regex matching invisible/zero-width Unicode characters that can be embedded
 * in bash commands to bypass pattern-matching security checks. Covers:
 *   U+00AD  soft hyphen
 *   U+034F  combining grapheme joiner
 *   U+061C  Arabic letter mark
 *   U+180E  Mongolian vowel separator
 *   U+200B  zero-width space (ZWSP)
 *   U+200C  zero-width non-joiner (ZWNJ)
 *   U+200D  zero-width joiner (ZWJ)
 *   U+200E  left-to-right mark
 *   U+200F  right-to-left mark
 *   U+2060  word joiner
 *   U+2062  invisible times
 *   U+2063  invisible separator
 *   U+2064  invisible plus
 *   U+206A-U+206F  deprecated formatting characters
 *   U+FEFF  byte order mark / zero-width no-break space
 */
export const INVISIBLE_UNICODE_RE =
  /[\u00AD\u034F\u061C\u180E\u200B-\u200F\u2060\u2062-\u2064\u206A-\u206F\uFEFF]/;

export const INVISIBLE_UNICODE_RE_GLOBAL =
  /[\u00AD\u034F\u061C\u180E\u200B-\u200F\u2060\u2062-\u2064\u206A-\u206F\uFEFF]/g;

/**
 * Strip all invisible/zero-width Unicode characters from the input string.
 * These characters render as nothing visible but can break pattern matching
 * (e.g. 'r\u200Bm' looks like 'rm' to a human but won't match /\brm\b/).
 */
export function stripInvisibleUnicode(input: string): string {
  return input.replace(INVISIBLE_UNICODE_RE_GLOBAL, '');
}

/**
 * Returns true if the input contains any invisible/zero-width Unicode
 * characters that could be used to evade bash command classification.
 */
export function detectInvisibleUnicode(input: string): boolean {
  return INVISIBLE_UNICODE_RE.test(input);
}

/**
 * Valid `timeout`/`sleep`-style duration: an integer or decimal with a leading
 * digit and an optional single unit suffix (s/m/h/d). Mirrors the reference's
 * duration regex in utils/bash/ast.ts — it deliberately REJECTS `.5` (no
 * leading digit), `+5` (sign), and `inf` (word) so a non-duration token in the
 * duration position can never be mistaken for the wrapped command.
 */
// eslint-disable-next-line security/detect-unsafe-regex -- `\d+` then the optional `(?:\.\d+)?` arm (begins with a literal `.`, disjoint from the digit run) then an optional single `[smhd]`, fully anchored; no ambiguous overlap, linear
export const WRAPPER_DURATION_RE = /^\d+(?:\.\d+)?[smhd]?$/;

// Bounded-recursion safety cap on nested safe-wrapper prefixes (parity with
// the reference's prefix.ts wrapperCount/recursionDepth caps). A legitimate chain
// nests at most a handful of wrappers (`nohup timeout 5 nice -n5 stdbuf -oL cmd`
// = 4); a prefix that stacks far more is a pathological obfuscation / DoS attempt
// to blow past the per-wrapper flag parsers, so it fails closed and must be
// approved manually rather than auto-verified.
export const MAX_WRAPPER_NESTING = 10;

export function fail(checkId: number, message: string): SecurityCheckResult {
  return { safe: false, checkId, message };
}

export const PASS: SecurityCheckResult = { safe: true, checkId: 0, message: '' };

export function extractQuotedContent(command: string): { withDoubleQuotes: string; fullyUnquoted: string } {
  let withDoubleQuotes = '';
  let fullyUnquoted = '';
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let escaped = false;

  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (escaped) {
      escaped = false;
      if (!inSingleQuote) withDoubleQuotes += c;
      if (!inSingleQuote && !inDoubleQuote) fullyUnquoted += c;
      continue;
    }
    if (c === '\\' && !inSingleQuote) {
      escaped = true;
      if (!inSingleQuote) withDoubleQuotes += c;
      if (!inSingleQuote && !inDoubleQuote) fullyUnquoted += c;
      continue;
    }
    if (c === "'" && !inDoubleQuote) {
      inSingleQuote = !inSingleQuote;
      continue;
    }
    if (c === '"' && !inSingleQuote) {
      inDoubleQuote = !inDoubleQuote;
      continue;
    }
    if (!inSingleQuote) withDoubleQuotes += c;
    if (!inSingleQuote && !inDoubleQuote) fullyUnquoted += c;
  }

  return { withDoubleQuotes, fullyUnquoted };
}

export function stripSafeRedirections(content: string): string {
  return content
    .replace(/\s+[012]?\s*>&\s*[012](?=[\s;|&]|$)/g, '')
    .replace(/[012]?\s*>\s*\/dev\/null(?=[\s;|&]|$)/g, '')
    .replace(/\s*<\s*\/dev\/null(?=[\s;|&]|$)/g, '');
}

export function isEscapedAtPosition(content: string, pos: number): boolean {
  let count = 0;
  let i = pos - 1;
  while (i >= 0 && content[i] === '\\') {
    count++;
    i--;
  }
  return count % 2 === 1;
}

export function validateControlCharacters(command: string): SecurityCheckResult {
  if (CONTROL_CHAR_RE.test(command)) {
    return fail(CHECK_IDS.CONTROL_CHARACTERS, 'Command contains non-printable control characters that could bypass security checks');
  }
  return PASS;
}

export function validateCarriageReturn(command: string): SecurityCheckResult {
  if (!command.includes('\r')) return PASS;

  let inSingleQuote = false;
  let inDoubleQuote = false;
  let escaped = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (escaped) { escaped = false; continue; }
    if (c === '\\' && !inSingleQuote) { escaped = true; continue; }
    if (c === "'" && !inDoubleQuote) { inSingleQuote = !inSingleQuote; continue; }
    if (c === '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; continue; }
    if (c === '\r' && !inDoubleQuote) {
      return fail(CHECK_IDS.CARRIAGE_RETURN, 'Command contains carriage return (\\r) outside double quotes — potential tokenization bypass');
    }
  }
  return PASS;
}

export function validateIFSInjection(command: string): SecurityCheckResult {
  if (/\$IFS|\$\{[^}]*IFS/.test(command)) {
    return fail(CHECK_IDS.IFS_INJECTION, 'Command contains IFS variable usage which could bypass security validation');
  }
  return PASS;
}

export function validateProcEnvironAccess(command: string): SecurityCheckResult {
  if (/\/proc\/.*\/environ/.test(command)) {
    return fail(CHECK_IDS.PROC_ENVIRON_ACCESS, 'Command accesses /proc/*/environ which could expose sensitive environment variables');
  }
  return PASS;
}

export function validateDanglingOperator(command: string): SecurityCheckResult {
  const trimmed = command.trimEnd();
  const m = /(&&|\|\||[;|])$/.exec(trimmed);
  if (!m) return PASS;
  // A backslash-escaped trailing single-char operator (`\;`, `\|`) is a literal
  // word argument, NOT a dangling operator — this is the standard POSIX
  // `find … -exec cmd {} \;` terminator. Only skip when the `;`/`|` is preceded
  // by an ODD number of backslashes (genuinely escaped); an even count means the
  // backslashes are themselves literal and the operator still dangles.
  if (m[1] === ';' || m[1] === '|') {
    let backslashes = 0;
    for (let k = m.index - 1; k >= 0 && trimmed[k] === '\\'; k--) backslashes++;
    if (backslashes % 2 === 1) return PASS;
  }
  return fail(CHECK_IDS.DANGLING_OPERATOR, 'Blocked: command ends with a dangling operator (&&, ||, ;, |) — the next command could be injected');
}

export function validateIncompleteCommand(command: string): SecurityCheckResult {
  // A command that BEGINS with a continuation fragment — a leading tab, a bare
  // shell operator (`;`/`&&`/`||`/`|`/`>`/`>>`/`<`), or a leading flag with no
  // command name — is a truncated/injected fragment: no command name is present
  // for per-command classification, so it must not be auto-approved. (The
  // TRAILING dangling-operator case is handled by validateDanglingOperator.)
  if (/^\s*\t/.test(command)) {
    return fail(CHECK_IDS.INCOMPLETE_COMMAND, 'Blocked: command begins with a tab — likely an incomplete/continuation fragment');
  }
  const trimmed = command.trimStart();
  if (/^(?:&&|\|\||;|\||>>?|<)/.test(trimmed)) {
    return fail(CHECK_IDS.INCOMPLETE_COMMAND, 'Blocked: command begins with a shell operator — likely an injected continuation fragment');
  }
  if (/^-/.test(trimmed)) {
    return fail(CHECK_IDS.INCOMPLETE_COMMAND, 'Blocked: command begins with a flag with no command name — likely an incomplete fragment');
  }
  return PASS;
}

export function validateObfuscatedFlags(command: string): SecurityCheckResult {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const baseCommand = stripped.trim().split(/\s+/)[0] || '';
  const hasOps = /[|&;]/.test(command);
  if (baseCommand === 'echo' && !hasOps) return PASS;

  if (/\$'[^']*'/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains ANSI-C quoting ($\'...\') which can hide characters');
  }
  if (/\$"[^"]*"/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains locale quoting ($"...") which can hide characters');
  }
  if (/\$['"]{2}\s*-/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains empty special quotes before dash (potential bypass)');
  }
  if (/(?:^|\s)(?:''|"")+\s*-/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains empty quotes before dash (potential bypass)');
  }
  // Detect homogeneous empty quote pair + quoted dash: """-f"
  if (/(?:^|\s)(?:"")+"-/.test(command) || /(?:^|\s)(?:'')+'-/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains empty quote pairs before quoted dash (split-quote bypass)');
  }
  // Detect 3+ consecutive quotes at word start
  if (/(?:^|\s)['"]{3,}/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains 3+ consecutive quotes at word start (obfuscation)');
  }
  // Adjacent quote chaining: "-""exec"
  if (/"-"['"]+\w/.test(command) || /'-'['"]+\w/.test(command)) {
    return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains adjacent quote chaining around dash (split-quote flag bypass)');
  }
  // Quote-state tracking flag scanner for split-quote obfuscation
  const FLAG_CONTINUATION = /[a-zA-Z0-9_=]/;
  const words = command.split(/\s+/);
  for (const word of words) {
    if (word.length < 3) continue;
    let inSQ = false, inDQ = false, dashSeen = false, quoteTransitions = 0;
    for (let i = 0; i < word.length; i++) {
      const c = word[i];
      if (c === "'" && !inDQ) { inSQ = !inSQ; quoteTransitions++; }
      else if (c === '"' && !inSQ) { inDQ = !inDQ; quoteTransitions++; }
      else if (c === '-' && !inSQ && !inDQ) dashSeen = true;
      else if (dashSeen && FLAG_CONTINUATION.test(c)) {
        if (quoteTransitions >= 3) {
          return fail(CHECK_IDS.OBFUSCATED_FLAGS, 'Command contains multi-quote-transition flag (split-quote obfuscation)');
        }
      }
    }
  }
  return PASS;
}

export function validateInvisibleUnicode(command: string): SecurityCheckResult {
  if (detectInvisibleUnicode(command)) {
    return fail(CHECK_IDS.INVISIBLE_UNICODE, 'Command contains invisible zero-width Unicode characters that could bypass security checks');
  }
  return PASS;
}

export function validateBraceExpansion(fullyUnquoted: string, originalCommand: string): SecurityCheckResult {
  let openCount = 0;
  let closeCount = 0;
  for (let i = 0; i < fullyUnquoted.length; i++) {
    if (fullyUnquoted[i] === '{' && !isEscapedAtPosition(fullyUnquoted, i)) openCount++;
    else if (fullyUnquoted[i] === '}' && !isEscapedAtPosition(fullyUnquoted, i)) closeCount++;
  }

  if (openCount > 0 && closeCount > openCount) {
    return fail(CHECK_IDS.BRACE_EXPANSION, 'Command has excess closing braces after quote stripping — possible brace expansion obfuscation');
  }

  if (openCount > 0 && /['"][{}]['"]/.test(originalCommand)) {
    return fail(CHECK_IDS.BRACE_EXPANSION, 'Command contains quoted brace character inside brace context (potential obfuscation)');
  }

  for (let i = 0; i < fullyUnquoted.length; i++) {
    if (fullyUnquoted[i] !== '{') continue;
    if (isEscapedAtPosition(fullyUnquoted, i)) continue;

    let depth = 1;
    let matchingClose = -1;
    for (let j = i + 1; j < fullyUnquoted.length; j++) {
      if (fullyUnquoted[j] === '{' && !isEscapedAtPosition(fullyUnquoted, j)) depth++;
      else if (fullyUnquoted[j] === '}' && !isEscapedAtPosition(fullyUnquoted, j)) {
        depth--;
        if (depth === 0) { matchingClose = j; break; }
      }
    }
    if (matchingClose === -1) continue;

    let innerDepth = 0;
    for (let k = i + 1; k < matchingClose; k++) {
      const ch = fullyUnquoted[k];
      if (ch === '{' && !isEscapedAtPosition(fullyUnquoted, k)) innerDepth++;
      else if (ch === '}' && !isEscapedAtPosition(fullyUnquoted, k)) innerDepth--;
      else if (innerDepth === 0) {
        if (ch === ',' || (ch === '.' && k + 1 < matchingClose && fullyUnquoted[k + 1] === '.')) {
          return fail(CHECK_IDS.BRACE_EXPANSION, 'Command contains brace expansion that could alter command parsing');
        }
      }
    }
  }

  return PASS;
}

export function validateDangerousVariables(fullyUnquoted: string): SecurityCheckResult {
  if (
    /[<>|]\s*\$[A-Za-z_]/.test(fullyUnquoted) ||
    /\$[A-Za-z_][A-Za-z0-9_]*\s*[|<>]/.test(fullyUnquoted)
  ) {
    return fail(CHECK_IDS.DANGEROUS_VARIABLES, 'Command contains variables in dangerous contexts (redirections or pipes)');
  }
  return PASS;
}
