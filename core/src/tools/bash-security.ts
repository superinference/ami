import { parseBashAST, extractCommands, CommandNode } from './bash-parser';
import { normalizeCommand } from './command-normalize';

export interface SecurityCheckResult {
  safe: boolean;
  checkId: number;
  message: string;
}

const CHECK_IDS = {
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
} as const;

// eslint-disable-next-line no-control-regex
const CONTROL_CHAR_RE = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/;

const UNICODE_WS_RE =
  new RegExp("[\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000\uFEFF]");

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
const INVISIBLE_UNICODE_RE =
  /[\u00AD\u034F\u061C\u180E\u200B-\u200F\u2060\u2062-\u2064\u206A-\u206F\uFEFF]/;

const INVISIBLE_UNICODE_RE_GLOBAL =
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

const ZSH_DANGEROUS_COMMANDS = new Set([
  'zmodload', 'emulate',
  'sysopen', 'sysread', 'syswrite', 'sysseek',
  'zpty', 'ztcp', 'zsocket', 'mapfile',
  'zf_rm', 'zf_mv', 'zf_ln', 'zf_chmod', 'zf_chown', 'zf_mkdir', 'zf_rmdir', 'zf_chgrp',
]);

const ZSH_PRECOMMAND_MODIFIERS = new Set(['command', 'builtin', 'noglob', 'nocorrect']);

const SAFE_WRAPPERS = new Set(['timeout', 'time', 'nice', 'nohup', 'stdbuf', 'env']);

function stripSafeWrappers(command: string): string {
  let remaining = command.trim();
  let changed = true;
  while (changed) {
    changed = false;
    const tokens = remaining.split(/\s+/);
    if (tokens.length === 0) break;
    const first = tokens[0];
    if (!SAFE_WRAPPERS.has(first)) break;

    if (first === 'env') {
      let i = 1;
      while (i < tokens.length && /^[A-Za-z_]\w*=/.test(tokens[i])) i++;
      if (i < tokens.length) {
        remaining = tokens.slice(i).join(' ');
        changed = true;
      } else {
        break;
      }
    } else if (first === 'timeout' || first === 'nice' || first === 'stdbuf') {
      let i = 1;
      while (i < tokens.length && tokens[i].startsWith('-')) i++;
      if (first === 'timeout' && i < tokens.length && /^\d/.test(tokens[i])) i++;
      if (first === 'nice' && i < tokens.length && /^\d+$/.test(tokens[i])) i++;
      if (i < tokens.length) {
        remaining = tokens.slice(i).join(' ');
        changed = true;
      } else {
        break;
      }
    } else {
      if (tokens.length > 1) {
        remaining = tokens.slice(1).join(' ');
        changed = true;
      } else {
        break;
      }
    }
  }
  return remaining;
}

function fail(checkId: number, message: string): SecurityCheckResult {
  return { safe: false, checkId, message };
}

const PASS: SecurityCheckResult = { safe: true, checkId: 0, message: '' };

function extractQuotedContent(command: string): { withDoubleQuotes: string; fullyUnquoted: string } {
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

function stripSafeRedirections(content: string): string {
  return content
    .replace(/\s+[012]?\s*>&\s*[012](?=[\s;|&]|$)/g, '')
    .replace(/[012]?\s*>\s*\/dev\/null(?=[\s;|&]|$)/g, '')
    .replace(/\s*<\s*\/dev\/null(?=[\s;|&]|$)/g, '');
}

function isEscapedAtPosition(content: string, pos: number): boolean {
  let count = 0;
  let i = pos - 1;
  while (i >= 0 && content[i] === '\\') {
    count++;
    i--;
  }
  return count % 2 === 1;
}

function validateControlCharacters(command: string): SecurityCheckResult {
  if (CONTROL_CHAR_RE.test(command)) {
    return fail(CHECK_IDS.CONTROL_CHARACTERS, 'Command contains non-printable control characters that could bypass security checks');
  }
  return PASS;
}

function validateCarriageReturn(command: string): SecurityCheckResult {
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

function validateIFSInjection(command: string): SecurityCheckResult {
  if (/\$IFS|\$\{[^}]*IFS/.test(command)) {
    return fail(CHECK_IDS.IFS_INJECTION, 'Command contains IFS variable usage which could bypass security validation');
  }
  return PASS;
}

function validateProcEnvironAccess(command: string): SecurityCheckResult {
  if (/\/proc\/.*\/environ/.test(command)) {
    return fail(CHECK_IDS.PROC_ENVIRON_ACCESS, 'Command accesses /proc/*/environ which could expose sensitive environment variables');
  }
  return PASS;
}

function validateObfuscatedFlags(command: string): SecurityCheckResult {
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

function validateUnicodeWhitespace(command: string): SecurityCheckResult {
  if (UNICODE_WS_RE.test(command)) {
    return fail(CHECK_IDS.UNICODE_WHITESPACE, 'Command contains Unicode whitespace characters that could cause parsing inconsistencies');
  }
  return PASS;
}

function validateInvisibleUnicode(command: string): SecurityCheckResult {
  if (detectInvisibleUnicode(command)) {
    return fail(CHECK_IDS.INVISIBLE_UNICODE, 'Command contains invisible zero-width Unicode characters that could bypass security checks');
  }
  return PASS;
}

function validateBraceExpansion(fullyUnquoted: string, originalCommand: string): SecurityCheckResult {
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

function validateDangerousVariables(fullyUnquoted: string): SecurityCheckResult {
  if (
    /[<>|]\s*\$[A-Za-z_]/.test(fullyUnquoted) ||
    /\$[A-Za-z_][A-Za-z0-9_]*\s*[|<>]/.test(fullyUnquoted)
  ) {
    return fail(CHECK_IDS.DANGEROUS_VARIABLES, 'Command contains variables in dangerous contexts (redirections or pipes)');
  }
  return PASS;
}

function validateZshDangerousCommands(command: string): SecurityCheckResult {
  const trimmed = command.trim();
  const tokens = trimmed.split(/\s+/);
  let baseCmd = '';
  for (const token of tokens) {
    if (/^[A-Za-z_]\w*=/.test(token)) continue;
    if (ZSH_PRECOMMAND_MODIFIERS.has(token)) continue;
    baseCmd = token;
    break;
  }

  if (ZSH_DANGEROUS_COMMANDS.has(baseCmd)) {
    return fail(CHECK_IDS.ZSH_DANGEROUS_COMMANDS, `Command uses Zsh-specific '${baseCmd}' which can bypass security checks`);
  }

  if (baseCmd === 'fc' && /\s-\S*e/.test(trimmed)) {
    return fail(CHECK_IDS.ZSH_DANGEROUS_COMMANDS, "Command uses 'fc -e' which can execute arbitrary commands via editor");
  }

  return PASS;
}

function validateBackslashEscapedOperators(command: string): SecurityCheckResult {
  let inSingleQuote = false;
  let inDoubleQuote = false;
  const operators = new Set([';', '|', '&', '<', '>']);

  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (c === '\\' && !inSingleQuote) {
      i++; // skip next char unconditionally (even inside double quotes for correct parity)
      if (i < command.length && !inDoubleQuote && operators.has(command[i])) {
        return fail(CHECK_IDS.BACKSLASH_ESCAPED_OPERATORS, 'Command contains backslash before a shell operator — can hide command structure');
      }
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
  }
  return PASS;
}

function validateNewlines(command: string): SecurityCheckResult {
  const extracted = extractQuotedContent(command);
  if (!/[\n\r]/.test(extracted.fullyUnquoted)) return PASS;
  if (/(?<![\\])[\n\r]\s*\S/.test(extracted.fullyUnquoted)) {
    return fail(CHECK_IDS.NEWLINES, 'Command contains newlines that could separate multiple commands');
  }
  return PASS;
}

function validateSedDangerous(command: string): SecurityCheckResult {
  const trimmed = command.trim();
  const tokens = trimmed.split(/\s+/);
  let baseCmd = '';
  for (const token of tokens) {
    if (/^[A-Za-z_]\w*=/.test(token)) continue;
    baseCmd = token;
    break;
  }
  if (baseCmd !== 'sed') return PASS;

  // Extract sed expression (everything after flags)
  const sedArgs = trimmed.replace(/^sed\s+/, '');

  // Block non-ASCII characters in sed expressions (potential injection)
  if (/[^\x00-\x7F]/.test(sedArgs)) {
    return fail(CHECK_IDS.SED_DANGEROUS, 'sed expression contains non-ASCII characters');
  }

  // Block 'e' flag in substitutions (executes replacement as shell command)
  if (/\be\b/.test(trimmed) && /s[\/|#@,]/.test(trimmed)) {
    const substitutionWithExec = /s([\/|#@,])(?:[^\\]|\\.)*?\1(?:[^\\]|\\.)*?\1[^;]*e/;
    if (substitutionWithExec.test(trimmed)) {
      return fail(CHECK_IDS.SED_DANGEROUS, "sed substitution with 'e' flag executes the replacement as a shell command");
    }
  }

  // Block 'w' command (writes to file)
  if (/\bw\s+\S/.test(trimmed) || /['"]\s*w\s+\S/.test(trimmed)) {
    const wCommandPattern = /(?:^|[;}\s])w\s+(\S+)/;
    const match = trimmed.match(wCommandPattern);
    if (match) {
      return fail(CHECK_IDS.SED_DANGEROUS, `sed 'w' command writes to file: ${match[1]}`);
    }
  }

  // Block 'R'/'W' commands (read/write arbitrary files)
  if (/\bR\s+\S/.test(trimmed) || /\bW\s+\S/.test(trimmed)) {
    return fail(CHECK_IDS.SED_DANGEROUS, "sed 'R' or 'W' commands can read/write arbitrary files");
  }

  // Block 'y' (transliterate) with potential follow-up commands
  if (/(?:^|[;'"])y[\/|#]/.test(sedArgs)) {
    return fail(CHECK_IDS.SED_DANGEROUS, "sed 'y' (transliterate) command can be chained with dangerous operations");
  }

  // Block negation operator '!' which can invert address matching
  if (/!\s*[dewWrRqQ]/.test(sedArgs)) {
    return fail(CHECK_IDS.SED_DANGEROUS, "sed negation operator '!' with command can bypass address restrictions");
  }

  // Block tilde addresses (GNU sed extension)
  if (/~/.test(sedArgs) && !/s[\/|#@,].*~/.test(sedArgs)) {
    return fail(CHECK_IDS.SED_DANGEROUS, "sed tilde address is a GNU extension with non-obvious semantics");
  }

  // Block in-place editing of sensitive paths
  if (/(?:^|\s)-i(?:\s|$|[.'"\\])/.test(trimmed) || /--in-place/.test(trimmed)) {
    const sensitivePatterns = [
      /\/etc\//, /\/root\//, /\.ssh\//, /\.env\b/, /\.bashrc/, /\.profile/,
      /\.zshrc/, /\.bash_profile/, /authorized_keys/, /shadow/, /passwd/,
    ];
    for (const pat of sensitivePatterns) {
      if (pat.test(trimmed)) {
        return fail(CHECK_IDS.SED_DANGEROUS, `sed in-place edit targets sensitive path matching ${pat.source}`);
      }
    }
  }

  return PASS;
}

function validateJqDangerous(command: string): SecurityCheckResult {
  if (/\bjq\b/.test(command) && /\bsystem\s*\(/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'jq system() calls can execute arbitrary commands');
  }
  if (/\b[gm]?awk\b/.test(command) && /\bsystem\s*\(/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'awk system() calls can execute arbitrary commands');
  }
  if (/\b[gm]?awk\b/.test(command) && /\|\s*getline\b/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'awk pipe-to-getline can execute arbitrary commands');
  }
  return PASS;
}

function validateShellMetacharsInCommands(command: string): SecurityCheckResult {
  // Exclude | (pipe) — piping to/from find/grep/xargs is normal shell usage
  if (/\b(find|grep|xargs)\b/.test(command) && /[;&`$]/.test(command)) {
    const hasQuoted = /(['"]).*[;&`$].*\1/.test(command);
    if (!hasQuoted) return fail(CHECK_IDS.SHELL_METACHARS_IN_COMMANDS, 'Unquoted shell metacharacters in find/grep/xargs command');
  }
  return PASS;
}

function validateCommentQuoteDesync(command: string): SecurityCheckResult {
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let escaped = false;

  for (let i = 0; i < command.length; i++) {
    const char = command[i];

    if (escaped) { escaped = false; continue; }

    if (inSingleQuote) {
      if (char === "'") inSingleQuote = false;
      continue;
    }

    if (char === '\\') { escaped = true; continue; }

    if (inDoubleQuote) {
      if (char === '"') inDoubleQuote = false;
      continue;
    }

    if (char === "'") { inSingleQuote = true; continue; }
    if (char === '"') { inDoubleQuote = true; continue; }

    if (char === '#') {
      const lineEnd = command.indexOf('\n', i);
      const commentText = command.slice(i + 1, lineEnd === -1 ? command.length : lineEnd);
      if (/['"]/.test(commentText)) {
        return fail(CHECK_IDS.COMMENT_QUOTE_DESYNC, 'Quote characters inside # comment can desync quote tracking');
      }
      if (lineEnd === -1) break;
      i = lineEnd;
    }
  }
  return PASS;
}

function validateQuotedNewline(command: string): SecurityCheckResult {
  if (/\$'[^']*\\n[^']*'/.test(command)) {
    return fail(CHECK_IDS.QUOTED_NEWLINE, 'ANSI-C quoted newline ($\'...\\n...\') can inject commands');
  }

  if (!command.includes('\n') || !command.includes('#')) return PASS;

  let inSingleQuote = false;
  let inDoubleQuote = false;
  let escaped = false;

  for (let i = 0; i < command.length; i++) {
    const char = command[i];

    if (escaped) { escaped = false; continue; }
    if (char === '\\' && !inSingleQuote) { escaped = true; continue; }
    if (char === "'" && !inDoubleQuote) { inSingleQuote = !inSingleQuote; continue; }
    if (char === '"' && !inSingleQuote) { inDoubleQuote = !inDoubleQuote; continue; }

    if (char === '\n' && (inSingleQuote || inDoubleQuote)) {
      const lineStart = i + 1;
      const nextNewline = command.indexOf('\n', lineStart);
      const lineEnd = nextNewline === -1 ? command.length : nextNewline;
      const nextLine = command.slice(lineStart, lineEnd);
      if (nextLine.trim().startsWith('#')) {
        return fail(CHECK_IDS.QUOTED_NEWLINE, 'Quoted newline followed by #-prefixed line can hide arguments from line-based permission checks');
      }
    }
  }
  return PASS;
}

function validateMidWordHash(command: string): SecurityCheckResult {
  if (/\S#\S/.test(command) && !/'[^']*#[^']*'/.test(command) && !/"[^"]*#[^"]*"/.test(command)) {
    return fail(CHECK_IDS.MID_WORD_HASH, 'Mid-word # character may cause unexpected comment behavior in some shells');
  }
  return PASS;
}

function validateProcessSubstitution(command: string): SecurityCheckResult {
  if (/[<>]\(/.test(command)) return fail(CHECK_IDS.PROCESS_SUBSTITUTION, 'Process substitution (<() or >()) can execute arbitrary commands');
  return PASS;
}

function validateBacktickInjection(command: string): SecurityCheckResult {
  const cleaned = command.replace(/\\`/g, '');
  if (/`[^`]+`/.test(cleaned)) {
    return fail(CHECK_IDS.BACKTICK_INJECTION, 'Backtick command substitution can execute arbitrary commands; use $() instead');
  }
  return PASS;
}

function validateShellQuoteBug(command: string): SecurityCheckResult {
  if (/'\\'/.test(command)) {
    return fail(CHECK_IDS.SHELL_QUOTE_BUG, 'Blocked: shell-quote single-quote backslash pattern detected (library misparse vector)');
  }
  return PASS;
}

const HEREDOC_IN_SUBSTITUTION = /\$\(cat[ \t]*<<(-?)[ \t]*(?:'+[A-Za-z_]\w*'+|\\[A-Za-z_]\w*)/;

function stripSafeHeredocSubstitutions(command: string): string | null {
  if (!HEREDOC_IN_SUBSTITUTION.test(command)) return null;

  const heredocPattern = /\$\(cat[ \t]*<<(-?)[ \t]*(?:'+([A-Za-z_]\w*)'+|\\([A-Za-z_]\w*))/g;
  let result = command;
  let found = false;
  let match;
  const ranges: Array<{ start: number; end: number }> = [];

  while ((match = heredocPattern.exec(command)) !== null) {
    if (match.index > 0 && command[match.index - 1] === '\\') continue;
    const delimiter = match[2] || match[3];
    if (!delimiter) continue;
    const isDash = match[1] === '-';
    const operatorEnd = match.index + match[0].length;

    const afterOperator = command.slice(operatorEnd);
    const openLineEnd = afterOperator.indexOf('\n');
    if (openLineEnd === -1) continue;
    if (!/^[ \t]*$/.test(afterOperator.slice(0, openLineEnd))) continue;

    const bodyStart = operatorEnd + openLineEnd + 1;
    const bodyLines = command.slice(bodyStart).split('\n');
    for (let i = 0; i < bodyLines.length; i++) {
      const rawLine = bodyLines[i]!;
      const line = isDash ? rawLine.replace(/^\t*/, '') : rawLine;
      if (line.startsWith(delimiter)) {
        const after = line.slice(delimiter.length);
        let closePos = -1;
        if (/^[ \t]*\)/.test(after)) {
          const lineStart = bodyStart + bodyLines.slice(0, i).join('\n').length + (i > 0 ? 1 : 0);
          closePos = command.indexOf(')', lineStart);
        } else if (after === '') {
          const nextLine = bodyLines[i + 1];
          if (nextLine !== undefined && /^[ \t]*\)/.test(nextLine)) {
            const nextLineStart = bodyStart + bodyLines.slice(0, i + 1).join('\n').length + 1;
            closePos = command.indexOf(')', nextLineStart);
          }
        }
        if (closePos !== -1) {
          ranges.push({ start: match.index, end: closePos + 1 });
          found = true;
        }
        break;
      }
    }
  }

  if (!found) return null;
  for (let i = ranges.length - 1; i >= 0; i--) {
    const r = ranges[i]!;
    result = result.slice(0, r.start) + result.slice(r.end);
  }
  return result;
}

function validateCommandSubstitution(command: string, extracted: { fullyUnquoted: string }): SecurityCheckResult {
  let content = extracted.fullyUnquoted;

  const stripped = stripSafeHeredocSubstitutions(command);
  if (stripped !== null) {
    const reExtracted = extractQuotedContent(stripped);
    content = reExtracted.fullyUnquoted;
  }

  const patterns: [RegExp, string][] = [
    [/\$\(/, '$() command substitution'],
    [/\$\{/, '${} parameter expansion'],
    [/\$\[/, '$[] arithmetic expansion'],
    [/~\[/, '~[] Zsh parameter expansion'],
    [/=\(/, '=() Zsh process substitution'],
    [/\(e:/, '(e:...:) Zsh glob qualifier eval'],
    [/\(\+/, '(+ Zsh glob qualifier'],
    [/\}\s*always\s*\{/, '} always { Zsh always block'],
    [/<#/, '<# PowerShell comment'],
  ];
  for (const [pat, desc] of patterns) {
    if (pat.test(content)) {
      return fail(CHECK_IDS.COMMAND_SUBSTITUTION, `Blocked: ${desc} detected`);
    }
  }
  return PASS;
}

function validateRedirections(command: string, extracted: { fullyUnquoted: string }): SecurityCheckResult {
  const content = stripSafeRedirections(extracted.fullyUnquoted);
  if (/[<>]/.test(content)) {
    return fail(CHECK_IDS.REDIRECTIONS, 'Blocked: unquoted redirect operators detected');
  }
  return PASS;
}

function validateDevTcpUdpRedirects(command: string): SecurityCheckResult {
  if (/\/dev\/(?:tcp|udp)\//i.test(command)) {
    return fail(CHECK_IDS.DEV_TCP_UDP_REDIRECT, 'Blocked: /dev/tcp or /dev/udp network redirect detected');
  }
  return PASS;
}

function validateBackslashEscapedWhitespace(command: string, extracted: { fullyUnquoted: string }): SecurityCheckResult {
  const content = extracted.fullyUnquoted;
  if (/\\[ \t]/.test(content)) {
    return fail(CHECK_IDS.BACKSLASH_WHITESPACE, 'Blocked: backslash-escaped whitespace detected (potential path traversal)');
  }
  return PASS;
}

function validateMalformedTokenInjection(command: string): SecurityCheckResult {
  if (/['"]\s*[;|&]/.test(command) && (command.split("'").length % 2 === 0 || command.split('"').length % 2 === 0)) {
    return fail(CHECK_IDS.MALFORMED_TOKEN, 'Blocked: potentially malformed quoting with command separators');
  }
  return PASS;
}

const AST_BLOCKED_COMMANDS = new Set([
  'nc', 'ncat', 'socat', 'telnet',
  'dd',
  'mkfs', 'fdisk', 'parted',
  'iptables', 'ip6tables', 'nft',
  'crontab',
  'useradd', 'userdel', 'usermod', 'groupadd', 'groupdel',
  'mount', 'umount',
  'insmod', 'rmmod', 'modprobe',
]);

function validateASTCommands(command: string): SecurityCheckResult {
  const ast = parseBashAST(command);
  if (!ast) return PASS;

  const commands = extractCommands(ast);
  for (const cmd of commands) {
    const base = cmd.replace(/^.*\//, '');
    if (AST_BLOCKED_COMMANDS.has(base)) {
      return fail(CHECK_IDS.AST_HIDDEN_COMMAND, `AST analysis found blocked command '${base}' in compound expression`);
    }
  }

  function checkSubshellDepth(node: CommandNode, depth: number): SecurityCheckResult {
    if (depth > 5) {
      return fail(CHECK_IDS.AST_HIDDEN_COMMAND, 'Excessive subshell nesting depth (>5) — potential obfuscation');
    }
    if (node.children) {
      for (const child of node.children) {
        const childDepth = (child.type === 'subshell' || child.type === 'command_substitution') ? depth + 1 : depth;
        const r = checkSubshellDepth(child, childDepth);
        if (!r.safe) return r;
      }
    }
    return PASS;
  }

  const depthResult = checkSubshellDepth(ast, ast.type === 'subshell' ? 1 : 0);
  if (!depthResult.safe) return depthResult;

  return PASS;
}

const MAX_AUTO_APPROVE_LENGTH = 10_000;

export function validateBashSecurity(command: string, options?: { detachedMode?: boolean }): SecurityCheckResult {
  if (command.length > MAX_AUTO_APPROVE_LENGTH) {
    return fail(CHECK_IDS.COMMAND_TOO_LONG, `Command is ${command.length} characters (limit ${MAX_AUTO_APPROVE_LENGTH}) — too long to auto-approve`);
  }

  const detached = options?.detachedMode === true;
  let result: SecurityCheckResult;

  // Invisible Unicode check — must run FIRST, before any pattern matching,
  // because zero-width characters can split tokens and evade all regexes below.
  result = validateInvisibleUnicode(command);
  if (!result.safe) return result;

  // Pre-validators (run before any content extraction)
  result = validateShellQuoteBug(command);
  if (!result.safe) return result;

  result = validateControlCharacters(command);
  if (!result.safe) return result;

  result = validateCarriageReturn(command);
  if (!result.safe) return result;

  result = validateIFSInjection(command);
  if (!result.safe) return result;

  result = validateMalformedTokenInjection(command);
  if (!result.safe) return result;

  result = validateProcEnvironAccess(command);
  if (!result.safe) return result;

  result = validateObfuscatedFlags(command);
  if (!result.safe) return result;

  result = validateUnicodeWhitespace(command);
  if (!result.safe) return result;

  // Detached mode: skip newline validation — multi-line commands (heredocs,
  // compound build scripts) are model-generated, not adversarial injection.
  if (!detached) {
    result = validateNewlines(command);
    if (!result.safe) return result;
  }

  const extracted = extractQuotedContent(command);
  const fullyUnquoted = stripSafeRedirections(extracted.fullyUnquoted);

  // New critical validators on extracted content
  result = validateCommandSubstitution(command, extracted);
  if (!result.safe) return result;

  // /dev/tcp and /dev/udp are bash network redirects — always block, even in
  // detached mode, since they open arbitrary network connections.
  result = validateDevTcpUdpRedirects(command);
  if (!result.safe) return result;

  // Detached mode: skip redirect validation — file output redirections
  // (capturing test output, writing reports) are core to SWE-bench workflows.
  if (!detached) {
    result = validateRedirections(command, extracted);
    if (!result.safe) return result;
  }

  result = validateBackslashEscapedWhitespace(command, extracted);
  if (!result.safe) return result;

  result = validateBraceExpansion(fullyUnquoted, command);
  if (!result.safe) return result;

  result = validateDangerousVariables(fullyUnquoted);
  if (!result.safe) return result;

  const unwrapped = stripSafeWrappers(command);
  result = validateZshDangerousCommands(unwrapped);
  if (!result.safe) return result;

  result = validateBackslashEscapedOperators(command);
  if (!result.safe) return result;

  result = validateSedDangerous(command);
  if (!result.safe) return result;

  result = validateASTCommands(command);
  if (!result.safe) return result;

  result = validateJqDangerous(command);
  if (!result.safe) return result;

  // Detached mode: skip shell metachar validation in find/grep/xargs —
  // pipes with fd redirects (2>&1 | grep) are standard build patterns.
  if (!detached) {
    result = validateShellMetacharsInCommands(command);
    if (!result.safe) return result;
  }

  result = validateCommentQuoteDesync(command);
  if (!result.safe) return result;

  result = validateQuotedNewline(command);
  if (!result.safe) return result;

  result = validateMidWordHash(command);
  if (!result.safe) return result;

  result = validateProcessSubstitution(command);
  if (!result.safe) return result;

  result = validateBacktickInjection(command);
  if (!result.safe) return result;

  return PASS;
}

const PATH_COMMANDS: Record<string, (args: string[]) => string[]> = {
  cd: (a) => a.filter(x => !x.startsWith('-')),
  ls: (a) => a.filter(x => !x.startsWith('-')),
  cat: (a) => a.filter(x => !x.startsWith('-')),
  head: (a) => a.filter(x => !x.startsWith('-')),
  tail: (a) => a.filter(x => !x.startsWith('-')),
  sort: (a) => a.filter(x => !x.startsWith('-')),
  wc: (a) => a.filter(x => !x.startsWith('-')),
  touch: (a) => a.filter(x => !x.startsWith('-')),
  mkdir: (a) => a.filter(x => !x.startsWith('-')),
  rm: (a) => a.filter(x => !x.startsWith('-') && x !== '--'),
  rmdir: (a) => a.filter(x => !x.startsWith('-')),
  mv: (a) => a.filter(x => !x.startsWith('-')),
  cp: (a) => a.filter(x => !x.startsWith('-')),
  find: (a) => { const paths: string[] = []; for (let i = 0; i < a.length; i++) { if (!a[i].startsWith('-') && a[i] !== '--') paths.push(a[i]); else break; } return paths; },
  stat: (a) => a.filter(x => !x.startsWith('-')),
  diff: (a) => a.filter(x => !x.startsWith('-')),
  file: (a) => a.filter(x => !x.startsWith('-')),
  strings: (a) => a.filter(x => !x.startsWith('-')),
  grep: (a) => { const idx = a.findIndex(x => !x.startsWith('-')); return idx >= 0 ? a.slice(idx + 1).filter(x => !x.startsWith('-')) : []; },
  rg: (a) => { const idx = a.findIndex(x => !x.startsWith('-')); return idx >= 0 ? a.slice(idx + 1).filter(x => !x.startsWith('-')) : []; },
  sed: (a) => a.filter(x => !x.startsWith('-') && !/^s[/|]/.test(x)),
  awk: (a) => a.filter(x => !x.startsWith('-') && !x.startsWith("'")),
  git: (a) => a.slice(1).filter(x => !x.startsWith('-')),
};

const FILE_TAKING_FLAGS: Record<string, Set<string>> = {
  grep: new Set(['-f', '--file', '--include-from']),
  rg: new Set(['-f', '--file']),
  sort: new Set(['-o', '--output']),
  xargs: new Set(['-a', '--arg-file']),
  tar: new Set(['-f', '--file', '-C', '--directory']),
  curl: new Set(['-o', '--output', '-K', '--config']),
  wget: new Set(['-O', '--output-document', '-i', '--input-file']),
  rsync: new Set(['--include-from', '--exclude-from', '--files-from']),
  tee: new Set([]),
  dd: new Set([]),
};

function looksLikePath(value: string): boolean {
  return /^[/~.]/.test(value) || value.includes('/');
}

export function extractOptionValuePaths(cmd: string, args: string[]): string[] {
  const paths: string[] = [];
  const flagSet = FILE_TAKING_FLAGS[cmd];

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    const eqIdx = arg.indexOf('=');
    if (arg.startsWith('-') && eqIdx > 0) {
      const value = arg.slice(eqIdx + 1).replace(/^["']|["']$/g, '');
      if (value && looksLikePath(value)) paths.push(value);
      continue;
    }

    if (flagSet && arg.startsWith('-') && flagSet.has(arg) && i + 1 < args.length) {
      const value = args[i + 1].replace(/^["']|["']$/g, '');
      if (value) paths.push(value);
      i++;
    }
  }

  return paths;
}

/**
 * Credential / persistence paths that an autonomous agent must never write to or
 * copy out of via bash — the SSH-key / cloud-cred exfil & persistence vector.
 */
const CREDENTIAL_PATH_PATTERNS: RegExp[] = [
  /(^|\/)\.ssh\//,
  /\.aws\/credentials/,
  /\.aws\/config$/,
  /\.config\/gcloud\//,
  /(^|\/)\.gnupg\//,
  /\.docker\/config\.json/,
  /\.kube\/config/,
  /(^|\/)\.netrc$/,
  /(^|\/)_netrc$/,
  /(^|\/)\.git-credentials$/,
  /\.config\/git\/credentials$/,
  /\.config\/gh\//,
  /(^|\/)\.npmrc$/,
  /\.claude\/\.credentials\.json$/,
  /\/etc\/(passwd|shadow|sudoers)\b/,
];

export function isCredentialPath(p: string): boolean {
  const norm = p.replace(/\\/g, '/');
  return CREDENTIAL_PATH_PATTERNS.some((re) => re.test(norm));
}

/**
 * True if any path token in the command points at a known credential/secret
 * file. Used to ensure even read-only commands (cat/head/less/…) touching
 * secrets are never auto-classified 'safe' — they must prompt.
 */
export function commandTargetsCredentialPath(command: string): boolean {
  try {
    for (const p of extractCommandPaths(command)) {
      if (isCredentialPath(p)) return true;
    }
  } catch { /* best-effort */ }
  // Fall back to a raw token scan for paths extractCommandPaths does not model.
  for (const tok of command.split(/[\s;|&]+/)) {
    const t = tok.replace(/^["']|["']$/g, '');
    if (t && isCredentialPath(t)) return true;
  }
  return false;
}

/**
 * Block write-intent bash commands that target (or copy out of) a credential
 * path. Unconditional — runs regardless of permission mode. Covers cp/mv/tee/
 * install/ln/dd/truncate/chmod/chown, `sed -i`, and `>`/`>>` redirects.
 */
/** Collect candidate write-target paths from a write-intent bash command. */
function collectWriteTargets(command: string): string[] {
  const writeIntent = /\b(rm|cp|mv|tee|install|ln|dd|truncate|chmod|chown)\b|\bsed\b[^|]*\s-i\b|>>?\s*\S/;
  if (!writeIntent.test(command)) return [];
  const targets: string[] = [];
  // Redirect targets, incl. the `>|` noclobber-override form.
  for (const m of command.matchAll(/>>?\|?\s*(?:"([^"]+)"|'([^']+)'|([^\s;|&]+))/g)) {
    targets.push(m[1] ?? m[2] ?? m[3]);
  }
  for (const m of command.matchAll(/\bof=(?:"([^"]+)"|'([^']+)'|([^\s;|&]+))/g)) {
    targets.push(m[1] ?? m[2] ?? m[3]);
  }
  try { for (const p of extractCommandPaths(command)) targets.push(p); } catch { /* best-effort */ }
  // For any write-intent verb, treat every non-flag token as a candidate target
  // (covers tee/install/ln/truncate/chmod/chown whose args extractCommandPaths
  // does not model).
  if (/\b(rm|cp|mv|tee|install|ln|dd|truncate|chmod|chown)\b/.test(command)) {
    const VERB = /^(rm|cp|mv|tee|install|ln|dd|truncate|chmod|chown|sed|echo|printf|cat|sudo|env)$/;
    for (const tok of command.split(/[\s;|&]+/)) {
      const t = tok.replace(/^["']|["']$/g, '');
      if (t && !t.startsWith('-') && !t.includes('=') && !VERB.test(t)) targets.push(t);
      // Extract path values from --key=value tokens (e.g. --output=~/.ssh/authorized_keys)
      const eqIdx = t.indexOf('=');
      if (t.startsWith('-') && eqIdx > 0) {
        const val = t.slice(eqIdx + 1).replace(/^["']|["']$/g, '');
        if (val && looksLikePath(val)) targets.push(val);
      }
    }
  }
  return targets.filter(Boolean);
}

/**
 * Block write-intent bash commands that target (or copy out of) a credential
 * path. Unconditional — runs regardless of permission mode. Covers cp/mv/tee/
 * install/ln/dd/truncate/chmod/chown, `sed -i`, and `>`/`>>` redirects.
 */
export function detectCredentialPathWrite(command: string): { blocked: boolean; description: string | null } {
  for (const t of collectWriteTargets(command)) {
    if (isCredentialPath(t)) {
      return { blocked: true, description: `write to credential path "${t}"` };
    }
  }
  return { blocked: false, description: null };
}

/**
 * Block write-intent bash commands that would tamper with the agent's own
 * session transcript store (.superinference/sessions/*.jsonl). Unconditional.
 */
export function detectSessionTranscriptWrite(command: string): { blocked: boolean; description: string | null } {
  // Match both writes INSIDE the sessions dir and destructive ops on the dir
  // itself (e.g. `rm -rf .superinference/sessions`): trailing slash OR end-of-token.
  const re = /(^|\/)\.superinference\/sessions(\/|$)/;
  const norm = command.replace(/\\/g, '/');
  for (const t of collectWriteTargets(command)) {
    if (re.test(t.replace(/\\/g, '/'))) {
      return { blocked: true, description: `write to protected session transcript path "${t}"` };
    }
  }
  // `find <...sessions...> -delete` / `-exec rm`/`-execdir rm` deletes files
  // without a write-intent verb that collectWriteTargets models — catch it.
  // Use a looser path match (the dir token is followed by a space, not `/`).
  const sessionsAnywhere = /(^|\/)\.superinference\/sessions(?![\w])/;
  if (/\bfind\b/.test(norm) && sessionsAnywhere.test(norm) &&
      (/\s-delete\b/.test(norm) || /-exec(dir)?\s+rm\b/.test(norm))) {
    return { blocked: true, description: 'find-based deletion of a protected session transcript path' };
  }
  return { blocked: false, description: null };
}

// ---------------------------------------------------------------------------
// Nuclear command blocklist — un-bypassable, no flag/mode/env can disable
// ---------------------------------------------------------------------------
// These commands can brick a machine or cause unrecoverable mass data loss.
// Blocked unconditionally, even in --yolo mode.

const NUCLEAR_RM_TARGETS = new Set(['/', '/*', '/home', '/root', '/etc', '/usr', '/var', '/bin', '/sbin', '/lib', '/lib64', '/boot', '/opt', '/sys', '/proc', '/dev']);
const HOME_EXPANSIONS = /^(?:~\/?|\$HOME\/?|\$\{HOME\}\/?|\$\(echo\s+~\)\/?|\$\(eval\s+echo\s+~\)\/?|\/home\/\w+\/?)$/;
const RM_RF_RE = /\brm\b[^;]*(?:-\w*[rf]\w*\s+[^;]*-\w*[rf]\w*|-\w*r\w*f\w*|-\w*f\w*r\w*|--recursive\b[^;]*--force\b|--force\b[^;]*--recursive\b)/;
const DISK_WIPE_RE = /\b(?:mkfs|mkfs\.\w+|wipefs|shred)\b[^|;]*(?:\/dev\/[a-z]|\/dev\/mapper\/)/;
const DD_WIPE_RE = /\bdd\b[^|;]*(?:of\s*=\s*\/dev\/(?:sd|hd|vd|nvme|xvd|loop|mapper)|of\s*=\s*\/dev\/(?:zero|null|urandom))/;
const FORK_BOMB_RE = /:\(\)\s*\{\s*:\s*\|\s*:\s*&\s*\}\s*;?\s*:/;
// eslint-disable-next-line security/detect-unsafe-regex -- each inner group always advances (greedy \S+ then \s+), linear
const COMMAND_PREFIX_RE = /^(?:sudo\s+(?:-\S+\s+)*|env\s+(?:-\S+\s+)*(?:\S+=\S+\s+)*|[A-Za-z_]\w*=(?:"[^"]*"|'[^']*'|\S+)\s+|command\s+|\\|\/(?:usr\/)?(?:s?bin)\/)/;

export interface NuclearCheckResult {
  blocked: boolean;
  reason?: string;
}

export function detectNuclearCommand(command: string): NuclearCheckResult {
  if (!command || !command.trim()) return { blocked: false };

  const stripped = stripInvisibleUnicode(command);
  const normalized = normalizeCommand(stripped);

  if (FORK_BOMB_RE.test(normalized)) {
    return { blocked: true, reason: 'fork bomb detected — would crash the system by spawning infinite processes' };
  }

  // Check each semicolon/&&/||-separated segment independently so that
  // "echo hi; rm -rf /" is caught even though the full string doesn't
  // start with rm.
  const segments = normalized.split(/\s*(?:;|&&|\|\|)\s*/);
  for (const segment of segments) {
    const result = checkSegment(segment);
    if (result.blocked) return result;
  }

  // Check inside `bash -c "..."` / `sh -c "..."` nested commands.
  const nestedResult = checkNestedShellCommands(normalized);
  if (nestedResult.blocked) return nestedResult;

  return { blocked: false };
}

/**
 * Extract and check commands nested inside `bash -c "..."` / `sh -c "..."` and
 * `$(...)` command substitutions. Without this, nuclear payloads inside quoted
 * string arguments or subshells bypass the segment-based check.
 */
function checkNestedShellCommands(command: string): NuclearCheckResult {
  const shellExecRe = /\b(?:ba)?sh\s+-c\s+/g;
  let match;
  while ((match = shellExecRe.exec(command)) !== null) {
    const afterFlag = command.slice(match.index + match[0].length);
    const inner = extractQuotedArg(afterFlag);
    if (!inner) continue;
    const innerSegments = inner.split(/\s*(?:;|&&|\|\|)\s*/);
    for (const seg of innerSegments) {
      const result = checkSegment(seg);
      if (result.blocked) {
        return { blocked: true, reason: `${result.reason} (inside bash -c)` };
      }
    }
  }

  // Check inside $(...) command substitutions
  const subshellRe = /\$\(([^)]+)\)/g;
  let subMatch;
  while ((subMatch = subshellRe.exec(command)) !== null) {
    const inner = subMatch[1];
    const innerSegments = inner.split(/\s*(?:;|&&|\|\|)\s*/);
    for (const seg of innerSegments) {
      const result = checkSegment(seg);
      if (result.blocked) {
        return { blocked: true, reason: `${result.reason} (inside subshell)` };
      }
    }
  }

  return { blocked: false };
}

function extractQuotedArg(s: string): string | null {
  const trimmed = s.trimStart();
  if (trimmed.startsWith('"')) {
    const end = trimmed.indexOf('"', 1);
    if (end !== -1) return trimmed.slice(1, end);
  }
  if (trimmed.startsWith("'")) {
    const end = trimmed.indexOf("'", 1);
    if (end !== -1) return trimmed.slice(1, end);
  }
  if (trimmed.startsWith('$\'')) {
    const end = trimmed.indexOf("'", 2);
    if (end !== -1) return trimmed.slice(2, end);
  }
  // Unquoted: take until whitespace or shell meta
  const unquoted = trimmed.match(/^(\S+)/);
  return unquoted ? unquoted[1] : null;
}

function stripCommandPrefix(segment: string): string {
  let s = segment.trim();
  let prev = '';
  while (prev !== s) {
    prev = s;
    s = s.replace(COMMAND_PREFIX_RE, '').trim();
  }
  return s;
}

function stripQuotes(s: string): string {
  return s.replace(/^["']|["']$/g, '');
}

function checkSegment(segment: string): NuclearCheckResult {
  const bare = stripCommandPrefix(segment);

  if (DD_WIPE_RE.test(bare)) {
    return { blocked: true, reason: 'dd targeting a block device — would wipe the disk' };
  }

  if (DISK_WIPE_RE.test(bare)) {
    return { blocked: true, reason: 'filesystem format/wipe command — would destroy the disk' };
  }

  if (RM_RF_RE.test(bare)) {
    const targets = extractRmTargets(bare);
    for (const target of targets) {
      const unquoted = stripQuotes(target);
      const clean = unquoted.replace(/\/+$/, '') || '/';
      if (NUCLEAR_RM_TARGETS.has(clean)) {
        return { blocked: true, reason: `rm -rf targeting ${clean} — would destroy the filesystem` };
      }
      if (HOME_EXPANSIONS.test(unquoted)) {
        return { blocked: true, reason: 'rm -rf targeting home directory — would destroy all user data' };
      }
    }
  }

  return { blocked: false };
}

function extractRmTargets(command: string): string[] {
  const targets: string[] = [];
  const tokens = command.split(/\s+/);
  let pastRm = false;
  for (const tok of tokens) {
    if (!pastRm) {
      if (/\brm$/.test(tok)) pastRm = true;
      continue;
    }
    if (tok.startsWith('-') && !tok.startsWith('/')) continue;
    if (tok === '--') continue;
    targets.push(tok);
  }
  return targets;
}

// ---------------------------------------------------------------------------
// Dangerous rm detector — flags `rm` with unguarded variables
// ---------------------------------------------------------------------------

export function detectDangerousRm(command: string): { dangerous: boolean; suggestion?: string; variable?: string } {
  if (!/\brm\b/.test(command)) return { dangerous: false };

  // Detect rm -rf with command substitution target — the output is unknowable
  // at static analysis time, so it must always require permission.
  // eslint-disable-next-line security/detect-unsafe-regex -- [rfdi] and \s are disjoint, no overlap
  const rmRecursive = /\brm\s+(?:-[rfdi]+\s+)*/;
  if (rmRecursive.test(command)) {
    const afterRm = command.replace(rmRecursive, '');
    if (/\$\(/.test(afterRm) || /`[^`]+`/.test(afterRm)) {
      return {
        dangerous: true,
        suggestion: 'rm with command substitution target — cannot verify safety at analysis time',
      };
    }
  }

  // eslint-disable-next-line security/detect-unsafe-regex -- [rfdi] and \s are disjoint, no overlap
  const rmWithVar = /\brm\s+(?:-[rfdi]+\s+)*\$\{?(\w+)\}?/;
  const match = command.match(rmWithVar);
  if (!match) return { dangerous: false };
  const varName = match[1];
  const guardedPattern = new RegExp(`\\$\\{${varName}:[-?]`);
  if (guardedPattern.test(command)) return { dangerous: false };
  return {
    dangerous: true,
    variable: varName,
    suggestion: `Use \${${varName}:?} guard: rm -rf "\${${varName}:?}"`,
  };
}

// ---------------------------------------------------------------------------
// Data-exfiltration detector
// ---------------------------------------------------------------------------
// Blocks commands that combine bulk file reads with outbound network transfer.
// Standalone tar, standalone curl, etc. are fine — it's the combination that
// signals data exfiltration.

const FILE_READ_CMDS = /\b(tar|zip|cat|base64|dd|gzip|bzip2|xz|zcat|head|tail|xxd|od)\b/;
const NETWORK_SEND_CMDS = /\b(curl|wget|nc|ncat|netcat|socat)\b/;
const PIPE_OR_REDIRECT = /[|]|<\s*\//;

export function detectDataExfiltration(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);
  const norm = normalizeCommand(stripped);

  // 1. Pipe: file-read | network-send
  if (PIPE_OR_REDIRECT.test(norm)) {
    const segments = norm.split(/\|/);
    for (let i = 0; i < segments.length - 1; i++) {
      const left = segments[i].trim();
      const right = segments.slice(i + 1).join('|').trim();
      if (FILE_READ_CMDS.test(left) && NETWORK_SEND_CMDS.test(right)) {
        return { blocked: true, reason: 'bulk file data piped to a network command' };
      }
    }
  }

  // 2. curl/wget posting file contents: -d @/path, --data-binary @/path,
  //    -F "file=@/path", --upload-file /path, -T /path, --post-file=/path
  if (/\bcurl\b/.test(norm)) {
    if (/(?:-d\s+@|--data-binary\s+@|--data\s+@)\//.test(norm)) {
      return { blocked: true, reason: 'curl posting local file contents' };
    }
    if (/-F\s+[^\s]*@\//.test(norm) || /-F\s+"[^"]*@\//.test(norm) || /-F\s+'[^']*@\//.test(norm)) {
      return { blocked: true, reason: 'curl uploading local file via form field' };
    }
    if (/(?:--upload-file|-T)\s+\//.test(norm)) {
      return { blocked: true, reason: 'curl uploading local file' };
    }
  }
  if (/\bwget\b/.test(norm) && /--post-file[= ]\s*\//.test(norm)) {
    return { blocked: true, reason: 'wget posting local file contents' };
  }

  // 3. scp with -r (recursive = bulk): scp -r[p...] /local user@host:
  if (/\bscp\b/.test(norm) && /-[^\s]*r/.test(norm) && /\S+@\S+:/.test(norm)) {
    return { blocked: true, reason: 'recursive scp to remote host (bulk transfer)' };
  }

  // 4. rsync to remote (src is local path, dst contains user@host:)
  if (/\brsync\b/.test(norm)) {
    const tokens = norm.split(/\s+/);
    const nonFlagTokens = tokens.filter(t => !t.startsWith('-') && t !== 'rsync');
    // rsync <options> <src> <dst> — dst is remote if it has user@host:
    if (nonFlagTokens.length >= 2) {
      const dst = nonFlagTokens[nonFlagTokens.length - 1];
      const src = nonFlagTokens[nonFlagTokens.length - 2];
      if (/\S+@\S+:/.test(dst) && !/@/.test(src)) {
        return { blocked: true, reason: 'rsync uploading local data to remote host' };
      }
    }
  }

  // 5. nc/netcat with file redirect: nc host port < /path
  if (/\b(nc|ncat|netcat)\b/.test(norm) && /<\s*\//.test(norm)) {
    return { blocked: true, reason: 'netcat sending local file to remote host' };
  }

  return { blocked: false };
}

// ---------------------------------------------------------------------------
// System directory write guard
// ---------------------------------------------------------------------------

const SYSTEM_DIR_PREFIXES = [
  '/etc', '/usr', '/bin', '/sbin', '/var',
  '/sys', '/proc', '/dev', '/boot', '/lib', '/lib64', '/opt',
];

function isSystemDirPath(p: string): boolean {
  const resolved = require('path').resolve(p);
  return SYSTEM_DIR_PREFIXES.some(
    sd => resolved === sd || resolved.startsWith(sd + '/'),
  );
}

export function commandTargetsSystemDir(command: string): boolean {
  const targets = collectWriteTargets(command);
  return targets.some(t => isSystemDirPath(t));
}

export function extractCommandPaths(command: string): string[] {
  const tokens = command.split(/\s+/);
  const cmd = tokens[0];
  const args = tokens.slice(1);
  const ddIdx = args.indexOf('--');
  const effectiveArgs = ddIdx >= 0 ? args.slice(ddIdx + 1) : args;
  const extractor = PATH_COMMANDS[cmd];
  const positional = extractor ? extractor(effectiveArgs) : [];
  const optionPaths = extractOptionValuePaths(cmd, args);
  return [...positional, ...optionPaths];
}

// ---------------------------------------------------------------------------
// Code-loading & policy-source variable protection
// ---------------------------------------------------------------------------

export const CODE_LOADING_VARIABLES = [
  'LD_PRELOAD',
  'LD_LIBRARY_PATH',
  'LD_AUDIT',
  'DYLD_INSERT_LIBRARIES',
  'DYLD_LIBRARY_PATH',
  'DYLD_FRAMEWORK_PATH',
  'BASH_ENV',
  'ENV',
  'ZDOTDIR',
  'NODE_OPTIONS',
] as const;

export const POLICY_SOURCE_VARIABLES = [
  'SUPERINFERENCE_MANAGED_SETTINGS_PATH',
  'SUPERINFERENCE_CONFIG_DIR',
] as const;

const PINNED_VARIABLES_UPPER = new Set(
  [...CODE_LOADING_VARIABLES, ...POLICY_SOURCE_VARIABLES].map(v => v.toUpperCase()),
);

export function detectCodeLoadingEnvSet(command: string): { blocked: boolean; variable?: string } {
  const patterns = [
    /\bexport\s+([A-Za-z_]\w*)=/g,
    /\bdeclare\s+-x\s+([A-Za-z_]\w*)=/g,
    /\bunset\s+([A-Za-z_]\w*)/g,
    /\benv\s+(?:-\S+\s+)*([A-Za-z_]\w*)=/g, // eslint-disable-line security/detect-unsafe-regex -- \S and \s are disjoint, each iteration advances
    /(?:^|(?<=\s|;|&&|\|\|))([A-Za-z_]\w*)=\S+\s+\S/g, // eslint-disable-line security/detect-unsafe-regex -- lookbehind is constant-length per alternative, linear
  ];

  for (const re of patterns) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(command)) !== null) {
      const varName = m[1];
      if (varName && PINNED_VARIABLES_UPPER.has(varName.toUpperCase())) {
        return { blocked: true, variable: varName };
      }
    }
  }
  return { blocked: false };
}

/**
 * Detect BASH_ARGV0 assignment used to masquerade a command's process name.
 * `BASH_ARGV0=fake_name real_command` makes the process appear as `fake_name`
 * in `ps` and `$0`, which can bypass pattern-matching permission checks.
 */
export function detectBashArgv0Assignment(command: string): { blocked: boolean; reason?: string } {
  if (/\bBASH_ARGV0\s*=/.test(command)) {
    return { blocked: true, reason: 'BASH_ARGV0 assignment can masquerade process name — blocks permission-check bypass' };
  }
  return { blocked: false };
}

export function detectBashpidAssignment(command: string): { blocked: boolean; reason?: string } {
  if (/\bBASHPID\s*=/.test(command)) {
    return { blocked: true, reason: 'BASHPID assignment with arithmetic expression can execute code — blocked' };
  }
  return { blocked: false };
}

export {
  validateControlCharacters,
  validateCarriageReturn,
  validateIFSInjection,
  validateProcEnvironAccess,
  validateObfuscatedFlags,
  validateUnicodeWhitespace,
  validateBraceExpansion,
  validateDangerousVariables,
  validateZshDangerousCommands,
  validateBackslashEscapedOperators,
  validateNewlines,
  extractQuotedContent,
  stripSafeRedirections,
  isEscapedAtPosition,
  stripSafeWrappers,
  validateSedDangerous,
  validateASTCommands,
  validateJqDangerous,
  validateShellMetacharsInCommands,
  validateCommentQuoteDesync,
  validateQuotedNewline,
  validateMidWordHash,
  validateProcessSubstitution,
  validateBacktickInjection,
  validateShellQuoteBug,
  validateCommandSubstitution,
  validateRedirections,
  validateDevTcpUdpRedirects,
  validateBackslashEscapedWhitespace,
  validateMalformedTokenInjection,
  validateInvisibleUnicode,
  stripSafeHeredocSubstitutions,
  CHECK_IDS,
  SENSITIVE_ENV_RE,
  redactEnvVarsInOutput,
};

const SENSITIVE_ENV_RE =
  /(?:API[_-]?KEY|SECRET|TOKEN|PASSWORD|CREDENTIAL|AUTH|PRIVATE[_-]?KEY|ACCESS[_-]?KEY|AWS[_-]SECRET|GITHUB[_-]TOKEN|OPENAI[_-]?KEY|ANTHROPIC[_-]?KEY|DATABASE[_-]URL|DB[_-]PASS)/i;

// ---------------------------------------------------------------------------
// Zsh [[ =~ ]] regex conditional — command substitution bypass
// ---------------------------------------------------------------------------
// In zsh, the RHS of [[ $var =~ pattern ]] can contain command substitution
// that executes during evaluation, allowing hidden command execution.

export function detectZshRegexConditional(command: string): { blocked: boolean; reason?: string } {
  if (!/\[\[.*=~/.test(command)) return { blocked: false };
  const regexMatch = command.match(/\[\[.*=~\s*(.*?)\s*\]\]/s);
  if (!regexMatch) return { blocked: false };
  const rhs = regexMatch[1];
  if (/\$\(/.test(rhs) || /`[^`]+`/.test(rhs)) {
    return {
      blocked: true,
      reason: 'Command substitution inside [[ =~ ]] regex conditional can execute arbitrary commands',
    };
  }
  return { blocked: false };
}

function redactEnvVarsInOutput(output: string): string {
  if (!output) return output;
  return output.split('\n').map(line => {
    // eslint-disable-next-line security/detect-unsafe-regex -- anchored both sides, (.*) is linear with $
    const m = line.match(/^((?:export\s+|declare\s+-x\s+)?)([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!m) return line;
    const [, prefix, name, rest] = m;
    if (!SENSITIVE_ENV_RE.test(name)) return line;
    if (rest.startsWith('"') && rest.endsWith('"') && rest.length > 1) {
      return `${prefix}${name}="[REDACTED]"`;
    }
    if (rest.startsWith("'") && rest.endsWith("'") && rest.length > 1) {
      return `${prefix}${name}='[REDACTED]'`;
    }
    return `${prefix}${name}=[REDACTED]`;
  }).join('\n');
}
