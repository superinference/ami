import { parseBashAST, extractCommands, CommandNode } from './bash-parser';

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
} as const;

// eslint-disable-next-line no-control-regex
const CONTROL_CHAR_RE = /[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/;

const UNICODE_WS_RE =
  new RegExp("[\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000\uFEFF]");

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

export function validateBashSecurity(command: string): SecurityCheckResult {
  let result: SecurityCheckResult;

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

  result = validateNewlines(command);
  if (!result.safe) return result;

  const extracted = extractQuotedContent(command);
  const fullyUnquoted = stripSafeRedirections(extracted.fullyUnquoted);

  // New critical validators on extracted content
  result = validateCommandSubstitution(command, extracted);
  if (!result.safe) return result;

  result = validateRedirections(command, extracted);
  if (!result.safe) return result;

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

  result = validateShellMetacharsInCommands(command);
  if (!result.safe) return result;

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

export function extractCommandPaths(command: string): string[] {
  const tokens = command.split(/\s+/);
  const cmd = tokens[0];
  const args = tokens.slice(1);
  const ddIdx = args.indexOf('--');
  const effectiveArgs = ddIdx >= 0 ? args.slice(ddIdx + 1) : args;
  const extractor = PATH_COMMANDS[cmd];
  return extractor ? extractor(effectiveArgs) : [];
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
  validateBackslashEscapedWhitespace,
  validateMalformedTokenInjection,
  stripSafeHeredocSubstitutions,
  CHECK_IDS,
};
