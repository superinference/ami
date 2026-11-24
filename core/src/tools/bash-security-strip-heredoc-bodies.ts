// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { parseBashAST, PARSE_ABORTED, hasActualOperatorNodes } from './bash-parser';
import { CHECK_IDS, PASS, extractQuotedContent, fail } from './bash-security-strip-invisible-unicode';
import type { SecurityCheckResult } from './bash-security-strip-invisible-unicode';

export function validateBackslashEscapedOperators(command: string): SecurityCheckResult {
  let inSingleQuote = false;
  let inDoubleQuote = false;
  const operators = new Set([';', '|', '&', '<', '>']);

  let foundEscapedOperator = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (c === '\\' && !inSingleQuote) {
      i++; // skip next char unconditionally (even inside double quotes for correct parity)
      if (i < command.length && !inDoubleQuote && operators.has(command[i])) {
        foundEscapedOperator = true;
        break;
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

  if (!foundEscapedOperator) return PASS;

  // AST gate (parity: the reference `hasActualOperatorNodes`): a backslash-escaped
  // operator can only HIDE command structure if the command actually HAS real
  // control operators. When the command parses to a tree with no `list`/`pipeline`
  // node, the escaped `\;`/`\|`/`\&` is necessarily a literal word argument (e.g.
  // the POSIX `find … -exec cmd {} \;` terminator), so blocking it here would be a
  // false positive — allow it. A null/aborted parse does NOT clear the finding
  // (fail-closed: if we cannot prove the command is operator-free, keep blocking).
  const ast = parseBashAST(command);
  if (ast && ast !== PARSE_ABORTED && !hasActualOperatorNodes(ast)) return PASS;

  return fail(CHECK_IDS.BACKSLASH_ESCAPED_OPERATORS, 'Command contains backslash before a shell operator — can hide command structure');
}

/**
 * Replace heredoc constructs — `<<DELIM … DELIM` and the dash form `<<-DELIM … <indent>DELIM`,
 * with quoted (`<<'EOF'` / `<<"EOF"`) or bare delimiters — with a single-line placeholder so the
 * newline / redirection guards do not treat a heredoc BODY (inert stdin data) as command
 * separation (ui#26). This loosens ONLY those structural checks: any dangerous content inside a
 * heredoc body is still scanned by the destructive-command detectors (detectDangerousRm /
 * detectNuclearCommand / command-substitution guards), which run on the RAW command string,
 * unaffected by this stripping.
 *
 * Parsing fidelity (parity with the reference's extractHeredocs, utils/bash/heredoc.ts): a naive
 * regex that treats every `<<WORD` as a heredoc lets an attacker HIDE real commands from the
 * structural guards. This implementation refuses to strip in the cases where bash would NOT begin
 * a heredoc body, so smuggled commands stay visible to the guards:
 *   - `<<` inside single/double quotes is literal text, not an operator (`echo "x <<EOF"\nls`).
 *   - `<<` after an unquoted `#` is inside a comment (`# <<EOF\nwhoami\nEOF`).
 *   - `\<<EOF` (odd backslash run) is an escaped `<`, not a heredoc.
 *   - the body begins at the first UNQUOTED newline (a quoted newline on the opening line extends
 *     the logical line — `cat <<'EOF' 'a\nb' ; cmd\nEOF`).
 *   - a backslash-newline continuation on the opening line joins the next physical line into the
 *     COMMAND, not the body, so we bail.
 *   - `<<-` strips leading TABS only (not spaces) when matching the closing delimiter, per POSIX.
 *   - a bare `(( a << B ))` arithmetic context, `$'…'`/`$"…"` quoting, or a backtick before the
 *     first `<<` can desync the simplified scanner, so we bail (leave the command fully visible).
 * Exported for unit coverage.
 */
export function stripHeredocBodies(command: string, options?: { quotedOnly?: boolean }): string {
  if (!command.includes('<<')) return command;
  const quotedOnly = options?.quotedOnly === true;

  // Fail-SAFE bail-outs (match reference): constructs our simplified quote/comment scanner cannot
  // track reliably. Leaving the command unchanged keeps ALL of it visible to the structural guards.
  if (/\$['"]/.test(command)) return command; // ANSI-C / locale quoting ($'…', $"…")
  const firstHeredocPos = command.indexOf('<<');
  if (firstHeredocPos > 0) {
    const before = command.slice(0, firstHeredocPos);
    if (before.includes('`')) return command; // backtick before `<<` (PST_EOFTOKEN early closure)
    const openArith = (before.match(/\(\(/g) || []).length;
    const closeArith = (before.match(/\)\)/g) || []).length;
    if (openArith > closeArith) return command; // `(( … << … ))` — `<<` is a bit-shift, not a heredoc
  }

  // Operator: `<<`/`<<-`, NOT part of a `<<<` herestring, optional spaces, then a delimiter WORD
  // that is quoted (`<<'EOF'` / `<<"EOF"`, backslash may be literal inside quotes) or unquoted
  // (`<<EOF`, an outer `\` is an escape). [ \t]* (not \s*) so the operator cannot span a newline.
  const open = /(?<!<)<<(?!<)(-)?[ \t]*(?:(['"])(\\?\w+)\2|\\?(\w+))/g;
  let result = '';
  let lastIndex = 0;
  let m: RegExpExecArray | null;

  // Incremental quote/comment/escape scanner (mirrors the reference advanceScan). scanPos tracks how
  // far we have classified; state reflects the parser immediately BEFORE the next match position.
  let scanPos = 0;
  let inSingle = false, inDouble = false, inComment = false, dqEscape = false, backslashRun = 0;
  const advanceScan = (target: number): void => {
    for (let i = scanPos; i < target; i++) {
      const ch = command[i];
      if (ch === '\n') inComment = false; // any physical newline clears comment state
      if (inSingle) { if (ch === "'") inSingle = false; continue; }
      if (inDouble) {
        if (dqEscape) { dqEscape = false; continue; }
        if (ch === '\\') { dqEscape = true; continue; }
        if (ch === '"') inDouble = false;
        continue;
      }
      if (ch === '\\') { backslashRun++; continue; }
      const escaped = backslashRun % 2 === 1;
      backslashRun = 0;
      if (escaped) continue;
      if (ch === "'") inSingle = true;
      else if (ch === '"') inDouble = true;
      else if (!inComment && ch === '#') inComment = true;
    }
    scanPos = target;
  };

  while ((m = open.exec(command)) !== null) {
    if (m.index < lastIndex) continue; // `<<` inside a body we already stripped — not an operator
    advanceScan(m.index);
    // Skip `<<` that is not actually a heredoc operator in bash.
    if (inSingle || inDouble) continue;   // inside a quoted string
    if (inComment) continue;              // inside a `#` comment
    if (backslashRun % 2 === 1) continue; // escaped `\<<`

    const fullMatch = m[0];
    const dashForm = m[1] === '-';
    const quoteChar = m[2];
    const delim = (m[3] ?? m[4]) as string;
    const opEnd = m.index + fullMatch.length;
    // Delimiter-capture sanity: a quoted delimiter must have its closing quote (\w+ stops at a
    // space/hyphen), and the operator must be followed by a bash word terminator — otherwise our
    // delimiter differs from bash's and we must not strip.
    if (quoteChar && command[opEnd - 1] !== quoteChar) continue;
    if (opEnd < command.length && !/^[ \t\n|&;()<>]$/.test(command[opEnd])) continue;

    // In `quotedOnly` mode, strip ONLY heredocs whose delimiter is quoted
    // (`<<'EOF'` / `<<"EOF"`) or backslash-escaped (`<<\EOF`): bash treats their
    // body as LITERAL text with no $()/${}/` ` expansion, so it is inert data a
    // substitution/expansion validator must not scan. An UNQUOTED `<<EOF` body
    // IS expanded by bash, so it is deliberately left visible. (Default mode —
    // used by validateNewlines/validateRedirections — strips every heredoc body,
    // since those guards treat all heredoc bodies as stdin data.)
    const isQuotedDelim = quoteChar !== undefined || /\\\w+$/.test(fullMatch);
    if (quotedOnly && !isQuotedDelim) continue;

    // Body starts at the first UNQUOTED newline after the operator (a quoted newline extends the
    // logical opening line). Track quote state locally from a known-clean start (opEnd).
    let bodyNl = -1;
    {
      let s = false, d = false;
      for (let k = opEnd; k < command.length; k++) {
        const ch = command[k];
        if (s) { if (ch === "'") s = false; continue; }
        if (d) { if (ch === '\\') { k++; continue; } if (ch === '"') d = false; continue; }
        if (ch === '\n') { bodyNl = k; break; }
        let bs = 0;
        for (let j = k - 1; j >= opEnd && command[j] === '\\'; j--) bs++;
        if (bs % 2 === 1) continue;
        if (ch === "'") s = true; else if (ch === '"') d = true;
      }
    }
    if (bodyNl === -1) continue; // logical line never ends (open quote) / no body → not stripped

    // Backslash-newline continuation on the opening line joins the next physical line into the
    // COMMAND (bash does this BEFORE heredoc parsing), so the "body" would hide a real command.
    const sameLine = command.slice(opEnd, bodyNl);
    let trailBs = 0;
    for (let j = sameLine.length - 1; j >= 0 && sameLine[j] === '\\'; j--) trailBs++;
    if (trailBs % 2 === 1) continue;

    // Find the closing delimiter on its own line. `<<-` strips leading TABS only (not spaces).
    const afterNl = command.slice(bodyNl + 1);
    const contentLines = afterNl.split('\n');
    let closeLine = -1;
    for (let i = 0; i < contentLines.length; i++) {
      const line = contentLines[i];
      const stripped = dashForm ? line.replace(/^\t*/, '') : line;
      if (stripped === delim) { closeLine = i; break; }
      // Early-closure (PST_EOFTOKEN): a line starting with the delimiter followed by a shell
      // metacharacter / substitution closer may close the heredoc early — bail rather than misparse.
      if (stripped.length > delim.length && stripped.startsWith(delim) && /^[)}`|&;(<>]$/.test(stripped[delim.length])) {
        closeLine = -1;
        break;
      }
    }
    if (closeLine === -1) continue; // unterminated / ambiguous → leave visible

    const contentEnd = bodyNl + 1 + contentLines.slice(0, closeLine + 1).join('\n').length;
    // Drop the `<<WORD` operator and the body+delimiter, but KEEP the opening line's remainder
    // (e.g. a trailing `| grep x`) so the other guards still see it.
    result += command.slice(lastIndex, m.index) + ' ' + sameLine + ' __HEREDOC__';
    lastIndex = contentEnd;
    // Re-sync the scanner past the stripped body (a fresh logical line, clean quote state).
    scanPos = contentEnd;
    inSingle = inDouble = inComment = dqEscape = false;
    backslashRun = 0;
  }
  result += command.slice(lastIndex);
  return result;
}

export function validateNewlines(command: string): SecurityCheckResult {
  // Heredoc bodies are data, not command separation — strip them before the structural check so
  // legitimate multi-line heredocs are not mistaken for smuggled extra commands (ui#26).
  const extracted = extractQuotedContent(stripHeredocBodies(command));
  if (!/[\n\r]/.test(extracted.fullyUnquoted)) return PASS;
  // A backslash-newline is a line continuation ONLY at a word boundary (preceded
  // by whitespace). A MID-word continuation like `tr\<newline>aceroute` joins in
  // bash to `traceroute`, but token/allowlist classification sees the command as
  // `tr` — the shell-quote/bash command-name differential. So only a whitespace-
  // then-backslash before the newline is exempted; a non-whitespace `\<newline>`
  // is still flagged.
  if (/(?<![\s]\\)[\n\r]\s*\S/.test(extracted.fullyUnquoted)) {
    return fail(CHECK_IDS.NEWLINES, 'Command contains newlines that could separate multiple commands');
  }
  return PASS;
}

/**
 * Quote-aware splitter on unquoted shell separators (`;`, `&&`, `||`, `|`, `&`).
 * Keeps separators that live inside single/double quotes intact (so a sed
 * delimiter like `s|a|b|` is NOT split). Used to validate dangerous commands in
 * EVERY pipeline stage / compound segment, not only when they are the first word.
 */
export function splitShellSegments(command: string): string[] {
  const segs: string[] = [];
  let cur = '';
  let inS = false, inD = false, esc = false;
  for (let i = 0; i < command.length; i++) {
    const c = command[i];
    if (esc) { cur += c; esc = false; continue; }
    if (c === '\\' && !inS) { cur += c; esc = true; continue; }
    if (c === "'" && !inD) { inS = !inS; cur += c; continue; }
    if (c === '"' && !inS) { inD = !inD; cur += c; continue; }
    if (!inS && !inD && (c === ';' || c === '|' || c === '&')) {
      segs.push(cur);
      cur = '';
      while (i + 1 < command.length && (command[i + 1] === ';' || command[i + 1] === '|' || command[i + 1] === '&')) i++;
      continue;
    }
    cur += c;
  }
  if (cur) segs.push(cur);
  return segs;
}

/**
 * Remove well-formed sed substitution (`s<d>…<d>…<d>flags`) and transliterate
 * (`y<d>…<d>…<d>`) commands from an expression so their internal delimiters and
 * replacement text are not misread as addresses or bare commands when scanning
 * for `w`/`e`/`R` commands. Delimiter-agnostic via a backreference.
 */
export function stripSedSubCommands(expr: string): string {
  // eslint-disable-next-line security/detect-unsafe-regex -- each alt consumes >=1 char, lazy, bounded by the \1 backreference; linear
  const yRe = /\by([^\\\n])(?:\\.|(?!\1)[^\n])*?\1(?:\\.|(?!\1)[^\n])*?\1/;
  // eslint-disable-next-line security/detect-unsafe-regex -- each alt consumes >=1 char, lazy, bounded by the \1 backreference; linear
  const sRe = /\bs([^\\\n])(?:\\.|(?!\1)[^\n])*?\1(?:\\.|(?!\1)[^\n])*?\1[a-zA-Z0-9]*/;
  let out = expr;
  let prev = '';
  do {
    prev = out;
    out = out.replace(yRe, '');
    out = out.replace(sRe, '');
  } while (out !== prev);
  return out;
}

/**
 * Extract inline sed script expressions: quoted strings, the fused `-e=`/
 * `--expression=` forms, AND the space-separated `-e EXPR` / `--expression EXPR`
 * forms. The shell splits the expression into its own token, so an UNQUOTED
 * expression such as `sed -e e file` (the `e` command executes the pattern space
 * as a shell command) must be pulled out and inspected — otherwise it hides from
 * the address-anchored write/exec detectors behind the leading `-e ` flag.
 * Parity with the reference sedValidation.ts `extractSedExpressions` `-e` /
 * `--expression` token handling.
 */
export function extractSedExpressions(seg: string): string[] {
  const exprs: string[] = [];
  const quoted = /'([^']*)'|"([^"]*)"/g;
  let m: RegExpExecArray | null;
  while ((m = quoted.exec(seg)) !== null) exprs.push(m[1] ?? m[2]);
  for (const mm of seg.matchAll(/(?:--expression=|(?:^|\s)-e=)([^\s'"]+)/g)) {
    exprs.push(mm[1]);
  }
  // Space-separated script flag with an unquoted expression. The first char is
  // restricted to a non-quote, non-`=`, non-`-` token so quoted expressions
  // (already captured above) and the fused `=`/following-flag forms are not
  // re-captured or mis-read.
  for (const mm of seg.matchAll(/(?:--expression|(?:^|\s)-e)\s+([^\s'"=-]\S*)/g)) {
    exprs.push(mm[1]);
  }
  return exprs;
}

// Address prefix that can precede a bare sed command (line number, range, `$`,
// `/regex/`), reused by the write/execute command detectors. The range's second
// operand also models GNU relative/offset forms `,+N` (addr1,+N) and `,~N`
// (addr1,~N) — without them `/pat/,+2 w FILE` evaded the address-aware write/exec
// detectors (the reference sedValidation.ts L524-527 rejects comma-offset ranges).
export const SED_ADDR = '(?:[0-9]+(?:~[0-9]+)?(?:\\s*,\\s*(?:[+~]?[0-9]+|\\$|\\/[^/\\n]*\\/))?|\\$|\\/[^/\\n]*\\/(?:\\s*,\\s*(?:[+~]?[0-9]+|\\$|\\/[^/\\n]*\\/))?)';

/** Inspect a single extracted sed expression for write/execute commands. */
export function checkSedExpression(expr: string): string | null {
  // Delimiter-agnostic substitution exec (`s<d>…<d>…<d>…e`) — the replacement is
  // run as a shell command. Caught BEFORE stripping substitutions.
  // eslint-disable-next-line security/detect-unsafe-regex -- each alt consumes >=1 char, lazy, bounded by the \1 backreference; linear
  const sExec = /\bs([^\\\n])(?:\\.|(?!\1)[^\n])*?\1(?:\\.|(?!\1)[^\n])*?\1[a-zA-Z0-9]*e/;
  if (sExec.test(expr)) {
    return "sed substitution with 'e' flag executes the replacement as a shell command";
  }
  // Delimiter-agnostic substitution write flag (`s<d>…<d>…<d>…w FILE`).
  // eslint-disable-next-line security/detect-unsafe-regex -- each alt consumes >=1 char, lazy, bounded by the \1 backreference; linear
  const sWrite = /\bs([^\\\n])(?:\\.|(?!\1)[^\n])*?\1(?:\\.|(?!\1)[^\n])*?\1[a-zA-Z0-9]*w\s+\S/;
  if (sWrite.test(expr)) {
    return "sed substitution 'w' flag writes the result to a file";
  }

  // Backslash-DELIMITED substitution carrying a write/execute op. The sExec /
  // sWrite detectors above require a non-backslash delimiter (`[^\\\n]`), so a
  // substitution that uses `\` itself as the delimiter — `s\pat\rep\w FILE`,
  // `s\pat\rep\e` — slipped past them. Anchored to a command boundary (start,
  // `;`, `{`/`}`, or an address) so a literal `s\` inside a normal replacement
  // (e.g. `s/a/pas\s/`) is NOT misread as a delimiter (the reference
  // sedValidation.ts L532 rejects backslash alternate-delimiter tricks).
  // eslint-disable-next-line security/detect-unsafe-regex -- delimiter is a literal `\`; sections are (?:\\.|[^\\\n])* whose two arms are disjoint on the first char (backslash vs non-backslash), so matching is linear with no ambiguous overlap
  const sBackslash = new RegExp(`(?:^|[;{}])\\s*(?:${SED_ADDR})?\\s*s\\\\(?:\\\\.|[^\\\\\\n])*\\\\(?:\\\\.|[^\\\\\\n])*\\\\[a-zA-Z0-9]*(?:[wWe]|\\s+[wWeR])`);
  if (sBackslash.test(expr)) {
    return 'sed substitution using a backslash delimiter can hide a write/execute operation from pattern-based checks';
  }

  const bare = stripSedSubCommands(expr);

  // 'w'/'W' write command in any address form (start, after `;`/`{`/`}`, after a
  // line number / range / `$` / `/regex/` address).
  const wCmd = new RegExp(`(?:^|[;{}])\\s*(?:${SED_ADDR})?\\s*!?\\s*[wW]\\s+\\S`);
  if (wCmd.test(bare)) {
    return "sed 'w'/'W' command writes to an arbitrary file";
  }
  // 'R' read command (reads an arbitrary file line-by-line).
  const rCmd = new RegExp(`(?:^|[;{}])\\s*(?:${SED_ADDR})?\\s*!?\\s*R\\s+\\S`);
  if (rCmd.test(bare)) {
    return "sed 'R' command reads an arbitrary file";
  }
  // 'e' execute command in bare/address forms (`e cmd`, `1e`, `$e`, `/pat/e`).
  const eCmd = new RegExp(`(?:^|[;{}])\\s*(?:${SED_ADDR})?\\s*!?\\s*e(?=\\s|;|$)`);
  if (eCmd.test(bare)) {
    return "sed 'e' command executes an arbitrary shell command";
  }
  return null;
}

/**
 * Conservative "when in doubt, unsafe" rejection of sed scripts that are too
 * complex to reason about statically (parity: the reference
 * sedValidation.ts `containsDangerousOperations`, L488-504). A `{ … }` block can
 * hide arbitrary commands behind address grouping, and an embedded newline turns
 * a one-liner into a multi-line script — both defeat the pattern-based write/exec
 * detectors, so we refuse to auto-approve them. Returns a reason string when the
 * expression should be rejected, or null when it is simple enough to inspect.
 *
 * Note: the reference also blanket-rejects any `#` not acting as an `s#…` delimiter. AMI
 * deliberately does NOT port that — its /regex/ address handling allows the
 * precise, benign `sed '/^#/d'` (delete comment lines), a superset over the reference's
 * cruder block. That precision is intentional and locked by tests.
 */
export function sedExpressionTooComplex(expr: string): string | null {
  if (expr.includes('{') || expr.includes('}')) {
    return 'sed script contains a { } block, which is too complex to statically verify as safe';
  }
  if (/[\n\r]/.test(expr)) {
    return 'sed script contains an embedded newline (multi-line scripts are too complex to statically verify as safe)';
  }
  return null;
}

export function validateJqDangerous(command: string): SecurityCheckResult {
  if (/\bjq\b/.test(command) && /\bsystem\s*\(/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'jq system() calls can execute arbitrary commands');
  }
  // jq file-reading / module-loading flags: --rawfile/--slurpfile read an
  // arbitrary file's contents into a variable (arbitrary-read / exfil), and
  // -L/--library-path loads jq modules from an attacker-chosen directory
  // (code-load). `-f/--from-file` is intentionally NOT hard-blocked here —
  // loading the filter program from a file is ordinary jq usage. the reference's
  // jq flag check (utils/bash/ast.ts L2604-2609) returns ok:false for -f, which
  // feeds its AUTO-APPROVAL (ask) decision, not a hard execution deny; AMI maps
  // that to classifyBashCommand → 'unsafe' (approval required), so `jq -f` is
  // already gated at the correct layer without over-refusing it outright.
  if (/\bjq\b/.test(command) && /(?:^|\s)(?:--rawfile|--slurpfile|-L|--library-path)\b/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'jq --rawfile/--slurpfile/-L/--library-path can read arbitrary files or load code from an arbitrary path');
  }
  if (/\b[gm]?awk\b/.test(command) && /\bsystem\s*\(/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'awk system() calls can execute arbitrary commands');
  }
  if (/\b[gm]?awk\b/.test(command) && /\|\s*getline\b/.test(command)) {
    return fail(CHECK_IDS.JQ_DANGEROUS, 'awk pipe-to-getline can execute arbitrary commands');
  }
  return PASS;
}

export function validateShellMetacharsInCommands(command: string): SecurityCheckResult {
  // Drop backslash-escaped characters first: a `\;`/`\&`/`` \` ``/`\$` is a literal
  // word argument, not a live shell metacharacter — most notably the standard POSIX
  // `find … -exec cmd {} \;` terminator. Scanning the escape-stripped view keeps
  // genuine injection (`find . ; rm`, `` find `id` ``) blocked while clearing the
  // find/grep/xargs escaped-terminator false positive (parity: the reference treats `\;`
  // as part of a word node, not an operator).
  const stripped = command.replace(/\\./g, '');
  // Exclude | (pipe) — piping to/from find/grep/xargs is normal shell usage
  if (/\b(find|grep|xargs)\b/.test(stripped) && /[;&`$]/.test(stripped)) {
    const hasQuoted = /(['"]).*[;&`$].*\1/.test(stripped);
    if (!hasQuoted) return fail(CHECK_IDS.SHELL_METACHARS_IN_COMMANDS, 'Unquoted shell metacharacters in find/grep/xargs command');
  }
  return PASS;
}
