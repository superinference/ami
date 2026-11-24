// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { redactCredentials } from '../credential-guard';
import { extractQuotedContent, stripInvisibleUnicode } from './bash-security-strip-invisible-unicode';

// ---------------------------------------------------------------------------
// Eval-like / code-evaluating shell builtins
// ---------------------------------------------------------------------------
// Several bash builtins execute attacker-controlled argv as code the moment the
// command runs (guaranteed execution), bypassing command-name allow/deny checks
// that only look at the leading word. `eval`/`source`/`.` are handled by
// detectSourceExecution and `mapfile` by validateZshDangerousCommands; this guard
// covers the remaining high-signal, rarely-legitimate forms. `trap`, `command`,
// `exec`, and `let` are deliberately NOT blocked here: they have common
// legitimate uses (cleanup traps, `command -v`, `exec >file`, arithmetic) and a
// hard block would regress real build/test scripts (superset divergence from
// the reference, which prompts rather than blocks).
export function detectEvalLikeBuiltins(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);
  // Command-position boundary: start-of-string or just after a command separator
  // (`;`/`|`/`&`/`(`/newline). A plain space is deliberately NOT a boundary so a
  // builtin name used as an ARGUMENT (`echo coproc`, `grep enable file`) is not
  // mistaken for an invocation.
  const B = '(?:^|[;|&(\\n]\\s*)';

  // enable -f lib.so name → dlopens an arbitrary shared object as a builtin.
  if (new RegExp(`${B}enable\\s+(?:-\\w+\\s+)*-f\\b`).test(stripped)) {
    return { blocked: true, reason: 'enable -f dlopens an arbitrary shared object as a shell builtin' };
  }
  // hash -p /path name → points a command name at an arbitrary binary.
  if (new RegExp(`${B}hash\\s+(?:-\\w+\\s+)*-p\\b`).test(stripped)) {
    return { blocked: true, reason: 'hash -p poisons command lookup to point a name at an arbitrary binary' };
  }
  // compgen -C cmd / -F func → runs a command/function immediately to generate
  // completions.
  if (new RegExp(`${B}compgen\\b[^;|&]*\\s-[CF]\\b`).test(stripped)) {
    return { blocked: true, reason: 'compgen -C/-F executes an arbitrary command or function' };
  }
  // complete -C cmd / -F func → registers a completion spec whose -C command or
  // -F function bash runs to generate completions (mirrors compgen -C/-F).
  if (new RegExp(`${B}complete\\b[^;|&]*\\s-[CF]\\b`).test(stripped)) {
    return { blocked: true, reason: 'complete -C/-F runs an arbitrary command or function to generate completions' };
  }
  // trap 'cmd' SIGNAL → cmd runs as shell code on the signal; a trap on EXIT
  // fires at the end of EVERY command invocation, so this is guaranteed
  // execution of an argument that looks inert to argv/flag validation. The
  // listing/reset forms (`trap`, `trap -l`, `trap -p`, `trap - SIG`) carry no
  // handler and are left alone; only a real handler argument (a quoted string
  // or a non-dash word) is blocked.
  if (new RegExp(`${B}trap\\s+(?:-\\S+\\s+)*(?:'[^']*'|"[^"]*"|[^\\s;|&-][^\\s;|&]*)`).test(stripped)) {
    return { blocked: true, reason: 'trap registers a handler that bash runs as shell code on a signal/EXIT (guaranteed execution)' };
  }
  // coproc [NAME] cmd → spawns an arbitrary command as a background coprocess.
  if (new RegExp(`${B}coproc\\b`).test(stripped)) {
    return { blocked: true, reason: 'coproc spawns an arbitrary command as a background coprocess' };
  }
  // readarray/mapfile -C callback → runs a callback command while reading.
  if (new RegExp(`${B}readarray\\b[^;|&]*\\s-C\\b`).test(stripped)) {
    return { blocked: true, reason: 'readarray -C runs an arbitrary callback command' };
  }
  // fc -e / fc -s → re-executes history entries as shell commands.
  if (new RegExp(`${B}fc\\s+(?:-\\w+\\s+)*-[es]\\b`).test(stripped)) {
    return { blocked: true, reason: 'fc -e/-s re-executes history entries as shell commands' };
  }
  // bind -x '"seq":cmd' → binds a key sequence to execute a command.
  if (new RegExp(`${B}bind\\s+(?:-\\w+\\s+)*-x\\b`).test(stripped)) {
    return { blocked: true, reason: 'bind -x binds a key sequence to execute an arbitrary command' };
  }
  // let EXPR → evaluates its argument as a bash arithmetic expression, which is
  // identical to `$((…))`: bash recursively re-evaluates referenced variable
  // VALUES as arithmetic, so `x='a[$(cmd)]'; let "z = x"` executes cmd (Vidar
  // Holen arithmetic injection). AMI already hard-denies every `$((…))`; `let`
  // is the same evaluator by another name, so it is denied the same way.
  if (new RegExp(`${B}let\\b`).test(stripped)) {
    return { blocked: true, reason: 'let evaluates its argument as bash arithmetic, which recursively re-evaluates variable values as code (arithmetic injection — same evaluator as $((…)))' };
  }

  return { blocked: false };
}

/**
 * A value is an arithmetic-injection payload if it carries a command
 * substitution (`$(…)` / backtick) or an array subscript (`name[…]`): bash
 * re-evaluates such a value as an arithmetic expression, so `a[$(cmd)]`
 * executes cmd when the holding variable is used in arithmetic context. The
 * substitution forms matter because the payload can hide inside SINGLE quotes
 * (which the command-substitution extractor strips), slipping past that guard.
 */
export function isArithmeticInjectionPayload(value: string): boolean {
  return /\$\(|`|[A-Za-z_]\w*\s*\[/.test(value);
}

/**
 * Detect arithmetic-injection delivered through the `[[ … -eq/-ne/-lt/-gt/-le/-ge
 * … ]]` conditional. bash evaluates the operands of those arithmetic comparison
 * operators AS ARITHMETIC, so a non-numeric operand carrying a subscript /
 * command substitution — directly, or via a same-line variable whose value is
 * such a payload — executes code exactly like `$((…))`.
 *
 * `x='a[$(cmd)]'; [[ "$x" -eq 0 ]]` runs cmd even though the payload hid in
 * single quotes (so the command-substitution extractor never saw the `$(`).
 * Numeric comparisons (`[[ $n -eq 5 ]]`, `[[ 3 -lt 4 ]]`) are left untouched.
 */
export function detectArithmeticConditionalInjection(command: string): { blocked: boolean; reason?: string } {
  const stripped = stripInvisibleUnicode(command);

  // Same-line assignments whose value is an injection payload taint the variable.
  const tainted = new Set<string>();
  for (const m of stripped.matchAll(/(?:^|[;|&(\n]\s*)([A-Za-z_]\w*)=('[^']*'|"[^"]*"|[^\s;|&]*)/g)) {
    const raw = m[2].replace(/^['"]|['"]$/g, '');
    if (isArithmeticInjectionPayload(raw)) tainted.add(m[1]);
  }

  for (const m of stripped.matchAll(/\[\[([\s\S]*?)\]\]/g)) {
    const body = m[1];
    if (!/-(?:eq|ne|lt|gt|le|ge)\b/.test(body)) continue;
    for (const tok of body.split(/\s+/)) {
      if (!tok || /^-(?:eq|ne|lt|gt|le|ge)$/.test(tok)) continue;
      if (/^-?\d+$/.test(tok)) continue; // bare integer operand is safe
      // Direct payload in the operand, or a reference to a tainted variable.
      if (isArithmeticInjectionPayload(tok)) {
        return { blocked: true, reason: 'arithmetic comparison operand inside [[ … -eq/-ne/-lt/-gt/-le/-ge … ]] contains a subscript or command substitution — bash evaluates it as arithmetic (arithmetic injection)' };
      }
      for (const name of tainted) {
        if (new RegExp(`\\$\\{?${name}\\b`).test(tok)) {
          return { blocked: true, reason: `arithmetic comparison inside [[ … -eq/-ne/-lt/-gt/-le/-ge … ]] references variable "${name}" whose value contains a subscript or command substitution — bash re-evaluates it as code (arithmetic injection)` };
        }
      }
    }
  }

  return { blocked: false };
}

export const SENSITIVE_ENV_RE =
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

// ---------------------------------------------------------------------------
// Non-ASCII homoglyph command-name folding
// ---------------------------------------------------------------------------
// A command NAME written with non-ASCII look-alike letters masquerades as a
// different command at runtime: the OS/resolver folds characters like U+017F (ſ)
// → 's' and U+0131 (ı) → 'i' (and Cyrillic/Greek homoglyphs render identically
// to ASCII), so `ſudo rm -rf /`, `ѕudo …` (Cyrillic es) or `ſtart-proceſſ`
// resolve to the real `sudo` / `Start-Process` while every ASCII-anchored deny
// regex above (`\bsudo\b`, `\brm\b`, …) misses them. ASCII command names are the
// only legitimate case, so a command-name token carrying ANY char outside the
// ASCII range can never be auto-approved — it must be surfaced for approval.
// (Parity with the reference's parser.ts forcing nameType='application' for any
// command name containing U+0080–U+FFFF, utils/powershell/parser.ts L872-886.)
// Invisible/zero-width chars are already rejected upstream by
// validateInvisibleUnicode, so this targets the VISIBLE homoglyph case.

export const NON_ASCII_CHAR_RE = /[^\u0000-\u007F]/;

export const COMMAND_NAME_SEGMENT_SPLIT_RE = /[\n;|&]+/;

/**
 * Extract the executable/command-name token from each `;`/`|`/`&`/newline
 * separated segment, skipping leading subshell/group/negation markers, env-var
 * assignments (`FOO=bar cmd`) and leading redirections so the token returned is
 * the actual command name, never an argument or assignment value.
 */
export function extractCommandNameTokens(command: string): string[] {
  const names: string[] = [];
  for (const rawSeg of command.split(COMMAND_NAME_SEGMENT_SPLIT_RE)) {
    const seg = rawSeg.replace(/^[\s({!]+/, '').trim();
    if (!seg) continue;
    const tokens = seg.split(/\s+/).filter(Boolean);
    let idx = 0;
    while (idx < tokens.length) {
      const t = tokens[idx];
      // leading VAR=value assignment → the command name is a later token
      if (/^[A-Za-z_][A-Za-z0-9_]*=/.test(t)) { idx++; continue; }
      // leading redirection operator (`>file`, `2>`, `<`, `>>`) → skip
      if (/^\d*[<>]/.test(t)) { idx++; continue; }
      break;
    }
    if (idx < tokens.length) {
      // strip surrounding quotes so `"ѕudo"` is still inspected
      names.push(tokens[idx].replace(/^['"]+|['"]+$/g, ''));
    }
  }
  return names;
}

export function detectHomoglyphCommandName(command: string): { blocked: boolean; reason?: string } {
  for (const name of extractCommandNameTokens(command)) {
    if (NON_ASCII_CHAR_RE.test(name)) {
      const ch = [...name].find(c => c.codePointAt(0)! > 0x7f)!;
      const cp = ch.codePointAt(0)!.toString(16).toUpperCase().padStart(4, '0');
      return {
        blocked: true,
        reason: `Command name "${name}" contains a non-ASCII character (U+${cp}) that can homoglyph-masquerade as a different command at runtime — cannot be auto-approved`,
      };
    }
  }
  return { blocked: false };
}

export function redactEnvVarsInOutput(output: string): string {
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

// ---------------------------------------------------------------------------
// OSC 8 hyperlink stripping — removes terminal hyperlink sequences that could
// disguise malicious URLs as innocent-looking text in tool output.
// Format: \x1b]8;params;URI\x1b\\ visible text \x1b]8;;\x1b\\
// ---------------------------------------------------------------------------

export const OSC8_RE = /\x1b\]8;[^\x1b]*(?:\x1b\\|\x07)/g;

// Matches all ANSI CSI escape sequences (colors, cursor movement, etc.)
export const ANSI_OUTPUT_RE = /\x1b\[[0-9;]*[A-Za-z]/g;

// Matches all OSC sequences (not just OSC8) — title sets, clipboard, etc.
// Format: ESC ] ... (BEL | ESC \)
export const OSC_ALL_RE = /\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)/g;

export function stripOsc8Hyperlinks(output: string): string {
  if (!output) return output;
  return output.replace(OSC8_RE, '');
}

// ---------------------------------------------------------------------------
// Unsafe help/man command detector (Gap #49)
// ---------------------------------------------------------------------------
// Some help and man commands can execute arbitrary code, open browsers, or
// process untrusted files. These should NOT be auto-approved even when
// help/man commands are in an allow rule.

export function detectUnsafeHelpCommand(command: string): boolean {
  const stripped = stripInvisibleUnicode(command);

  // --- man unsafe flags ---
  if (/\bman\b/.test(stripped)) {
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bman\s+(?:-\S+\s+)*-\S*l/.test(stripped)) return true;
    // man --local-file — long-form of -l
    if (/\bman\s+.*--local-file/.test(stripped)) return true;
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bman\s+(?:-\S+\s+)*-\S*H/.test(stripped)) return true;
    // man --html — long-form of -H
    if (/\bman\s+.*--html/.test(stripped)) return true;
    // man --pager=<cmd> — arbitrary command as pager
    if (/\bman\s+.*--pager\s*=/.test(stripped)) return true;
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bman\s+(?:-\S+\s+)*-P\s/.test(stripped)) return true;
  }

  // --- git help unsafe flags ---
  if (/\bgit\s+help\b/.test(stripped)) {
    // eslint-disable-next-line security/detect-unsafe-regex -- \S and \s are disjoint, linear scan
    if (/\bgit\s+help\s+(?:-\S+\s+)*-\S*w/.test(stripped)) return true;
    // git help --web — long-form
    if (/\bgit\s+help\s+.*--web/.test(stripped)) return true;
  }

  // --- info with --subnodes piped ---
  if (/\binfo\b/.test(stripped) && /\binfo\s+.*--subnodes/.test(stripped) && /\|/.test(stripped)) {
    return true;
  }

  return false;
}

// Harness side-channel marker tags. `<system-reminder>…</system-reminder>` is the
// ONLY tag AMI injects into the model stream as a trusted, harness-authored side
// channel (see provider.ts / context-manager.ts). Any occurrence of it in TOOL
// OUTPUT is therefore necessarily forged — a command that prints the tag is trying
// to smuggle harness-privileged instructions past the model boundary. We neutralize
// (not delete) both the open and close forms so the surrounding text is preserved
// but the tag loses its side-channel meaning. Canonical definition — also consumed
// by hooks.ts (hook output is the same model boundary), so the escaping lives in
// exactly one place. Parity: the reference strips its `<claude-code-hint …/>`
// harness side channel from model-visible tool output (utils/claudeCodeHints.ts).
export const HARNESS_MARKER_TAG_RE = /<\/?system-reminder>/gi;

/**
 * Neutralize harness side-channel marker tags embedded in text that crosses the
 * model boundary (tool output, hook output), so untrusted content cannot forge a
 * harness-authored `<system-reminder>` block. Replaces the tag with an inert
 * `[tag-escaped]` sentinel, leaving the surrounding text intact.
 */
export function escapeHarnessMarkerTags(text: string): string {
  return text.replace(HARNESS_MARKER_TAG_RE, '[tag-escaped]');
}

export function sanitizeToolOutput(output: string): string {
  if (!output) return output;
  // 1. Strip ANSI CSI escape sequences (colors, cursor, etc.)
  let result = output.replace(ANSI_OUTPUT_RE, '');
  // 2. Strip all OSC sequences (hyperlinks, title sets, clipboard, etc.)
  result = result.replace(OSC_ALL_RE, '');
  // 3. Redact sensitive env vars
  result = redactEnvVarsInOutput(result);
  // 4. Redact credential patterns (API keys, tokens, JWTs, etc.)
  result = redactCredentials(result);
  // 5. Neutralize forged harness side-channel marker tags (prompt-injection guard)
  result = escapeHarnessMarkerTags(result);
  return result;
}

// ---------------------------------------------------------------------------
// find -exec/-delete detection — prevents Bash(find:*) allow rules from
// auto-approving destructive find actions.
// ---------------------------------------------------------------------------

export const FIND_CMD_RE = /\bfind\b/;

export const FIND_DANGEROUS_ACTIONS = /\s-(?:exec|execdir|delete|ok)\b/;

export function detectDangerousFindArgs(command: string): boolean {
  if (!FIND_CMD_RE.test(command)) return false;
  // Extract the find command segment (up to the first pipe, semicolon, or &&/||)
  // so that `find . -type f | xargs -exec ...` doesn't false-positive.
  const match = command.match(/\bfind\b[^|;&]*/);
  if (!match) return false;
  return FIND_DANGEROUS_ACTIONS.test(match[0]);
}

// ---------------------------------------------------------------------------
// Tilde-expansion-variant TOCTOU guard — parity with the reference's validatePath
// tilde-variant rejection (utils/permissions/pathValidation.ts L394-411).
//
// `expandHomeDirReferences` (and the reference's expandTilde) resolve ONLY `~` and
// `~/…` to $HOME. The other tilde forms the shell still expands —
//   ~user  → that user's home (e.g. ~root → /var/root or /root),
//   ~+     → $PWD,
//   ~-     → $OLDPWD,
//   ~N     → the N-th `dirs`/pushd stack entry,
// are left as LITERAL text by the validator and therefore resolve as relative
// paths (e.g. `/cwd/~root/.ssh/id_rsa`). The shell, however, expands them to a
// DIFFERENT absolute path at execution time — a TOCTOU gap where we validate one
// path but bash reads another. Such a command must never be silently auto-run.
//
// Scoped to the path-TRAVERSAL form (a tilde variant immediately followed by a
// `/`, e.g. `~root/.ssh/…`, `~+/x`, `~-/x`, `~2/x`): this is the exfil/escape
// vector and keeps the guard free of false positives on arithmetic bitwise-NOT
// (`$((~5))`) and bare directory references (`~root` with no path). Bare `~` and
// `~/…` (already resolved to an absolute path) are intentionally NOT flagged.
// Only UNQUOTED occurrences count — a quoted `'~root/x'` is literal to bash and
// is removed by `extractQuotedContent`, mirroring the shell's own semantics.
// eslint-disable-next-line security/detect-unsafe-regex -- alternation arms ([A-Za-z_]… | [+-] | \d+) are disjoint on the first char and each is followed by a literal `/`; anchored at a word boundary, linear
export const TILDE_VARIANT_RE = /(?:^|[\s;|&(=])~(?:[A-Za-z_][\w-]*|[+-]|\d+)\//;

export function detectTildeExpansionVariant(command: string): { blocked: boolean; reason?: string } {
  if (!command.includes('~')) return { blocked: false };
  const { fullyUnquoted } = extractQuotedContent(command);
  const m = fullyUnquoted.match(TILDE_VARIANT_RE);
  if (!m) return { blocked: false };
  return {
    blocked: true,
    reason:
      'Tilde-expansion variant (~user, ~+, ~-, ~N) in a path requires manual approval — the shell expands it to a different directory than static validation sees (TOCTOU)',
  };
}
