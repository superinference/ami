/**
 * Parser + applier for sed in-place edit commands (`sed -i 's/pat/rep/flags' file`).
 *
 * A `sed -i` substitution is semantically a FILE EDIT, not an opaque shell line:
 * it reads a file and rewrites it in place. This module parses the substitution
 * (target path + pattern + replacement + flags) and applies it to file content
 * in memory, so a permission prompt can render a concrete before → after diff of
 * exactly what the command will change BEFORE the user approves it — the same
 * affordance AMI already gives `file_edit` / `notebook_edit` via
 * `common/src/permission-preview.ts`.
 *
 * Tokenization reuses AMI's canonical shell parser (`parseBashAST` +
 * `resolveShellWord`) — there is no second shell grammar here. Only a single,
 * lone `sed` command with a `s///` substitution and `/` delimiter is recognised;
 * anything with pipes, chains, subshells, redirections, globs, multiple files, or
 * a non-substitution script returns `null` so the caller falls back to showing
 * the raw command.
 *
 * Mirrors Claude Code's `tools/BashTool/sedEditParser.ts`
 * (`parseSedEditCommand` / `applySedSubstitution`).
 */

import { randomBytes } from 'crypto';
import * as fs from 'fs';
import { isAbsolute, resolve as resolvePath } from 'path';
import { parseBashAST, resolveShellWord } from './bash-parser';
import type { CommandNode } from './bash-parser';

// BRE→ERE conversion placeholders (null-byte sentinels, never appear in user input)
const BACKSLASH_PLACEHOLDER = '\x00BACKSLASH\x00';
const PLUS_PLACEHOLDER = '\x00PLUS\x00';
const QUESTION_PLACEHOLDER = '\x00QUESTION\x00';
const PIPE_PLACEHOLDER = '\x00PIPE\x00';
const LPAREN_PLACEHOLDER = '\x00LPAREN\x00';
const RPAREN_PLACEHOLDER = '\x00RPAREN\x00';
const BACKSLASH_PLACEHOLDER_RE = new RegExp(BACKSLASH_PLACEHOLDER, 'g');
const PLUS_PLACEHOLDER_RE = new RegExp(PLUS_PLACEHOLDER, 'g');
const QUESTION_PLACEHOLDER_RE = new RegExp(QUESTION_PLACEHOLDER, 'g');
const PIPE_PLACEHOLDER_RE = new RegExp(PIPE_PLACEHOLDER, 'g');
const LPAREN_PLACEHOLDER_RE = new RegExp(LPAREN_PLACEHOLDER, 'g');
const RPAREN_PLACEHOLDER_RE = new RegExp(RPAREN_PLACEHOLDER, 'g');

export type SedEditInfo = {
  /** The file path being edited (as written on the command line, unresolved). */
  filePath: string;
  /** The search pattern (sed regex, pre-BRE/ERE normalisation). */
  pattern: string;
  /** The replacement string (sed replacement, pre-normalisation). */
  replacement: string;
  /** Substitution flags (g, i, m, numeric, …). */
  flags: string;
  /** Whether extended regex is in effect (`-E` / `-r` / `--regexp-extended`). */
  extendedRegex: boolean;
  /**
   * The in-place backup suffix, when one was supplied (`-i.bak` / `-i .bak` /
   * BSD `-i ''`). `undefined` means a plain `-i` / `--in-place` with no backup;
   * an empty string means an explicit empty suffix (BSD "no backup") was given.
   * A non-empty suffix means `sed` writes `<file><suffix>` with the original
   * contents, which the in-memory simulator reproduces so the previewed write is
   * byte-identical to a real `sed -i<suffix>` run.
   */
  inPlaceSuffix?: string;
};

/**
 * Parse a `sed` in-place edit command, returning its edit info or `null` when the
 * command is not a single, simple `sed -i` substitution this module can simulate.
 */
export function parseSedEditCommand(command: string): SedEditInfo | null {
  // Tokenize via AMI's canonical bash parser (no second shell grammar). The
  // command must be exactly one simple `sed` invocation — reject pipelines,
  // lists (`&&`/`;`), subshells, command substitutions, and output redirections,
  // all of which make the "lone file edit" framing wrong.
  const ast = parseBashAST(command);
  // `null` (unparseable/empty) and `PARSE_ABORTED` (too complex to analyze) both
  // mean "not a simple sed edit". `PARSE_ABORTED` is a symbol whose `.type` is
  // `undefined`, so the single `.type !== 'simple'` guard covers it alongside
  // every non-simple node — no dedicated sentinel branch needed.
  if (!ast) return null;
  if ((ast as CommandNode).type !== 'simple') return null;
  const ast2 = ast as CommandNode;
  // A `simple` node only carries `redirections` when it actually redirects, so
  // its mere presence means the command is more than a lone file edit.
  if (ast2.redirections) return null;
  // A simple node always carries a resolved command name and an args array.
  if (resolveShellWord(ast2.command!) !== 'sed') return null;

  const rawArgs = ast2.args!;

  // Resolve quoting/escaping on each argument the way the shell would, so a
  // single-quoted `'s/a\/b/c/'` yields the literal expression `s/a\/b/c/`. Reject
  // any argument carrying an unexpanded command substitution — simulating it
  // would be wrong and a security footgun.
  const args: string[] = [];
  for (const raw of rawArgs) {
    if (raw.includes('$(') || raw.includes('`')) return null;
    args.push(resolveShellWord(raw));
  }

  let hasInPlaceFlag = false;
  let extendedRegex = false;
  let expression: string | null = null;
  let filePath: string | null = null;
  // `undefined` until an `-i` backup suffix is actually supplied, so a plain
  // `-i` keeps the five-field SedEditInfo shape (no `inPlaceSuffix` key).
  let inPlaceSuffix: string | undefined;

  let i = 0;
  while (i < args.length) {
    const arg = args[i]!;

    // -i / --in-place (with optional backup suffix)
    if (arg === '-i' || arg === '--in-place') {
      hasInPlaceFlag = true;
      i++;
      // GNU sed takes no suffix; BSD/macOS sed requires one (possibly empty).
      // Consume a following token only when it looks like a backup suffix
      // (empty or dot-prefixed) — never a flag or a sed script.
      if (i < args.length) {
        const nextArg = args[i]!;
        if (!nextArg.startsWith('-') && (nextArg === '' || nextArg.startsWith('.'))) {
          inPlaceSuffix = nextArg; // record the backup suffix (possibly empty)
          i++; // skip the backup suffix
        }
      }
      continue;
    }
    if (arg.startsWith('-i')) {
      // Inline suffix form: -i.bak
      hasInPlaceFlag = true;
      inPlaceSuffix = arg.slice(2);
      i++;
      continue;
    }

    // Extended-regex flags
    if (arg === '-E' || arg === '-r' || arg === '--regexp-extended') {
      extendedRegex = true;
      i++;
      continue;
    }

    // -e / --expression script (single expression only)
    if (arg === '-e' || arg === '--expression') {
      if (i + 1 < args.length) {
        if (expression !== null) return null;
        expression = args[i + 1]!;
        i += 2;
        continue;
      }
      return null;
    }
    if (arg.startsWith('--expression=')) {
      if (expression !== null) return null;
      expression = arg.slice('--expression='.length);
      i++;
      continue;
    }

    // Any other flag is not safe to simulate.
    if (arg.startsWith('-')) {
      return null;
    }

    // Positional: first is the script, second is the file. More than one file
    // is unsupported for a single-file edit preview.
    if (expression === null) {
      expression = arg;
    } else if (filePath === null) {
      filePath = arg;
    } else {
      return null;
    }

    i++;
  }

  if (!hasInPlaceFlag || !expression || !filePath) {
    return null;
  }

  // Only `s/…/…/…` with `/` as the delimiter is supported.
  if (!expression.startsWith('s/')) {
    return null;
  }

  const rest = expression.slice(2); // drop the leading `s/`

  let pattern = '';
  let replacement = '';
  let flags = '';
  let state: 'pattern' | 'replacement' | 'flags' = 'pattern';
  let j = 0;

  while (j < rest.length) {
    const char = rest[j]!;

    if (char === '\\' && j + 1 < rest.length) {
      // Keep the escape intact for the respective section.
      if (state === 'pattern') {
        pattern += char + rest[j + 1];
      } else if (state === 'replacement') {
        replacement += char + rest[j + 1];
      } else {
        flags += char + rest[j + 1];
      }
      j += 2;
      continue;
    }

    if (char === '/') {
      if (state === 'pattern') {
        state = 'replacement';
      } else if (state === 'replacement') {
        state = 'flags';
      } else {
        // Extra unescaped delimiter inside flags — malformed.
        return null;
      }
      j++;
      continue;
    }

    if (state === 'pattern') {
      pattern += char;
    } else if (state === 'replacement') {
      replacement += char;
    } else {
      flags += char;
    }
    j++;
  }

  // Need pattern + replacement delimiters present (reached the flags section).
  if (state !== 'flags') {
    return null;
  }

  // Only safe substitution flags.
  const validFlags = /^[gpimIM1-9]*$/;
  if (!validFlags.test(flags)) {
    return null;
  }

  return {
    filePath,
    pattern,
    replacement,
    flags,
    extendedRegex,
    ...(inPlaceSuffix !== undefined ? { inPlaceSuffix } : {}),
  };
}

/**
 * Apply a parsed sed substitution to `content` in memory and return the new
 * content. Converts the sed regex/replacement to JavaScript semantics (BRE→ERE
 * when not extended, `&`/`\n` handling, flag mapping). On an invalid regex the
 * original content is returned unchanged (never throws).
 */
export function applySedSubstitution(content: string, sedInfo: SedEditInfo): string {
  let regexFlags = '';
  if (sedInfo.flags.includes('g')) regexFlags += 'g';
  if (sedInfo.flags.includes('i') || sedInfo.flags.includes('I')) regexFlags += 'i';
  if (sedInfo.flags.includes('m') || sedInfo.flags.includes('M')) regexFlags += 'm';

  let jsPattern = sedInfo.pattern
    // Unescape \/ to /
    .replace(/\\\//g, '/');

  // In BRE (no -E), `\+ \? \| \( \)` are the special forms and bare
  // `+ ? | ( )` are literal — the inverse of ERE/JS. Normalise to ERE/JS.
  if (!sedInfo.extendedRegex) {
    jsPattern = jsPattern
      // Protect literal backslashes first.
      .replace(/\\\\/g, BACKSLASH_PLACEHOLDER)
      // Escaped metacharacters → placeholders (become unescaped in JS).
      .replace(/\\\+/g, PLUS_PLACEHOLDER)
      .replace(/\\\?/g, QUESTION_PLACEHOLDER)
      .replace(/\\\|/g, PIPE_PLACEHOLDER)
      .replace(/\\\(/g, LPAREN_PLACEHOLDER)
      .replace(/\\\)/g, RPAREN_PLACEHOLDER)
      // Bare metacharacters are literal in BRE → escape for JS.
      .replace(/\+/g, '\\+')
      .replace(/\?/g, '\\?')
      .replace(/\|/g, '\\|')
      .replace(/\(/g, '\\(')
      .replace(/\)/g, '\\)')
      // Restore placeholders as their JS special forms.
      .replace(BACKSLASH_PLACEHOLDER_RE, '\\\\')
      .replace(PLUS_PLACEHOLDER_RE, '+')
      .replace(QUESTION_PLACEHOLDER_RE, '?')
      .replace(PIPE_PLACEHOLDER_RE, '|')
      .replace(LPAREN_PLACEHOLDER_RE, '(')
      .replace(RPAREN_PLACEHOLDER_RE, ')');
  }

  // sed replacement → JS replacement. `&` = whole match (`$&` in JS); `\&` is a
  // literal `&`. A random salt in the placeholder prevents replacement-string
  // injection of the sentinel itself.
  const salt = randomBytes(8).toString('hex');
  const ESCAPED_AMP_PLACEHOLDER = `___ESCAPED_AMPERSAND_${salt}___`;
  const jsReplacement = sedInfo.replacement
    .replace(/\\\//g, '/')
    .replace(/\\&/g, ESCAPED_AMP_PLACEHOLDER)
    .replace(/&/g, '$$&')
    .replace(new RegExp(ESCAPED_AMP_PLACEHOLDER, 'g'), '&');

  try {
    const regex = new RegExp(jsPattern, regexFlags);
    return content.replace(regex, jsReplacement);
  } catch {
    return content;
  }
}

// Pattern-side backslash escapes that `applySedSubstitution` reproduces with the
// SAME meaning as `sed` — either because it converts them (BRE `\+ \? \| \( \)`)
// or because `sed` BRE and JavaScript `RegExp` agree on them (`\\ \/ \. \* \^ \$
// \[ \]`). Any OTHER backslash escape (`\1`-`\9` backrefs, `\{` intervals, `\<`
// `\>` `\b` word ops, …) would be written differently by the real `sed` binary,
// so a command using one is NOT a faithful simulation candidate.
const FAITHFUL_PATTERN_ESCAPES = new Set([
  '/', '\\', '+', '?', '|', '(', ')', '.', '*', '^', '$', '[', ']',
]);

function sedPatternIsFaithful(pattern: string): boolean {
  // POSIX bracket expressions (`[[:alpha:]]`, `[[.ch.]]`, `[[=a=]]`) are valid in
  // sed but unsupported by JS RegExp, so they would silently diverge.
  if (pattern.includes('[[:') || pattern.includes('[[.') || pattern.includes('[[=')) {
    return false;
  }
  for (let i = 0; i < pattern.length; i++) {
    if (pattern[i] === '\\') {
      const next = pattern[i + 1];
      if (next === undefined || !FAITHFUL_PATTERN_ESCAPES.has(next)) return false;
      i++; // consume the escaped char
    }
  }
  return true;
}

function sedReplacementIsFaithful(replacement: string): boolean {
  // `$` is a literal in a sed replacement but is special in a JS replacement
  // string (`$&`, `$1`, `$$`), so any `$` means the write could diverge.
  if (replacement.includes('$')) return false;
  for (let i = 0; i < replacement.length; i++) {
    if (replacement[i] === '\\') {
      const next = replacement[i + 1];
      // `applySedSubstitution` only reproduces `\/` (escaped delimiter) and `\&`
      // (literal ampersand). Backreferences (`\1`-`\9`), case conversions
      // (`\U \L \l \u \E`), `\n`/`\t`, and a literal `\\` are all written
      // differently by real sed, so they are not faithful candidates.
      if (next !== '/' && next !== '&') return false;
      i++; // consume the escaped char
    }
  }
  return true;
}

/**
 * Whether {@link applySedSubstitution} reproduces this substitution byte-for-byte
 * with what the real `sed` binary would write. Used to gate BOTH the permission
 * preview and the simulated write: only when this is `true` may AMI show a diff
 * and perform the in-memory write; otherwise it falls back to the raw command
 * (no misleading diff) and lets the real `sed` run — so the user never approves a
 * preview that differs from the bytes actually written (the `_simulatedSedEdit`
 * exactness guarantee from Claude Code's SedEditPermissionRequest).
 */
export function canSimulateSedEditFaithfully(sedInfo: SedEditInfo): boolean {
  return sedPatternIsFaithful(sedInfo.pattern) && sedReplacementIsFaithful(sedInfo.replacement);
}

/** A resolved sed in-place edit the simulator can reproduce exactly. */
export interface SedEditSimulation {
  /** Absolute path of the file the edit targets. */
  filePath: string;
  /** The file's current contents (before the edit). */
  oldContent: string;
  /** The contents after applying the substitution with {@link applySedSubstitution}. */
  newContent: string;
  /** The backup suffix, if the command requested one (`-i.bak` → `.bak`). */
  inPlaceSuffix?: string;
}

function defaultReadFileUtf8(absPath: string): string | null {
  try {
    return fs.readFileSync(absPath, 'utf8');
  } catch {
    return null;
  }
}

/**
 * Compute the in-memory result of a `sed -i 's/…/…/' file` command WITHOUT running
 * `sed`, so a permission prompt and the actual write share ONE source of truth for
 * the resulting bytes. Returns `null` (the caller must fall back to the raw shell
 * command) when the command is not a single simple substitution, uses a feature
 * {@link applySedSubstitution} cannot reproduce faithfully, targets an unreadable
 * file, or changes nothing. The file read is injectable for testing.
 */
export function computeSedEdit(
  command: string,
  cwd: string,
  readFile: (absPath: string) => string | null = defaultReadFileUtf8,
): SedEditSimulation | null {
  const sedInfo = parseSedEditCommand(command);
  if (!sedInfo) return null;
  if (!canSimulateSedEditFaithfully(sedInfo)) return null;
  const filePath = isAbsolute(sedInfo.filePath) ? sedInfo.filePath : resolvePath(cwd, sedInfo.filePath);
  const oldContent = readFile(filePath);
  if (oldContent === null) return null;
  const newContent = applySedSubstitution(oldContent, sedInfo);
  if (newContent === oldContent) return null;
  return {
    filePath,
    oldContent,
    newContent,
    ...(sedInfo.inPlaceSuffix !== undefined ? { inPlaceSuffix: sedInfo.inPlaceSuffix } : {}),
  };
}

/** Pluggable filesystem for {@link applySimulatedSedEdit} (defaults to real `fs`). */
export interface SedEditIo {
  readFile?: (absPath: string) => string | null;
  writeFile?: (absPath: string, content: string) => void;
}

function defaultWriteFileUtf8(absPath: string, content: string): void {
  fs.writeFileSync(absPath, content, 'utf8');
}

/**
 * Perform a `sed -i` in-place edit by WRITING the {@link applySedSubstitution}
 * result directly, instead of executing the real `sed` binary — guaranteeing the
 * bytes written are exactly the ones a preview showed (Claude Code's
 * `_simulatedSedEdit` exactness guarantee). Returns the simulation on success, or
 * `null` when the caller must fall back to running the real `sed` command: the
 * command is not a faithful single substitution, a non-empty backup suffix cannot
 * be reproduced (contains `*`, which GNU sed expands to the filename), or the
 * write failed (so the real binary surfaces the canonical error). Never throws.
 */
export function applySimulatedSedEdit(
  command: string,
  cwd: string,
  io: SedEditIo = {},
): SedEditSimulation | null {
  const sim = computeSedEdit(command, cwd, io.readFile ?? defaultReadFileUtf8);
  if (!sim) return null;
  const write = io.writeFile ?? defaultWriteFileUtf8;
  // A non-empty backup suffix means sed first copies the original to
  // `<file><suffix>`. GNU sed expands a `*` in the suffix to the filename, which
  // we do not reproduce — fall back to the real binary for that rare form.
  const suffix = sim.inPlaceSuffix;
  const needsBackup = suffix !== undefined && suffix !== '';
  if (needsBackup && suffix!.includes('*')) return null;
  try {
    if (needsBackup) {
      write(sim.filePath + suffix, sim.oldContent);
    }
    write(sim.filePath, sim.newContent);
  } catch {
    return null;
  }
  return sim;
}
