export function collapseWhitespace(input: string): string {
  return input.replace(/[\t ]+/g, ' ').trim();
}

/**
 * Default cap on the deobfuscation fixpoint loop. A legitimate command (even a
 * heavily obfuscated one) converges in only a handful of passes because each
 * pass removes every match of its three global regexes at once, so the number
 * of passes is bounded by the *nesting depth* of the tricks, never their count.
 * The cap only ever matters for pathological/adversarial input that is crafted
 * to keep shrinking by one character per pass (e.g. a long run of backslashes),
 * where an unbounded fixpoint would do O(n) passes of O(n) work (quadratic).
 * The value is far above anything real input reaches, so it never changes the
 * result of a genuine command.
 */
export const DEFAULT_MAX_DEOBFUSCATE_ITERATIONS = 1000;

export interface DeobfuscateOptions {
  /**
   * Maximum number of fixpoint passes. Bounds worst-case work on adversarial
   * input. Defaults to {@link DEFAULT_MAX_DEOBFUSCATE_ITERATIONS}. Mirrors the
   * MAX_NODES / PARSE_TIMEOUT_MS work-budget knobs on upstream command parsers.
   */
  maxIterations?: number;
}

export function deobfuscateEnvExpansion(input: string, options: DeobfuscateOptions = {}): string {
  // Collapse shell obfuscation tricks that split a command word so it evades
  // literal blocklist patterns while still executing as the real command:
  //   r""m / r''m          empty-string concatenation
  //   r$()m / r${}m / r``m  empty command-substitution / parameter-expansion
  //   r\m                   backslash-escaped ordinary letter (shell: \x === x)
  // Iterated to a fixpoint so stacked tricks (e.g. r""""m, r""$()m) fully
  // collapse. The result is used ONLY for security pattern matching, never for
  // execution or display, so aggressive collapsing is safe: at worst it blocks a
  // contrived-looking command, and `echo "hello world"` is left untouched
  // because its quotes enclose content (no empty "" / '' pair).
  //
  // The fixpoint is bounded by maxIterations so adversarial input cannot force
  // super-linear work. Returning a not-yet-converged (partially collapsed)
  // string is still safe for security matching: it is at least as collapsed as
  // the input, so it never hides an obfuscated token the raw input would reveal.
  const maxIterations =
    options.maxIterations !== undefined && options.maxIterations > 0
      ? options.maxIterations
      : DEFAULT_MAX_DEOBFUSCATE_ITERATIONS;
  let out = input;
  let prev: string;
  let iterations = 0;
  do {
    prev = out;
    out = out.replace(/""|''/g, '');
    out = out.replace(/\$\(\s*\)|\$\{\s*\}|``/g, '');
    out = out.replace(/\\([A-Za-z])/g, '$1');
    iterations++;
  } while (out !== prev && iterations < maxIterations);
  return out;
}

const ANSI_RE = /\x1b\[[0-9;]*[A-Za-z]/g;

export function normalizeCommand(input: string): string {
  if (!input) return '';
  let cmd = input;
  // Strip ANSI escape sequences
  cmd = cmd.replace(ANSI_RE, '');
  // Normalize backslash-newline continuations
  cmd = cmd.replace(/\\\n\s*/g, '');
  // Strip inline comments (# outside quotes)
  cmd = stripInlineComment(cmd);
  // Collapse whitespace
  cmd = collapseWhitespace(cmd);
  return cmd;
}

function stripInlineComment(cmd: string): string {
  let inSingle = false;
  let inDouble = false;
  let escaped = false;
  for (let i = 0; i < cmd.length; i++) {
    const ch = cmd[i];
    if (escaped) {
      // The previous char was an unescaped backslash, so this char is a literal
      // and cannot open/close a quote or start a comment. In bash, `\"` inside a
      // double-quoted string does NOT close the quote, and a `\#` outside quotes
      // is a literal `#`, not a comment delimiter. Skipping this char keeps the
      // quote-state tracker in sync so a `#` genuinely inside quotes is never
      // mistaken for a comment (which would silently drop a trailing `; rm -rf`
      // from the text security checks inspect — an under-normalization bypass).
      escaped = false;
      continue;
    }
    if (ch === '\\' && !inSingle) {
      // Backslash escapes the next char everywhere except inside single quotes,
      // where bash treats a backslash literally (no escaping).
      escaped = true;
      continue;
    }
    if (ch === "'" && !inDouble) inSingle = !inSingle;
    else if (ch === '"' && !inSingle) inDouble = !inDouble;
    else if (ch === '#' && !inSingle && !inDouble) {
      // Check if preceded by whitespace (or start of string)
      if (i === 0 || /\s/.test(cmd[i - 1])) {
        return cmd.slice(0, i).trimEnd();
      }
    }
  }
  return cmd;
}
