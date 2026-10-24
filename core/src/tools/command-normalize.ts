export function collapseWhitespace(input: string): string {
  return input.replace(/[\t ]+/g, ' ').trim();
}

export function deobfuscateEnvExpansion(input: string): string {
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
  let out = input;
  let prev: string;
  do {
    prev = out;
    out = out.replace(/""|''/g, '');
    out = out.replace(/\$\(\s*\)|\$\{\s*\}|``/g, '');
    out = out.replace(/\\([A-Za-z])/g, '$1');
  } while (out !== prev);
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
  for (let i = 0; i < cmd.length; i++) {
    const ch = cmd[i];
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
