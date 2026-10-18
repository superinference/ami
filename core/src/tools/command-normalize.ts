export function collapseWhitespace(input: string): string {
  return input.replace(/[\t ]+/g, ' ').trim();
}

export function deobfuscateEnvExpansion(input: string): string {
  // Remove empty-string concatenation tricks: r""m → rm, r''m → rm
  return input.replace(/([a-zA-Z])(?:""|\'\')([a-zA-Z])/g, '$1$2');
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
