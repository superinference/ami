// Split from bash.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

export const ERROR_HINTS: Array<{ pattern: RegExp; hint: string }> = [
  { pattern: /ENOSPC|No space left on device/i, hint: 'Disk is full. Free up space in the working directory or /tmp. Check usage with `df -h` and find large files with `du -sh * | sort -rh | head`.' },
  { pattern: /Transform failed|SyntaxError:.*Unexpected/i, hint: 'Your code has a syntax error (missing bracket, semicolon, or mismatched quotes). Use file_read to check the file you last edited.' },
  { pattern: /Cannot find module|ERR_MODULE_NOT_FOUND/i, hint: 'Module not found. Check the import path with list_dir or glob, and try running via npm test instead of raw node commands.' },
  { pattern: /ENOENT.*no such file/i, hint: 'File or directory does not exist. Use list_dir to check what files are available.' },
  { pattern: /TypeError:.*is not a function/i, hint: 'A function call is wrong. Use file_read to check the export names and signatures in the imported module.' },
  { pattern: /ReferenceError:.*is not defined/i, hint: 'A variable or import is missing. Check your imports and variable declarations with file_read.' },
  { pattern: /Operation not permitted|\bEPERM\b/i, hint: 'This can be a sandbox restriction — the command ran in an isolated sandbox that blocks writes outside the project, network access, or certain syscalls. If the operation is legitimate, retry the SAME command with dangerouslyDisableSandbox: true.' },
  { pattern: /EACCES.*permission denied/i, hint: 'Permission denied. Check file permissions with ls -la, or use a different directory.' },
  { pattern: /EADDRINUSE/i, hint: 'Port already in use. Try a different port or find and kill the process using it (lsof -i :PORT).' },
  { pattern: /ENOMEM|Cannot allocate memory|out of memory/i, hint: 'Out of memory. Try reducing the workload, closing other processes, or increasing available memory.' },
  { pattern: /ETIMEDOUT|ECONNREFUSED|ECONNRESET/i, hint: 'Network error. Check if the target service is running and reachable.' },
  { pattern: /EPIPE|broken pipe/i, hint: 'Broken pipe. The receiving process terminated before all input was consumed.' },
  { pattern: /EMFILE|ENFILE|too many open files/i, hint: 'Too many open files. Close unused file handles or increase the ulimit (ulimit -n).' },
  { pattern: /TS\d+:/i, hint: 'TypeScript compilation error. Use file_read to check the file at the reported line number.' },
  { pattern: /error\[E\d+\]/i, hint: 'Rust compilation error. Check the file and line number in the error message.' },
  { pattern: /npm ERR!.*ERESOLVE/i, hint: 'NPM dependency resolution conflict. Try npm install --legacy-peer-deps or check package.json version constraints.' },
  { pattern: /fatal: not a git repository/i, hint: 'Not inside a git repository. Check the current directory.' },
  { pattern: /API rate limit exceeded|gh: api rate limit|403.*rate limit/i, hint: 'GitHub API rate limit exceeded. Wait a few minutes before retrying, or authenticate with `gh auth login` for higher limits.' },
  { pattern: /secondary rate limit|abuse detection/i, hint: 'GitHub secondary rate limit triggered. Slow down requests — wait at least 60 seconds before retrying.' },
  { pattern: /missing required scopes|not been granted the required scopes|Resource not accessible by (?:integration|personal access token)|HTTP 403:.*(?:scope|admin)/i, hint: 'GitHub CLI token is missing required scopes. Run `gh auth refresh -h github.com -s repo,workflow` to grant them, or `gh auth login` to re-authenticate.' },
];

export function diagnoseError(output: string): string | null {
  for (const { pattern, hint } of ERROR_HINTS) {
    if (pattern.test(output)) return hint;
  }
  return null;
}

export interface ExitCodeInfo {
  message: string;
  isNonError?: boolean;
}

/**
 * Reduce a raw command token to its base command name for exit-code semantics:
 * strip a PowerShell call operator (`&`/`.`) prefix, surrounding quotes, the
 * directory portion of a path (`/usr/bin/grep` → `grep`, Windows `\` too), and a
 * trailing `.exe` (case-insensitive). Without this, a path-qualified or quoted
 * command (`/usr/bin/grep`, `"grep"`, `grep.exe`) would miss its exit-code rule.
 */
export function normalizeBaseCommand(raw: string): string {
  let b = (raw || '').trim();
  b = b.replace(/^[&.]+\s*/, ''); // PowerShell call/dot-source operator
  if (b.length >= 2 && ((b[0] === '"' && b[b.length - 1] === '"') || (b[0] === "'" && b[b.length - 1] === "'"))) {
    b = b.slice(1, -1);
  }
  const slash = Math.max(b.lastIndexOf('/'), b.lastIndexOf('\\'));
  if (slash >= 0) b = b.slice(slash + 1);
  b = b.replace(/\.exe$/i, '');
  return b;
}

export function interpretExitCode(command: string, exitCode: number): ExitCodeInfo | null {
  const parts = command.trim().split(/[|;&]\s*/);
  const lastCmd = parts[parts.length - 1].trim().split(/\s+/)[0];
  const base = normalizeBaseCommand(lastCmd || command.trim().split(/\s+/)[0]);
  // robocopy uses a bitfield exit code: bits 0-2 (values 1/2/4) are informational
  // success states, bit 3 (8) = mismatched/failed files, bit 4 (16) = fatal. So an
  // exit code < 8 is success, not an error.
  if (base.toLowerCase() === 'robocopy' && exitCode >= 0 && exitCode < 8) {
    const notes: string[] = [];
    if (exitCode & 1) notes.push('files copied');
    if (exitCode & 2) notes.push('extra files/dirs present');
    if (exitCode & 4) notes.push('mismatched files/dirs present');
    const detail = notes.length ? notes.join(', ') : 'no files needed copying';
    return { message: `(robocopy success — exit ${exitCode}: ${detail})`, isNonError: true };
  }
  // grep family: grep/egrep/fgrep/rg (POSIX + ripgrep) and findstr (Windows
  // native text search) all exit 1 on no-match and >=2 on a real error.
  const isGrepFamily = base === 'grep' || base === 'egrep' || base === 'fgrep' || base === 'rg' || base.toLowerCase() === 'findstr';
  if (exitCode === 1 && isGrepFamily) return { message: '(no matches found — not an error)', isNonError: true };
  if (exitCode === 1 && base === 'diff') return { message: '(files differ — not an error)', isNonError: true };
  if (exitCode === 1 && base === 'git') {
    const args = command.trim().split(/\s+/).slice(1);
    if (args[0] === 'grep' || args[0] === 'diff') return { message: '(no matches/differences found — not an error)', isNonError: true };
  }
  if (exitCode === 1 && (base === 'test' || base === '[')) return { message: '(condition is false)', isNonError: true };
  if (exitCode === 1 && base === 'find') return { message: '(some paths were inaccessible)', isNonError: true };
  if (exitCode === 2 && isGrepFamily) return { message: '(error in grep pattern or file access)' };
  if (exitCode === 126) return { message: '(permission denied — command not executable)' };
  if (exitCode === 127) return { message: '(command not found)' };
  if (exitCode === 128 + 9) return { message: '(killed by SIGKILL)' };
  if (exitCode === 128 + 15) return { message: '(killed by SIGTERM)' };
  return null;
}

/**
 * Render a millisecond duration as a compact human-readable string for
 * model/user-facing messages (timeouts). Parity: the reference
 * ShellCommand `Command timed out after ${formatDuration(timeout)}` — e.g.
 * 120000 → "2m", 90000 → "1m 30s", 3661000 → "1h 1m 1s". As a superset over
 * the reference's whole-second rendering, a sub-second value keeps ms precision
 * ("400ms") rather than collapsing to "0s". Non-finite/negative input falls back
 * to the raw `${ms}ms` form so the message is never silently wrong.
 */
export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 1000) return `${ms}ms`;
  const totalSeconds = Math.floor(ms / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts: string[] = [];
  if (days) parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  if (minutes) parts.push(`${minutes}m`);
  if (seconds || parts.length === 0) parts.push(`${seconds}s`);
  return parts.join(' ');
}

export function truncateBashText(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  let half = Math.floor((maxLength - 100) / 2);

  // Avoid splitting a UTF-16 surrogate pair at the head boundary.
  if (half > 0 && half < text.length) {
    const code = text.charCodeAt(half - 1);
    if (code >= 0xD800 && code <= 0xDBFF) half--; // back off lone high surrogate
  }

  // Avoid splitting a UTF-16 surrogate pair at the tail boundary.
  let tailStart = text.length - half;
  if (tailStart > 0 && tailStart < text.length) {
    const code = text.charCodeAt(tailStart);
    if (code >= 0xDC00 && code <= 0xDFFF) tailStart++; // skip lone low surrogate
  }

  return (
    text.slice(0, half) +
    `\n\n... [truncated ${text.length - maxLength + 100} characters] ...\n\n` +
    text.slice(tailStart)
  );
}
