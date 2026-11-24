// Split from bash-security.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { SENSITIVE_DIR_RE, SENSITIVE_SYSTEM_DIRS } from './bash-security-detect-git-worktree-escape';

/**
 * Return the target directory of a `cd`/`pushd`/`chdir`/PowerShell `Set-Location`
 * segment: the path string for `cd foo`, `''` for a bare `cd` (→ $HOME, not
 * sensitive), or `null` when the segment is not a directory-change command.
 */
export function cdTargetOfSegment(seg: string): string | null {
  const toks = seg.trim().split(/\s+/).filter(Boolean);
  let i = 0;
  while (i < toks.length && /^[A-Za-z_]\w*=/.test(toks[i]!)) i++;
  const base = (toks[i] ?? '').replace(/^.*\//, '').toLowerCase();
  if (base !== 'cd' && base !== 'pushd' && base !== 'chdir' && base !== 'set-location') {
    return null;
  }
  for (i++; i < toks.length; i++) {
    const t = toks[i]!.replace(/^["']|["']$/g, '');
    if (!t || t.startsWith('-')) continue;
    return t;
  }
  return '';
}

/** True when a `cd` TARGET resolves to a credential or sensitive system dir. */
export function isSensitiveCdTarget(dir: string): boolean {
  if (!dir) return false;
  const norm = dir.replace(/\\/g, '/').replace(/\/+$/, '') || dir;
  if (SENSITIVE_DIR_RE.test(norm)) return true;
  if (norm.startsWith('/')) {
    const resolved = require('path').resolve(norm) as string;
    return SENSITIVE_SYSTEM_DIRS.some(sd => resolved === sd || resolved.startsWith(sd + '/'));
  }
  return false;
}

// ---------------------------------------------------------------------------
// PowerShell encoded / download-and-execute guard
// ---------------------------------------------------------------------------
// PowerShell offers several opaque execution and download primitives that evade
// textual security checks: -EncodedCommand (base64 script), web-request download
// cradles piped to Invoke-Expression, base64 decode-to-iex, and LOLBAS download
// utilities (certutil -urlcache, bitsadmin /transfer, Start-BitsTransfer).

export const POWERSHELL_EXE_RE = /\b(?:pwsh|powershell(?:\.exe)?)\b/i;

// PowerShell accepts en-dash (U+2013), em-dash (U+2014) and horizontal-bar
// (U+2015) as equivalent to the ASCII hyphen-minus for parameters
// (SpecialCharacters.IsDash), so `powershell –enc <b64>` is the same as
// `powershell -enc <b64>`. Normalize those visible dash variants at a token
// boundary to `-` before matching, otherwise the flag checks below (which look
// for an ASCII `-`) are trivially bypassed. The `/` prefix (Windows PowerShell
// 5.1's CommandLineParameterParser) is accepted directly by the flag regexes
// via a `[-/]` character class rather than normalized, since `/` is also a
// legitimate path separator.
export const PS_ALT_DASH_RE = /(^|\s)[\u2013\u2014\u2015](?=\S)/g;

// Build an alternation matching any prefix (length >= minLen) of a parameter
// name, mirroring PowerShell's unambiguous parameter abbreviation
// (commandHasArgAbbreviation): `-e`, `-en`, `-enc`, … `-encodedcommand` all
// select -EncodedCommand. Longest-first so the regex engine prefers the full
// word before shorter prefixes.
export function psAbbrevAlternation(word: string, minLen = 1): string {
  const parts: string[] = [];
  for (let n = word.length; n >= minLen; n--) parts.push(word.slice(0, n));
  return parts.join('|');
}

// ---------------------------------------------------------------------------
// Line continuation joining — collapses backslash-newline sequences so
// split commands like r\<NL>m are reassembled before security checks.
// ---------------------------------------------------------------------------

export function joinLineContinuations(command: string): string {
  // A backslash-newline is a line continuation ONLY when an ODD number of
  // backslashes precedes the newline: the final backslash escapes the newline.
  // With an EVEN count the backslashes pair up as literal escape sequences and
  // the newline is a genuine command separator — joining there would hide the
  // second command from per-command checks (e.g. `echo \\<newline>rm -rf /` is
  // two commands in bash). So only odd runs collapse (dropping one backslash +
  // the newline); even runs are left intact.
  return command.replace(/(\\+)\n/g, (match, backslashes: string) =>
    backslashes.length % 2 === 1 ? backslashes.slice(0, -1) : match,
  );
}
