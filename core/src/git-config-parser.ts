/**
 * Lightweight, no-exec parser for `.git/config` files.
 *
 * Reads git config values by parsing the config file DIRECTLY off disk — it never
 * shells out to `git config`. Spawning `git config` over an attacker-controlled
 * repository is an execution surface: git would process `include.path` /
 * `includeIf.*.path` directives (arbitrary local file read) and can trigger
 * command execution via keys such as `core.fsmonitor`, `core.sshCommand`,
 * `core.pager`, and aliases. A direct reader avoids all of that, and additionally
 * degrades gracefully when the git binary is absent, broken, or would hang.
 *
 * Verified against git's `config.c`:
 *   - Section names: case-insensitive, alphanumeric + hyphen
 *   - Subsection names (quoted): case-sensitive, backslash escapes (`\\` and `\"`)
 *   - Key names: case-insensitive, alphanumeric + hyphen
 *   - Values: optional quoting, inline comments (`#` or `;`), backslash escapes
 *
 * Divergence from the reference `gitConfigParser.ts` (intentional, labelled):
 * `parseGitConfigValue` is SYNCHRONOUS (`readFileSync`) rather than async. AMI's
 * repo-identity helpers in `utils/git.ts` (`getRemoteOriginUrl`,
 * `getRepoRemoteHash`, `classifyRepoRemote`) are all synchronous; a sync reader
 * slots into them as a filesystem-first path without rewriting every caller — and
 * a `.git/config` is a tiny file, so the blocking read is negligible. The parse
 * algorithm itself (`parseConfigString`) is pure and shared, so there is one
 * canonical tokenizer regardless of how the bytes are obtained.
 */

import { readFileSync } from 'fs';
import { join } from 'path';

/**
 * Read a single value from a repository's `.git/config` WITHOUT invoking git.
 * Returns the first matching key under the given section/subsection, or `null`
 * when the key is absent, the file is unreadable, or `gitDir` has no config.
 * Never throws.
 */
export function parseGitConfigValue(
  gitDir: string,
  section: string,
  subsection: string | null,
  key: string,
): string | null {
  let config: string;
  try {
    config = readFileSync(join(gitDir, 'config'), 'utf-8');
  } catch {
    return null;
  }
  return parseConfigString(config, section, subsection, key);
}

/**
 * Parse a config value from an in-memory config string. Pure (no I/O) and
 * never-throwing — the canonical tokenizer, exported for direct use and testing.
 */
export function parseConfigString(
  config: string,
  section: string,
  subsection: string | null,
  key: string,
): string | null {
  const lines = config.split('\n');
  const sectionLower = section.toLowerCase();
  const keyLower = key.toLowerCase();

  let inSection = false;
  for (const line of lines) {
    const trimmed = line.trim();

    // Skip empty lines and comment-only lines.
    if (trimmed.length === 0 || trimmed[0] === '#' || trimmed[0] === ';') {
      continue;
    }

    // Section header.
    if (trimmed[0] === '[') {
      inSection = matchesSectionHeader(trimmed, sectionLower, subsection);
      continue;
    }

    if (!inSection) {
      continue;
    }

    // Key-value line: find the key name.
    const parsed = parseKeyValue(trimmed);
    if (parsed && parsed.key.toLowerCase() === keyLower) {
      return parsed.value;
    }
  }

  return null;
}

/**
 * Parse a `key = value` line. Returns `null` when the line has no valid key
 * (e.g. a boolean/valueless key, or a key with no `=`), since those are not
 * relevant to value lookups.
 */
function parseKeyValue(line: string): { key: string; value: string } | null {
  // Read key: alphanumeric + hyphen.
  let i = 0;
  while (i < line.length && isKeyChar(line[i]!)) {
    i++;
  }
  if (i === 0) {
    return null;
  }
  const key = line.slice(0, i);

  // Skip whitespace.
  while (i < line.length && (line[i] === ' ' || line[i] === '\t')) {
    i++;
  }

  // Must have '='.
  if (i >= line.length || line[i] !== '=') {
    // Boolean key with no value — not relevant for value lookups.
    return null;
  }
  i++; // skip '='

  // Skip whitespace after '='.
  while (i < line.length && (line[i] === ' ' || line[i] === '\t')) {
    i++;
  }

  const value = parseValue(line, i);
  return { key, value };
}

/**
 * Parse a config value starting at position `start`. Handles quoted strings,
 * escape sequences, and inline comments. Bounds-checked so malformed input
 * never throws.
 */
function parseValue(line: string, start: number): string {
  let result = '';
  let inQuote = false;
  let i = start;

  while (i < line.length) {
    const ch = line[i]!;

    // Inline comments outside quotes end the value.
    if (!inQuote && (ch === '#' || ch === ';')) {
      break;
    }

    if (ch === '"') {
      inQuote = !inQuote;
      i++;
      continue;
    }

    if (ch === '\\' && i + 1 < line.length) {
      const next = line[i + 1]!;
      if (inQuote) {
        // Inside quotes: recognize escape sequences.
        switch (next) {
          case 'n':
            result += '\n';
            break;
          case 't':
            result += '\t';
            break;
          case 'b':
            result += '\b';
            break;
          case '"':
            result += '"';
            break;
          case '\\':
            result += '\\';
            break;
          default:
            // Git silently drops the backslash for unknown escapes.
            result += next;
            break;
        }
        i += 2;
        continue;
      }
      // Outside quotes: backslash is literal except `\\`.
      if (next === '\\') {
        result += '\\';
        i += 2;
        continue;
      }
      // Fallthrough — treat backslash literally outside quotes.
    }

    result += ch;
    i++;
  }

  // Git trims trailing whitespace that isn't inside quotes. Processing
  // char-by-char with a quote toggle, the correct result for a single-line
  // value is to trim only when the value did not end inside a quote.
  if (!inQuote) {
    result = trimTrailingWhitespace(result);
  }

  return result;
}

function trimTrailingWhitespace(s: string): string {
  let end = s.length;
  while (end > 0 && (s[end - 1] === ' ' || s[end - 1] === '\t')) {
    end--;
  }
  return s.slice(0, end);
}

/**
 * Check whether a header line like `[remote "origin"]` matches the given
 * section/subsection. Section matching is case-insensitive; subsection matching
 * is case-sensitive (parity with git).
 */
function matchesSectionHeader(
  line: string,
  sectionLower: string,
  subsection: string | null,
): boolean {
  // line starts with '['.
  let i = 1;

  // Read section name (stops at ']', whitespace, or an opening quote).
  while (
    i < line.length &&
    line[i] !== ']' &&
    line[i] !== ' ' &&
    line[i] !== '\t' &&
    line[i] !== '"'
  ) {
    i++;
  }
  const foundSection = line.slice(1, i).toLowerCase();

  if (foundSection !== sectionLower) {
    return false;
  }

  if (subsection === null) {
    // Simple section: must end with ']'.
    return i < line.length && line[i] === ']';
  }

  // Skip whitespace before the subsection quote.
  while (i < line.length && (line[i] === ' ' || line[i] === '\t')) {
    i++;
  }

  // Must have an opening quote.
  if (i >= line.length || line[i] !== '"') {
    return false;
  }
  i++; // skip opening quote

  // Read subsection — case-sensitive, handle `\\` and `\"` escapes.
  let foundSubsection = '';
  while (i < line.length && line[i] !== '"') {
    if (line[i] === '\\' && i + 1 < line.length) {
      const next = line[i + 1]!;
      // Git unescapes `\\` and `\"` and drops the backslash for other escapes.
      foundSubsection += next;
      i += 2;
      continue;
    }
    foundSubsection += line[i];
    i++;
  }

  // Must have a closing quote followed by ']'.
  if (i >= line.length || line[i] !== '"') {
    return false;
  }
  i++; // skip closing quote

  if (i >= line.length || line[i] !== ']') {
    return false;
  }

  return foundSubsection === subsection;
}

function isKeyChar(ch: string): boolean {
  return (
    (ch >= 'a' && ch <= 'z') ||
    (ch >= 'A' && ch <= 'Z') ||
    (ch >= '0' && ch <= '9') ||
    ch === '-'
  );
}
