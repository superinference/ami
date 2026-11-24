// Split from memory.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { log as coreLog } from './logger';
import { abbreviateHome, isPathWithin } from './utils/path';
import { MAX_INSTRUCTION_FILE_SIZE, redactLogPath } from './memory-memory-diagnostic';
import { realpathDeepest } from './memory-truncate-entrypoint-content';

export const INJECTION_PATTERNS = [
  /ignore .{0,10}previous instructions/i,
  /you are now /i,
  /new instructions?:/i,
  /\[SYSTEM\]/i,
  /override safety/i,
  /disregard .{0,10}(safety|rules|instructions)/i,
  /pretend you are/i,
  /from now on.{0,3}you/i,
];

/** Strip zero-width characters and apply NFKD normalization to defeat homoglyph/invisible-char bypasses. */
export function normalizeForScanning(text: string): string {
  // Strip all Unicode format (Cf), control (Cc), and nonspacing mark (Mn) characters
  // This catches RTL overrides, null bytes, zero-width chars, combining marks
  // eslint-disable-next-line security/detect-bidi-characters
  let normalized = text.replace(/[--­؀-؅؜۝܏࣢᠎​-‏‪-‮⁠-⁤⁦-⁯︀-️﻿￰-￸￾￿]/g, '');
  // NFKD normalization — decomposes ligatures and compatibility characters
  normalized = normalized.normalize('NFKD');
  return normalized;
}

/**
 * Strip block-level HTML comments (`<!-- … -->`) from instruction/memory
 * content before it is injected into the prompt. Parity with the reference's
 * `stripHtmlComments` (the reference's markdown-memory module): authorial notes written as HTML
 * comments are editorial, not instructions, and should not reach the model.
 *
 * Preserved verbatim (never stripped):
 *  - comments inside FENCED code blocks (```… ```), which are documentation of
 *    the comment syntax itself;
 *  - comments inside INLINE code spans (`` `…` ``);
 *  - an UNCLOSED `<!--` (no matching `-->`) — a typo must not silently swallow
 *    the rest of the file, so only well-formed spans are removed.
 */
export function stripHtmlComments(content: string): string {
  if (!content.includes('<!--')) return content;
  // Mask code so comment-like text inside it is untouchable, strip the real
  // comment spans, then restore the code. NUL sentinels can't occur in the masks
  // (fenced/inline code), and are fully restored before returning.
  const fenced: string[] = [];
  let masked = content.replace(/```[\s\S]*?```/g, (m) => {
    fenced.push(m);
    return `\u0000F${fenced.length - 1}\u0000`;
  });
  const inline: string[] = [];
  masked = masked.replace(/`[^`\n]*`/g, (m) => {
    inline.push(m);
    return `\u0000I${inline.length - 1}\u0000`;
  });
  masked = masked.replace(/<!--[\s\S]*?-->/g, '');
  masked = masked.replace(/\u0000I(\d+)\u0000/g, (_m, n) => inline[Number(n)]);
  masked = masked.replace(/\u0000F(\d+)\u0000/g, (_m, n) => fenced[Number(n)]);
  return masked;
}

/**
 * Shared source pattern for a bare `@path` import token (`@relative`, `@./rel`,
 * `@~/home`, `@/abs`), allowing an escaped space (`\ `) and a `#fragment` suffix.
 * ONE definition so {@link MemoryManager.resolveAtImports} (inline substitution)
 * and {@link MemoryManager.buildImportGraph} (parent-tracked hierarchy) tokenize
 * imports identically and can never drift. Group 1 is the leading boundary
 * (line-start or whitespace), group 2 the raw path.
 */
export const AT_IMPORT_PATTERN = '(^|\\s)@(~?(?:\\\\ |[\\w./~#-])+)';

/**
 * Yield the PROSE lines of markdown `content` with code masked out — fenced
 * blocks (```` ``` ````) and indented code (≥4 spaces / a tab) are dropped
 * entirely and inline code spans (`` `…` ``) are blanked — so an `@path` written
 * as documentation inside code is never mistaken for a real import. Mirrors the
 * code-skipping {@link MemoryManager.resolveAtImports} applies while inlining, so
 * the import graph only sees the SAME tokens the inline resolver would act on.
 */
export function* prosePlainLines(content: string): Generator<string> {
  let inFence = false;
  for (const raw of content.split('\n')) {
    if (/^\s*```/.test(raw)) { inFence = !inFence; continue; }
    if (inFence) continue;
    if (/^(?: {4}|\t)/.test(raw)) continue;
    yield raw.replace(/`[^`\n]*`/g, '');
  }
}

/**
 * One node in a memory/instruction `@include` hierarchy: the imported file, the
 * file that imported it (`null` for the root entrypoint), and its nesting depth
 * (0 = root). Parity with the reference's `MemoryFileInfo.parent`
 * (the reference's markdown-memory module), which models `@includes` as SEPARATE parent-tracked
 * entries rather than inline substitution.
 */
export interface MemoryImportNode {
  /** Absolute, symlink-resolved path of the imported file. */
  path: string;
  /** Absolute path of the file that imported this one; `null` for the root. */
  parent: string | null;
  /** Nesting depth below the root entrypoint (0 = the root itself). */
  depth: number;
}

/**
 * Render a parent-before-child {@link MemoryImportNode} list as indented tree
 * rows: the root on its own line, each nested import as `"  "×(depth-1) + "L
 * <path>"`. Parity with the reference's MemoryFileSelector nested `L <path>` view.
 * Paths inside `cwd` are shown workspace-relative (`./…`); anything else is
 * home-abbreviated so a user-global entrypoint stays legible.
 */
export function formatImportTree(nodes: MemoryImportNode[], cwd: string): string[] {
  const root = path.resolve(cwd);
  return nodes.map(n => {
    const display = isPathWithin(n.path, root, root)
      ? `./${path.relative(root, n.path)}`
      : abbreviateHome(n.path);
    if (n.depth === 0) return display;
    return `${'  '.repeat(n.depth - 1)}L ${display}`;
  });
}

export function sanitizeContent(content: string, filePath: string): string {
  // Drop editorial HTML comments FIRST (before the size cap) so comment bulk
  // doesn't count toward the truncation budget and comment text never reaches
  // the injection scanner / the model.
  content = stripHtmlComments(content);
  if (content.length > MAX_INSTRUCTION_FILE_SIZE) {
    coreLog('memory', `Instruction file ${redactLogPath(filePath)} exceeds ${MAX_INSTRUCTION_FILE_SIZE} bytes — truncating`);
    let cut = content.slice(0, MAX_INSTRUCTION_FILE_SIZE);
    // Cut back to the last newline inside the cap so an instruction/memory file is
    // never sliced mid-line (which can split a markdown construct or a sentence and
    // feed the model a corrupt half-line). Fall back to the raw slice only when the
    // kept prefix has no newline at all. Append a visible marker so the truncation
    // is never silent — mirrors the entrypoint cap's named-warning behavior.
    const nl = cut.lastIndexOf('\n');
    if (nl > 0) cut = cut.slice(0, nl);
    content = `${cut}\n[truncated — ${path.basename(filePath)} exceeded the ${MAX_INSTRUCTION_FILE_SIZE}-byte cap]`;
  }

  const lines = content.split('\n');
  const clean: string[] = [];
  let injectionFound = false;
  for (const line of lines) {
    const normalized = normalizeForScanning(line);
    if (INJECTION_PATTERNS.some(p => p.test(normalized))) {
      injectionFound = true;
      clean.push(`[BLOCKED: potential prompt injection removed]`);
    } else {
      clean.push(line);
    }
  }
  if (injectionFound) {
    coreLog('memory', `Prompt injection patterns detected in ${redactLogPath(filePath)}`);
  }
  return clean.join('\n');
}

/**
 * Check whether a file path matches any of the given glob patterns from a rule's
 * `paths:` frontmatter. Supports `*` (any characters) and `?` (single character).
 */
export function matchesPaths(pathGlobs: string[], currentFile: string): boolean {
  for (const glob of pathGlobs) {
    // Escape regex special chars except * and ?, then convert glob wildcards
    let escaped = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&');
    escaped = escaped.replace(/\*\*/g, '⧫⧫');
    const pattern = escaped.replace(/\*/g, '[^/]*').replace(/⧫⧫/g, '.*').replace(/\?/g, '[^/]');
    if (new RegExp(`^${pattern}$`).test(currentFile)) return true;
  }
  return false;
}

/**
 * Whether an instruction file is excluded by the {@link MEMORY_EXCLUDES_ENV}
 * glob list. Matched against the raw path, its forward-slash form, AND the
 * symlink-resolved realpath (both forms) so an exclude written against a logical
 * path still matches when the cwd resolves through a symlink (e.g. `/tmp` →
 * `/private/tmp`). Parity with the reference's `isClaudeMdExcluded`
 * realpath-expanded matching.
 */
export function isInstructionExcluded(filePath: string, excludes: string[]): boolean {
  if (excludes.length === 0) return false;
  const candidates = new Set<string>([filePath, filePath.replace(/\\/g, '/')]);
  const real = realpathDeepest(filePath);
  if (real) {
    candidates.add(real);
    candidates.add(real.replace(/\\/g, '/'));
  }
  for (const c of candidates) {
    if (matchesPaths(excludes, c)) return true;
  }
  return false;
}

/**
 * Parse a multi-value frontmatter field (comma-separated or YAML list).
 * Handles both `paths: src/*.ts, lib/*.ts` and multi-line YAML arrays.
 */
export function parseFrontmatterList(value: string): string[] {
  if (!value) return [];
  return value.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

/**
 * Normalize a rule's `paths:` globs the way the reference's `parseFrontmatterPaths`
 * does (the reference's markdown-memory module): if EVERY pattern is the match-all `**` (after ignoring a
 * trailing `/**`, since `path` and `path/**` denote the same membership), the rule is
 * UNCONDITIONAL — `null` is returned so the caller loads it eagerly instead of routing
 * it through the per-file conditional filter.
 *
 * The *matching* globs are returned UNCHANGED (trailing `/**` preserved): AMI's
 * {@link matchesPaths} does a full-string regex match where `src/**` already matches
 * `src/foo.ts` via `**`→`.*`, so stripping `/**` to `src` (which would full-match only
 * the literal string `src`) would break subtree matching. The `/**` strip is therefore
 * applied ONLY to the unconditional decision, not to the patterns handed to the matcher.
 *
 * @returns the glob list to match against, or `null` when the rule applies to all files
 *   (empty after filtering, or every pattern is a match-all double-star).
 */
export function normalizeFrontmatterPaths(globs: string[]): string[] | null {
  const kept = globs.filter(p => p.length > 0);
  const stripped = kept.map(p => (p.endsWith('/**') ? p.slice(0, -3) : p)).filter(p => p.length > 0);
  if (stripped.length === 0 || stripped.every(p => p === '**')) return null;
  return kept;
}

/** Directory name for SuperInference memory storage. */

export const STALE_MEMORY_DAYS = 30;

export function memoryAgeDays(mtimeMs: number, now: number = Date.now()): number {
  // Clamp to 0: a memory is never "from the future". A file mtime slightly ahead
  // of `now` (clock skew, NTP correction, or coarse filesystem mtime
  // granularity) would otherwise yield a negative age and render as the
  // nonsensical "-1 days ago" instead of "today".
  //
  // `now` is injectable so callers that already hold a reference instant (e.g.
  // the SuperInference memory gate computing per-entry staleness against a single
  // snapshot) can compute age deterministically; it defaults to Date.now().
  return Math.max(0, Math.floor((now - mtimeMs) / (1000 * 60 * 60 * 24)));
}

/**
 * Per-read staleness caveat for a memory FILE surfaced through a caller that
 * does NOT add its own system-reminder wrapper (e.g. the file_read tool, which
 * line-numbers and returns raw file content). The note is wrapped in
 * `<system-reminder>…</system-reminder>\n` so the caveat carries system-level
 * trust weight into the transcript rather than reading as ordinary file text a
 * planted memory could spoof. Returns '' for files ≤ 1 day old (not stale).
 *
 * Distinct from read-memory.ts's `memoryFreshnessNote(ageDays)`, which returns a
 * plain-markdown note for a surface that emits the body directly to the model;
 * this variant is for the file_read code path and takes the file mtime so the
 * age is computed via the single canonical {@link memoryAgeDays} clamp. Mirrors
 * the reference's memdir/memoryAge.memoryFreshnessNote(mtimeMs).
 */
export function memoryFreshnessReminder(mtimeMs: number): string {
  const ageDays = memoryAgeDays(mtimeMs);
  if (ageDays <= 1) return '';
  return `<system-reminder>Memory file — last modified ${ageDays} days ago. Verify before relying on this data.</system-reminder>\n`;
}

export function memoryFreshness(ageDays: number): string {
  if (ageDays === 0) return 'today';
  if (ageDays === 1) return 'yesterday';
  if (ageDays <= 7) return `${ageDays} days ago`;
  if (ageDays <= 30) return `${Math.floor(ageDays / 7)} weeks ago`;
  return `${Math.floor(ageDays / 30)} months ago`;
}

/**
 * Parse YAML-like frontmatter from a markdown string.
 * Returns the extracted fields and the body after the frontmatter block.
 *
 * Delimiter matching is LINE-ANCHORED (parity with + superset of the reference's
 * `FRONTMATTER_REGEX` `/^---\s*\n([\s\S]*?)---\s*\n?/`): both the opening and the
 * closing `---` must occupy a line by themselves (trailing whitespace allowed).
 * This fixes two defects of a naive `startsWith('---')` + `indexOf('---', 3)`:
 *   (a) a run-together `---foo` is NOT a valid opener (needs an end-of-line after
 *       the dashes), and
 *   (b) a `---` substring INSIDE a value (e.g. `description: a --- b`) no longer
 *       prematurely closes the block, so later keys (`type:` …) survive.
 *
 * @param sourcePath optional path of the file being parsed; when a block opens
 *   with `---` but is never closed (malformed frontmatter) a `warn` diagnostic is
 *   logged naming the file, mirroring the reference's `logForDebugging(...Failed to
 *   parse YAML frontmatter in ${sourcePath})`, instead of silently swallowing it.
 */
/**
 * Strip a single layer of matching surrounding quotes from a scalar YAML value,
 * unescaping that quote style's escapes. Parity with a real YAML parser's scalar
 * handling (the reference `frontmatterParser`): a value serialized quoted to defeat
 * YAML injection (see {@link sanitizeFrontmatterValue}, e.g. `"a: b"`) must read
 * back as `a: b`, not `"a: b"`. Double-quoted values unescape `\"` and `\\`;
 * single-quoted values unescape the YAML `''` → `'`. A value without matching
 * surrounding quotes (or shorter than two chars) is returned unchanged.
 */
export function stripSurroundingQuotes(value: string): string {
  if (value.length >= 2) {
    const q = value[0];
    if ((q === '"' || q === "'") && value[value.length - 1] === q) {
      const inner = value.slice(1, -1);
      return q === '"'
        ? inner.replace(/\\(["\\])/g, '$1')
        : inner.replace(/''/g, "'");
    }
  }
  return value;
}

/**
 * Parse an inline YAML flow sequence (`[a, b, "c, d"]`) into its items, honoring
 * quoted items so a comma INSIDE quotes does not split the list. Each item is
 * unquoted via {@link stripSurroundingQuotes} and empty items are dropped.
 * Returns `null` when `value` is not a well-formed (closed) `[...]` flow sequence
 * — the caller surfaces that as a malformed-frontmatter diagnostic. Parity with a
 * real YAML parser's flow-sequence handling.
 */
export function parseInlineFlowSequence(value: string): string[] | null {
  if (!(value.startsWith('[') && value.endsWith(']'))) return null;
  const inner = value.slice(1, -1).trim();
  if (inner === '') return [];
  const items: string[] = [];
  let buf = '';
  let quote: '"' | "'" | null = null;
  for (let i = 0; i < inner.length; i++) {
    const ch = inner[i];
    if (quote) {
      buf += ch;
      if (ch === quote) quote = null;
    } else if (ch === '"' || ch === "'") {
      quote = ch;
      buf += ch;
    } else if (ch === ',') {
      items.push(stripSurroundingQuotes(buf.trim()));
      buf = '';
    } else {
      buf += ch;
    }
  }
  items.push(stripSurroundingQuotes(buf.trim()));
  return items.filter(s => s.length > 0);
}
