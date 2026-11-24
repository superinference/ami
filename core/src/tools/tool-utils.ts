import * as path from 'path';
import 'fs';
import type { DiffHunk } from '../types';
import './bash-security';
import '../utils/path';
import '../path-safety';
// Re-export the canonical errno-code accessor (now owned by ../utils/errno, the
// single home for typed errno helpers) so tool-utils' own uses and existing
// importers keep resolving it from here without a parallel implementation.
import { getErrnoCode } from '../utils/errno';
export { getErrnoCode };

// ---------------------------------------------------------------------------
// Case-insensitive-filesystem-aware path comparison
// ---------------------------------------------------------------------------

/**
 * Platform seam for case-insensitive-filesystem detection. Default is the real
 * `process.platform`; tests override it to exercise the Windows/macOS case-folding
 * branch deterministically on a case-sensitive CI host.
 */
export const _pathCompareHooks: { platform: NodeJS.Platform } = { platform: process.platform };

/**
 * True when the host filesystem is case-INSENSITIVE by default (Windows, macOS/APFS).
 * On these platforms two spellings of a path that differ only by letter-case name the
 * SAME file, so path-equality/dedup keys must fold case to match the OS.
 */
export function isCaseInsensitiveFs(platform: NodeJS.Platform = _pathCompareHooks.platform): boolean {
  return platform === 'win32' || platform === 'darwin';
}

/**
 * Canonicalize a path for COMPARISON/dedup: always collapse `.`/`..`/duplicate
 * separators via `path.normalize`, and — only on a case-insensitive filesystem —
 * unify separators to `/` and lowercase, so the same file reached via two letter-cases
 * (or mixed `\`/`/`) resolves to one key. On a case-sensitive FS (Linux) this is just
 * `path.normalize` (case preserved). Mirrors the reference's `normalizePathForComparison`.
 */
export function normalizePathForComparison(filePath: string): string {
  // NFC-normalize so the two Unicode spellings of the same filename (e.g. a
  // precomposed "é" U+00E9 vs the decomposed "e" + U+0301) collapse to ONE
  // comparison key — otherwise a path reached via a decomposed spelling would
  // dodge a dedup/containment check that stored the composed form. Mirrors the reference
  // Code's NFC normalization of resolved paths used for containment checks.
  const normalized = path.normalize(filePath).normalize('NFC');
  if (isCaseInsensitiveFs()) {
    return normalized.replace(/\\/g, '/').toLowerCase();
  }
  return normalized;
}

// Lazily compiled rule caches (compiled once on first use). Scan needs
// stateless, non-global `test` regexes; redact needs global `replace` regexes.
// Both derive from the SAME SECRET_RULES, keeping detection + redaction in lockstep.
let scanCompiled: Array<{ id: string; re: RegExp }> | null = null;
let redactCompiled: Array<{ re: RegExp }> | null = null;

function getScanRules(): Array<{ id: string; re: RegExp }> {
  if (scanCompiled === null) {
    scanCompiled = SECRET_RULES.map(r => ({
      id: r.id,
      // eslint-disable-next-line security/detect-non-literal-regexp -- sources are internal constants, never user input
      re: new RegExp(r.source, (r.flags ?? '').replace('g', '')),
    }));
  }
  return scanCompiled;
}

function getRedactRules(): Array<{ re: RegExp }> {
  if (redactCompiled === null) {
    redactCompiled = SECRET_RULES.map(r => ({
      // eslint-disable-next-line security/detect-non-literal-regexp -- sources are internal constants, never user input
      re: new RegExp(r.source, (r.flags ?? '').replace('g', '') + 'g'),
    }));
  }
  return redactCompiled;
}

/**
 * Scan content for leaked credentials. Returns one {@link SecretMatch} per rule
 * that fired (deduplicated by rule id). The matched bytes are DELIBERATELY never
 * returned — callers name the rule/label and must never echo the secret value.
 */
export function scanForSecrets(content: string): SecretMatch[] {
  // Scan the FULL content — NOT a per-line 500-char slice. A single-line .env
  // export, a minified JSON/JS config, a base64 blob, or a one-line connection
  // string can carry a credential past column 500; truncating each line before
  // matching let those slip through the write-time guard entirely. Every rule is
  // prefix-anchored (`sk-`, `AIza`, `ghp_`, …) so a full-content `test` does not
  // backtrack on long non-matching input (freeze-protection test still holds).
  // Mirrors the reference's secretScanner, which tests `rule.re.test(content)`.
  // One entry per rule that fires (rule ids are unique, so results are naturally
  // deduplicated). The matched bytes are never captured into the result.
  const matches: SecretMatch[] = [];
  for (const rule of getScanRules()) {
    if (rule.re.test(content)) {
      matches.push({ ruleId: rule.id, label: ruleIdToLabel(rule.id) });
    }
  }
  return matches;
}

/**
 * Replace detected secret tokens with a stable marker, preserving surrounding
 * boundary characters. Used before writing shared transcripts (/export) so API
 * keys and credentials never land in a file the user might paste or publish.
 */
export function redactSecrets(content: string): string {
  const lines = content.split('\n');
  let out = lines.map(line => {
    const trimmed = line.length > MAX_SCAN_LINE_LENGTH * 2 ? line.slice(0, MAX_SCAN_LINE_LENGTH * 2) + '…' : line;
    return trimmed.replace(INVISIBLE_CHARS_RE, '');
  }).join('\n');

  // 1. Private key blocks — must run before token patterns to avoid partial matches.
  out = out.replace(
    /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
    '[REDACTED-PRIVATE-KEY]',
  );

  // 2. Shared token/credential rules (SAME SECRET_RULES source as scanForSecrets).
  // Every rule captures the secret in group 1; replace ONLY that captured run so
  // the surrounding boundary characters / quotes survive.
  for (const { re } of getRedactRules()) {
    out = out.replace(re, (match: string, secret: string) => match.replace(secret, '[REDACTED-SECRET]'));
  }

  // 3. Bearer / Basic auth headers — redact long token values (>= 20 chars).
  // Handles standard "Bearer <token>", percent-encoded "Bearer%20<token>",
  // and positional variants like "Bearer was <token>" or "Bearer: <token>".
  out = out.replace(
    /((?:Bearer|Basic)(?:\s+|%20))[A-Za-z0-9+/=_.~%-]{20,}/gi,
    '$1[REDACTED-SECRET]',
  );
  out = out.replace(
    /((?:Bearer|Basic)\s+(?:was|is|token|value|=)\s*:?\s*)[A-Za-z0-9+/=_.~%-]{20,}/gi,
    '$1[REDACTED-SECRET]',
  );

  // 4. URL credentials — https://user:password@host (password >= 4 chars).
  // Passwords may contain @, so match greedily up to the last @ before a host.
  out = out.replace(
    /(https?:\/\/[^:@/\s]+:)(.{4,}?)(@[^@/\s]+)/g,
    '$1[REDACTED-SECRET]$3',
  );

  // 5. Database / messaging connection strings — same pattern for known schemes.
  out = out.replace(
    /((?:mysql|postgres|postgresql|mongodb|redis|amqp):\/\/[^:@/\s]+:)(.{4,}?)(@[^@/\s]+)/g,
    '$1[REDACTED-SECRET]$3',
  );

  // 6. JSON-shaped credential: "api_key": "value" (quoted key), covering common aliases.
  // (The simpler `key: "value"` form is covered by the shared credential-assignment rule above.)
  // PARITY ADD (the reference SECRET_FIELD_NAMES — bridge/debugUtils.ts): redact by JSON
  // FIELD NAME regardless of the value's shape, so an opaque custom token under
  // "access_token"/"session_ingress_token"/"environment_secret" (values that match no
  // provider prefix) is scrubbed on the export/log path, not just known-shape tokens.
  out = out.replace(/("(?:password|secret|token|api_?key|access_?token|access_?key|secret_?key|client_secret|auth|session_ingress_token|environment_secret)"\s*:\s*")[^"]{6,}(")/gi, '$1[REDACTED-SECRET]$2');
  // 7. Unquoted KEY=value assignments (e.g. PASSWORD=mysecret, export API_KEY=abc123).
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored (^|\s), fixed keyword alternation, no overlap
  out = out.replace(/((?:^|\s)(?:export\s+)?(?:PASSWORD|SECRET|TOKEN|API_?KEY|ACCESS_?KEY|SECRET_?KEY|CLIENT_SECRET|AUTH|PRIVATE_?KEY|GITHUB_?TOKEN|OPENAI_?KEY|ANTHROPIC_?KEY|DATABASE_?URL|DB_?PASS(?:WORD)?)\s*=\s*)\S{6,}/gim, '$1[REDACTED-SECRET]');
  return out;
}

/**
 * Scan only lines that this edit introduces. A file that already contains
 * `Password = "placeholder"` must stay editable; a newly added secret must not.
 */
export function scanAddedSecrets(before: string, after: string): SecretMatch[] {
  const beforeCounts = new Map<string, number>();
  for (const line of normalizeToLf(before).split('\n')) {
    beforeCounts.set(line, (beforeCounts.get(line) ?? 0) + 1);
  }
  const added: string[] = [];
  for (const line of normalizeToLf(after).split('\n')) {
    const remaining = beforeCounts.get(line) ?? 0;
    if (remaining > 0) beforeCounts.set(line, remaining - 1);
    else added.push(line);
  }
  if (added.length === 0) return [];
  return scanForSecrets(added.join('\n'));
}

import { INVISIBLE_CHARS_RE, MAX_SCAN_LINE_LENGTH, SECRET_RULES, ruleIdToLabel } from './tool-utils-parse-file-mode';
import type { SecretMatch } from './tool-utils-parse-file-mode';
import { DIFF_CONTEXT_LINES, DIFF_SINGLE_HUNK_CONTEXT, DIFF_SNIPPET_CONTEXT_LINES, DIFF_SNIPPET_MAX_BYTES, MAX_DIFF_CELLS, normalizeToLf } from './tool-utils-write-file-atomic-sync';
import type { UnifiedDiffOptions } from './tool-utils-write-file-atomic-sync';
export { ACTION_VERBS, BLOCKED_PATHS, MAX_DIRS_TO_LIST, ORPHANED_PLUGIN_HOLD_DIRNAME, _toolUtilsHooks, describeAction, formatDirectoryList, isBlockedPath, isSensitivePath, isUNCPath, isWithinWorkspaceRoots, outsideWorkspaceMessage, rejectEscapingPattern, resolveDeepestExistingAncestor, resolveSearchPath, sanitizePathForDisplay, validateFilePath, validateRequiredString } from './tool-utils-resolve-deepest-existing-ancestor';
export type { AncestorResolution, ToolDescriptionContext } from './tool-utils-resolve-deepest-existing-ancestor';
export { BOM_PROBE_BYTES, FILE_ENCODINGS, assertReadableTarget, decodeBuffer, describeSymlinkTarget, detectEncoding, detectFileEncoding, encodeText, extractNonNegativeInt, extractQuery, isNonUtfEncoding, plural, resolveFilePath, safeLstat, safeReadFile, validatePatternAndPath } from './tool-utils-resolve-file-path';
export type { FileEncoding, SymlinkDisclosure } from './tool-utils-resolve-file-path';
export { GIT_NO_PROMPT_ENV, buildToolDescriptionContext, deduplicateToolCalls, detectLineEnding, detectVcs, isMcpToolName, parseFileMode, partitionAndSortTools, renderToolDescription, secretWarning, toolCallFingerprint, validateFileWritePath } from './tool-utils-parse-file-mode';
export type { SecretMatch } from './tool-utils-parse-file-mode';
export { DIFF_CONTEXT_LINES, DIFF_SINGLE_HUNK_CONTEXT, DIFF_SNIPPET_CONTEXT_LINES, DIFF_SNIPPET_MAX_BYTES, convertToLineEnding, isMarkdownPath, normalizeToLf, stripTrailingWhitespace, writeFileAtomicSync } from './tool-utils-write-file-atomic-sync';
export type { UnifiedDiffOptions } from './tool-utils-write-file-atomic-sync';

type DiffOpType = ' ' | '-' | '+';
interface DiffOp { type: DiffOpType; line: string }

/** Normalizer used for equality when {@link UnifiedDiffOptions.ignoreWhitespace} is set. */
function diffEq(a: string, b: string, ignoreWhitespace: boolean): boolean {
  if (a === b) return true;
  if (!ignoreWhitespace) return false;
  return a.replace(/\s+/g, ' ').trim() === b.replace(/\s+/g, ' ').trim();
}

/**
 * Exact line-level LCS → a flat op sequence (equal/remove/add). O(n·m); callers
 * must gate on {@link MAX_DIFF_CELLS} before invoking it.
 */
function lineDiffOps(oldLines: string[], newLines: string[], ignoreWhitespace: boolean): DiffOp[] {
  const n = oldLines.length;
  const m = newLines.length;
  const dp: Uint32Array[] = Array.from({ length: n + 1 }, () => new Uint32Array(m + 1));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = diffEq(oldLines[i], newLines[j], ignoreWhitespace)
        ? dp[i + 1][j + 1] + 1
        : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const ops: DiffOp[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (diffEq(oldLines[i], newLines[j], ignoreWhitespace)) {
      ops.push({ type: ' ', line: oldLines[i] });
      i++; j++;
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      ops.push({ type: '-', line: oldLines[i] });
      i++;
    } else {
      ops.push({ type: '+', line: newLines[j] });
      j++;
    }
  }
  while (i < n) ops.push({ type: '-', line: oldLines[i++] });
  while (j < m) ops.push({ type: '+', line: newLines[j++] });
  return ops;
}

/** Render a flat op sequence into one-or-more `@@` hunks with `context` lines around each change. */
/**
 * Group a line-diff op stream into {@link DiffHunk} objects — the ONE canonical
 * hunk-grouping used by BOTH the text renderer ({@link renderHunks}) and the
 * machine-readable {@link buildStructuredPatch}, so the two can never diverge.
 * Every op within `context` positions of a change is kept; contiguous kept runs
 * become one hunk (changes separated by <= 2·context unchanged lines merge, like
 * a unified diff). Each hunk's `lines` carry the leading ` `/`+`/`-` prefix,
 * matching the reference's `structuredPatch` hunk shape. Returns `[]` when there
 * is no change.
 */
function groupHunks(ops: DiffOp[], context: number): DiffHunk[] {
  const changed = ops.map(o => o.type !== ' ');
  if (!changed.some(Boolean)) return [];
  const keep = new Array(ops.length).fill(false);
  for (let k = 0; k < ops.length; k++) {
    if (!changed[k]) continue;
    for (let d = -context; d <= context; d++) {
      const idx = k + d;
      if (idx >= 0 && idx < ops.length) keep[idx] = true;
    }
  }

  const hunks: DiffHunk[] = [];
  let oldNo = 0; // old lines consumed before the current op
  let newNo = 0; // new lines consumed before the current op
  let k = 0;
  while (k < ops.length) {
    if (!keep[k]) {
      if (ops[k].type !== '+') oldNo++;
      if (ops[k].type !== '-') newNo++;
      k++;
      continue;
    }
    const hunkStartOld = oldNo;
    const hunkStartNew = newNo;
    const lines: string[] = [];
    let oldLen = 0;
    let newLen = 0;
    while (k < ops.length && keep[k]) {
      const op = ops[k];
      lines.push(`${op.type}${op.line}`);
      if (op.type !== '+') { oldNo++; oldLen++; }
      if (op.type !== '-') { newNo++; newLen++; }
      k++;
    }
    hunks.push({
      oldStart: oldLen > 0 ? hunkStartOld + 1 : hunkStartOld,
      oldLines: oldLen,
      newStart: newLen > 0 ? hunkStartNew + 1 : hunkStartNew,
      newLines: newLen,
      lines,
    });
  }
  return hunks;
}

function renderHunks(ops: DiffOp[], label: string, context: number): string {
  const hunks = groupHunks(ops, context);
  if (hunks.length === 0) return '(no changes detected)';
  const out: string[] = [`--- ${label}`, `+++ ${label}`];
  for (const h of hunks) {
    out.push(`@@ -${h.oldStart},${h.oldLines} +${h.newStart},${h.newLines} @@`);
    out.push(...h.lines);
  }
  return out.join('\n');
}

/** Cheap O(n) single-region differ used as the degenerate-input fallback. */
function fastSingleRegionDiff(oldLines: string[], newLines: string[], label: string, context: number): string {
  let firstDiff = 0;
  while (firstDiff < oldLines.length && firstDiff < newLines.length && oldLines[firstDiff] === newLines[firstDiff]) {
    firstDiff++;
  }
  let oldEnd = oldLines.length - 1;
  let newEnd = newLines.length - 1;
  while (oldEnd > firstDiff && newEnd > firstDiff && oldLines[oldEnd] === newLines[newEnd]) {
    oldEnd--;
    newEnd--;
  }
  if (firstDiff > oldEnd && firstDiff > newEnd) return '(no changes detected)';

  const ctxStart = Math.max(0, firstDiff - context);
  const result: string[] = [`--- ${label}`, `+++ ${label}`];
  const oldWindowEnd = Math.min(oldLines.length - 1, oldEnd + context);
  const newWindowEnd = Math.min(newLines.length - 1, newEnd + context);
  result.push(`@@ -${ctxStart + 1},${oldWindowEnd - ctxStart + 1} +${ctxStart + 1},${newWindowEnd - ctxStart + 1} @@`);
  for (let i = ctxStart; i < firstDiff; i++) result.push(` ${oldLines[i]}`);
  for (let i = firstDiff; i <= oldEnd; i++) result.push(`-${oldLines[i]}`);
  for (let i = firstDiff; i <= newEnd; i++) result.push(`+${newLines[i]}`);
  for (let i = oldEnd + 1; i <= oldWindowEnd; i++) result.push(` ${oldLines[i]}`);
  return result.join('\n');
}

/**
 * Apply the {@link UnifiedDiffOptions.maxBytes} cap: truncate at the last line
 * boundary within the cap and append a `... [N lines truncated] ...` marker
 * (matching the reference's snippet-truncation convention), so a huge diff can
 * never blow the per-turn token budget.
 */
function capDiffBytes(diff: string, maxBytes: number): string {
  if (Buffer.byteLength(diff, 'utf8') <= maxBytes) return diff;
  // Operate on characters bounded by the byte cap (a byte cap is an upper bound
  // on the char count for UTF-8), finding the last newline that fits.
  let cutoff = diff.lastIndexOf('\n', maxBytes);
  if (cutoff <= 0) cutoff = Math.min(maxBytes, diff.length);
  const kept = diff.slice(0, cutoff);
  const remaining = diff.slice(cutoff).split('\n').length;
  return `${kept}\n\n... [${remaining} lines truncated] ...`;
}

/**
 * Build a unified diff between two text blobs. Produces SEPARATE `@@` hunks for
 * each non-contiguous change region (with `contextLines` of context around each),
 * so scattered edits no longer collapse into one unbounded hunk. Single source of
 * truth for tool diff previews (full-file writes, replace edits, notebook cell
 * edits) so the diff format is never re-implemented per tool. `label` is the
 * path/identifier shown on the ---/+++ header.
 *
 * Options (all optional, defaults preserve prior behavior):
 *  - `contextLines`  — unchanged lines around each change (default 3).
 *  - `ignoreWhitespace` — treat whitespace-only differences as equal.
 *  - `singleHunk` — fold everything into one hunk (context 100 000).
 *  - `maxBytes` — cap the rendered diff, truncating with a `[N lines truncated]` marker.
 */
export function buildUnifiedDiff(
  oldContent: string,
  newContent: string,
  label: string,
  options: UnifiedDiffOptions = {},
): string {
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  const context = options.singleHunk
    ? DIFF_SINGLE_HUNK_CONTEXT
    : (options.contextLines ?? DIFF_CONTEXT_LINES);

  let diff: string;
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) {
    // Degenerate input: skip the exact LCS and degrade to the bounded O(n) differ.
    diff = fastSingleRegionDiff(oldLines, newLines, label, Math.min(context, DIFF_CONTEXT_LINES));
  } else {
    const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
    diff = renderHunks(ops, label, context);
  }

  if (diff === '(no changes detected)') return diff;
  return options.maxBytes !== undefined ? capDiffBytes(diff, options.maxBytes) : diff;
}

/**
 * Build the machine-readable {@link DiffHunk}[] for a file mutation — parity with
 * the reference's FileEdit `structuredPatch` output field, which lets a frontend
 * render/fold a diff per-hunk programmatically instead of re-parsing the text
 * `output`. Shares the EXACT hunk-grouping of {@link buildUnifiedDiff} (via
 * {@link groupHunks}) so the structured patch and the text diff are always
 * consistent. Returns `[]` when there is no change. The degenerate-input guard
 * ({@link MAX_DIFF_CELLS}) returns `[]` rather than an approximate single-region
 * hunk: callers already carry the (capped) text diff for display, and a huge
 * machine patch would blow the per-turn budget it is meant to economize.
 */
export function buildStructuredPatch(
  oldContent: string,
  newContent: string,
  options: { contextLines?: number; ignoreWhitespace?: boolean } = {},
): DiffHunk[] {
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) return [];
  const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
  return groupHunks(ops, options.contextLines ?? DIFF_CONTEXT_LINES);
}

/**
 * Build a BOUNDED, line-numbered snippet of the SURVIVING (non-deleted) content
 * of the change from `before` to `after` — the model-facing view of an
 * out-of-band file modification (a formatter/linter rewrite, a format-on-save),
 * so a stale-file error can show WHAT changed instead of only reporting THAT it
 * changed. Parity: the reference's FileEditTool/utils.ts `getSnippetForTwoFileDiff`
 * (context-8 structuredPatch → keep non-`-` lines → line numbers →
 * hunks joined by `\n...\n` → 8 KiB byte cap with a `[N lines truncated]` marker).
 *
 * Reuses the ONE canonical hunk grouping ({@link buildStructuredPatch}) and the
 * ONE canonical line-boundary truncation ({@link capDiffBytes}) — no parallel
 * diff/snippet logic. Returns `''` when there is no change (nothing to surface).
 */
export function boundedChangeSnippet(before: string, after: string): string {
  const hunks = buildStructuredPatch(before, after, { contextLines: DIFF_SNIPPET_CONTEXT_LINES });
  if (hunks.length === 0) return '';
  const full = hunks
    .map(h => h.lines
      // Keep only surviving lines (context ` ` + additions `+`), dropping the
      // removed (`-`) lines, then strip the one-char diff prefix and number each
      // from the hunk's old-side start (matches addLineNumbers(startLine)).
      .filter(l => !l.startsWith('-'))
      .map((l, i) => `${h.oldStart + i}: ${l.slice(1)}`)
      .join('\n'))
    .join('\n...\n');
  return capDiffBytes(full, DIFF_SNIPPET_MAX_BYTES);
}

/**
 * Count the lines added vs removed between two blobs. Mirrors the reference's
 * `countLinesChanged`: a brand-new file (empty old content) counts every new
 * line as an addition; otherwise additions/removals are tallied independently
 * from the line LCS so a modified line shows as one removal + one addition.
 */
export function countLinesChanged(
  oldContent: string,
  newContent: string,
  options: { ignoreWhitespace?: boolean } = {},
): { additions: number; removals: number } {
  if (oldContent.length === 0) {
    return { additions: newContent.length === 0 ? 0 : newContent.split('\n').length, removals: 0 };
  }
  const oldLines = oldContent.split('\n');
  const newLines = newContent.split('\n');
  if (oldLines.length * newLines.length > MAX_DIFF_CELLS) {
    // Bounded fallback: approximate via the net line-count delta rather than an
    // exact LCS, consistent with buildUnifiedDiff's degenerate-input guard.
    const delta = newLines.length - oldLines.length;
    return { additions: Math.max(0, delta), removals: Math.max(0, -delta) };
  }
  const ops = lineDiffOps(oldLines, newLines, options.ignoreWhitespace ?? false);
  let additions = 0;
  let removals = 0;
  for (const op of ops) {
    if (op.type === '+') additions++;
    else if (op.type === '-') removals++;
  }
  return { additions, removals };
}

/**
 * Count the lines in a blob the way an editor numbers them: a trailing newline
 * is a line TERMINATOR, not the start of a new empty line. `"a\nb\n"` is 2 lines,
 * `"a\nb"` is 2 lines, `""` is 0 lines. Mirrors the reference's `countLines`
 * (`content.endsWith(EOL) ? parts.length - 1 : parts.length`) so a "wrote N lines"
 * display matches editor line numbering instead of over-counting a normal
 * newline-terminated file by one.
 */
export function countContentLines(content: string): number {
  if (content.length === 0) return 0;
  const parts = content.split('\n');
  return content.endsWith('\n') ? parts.length - 1 : parts.length;
}

/**
 * Marker included in file-not-found error messages that carry a working-directory
 * note, so the model can self-correct relative-vs-absolute path confusion and a
 * renderer can detect the "file not found" shape. Parity: the reference's
 * `FILE_NOT_FOUND_CWD_NOTE`.
 */
export const FILE_NOT_FOUND_CWD_NOTE = 'Note: your current working directory is';
