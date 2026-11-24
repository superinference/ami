import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { fuzzyFindAndReplace, findClosestLines, NOOP_EDIT_ERROR } from './fuzzy-match';
import { areFileEditsEquivalent } from './file-edit-equivalence';
import { detectLineEnding, normalizeToLf, convertToLineEnding, resolveFilePath, scanAddedSecrets, secretWarning, detectFileEncoding, encodeText, decodeBuffer, isBlockedPath, isNonUtfEncoding, validateFilePath, validateFileWritePath, sanitizePathForDisplay, writeFileAtomicSync, buildUnifiedDiff, buildStructuredPatch, countLinesChanged, countContentLines, stripTrailingWhitespace, isMarkdownPath, parseFileMode, FILE_NOT_FOUND_CWD_NOTE } from './tool-utils';
import { getErrnoCode, getErrnoPath, isENOENT, isFsInaccessible } from '../utils/errno';
import { recordAgentFileContribution, computeAgentCharContribution, normalizeAttributionPath } from './attribution-store';
import type { FileEncoding } from './tool-utils';
import { suggestPathUnderCwd, suggestSimilarPaths, suggestPathCorrection } from './path-suggest';
import { extractContextWindow, renderContextWindow, DEFAULT_EDIT_CONTEXT_LINES, readFileCapped } from './read-edit-context';
import '../config';
import '../permissions';
import { coerceBool } from '../utils/coerce';
import { getFileCache } from '../file-cache';

import { trackFileHistory } from './file-history';
import '../memory';
import './file-read-limits';
import { EDIT_DIFF_MAX_BYTES, MAX_EDIT_FILE_SIZE, buildEditMetadata, buildGitDiff, checkEditSizeLimit, firstChangedLine, isTeamMemoryTarget, recheckStaleBeforeWrite, requireReadBeforeEdit, staleFileChangeOutput, validateJsonEdit } from './file-edit-edit-modes';
import type { SequentialEdit } from './file-edit-edit-modes';
import { buildFileEditDescription, disclaimSymlinkTarget, verifyWriteTarget } from './file-edit-verify-write-target';
export { EDIT_MODES } from './file-edit-edit-modes';
export type { EditMode } from './file-edit-edit-modes';
export { buildFileEditDescription, disclaimSymlinkTarget, verifyWriteTarget } from './file-edit-verify-write-target';

/**
 * Size ceiling, in bytes, past which the overwrite path STOPS computing a
 * line-by-line diff and degrades to a size-only summary. Even though the
 * rendered diff is already output-capped at {@link EDIT_DIFF_MAX_BYTES}, the
 * underlying {@link buildUnifiedDiff}/{@link buildStructuredPatch} still split
 * BOTH the whole old and new contents into per-line arrays — an O(file) memory +
 * CPU cost that is wasteful for a many-MB file whose diff nobody can read anyway.
 * Mirrors the reference's MAX_SCAN_BYTES, which degrades a whole-file write to the
 * plain "create" view "rather than OOMing on a diff of a multi-GB file". The hard
 * 1 GiB {@link MAX_EDIT_FILE_SIZE} cap still rejects genuinely huge files first;
 * this bounds the diff work for everything under it.
 */
const EDIT_DIFF_SCAN_MAX_BYTES = 10 * 1024 * 1024; // 10 MiB

/**
 * True when either side of a before/after edit exceeds {@link EDIT_DIFF_SCAN_MAX_BYTES},
 * past which splitting BOTH whole contents into per-line arrays for a unified /
 * structured diff is wasteful O(file) work on a diff nobody can read. Shared by
 * every success path (overwrite, single-replace, replace_all, multi-edit) so the
 * degrade-to-size-only behavior is applied UNIFORMLY, not only on the overwrite
 * path (parity with the reference's MAX_SCAN_BYTES diff fallback across edit modes).
 */
function exceedsDiffScanCap(oldContent: string, newContent: string): boolean {
  return Buffer.byteLength(oldContent) > EDIT_DIFF_SCAN_MAX_BYTES ||
    Buffer.byteLength(newContent) > EDIT_DIFF_SCAN_MAX_BYTES;
}

/**
 * The size-only success body used when {@link exceedsDiffScanCap} holds: report the
 * line count and that the diff was omitted for cost, with no per-line diff work.
 */
function diffOmittedSummary(resolved: string, lineCount: number): string {
  return `Successfully wrote ${resolved} (${lineCount} lines)\n\n(diff omitted: file exceeds the ${(EDIT_DIFF_SCAN_MAX_BYTES / (1024 * 1024)).toFixed(0)} MiB diff cap — showing size only)`;
}

/**
 * Indirection seam for the pre-write file-history backup, matching the
 * {@link lspHooks} pattern. Production calls the real {@link trackFileHistory};
 * tests swap `track` to deterministically inject a concurrent on-disk
 * modification DURING the backup's await, exercising the post-backup staleness
 * re-check (the atomicity guard that closes the check→write race window).
 * Behavior is unchanged in production.
 */
export const fileHistoryHooks: { track: typeof trackFileHistory } = {
  track: trackFileHistory,
};

/**
 * Indirection seam for the memory-bounded existing-file read, matching the
 * {@link fileHistoryHooks} pattern. Production calls the real {@link readFileCapped};
 * tests swap `readCapped` to deterministically return a `truncated` result,
 * exercising the overwrite path's grew-past-the-cap (TOCTOU) branch without
 * creating a multi-GB file. Behavior is unchanged in production.
 */
export const readHooks: { readCapped: typeof readFileCapped } = {
  readCapped: readFileCapped,
};

/**
 * Every parameter the file_edit tool understands, including the accepted aliases
 * for the target path (`path`) and the write-mode content (`content`,
 * `file_text`, `file_content`). Any key outside this set is rejected up front —
 * mirroring the reference's strict input schema — so a hallucinated/typo'd
 * parameter (e.g. `filepath`, `newText`) fails loudly instead of being silently
 * dropped and producing a confusing "file not found" / no-op later.
 */
const ALLOWED_EDIT_KEYS = new Set([
  'file_path', 'path',
  'old_string', 'new_string',
  'content', 'file_text', 'file_content',
  'replace_all', 'edits', 'mode',
]);

/**
 * Indirection seam for the LSP client lookup. Production loads the real client
 * lazily via require; tests swap it to exercise the notification error handling
 * (client lookup failure, rejected notify promises) deterministically. Behavior
 * is unchanged in production.
 */
export const lspHooks: { getClient: () => any; clearDeliveredDiagnostics: (filePath: string) => void } = {
  getClient: () => require('../lsp').getLSPClient(),
  // Invalidate the async-registry cross-turn dedup cache for an edited file so its
  // next diagnostics are re-delivered even if identical to previously shown ones.
  clearDeliveredDiagnostics: (filePath: string) =>
    require('../lsp').getLSPDiagnosticRegistry().clearDeliveredDiagnosticsForFile(filePath),
};

function notifyLsp(resolved: string, content: string, context: ToolContext): void {
  if (context.detachedMode) return;
  try {
    const lsp = lspHooks.getClient();
    lsp.notifyDidChange(resolved, content, context.cwd).catch(() => {});
    lsp.notifyDidSave(resolved, context.cwd).catch(() => {});
  } catch {}
}

/**
 * Snapshot the file's diagnostics into the LSP baseline BEFORE the edit is
 * applied, so a post-edit `diagnostics` query can surface ONLY the issues this
 * edit introduced (new-vs-baseline delta). Fire-and-forget and detached-aware,
 * exactly like {@link notifyLsp}: it never blocks or fails the edit.
 */
export function captureLspBaseline(resolved: string, context: ToolContext): void {
  if (context.detachedMode) return;
  try {
    const lsp = lspHooks.getClient();
    lsp.diagnostics?.beforeFileEdited?.(resolved)?.catch?.(() => {});
  } catch {}
  // An edit changes the file's diagnostics, so forget what was already delivered
  // for it — the next drain must show its fresh diagnostics even if unchanged in text.
  try {
    lspHooks.clearDeliveredDiagnostics(resolved);
  } catch {}
}

export function commitWrite(
  resolved: string,
  rawToWrite: string,
  cacheContent: string,
  context: ToolContext,
  mode?: number,
): string | undefined {
  // Capture the pre-write content (from the warm read-before-edit cache) BEFORE
  // overwriting, so the agent's character contribution can be measured as the
  // added chars of this edit. Cache miss / brand-new file → '' (whole new content
  // counts as authored). Never re-reads disk itself.
  const fileCache = getFileCache(context.cwd, context.agentId);
  const prevContent = fileCache.get(resolved)?.content ?? '';
  try {
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    const symlinkErr = verifyWriteTarget(resolved, context.cwd);
    if (symlinkErr) return symlinkErr;
    // Recover the existing file's BOM/encoding by probing ONLY its leading bytes
    // (detectFileEncoding opens the fd and reads 4 bytes) instead of slurping the
    // whole file just to inspect 4 — the old `readFileSync(resolved).subarray(0,4)`
    // re-read a multi-megabyte file on every write. Absent/unreadable → utf-8.
    const enc: FileEncoding = detectFileEncoding(resolved);
    writeFileAtomicSync(resolved, encodeText(rawToWrite, enc), mode !== undefined ? { mode } : undefined);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return `Error writing file: ${message}`;
  }
  fileCache.setWritten(resolved, cacheContent);
  context.filesRead?.add(resolved);
  notifyLsp(resolved, rawToWrite, context);
  // Record the agent's per-file character contribution for commit attribution
  // (parity: the reference's AttributionSnapshot fileStates). Best-effort — an
  // accounting failure must never fail an otherwise-successful write.
  try {
    recordAgentFileContribution(
      context.sessionId,
      // Store under a realpath-normalized repo-relative key so it matches the key
      // git_commit looks up by (parity: the reference's normalizeFilePath keying).
      normalizeAttributionPath(resolved, context.cwd),
      cacheContent,
      // Char-level authored-chars (common prefix/suffix), which correctly scores
      // same-length and interior replacements a line diff would miss.
      computeAgentCharContribution(normalizeToLf(prevContent), normalizeToLf(cacheContent)),
      Date.now(),
    );
  } catch { /* attribution accounting is best-effort */ }
  return undefined;
}

export async function writeEntireFile(
  resolved: string,
  content: string,
  context: ToolContext,
  mode?: number,
): Promise<ToolResult> {
  if (isBlockedPath(resolved)) {
    return { output: `Error: Cannot edit device or special file: ${resolved}`, isError: true };
  }

  try {
    const targetStat = await fs.promises.stat(resolved);
    if (targetStat.isDirectory()) {
      return { output: `Error: "${sanitizePathForDisplay(resolved)}" is a directory, not a file. Specify a file path instead.`, isError: true };
    }
  } catch {}

  // OOM guard: refuse to read a multi-GB existing file into memory for the
  // overwrite diff. Applies the same cap as the single-replace path.
  const sizeErr = checkEditSizeLimit(resolved);
  if (sizeErr) return sizeErr;

  let oldContent = '';
  let fileExists = false;
  let readErrResult: ToolResult | null = null;
  try {
    // Memory-bounded read: materialize the existing file in chunks under a hard
    // byte cap instead of one unbounded fs.readFile. checkEditSizeLimit stat-guards
    // ABOVE, but a file can grow past that stat before this read (a TOCTOU window,
    // or a pseudo/growing file); the capped read closes it by reporting `truncated`
    // and never slurping the whole over-cap file into one allocation (parity:
    // the reference's readCapped diff-read fallback).
    const capped = await readHooks.readCapped(resolved, MAX_EDIT_FILE_SIZE);
    if (capped === null) {
      // ENOENT — brand-new file, nothing to lose; fileExists stays false.
    } else if (capped.truncated) {
      readErrResult = {
        output: `Error: File exceeds the 1 GiB edit limit (it grew past the size check while being read). Large files cannot be edited.`,
        isError: true,
        behavior: 'ask',
      };
    } else if (isNonUtfEncoding(capped.buffer)) {
      readErrResult = {
        output: `Error: "${resolved}" appears to use a non-UTF encoding (Shift-JIS, GBK, Windows-1252, or similar). Editing it as UTF-8 would silently corrupt the file. Use a dedicated encoding-aware editor instead.`,
        isError: true,
      };
    } else {
      oldContent = decodeBuffer(capped.buffer).text;
      fileExists = true;
    }
  } catch (err) {
    // Only a genuinely-absent file (ENOENT) means "new file, nothing to lose".
    // Any OTHER errno (EACCES/EIO/ELOOP/ENOTDIR…) means the file may exist but
    // could not be read — silently treating it as new and overwriting would
    // clobber unreadable content, so surface an actionable error instead. Name
    // the kernel-reported offending path (getErrnoPath) when it differs from the
    // target (a parent component / symlink target), and flag a grouped
    // inaccessible code distinctly from a transient IO error.
    if (!isENOENT(err)) {
      const code = getErrnoCode(err);
      const offending = getErrnoPath(err);
      const where = offending && offending !== resolved ? ` (at "${offending}")` : '';
      const hint = isFsInaccessible(err)
        ? 'the path is missing, inaccessible, or unreachable — fix the permissions/path'
        : 'resolve the access/IO problem';
      readErrResult = {
        output: `Error: cannot read existing file "${resolved}"${where} to prepare the overwrite (${code}): ${err instanceof Error ? err.message : String(err)}. ${hint} before writing.`,
        isError: true,
      };
    }
  }
  if (readErrResult) return readErrResult;
  const isNew = !fileExists || oldContent.length === 0;

  // H10: read-before-overwrite. Overwriting an existing non-empty file without
  // having read it first risks silently clobbering content the model never saw.
  // Require a prior file_read (which seeds filesRead) before a full overwrite.
  // New-file / empty-file creation is exempt (isNew) — there is nothing to lose.
  // When filesRead tracking is disabled (undefined) the guard does not apply,
  // preserving the lenient behavior for non-tracked contexts.
  // H10: read-before-overwrite. Overwriting an existing non-empty file without
  // having read it first risks silently clobbering content the model never saw.
  // Require a prior file_read (which seeds filesRead) before a full overwrite.
  // New-file / empty-file creation is exempt (isNew) — there is nothing to lose.
  // When filesRead tracking is disabled (undefined) the guard does not apply,
  // preserving the lenient behavior for non-tracked contexts.
  if (fileExists && oldContent.length > 0 && context.filesRead && !context.filesRead.has(resolved)) {
    return {
      output: `Error: You must file_read "${resolved}" before overwriting it, to avoid clobbering content you have not seen. Read the file first, then write.`,
      isError: true,
    };
  }

  if (fileExists && oldContent.length > 0 && context.filesRead?.has(resolved)) {
    const fileCache = getFileCache(context.cwd, context.agentId);
    // A partial/size-capped read is not a safe basis for a full overwrite: the
    // model never saw the tail of the file. Require an explicit full read first.
    if (fileCache.isPartialView(resolved)) {
      return {
        output: `Error: You have only read a PARTIAL view of "${resolved}" (the file was size-capped on read). Read the full file (file_read with allow_large:true, or offset/limit over the whole file) before overwriting it, to avoid clobbering content you have not seen.`,
        isError: true,
      };
    }
    const before = fileCache.peekContent(resolved);
    const change = fileCache.describeChange(resolved);
    if (change.changed) {
      fileCache.delete(resolved);
      return { output: staleFileChangeOutput(change.reason, 'overwriting', before, oldContent), isError: true };
    }
  }

  const secrets = scanAddedSecrets(oldContent, content);
  if (secrets.length > 0) {
    return { output: secretWarning(secrets, 'content', { teamMemory: isTeamMemoryTarget(resolved, context.cwd) }), isError: true };
  }

  const jsonErr = validateJsonEdit(resolved, oldContent, content);
  if (jsonErr) return jsonErr;

  // A full-content write is a deliberate replacement: honor the line endings the
  // model sent in `content` instead of re-converting to the old file's EOL.
  // Re-converting silently corrupted files whose intended EOL differs from the
  // old one (e.g. rewriting a CRLF file to an LF shell script) — parity with
  // the reference's FileWriteTool, which removed that preservation for this reason.
  const finalContent = content;

  if (fileExists && oldContent.length > 0) {
    await fileHistoryHooks.track(resolved, oldContent, context.cwd);
    // The backup above is async: re-check staleness synchronously right before
    // the write so a concurrent modification during that await cannot be
    // silently clobbered (atomicity of the staleness-check→write span).
    const staleErr = recheckStaleBeforeWrite(resolved, 'overwriting', context);
    if (staleErr) return staleErr;
  }

  const writeErr = commitWrite(resolved, finalContent, content, context, mode);
  if (writeErr) return { output: writeErr, isError: true };

  const lines = content.split('\n');
  // Trailing-newline-aware count: a file ending in '\n' is N lines, not N+1 —
  // the empty element after the final newline is a terminator, not a line.
  const lineCount = countContentLines(content);
  if (isNew) {
    const preview = lines.slice(0, 20);
    // Empty-write sentinel: a zero-length write renders a visible '(No content)'
    // marker instead of a bare '+' line / blank preview region.
    const body = content.length === 0 ? '(No content)' : preview.map(l => `+${l}`).join('\n');
    const truncNote = content.length > 0 && lineCount > 20 ? `\n... (+${lineCount - 20} more lines)` : '';
    const createdLabel = !fileExists ? 'Created new file' : 'Populated empty file';
    const createPatch = `--- /dev/null\n+++ ${resolved}\n@@ -0,0 +1,${lineCount} @@\n${body}${truncNote}`;
    return {
      output: `${createdLabel}: ${resolved}\nSuccessfully wrote ${resolved} (new file, ${lineCount} lines)\n\n${createPatch}`,
      structuredPatch: buildStructuredPatch(normalizeToLf(oldContent), normalizeToLf(content)),
      metadata: buildEditMetadata({
        filePath: resolved,
        editMode: 'single',
        replaceAll: false,
        userModified: context.userModified,
        gitDiff: buildGitDiff(resolved, 'added', lineCount, 0, createPatch),
      }),
    };
  }

  // OOM/cost guard: past EDIT_DIFF_SCAN_MAX_BYTES, splitting both whole contents
  // into per-line arrays for a diff is wasteful work on a diff nobody can read.
  // Degrade to a size-only summary (parity: the reference's MAX_SCAN_BYTES fallback to
  // the plain "create" view instead of diffing a multi-GB file).
  if (exceedsDiffScanCap(oldContent, content)) {
    return {
      output: diffOmittedSummary(resolved, lineCount),
      structuredPatch: [],
      structuredPatchTruncated: true,
      metadata: buildEditMetadata({ filePath: resolved, editMode: 'single', replaceAll: false, userModified: context.userModified }),
    };
  }

  // Minimal structured diff: emit only the changed hunks (+ surrounding context)
  // via the common-prefix/suffix differ, instead of marking the entire old file
  // as removed and the entire new file as added. For a small edit inside a large
  // file this keeps the overwrite diff proportional to what actually changed.
  const oldLf = normalizeToLf(oldContent);
  const newLf = normalizeToLf(content);
  const diff = buildUnifiedDiff(oldLf, newLf, resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });
  const { additions, removals } = countLinesChanged(oldLf, newLf);
  return {
    output: `Successfully wrote ${resolved} (${lineCount} lines)\n\n${diff}`,
    structuredPatch: buildStructuredPatch(oldLf, newLf),
    metadata: buildEditMetadata({
      filePath: resolved,
      editMode: 'single',
      replaceAll: false,
      userModified: context.userModified,
      gitDiff: buildGitDiff(resolved, 'modified', additions, removals, diff),
    }),
  };
}

async function applySequentialEdits(
  resolved: string,
  edits: SequentialEdit[],
  context: ToolContext,
): Promise<ToolResult> {
  // OOM guard: enforce the size cap before reading the file (parity with the
  // single-replace and overwrite paths).
  const sizeErr = checkEditSizeLimit(resolved);
  if (sizeErr) return sizeErr;

  let rawContent: string;
  try {
    const rawBuf = await fs.promises.readFile(resolved);
    if (isNonUtfEncoding(rawBuf)) {
      return {
        output: `Error: "${resolved}" appears to use a non-UTF encoding (Shift-JIS, GBK, Windows-1252, or similar). Editing it as UTF-8 would silently corrupt the file. Use a dedicated encoding-aware editor instead.`,
        isError: true,
      };
    }
    rawContent = decodeBuffer(rawBuf).text;
  } catch (err) {
    return {
      output: `Error: Cannot read file "${resolved}": ${err instanceof Error ? err.message : String(err)}`,
      isError: true,
    };
  }

  const readErr = requireReadBeforeEdit(resolved, context);
  if (readErr) return readErr;

  const fileCache = getFileCache(context.cwd, context.agentId);
  const freshBefore = fileCache.peekContent(resolved);
  const freshness = fileCache.describeChange(resolved);
  if (freshness.changed) {
    fileCache.delete(resolved);
    return { output: staleFileChangeOutput(freshness.reason, 'editing', freshBefore, rawContent), isError: true, behavior: 'ask' };
  }

  const originalEnding = detectLineEnding(rawContent);
  let content = normalizeToLf(rawContent);
  // The original (pre-mutation) LF-normalized file, captured once as the stable
  // base for edit-equivalence comparisons below (strings are immutable, so this
  // holds the starting content even as `content` is rewritten per edit).
  const originalLf = content;
  // Markdown (.md/.mdx) uses two trailing spaces as a hard line break, so the
  // trailing-whitespace normalization of the replacement text is skipped there.
  const stripWs = !isMarkdownPath(resolved);

  const applied: string[] = [];
  const failed: string[] = [];
  // Edits already seen (not duplicates) in this batch, used to recognize and skip
  // a later edit that is EQUIVALENT to an earlier one — same result on the
  // original file, or the same reproducible failure — instead of re-applying it
  // (which would double-apply) or surfacing a confusing "not found" once the
  // earlier edit already rewrote the region. Parity: the reference's
  // areFileEditsEquivalent idempotent/duplicate-edit recognition.
  const seenEdits: SequentialEdit[] = [];
  const skipped: string[] = [];
  // Track the new_strings of edits already applied in this batch. A later edit
  // whose old_string is a substring of an earlier edit's new_string is almost
  // always a model mistake (it would re-edit text the previous edit just wrote,
  // not the original file), so reject it — mirroring the reference's sequential-edit
  // guard (FileEditTool/utils.ts). Trailing newlines are stripped before the
  // containment test, matching the reference.
  const appliedNewStrings: string[] = [];

  for (let i = 0; i < edits.length; i++) {
    const { old_string, new_string, replace_all: replaceAll } = edits[i];

    // Idempotent-duplicate guard: if this edit is equivalent to one already seen
    // in the batch (identical inputs, or the same effect / same failure on the
    // original file), skip it rather than re-applying or failing on now-rewritten
    // text. Only a non-empty old_string is a candidate — an empty one is a
    // validation failure handled below.
    if (old_string && old_string.length > 0 &&
        seenEdits.some(prev => areFileEditsEquivalent([prev], [edits[i]], originalLf))) {
      skipped.push(`Edit ${i + 1}: skipped (duplicate of an earlier edit in this batch)`);
      continue;
    }
    seenEdits.push(edits[i]);

    if (!old_string || old_string.length === 0) {
      failed.push(`Edit ${i + 1}: old_string must not be empty`);
      continue;
    }

    if (old_string === new_string) {
      failed.push(`Edit ${i + 1}: old_string and new_string are identical`);
      continue;
    }

    const oldStringToCheck = old_string.replace(/\n+$/, '');
    if (oldStringToCheck !== '' && appliedNewStrings.some(prev => prev.includes(oldStringToCheck))) {
      failed.push(`Edit ${i + 1}: old_string is a substring of a new_string from a previous edit. Each edit must target the original file content, not text inserted by an earlier edit in this batch.`);
      continue;
    }

    const normalizedOld = normalizeToLf(old_string);
    const normalizedNew = normalizeToLf(stripWs ? stripTrailingWhitespace(new_string ?? '') : (new_string ?? ''));

    const result = fuzzyFindAndReplace(content, normalizedOld, normalizedNew);

    if (result.error) {
      if (result.error === NOOP_EDIT_ERROR) {
        failed.push(`Edit ${i + 1}: no change — old_string and new_string are equivalent after normalization`);
        continue;
      }
      if (result.matchCount === 0) {
        const searchLines = old_string.split('\n');
        const hints = findClosestLines(content, searchLines, 2);
        let hintText = '';
        if (hints.length > 0) {
          hintText = ' — did you mean line ' + hints.map(h => h.lineNumber).join(' or ') + '?';
        }
        failed.push(`Edit ${i + 1}: old_string not found in file${hintText}`);
        continue;
      }
      if (replaceAll && result.matchCount > 1) {
        content = content.split(normalizedOld).join(normalizedNew);
        appliedNewStrings.push(normalizedNew);
        applied.push(`Edit ${i + 1}: replaced ${result.matchCount} occurrences`);
        continue;
      }
      failed.push(`Edit ${i + 1}: old_string matches ${result.matchCount} locations. Add more surrounding context to make it unique, or use replace_all: true to replace all occurrences.`);
      continue;
    }

    content = result.newContent!;
    appliedNewStrings.push(normalizedNew);
    const strategyNote = result.strategy !== 'exact' ? ` (${result.strategy})` : '';
    applied.push(`Edit ${i + 1}: applied${strategyNote}`);
  }

  if (applied.length === 0) {
    const notes = [...failed, ...skipped];
    return {
      output: `No edits applied.\n${notes.join('\n')}`,
      isError: true,
      behavior: 'ask',
    };
  }

  const secrets = scanAddedSecrets(rawContent, content);
  if (secrets.length > 0) {
    return { output: secretWarning(secrets, 'edited content', { teamMemory: isTeamMemoryTarget(resolved, context.cwd) }), isError: true };
  }

  const jsonErr = validateJsonEdit(resolved, rawContent, content);
  if (jsonErr) return jsonErr;

  const finalContent = convertToLineEnding(content, originalEnding);
  await fileHistoryHooks.track(resolved, rawContent, context.cwd);
  const staleErr = recheckStaleBeforeWrite(resolved, 'editing', context);
  if (staleErr) return staleErr;
  const writeErr = commitWrite(resolved, finalContent, content, context);
  if (writeErr) return { output: writeErr, isError: true };

  // OOM/cost guard: past the diff-scan cap, computing counts + a structured/unified
  // diff splits both whole contents per line — wasteful work on a diff nobody can
  // read. Degrade to a size-only summary, uniformly with the overwrite path.
  if (exceedsDiffScanCap(rawContent, content)) {
    const summary = [
      `[edit: ${path.basename(resolved)}]`,
      `Successfully edited ${resolved}`,
      `${applied.length}/${edits.length} edits applied.`,
      `\n(diff omitted: file exceeds the ${(EDIT_DIFF_SCAN_MAX_BYTES / (1024 * 1024)).toFixed(0)} MiB diff cap — showing summary only)`,
    ];
    if (skipped.length > 0) summary.push(`\nSkipped edits:\n${skipped.join('\n')}`);
    if (failed.length > 0) summary.push(`\nFailed edits:\n${failed.join('\n')}`);
    return {
      output: summary.join('\n'),
      isError: false,
      structuredPatch: [],
      structuredPatchTruncated: true,
      metadata: buildEditMetadata({ filePath: resolved, editMode: 'multiple', replaceAll: false, userModified: context.userModified }),
    };
  }

  const { additions, removals } = countLinesChanged(originalLf, content);
  // Compute the structured patch once and reuse it for both the location tag and
  // the `structuredPatch` field. Surface the TRUE 1-based line where the first
  // change landed so the model/UI can locate the edit without re-reading the file
  // — the SAME `[edit: file:LINE, +a -b]` tag the single-replace path emits
  // (parity: the reference's readEditContext lineOffset, applied uniformly across
  // both replace modes).
  const patch = buildStructuredPatch(originalLf, content);
  const editLine = firstChangedLine(patch);
  const locationNote = editLine !== undefined ? `:${editLine}` : '';
  const summary = [
    `[edit: ${path.basename(resolved)}${locationNote}, +${additions} -${removals}]`,
    `Successfully edited ${resolved}`,
    `${applied.length}/${edits.length} edits applied (+${additions} -${removals}).`,
  ];
  if (skipped.length > 0) {
    summary.push(`\nSkipped edits:\n${skipped.join('\n')}`);
  }
  if (failed.length > 0) {
    summary.push(`\nFailed edits:\n${failed.join('\n')}`);
  }

  // Append a unified diff so the TUI diff view renders the multi-edit result,
  // just like the single-replace and full-write modes do.
  const diff = buildUnifiedDiff(originalLf, content, resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });
  if (diff !== '(no changes detected)') {
    summary.push(`\n${diff}`);
  }

  return {
    output: summary.join('\n'),
    isError: false,
    structuredPatch: patch,
    metadata: buildEditMetadata({
      filePath: resolved,
      editMode: 'multiple',
      replaceAll: false,
      userModified: context.userModified,
      gitDiff: buildGitDiff(resolved, 'modified', additions, removals, diff),
    }),
  };
}

export const fileEditTool: ToolDefinition = {
  name: 'file_edit',
  // Discovery hint for tool_search: synonyms a model might phrase the intent with
  // that are not in the formal name/description (parity: the reference's Tool.searchHint
  // 'modify file contents in place'). Consumed by tool-search.ts scoring.
  searchHint: 'modify file contents in place create overwrite write edit replace rewrite patch',
  description: buildFileEditDescription(),
  inputSchema: {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to create or edit.',
      },
      old_string: {
        type: 'string',
        description:
          'Exact text to replace. Omit or pass empty string to write/create the entire file using new_string or content.',
      },
      new_string: {
        type: 'string',
        description: 'Replacement text (must be different from old_string), or the full file contents when old_string is empty/omitted.',
      },
      content: {
        type: 'string',
        description: 'Alias for new_string when writing or creating a file (old_string empty/omitted).',
      },
      replace_all: {
        type: 'boolean',
        description: 'Replace all occurrences of old_string instead of requiring uniqueness (useful to rename a variable). Default false.',
        default: false,
      },
      mode: {
        type: 'string',
        description: 'Optional octal permission bits (e.g. "600", "755") to apply when CREATING or overwriting the file in write mode. Ignored for in-place replace/edits (existing permissions are preserved). Use to create a private secret (600) or an executable script (755).',
      },
      edits: {
        type: 'array',
        description: 'Sequential search-and-replace edits applied in order to one file.',
        items: {
          type: 'object',
          properties: {
            old_string: { type: 'string', description: 'The exact string to find.' },
            new_string: { type: 'string', description: 'The replacement string (must be different from old_string).' },
            replace_all: { type: 'boolean', description: 'Replace every occurrence of this old_string.' },
          },
          required: ['old_string', 'new_string'],
        },
      },
    },
    required: ['file_path'],
  },
  // Structured result contract (parity: the reference's FileEditTool `outputSchema`).
  // Describes the keys {@link buildEditMetadata} attaches to a successful edit's
  // {@link ToolResult.metadata}, so a resumed/persisted result can be validated and
  // a frontend can read the edit's shape without re-parsing the text output.
  outputSchema: {
    type: 'object',
    properties: {
      filePath: { type: 'string', description: 'The file path that was edited.' },
      editMode: { type: 'string', description: 'single (one edit / write) or multiple (an edits[] batch).' },
      replaceAll: { type: 'boolean', description: 'Whether every occurrence was replaced.' },
      userModified: { type: 'boolean', description: 'Whether the applied content diverged from the model\'s proposal because the user edited this call\'s input at the permission prompt (approve-with-edited-input) before approving.' },
      structuredPatch: { type: 'array', description: 'Per-hunk structured diff of the change.' },
      gitDiff: {
        type: 'object',
        description: 'Change summary: status, additions/deletions/changes counts, unified patch.',
        properties: {
          filename: { type: 'string' },
          status: { type: 'string', description: 'modified or added.' },
          additions: { type: 'number' },
          deletions: { type: 'number' },
          changes: { type: 'number' },
          patch: { type: 'string' },
        },
      },
    },
  },
  isReadOnly: false,
  // Data-minimization projection for the auto-mode safety classifier (parity: the reference's
  // per-tool `Tool.toAutoClassifierInput`). The security-relevant signal of an edit is
  // WHICH file is being written and in what mode — NOT the (potentially large / secret)
  // content. Expose only the path + write-vs-replace + any octal mode bits; never ship
  // `content`/`old_string`/`new_string` to the secondary reviewer. Resolved uniformly
  // via {@link import('./auto-classifier-projection').resolveAutoClassifierInput}.
  toAutoClassifierInput: (input) => {
    const filePath = (input.file_path ?? input.path);
    if (typeof filePath !== 'string' || filePath.trim().length === 0) return '';
    const hasOldString = typeof input.old_string === 'string' && input.old_string.length > 0;
    const action = hasOldString || Array.isArray(input.edits) ? 'edit' : 'write';
    const mode = typeof input.mode === 'string' && input.mode.length > 0 ? ` mode=${input.mode}` : '';
    return `${action} ${filePath}${mode}`;
  },

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    let filePath = (input.file_path ?? input.path) as string;
    const replaceAll = coerceBool(input.replace_all);
    const editsInput = input.edits;

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    // Strict schema: reject unknown parameters instead of silently ignoring them.
    const unknownKeys = Object.keys(input).filter(k => !ALLOWED_EDIT_KEYS.has(k));
    if (unknownKeys.length > 0) {
      return {
        output: `Error: Unknown parameter(s): ${unknownKeys.join(', ')}. Allowed: file_path, old_string, new_string, content, replace_all, edits.`,
        isError: true,
      };
    }

    const nullCheck = validateFilePath(filePath);
    if (nullCheck) return nullCheck;

    // Glob patterns are never valid as a write/edit target — the tool uses the
    // path literally, so a `*`/`?` wildcard can only be a model mistake that would
    // either create a literally-named file or silently miss the intended one.
    // Reject with an actionable, self-correcting message (parity with the reference
    // Code's write-path glob guard). Only the unambiguous wildcards `*`/`?` are
    // flagged so legitimate bracket/brace filenames (e.g. Next.js `[id].tsx`,
    // `{a,b}.ts`) stay editable — an intentional superset divergence from
    // the reference's broader `[*?[]{}]` set to avoid breaking real filenames.
    if (/[*?]/.test(filePath)) {
      return {
        output: `Error: Glob patterns are not allowed in write operations. Please specify an exact file path (got "${filePath}").`,
        isError: true,
      };
    }

    // Expand a leading `~` to the home directory BEFORE path resolution so the
    // tool writes to the SAME canonical path the permission layer checks (which
    // also expands `~`). Otherwise `~/notes.txt` would resolve to a literal `~`
    // directory under cwd while the permission allowlist was evaluated against
    // <home>/notes.txt — a bypass. Parity: the reference's expandPath-before-write.
    if (filePath === '~' || filePath.startsWith('~/') || filePath.startsWith('~\\')) {
      filePath = path.join(os.homedir(), filePath.slice(1));
    }

    if (filePath.startsWith('\\\\') || filePath.startsWith('//')) {
      return { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${filePath}`, isError: true };
    }

    // Block device/special files before path resolution so the guard fires
    // regardless of workspace boundaries.
    if (isBlockedPath(filePath)) {
      return { output: `Error: Cannot edit device or special file: ${filePath}`, isError: true };
    }

    const { resolved, error: pathError } = resolveFilePath(filePath, context.cwd, context.allowedReadRoots, 'write');
    if (pathError) return pathError;

    if (isBlockedPath(resolved)) {
      return { output: `Error: Cannot edit device or special file: ${resolved}`, isError: true };
    }

    // Symlink guard: block writes to symlinks to prevent redirect attacks. When the
    // block fired on a symlink, disclose WHERE it redirects (resolved target +
    // inside/outside-workspace) so the blocked boundary crossing is visible to the
    // user (parity: the reference's symlink-target surfacing, adapted to AMI's
    // stronger block-all-symlinks posture). A non-symlink failure gets no suffix.
    const writePathCheck = validateFileWritePath(resolved);
    if (!writePathCheck.valid) {
      return { output: writePathCheck.error! + disclaimSymlinkTarget(resolved, context.cwd), isError: true };
    }

    try {
      const targetStat = await fs.promises.stat(resolved);
      if (targetStat.isDirectory()) {
        return { output: `Error: "${sanitizePathForDisplay(resolved)}" is a directory, not a file. Specify a file path instead.`, isError: true };
      }
    } catch {}

    if (resolved.endsWith('.ipynb')) {
      return {
        output: `Error: Cannot edit Jupyter notebooks (.ipynb) with file_edit. Use notebook_edit instead, which understands cell structure and properly updates execution state.`,
        isError: true,
      };
    }

    // Capture a pre-edit diagnostics baseline so the post-edit delta is accurate.
    captureLspBaseline(resolved, context);

    // Markdown (.md/.mdx) treats two trailing spaces as a hard line break, so the
    // trailing-whitespace normalization of inserted/written text is skipped there.
    const stripWs = !isMarkdownPath(resolved);

    if (editsInput !== undefined) {
      if (!Array.isArray(editsInput) || editsInput.length === 0) {
        return { output: 'Error: edits must be a non-empty array.', isError: true };
      }
      const edits: SequentialEdit[] = [];
      for (const raw of editsInput) {
        if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
          return { output: 'Error: each edit must be an object with old_string and new_string.', isError: true };
        }
        const e = raw as Record<string, unknown>;
        edits.push({
          old_string: String(e.old_string ?? ''),
          new_string: String(e.new_string ?? ''),
          replace_all: coerceBool(e.replace_all),
        });
      }
      return applySequentialEdits(resolved, edits, context);
    }

    const newString = (input.new_string ?? input.content ?? input.file_text ?? input.file_content) as string | undefined;
    const oldString = input.old_string as string | undefined;
    const writeMode = oldString === undefined || oldString === null || oldString === '';

    if (writeMode) {
      if (newString === undefined || newString === null) {
        return { output: 'Error: new_string (or content) must be provided to create or overwrite a file.', isError: true };
      }
      // Explicit permission bits only apply to a create/overwrite; parse them
      // here so an invalid value simply falls back to default perms.
      const mode = parseFileMode(input.mode);
      return writeEntireFile(resolved, stripWs ? stripTrailingWhitespace(newString) : newString, context, mode);
    }

    if (newString === undefined || newString === null) {
      return { output: 'Error: new_string must be provided.', isError: true };
    }

    if (oldString === newString) {
      return {
        output: 'Error: old_string and new_string are identical. No changes needed.',
        isError: true,
        behavior: 'ask',
      };
    }

    const sizeErr = checkEditSizeLimit(resolved);
    if (sizeErr) return sizeErr;

    let rawContent: string;
    try {
      const rawBuf = await fs.promises.readFile(resolved);
      if (isNonUtfEncoding(rawBuf)) {
        return {
          output: `Error: "${resolved}" appears to use a non-UTF encoding (Shift-JIS, GBK, Windows-1252, or similar). Editing it as UTF-8 would silently corrupt the file. Use a dedicated encoding-aware editor instead.`,
          isError: true,
        };
      }
      rawContent = decodeBuffer(rawBuf).text;
    } catch (err: unknown) {
      if (isENOENT(err)) {
        let suggestions = '';
        // Canonical "did you mean" recovery, reusing the ONE shared path-suggestion
        // helper (the same one file_read uses) instead of a crude 3-char-prefix
        // substring scan. suggestSimilarPaths ranks a same-basename/different-
        // extension sibling (e.g. `config.ts` requested, `config.toml` present) and a
        // case-only mismatch ahead of plain typos — parity with the reference's
        // findSimilarFile (same base name, different extension) — and never over-
        // matches on a shared 3-letter prefix.
        const similar = suggestSimilarPaths(resolved, path.dirname(resolved), 5);
        if (similar.length > 0) {
          suggestions = `\nDid you mean: ${similar.map(s => path.basename(s)).join(', ')}?`;
        } else {
          // Wrong-ancestor recovery: when the IMMEDIATE parent directory does not
          // exist (a misspelled directory component), walk up to the nearest
          // existing ancestor and correct the missing segment there.
          const corrected = suggestPathCorrection(resolved, context.cwd);
          if (corrected.kind === 'missing-dir' && corrected.suggestions.length > 0) {
            suggestions = `\nDid you mean: ${corrected.suggestions.join(', ')}?`;
          }
        }
        // "Dropped repo folder" correction: when the model built an absolute path
        // missing the repo-directory segment, point it at the real path under cwd.
        const underCwd = suggestPathUnderCwd(resolved, context.cwd);
        if (underCwd) suggestions += `\nDid you mean "${underCwd}" (under the working directory)?`;
        // Working-directory note so the model can resolve relative-vs-absolute
        // path confusion from the boundary context it was operating in.
        return { output: `Error: File not found: ${resolved}${suggestions}\n${FILE_NOT_FOUND_CWD_NOTE} ${context.cwd}`, isError: true };
      }
      // Grouped fs-inaccessible errno (EACCES/EPERM/ENOTDIR/ELOOP — ENOENT already
      // handled above): the file is present but cannot be read, which is a
      // different, more actionable failure than a transient IO error. Name the
      // kernel-reported offending path when it differs from the target.
      if (isFsInaccessible(err)) {
        const offending = getErrnoPath(err);
        const where = offending && offending !== resolved ? ` (at "${offending}")` : '';
        return {
          output: `Error: "${resolved}"${where} cannot be read (${getErrnoCode(err)}): the path is missing, inaccessible, or unreachable. Fix the permissions/path and retry.`,
          isError: true,
        };
      }
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error reading file: ${message}`,
        isError: true,
      };
    }

    const readErr = requireReadBeforeEdit(resolved, context);
    if (readErr) return readErr;

    const fileCache = getFileCache(context.cwd, context.agentId);
    const freshBefore = fileCache.peekContent(resolved);
    const freshness = fileCache.describeChange(resolved);
    if (freshness.changed) {
      fileCache.delete(resolved);
      return { output: staleFileChangeOutput(freshness.reason, 'editing', freshBefore, rawContent), isError: true, behavior: 'ask' };
    }

    const originalEnding = detectLineEnding(rawContent);
    const content = normalizeToLf(rawContent);
    const normalizedOld = normalizeToLf(oldString);
    const normalizedNew = normalizeToLf(stripWs ? stripTrailingWhitespace(newString) : newString);

    // Large-needle short-circuit: an old_string longer than the ENTIRE file can
    // never match, so skip the full-file scan + the O(needle) overlap-buffer
    // allocation fuzzyFindAndReplace would make for a pathologically large needle
    // (parity: the reference's CHUNK_SIZE large-old_string fast path, which diffs inputs
    // instead of scanning). Emit a precise diagnostic instead of the generic
    // "not found" so the model learns the needle is oversized, not merely absent.
    if (normalizedOld.length > content.length) {
      fileCache.delete(resolved);
      return {
        output: `Error: old_string (${normalizedOld.length} chars) is larger than the entire file "${resolved}" (${content.length} chars), so it cannot match. Re-read the file with file_read and target a smaller, exact span.`,
        isError: true,
        behavior: 'ask',
      };
    }

    const result = fuzzyFindAndReplace(content, normalizedOld, normalizedNew);

    if (result.error) {
      if (result.error === NOOP_EDIT_ERROR) {
        return {
          output: `Error: the edit to ${resolved} produces no change — old_string and new_string are equivalent after normalization. No edit applied.`,
          isError: true,
          behavior: 'ask',
        };
      }
      if (result.matchCount === 0) {
        fileCache.delete(resolved);

        const searchLines = oldString.split('\n');
        const hints = findClosestLines(content, searchLines, 3);
        let hintText = '';
        if (hints.length > 0) {
          hintText = '\n\nDid you mean one of these lines?\n' +
            hints.map(h => `  Line ${h.lineNumber}: ${h.line}`).join('\n');
        }
        // When a close match exists, show a bounded context WINDOW around it with
        // true 1-based line numbers and omission markers (parity: the reference's
        // scanForContext/sliceContext, which show the match's surroundings re-based
        // to its real file position with a truncation signal) — far more useful on
        // a large file than always dumping the first 30 lines from the top. With no
        // close match, fall back to the from-the-top preview.
        let previewBlock: string;
        if (hints.length > 0) {
          const window = extractContextWindow(content, hints[0].lineNumber, DEFAULT_EDIT_CONTEXT_LINES * 3);
          previewBlock = `\n\nFile content around the closest match (lines ${window.lines[0].lineNumber}-${window.lines[window.lines.length - 1].lineNumber}):\n${renderContextWindow(window)}`;
        } else {
          const lines = content.split('\n');
          const preview = lines.slice(0, 30).map((l, i) => `${i + 1}: ${l}`).join('\n');
          const truncNote = lines.length > 30 ? `\n... (${lines.length - 30} more lines)` : '';
          previewBlock = `\n\nCurrent file content (first 30 lines):\n${preview}${truncNote}`;
        }
        return {
          output: `Error: old_string not found in ${resolved}. The string must match the file content EXACTLY. Re-read the file with file_read before retrying.${hintText}${previewBlock}`,
          isError: true,
          behavior: 'ask',
        };
      }
      if (replaceAll) {
        const replaced = content.split(normalizedOld).join(normalizedNew);
        const addedSecrets = scanAddedSecrets(content, replaced);
        if (addedSecrets.length > 0) {
          return { output: secretWarning(addedSecrets, 'edited content', { teamMemory: isTeamMemoryTarget(resolved, context.cwd) }), isError: true };
        }
        const jsonErr = validateJsonEdit(resolved, rawContent, replaced);
        if (jsonErr) return jsonErr;
        const finalContent = convertToLineEnding(replaced, originalEnding);
        await fileHistoryHooks.track(resolved, rawContent, context.cwd);
        const staleErr = recheckStaleBeforeWrite(resolved, 'editing', context);
        if (staleErr) return staleErr;
        const writeErr = commitWrite(resolved, finalContent, normalizeToLf(finalContent), context);
        if (writeErr) return { output: writeErr, isError: true };
        // Diff-cost cap: skip the O(file) unified + structured diff past the cap,
        // uniformly with the overwrite and multi-edit paths.
        if (exceedsDiffScanCap(content, replaced)) {
          return {
            output: `Successfully replaced ${result.matchCount} occurrences in ${resolved}\n\n(diff omitted: file exceeds the ${(EDIT_DIFF_SCAN_MAX_BYTES / (1024 * 1024)).toFixed(0)} MiB diff cap — showing summary only)`,
            structuredPatch: [],
            structuredPatchTruncated: true,
            metadata: buildEditMetadata({ filePath: resolved, editMode: 'single', replaceAll: true, userModified: context.userModified }),
          };
        }
        const diff = buildUnifiedDiff(content, replaced, resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });
        const raCounts = countLinesChanged(content, replaced);
        return {
          output: `Successfully replaced ${result.matchCount} occurrences in ${resolved}\n\n${diff}`,
          structuredPatch: buildStructuredPatch(content, replaced),
          metadata: buildEditMetadata({
            filePath: resolved,
            editMode: 'single',
            replaceAll: true,
            userModified: context.userModified,
            gitDiff: buildGitDiff(resolved, 'modified', raCounts.additions, raCounts.removals, diff),
          }),
        };
      }
      return {
        output: `Error: old_string matches ${result.matchCount} locations in ${resolved}. Add more surrounding context to make it unique, or use replace_all: true to replace all ${result.matchCount} occurrences.`,
        isError: true,
        behavior: 'ask',
      };
    }

    const newContent = convertToLineEnding(result.newContent!, originalEnding);
    const strategy = result.strategy!;

    const jsonErr = validateJsonEdit(resolved, rawContent, newContent);
    if (jsonErr) return jsonErr;

    const secrets = scanAddedSecrets(rawContent, newContent);
    if (secrets.length > 0) {
      return { output: secretWarning(secrets, 'content', { teamMemory: isTeamMemoryTarget(resolved, context.cwd) }), isError: true };
    }

    await fileHistoryHooks.track(resolved, rawContent, context.cwd);
    const staleErr = recheckStaleBeforeWrite(resolved, 'editing', context);
    if (staleErr) return staleErr;
    // newContent is immutable here; normalizeToLf is a pure O(file) /\r\n/g replace.
    // Compute the LF form ONCE and reuse it across commitWrite, the diff-cap check,
    // the unified diff, the line count and the structured patch — the old code ran
    // this full-file scan five separate times on the same value per successful edit.
    const newLf = normalizeToLf(newContent);
    const writeErr = commitWrite(resolved, newContent, newLf, context);
    if (writeErr) return { output: writeErr, isError: true };

    const strategyNote = strategy !== 'exact' ? ` (matched via ${strategy} strategy)` : '';

    // Diff-cost cap: skip the O(file) unified + structured diff and the per-line
    // count past the cap, uniformly with the overwrite, replace_all and multi-edit
    // paths (parity: the reference's MAX_SCAN_BYTES diff fallback).
    if (exceedsDiffScanCap(content, newLf)) {
      return {
        output: `[edit: ${path.basename(resolved)}]\nSuccessfully edited ${resolved}${strategyNote}\n\n(diff omitted: file exceeds the ${(EDIT_DIFF_SCAN_MAX_BYTES / (1024 * 1024)).toFixed(0)} MiB diff cap — showing summary only)`,
        structuredPatch: [],
        structuredPatchTruncated: true,
        metadata: buildEditMetadata({ filePath: resolved, editMode: 'single', replaceAll: false, userModified: context.userModified }),
      };
    }

    const diff = buildUnifiedDiff(content, newLf, resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });

    const { additions, removals } = countLinesChanged(content, newLf);
    // Surface the TRUE 1-based line where the edit landed (the first hunk's
    // oldStart), so the model/UI can locate the change in the file without
    // re-reading it — parity with the reference's readEditContext, which reports the
    // edit in its surrounding context at its real line number (lineOffset). The
    // structured patch is computed once and reused for both the location tag and
    // the `structuredPatch` field.
    const patch = buildStructuredPatch(content, newLf);
    const editLine = firstChangedLine(patch);
    const locationNote = editLine !== undefined ? `:${editLine}` : '';
    const patchInfo = `[edit: ${path.basename(resolved)}${locationNote}, +${additions} -${removals}]`;
    return {
      output: `${patchInfo}\nSuccessfully edited ${resolved}${strategyNote}\n\n${diff}`,
      structuredPatch: patch,
      metadata: buildEditMetadata({
        filePath: resolved,
        editMode: 'single',
        replaceAll: false,
        userModified: context.userModified,
      gitDiff: buildGitDiff(resolved, 'modified', additions, removals, diff),
      }),
    };
  },
};

