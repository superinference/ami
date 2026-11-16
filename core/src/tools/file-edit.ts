import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { fuzzyFindAndReplace, findClosestLines } from './fuzzy-match';
import { detectLineEnding, normalizeToLf, convertToLineEnding, resolveFilePath, scanAddedSecrets, secretWarning, detectEncoding, encodeText, decodeBuffer, isBlockedPath, isNonUtfEncoding, validateFilePath, validateFileWritePath, sanitizePathForDisplay, writeFileAtomicSync, getErrnoCode, buildUnifiedDiff, buildStructuredPatch, countLinesChanged, countContentLines, stripTrailingWhitespace, isMarkdownPath, FILE_NOT_FOUND_CWD_NOTE } from './tool-utils';
import { suggestPathUnderCwd } from './path-suggest';
import { findUnknownConfigKeys, validateProjectConfigValues, suggestKnownConfigKey, getManagedConfigPath, KNOWN_CONFIG_KEYS } from '../config';
import { coerceBool } from '../utils/coerce';
import { getFileCache, describeFileChangeMessage } from '../file-cache';
import { trackFileHistory } from './file-history';

const MAX_EDIT_FILE_SIZE = 1_073_741_824; // 1 GiB

/**
 * Byte cap on an edit/overwrite success diff. A large scattered edit can produce
 * a diff bigger than the content it describes; bounding it here keeps the
 * per-turn token cost proportional (parity with Claude Code's snippet cap) while
 * still showing the leading hunks. buildUnifiedDiff truncates at a line boundary
 * and appends a `... [N lines truncated] ...` marker past this size.
 */
const EDIT_DIFF_MAX_BYTES = 16_384;

/**
 * Every parameter the file_edit tool understands, including the accepted aliases
 * for the target path (`path`) and the write-mode content (`content`,
 * `file_text`, `file_content`). Any key outside this set is rejected up front —
 * mirroring Claude Code's strict input schema — so a hallucinated/typo'd
 * parameter (e.g. `filepath`, `newText`) fails loudly instead of being silently
 * dropped and producing a confusing "file not found" / no-op later.
 */
const ALLOWED_EDIT_KEYS = new Set([
  'file_path', 'path',
  'old_string', 'new_string',
  'content', 'file_text', 'file_content',
  'replace_all', 'edits',
]);

/**
 * Enforce the edit size ceiling on EVERY edit mode BEFORE the existing file is
 * read into memory. The single-replace path already stat-guarded, but
 * writeEntireFile and applySequentialEdits read the file unconditionally and
 * could OOM on a multi-GB file. Centralize the check so all three modes share
 * one cap. Returns an error ToolResult when the existing file exceeds the cap;
 * undefined otherwise (including when the file does not exist yet — a brand-new
 * write has nothing to over-read, and downstream read handles real IO errors).
 */
function checkEditSizeLimit(resolved: string): ToolResult | undefined {
  try {
    const stat = fs.statSync(resolved);
    if (stat.size > MAX_EDIT_FILE_SIZE) {
      return { output: `Error: File size (${(stat.size / 1_073_741_824).toFixed(2)} GiB) exceeds 1 GiB limit. Large files cannot be edited.`, isError: true };
    }
  } catch {
    // File does not exist yet (or cannot be stat'd) — the size cap does not
    // apply; the subsequent read surfaces any genuine existence/IO error.
  }
  return undefined;
}

function isJsonConfigPath(resolved: string): boolean {
  return resolved.endsWith('config.json') || resolved.endsWith('config.local.json') ||
    resolved.endsWith('settings.json') ||
    resolved.endsWith('tsconfig.json') || resolved.endsWith('package.json');
}

/**
 * True when `resolved` is an AMI settings/config file governed by the
 * {@link ProjectConfig} schema — a `config.json` / `config.local.json` under an
 * `.ami` directory, or the managed/system config path. These files drive
 * security-governing behavior (permission mode, hooks, env, spend caps), so an
 * edit must keep them schema-valid, not merely JSON-parseable. tsconfig.json /
 * package.json match {@link isJsonConfigPath} for the syntax check but are NOT
 * AMI settings, so they are excluded here.
 */
function isAmiSettingsConfigPath(resolved: string): boolean {
  const base = path.basename(resolved);
  if (base !== 'config.json' && base !== 'config.local.json') return false;
  if (path.basename(path.dirname(resolved)) === '.ami') return true;
  try {
    if (path.resolve(resolved) === path.resolve(getManagedConfigPath())) return true;
  } catch { /* managed path not resolvable — fall through */ }
  return false;
}

/**
 * Schema-validate an edit to an AMI settings file (parity with Claude Code's
 * `validateInputForSettingsFileEdit`): a JSON-syntactically-valid edit can still
 * corrupt the security-governing config by introducing a wrong-typed value, an
 * out-of-enum value, or an unrecognized (typo'd) key. Simulate the post-edit
 * content and, ONLY when the file was schema-valid before the edit, block an edit
 * that would make it schema-invalid — reusing the ONE canonical config validator
 * ({@link validateProjectConfigValues} / {@link findUnknownConfigKeys}), never a
 * parallel schema. A file that was already schema-invalid (or not an object) is
 * left editable so a repair-in-progress is never blocked. Returns an error
 * ToolResult when a previously-valid settings file would become schema-invalid;
 * undefined otherwise.
 */
function validateAmiSettingsSchema(resolved: string, originalContent: string, parsedNew: unknown): ToolResult | undefined {
  if (!isAmiSettingsConfigPath(resolved)) return undefined;
  if (parsedNew === null || typeof parsedNew !== 'object' || Array.isArray(parsedNew)) return undefined;

  let parsedOld: unknown;
  try { parsedOld = JSON.parse(originalContent); } catch { return undefined; }
  const oldWasValidObject = parsedOld !== null && typeof parsedOld === 'object' && !Array.isArray(parsedOld);
  // Only guard a valid→invalid transition: an already-broken baseline is a repair.
  if (!oldWasValidObject) return undefined;
  const oldConfig = parsedOld as Parameters<typeof validateProjectConfigValues>[0];
  if (validateProjectConfigValues(oldConfig).length > 0 || findUnknownConfigKeys(oldConfig).length > 0) {
    return undefined;
  }

  const newConfig = parsedNew as Parameters<typeof validateProjectConfigValues>[0];
  const valueIssues = validateProjectConfigValues(newConfig);
  const unknownKeys = findUnknownConfigKeys(newConfig);
  if (valueIssues.length === 0 && unknownKeys.length === 0) return undefined;

  const lines = [
    `Warning: Edit would make ${path.basename(resolved)} violate the settings schema (it was valid before this edit). Fix the issues below:`,
  ];
  for (const issue of valueIssues) {
    lines.push(`  - ${issue.field}: ${issue.message}`);
  }
  for (const key of unknownKeys) {
    const suggestion = suggestKnownConfigKey(key);
    lines.push(`  - unknown key "${key}"${suggestion ? ` — did you mean "${suggestion}"?` : ''}`);
  }
  // env-protection hint: the env block feeds process environment and is shared
  // with collaborators; warn against touching it unless explicitly instructed.
  if (Object.prototype.hasOwnProperty.call(newConfig, 'env')) {
    lines.push('  Note: do not modify the "env" block unless explicitly instructed — it affects process environment and is shared with collaborators.');
  }
  lines.push(`  Recognized settings keys: ${[...KNOWN_CONFIG_KEYS].sort().join(', ')}`);
  return { output: lines.join('\n'), isError: true };
}

/**
 * Config/settings files (package.json, tsconfig.json, .claude/.ami settings, …)
 * must stay parseable, and this must hold across EVERY edit mode (single
 * replace, replace_all, multi-edit, and full overwrite), not only the single
 * exact-replace path. But only block a valid→invalid transition: if the file was
 * ALREADY invalid (or brand-new / empty), an edit that leaves it invalid is a
 * repair-in-progress and is allowed — otherwise a broken config could never be
 * fixed with the edit tool. Returns an error ToolResult when a previously-valid
 * config would become invalid JSON; undefined otherwise.
 */
function validateJsonEdit(resolved: string, originalContent: string, newContent: string): ToolResult | undefined {
  if (!isJsonConfigPath(resolved)) return undefined;
  try {
    JSON.parse(originalContent);
  } catch {
    // Original was not valid JSON (already broken, empty, or a new file) — allow
    // the edit so the config can be repaired incrementally.
    return undefined;
  }
  let parsedNew: unknown;
  try {
    parsedNew = JSON.parse(newContent);
  } catch (e) {
    return { output: `Warning: Edit would create invalid JSON in ${path.basename(resolved)}. Check syntax.\n${(e as Error).message}`, isError: true };
  }
  // Beyond JSON syntax, an AMI settings file must stay schema-valid.
  return validateAmiSettingsSchema(resolved, originalContent, parsedNew);
}

interface SequentialEdit {
  old_string: string;
  new_string: string;
  replace_all?: boolean;
}

/**
 * Indirection seam for the LSP client lookup. Production loads the real client
 * lazily via require; tests swap it to exercise the notification error handling
 * (client lookup failure, rejected notify promises) deterministically. Behavior
 * is unchanged in production.
 */
export const lspHooks: { getClient: () => any } = {
  getClient: () => require('../lsp').getLSPClient(),
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
}

export function verifyWriteTarget(resolved: string, cwd: string): string | undefined {
  const cwdAbs = path.resolve(cwd);

  try {
    const stat = fs.lstatSync(resolved);
    if (stat.isSymbolicLink()) {
      let target: string;
      try {
        target = fs.realpathSync(resolved);
      } catch (realErr) {
        // realpath threw ENOENT but lstat says this entry IS a symlink → it is a
        // DANGLING symlink (its target does not exist yet). An attacker can pre-place
        // such a link (e.g. x -> ~/.ssh/authorized_keys) so a later write creates and
        // populates the target OUTSIDE the workspace. Resolve the link's DECLARED
        // destination and block it when it escapes, instead of silently falling
        // through to the "nonexistent, nothing to escape" path.
        if (getErrnoCode(realErr) === 'ENOENT') {
          const linkDest = path.resolve(path.dirname(resolved), fs.readlinkSync(resolved));
          if (!linkDest.startsWith(cwdAbs + path.sep) && linkDest !== cwdAbs) {
            return `Error: "${resolved}" is a dangling symlink pointing to "${linkDest}" outside the workspace — write blocked (symlink-escape).`;
          }
          // Dangling but points back inside the workspace — harmless; fall through to
          // the parent-directory checks below.
          target = linkDest;
        } else {
          throw realErr;
        }
      }
      if (!target.startsWith(cwdAbs + path.sep) && target !== cwdAbs) {
        return `Error: "${resolved}" is a symlink resolving to "${target}" outside the workspace — write blocked.`;
      }
    }
  } catch (err) {
    // A thrown error from the dangling-symlink branch above must propagate as a block,
    // not be swallowed as "nonexistent". Only a genuinely-absent path (lstat ENOENT)
    // means there is no symlink to escape through.
    if (getErrnoCode(err) !== 'ENOENT') {
      return `Error: cannot verify write target "${resolved}": ${err instanceof Error ? err.message : String(err)}`;
    }
  }

  // Check parent directories for symlinks escaping the workspace
  let dir = path.dirname(path.resolve(resolved));
  while (dir.startsWith(cwdAbs + path.sep) || dir === cwdAbs) {
    if (dir === cwdAbs) break;
    try {
      const dirStat = fs.lstatSync(dir);
      if (dirStat.isSymbolicLink()) {
        const realDir = fs.realpathSync(dir);
        if (!realDir.startsWith(cwdAbs + path.sep) && realDir !== cwdAbs) {
          return `Error: parent directory "${dir}" is a symlink resolving to "${realDir}" outside the workspace — write blocked.`;
        }
      }
    } catch {
      // Directory doesn't exist yet — will be created by mkdirSync. (A non-ENOENT
      // problem in an ancestor makes the write-target lstat above throw first,
      // where it already fails closed, so this path only sees genuine absence.)
    }
    dir = path.dirname(dir);
  }

  return undefined;
}

export function commitWrite(
  resolved: string,
  rawToWrite: string,
  cacheContent: string,
  context: ToolContext,
): string | undefined {
  try {
    fs.mkdirSync(path.dirname(resolved), { recursive: true });
    const symlinkErr = verifyWriteTarget(resolved, context.cwd);
    if (symlinkErr) return symlinkErr;
    let enc: ReturnType<typeof detectEncoding> = 'utf-8';
    try { if (fs.existsSync(resolved)) enc = detectEncoding(fs.readFileSync(resolved).subarray(0, 4)); } catch { /* default utf-8 */ }
    writeFileAtomicSync(resolved, encodeText(rawToWrite, enc));
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return `Error writing file: ${message}`;
  }
  getFileCache(context.cwd, context.agentId).setWritten(resolved, cacheContent);
  context.filesRead?.add(resolved);
  notifyLsp(resolved, rawToWrite, context);
  return undefined;
}

export async function writeEntireFile(
  resolved: string,
  content: string,
  context: ToolContext,
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
    const rawBuf = await fs.promises.readFile(resolved);
    if (isNonUtfEncoding(rawBuf)) {
      readErrResult = {
        output: `Error: "${resolved}" appears to use a non-UTF encoding (Shift-JIS, GBK, Windows-1252, or similar). Editing it as UTF-8 would silently corrupt the file. Use a dedicated encoding-aware editor instead.`,
        isError: true,
      };
    } else {
      oldContent = decodeBuffer(rawBuf).text;
      fileExists = true;
    }
  } catch (err) {
    // Only a genuinely-absent file (ENOENT) means "new file, nothing to lose".
    // Any OTHER errno (EACCES/EIO/ELOOP/ENOTDIR…) means the file may exist but
    // could not be read — silently treating it as new and overwriting would
    // clobber unreadable content, so surface an actionable error instead.
    const code = getErrnoCode(err);
    if (code && code !== 'ENOENT') {
      readErrResult = {
        output: `Error: cannot read existing file "${resolved}" to prepare the overwrite (${code}): ${err instanceof Error ? err.message : String(err)}. Resolve the access/IO problem before writing.`,
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
    const change = fileCache.describeChange(resolved);
    if (change.changed) {
      fileCache.delete(resolved);
      return { output: `Error: ${describeFileChangeMessage(change.reason, 'overwriting')}`, isError: true };
    }
  }

  const secrets = scanAddedSecrets(oldContent, content);
  if (secrets.length > 0) {
    return { output: secretWarning(secrets, 'content'), isError: true };
  }

  const jsonErr = validateJsonEdit(resolved, oldContent, content);
  if (jsonErr) return jsonErr;

  // A full-content write is a deliberate replacement: honor the line endings the
  // model sent in `content` instead of re-converting to the old file's EOL.
  // Re-converting silently corrupted files whose intended EOL differs from the
  // old one (e.g. rewriting a CRLF file to an LF shell script) — parity with
  // Claude Code's FileWriteTool, which removed that preservation for this reason.
  const finalContent = content;

  if (fileExists && oldContent.length > 0) {
    await trackFileHistory(resolved, oldContent, context.cwd);
  }

  const writeErr = commitWrite(resolved, finalContent, content, context);
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
    return {
      output: `${createdLabel}: ${resolved}\nSuccessfully wrote ${resolved} (new file, ${lineCount} lines)\n\n--- /dev/null\n+++ ${resolved}\n@@ -0,0 +1,${lineCount} @@\n${body}${truncNote}`,
      structuredPatch: buildStructuredPatch(normalizeToLf(oldContent), normalizeToLf(content)),
    };
  }

  // Minimal structured diff: emit only the changed hunks (+ surrounding context)
  // via the common-prefix/suffix differ, instead of marking the entire old file
  // as removed and the entire new file as added. For a small edit inside a large
  // file this keeps the overwrite diff proportional to what actually changed.
  const diff = buildUnifiedDiff(normalizeToLf(oldContent), normalizeToLf(content), resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });
  return {
    output: `Successfully wrote ${resolved} (${lineCount} lines)\n\n${diff}`,
    structuredPatch: buildStructuredPatch(normalizeToLf(oldContent), normalizeToLf(content)),
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

  if (context.filesRead && !context.filesRead.has(resolved)) {
    context.filesRead.add(resolved);
    getFileCache(context.cwd, context.agentId).setWritten(resolved, rawContent);
  }

  const fileCache = getFileCache(context.cwd, context.agentId);
  const freshness = fileCache.describeChange(resolved);
  if (freshness.changed) {
    fileCache.delete(resolved);
    return { output: `Error: ${describeFileChangeMessage(freshness.reason, 'editing')}`, isError: true };
  }

  const originalEnding = detectLineEnding(rawContent);
  let content = normalizeToLf(rawContent);
  // Markdown (.md/.mdx) uses two trailing spaces as a hard line break, so the
  // trailing-whitespace normalization of the replacement text is skipped there.
  const stripWs = !isMarkdownPath(resolved);

  const applied: string[] = [];
  const failed: string[] = [];
  // Track the new_strings of edits already applied in this batch. A later edit
  // whose old_string is a substring of an earlier edit's new_string is almost
  // always a model mistake (it would re-edit text the previous edit just wrote,
  // not the original file), so reject it — mirroring Claude Code's sequential-edit
  // guard (FileEditTool/utils.ts). Trailing newlines are stripped before the
  // containment test, matching the reference.
  const appliedNewStrings: string[] = [];

  for (let i = 0; i < edits.length; i++) {
    const { old_string, new_string, replace_all: replaceAll } = edits[i];

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
    return {
      output: `No edits applied.\n${failed.join('\n')}`,
      isError: true,
    };
  }

  const secrets = scanAddedSecrets(rawContent, content);
  if (secrets.length > 0) {
    return { output: secretWarning(secrets, 'edited content'), isError: true };
  }

  const jsonErr = validateJsonEdit(resolved, rawContent, content);
  if (jsonErr) return jsonErr;

  const finalContent = convertToLineEnding(content, originalEnding);
  await trackFileHistory(resolved, rawContent, context.cwd);
  const writeErr = commitWrite(resolved, finalContent, content, context);
  if (writeErr) return { output: writeErr, isError: true };

  const { additions, removals } = countLinesChanged(normalizeToLf(rawContent), content);
  const summary = [
    `Successfully edited ${resolved}`,
    `${applied.length}/${edits.length} edits applied (+${additions} -${removals}).`,
  ];
  if (failed.length > 0) {
    summary.push(`\nFailed edits:\n${failed.join('\n')}`);
  }

  // Append a unified diff so the TUI diff view renders the multi-edit result,
  // just like the single-replace and full-write modes do.
  const diff = buildUnifiedDiff(normalizeToLf(rawContent), content, resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });
  if (diff !== '(no changes detected)') {
    summary.push(`\n${diff}`);
  }

  return {
    output: summary.join('\n'),
    isError: false,
    structuredPatch: buildStructuredPatch(normalizeToLf(rawContent), content),
  };
}

export const fileEditTool: ToolDefinition = {
  name: 'file_edit',
  // Discovery hint for tool_search: synonyms a model might phrase the intent with
  // that are not in the formal name/description (parity: Claude's Tool.searchHint
  // 'modify file contents in place'). Consumed by tool-search.ts scoring.
  searchHint: 'modify file contents in place create overwrite write edit replace rewrite patch',
  description:
    'Create or edit a file. You MUST file_read an existing file before changing it. ALWAYS prefer editing an existing file (one of the replace modes, which send only the diff) over creating a new one; use the Write/create mode only to create a genuinely new file or for a complete rewrite. NEVER proactively create documentation files (*.md) or README files unless the User explicitly requests them. Only write emojis into a file if the User explicitly asks for them. Modes: (1) Replace — file_path + old_string + new_string; old_string must match exactly (use the smallest unique 2–4 line span). Set replace_all to replace every occurrence. (2) Multiple replaces — file_path + edits: [{old_string, new_string, replace_all?}]; applied in order. (3) Write/create — file_path + new_string (or content) with old_string omitted or empty: creates the file or replaces its entire contents. Do not include line numbers. Never use bash sed/awk to edit files.',
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
  isReadOnly: false,

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
    // Reject with an actionable, self-correcting message (parity with Claude
    // Code's write-path glob guard). Only the unambiguous wildcards `*`/`?` are
    // flagged so legitimate bracket/brace filenames (e.g. Next.js `[id].tsx`,
    // `{a,b}.ts`) stay editable — an intentional superset divergence from
    // Claude's broader `[*?[]{}]` set to avoid breaking real filenames.
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
    // <home>/notes.txt — a bypass. Parity: Claude Code's expandPath-before-write.
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

    const { resolved, error: pathError } = resolveFilePath(filePath, context.cwd, context.allowedReadRoots);
    if (pathError) return pathError;

    if (isBlockedPath(resolved)) {
      return { output: `Error: Cannot edit device or special file: ${resolved}`, isError: true };
    }

    // Symlink guard: block writes to symlinks to prevent redirect attacks
    const writePathCheck = validateFileWritePath(resolved);
    if (!writePathCheck.valid) {
      return { output: writePathCheck.error!, isError: true };
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
      return writeEntireFile(resolved, stripWs ? stripTrailingWhitespace(newString) : newString, context);
    }

    if (newString === undefined || newString === null) {
      return { output: 'Error: new_string must be provided.', isError: true };
    }

    if (oldString === newString) {
      return {
        output: 'Error: old_string and new_string are identical. No changes needed.',
        isError: true,
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
      if (getErrnoCode(err) === 'ENOENT') {
        const dir = path.dirname(resolved);
        const base = path.basename(resolved);
        let suggestions = '';
        try {
          const files = fs.readdirSync(dir).filter(f => f.includes(base.slice(0, 3)) || base.includes(f.slice(0, 3)));
          if (files.length > 0) suggestions = `\nDid you mean: ${files.slice(0, 5).join(', ')}?`;
        } catch {}
        // "Dropped repo folder" correction: when the model built an absolute path
        // missing the repo-directory segment, point it at the real path under cwd.
        const underCwd = suggestPathUnderCwd(resolved, context.cwd);
        if (underCwd) suggestions += `\nDid you mean "${underCwd}" (under the working directory)?`;
        // Working-directory note so the model can resolve relative-vs-absolute
        // path confusion from the boundary context it was operating in.
        return { output: `Error: File not found: ${resolved}${suggestions}\n${FILE_NOT_FOUND_CWD_NOTE} ${context.cwd}`, isError: true };
      }
      const message = err instanceof Error ? err.message : String(err);
      return {
        output: `Error reading file: ${message}`,
        isError: true,
      };
    }

    if (context.filesRead && !context.filesRead.has(resolved)) {
      context.filesRead.add(resolved);
      getFileCache(context.cwd, context.agentId).setWritten(resolved, rawContent);
    }

    const fileCache = getFileCache(context.cwd, context.agentId);
    const freshness = fileCache.describeChange(resolved);
    if (freshness.changed) {
      fileCache.delete(resolved);
      return { output: `Error: ${describeFileChangeMessage(freshness.reason, 'editing')}`, isError: true };
    }

    const originalEnding = detectLineEnding(rawContent);
    const content = normalizeToLf(rawContent);
    const normalizedOld = normalizeToLf(oldString);
    const normalizedNew = normalizeToLf(stripWs ? stripTrailingWhitespace(newString) : newString);

    const result = fuzzyFindAndReplace(content, normalizedOld, normalizedNew);

    if (result.error) {
      if (result.matchCount === 0) {
        fileCache.delete(resolved);

        const searchLines = oldString.split('\n');
        const hints = findClosestLines(content, searchLines, 3);
        let hintText = '';
        if (hints.length > 0) {
          hintText = '\n\nDid you mean one of these lines?\n' +
            hints.map(h => `  Line ${h.lineNumber}: ${h.line}`).join('\n');
        }
        const lines = content.split('\n');
        const preview = lines.slice(0, 30).map((l, i) => `${i + 1}: ${l}`).join('\n');
        const truncNote = lines.length > 30 ? `\n... (${lines.length - 30} more lines)` : '';
        return {
          output: `Error: old_string not found in ${resolved}. The string must match the file content EXACTLY. Re-read the file with file_read before retrying.${hintText}\n\nCurrent file content (first 30 lines):\n${preview}${truncNote}`,
          isError: true,
        };
      }
      if (replaceAll) {
        const replaced = content.split(normalizedOld).join(normalizedNew);
        const addedSecrets = scanAddedSecrets(content, replaced);
        if (addedSecrets.length > 0) {
          return { output: secretWarning(addedSecrets, 'edited content'), isError: true };
        }
        const jsonErr = validateJsonEdit(resolved, rawContent, replaced);
        if (jsonErr) return jsonErr;
        const finalContent = convertToLineEnding(replaced, originalEnding);
        await trackFileHistory(resolved, rawContent, context.cwd);
        const writeErr = commitWrite(resolved, finalContent, normalizeToLf(finalContent), context);
        if (writeErr) return { output: writeErr, isError: true };
        const diff = buildUnifiedDiff(content, replaced, resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });
        return {
          output: `Successfully replaced ${result.matchCount} occurrences in ${resolved}\n\n${diff}`,
          structuredPatch: buildStructuredPatch(content, replaced),
        };
      }
      return {
        output: `Error: old_string matches ${result.matchCount} locations in ${resolved}. Add more surrounding context to make it unique, or use replace_all: true to replace all ${result.matchCount} occurrences.`,
        isError: true,
      };
    }

    const newContent = convertToLineEnding(result.newContent!, originalEnding);
    const strategy = result.strategy!;

    const jsonErr = validateJsonEdit(resolved, rawContent, newContent);
    if (jsonErr) return jsonErr;

    const secrets = scanAddedSecrets(rawContent, newContent);
    if (secrets.length > 0) {
      return { output: secretWarning(secrets, 'content'), isError: true };
    }

    await trackFileHistory(resolved, rawContent, context.cwd);
    const writeErr = commitWrite(resolved, newContent, normalizeToLf(newContent), context);
    if (writeErr) return { output: writeErr, isError: true };

    const diff = buildUnifiedDiff(content, normalizeToLf(newContent), resolved, { maxBytes: EDIT_DIFF_MAX_BYTES });

    const strategyNote = strategy !== 'exact' ? ` (matched via ${strategy} strategy)` : '';
    const { additions, removals } = countLinesChanged(content, normalizeToLf(newContent));
    const patchInfo = `[edit: ${path.basename(resolved)}, +${additions} -${removals}]`;
    return {
      output: `${patchInfo}\nSuccessfully edited ${resolved}${strategyNote}\n\n${diff}`,
      structuredPatch: buildStructuredPatch(content, normalizeToLf(newContent)),
    };
  },
};

