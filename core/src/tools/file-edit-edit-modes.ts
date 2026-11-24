// Split from file-edit.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { ToolContext, ToolResult, DiffHunk, GitDiffSummary } from '../types';
import { normalizeToLf, boundedChangeSnippet } from './tool-utils';
import { findUnknownConfigKeys, validateProjectConfigValues, suggestKnownConfigKey, getManagedConfigPath, KNOWN_CONFIG_KEYS } from '../config';
import { filterInvalidPermissionRules } from '../permissions';
import { getFileCache, describeFileChangeMessage } from '../file-cache';
import type { FileChangeReason } from '../file-cache';
import { MemoryManager } from '../memory';

/**
 * True when `resolved` lives under the shared TEAM-memory tree for `cwd`. A write
 * there is replicated to every repository collaborator, so a secret in it is
 * strictly worse than one committed to this checkout — the secret guard escalates
 * to a collaborator-aware block for such paths (parity: the reference's
 * teamMemSecretGuard `isTeamMemPath` scoping). Reuses the ONE canonical
 * team-memory classifier ({@link MemoryManager.isTeamMemFile}) rather than a
 * second path check; best-effort — any classifier failure falls back to the
 * generic (non-team) warning so a secret is never under-reported.
 */
export function isTeamMemoryTarget(resolved: string, cwd: string): boolean {
  try {
    return new MemoryManager(cwd).isTeamMemFile(resolved);
  } catch {
    return false;
  }
}

export const MAX_EDIT_FILE_SIZE = 1_073_741_824; // 1 GiB

/**
 * Byte cap on an edit/overwrite success diff. A large scattered edit can produce
 * a diff bigger than the content it describes; bounding it here keeps the
 * per-turn token cost proportional (parity with the reference's snippet cap) while
 * still showing the leading hunks. buildUnifiedDiff truncates at a line boundary
 * and appends a `... [N lines truncated] ...` marker past this size.
 */
export const EDIT_DIFF_MAX_BYTES = 16_384;

/**
 * Compose the stale-file error message, surfacing a BOUNDED, line-numbered
 * snippet of WHAT changed on disk since the model last read the file — not merely
 * THAT it changed. `before` is the content the model last read (from the warm
 * {@link FileCache} entry); `after` is the current on-disk content already in hand
 * at the call site. This is AMI's analogue of the reference's `edited_text_file`
 * attachment ({@link boundedChangeSnippet} → getSnippetForTwoFileDiff): a
 * format-on-save / linter rewrite is shown with its surrounding context so the
 * model can decide what to re-read, instead of a bare "read it again". Degrades to
 * the plain {@link describeFileChangeMessage} when no cached content is available
 * (undefined `before`) or the change produced no surviving snippet (a deletion, or
 * identical content). The caller sets `isError`/`behavior`.
 */
export function staleFileChangeOutput(reason: FileChangeReason, verb: string, before: string | undefined, after: string): string {
  let output = `Error: ${describeFileChangeMessage(reason, verb)}`;
  const snippet = before !== undefined
    ? boundedChangeSnippet(normalizeToLf(before), normalizeToLf(after))
    : '';
  if (snippet) {
    output += `\n\nWhat changed on disk (current content around the change, line-numbered):\n${snippet}`;
  }
  return output;
}

/**
 * Final synchronous staleness guard, run IMMEDIATELY before {@link commitWrite}
 * with no `await` between it and the write. The initial freshness check happens
 * earlier, but the pre-write {@link trackFileHistory} backup is asynchronous: its
 * `await` yields the event loop, during which a concurrent process can modify the
 * target. Without this re-check the subsequent write would silently clobber that
 * concurrent change. Re-checking here — synchronously, right before the write —
 * keeps the staleness-confirmation→write span atomic (parity with the reference's
 * FileWriteTool/FileEditTool, which keep "no async operations between the
 * staleness check and writing to disk"). Returns an error ToolResult when the
 * file changed under us (and drops the now-stale cache entry); undefined when it
 * is safe to proceed.
 */
export function recheckStaleBeforeWrite(resolved: string, verb: string, context: ToolContext): ToolResult | undefined {
  const fileCache = getFileCache(context.cwd, context.agentId);
  const before = fileCache.peekContent(resolved);
  const change = fileCache.describeChange(resolved);
  if (change.changed) {
    fileCache.delete(resolved);
    // Re-read the current on-disk bytes (best-effort) so the error can show the
    // concurrent modification that landed during the async history-backup await.
    let after = '';
    try { after = fs.readFileSync(resolved, 'utf-8'); } catch { /* deleted/unreadable — plain message */ }
    return { output: staleFileChangeOutput(change.reason, verb, before, after), isError: true };
  }
  return undefined;
}

/**
 * Hard read-before-edit guard for the string-replace and multi-edit paths (parity:
 * the reference's FileEditTool `validateInput` errorCode 6 — "File has not been read
 * yet. Read it first before writing to it."). A targeted edit against an existing
 * file the model never read still risks matching/replacing a span it cannot see, so
 * the reference blocks it outright instead of proceeding. AMI previously AUTO-READ on this
 * path (seeding {@link ToolContext.filesRead} itself), a leniency that let a blind
 * edit through and silently contradicted the tool's own "You MUST file_read an
 * existing file before changing it" contract. This replaces that auto-read with the
 * reference's hard block so the contract is actually enforced, uniformly with the
 * overwrite path's read-before-overwrite guard. A PARTIAL (size-capped) prior read
 * is rejected too — the model has not seen the whole file, so an edit could still
 * clobber an unseen region. Only enforced when filesRead tracking is active;
 * undefined tracking (non-tracked contexts) stays lenient, matching the overwrite
 * path. Returns an error ToolResult when the read requirement is unmet; undefined
 * when it is safe to proceed.
 */
export function requireReadBeforeEdit(resolved: string, context: ToolContext): ToolResult | undefined {
  if (!context.filesRead) return undefined;
  if (!context.filesRead.has(resolved)) {
    return {
      output: `Error: You must file_read "${resolved}" before editing it, to avoid replacing content you have not seen. Read the file first, then edit.`,
      isError: true,
      behavior: 'ask',
    };
  }
  if (getFileCache(context.cwd, context.agentId).isPartialView(resolved)) {
    return {
      output: `Error: You have only read a PARTIAL view of "${resolved}" (the file was size-capped on read). Read the full file (file_read with allow_large:true, or offset/limit over the whole file) before editing it, to avoid clobbering content you have not seen.`,
      isError: true,
      behavior: 'ask',
    };
  }
  return undefined;
}

/**
 * Enforce the edit size ceiling on EVERY edit mode BEFORE the existing file is
 * read into memory. The single-replace path already stat-guarded, but
 * writeEntireFile and applySequentialEdits read the file unconditionally and
 * could OOM on a multi-GB file. Centralize the check so all three modes share
 * one cap. Returns an error ToolResult when the existing file exceeds the cap;
 * undefined otherwise (including when the file does not exist yet — a brand-new
 * write has nothing to over-read, and downstream read handles real IO errors).
 */
export function checkEditSizeLimit(resolved: string): ToolResult | undefined {
  try {
    const stat = fs.statSync(resolved);
    if (stat.size > MAX_EDIT_FILE_SIZE) {
      return { output: `Error: File size (${(stat.size / 1_073_741_824).toFixed(2)} GiB) exceeds 1 GiB limit. Large files cannot be edited.`, isError: true, behavior: 'ask' };
    }
  } catch {
    // File does not exist yet (or cannot be stat'd) — the size cap does not
    // apply; the subsequent read surfaces any genuine existence/IO error.
  }
  return undefined;
}

export function isJsonConfigPath(resolved: string): boolean {
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
export function isAmiSettingsConfigPath(resolved: string): boolean {
  const base = path.basename(resolved);
  if (base !== 'config.json' && base !== 'config.local.json') return false;
  if (path.basename(path.dirname(resolved)) === '.ami') return true;
  try {
    if (path.resolve(resolved) === path.resolve(getManagedConfigPath())) return true;
  } catch { /* managed path not resolvable — fall through */ }
  return false;
}

/**
 * Schema-validate an edit to an AMI settings file (parity with the reference's
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
export function validateAmiSettingsSchema(resolved: string, originalContent: string, parsedNew: unknown): ToolResult | undefined {
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
 * True when `resolved` is a Claude-Code-compatible permission settings file
 * (`settings.json` / `settings.local.json`) — the files that carry a
 * `permissions: { allow, deny, ask }` rule block. A syntactically-valid edit can
 * still corrupt the security surface by introducing a malformed permission rule
 * (empty rule, mismatched parentheses, `:*` in the wrong place, a URL-form
 * web_fetch pattern, …), so an edit to one of these must keep the rules valid.
 */
export function isPermissionSettingsPath(resolved: string): boolean {
  const base = path.basename(resolved);
  return base === 'settings.json' || base === 'settings.local.json';
}

/**
 * Collect the human-readable warnings for every INVALID permission rule found in
 * a parsed settings object's `permissions.{allow,deny,ask}` arrays, reusing the
 * ONE canonical rule validator ({@link filterInvalidPermissionRules} →
 * {@link validatePermissionRule}) rather than a parallel check. A non-object
 * input, a missing/non-object `permissions` block, or a non-array rule list
 * yields no warnings (nothing to validate).
 */
export function collectPermissionRuleWarnings(parsed: unknown): string[] {
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return [];
  const perms = (parsed as Record<string, unknown>).permissions;
  if (perms === null || typeof perms !== 'object' || Array.isArray(perms)) return [];
  const warnings: string[] = [];
  for (const key of ['allow', 'deny', 'ask'] as const) {
    const rules = (perms as Record<string, unknown>)[key];
    if (rules === undefined) continue;
    warnings.push(...filterInvalidPermissionRules(rules, `permissions.${key}`).warnings);
  }
  return warnings;
}

/**
 * Schema-validate the PERMISSION RULES of an edit to a Claude-compatible settings
 * file (parity with the reference's `validateInputForSettingsFileEdit`, which
 * validates the whole settings schema — including permission-rule validity —
 * after an edit). A JSON-syntactically-valid edit can still introduce a malformed
 * rule that would silently never match (a security hole). Block ONLY a
 * valid→invalid transition: when the file's permission rules were all valid before
 * the edit but the post-edit content introduces an invalid one. A file that
 * already contained an invalid rule (or no permissions block) is left editable so
 * a repair-in-progress is never blocked. Returns an error ToolResult naming each
 * newly-introduced invalid rule; undefined otherwise.
 */
export function validatePermissionSettingsEdit(resolved: string, originalContent: string, parsedNew: unknown): ToolResult | undefined {
  if (!isPermissionSettingsPath(resolved)) return undefined;
  const newWarnings = collectPermissionRuleWarnings(parsedNew);
  if (newWarnings.length === 0) return undefined;

  let parsedOld: unknown;
  try { parsedOld = JSON.parse(originalContent); } catch { return undefined; }
  // Only guard a valid→invalid transition: an already-broken rule set is a repair.
  if (collectPermissionRuleWarnings(parsedOld).length > 0) return undefined;

  const lines = [
    `Warning: Edit would introduce invalid permission rule(s) in ${path.basename(resolved)} (its permission rules were all valid before this edit). Fix the issues below:`,
    ...newWarnings.map(w => `  - ${w}`),
  ];
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
export function validateJsonEdit(resolved: string, originalContent: string, newContent: string): ToolResult | undefined {
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
  const schemaErr = validateAmiSettingsSchema(resolved, originalContent, parsedNew);
  if (schemaErr) return schemaErr;
  // And a Claude-compatible settings file must keep its permission rules valid.
  return validatePermissionSettingsEdit(resolved, originalContent, parsedNew);
}

/**
 * The TRUE 1-based line in the OLD file where the first change of `hunks` lands.
 * A structured-patch hunk's {@link DiffHunk.oldStart} is the hunk's FIRST line —
 * which includes the leading context lines — not the first actually-changed line.
 * Walk the first hunk's leading context (lines prefixed `' '`) to the first
 * removed/added line (`'-'`/`'+'`), advancing the old-side counter past each
 * context line, so the reported location points at the edit itself (parity:
 * the reference's readEditContext lineOffset, which locates the edit, not the window
 * start). Returns undefined when there is no change.
 */
export function firstChangedLine(hunks: DiffHunk[]): number | undefined {
  if (hunks.length === 0) return undefined;
  const h = hunks[0];
  let line = h.oldStart;
  for (const l of h.lines) {
    if (l.startsWith('-') || l.startsWith('+')) return line;
    line++; // a context line advances the old-side line counter
  }
  return h.oldStart;
}

export interface SequentialEdit {
  old_string: string;
  new_string: string;
  replace_all?: boolean;
}

/**
 * Single-edit vs edits[]-batch discriminator attached to a successful edit result
 * so a diff/permission UI can branch on how many edits it is presenting (parity:
 * the reference's IDEDiffConfig `editMode`, stamped `'single'` by
 * createSingleEditDiffConfig and `'multiple'` for an `edits[]` batch).
 */
export const EDIT_MODES = ['single', 'multiple'] as const;

export type EditMode = (typeof EDIT_MODES)[number];

/**
 * Build the structured result metadata every SUCCESSFUL file_edit emits (parity:
 * the reference's FileEditTool `outputSchema`), carried on {@link ToolResult.metadata}
 * so a programmatic caller reads the edit's shape/magnitude without re-parsing the
 * human `output`. `userModified` reflects {@link ToolContext.userModified} — true
 * when the user EDITED this tool call's input at the permission prompt (AMI's
 * approve-with-edited-input / `updatedInput` accept-with-edits flow) before
 * approving, so the applied bytes diverge from the model's original proposal. This
 * is AMI's analogue of the reference's diff-editor `computeEditsFromContents`
 * (hooks/useDiffInIDE.ts): where the reference re-derives the FileEdit[] from the user's
 * hand-modified file content, AMI rewrites the edit INPUT the user approved and
 * records here that what landed was the human-corrected call, not the proposal.
 * Threaded via the ONE canonical permission-layer `userModified` signal — no
 * parallel accept-with-edits path. `gitDiff` is attached only when a per-line diff
 * was computed (omitted on the diff-cost-capped paths).
 */
export function buildEditMetadata(opts: {
  filePath: string;
  editMode: EditMode;
  replaceAll: boolean;
  userModified?: boolean;
  gitDiff?: GitDiffSummary;
}): Record<string, unknown> {
  const meta: Record<string, unknown> = {
    filePath: opts.filePath,
    editMode: opts.editMode,
    replaceAll: opts.replaceAll,
    userModified: opts.userModified === true,
  };
  if (opts.gitDiff) meta.gitDiff = opts.gitDiff;
  return meta;
}

/** Assemble a {@link GitDiffSummary} from counts + the already-computed patch text. */
export function buildGitDiff(
  filename: string,
  status: GitDiffSummary['status'],
  additions: number,
  deletions: number,
  patch: string,
): GitDiffSummary {
  return { filename, status, additions, deletions, changes: additions + deletions, patch };
}
