// Split from attribution-store.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { realpathSync } from 'fs';
import { isAbsolute, relative, sep } from 'path';

/**
 * In-process, session-scoped store of per-file AGENT attribution. It accumulates
 * how many characters the agent authored in each file it edits/creates during a
 * session, so a later `git_commit` can surface a per-file contribution figure in
 * its attribution introspection note. This is the SOURCE wiring for the existing
 * (previously unconsumed) {@link FileAttributionState} / {@link recordFileContribution}
 * data model in session.ts — parity with the reference's AttributionSnapshot, whose
 * `fileStates` track `claudeContribution` characters per file.
 *
 * Keyed by the tool-context `sessionId` (a sub-engine's `agentId` is folded in by
 * the caller when it wants per-agent isolation). Absent/empty session ids share a
 * single global bucket — mirroring the todo-store's `default:` fallback, so bare
 * engines and tests still record consistently. The map lives only for the process
 * lifetime; nothing is persisted here (the git note is the durable surface).
 */

/**
 * Measure how many characters the agent authored in a single edit by isolating the
 * ACTUALLY-changed region of `oldContent → newContent` via a common-prefix then
 * common-suffix scan, then taking `max(oldChangedLen, newChangedLen)` as the
 * authored-character count. Unlike a naive `abs(newLen - oldLen)`, it correctly
 * scores a SAME-LENGTH replacement (e.g. `"Esc" → "esc"` ⇒ 1, not 0) and
 * sub-string substitutions where only an interior slice changed. A brand-new file
 * (`oldContent === ''`) scores the whole new length; a full deletion
 * (`newContent === ''`) scores the whole old length. Parity: the reference's
 * `computeFileModificationState` char-contribution core (commitAttribution.ts
 * L325-L367).
 */
export function computeAgentCharContribution(oldContent: string, newContent: string): number {
  if (oldContent === '' || newContent === '') {
    return oldContent === '' ? newContent.length : oldContent.length;
  }
  const minLen = Math.min(oldContent.length, newContent.length);
  let prefixEnd = 0;
  while (prefixEnd < minLen && oldContent[prefixEnd] === newContent[prefixEnd]) {
    prefixEnd++;
  }
  let suffixLen = 0;
  while (
    suffixLen < minLen - prefixEnd &&
    oldContent[oldContent.length - 1 - suffixLen] === newContent[newContent.length - 1 - suffixLen]
  ) {
    suffixLen++;
  }
  const oldChangedLen = oldContent.length - prefixEnd - suffixLen;
  const newChangedLen = newContent.length - prefixEnd - suffixLen;
  return Math.max(oldChangedLen, newChangedLen);
}

/**
 * Normalize `filePath` to a stable, cross-platform attribution key RELATIVE to
 * `repoRoot`, so the key the edit/write tools store under matches the one the
 * commit tool looks up under even when symlinks differ between the two paths
 * (e.g. macOS `/tmp` → `/private/tmp`). Both `filePath` and `repoRoot` are
 * realpath-resolved before the relative key is computed, and the result is forced
 * to forward slashes so it also matches `git diff` output on Windows. A relative
 * input is returned unchanged; an absolute path outside the repo falls back to the
 * raw-prefix relative form, and finally to the original path. Parity: the reference's
 * `normalizeFilePath` (commitAttribution.ts L252-L291).
 */
export function normalizeAttributionPath(filePath: string, repoRoot: string): string {
  if (!isAbsolute(filePath)) {
    return filePath;
  }

  let resolvedPath = filePath;
  let resolvedRoot = repoRoot;
  try { resolvedPath = realpathSync(filePath); } catch { /* file may not exist yet */ }
  try { resolvedRoot = realpathSync(repoRoot); } catch { /* keep original root */ }

  if (resolvedPath === resolvedRoot || resolvedPath.startsWith(resolvedRoot + sep)) {
    return relative(resolvedRoot, resolvedPath).split(sep).join('/');
  }
  // Fallback: compare the un-resolved paths (realpath may have failed above).
  if (filePath === repoRoot || filePath.startsWith(repoRoot + sep)) {
    return relative(repoRoot, filePath).split(sep).join('/');
  }
  return filePath;
}
