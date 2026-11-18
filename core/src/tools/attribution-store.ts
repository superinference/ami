import { realpathSync } from 'fs';
import { isAbsolute, relative, sep } from 'path';
import { FileAttributionState, recordFileContribution } from '../session';
import { isGeneratedFile } from '../generated-files';
import { log } from '../logger';

/**
 * In-process, session-scoped store of per-file AGENT attribution. It accumulates
 * how many characters the agent authored in each file it edits/creates during a
 * session, so a later `git_commit` can surface a per-file contribution figure in
 * its attribution introspection note. This is the SOURCE wiring for the existing
 * (previously unconsumed) {@link FileAttributionState} / {@link recordFileContribution}
 * data model in session.ts — parity with Claude Code's AttributionSnapshot, whose
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
 * (`newContent === ''`) scores the whole old length. Parity: Claude Code's
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
 * raw-prefix relative form, and finally to the original path. Parity: Claude Code's
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

const GLOBAL_KEY = '__global__';

const sessionFileAttribution = new Map<string, Record<string, FileAttributionState>>();

function keyFor(sessionId: string | undefined): string {
  return sessionId && sessionId.length > 0 ? sessionId : GLOBAL_KEY;
}

/**
 * Record that the agent authored `contributedChars` characters of `filePath`,
 * whose new full content is `content` (last modified at `mtime`). Delegates the
 * per-file accumulation + content-hash refresh to session.ts's
 * {@link recordFileContribution} (the one canonical update), keeping the running
 * total across successive edits in the same session.
 */
export function recordAgentFileContribution(
  sessionId: string | undefined,
  filePath: string,
  content: string,
  contributedChars: number,
  mtime: number,
): void {
  const key = keyFor(sessionId);
  const prev = sessionFileAttribution.get(key) ?? {};
  const added = contributedChars > 0 ? contributedChars : 0;
  sessionFileAttribution.set(
    key,
    recordFileContribution(prev, filePath, content, contributedChars, mtime),
  );
  // Observability affordance (parity: Claude Code's `Attribution: Tracked N chars
  // for <path>` debug hint). Routed through the standard verbose debug path, so it
  // only surfaces under AMI_DEBUG_LOG_LEVEL=verbose (optionally narrowed with
  // AMI_DEBUG_COMPONENTS=attribution) and costs nothing otherwise.
  log('attribution', `Tracked ${added} chars for ${filePath}`, { filePath, added }, 'verbose');
}

/**
 * The per-file attribution map for a session (keyed by absolute file path), or an
 * empty object when nothing has been recorded. Returns the live reference — the
 * caller must not mutate it (consumers only read).
 */
export function getSessionFileAttribution(
  sessionId: string | undefined,
): Record<string, FileAttributionState> {
  return sessionFileAttribution.get(keyFor(sessionId)) ?? {};
}

/**
 * Session-scoped count of USER PROMPTS (each top-level engine `submit()` is one
 * prompt — a "shot"). This is the honest source for the "N-shotted" figure in a
 * commit's attribution introspection (parity: Claude Code's transcript
 * `countUserPromptsFromEntries` / "N-shotted by …"). Keyed like the file
 * attribution store so a commit can read the live count via the same `sessionId`
 * the tool executor already threads. Process-lifetime only; the git note is the
 * durable surface.
 */
const sessionPromptCount = new Map<string, number>();

/**
 * Record that one user prompt was submitted in this session (increments the
 * session-scoped "shot" counter). Wired from the engine's `submit()` entry so the
 * count reflects genuine user turns, not internal tool loops.
 */
export function recordUserPrompt(sessionId: string | undefined): void {
  const key = keyFor(sessionId);
  sessionPromptCount.set(key, (sessionPromptCount.get(key) ?? 0) + 1);
}

/**
 * The number of user prompts ("shots") recorded for a session so far, or `0` when
 * none were recorded (bare engines / tests). Read by `git_commit` to compose the
 * "N-shotted" attribution-introspection figure.
 */
export function getSessionPromptCount(sessionId: string | undefined): number {
  return sessionPromptCount.get(keyFor(sessionId)) ?? 0;
}

/** Overall authorship summary for a commit (parity: Claude Code's
 *  `AttributionSummary`, with vendor-neutral `agent*` naming). */
export interface AttributionSummary {
  /** Integer percent (0..100) of the committed tracked chars authored by the agent. */
  agentPercent: number;
  /** Total characters authored by the agent across the committed tracked files. */
  agentChars: number;
  /** Total characters attributed to a human (pre-existing content in tracked files). */
  humanChars: number;
  /** Distinct client surfaces that produced the edits. */
  surfaces: string[];
}

/** Per-file authorship record in {@link AttributionData} (parity: Claude Code's
 *  `FileAttribution`). */
export interface FileAttribution {
  agentChars: number;
  humanChars: number;
  /** Integer percent (0..100) of this file's tracked chars authored by the agent. */
  percent: number;
  /** The client surface that authored this file. */
  surface: string;
}

/**
 * Versioned, structured attribution payload for a commit — the durable schema a
 * git note (or any downstream tool) can persist and re-parse. Parity: Claude
 * Code's `AttributionData` (commitAttribution.ts L214-224), with vendor-neutral
 * naming (`agentChars`/`agentPercent` rather than `claude*`). The numeric
 * `version` discriminant lets the schema evolve without ambiguity.
 */
export interface AttributionData {
  version: 1;
  summary: AttributionSummary;
  /** Per-file authorship, keyed by staged (repo-relative) path. */
  files: Record<string, FileAttribution>;
  /** Agent-authored chars + percent-of-total, grouped by client surface. */
  surfaceBreakdown: Record<string, { agentChars: number; percent: number }>;
  /** Staged paths skipped as machine-generated (never counted toward authorship). */
  excludedGenerated: string[];
  /** Session ids whose tracked contributions fed this payload. */
  sessions: string[];
}

function roundPercent(part: number, whole: number): number {
  if (whole <= 0) return 0;
  return Math.min(100, Math.max(0, Math.round((100 * part) / whole)));
}

/**
 * Assemble the versioned {@link AttributionData} payload for a commit from the
 * per-file contributions the edit/write tools recorded this session. Generated
 * staged files ({@link isGeneratedFile}) are excluded from authorship and surfaced
 * under {@link AttributionData.excludedGenerated}. For each tracked, non-generated
 * file the agent's authored chars come from the recorded contribution and the
 * human share is the MEASURED remainder of the committed size
 * (`max(0, totalChars - agentChars)`) — never a fabricated figure. A
 * `surfaceBreakdown` groups authored chars by surface with each surface's percent
 * of the total tracked content. Pure (no fs / git) so it is deterministic and
 * unit-testable; the caller supplies the already-sized contributions.
 */
export function buildAttributionData(params: {
  surface: string;
  sessions: string[];
  stagedFiles: readonly string[];
  contributions: ReadonlyArray<{ path: string; agentContribution: number; totalChars: number }>;
}): AttributionData {
  const { surface, sessions, stagedFiles, contributions } = params;
  const excludedGenerated = stagedFiles.filter(isGeneratedFile);
  const generated = new Set(excludedGenerated);

  const files: Record<string, FileAttribution> = {};
  const surfaceBreakdown: Record<string, { agentChars: number; percent: number }> = {};
  let totalAgentChars = 0;
  let totalHumanChars = 0;

  for (const c of contributions) {
    if (generated.has(c.path)) continue;
    const agentChars = Math.max(0, c.agentContribution);
    const total = Math.max(0, c.totalChars);
    const humanChars = Math.max(0, total - agentChars);
    files[c.path] = {
      agentChars,
      humanChars,
      percent: roundPercent(agentChars, agentChars + humanChars),
      surface,
    };
    totalAgentChars += agentChars;
    totalHumanChars += humanChars;
  }

  const grandTotal = totalAgentChars + totalHumanChars;
  if (totalAgentChars > 0 || Object.keys(files).length > 0) {
    surfaceBreakdown[surface] = {
      agentChars: totalAgentChars,
      percent: roundPercent(totalAgentChars, grandTotal),
    };
  }

  return {
    version: 1,
    summary: {
      agentPercent: roundPercent(totalAgentChars, grandTotal),
      agentChars: totalAgentChars,
      humanChars: totalHumanChars,
      surfaces: [surface],
    },
    files,
    surfaceBreakdown,
    excludedGenerated,
    sessions,
  };
}
