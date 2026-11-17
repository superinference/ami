/**
 * LSP-plugin recommendation — the extension→plugin matching pipeline and the
 * response/anti-nag state machine.
 *
 * When a user opens or edits a file whose language has no active language server
 * but an installable LSP plugin exists (and whose server binary is already on the
 * system), AMI can proactively recommend installing it. This module is the pure,
 * dependency-light CORE building block for that flow; the marketplace scan (which
 * lives in common) feeds it {@link LspPluginCandidate} entries and the TUI/engine
 * drives the {@link LspRecommendationSession} and renders the recommendation.
 *
 * It complements the config-level gating already in `types.ts`
 * ({@link isLspRecommendationDisabled}, {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT},
 * `lspRecommendationNeverPlugins` / `lspRecommendationDisabled` /
 * `lspRecommendationIgnoredCount`) by adding the parts that were missing:
 *   1. {@link getMatchingLspPlugins} — the staged, cheapest-first filter pipeline
 *      (extension match → never-list → already-installed → binary-gate → dedup →
 *      official-first ordering), mirroring Claude Code's
 *      `getMatchingLspPlugins` (utils/plugins/lspRecommendation.ts).
 *   2. {@link applyLspRecommendationResponse} — the pure reducer that turns a user's
 *      4-value response (`yes | no | never | disable`) into config-field updates,
 *      mirroring the menu's response schema
 *      (components/LspRecommendation/LspRecommendationMenu.tsx) and the hook's
 *      never-suggest / disable / ignored-count side effects
 *      (hooks/useLspPluginRecommendation.tsx).
 *   3. {@link LspRecommendationSession} — the once-per-session cap + checked-files
 *      dedup Set the edit-triggered loop needs so each file is examined at most
 *      once and at most one recommendation is shown per session.
 *   4. {@link formatLspRecommendationHint} — the user-facing "an LSP server for this
 *      file is available" affordance wording.
 *
 * Pure and platform-free: no engine/UI/common import. The only shared primitive it
 * reuses is the canonical {@link isBinaryInstalled} probe from `utils/which` (no
 * second `which` is re-implemented), injectable so callers/tests can seed it.
 */
import * as path from 'path';
import { isBinaryInstalled } from '../utils/which';
import {
  isLspRecommendationDisabled,
  MAX_LSP_RECOMMENDATION_IGNORED_COUNT,
  type EngineConfig,
} from '../types';

/**
 * A user's answer to an LSP-plugin recommendation, mirroring Claude Code's 4-value
 * menu response schema (LspRecommendationMenu.tsx `onResponse`):
 *   - `yes`     — install the plugin now,
 *   - `no`      — defer (counts as an ignore; auto-dismiss/timeout maps here too),
 *   - `never`   — never suggest THIS plugin again (persistent per-plugin opt-out),
 *   - `disable` — turn OFF all LSP recommendations (global opt-out).
 */
export type LspRecommendationResponse = 'yes' | 'no' | 'never' | 'disable';

/** The subset of {@link EngineConfig} this module reads/writes. */
export type LspRecommendationConfig = Pick<
  EngineConfig,
  'lspRecommendationNeverPlugins' | 'lspRecommendationDisabled' | 'lspRecommendationIgnoredCount'
>;

/**
 * One marketplace plugin that contributes a language server, as fed in by the
 * marketplace scan (common's marketplace-manager). This is the INPUT shape for
 * {@link getMatchingLspPlugins}; keeping the scan out of core preserves the
 * `core → nothing-upward` dependency direction.
 */
export interface LspPluginCandidate {
  /** Stable id, conventionally `${pluginName}@${marketplaceName}`. */
  pluginId: string;
  /** Plugin display name. */
  pluginName: string;
  /** Marketplace the plugin came from. */
  marketplaceName: string;
  /** Optional human description shown in the recommendation. */
  description?: string;
  /** Whether the plugin's marketplace is an official/trusted one (ordering key). */
  isOfficial: boolean;
  /** File extensions the server handles (with leading dot; matched case-insensitively). */
  extensions: string[];
  /** The language-server binary — only recommended when this is already installed. */
  command: string;
  /** True when the plugin is already installed (never re-recommended). */
  installed?: boolean;
}

/**
 * A recommendation surfaced to the user. Keyed by `fileExtension` (the trigger)
 * plus the plugin identity, mirroring the menu's `Props`
 * (pluginName, pluginDescription?, fileExtension).
 */
export interface LspPluginRecommendation {
  pluginId: string;
  pluginName: string;
  marketplaceName: string;
  description?: string;
  isOfficial: boolean;
  /** All extensions the plugin handles (deduped, lowercased). */
  extensions: string[];
  /** The language-server binary (already verified installed). */
  command: string;
  /** The extension of the file that triggered this recommendation (lowercased). */
  fileExtension: string;
}

/** Options for {@link getMatchingLspPlugins}. */
export interface MatchLspPluginsOptions {
  /** Config gating (never-list / disabled / ignored-count). */
  config?: LspRecommendationConfig;
  /**
   * Binary-presence probe, injectable for tests. Defaults to the canonical
   * session-cached {@link isBinaryInstalled} — this module never re-implements a
   * second `which`.
   */
  isInstalled?: (command: string) => boolean;
}

/**
 * The lowercased file extension (WITH leading dot) of a path, or `''` when the
 * file has none. Mirrors Claude Code's `extname(filePath).toLowerCase()` + the
 * no-extension early return.
 */
export function lspFileExtension(filePath: string): string {
  if (typeof filePath !== 'string' || filePath.length === 0) return '';
  return path.extname(filePath).toLowerCase();
}

/**
 * Find the LSP plugins that should be recommended for `filePath`, applying the
 * SAME staged, deliberately cheapest-first filter pipeline as Claude Code's
 * `getMatchingLspPlugins`:
 *   0. GLOBAL gate — if recommendations are disabled (explicit off-switch OR the
 *      ignored-count reached {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT}) → `[]`.
 *   0b. no file extension → `[]`.
 *   1. sync extension-set membership (case-insensitive),
 *   2. never-suggest list (per-plugin persistent opt-out),
 *   3. already-installed,
 *   4. binary-installed gate (only recommend a server whose binary is on PATH),
 * then DEDUP by `pluginId` (first entry wins) and order OFFICIAL marketplaces
 * FIRST (stable for ties). The first element is therefore the preferred
 * ("official prioritized") recommendation, matching the hook's `matches[0]`.
 */
export function getMatchingLspPlugins(
  filePath: string,
  candidates: readonly LspPluginCandidate[],
  options: MatchLspPluginsOptions = {},
): LspPluginRecommendation[] {
  const config = options.config ?? {};
  // 0. Global off-switch (explicit disable OR ignored-count threshold reached).
  if (isLspRecommendationDisabled(config)) return [];

  // 0b. No extension → nothing to match.
  const ext = lspFileExtension(filePath);
  if (!ext) return [];

  const neverPlugins = new Set(config.lspRecommendationNeverPlugins ?? []);
  const isInstalled = options.isInstalled ?? isBinaryInstalled;

  // Dedup by pluginId while filtering; the FIRST candidate for an id wins so a
  // duplicate entry (same plugin in two scans) can't produce two recommendations.
  const seen = new Set<string>();
  const matched: LspPluginCandidate[] = [];
  for (const candidate of candidates) {
    if (seen.has(candidate.pluginId)) continue;
    // 1. extension membership (case-insensitive).
    const handles = candidate.extensions.some((e) => e.toLowerCase() === ext);
    if (!handles) continue;
    // 2. never-suggest list.
    if (neverPlugins.has(candidate.pluginId)) continue;
    // 3. already installed.
    if (candidate.installed) continue;
    // 4. binary gate — only recommend a server whose binary is present.
    if (!isInstalled(candidate.command)) continue;
    seen.add(candidate.pluginId);
    matched.push(candidate);
  }

  // Official marketplaces first; stable for same-official-ness (preserves scan order).
  matched.sort((a, b) => Number(b.isOfficial) - Number(a.isOfficial));

  return matched.map((c) => ({
    pluginId: c.pluginId,
    pluginName: c.pluginName,
    marketplaceName: c.marketplaceName,
    description: c.description,
    isOfficial: c.isOfficial,
    extensions: [...new Set(c.extensions.map((e) => e.toLowerCase()))],
    command: c.command,
    fileExtension: ext,
  }));
}

/**
 * The single preferred recommendation for a file — the OFFICIAL-prioritized
 * `matches[0]` of {@link getMatchingLspPlugins} — or `null` when there is none.
 * Mirrors the hook's `matches[0]` selection ("official plugins prioritized").
 */
export function getTopLspRecommendation(
  filePath: string,
  candidates: readonly LspPluginCandidate[],
  options: MatchLspPluginsOptions = {},
): LspPluginRecommendation | null {
  return getMatchingLspPlugins(filePath, candidates, options)[0] ?? null;
}

/**
 * Apply a user's 4-value {@link LspRecommendationResponse} to the recommendation
 * config, returning a NEW config object (never mutates the input). Mirrors the
 * side effects of Claude Code's `useLspPluginRecommendation` hook:
 *   - `yes`     — no suppression change (install is handled by the caller),
 *   - `no`      — increment `lspRecommendationIgnoredCount` (anti-nag backoff; the
 *                 menu's 30s auto-dismiss/timeout also maps here),
 *   - `never`   — add `pluginId` to `lspRecommendationNeverPlugins` (deduped),
 *   - `disable` — set `lspRecommendationDisabled = true` (global off-switch).
 * The ignored-count climbing to {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT} makes
 * {@link isLspRecommendationDisabled} report `true` thereafter.
 */
export function applyLspRecommendationResponse(
  config: LspRecommendationConfig,
  response: LspRecommendationResponse,
  pluginId: string,
): LspRecommendationConfig {
  const next: LspRecommendationConfig = { ...config };
  switch (response) {
    case 'yes':
      break;
    case 'no':
      next.lspRecommendationIgnoredCount = (config.lspRecommendationIgnoredCount ?? 0) + 1;
      break;
    case 'never': {
      const never = new Set(config.lspRecommendationNeverPlugins ?? []);
      never.add(pluginId);
      next.lspRecommendationNeverPlugins = [...never];
      break;
    }
    case 'disable':
      next.lspRecommendationDisabled = true;
      break;
  }
  return next;
}

/**
 * One-line, user-facing affordance for an available-but-not-enabled language
 * server, e.g. `An LSP server for .rs files is available — install the "rust"
 * plugin (rust-analyzer) for go-to-definition and error checking.` Surfaced when a
 * recommendation is found; the wording includes the trigger extension, the plugin
 * name, and the server binary so the user knows exactly what would be installed.
 */
export function formatLspRecommendationHint(rec: LspPluginRecommendation): string {
  const desc = rec.description ? ` ${rec.description}` : '';
  return (
    `An LSP server for ${rec.fileExtension} files is available — install the ` +
    `"${rec.pluginName}" plugin (${rec.command}) for go-to-definition and error ` +
    `checking.${desc}`
  );
}

/**
 * Per-session anti-nag state for the proactive, edit-triggered recommendation
 * loop. Mirrors Claude Code's `useLspPluginRecommendation` runtime refs:
 *   - a `checkedFiles` Set so each file path is examined AT MOST ONCE per session
 *     (the hook's `checkedFilesRef`), and
 *   - a once-per-session cap so AT MOST ONE recommendation is shown
 *     (`hasShownLspRecommendationThisSession`).
 * This is the mutable runtime companion to the persistent config gating in
 * `types.ts`; a frontend/engine owns one instance for the session's lifetime.
 */
export class LspRecommendationSession {
  private readonly checkedFiles = new Set<string>();
  private shown = false;

  /**
   * Return `true` the FIRST time a path is seen (and record it), `false` on every
   * subsequent call — so the edit loop matches each new file exactly once.
   */
  shouldCheckFile(filePath: string): boolean {
    if (this.checkedFiles.has(filePath)) return false;
    this.checkedFiles.add(filePath);
    return true;
  }

  /** Whether a recommendation has already been shown this session. */
  get hasShownThisSession(): boolean {
    return this.shown;
  }

  /**
   * Whether a recommendation may be shown now: only when none has been shown this
   * session yet AND recommendations are not disabled for `config`.
   */
  canShow(config: LspRecommendationConfig): boolean {
    return !this.shown && !isLspRecommendationDisabled(config);
  }

  /** Mark that a recommendation has now been shown (enforces the once-per-session cap). */
  markShown(): void {
    this.shown = true;
  }

  /** Reset all session state (e.g. on `/clear` or a new session). */
  reset(): void {
    this.checkedFiles.clear();
    this.shown = false;
  }
}
