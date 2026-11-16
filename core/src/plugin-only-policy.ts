/**
 * Managed `strictPluginOnlyCustomization` policy — per-surface customization
 * locking. An admin/managed-only policy that confines named customization
 * surfaces (skills / agents / hooks / mcp) to plugin- and managed-provided
 * sources only, blocking user-level (`~/.ami`, `~/.claude`) and project-level
 * (`.ami`, `.claude`) filesystem sources for those surfaces.
 *
 * This is finer-grained than `safeMode` (all-or-nothing project-local block) and
 * composes with it: an admin can lock just `hooks` while still allowing project
 * skills. Parity: Claude Code's `strictPluginOnlyCustomization`
 * (utils/settings/pluginOnlyPolicy.ts + the schema preprocess/.catch in types.ts).
 *
 * The parser is intentionally degrade-safe (mirrors Claude's zod preprocess +
 * `.catch(undefined)`): an array drops unknown surface names so a future enum
 * value never fails the whole file (degrades to less-locked), and any non-array
 * invalid value degrades to `undefined` (unlocked-for-this-field) rather than
 * nulling the entire managed-settings file.
 */

/**
 * Surfaces lockable by `strictPluginOnlyCustomization`. Single source of truth
 * shared by the parser and {@link isRestrictedToPluginOnly}.
 */
export const CUSTOMIZATION_SURFACES = ['skills', 'agents', 'hooks', 'mcp'] as const;
export type CustomizationSurface = (typeof CUSTOMIZATION_SURFACES)[number];

/** The parsed, trustworthy shape of a `strictPluginOnlyCustomization` value. */
export type StrictPluginOnlyPolicy = boolean | CustomizationSurface[];

function isKnownSurface(x: unknown): x is CustomizationSurface {
  return typeof x === 'string' && (CUSTOMIZATION_SURFACES as readonly string[]).includes(x);
}

/**
 * Degrade-safely normalize a raw `strictPluginOnlyCustomization` value:
 *  - a boolean passes through (`true` = lock all, `false` = explicit no-op);
 *  - an array is filtered to KNOWN surfaces, dropping unknown/non-string entries
 *    so a future surface name degrades to less-locked, never fails the file;
 *  - anything else (string, object, number, null, undefined) → `undefined`,
 *    i.e. unlocked-for-this-field rather than everything-broken.
 * Mirrors Claude's zod `.preprocess(...)` + `.catch(undefined)`.
 */
export function parseStrictPluginOnlyCustomization(value: unknown): StrictPluginOnlyPolicy | undefined {
  if (typeof value === 'boolean') return value;
  if (Array.isArray(value)) return value.filter(isKnownSurface);
  return undefined;
}

/**
 * Whether `surface` is locked to plugin/managed-only sources by the given policy
 * value. The value may be already-parsed OR raw (as it lands from JSON) — it is
 * normalized via {@link parseStrictPluginOnlyCustomization} first, so a raw value
 * with an unknown surface or an invalid non-array shape decides correctly
 * (degrading to unlocked) without the caller having to pre-parse. `true` locks
 * all surfaces; an array locks only the listed ones; absent/`false`/invalid →
 * nothing locked (the default). Parity: Claude's `isRestrictedToPluginOnly`.
 */
export function isRestrictedToPluginOnly(surface: CustomizationSurface, policy: unknown): boolean {
  const parsed = parseStrictPluginOnlyCustomization(policy);
  if (parsed === true) return true;
  if (Array.isArray(parsed)) return parsed.includes(surface);
  return false;
}

/**
 * Sources that BYPASS `strictPluginOnlyCustomization` (always loaded for a locked
 * surface) because they are admin-trusted by construction:
 *   - `plugin` — gated separately by the marketplace allowlist policy;
 *   - `policySettings` — managed/admin settings, admin-controlled by definition;
 *   - `built-in` / `builtin` / `bundled` — ship with the CLI, not user-authored.
 * Everything else (user/project/local filesystem sources) is user-controlled and
 * is blocked when its surface is locked. Parity: Claude's ADMIN_TRUSTED_SOURCES.
 */
export const ADMIN_TRUSTED_SOURCES: ReadonlySet<string> = new Set([
  'plugin',
  'policySettings',
  'built-in',
  'builtin',
  'bundled',
]);

/** Whether a customization item's source is admin-trusted under the policy. */
export function isSourceAdminTrusted(source: string | undefined): boolean {
  return source !== undefined && ADMIN_TRUSTED_SOURCES.has(source);
}
