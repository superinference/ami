// Split from settings-guard.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/**
 * All dotted ancestors of `key`, nearest-first-excluded → `'a.b.c'` → `['a','a.b']`.
 * A flat key (no dot) has no ancestors. Used so a sub-key inherits the governance
 * of a managed/locked CONTAINER key (if the whole `permissions` key were managed,
 * `permissions.x` is managed too).
 */
export function dottedAncestors(key: string): string[] {
  const parts = key.split('.');
  if (parts.length < 2) return [];
  const out: string[] = [];
  for (let i = 1; i < parts.length; i++) out.push(parts.slice(0, i).join('.'));
  return out;
}

// ---------------------------------------------------------------------------
// JSON Schema generation for the settings file (parity: the reference's
// utils/settings/schemaOutput.ts generateSettingsJSONSchema). A published JSON
// Schema lets editors give autocomplete + live validation for settings.json (via
// a `$schema` ref) and gives one canonical, machine-consumable description of the
// policy keys AMI governs. the reference derives its schema from a Zod definition; AMI
// has no Zod, so the schema is DERIVED from this module's own authoritative key
// sets (MANAGED_ONLY_KEYS / TIER_LOCKED_KEYS / DANGEROUS_SHELL_SETTINGS) — it
// therefore self-updates when those sets change and can never silently drop a
// governed key (mirrors the campaign's "dynamic denominator" discipline). It is
// surfaced via `/settings-guard` and `ami --introspect`.
// ---------------------------------------------------------------------------

/** A single property node of the generated settings JSON Schema. */
export interface SettingsSchemaProperty {
  /** JSON Schema primitive type (omitted for union-typed keys left permissive). */
  type?: 'boolean' | 'number' | 'string' | 'array' | 'object';
  /** Human-readable description of the key. */
  description: string;
  /** Element schema, present only when `type === 'array'`. */
  items?: { type: 'string' | 'object' };
  /** Allowed string values, present only for enum-typed keys. */
  enum?: string[];
  /** True when the key is admin/managed-only (a {@link MANAGED_ONLY_KEYS} entry). */
  managed?: boolean;
  /** True when the key is {@link TIER_LOCKED_KEYS} (lower tiers cannot override it). */
  tierLocked?: boolean;
}
