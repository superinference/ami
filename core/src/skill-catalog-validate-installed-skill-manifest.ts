// Split from skill-catalog.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';

// ---------------------------------------------------------------------------
// Catalog index schema (served as index.json at catalog URLs)
// ---------------------------------------------------------------------------

export interface SkillCatalogEntry {
  name: string;
  version?: string;
  description?: string;
  files: string[];
  /**
   * Optional free-form category used to organize/filter a skill in discovery
   * listings (e.g. "productivity", "development"). Mirrors the reference's
   * marketplace-entry `category`.
   */
  category?: string;
  /**
   * Optional searchability tags for discovery. Mirrors the reference's
   * marketplace-entry `tags`. Non-string members are dropped during validation.
   */
  tags?: string[];
  /**
   * Optional integrity map: relative file path → lowercase hex SHA-256 of the
   * file's bytes. When present, the discovery layer verifies each downloaded
   * file against its declared digest and refuses a tampered/corrupt file.
   */
  checksums?: Record<string, string>;
  /**
   * Optional minimum AMI version (semver) required to install this skill. When
   * the running AMI is older, the discovery layer refuses the skill with an
   * actionable "run `ami update`" message rather than installing something it
   * cannot run. Absent/empty/malformed means "no floor" (graceful 0.0.0
   * fallback), mirroring the reference's `checkBridgeMinVersion` default.
   */
  requires?: string;
  /**
   * Optional list of this skill's relative file paths that must be materialized
   * with an owner-execute bit set — scripts/hooks a skill ships and later runs
   * (e.g. a `hooks/*.sh` the hook runner invokes with `sh -c "/path/script.sh"`).
   * Mirrors the reference's archive exec-bit preservation (officialMarketplaceGcs
   * `parseZipModes` → `chmod` when `mode & 0o111`): without it a shipped script
   * extracts non-executable and fails with EACCES. AMI declares executability
   * per-file in the catalog because files are fetched individually over HTTP
   * (there are no archive mode bits to recover). Only entries that ALSO appear
   * in `files` survive validation; others are dropped with an error.
   */
  executable?: string[];
}

/** Lowercase 64-char hex SHA-256 digest. */
export const HEX_SHA256_RE = /^[a-f0-9]{64}$/;

/** Whether `s` is a well-formed lowercase hex SHA-256 digest. */
export function isHexSha256(s: unknown): s is string {
  return typeof s === 'string' && HEX_SHA256_RE.test(s);
}

export interface SkillCatalogIndex {
  name?: string;
  description?: string;
  /**
   * Optional catalog-level version string (semver recommended). Surfaced so a
   * consumer can display and compare the publisher's declared catalog version.
   * Accepted either at the top level (`version`) or nested under `metadata`,
   * mirroring the reference's marketplace `metadata.version`.
   */
  version?: string;
  /**
   * Optional catalog owner/author display name. Accepted as a bare string, as
   * `{ name }`, or via an `author` alias — mirroring the reference's marketplace
   * `owner` (a `PluginAuthor` object). Only the display name is retained.
   */
  owner?: string;
  skills: SkillCatalogEntry[];
}

// ---------------------------------------------------------------------------
// Skill source tracking — where a skill was loaded from
// ---------------------------------------------------------------------------

export const SKILL_TRUST_TIERS = ['core', 'installed', 'project', 'user'] as const;

export type SkillTrustTier = (typeof SKILL_TRUST_TIERS)[number];

export interface SkillSource {
  tier: SkillTrustTier;
  url?: string;
  catalogName?: string;
  /**
   * For a plugin-contributed skill, the name of the originating plugin bundle
   * (`PluginBundle.name`). Absent for a skill loaded from a tier directory
   * (core/user/project) or a URL catalog. Surfaced in the skill listing as
   * ` · <bundle>` so a user/model can see which plugin a skill came from,
   * mirroring the reference's `skill.pluginInfo.pluginManifest.name` display field
   * (SkillsMenu renderSkill). Kept on the canonical source record so the origin
   * survives name-collision namespacing.
   */
  bundleName?: string;
}

/**
 * User-facing source annotation for a skill/command listing (the reference parity:
 * `formatDescriptionWithSource` — typeahead + help screens show WHERE each entry
 * comes from). Returns the parenthesized origin token WITHOUT a leading space —
 * e.g. `(my-plugin)`, `(bundled)`, `(installed)`, `(user)`, `(project)` — or an
 * empty string when the origin should carry no suffix (an unknown/absent source),
 * so a caller composes `${description} ${suffix}` only when the suffix is present.
 *
 * Mapping from AMI's `SkillTrustTier` to the annotation:
 * - a plugin-contributed entry (`bundleName` set) wins → `(<bundleName>)`, so the
 *   originating plugin is named exactly like the reference's `(<pluginName>)`;
 * - tier `core` is AMI's bundled catalog → `(bundled)`;
 * - tiers `installed` / `user` / `project` annotate with their own tier name.
 *
 * Pure (no I/O) so it is shared verbatim by every listing surface (`/skills`,
 * `/agents`, help) rather than re-deriving the suffix per call site.
 */
export function formatSkillSourceSuffix(
  source: Pick<SkillSource, 'tier' | 'bundleName'> | null | undefined,
): string {
  if (!source) return '';
  if (source.bundleName) return `(${source.bundleName})`;
  switch (source.tier) {
    case 'core':
      return '(bundled)';
    case 'installed':
      return '(installed)';
    case 'user':
      return '(user)';
    case 'project':
      return '(project)';
    default:
      return '';
  }
}

// ---------------------------------------------------------------------------
// Installed skill manifest (persisted at ~/.ami/installed-skills/)
// ---------------------------------------------------------------------------

/**
 * Current on-disk schema version for a persisted {@link InstalledSkillManifest}.
 * Bump this whenever the manifest layout changes incompatibly: a cached manifest
 * stamped with a different number is treated as absent (invalidated) so a format
 * change forces a clean re-pull instead of trusting a foreign layout. Mirrors
 * the reference's install-counts cache `version` gate (`INSTALL_COUNTS_CACHE_VERSION`).
 */
export const INSTALLED_MANIFEST_SCHEMA_VERSION = 1;

export interface InstalledSkillManifest {
  url: string;
  catalogName?: string;
  /** Catalog-level version declared by the publisher (see SkillCatalogIndex). */
  catalogVersion?: string;
  /** Catalog owner/author display name declared by the publisher. */
  owner?: string;
  /**
   * On-disk schema version stamp (see {@link INSTALLED_MANIFEST_SCHEMA_VERSION}).
   * Absent on legacy manifests written before the stamp existed — those are read
   * as the implicit baseline; a PRESENT-but-mismatched stamp invalidates the cache.
   */
  schemaVersion?: number;
  fetchedAt: string;
  /**
   * Stable `sha256:<hex>` checksum of the catalog index content the last pull
   * materialized, computed over a deep key-sorted normalization so it is
   * insensitive to key ordering / whitespace (mirrors the reference's
   * policyLimits / remoteManagedSettings `computeChecksum`). When a fresh pull
   * produces the identical checksum the catalog content is unchanged and the
   * per-skill file re-download is skipped. Absent on legacy manifests.
   */
  catalogChecksum?: string;
  skills: Array<{ name: string; version?: string; category?: string; tags?: string[] }>;
}

// ---------------------------------------------------------------------------
// Path safety — prevent directory traversal in catalog entries
// ---------------------------------------------------------------------------

export const UNSAFE_SEGMENT_RE = /[/\\]/;

/**
 * Whether a candidate string has the shape of a traversal/separator attack:
 * empty, a bare "." / "..", or containing a null byte or a path separator.
 */
export function hasUnsafeShape(s: string): boolean {
  if (s === '' || s === '.' || s === '..') return true;
  if (s.includes('\0')) return true;
  if (UNSAFE_SEGMENT_RE.test(s)) return true;
  return false;
}

/**
 * Expand a segment into every form an attacker (or a downstream layer) could
 * coerce it into before it reaches the filesystem: the raw string, every
 * percent-decoded form (iterated so double/triple URL-encoding is unwrapped),
 * and the NFKC Unicode normalization of each. This defeats encoded traversal
 * (`%2e%2e`, `%2f`, `%252e%252e`) and Unicode-normalization traversal
 * (fullwidth `．．` U+FF0E, `／` U+FF0F → `..`, `/`).
 *
 * Percent-decoding strictly shortens the string whenever it changes a `%XX`
 * escape, so the loop is guaranteed to terminate (it stops on a no-op decode
 * or on a malformed-escape throw).
 */
export function candidateForms(s: string): string[] {
  const forms: string[] = [s];
  let cur = s;
  for (;;) {
    let dec: string;
    try {
      dec = decodeURIComponent(cur);
    } catch {
      // Malformed percent-encoding (e.g. a literal "100%.md"): it cannot be a
      // valid encoded traversal, so stop decoding and keep the raw form.
      break;
    }
    if (dec === cur) break;
    forms.push(dec);
    cur = dec;
  }
  for (const f of [...forms]) {
    const normalized = f.normalize('NFKC');
    if (!forms.includes(normalized)) forms.push(normalized);
  }
  return forms;
}

export function isSafeSegment(segment: string): boolean {
  if (!segment || segment.length === 0) return false;
  if (segment.length > 255) return false;
  for (const form of candidateForms(segment)) {
    if (hasUnsafeShape(form)) return false;
  }
  return true;
}

export function isSafeRelativePath(filePath: string): boolean {
  if (!filePath || filePath.length === 0) return false;
  if (path.isAbsolute(filePath)) return false;
  const normalized = path.normalize(filePath);
  if (normalized.startsWith('..')) return false;
  const segments = normalized.split(path.sep);
  return segments.every(s => isSafeSegment(s));
}

/**
 * Whether `ref` is a URL-safe git ref (branch/tag/commit) we will splice into a
 * raw-content URL. A ref may contain `/` (e.g. `feature/x`) but must not carry
 * traversal (`..`), empty segments (`//`), trailing separators, whitespace, or
 * any character outside the git-ref-safe + URL-path-safe set — so a hostile
 * `#ref` fragment can never escape or inject into the resolved URL.
 */
export function isValidGitRef(ref: string): boolean {
  if (!ref || ref.length > 255) return false;
  if (ref.includes('..') || ref.includes('//')) return false;
  if (ref.endsWith('/') || ref.endsWith('.')) return false;
  return /^[A-Za-z0-9][A-Za-z0-9._/-]*$/.test(ref);
}

// ---------------------------------------------------------------------------
// Catalog index validation
// ---------------------------------------------------------------------------

export interface CatalogValidationResult {
  valid: boolean;
  errors: string[];
  index?: SkillCatalogIndex;
}

/**
 * Upper bounds on an untrusted catalog index to prevent a hostile/compromised
 * catalog from exhausting memory via an unbounded `skills` array or an
 * unbounded per-skill `files` array (resource-exhaustion / DoS guard). Mirrors
 * the entry-count ceilings applied when ingesting other untrusted bundles.
 */
export const MAX_CATALOG_SKILLS = 10_000;

export const MAX_FILES_PER_SKILL = 10_000;

/** Max number of discovery tags kept per skill (excess are dropped). */
export const MAX_TAGS_PER_SKILL = 100;

/**
 * Extract a catalog-level version string from a raw index object. Accepts a
 * top-level `version` string or a nested `metadata.version` string (the reference
 * nests it under `metadata`); returns undefined when neither is present.
 */
export function extractCatalogVersion(obj: Record<string, unknown>): string | undefined {
  if (typeof obj.version === 'string') return obj.version;
  const meta = obj.metadata;
  if (meta && typeof meta === 'object' && !Array.isArray(meta)) {
    const v = (meta as Record<string, unknown>).version;
    if (typeof v === 'string') return v;
  }
  return undefined;
}

/**
 * Extract a catalog owner/author display name from a raw index object. Accepts
 * `owner` as a bare string or as a `{ name }` object (the reference's
 * `PluginAuthor`), falling back to a bare `author` string; returns the display
 * name only, or undefined when none is present.
 */
export function extractCatalogOwner(obj: Record<string, unknown>): string | undefined {
  const { owner, author } = obj;
  if (typeof owner === 'string' && owner.trim()) return owner;
  if (owner && typeof owner === 'object' && !Array.isArray(owner)) {
    const n = (owner as Record<string, unknown>).name;
    if (typeof n === 'string' && n.trim()) return n;
  }
  if (typeof author === 'string' && author.trim()) return author;
  return undefined;
}

// ---------------------------------------------------------------------------
// Installed-manifest validation — defensive schema check of the on-disk cache
// ---------------------------------------------------------------------------

/**
 * Strictly validate an untrusted on-disk installed-skills manifest (the parsed
 * JSON of `~/.ami/installed-skills/<hash>/manifest.json`) before any fs op
 * trusts it. Returns a sanitized manifest or null (invalid/incompatible).
 *
 * This is the canonical manifest validator — the discovery layer parses the file
 * and delegates here so the "trust the on-disk cache" rule lives in exactly one
 * place (mirrors {@link validateCatalogIndex} for the remote index). Guarantees:
 *   - the object has a string `url` and an array `skills`;
 *   - every retained skill `name` is a SAFE single path segment, so a crafted
 *     manifest can never drive a traversal in a later rm/read of the cache;
 *   - a PRESENT-but-mismatched `schemaVersion` invalidates the whole manifest
 *     (returns null) so an incompatible cache format forces a clean re-pull,
 *     while a manifest with no stamp is accepted as the implicit baseline.
 */
export function validateInstalledSkillManifest(raw: unknown): InstalledSkillManifest | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const o = raw as Record<string, unknown>;
  if (typeof o.url !== 'string') return null;
  if (!Array.isArray(o.skills)) return null;

  // Schema-version gate (cache invalidation). An explicit stamp that is not a
  // number, or does not equal the current schema, means the cache was written by
  // an incompatible installer: discard it rather than trust a foreign layout.
  if (o.schemaVersion !== undefined && o.schemaVersion !== INSTALLED_MANIFEST_SCHEMA_VERSION) {
    return null;
  }

  const skills: InstalledSkillManifest['skills'] = [];
  for (const s of o.skills) {
    if (!s || typeof s !== 'object') continue;
    const sr = s as Record<string, unknown>;
    const name = sr.name;
    if (typeof name !== 'string' || !isSafeSegment(name)) continue;
    const version = typeof sr.version === 'string' ? sr.version : undefined;
    const category = typeof sr.category === 'string' ? sr.category : undefined;
    let tags: string[] | undefined;
    if (Array.isArray(sr.tags)) {
      const cleaned = sr.tags
        .filter((t): t is string => typeof t === 'string' && t.trim().length > 0)
        .slice(0, MAX_TAGS_PER_SKILL);
      if (cleaned.length > 0) tags = cleaned;
    }
    skills.push({ name, version, category, tags });
  }

  return {
    url: o.url,
    catalogName: typeof o.catalogName === 'string' ? o.catalogName : undefined,
    catalogVersion: typeof o.catalogVersion === 'string' ? o.catalogVersion : undefined,
    owner: typeof o.owner === 'string' ? o.owner : undefined,
    schemaVersion: typeof o.schemaVersion === 'number' ? o.schemaVersion : undefined,
    fetchedAt: typeof o.fetchedAt === 'string' ? o.fetchedAt : '',
    catalogChecksum: typeof o.catalogChecksum === 'string' ? o.catalogChecksum : undefined,
    skills,
  };
}

// ---------------------------------------------------------------------------
// Semver compatibility floor — "requires AMI vX" gate for catalog skills
// ---------------------------------------------------------------------------

/**
 * Compare two dotted-numeric version strings by their semver core
 * (`major.minor.patch`); any `-prerelease` / `+build` suffix is ignored.
 * Returns -1 if `a < b`, 0 if equal, 1 if `a > b`. A missing or non-numeric
 * segment is treated as 0, so malformed input degrades to a best-effort numeric
 * compare (graceful "0.0.0" fallback) rather than throwing.
 */
export function compareSemver(a: string, b: string): number {
  const parse = (v: string): number[] =>
    v.trim().split(/[-+]/)[0].split('.').map(seg => {
      const n = parseInt(seg, 10);
      return Number.isFinite(n) && n >= 0 ? n : 0;
    });
  const pa = parse(a);
  const pb = parse(b);
  const len = Math.max(pa.length, pb.length);
  for (let i = 0; i < len; i++) {
    const x = pa[i] ?? 0;
    const y = pb[i] ?? 0;
    if (x > y) return 1;
    if (x < y) return -1;
  }
  return 0;
}
