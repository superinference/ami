// Skill catalog types + path-safety utilities — building blocks for URL-based
// skill discovery. The management layer (fetch, cache, install) lives in
// common/src/skill-discovery.ts; core/ keeps only the types and validators.

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
   * listings (e.g. "productivity", "development"). Mirrors Claude Code's
   * marketplace-entry `category`.
   */
  category?: string;
  /**
   * Optional searchability tags for discovery. Mirrors Claude Code's
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
   * fallback), mirroring Claude Code's `checkBridgeMinVersion` default.
   */
  requires?: string;
  /**
   * Optional list of this skill's relative file paths that must be materialized
   * with an owner-execute bit set — scripts/hooks a skill ships and later runs
   * (e.g. a `hooks/*.sh` the hook runner invokes with `sh -c "/path/script.sh"`).
   * Mirrors Claude Code's archive exec-bit preservation (officialMarketplaceGcs
   * `parseZipModes` → `chmod` when `mode & 0o111`): without it a shipped script
   * extracts non-executable and fails with EACCES. AMI declares executability
   * per-file in the catalog because files are fetched individually over HTTP
   * (there are no archive mode bits to recover). Only entries that ALSO appear
   * in `files` survive validation; others are dropped with an error.
   */
  executable?: string[];
}

/** Lowercase 64-char hex SHA-256 digest. */
const HEX_SHA256_RE = /^[a-f0-9]{64}$/;

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
   * mirroring Claude Code's marketplace `metadata.version`.
   */
  version?: string;
  /**
   * Optional catalog owner/author display name. Accepted as a bare string, as
   * `{ name }`, or via an `author` alias — mirroring Claude Code's marketplace
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
   * mirroring Claude Code's `skill.pluginInfo.pluginManifest.name` display field
   * (SkillsMenu renderSkill). Kept on the canonical source record so the origin
   * survives name-collision namespacing.
   */
  bundleName?: string;
}

/**
 * User-facing source annotation for a skill/command listing (Claude parity:
 * `formatDescriptionWithSource` — typeahead + help screens show WHERE each entry
 * comes from). Returns the parenthesized origin token WITHOUT a leading space —
 * e.g. `(my-plugin)`, `(bundled)`, `(installed)`, `(user)`, `(project)` — or an
 * empty string when the origin should carry no suffix (an unknown/absent source),
 * so a caller composes `${description} ${suffix}` only when the suffix is present.
 *
 * Mapping from AMI's `SkillTrustTier` to the annotation:
 * - a plugin-contributed entry (`bundleName` set) wins → `(<bundleName>)`, so the
 *   originating plugin is named exactly like Claude's `(<pluginName>)`;
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
 * Claude Code's install-counts cache `version` gate (`INSTALL_COUNTS_CACHE_VERSION`).
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
   * insensitive to key ordering / whitespace (mirrors Claude Code's
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

const UNSAFE_SEGMENT_RE = /[/\\]/;

/**
 * Whether a candidate string has the shape of a traversal/separator attack:
 * empty, a bare "." / "..", or containing a null byte or a path separator.
 */
function hasUnsafeShape(s: string): boolean {
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
function candidateForms(s: string): string[] {
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

// ---------------------------------------------------------------------------
// Catalog/skill NAME validation — stricter than path-safety
// ---------------------------------------------------------------------------

/**
 * Names reserved by the loader/installer and therefore forbidden for a
 * catalog-supplied skill name. Mirrors Claude Code's reserved marketplace
 * names ("inline"/"builtin").
 */
const RESERVED_CATALOG_NAMES = new Set(['inline', 'builtin']);

/**
 * Whether a catalog/skill *name* is well-formed. Stricter than
 * {@link isSafeSegment}: in addition to full path-traversal safety it forbids
 * ANY whitespace (kebab-case convention) and the reserved loader names,
 * mirroring Claude Code's `MarketplaceNameSchema` (which rejects spaces and
 * reserves "inline"/"builtin"). Used for the identifier that becomes a
 * filesystem directory — not for free-form descriptions.
 */
export function isValidCatalogName(name: unknown): name is string {
  if (typeof name !== 'string') return false;
  if (!isSafeSegment(name)) return false;
  if (/\s/.test(name)) return false;
  if (RESERVED_CATALOG_NAMES.has(name.toLowerCase())) return false;
  return true;
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
 * top-level `version` string or a nested `metadata.version` string (Claude Code
 * nests it under `metadata`); returns undefined when neither is present.
 */
function extractCatalogVersion(obj: Record<string, unknown>): string | undefined {
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
 * `owner` as a bare string or as a `{ name }` object (Claude Code's
 * `PluginAuthor`), falling back to a bare `author` string; returns the display
 * name only, or undefined when none is present.
 */
function extractCatalogOwner(obj: Record<string, unknown>): string | undefined {
  const { owner, author } = obj;
  if (typeof owner === 'string' && owner.trim()) return owner;
  if (owner && typeof owner === 'object' && !Array.isArray(owner)) {
    const n = (owner as Record<string, unknown>).name;
    if (typeof n === 'string' && n.trim()) return n;
  }
  if (typeof author === 'string' && author.trim()) return author;
  return undefined;
}

export function validateCatalogIndex(raw: unknown): CatalogValidationResult {
  const errors: string[] = [];

  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { valid: false, errors: ['Catalog index must be a JSON object'] };
  }

  const obj = raw as Record<string, unknown>;

  if (!Array.isArray(obj.skills)) {
    return { valid: false, errors: ['Catalog index must have a "skills" array'] };
  }

  if (obj.skills.length > MAX_CATALOG_SKILLS) {
    return {
      valid: false,
      errors: [
        `Catalog index exceeds the maximum of ${MAX_CATALOG_SKILLS} skills (got ${obj.skills.length})`,
      ],
    };
  }

  const skills: SkillCatalogEntry[] = [];

  for (let i = 0; i < obj.skills.length; i++) {
    const entry = obj.skills[i] as Record<string, unknown>;

    if (!entry || typeof entry !== 'object') {
      errors.push(`skills[${i}]: must be an object`);
      continue;
    }

    if (typeof entry.name !== 'string' || !entry.name.trim()) {
      errors.push(`skills[${i}]: missing or empty "name"`);
      continue;
    }

    if (!isValidCatalogName(entry.name)) {
      errors.push(
        `skills[${i}]: unsafe skill name "${entry.name}" (names must have no whitespace, path separators, "."/".." or reserved values)`,
      );
      continue;
    }

    if (!Array.isArray(entry.files) || entry.files.length === 0) {
      errors.push(`skills[${i}] "${entry.name}": missing or empty "files" array`);
      continue;
    }

    if (entry.files.length > MAX_FILES_PER_SKILL) {
      errors.push(
        `skills[${i}] "${entry.name}": exceeds the maximum of ${MAX_FILES_PER_SKILL} files (got ${entry.files.length})`,
      );
      continue;
    }

    const safeFiles: string[] = [];
    for (const file of entry.files) {
      if (typeof file !== 'string') {
        errors.push(`skills[${i}] "${entry.name}": file entry must be a string`);
        continue;
      }
      if (!isSafeRelativePath(file)) {
        errors.push(`skills[${i}] "${entry.name}": unsafe file path "${file}"`);
        continue;
      }
      safeFiles.push(file);
    }

    if (safeFiles.length === 0) {
      errors.push(`skills[${i}] "${entry.name}": no valid file paths after safety check`);
      continue;
    }

    // Optional per-file integrity digests. Keep only well-formed hex SHA-256
    // entries that correspond to a validated safe file; flag malformed ones.
    let checksums: Record<string, string> | undefined;
    if (entry.checksums !== undefined) {
      if (!entry.checksums || typeof entry.checksums !== 'object' || Array.isArray(entry.checksums)) {
        errors.push(`skills[${i}] "${entry.name}": "checksums" must be an object`);
      } else {
        const parsed: Record<string, string> = {};
        for (const [file, digest] of Object.entries(entry.checksums as Record<string, unknown>)) {
          if (!safeFiles.includes(file)) {
            errors.push(`skills[${i}] "${entry.name}": checksum for unknown/unsafe file "${file}"`);
            continue;
          }
          if (!isHexSha256(digest)) {
            errors.push(`skills[${i}] "${entry.name}": invalid SHA-256 checksum for "${file}"`);
            continue;
          }
          parsed[file] = digest;
        }
        if (Object.keys(parsed).length > 0) checksums = parsed;
      }
    }

    // Optional executable-file declarations. Keep only well-formed paths that
    // correspond to a validated declared file, so a hostile catalog cannot mark
    // an undeclared/unsafe path executable; duplicates are collapsed.
    let executable: string[] | undefined;
    if (entry.executable !== undefined) {
      if (!Array.isArray(entry.executable)) {
        errors.push(`skills[${i}] "${entry.name}": "executable" must be an array`);
      } else {
        const execPaths: string[] = [];
        for (const p of entry.executable) {
          if (typeof p !== 'string') {
            errors.push(`skills[${i}] "${entry.name}": executable entry must be a string`);
            continue;
          }
          if (!safeFiles.includes(p)) {
            errors.push(`skills[${i}] "${entry.name}": executable path "${p}" is not a declared file`);
            continue;
          }
          if (!execPaths.includes(p)) execPaths.push(p);
        }
        if (execPaths.length > 0) executable = execPaths;
      }
    }

    // Optional discovery metadata: a free-form category and a tag list. Only
    // well-formed string values are retained; a tag array is filtered to strings
    // and capped so a hostile catalog cannot smuggle a huge/garbage collection.
    let tags: string[] | undefined;
    if (Array.isArray(entry.tags)) {
      const cleaned = entry.tags
        .filter((t): t is string => typeof t === 'string' && t.trim().length > 0)
        .slice(0, MAX_TAGS_PER_SKILL);
      if (cleaned.length > 0) tags = cleaned;
    }

    skills.push({
      name: entry.name,
      version: typeof entry.version === 'string' ? entry.version : undefined,
      description: typeof entry.description === 'string' ? entry.description : undefined,
      category: typeof entry.category === 'string' ? entry.category : undefined,
      tags,
      files: safeFiles,
      checksums,
      requires: typeof entry.requires === 'string' ? entry.requires : undefined,
      executable,
    });
  }

  if (skills.length === 0 && errors.length > 0) {
    return { valid: false, errors };
  }

  const index: SkillCatalogIndex = {
    name: typeof obj.name === 'string' ? obj.name : undefined,
    description: typeof obj.description === 'string' ? obj.description : undefined,
    version: extractCatalogVersion(obj),
    owner: extractCatalogOwner(obj),
    skills,
  };

  return { valid: true, errors, index };
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

/**
 * Whether `current` (the running AMI version) satisfies a skill's `required`
 * minimum version. An absent/empty `required` imposes no floor (returns true —
 * the graceful default), mirroring Claude Code's `checkBridgeMinVersion` which
 * passes when the configured minVersion is the default "0.0.0".
 */
export function isAmiVersionCompatible(required: string | undefined, current: string): boolean {
  if (!required || typeof required !== 'string' || !required.trim()) return true;
  return compareSemver(current, required) >= 0;
}

// ---------------------------------------------------------------------------
// URL normalization — resolve GitHub shorthand to raw content URLs
// ---------------------------------------------------------------------------

export function resolveSkillUrl(input: string): string {
  const trimmed = input.trim();

  // A trailing `#ref` fragment selects a git branch/tag/commit for GitHub
  // shorthand (`owner/repo#v1.2.3`), mirroring Claude Code's `#ref` convention.
  // Only a URL-safe, valid ref is honored; anything else is ignored (fall back
  // to the default branch) so a hostile fragment can never inject into the URL.
  const hashIdx = trimmed.indexOf('#');
  const body = hashIdx >= 0 ? trimmed.slice(0, hashIdx).trim() : trimmed;
  const refRaw = hashIdx >= 0 ? trimmed.slice(hashIdx + 1).trim() : '';
  const ref = refRaw && isValidGitRef(refRaw) ? refRaw : '';

  if (body.startsWith('http://') || body.startsWith('https://')) {
    return body.replace(/\/+$/, '');
  }

  const parts = body.split('/');
  if (parts.length >= 2 && parts[0] && parts[1]) {
    const owner = parts[0];
    const repo = parts[1];
    const subpath = parts.length > 2 ? parts.slice(2).join('/') : '';
    const branch = ref || 'main';
    const base = `https://raw.githubusercontent.com/${owner}/${repo}/${branch}`;
    return subpath ? `${base}/${subpath}` : base;
  }

  return body;
}

// ---------------------------------------------------------------------------
// Source policy — supply-chain allow/block-list gate for remote catalog hosts
// ---------------------------------------------------------------------------

/**
 * Allow/deny policy for remote skill/plugin catalog sources. An empty/absent
 * policy means "no restriction". `deny` takes precedence over `allow`; when a
 * non-empty `allow` list is present only matching hosts pass. Each entry is a
 * hostname (`example.com`) or a wildcard-subdomain pattern (`*.example.com`,
 * which also matches the apex `example.com`). Mirrors Claude Code's
 * blocklist-first-then-allowlist `isSourceAllowedByPolicy`.
 */
export interface SkillSourcePolicy {
  allow?: string[];
  deny?: string[];
}

/** Whether `host` matches a single policy pattern (exact host or `*.suffix`). */
function hostMatchesPattern(host: string, pattern: string): boolean {
  const p = pattern.trim().toLowerCase();
  if (!p) return false;
  if (p.startsWith('*.')) {
    const bare = p.slice(2);
    return host === bare || host.endsWith(`.${bare}`);
  }
  return host === p;
}

/**
 * Whether a catalog/source URL is permitted under `policy`. Returns true when
 * no policy (or an empty one) is supplied. An unparseable URL fails closed
 * (blocked) whenever any policy is in force. The canonical lowercased hostname
 * is matched against the deny list first (takes precedence), then the allow
 * list (an empty allow list imposes no positive requirement).
 */
export function isSkillSourceAllowed(url: string, policy?: SkillSourcePolicy): boolean {
  const deny = policy?.deny ?? [];
  const allow = policy?.allow ?? [];
  if (deny.length === 0 && allow.length === 0) return true;

  let host: string;
  try {
    host = new URL(url).hostname.replace(/^\[|\]$/g, '').toLowerCase();
  } catch {
    return false; // fail closed under an active policy
  }
  if (!host) return false;

  if (deny.some(p => hostMatchesPattern(host, p))) return false;
  if (allow.length === 0) return true;
  return allow.some(p => hostMatchesPattern(host, p));
}
