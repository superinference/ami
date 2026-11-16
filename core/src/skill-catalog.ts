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
}

// ---------------------------------------------------------------------------
// Installed skill manifest (persisted at ~/.ami/installed-skills/)
// ---------------------------------------------------------------------------

export interface InstalledSkillManifest {
  url: string;
  catalogName?: string;
  /** Catalog-level version declared by the publisher (see SkillCatalogIndex). */
  catalogVersion?: string;
  /** Catalog owner/author display name declared by the publisher. */
  owner?: string;
  fetchedAt: string;
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
