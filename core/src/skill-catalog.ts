// Skill catalog types + path-safety utilities — building blocks for URL-based
// skill discovery. The management layer (fetch, cache, install) lives in
// common/src/skill-discovery.ts; core/ keeps only the types and validators.

import 'path';
import { MAX_CATALOG_SKILLS, MAX_FILES_PER_SKILL, MAX_TAGS_PER_SKILL, compareSemver, extractCatalogOwner, extractCatalogVersion, isHexSha256, isSafeRelativePath, isSafeSegment, isValidGitRef } from './skill-catalog-validate-installed-skill-manifest';
import type { CatalogValidationResult, SkillCatalogEntry, SkillCatalogIndex } from './skill-catalog-validate-installed-skill-manifest';
export { INSTALLED_MANIFEST_SCHEMA_VERSION, MAX_CATALOG_SKILLS, MAX_FILES_PER_SKILL, MAX_TAGS_PER_SKILL, SKILL_TRUST_TIERS, compareSemver, formatSkillSourceSuffix, isHexSha256, isSafeRelativePath, isSafeSegment, isValidGitRef, validateInstalledSkillManifest } from './skill-catalog-validate-installed-skill-manifest';
export type { CatalogValidationResult, InstalledSkillManifest, SkillCatalogEntry, SkillCatalogIndex, SkillSource, SkillTrustTier } from './skill-catalog-validate-installed-skill-manifest';

// ---------------------------------------------------------------------------
// Catalog/skill NAME validation — stricter than path-safety
// ---------------------------------------------------------------------------

/**
 * Names reserved by the loader/installer and therefore forbidden for a
 * catalog-supplied skill name. Mirrors the reference's reserved marketplace
 * names ("inline"/"builtin").
 */
const RESERVED_CATALOG_NAMES = new Set(['inline', 'builtin']);

/**
 * Whether a catalog/skill *name* is well-formed. Stricter than
 * {@link isSafeSegment}: in addition to full path-traversal safety it forbids
 * ANY whitespace (kebab-case convention) and the reserved loader names,
 * mirroring the reference's `MarketplaceNameSchema` (which rejects spaces and
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

/** Parse the release triple of a version, ignoring any `-prerelease`/`+build` tag (so the
 *  whole comparator engine shares {@link compareSemver}'s prerelease-agnostic contract). A
 *  missing segment is 0 and a non-numeric segment (e.g. `x`) yields `NaN`, which the
 *  wildcard expander below replaces. */
function versionTriple(v: string): [number, number, number] {
  const core = v.trim().replace(/^[v=\s]+/, '').split(/[-+]/)[0];
  const segs = core.split('.');
  const num = (s: string | undefined): number => {
    if (s === undefined || s === '' || s === 'x' || s === 'X' || s === '*') return NaN;
    const n = parseInt(s, 10);
    return Number.isFinite(n) && n >= 0 ? n : NaN;
  };
  return [num(segs[0]), num(segs[1]), num(segs[2])];
}

/** A concrete comparator: an operator applied against a fully-resolved version triple. */
interface Comparator { op: '<' | '<=' | '>' | '>=' | '='; v: [number, number, number]; }

function cmpTriple(a: [number, number, number], b: [number, number, number]): number {
  for (let i = 0; i < 3; i++) { if (a[i] > b[i]) return 1; if (a[i] < b[i]) return -1; }
  return 0;
}

function testComparator(ver: [number, number, number], c: Comparator): boolean {
  const r = cmpTriple(ver, c.v);
  switch (c.op) {
    case '<': return r < 0;
    case '<=': return r <= 0;
    case '>': return r > 0;
    case '>=': return r >= 0;
    case '=': return r === 0;
  }
}

/**
 * Expand a single range token into the set of concrete {@link Comparator}s it implies
 * (AND-ed together). Supports the common npm-style forms: caret (`^1.2.3`), tilde
 * (`~1.2`), the comparators `>`/`>=`/`<`/`<=`/`=`, bare exacts (`1.2.3`), and `x`/`*`
 * wildcards (`1.x`, `1.2.*`, `*`). A bare `*`/`x`/empty token imposes no bound (matches
 * any version). Returns null for a token that cannot be parsed, so the whole range fails
 * closed rather than silently matching. Prerelease/build tags are ignored (shared with
 * {@link compareSemver}).
 */
function expandComparator(tokenRaw: string): Comparator[] | null {
  const token = tokenRaw.trim();
  if (token === '' || token === '*' || token === 'x' || token === 'X') return [];

  const m = /^(\^|~|>=|<=|>|<|=|)\s*(.+)$/.exec(token);
  if (!m) return null;
  const op = m[1];
  const t = versionTriple(m[2]);
  // How many leading segments were explicitly pinned (stop at the first wildcard/missing).
  let pinned = 0;
  for (; pinned < 3; pinned++) if (Number.isNaN(t[pinned])) break;
  if (pinned === 0) return op === '' ? [] : null; // "^x"/">=*" etc. — no lower bound
  const lo: [number, number, number] = [t[0], Number.isNaN(t[1]) ? 0 : t[1], Number.isNaN(t[2]) ? 0 : t[2]];

  if (op === '^') {
    // Caret: compatible-within-leading-nonzero. ^1.2.3 → >=1.2.3 <2.0.0; ^0.2.3 → <0.3.0;
    // ^0.0.3 → <0.0.4. Partial carets (^1.2 / ^0) widen the floor's missing segments to 0.
    let hi: [number, number, number];
    if (lo[0] !== 0) hi = [lo[0] + 1, 0, 0];
    else if (lo[1] !== 0) hi = [0, lo[1] + 1, 0];
    else hi = [0, 0, lo[2] + 1];
    return [{ op: '>=', v: lo }, { op: '<', v: hi }];
  }
  if (op === '~') {
    // Tilde: allow patch-level (or minor when only major is pinned) changes.
    const hi: [number, number, number] = pinned === 1 ? [lo[0] + 1, 0, 0] : [lo[0], lo[1] + 1, 0];
    return [{ op: '>=', v: lo }, { op: '<', v: hi }];
  }
  if (op === '' || op === '=') {
    // A partial bare version (`1`, `1.2`) is an implicit range over the unpinned segments
    // (1 → >=1.0.0 <2.0.0; 1.2 → >=1.2.0 <1.3.0); a full `1.2.3` is an exact match.
    if (pinned === 3) return [{ op: '=', v: lo }];
    const hi: [number, number, number] = pinned === 1 ? [lo[0] + 1, 0, 0] : [lo[0], lo[1] + 1, 0];
    return [{ op: '>=', v: lo }, { op: '<', v: hi }];
  }
  return [{ op: op as Comparator['op'], v: lo }];
}

/** Expand a space-separated AND-group, honoring the `A - B` hyphen-range form. */
function expandAndGroup(group: string): Comparator[] | null {
  const g = group.trim();
  // Hyphen range `A - B` — a standalone ` - ` (surrounded by whitespace) separates the two
  // bounds. Located by indexOf (no nested-quantifier regex) to stay ReDoS-safe.
  const dashIdx = g.indexOf(' - ');
  if (dashIdx > 0) {
    const lo = versionTriple(g.slice(0, dashIdx));
    const hi = versionTriple(g.slice(dashIdx + 3));
    if (Number.isNaN(lo[0]) || Number.isNaN(hi[0])) return null;
    return [
      { op: '>=', v: [lo[0], Number.isNaN(lo[1]) ? 0 : lo[1], Number.isNaN(lo[2]) ? 0 : lo[2]] },
      { op: '<=', v: [hi[0], Number.isNaN(hi[1]) ? 0 : hi[1], Number.isNaN(hi[2]) ? 0 : hi[2]] },
    ];
  }
  const out: Comparator[] = [];
  for (const tok of group.trim().split(/\s+/)) {
    const cs = expandComparator(tok);
    if (cs === null) return null;
    out.push(...cs);
  }
  return out;
}

/**
 * Whether `version` satisfies a semver `range` expression — the boundary engine AMI
 * previously lacked (it only had point-to-point {@link compareSemver}). Supports the
 * npm-style forms: caret (`^1.2.0`), tilde (`~1.3`), comparators (`>=1.0.0 <2.0.0`),
 * exact + partial versions (`1.2.3`, `1.2`, `1`), `x`/`*` wildcards (`1.x`, `*`), hyphen
 * ranges (`1.2.3 - 2.0.0`), compound AND (space) and OR (`||`). An empty range matches
 * any valid version; a malformed `version` or a wholly-unparseable range fails closed
 * (false). Prerelease/build tags are compared by their release portion, consistent with
 * {@link compareSemver}. Parity: the reference's `satisfies(version, range)` (utils/semver.ts),
 * implemented dependency-free so it stays a self-contained core building block.
 */
export function satisfies(version: string, range: string): boolean {
  if (typeof version !== 'string' || typeof range !== 'string') return false;
  const ver = versionTriple(version);
  if (Number.isNaN(ver[0])) return false; // not a usable version
  const triple: [number, number, number] = [ver[0], Number.isNaN(ver[1]) ? 0 : ver[1], Number.isNaN(ver[2]) ? 0 : ver[2]];
  const trimmed = range.trim();
  if (trimmed === '' || trimmed === '*' || trimmed === 'x' || trimmed === 'X') return true;
  // OR across `||` groups; within a group every comparator must hold (AND).
  return trimmed.split('||').some(group => {
    const comps = expandAndGroup(group);
    if (comps === null) return false;
    return comps.every(c => testComparator(triple, c));
  });
}

/**
 * Whether `current` (the running AMI version) satisfies a skill's `required` version
 * constraint. An absent/empty `required` imposes no floor (returns true — the graceful
 * default), mirroring the reference's `checkBridgeMinVersion` default "0.0.0".
 *
 * FUNCTIONAL (additive, labeled): `required` now accepts a FULL semver RANGE expression
 * (`^2.0.0`, `>=2 <4`, `2.x || 3.x`, `1.2.3 - 2.0.0`) via {@link satisfies}, not just a
 * single minimum. A plain `X.Y.Z` (no range operator) KEEPS AMI's documented "minimum
 * floor" semantics (`>=`) — a deliberate superset divergence from npm's "exact" reading of
 * a bare version, preserving existing skill `requires: "2.1.0"` manifests — while any
 * operator/wildcard/compound/OR form is evaluated as a true range.
 */
export function isAmiVersionCompatible(required: string | undefined, current: string): boolean {
  if (!required || typeof required !== 'string' || !required.trim()) return true;
  const req = required.trim();
  // A bare version (digits/dots only, no range operator) is a MINIMUM floor (AMI's perk);
  // anything with a range operator/wildcard/compound/OR is a full range expression.
  if (isBareVersion(req)) return compareSemver(current, req) >= 0;
  return satisfies(current, req);
}

/** Whether `s` is a plain `[v]MAJOR[.MINOR[.PATCH]]` version — all-numeric segments, no range
 *  operator/wildcard. Hand-rolled (no nested-quantifier regex) to stay ReDoS-safe. */
function isBareVersion(s: string): boolean {
  const body = s[0] === 'v' || s[0] === 'V' ? s.slice(1) : s;
  const parts = body.split('.');
  if (parts.length < 1 || parts.length > 3) return false;
  return parts.every(p => p.length > 0 && /^\d+$/.test(p));
}

// ---------------------------------------------------------------------------
// URL normalization — resolve GitHub shorthand to raw content URLs
// ---------------------------------------------------------------------------

export function resolveSkillUrl(input: string): string {
  const trimmed = input.trim();

  // A trailing `#ref` fragment selects a git branch/tag/commit for GitHub
  // shorthand (`owner/repo#v1.2.3`), mirroring the reference's `#ref` convention.
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
 * which also matches the apex `example.com`). Mirrors the reference's
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
