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
  fetchedAt: string;
  skills: Array<{ name: string; version?: string }>;
}

// ---------------------------------------------------------------------------
// Path safety — prevent directory traversal in catalog entries
// ---------------------------------------------------------------------------

const UNSAFE_SEGMENT_RE = /[/\\]/;

export function isSafeSegment(segment: string): boolean {
  if (!segment || segment.length === 0) return false;
  if (segment === '.' || segment === '..') return false;
  if (segment.includes('\0')) return false;
  if (UNSAFE_SEGMENT_RE.test(segment)) return false;
  if (segment.length > 255) return false;
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
// Catalog index validation
// ---------------------------------------------------------------------------

export interface CatalogValidationResult {
  valid: boolean;
  errors: string[];
  index?: SkillCatalogIndex;
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

    if (!isSafeSegment(entry.name)) {
      errors.push(`skills[${i}]: unsafe skill name "${entry.name}"`);
      continue;
    }

    if (!Array.isArray(entry.files) || entry.files.length === 0) {
      errors.push(`skills[${i}] "${entry.name}": missing or empty "files" array`);
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

    skills.push({
      name: entry.name,
      version: typeof entry.version === 'string' ? entry.version : undefined,
      description: typeof entry.description === 'string' ? entry.description : undefined,
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
    skills,
  };

  return { valid: true, errors, index };
}

// ---------------------------------------------------------------------------
// URL normalization — resolve GitHub shorthand to raw content URLs
// ---------------------------------------------------------------------------

export function resolveSkillUrl(input: string): string {
  const trimmed = input.trim();

  if (trimmed.startsWith('http://') || trimmed.startsWith('https://')) {
    return trimmed.replace(/\/+$/, '');
  }

  const parts = trimmed.split('/');
  if (parts.length >= 2 && parts[0] && parts[1]) {
    const owner = parts[0];
    const repo = parts[1];
    const subpath = parts.length > 2 ? parts.slice(2).join('/') : '';
    const base = `https://raw.githubusercontent.com/${owner}/${repo}/main`;
    return subpath ? `${base}/${subpath}` : base;
  }

  return trimmed;
}
