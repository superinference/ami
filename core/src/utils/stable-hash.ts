// Canonical deep-key-sorted serialization + content checksum.
//
// This is the ONE shared implementation (AGENTS.md No-Duplication Rule): the
// tool-repetition signature hasher and the skill-catalog content-cache checksum
// both need a key-order-insensitive serialization, and the reference computes the
// same thing (`sortKeysDeep` + `JSON.stringify` → sha256) in its policyLimits /
// remoteManagedSettings HTTP-cache layers. Rather than re-spell the recursion per
// site, every caller imports from here.

import { createHash } from 'crypto';

/**
 * Stable JSON stringify: object keys are emitted in sorted order (recursively),
 * so values that are structurally identical but differ only in key order / inner
 * whitespace serialize to the exact same string. Arrays keep their order
 * (position is semantically significant). `null`/`undefined` serialize to their
 * `String(...)` form so the serializer never throws on a sparse input.
 */
export function stableStringify(value: unknown): string {
  if (value === null || value === undefined) return String(value);
  if (typeof value !== 'object') return JSON.stringify(value) ?? String(value);
  if (Array.isArray(value)) {
    return '[' + value.map((v) => stableStringify(v)).join(',') + ']';
  }
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringify(obj[k])).join(',') + '}';
}

/**
 * Deterministic `sha256:<hex>` content checksum over the deep-key-sorted
 * serialization of `value`. Two structurally-equal inputs (any key order) yield
 * the same checksum, so it is a stable HTTP content-cache key — identical in
 * spirit to the reference's `computeChecksum` (`sha256:` + hex). Used to skip
 * re-downloading a remote catalog whose content has not changed.
 */
export function stableChecksum(value: unknown): string {
  return 'sha256:' + createHash('sha256').update(stableStringify(value)).digest('hex');
}
