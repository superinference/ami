import { createHash, timingSafeEqual } from 'node:crypto';

export const PROVENANCE_TIERS = ['builtin', 'managed', 'user', 'unknown'] as const;
export type ProvenanceTier = (typeof PROVENANCE_TIERS)[number];

/**
 * Integrity states a source can be in.
 *
 * - `verified` — content integrity was CRYPTOGRAPHICALLY checked (a hash/signature
 *   matched). Only claimed after a real check; never from origin tier alone.
 * - `attested` — the source is trusted by its origin {@link ProvenanceTier} (builtin/
 *   managed), but its content integrity was NOT independently verified. This is the
 *   honest label for tier-only trust — it avoids the false-assurance of claiming
 *   `verified` when no content check happened.
 * - `unverified` — an untrusted-origin source with no content check.
 * - `failed` — a content/signature check was attempted and did NOT match.
 */
export const PROVENANCE_INTEGRITYS = ['verified', 'attested', 'unverified', 'failed'] as const;
export type ProvenanceIntegrity = (typeof PROVENANCE_INTEGRITYS)[number];

/**
 * Concrete risky capabilities a source can introduce. These are origin-independent
 * signals describing WHAT a source can do, so a trust/permission surface can reason
 * about capability risk separately from the coarse origin {@link ProvenanceTier}.
 */
export const RISK_CAPABILITIES = ['hasHooks', 'hasBashAllow', 'hasDangerousEnv'] as const;
export type RiskCapabilityName = (typeof RISK_CAPABILITIES)[number];

/** Normalized (always-present) risk-capability flags stored on a record. */
export type RiskCapabilities = Record<RiskCapabilityName, boolean>;

/**
 * Optional, partial risk-capability signals supplied by a caller when it knows
 * what a source's settings grant (hooks, bash allow-rules, dangerous env vars).
 * All flags default to `false` when absent — an unspecified capability is never
 * assumed present.
 */
export interface SourceCapabilities {
  hasHooks?: boolean;
  hasBashAllow?: boolean;
  hasDangerousEnv?: boolean;
}

function normalizeCapabilities(caps?: SourceCapabilities): RiskCapabilities {
  return {
    hasHooks: caps?.hasHooks === true,
    hasBashAllow: caps?.hasBashAllow === true,
    hasDangerousEnv: caps?.hasDangerousEnv === true,
  };
}

export interface ProvenanceRecord {
  name: string;
  tier: ProvenanceTier;
  source: string;
  verifiedAt: number;
  signature?: string;
  integrity?: ProvenanceIntegrity;
  /**
   * Concrete risky capabilities this source introduces, as reported at
   * registration. Populated from the source's settings by the caller; defaults
   * to all-`false` when the caller supplied nothing.
   */
  capabilities: RiskCapabilities;
}

/** Human-readable severity of a trust/integrity signal, for display surfaces. */
export const PROVENANCE_SEVERITYS = ['ok', 'caution', 'warning'] as const;
export type ProvenanceSeverity = (typeof PROVENANCE_SEVERITYS)[number];

export interface ProvenanceTierInfo {
  label: string;
  description: string;
  severity: ProvenanceSeverity;
  trusted: boolean;
}

export interface ProvenanceIntegrityInfo {
  label: string;
  description: string;
  severity: ProvenanceSeverity;
}

/**
 * Human-readable trust guidance per {@link ProvenanceTier}. Surfaces like
 * `/provenance` and `/status` render these labels/descriptions instead of the
 * bare enum strings so the user understands what each tier means and its risk.
 */
export const PROVENANCE_TIER_INFO: Record<ProvenanceTier, ProvenanceTierInfo> = {
  builtin: {
    label: 'Built-in',
    description: 'Shipped with SuperInference. Fully trusted.',
    severity: 'ok',
    trusted: true,
  },
  managed: {
    label: 'Managed',
    description: 'Provisioned by managed/organization policy. Trusted.',
    severity: 'ok',
    trusted: true,
  },
  user: {
    label: 'User',
    description:
      'Installed by you from an external source. Not independently verified — review before granting elevated capabilities.',
    severity: 'caution',
    trusted: false,
  },
  unknown: {
    label: 'Unknown',
    description: 'Origin could not be determined. Treat as untrusted.',
    severity: 'warning',
    trusted: false,
  },
};

/** Human-readable guidance per integrity state, for display surfaces. */
export const PROVENANCE_INTEGRITY_INFO: Record<ProvenanceIntegrity, ProvenanceIntegrityInfo> = {
  verified: {
    label: 'Verified',
    description: 'Content integrity cryptographically verified against an expected hash.',
    severity: 'ok',
  },
  attested: {
    label: 'Attested',
    description:
      'Trusted by origin tier; content integrity not independently verified.',
    severity: 'caution',
  },
  unverified: {
    label: 'Unverified',
    description: 'Integrity not checked — source is untrusted.',
    severity: 'caution',
  },
  failed: {
    label: 'Failed',
    description: 'Integrity check failed — do not use.',
    severity: 'warning',
  },
};

/**
 * Deterministic display order for integrity states (best → worst), used by the
 * `/provenance` legend so the user sees the full integrity vocabulary in a stable
 * order rather than relying on object-key enumeration.
 */
export const PROVENANCE_INTEGRITY_ORDER = [
  'verified',
  'attested',
  'unverified',
  'failed',
] as const satisfies ReadonlyArray<ProvenanceIntegrity>;

/** Descriptor for {@link describeTier}/display: the tier info for a tier. */
export function describeTier(tier: ProvenanceTier): ProvenanceTierInfo {
  return PROVENANCE_TIER_INFO[tier];
}

/** Descriptor for an integrity state, for display surfaces. */
export function describeIntegrity(integrity: ProvenanceIntegrity): ProvenanceIntegrityInfo {
  return PROVENANCE_INTEGRITY_INFO[integrity];
}

/**
 * Build human-readable warning copy for a count of untrusted sources, or `null`
 * when nothing is untrusted. Mirrors the reference's trust-dialog guidance so surfaces
 * can explain the risk instead of showing an opaque count.
 */
export function provenanceWarning(untrustedCount: number): string | null {
  if (untrustedCount <= 0) return null;
  const noun = untrustedCount === 1 ? 'source' : 'sources';
  return (
    `${untrustedCount} untrusted ${noun} loaded. Untrusted sources are not verified by ` +
    'SuperInference and may behave unexpectedly — review them before granting elevated ' +
    'capabilities (bash, hooks, environment).'
  );
}

export function resolveProvenance(
  name: string,
  source: string,
  managedSources: Set<string>,
  builtinSources: Set<string>,
): ProvenanceTier {
  if (builtinSources.has(source)) return 'builtin';
  if (managedSources.has(source)) return 'managed';
  if (source && source !== 'unknown') return 'user';
  return 'unknown';
}

export function isProvenanceTrusted(tier: ProvenanceTier): boolean {
  return tier === 'builtin' || tier === 'managed';
}

/**
 * Compute the canonical content signature (sha256 hex) for an integrity check.
 * Pure and deterministic — identical bytes always yield the same signature — so a
 * source's content can be compared against an expected hash or a prior baseline.
 */
export function computeContentSignature(content: string | Uint8Array): string {
  return createHash('sha256').update(content).digest('hex');
}

/**
 * Constant-time equality for two hex signature strings. Unequal lengths are an
 * immediate mismatch (timingSafeEqual requires equal-length buffers); equal
 * lengths are compared without an early-exit side channel.
 */
function signaturesMatch(a: string, b: string): boolean {
  const ab = Buffer.from(a, 'utf8');
  const bb = Buffer.from(b, 'utf8');
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

/** Outcome of a content-integrity comparison. */
export interface IntegrityCheckResult {
  /** 'verified' when the content matched the expected signature, else 'failed'. */
  integrity: 'verified' | 'failed';
  /** The signature actually computed from the supplied content. */
  actual: string;
  /** The signature the content was compared against. */
  expected: string;
}

/**
 * Compare content against an externally-supplied expected signature. Returns
 * 'verified' ONLY on an exact constant-time match, 'failed' otherwise. This is
 * the real cryptographic check that the 'verified' integrity state is reserved
 * for — it is never derived from the origin tier. A 'failed' result is the
 * tampering/MITM analog of the reference's changed-host-key detection: the content does
 * not match the trusted reference, so it must not be silently trusted.
 */
export function verifyContentSignature(
  content: string | Uint8Array,
  expectedSignature: string,
): IntegrityCheckResult {
  const actual = computeContentSignature(content);
  return {
    integrity: signaturesMatch(actual, expectedSignature) ? 'verified' : 'failed',
    actual,
    expected: expectedSignature,
  };
}

/** The subset of risk capabilities a record actually introduces. */
export function elevatedCapabilities(record: ProvenanceRecord): RiskCapabilityName[] {
  return RISK_CAPABILITIES.filter(k => record.capabilities[k]);
}

/** True when a record introduces at least one risky capability. */
export function hasElevatedCapabilities(record: ProvenanceRecord): boolean {
  return elevatedCapabilities(record).length > 0;
}

export interface ProvenanceRiskAssessment {
  tier: ProvenanceTier;
  /** Origin-based trust (tier alone). */
  tierTrusted: boolean;
  /** Concrete risky capabilities this source introduces. */
  elevatedCapabilities: RiskCapabilityName[];
  /**
   * Whether this source warrants explicit review: any untrusted origin, OR a
   * trusted origin that pulls in concrete risky capabilities. This is what lets
   * the trust guard reflect concrete risk, not just tier.
   */
  requiresReview: boolean;
}

/**
 * Combine a record's origin tier with its concrete risk capabilities into a
 * single assessment. Unlike {@link isProvenanceTrusted} (tier-only), this flags
 * a trusted-origin source for review when it introduces risky capabilities.
 */
export function assessProvenanceRisk(record: ProvenanceRecord): ProvenanceRiskAssessment {
  const elevated = elevatedCapabilities(record);
  const tierTrusted = isProvenanceTrusted(record.tier);
  return {
    tier: record.tier,
    tierTrusted,
    elevatedCapabilities: elevated,
    requiresReview: !tierTrusted || elevated.length > 0,
  };
}

export class ProvenanceTracker {
  private _records = new Map<string, ProvenanceRecord>();
  private _managedSources: Set<string>;
  private _builtinSources: Set<string>;
  private _untrustedAccess: Array<{ name: string; tier: ProvenanceTier; timestamp: number }> = [];

  constructor(managedSources: Set<string> = new Set(), builtinSources: Set<string> = new Set()) {
    this._managedSources = managedSources;
    this._builtinSources = builtinSources;
  }

  register(name: string, source: string, capabilities?: SourceCapabilities): ProvenanceRecord {
    const tier = resolveProvenance(name, source, this._managedSources, this._builtinSources);
    const record: ProvenanceRecord = {
      name,
      tier,
      source,
      verifiedAt: Date.now(),
      // Honest integrity: a trusted origin tier yields 'attested' (tier-only trust),
      // NOT 'verified' — no content hash/signature check is performed at registration,
      // so claiming 'verified' here would be a false integrity signal. 'verified' is
      // reserved for an actual cryptographic check; untrusted origins are 'unverified'.
      integrity: isProvenanceTrusted(tier) ? 'attested' : 'unverified',
      capabilities: normalizeCapabilities(capabilities),
    };
    this._records.set(name, record);
    return record;
  }

  /**
   * Record a baseline content signature for an already-registered source WITHOUT
   * changing its integrity state. Establishes the reference a later
   * {@link verifyIntegrity} compares against (drift detection); recording a
   * baseline is not a trusted-hash match, so it never claims 'verified'. Returns
   * the record's current integrity, or undefined when the name is unknown.
   */
  recordSignature(name: string, content: string | Uint8Array): ProvenanceIntegrity | undefined {
    const record = this._records.get(name);
    if (!record) return undefined;
    record.signature = computeContentSignature(content);
    return record.integrity;
  }

  /**
   * Verify a registered source's content against an expected signature and persist
   * the result on its record. This is the ONLY path that can produce the honest
   * 'verified' (content matched a trusted hash) or 'failed' (mismatch — possible
   * tampering) integrity states that {@link register} alone, deriving integrity
   * from the origin tier, can never claim. The record's `signature` is updated to
   * the freshly-computed hash. A 'failed' check also logs an untrusted-access entry
   * so the tampering is auditable via {@link getUntrustedAccess}. Returns the new
   * integrity, or undefined when the name is unknown.
   */
  verifyIntegrity(
    name: string,
    content: string | Uint8Array,
    expectedSignature: string,
  ): ProvenanceIntegrity | undefined {
    const record = this._records.get(name);
    if (!record) return undefined;
    const result = verifyContentSignature(content, expectedSignature);
    record.signature = result.actual;
    record.integrity = result.integrity;
    if (result.integrity === 'failed') {
      this._untrustedAccess.push({ name, tier: record.tier, timestamp: Date.now() });
    }
    return record.integrity;
  }

  getRecord(name: string): ProvenanceRecord | undefined {
    return this._records.get(name);
  }

  getTier(name: string): ProvenanceTier {
    return this._records.get(name)?.tier ?? 'unknown';
  }

  isTrusted(name: string): boolean {
    return isProvenanceTrusted(this.getTier(name));
  }

  recordUntrustedAccess(name: string): void {
    const tier = this.getTier(name);
    if (!isProvenanceTrusted(tier)) {
      this._untrustedAccess.push({ name, tier, timestamp: Date.now() });
    }
  }

  getUntrustedAccess(): ReadonlyArray<{ name: string; tier: ProvenanceTier; timestamp: number }> {
    return this._untrustedAccess;
  }

  getAllRecords(): Map<string, ProvenanceRecord> {
    return new Map(this._records);
  }

  getRecordsByTier(tier: ProvenanceTier): ProvenanceRecord[] {
    return [...this._records.values()].filter(r => r.tier === tier);
  }

  clear(): void {
    this._records.clear();
    this._untrustedAccess = [];
  }
}

export interface ProvenanceSummary {
  tiers: Record<ProvenanceTier, number>;
  total: number;
  untrustedCount: number;
  /** Records that introduce at least one risky capability. */
  elevatedRiskCount: number;
  /** Records that warrant review (untrusted origin or elevated capabilities). */
  reviewCount: number;
  /**
   * Per-integrity-state counts across all records (records with no integrity set
   * are not counted). Surfaces how many sources passed ('verified') or FAILED a
   * real content check so /status can flag tampering, not just tier trust.
   */
  integrity: Record<ProvenanceIntegrity, number>;
  /** Records whose content integrity check failed — a tampering signal. */
  failedIntegrityCount: number;
  /** Human-readable warning copy when untrusted sources exist, else `null`. */
  warning: string | null;
  records: ProvenanceRecord[];
}

/**
 * Reduce a {@link ProvenanceTracker} to a render-ready summary for surfaces
 * like /status: per-tier counts, total records, untrusted count, concrete
 * risk-capability accounting, human-readable warning copy, and the underlying
 * records. Pure — no I/O, safe to call on every status read.
 */
export function summarizeProvenance(tracker: ProvenanceTracker): ProvenanceSummary {
  const records = [...tracker.getAllRecords().values()];
  const tiers: Record<ProvenanceTier, number> = { builtin: 0, managed: 0, user: 0, unknown: 0 };
  for (const r of records) tiers[r.tier]++;
  const untrustedCount = records.filter(r => !isProvenanceTrusted(r.tier)).length;
  const elevatedRiskCount = records.filter(hasElevatedCapabilities).length;
  const reviewCount = records.filter(r => assessProvenanceRisk(r).requiresReview).length;
  const integrity: Record<ProvenanceIntegrity, number> = {
    verified: 0,
    attested: 0,
    unverified: 0,
    failed: 0,
  };
  for (const r of records) if (r.integrity) integrity[r.integrity]++;
  return {
    tiers,
    total: records.length,
    untrustedCount,
    elevatedRiskCount,
    reviewCount,
    integrity,
    failedIntegrityCount: integrity.failed,
    warning: provenanceWarning(untrustedCount),
    records,
  };
}
