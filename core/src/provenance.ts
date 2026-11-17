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
export type ProvenanceIntegrity = 'verified' | 'attested' | 'unverified' | 'failed';

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
export type ProvenanceSeverity = 'ok' | 'caution' | 'warning';

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
 * when nothing is untrusted. Mirrors Claude's trust-dialog guidance so surfaces
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
  return {
    tiers,
    total: records.length,
    untrustedCount,
    elevatedRiskCount,
    reviewCount,
    warning: provenanceWarning(untrustedCount),
    records,
  };
}
