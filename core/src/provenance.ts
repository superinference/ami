export type ProvenanceTier = 'builtin' | 'managed' | 'user' | 'unknown';

export interface ProvenanceRecord {
  name: string;
  tier: ProvenanceTier;
  source: string;
  verifiedAt: number;
  signature?: string;
  integrity?: 'verified' | 'unverified' | 'failed';
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

export class ProvenanceTracker {
  private _records = new Map<string, ProvenanceRecord>();
  private _managedSources: Set<string>;
  private _builtinSources: Set<string>;
  private _untrustedAccess: Array<{ name: string; tier: ProvenanceTier; timestamp: number }> = [];

  constructor(managedSources: Set<string> = new Set(), builtinSources: Set<string> = new Set()) {
    this._managedSources = managedSources;
    this._builtinSources = builtinSources;
  }

  register(name: string, source: string): ProvenanceRecord {
    const tier = resolveProvenance(name, source, this._managedSources, this._builtinSources);
    const record: ProvenanceRecord = {
      name,
      tier,
      source,
      verifiedAt: Date.now(),
      integrity: isProvenanceTrusted(tier) ? 'verified' : 'unverified',
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
