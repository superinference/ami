import type { ProviderConfig, ProviderSubsystem } from './types';
import { inferProviderFromApiKey, inferProviderFromBaseUrl, inferProviderFromEnv, isFirstPartyBaseUrl } from './provider';
import { RateLimitTracker } from './rate-limiter';
import type { RateLimitStatus, RawUtilization } from './rate-limiter';
import { CredentialPool } from './credential-pool';
import { ProviderHealthTracker } from './provider-health';
import type { ProviderHealthStats } from './provider-health';
import type { ErrorCategory } from './error-classifier';
import { getModelDisplayName } from './model-registry';

/** Sentinel stored in {@link CoordinatorState} when the model/provider could not
 *  be inferred from the config, api key, base url, or env. Kept as a stable
 *  machine-readable value; the user-facing surfaces read the *Display fields. */
export const UNKNOWN_IDENTIFIER = 'unknown';

/** User-facing phrase shown for a provider that was auto-detected (inferred)
 *  rather than explicitly configured — a clearer fallback than the bare
 *  {@link UNKNOWN_IDENTIFIER} sentinel on a `/status` or model line. */
export const AUTO_DETECTED_PROVIDER_DISPLAY = 'auto-detected provider';

/** User-facing phrase shown for a model id that matched no known family and had
 *  no explicit id — a clearer fallback than the bare {@link UNKNOWN_IDENTIFIER}. */
export const UNIDENTIFIED_MODEL_DISPLAY = 'unidentified model';

/** Backpressure signal for a multi-credential pool, returned by
 *  {@link ProviderCoordinator.getBackpressure}. Lets a caller wait for the
 *  soonest reset instead of hammering an exhausted/throttled key. */
export interface CredentialBackpressure {
  /** True only when a pool exists AND every live credential is currently in
   *  cooldown (so {@link ProviderCoordinator.getConfig} would hand back the
   *  last-resort throttled primary key). False when a key is available, or when
   *  there is no pool (single-credential mode has no exhaustion concept). */
  exhausted: boolean;
  /** Soonest epoch (ms) a credential frees up: `Date.now()` when one is already
   *  available, the earliest `cooldownUntil` when all are throttled, or `null`
   *  when there is no pool or every credential is permanently dead. */
  nextAvailableAt: number | null;
}

export interface CoordinatorState {
  currentModel: string;
  currentProvider: string;
  /** Friendly, human-readable name for {@link currentModel} ("Sonnet 4.6") via
   *  the canonical {@link getModelDisplayName}; {@link UNIDENTIFIED_MODEL_DISPLAY}
   *  when the model is the unknown sentinel. The CLI renders this instead of the
   *  raw id (core stays formatting-free beyond the shared registry helper). */
  currentModelDisplay: string;
  /** User-facing provider label for {@link currentProvider};
   *  {@link AUTO_DETECTED_PROVIDER_DISPLAY} when the provider is the unknown
   *  sentinel (rather than surfacing the bare 'unknown' literal). */
  currentProviderDisplay: string;
  usedFallback: boolean;
  requestCount: number;
  errorCount: number;
}

/** A recovery transition emitted when sustained success on a fallback model
 *  lets the coordinator step the active model all the way back to the primary.
 *  Returned by {@link ProviderCoordinator.recordSuccess} so the engine can
 *  announce "returned to <primary> after recovery" — the mirror of the
 *  user-visible fallback notification (parity: the reference announces both the
 *  demotion to and the recovery from a degraded-capacity model). */
export interface ModelRecovery {
  /** The fallback model that was active when recovery completed. */
  from: string;
  /** The primary model the coordinator reverted to. */
  to: string;
}

const FALLBACK_COOLDOWN = 3;

export class ProviderCoordinator {
  private subsystem: ProviderSubsystem;
  // The stored state holds only the raw machine-readable fields; the friendly
  // *Display fields are derived on demand in getState() (never persisted).
  private state: Omit<CoordinatorState, 'currentModelDisplay' | 'currentProviderDisplay'>;
  private fallbackIndex = 0; // 0 = primary; 1..N = position in the fallback chain
  private fallbackSuccessCount = 0;
  private stickyFallback = false;
  private rateLimits: RateLimitTracker;
  private health = new ProviderHealthTracker();
  private credentialPool: CredentialPool | null = null;
  private activeCredentialId: string | null = null;

  constructor(subsystem: ProviderSubsystem, additionalKeys?: string[]) {
    this.subsystem = subsystem;
    // Honor the engine-supplied rate-limit tuning (warn/error thresholds +
    // proactive early-warning ladder) so the knob is reachable end-to-end rather
    // than always defaulting; falls back to provider defaults when absent.
    this.rateLimits = new RateLimitTracker(subsystem.rateLimit);
    const inferred = this.inferProvider(subsystem.primary);
    this.state = {
      currentModel: subsystem.primary.model || inferred?.defaultModel || UNKNOWN_IDENTIFIER,
      currentProvider: inferred?.provider || subsystem.primary.provider || UNKNOWN_IDENTIFIER,
      usedFallback: false,
      requestCount: 0,
      errorCount: 0,
    };

    if (additionalKeys && additionalKeys.length > 0) {
      this.credentialPool = new CredentialPool('round_robin');
      this.credentialPool.addKey(subsystem.primary.apiKey);
      for (const key of additionalKeys) {
        this.credentialPool.addKey(key);
      }
    }
  }

  /** Ordered fallback model chain (normalized; empty if none). */
  private get chain(): string[] {
    if (this.subsystem.fallbackModels && this.subsystem.fallbackModels.length) return this.subsystem.fallbackModels;
    return this.subsystem.fallbackModel ? [this.subsystem.fallbackModel] : [];
  }

  getConfig(): ProviderConfig {
    const activeModel = this.fallbackIndex > 0 ? this.chain[this.fallbackIndex - 1] : undefined;
    const base = activeModel
      ? { ...this.subsystem.primary, model: activeModel }
      : this.subsystem.primary;

    if (this.credentialPool) {
      const cred = this.credentialPool.acquire();
      if (cred) {
        this.activeCredentialId = cred.id;
        return { ...base, apiKey: cred.apiKey };
      }
    }

    return base;
  }

  getCompactionConfig(): ProviderConfig {
    const base: ProviderConfig = this.subsystem.compactionModel
      ? { ...this.subsystem.primary, model: this.subsystem.compactionModel }
      : { ...this.subsystem.primary };
    // Populate fallbackModel from the fallback chain so the compaction-refusal
    // retry path in the context manager actually has a model to fall back to.
    const chain = this.subsystem.fallbackModels?.length
      ? this.subsystem.fallbackModels
      : (this.subsystem.fallbackModel ? [this.subsystem.fallbackModel] : []);
    const fb = chain.find(m => m && m !== base.model);
    if (fb) base.fallbackModel = fb;
    return base;
  }

  getArchitectConfig(): ProviderConfig {
    return this.subsystem.architectModel
      ? { ...this.subsystem.primary, model: this.subsystem.architectModel }
      : { ...this.subsystem.primary };
  }

  getState(): CoordinatorState {
    return {
      ...this.state,
      currentModelDisplay: this.modelDisplay(),
      currentProviderDisplay: this.providerDisplay(),
    };
  }

  /** Friendly model name for status/introspection surfaces. Consolidates with the
   *  canonical {@link getModelDisplayName} registry helper (no parallel table);
   *  maps the unknown sentinel to a clearer phrase than the bare literal. */
  private modelDisplay(): string {
    const raw = this.state.currentModel;
    return raw === UNKNOWN_IDENTIFIER ? UNIDENTIFIED_MODEL_DISPLAY : getModelDisplayName(raw);
  }

  /** User-facing provider label; maps the unknown sentinel to an "auto-detected"
   *  phrase rather than surfacing the bare 'unknown' literal. */
  private providerDisplay(): string {
    const raw = this.state.currentProvider;
    return raw === UNKNOWN_IDENTIFIER ? AUTO_DETECTED_PROVIDER_DISPLAY : raw;
  }

  /**
   * Backpressure boundary for a multi-credential pool. {@link getConfig} silently
   * falls through to the primary key when {@link CredentialPool.acquire} returns
   * null, so it cannot tell a healthy key from the last-resort throttled one. This
   * exposes that distinction: the engine can consult it after a 429 to WAIT until
   * {@link CredentialBackpressure.nextAvailableAt} instead of immediately hammering
   * an exhausted key with no backoff (parity: the reference waits out the window
   * reset rather than reusing an exhausted credential). Single-credential mode has
   * no pool and is never "exhausted" (nothing to back off to).
   */
  getBackpressure(): CredentialBackpressure {
    if (!this.credentialPool) return { exhausted: false, nextAvailableAt: null };
    return {
      exhausted: this.credentialPool.availableCount === 0,
      nextAvailableAt: this.credentialPool.nextAvailableAt(),
    };
  }

  recordRequest(): void {
    this.state.requestCount++;
  }

  recordRequestLatency(latencyMs: number): void {
    this.health.recordRequest(latencyMs);
  }

  getHealthStats(): ProviderHealthStats {
    return this.health.getStats();
  }

  recordError(category?: ErrorCategory): boolean {
    this.state.errorCount++;
    this.health.recordError(0, category);
    // Step one position down the fallback chain, if any remain.
    if (this.fallbackIndex < this.chain.length) {
      this.fallbackIndex++;
      this.state.usedFallback = true;
      this.state.currentModel = this.chain[this.fallbackIndex - 1];
      return true;
    }
    return false;
  }

  /**
   * Record a successful request. When the active model is a fallback (and the
   * fallback is not sticky), a run of {@link FALLBACK_COOLDOWN} consecutive
   * successes reverts the coordinator to the primary model. In that event a
   * {@link ModelRecovery} describing the `fallback → primary` transition is
   * returned so the engine can surface a user-visible recovery notification
   * (mirror of the fallback notification); otherwise `null`.
   */
  recordSuccess(): ModelRecovery | null {
    if (this.fallbackIndex > 0 && !this.stickyFallback) {
      this.fallbackSuccessCount++;
      if (this.fallbackSuccessCount >= FALLBACK_COOLDOWN) {
        const from = this.state.currentModel;
        const to = this.subsystem.primary.model || UNKNOWN_IDENTIFIER;
        this.fallbackIndex = 0;
        this.fallbackSuccessCount = 0;
        this.state.currentModel = to;
        return { from, to };
      }
    }
    return null;
  }

  setStickyFallback(sticky: boolean): void {
    this.stickyFallback = sticky;
  }

  isAnthropicProvider(): boolean {
    const config = this.getConfig();
    const modelId = config.model || '';
    return modelId.startsWith('claude') ||
      this.isAnthropicBaseUrl(config.baseUrl) ||
      config.provider === 'anthropic';
  }

  /** Host-based (parsed) classification of an Anthropic first-party endpoint.
   *  A raw `baseUrl.includes('anthropic.com')` is host-spoofable — it matches
   *  `anthropic.com.evil.example`, `not-anthropic.com`, and
   *  `evil.example/?x=anthropic.com` alike. Delegates to the shared
   *  {@link isFirstPartyBaseUrl} host-boundary matcher so only `anthropic.com`
   *  and its genuine subdomains qualify (single canonical implementation). */
  private isAnthropicBaseUrl(baseUrl: string | undefined): boolean {
    return isFirstPartyBaseUrl(baseUrl, ['anthropic.com']);
  }

  hasFallback(): boolean {
    return this.chain.length > 0;
  }

  /** Replace the active primary provider config and reset to it (fallbackIndex 0).
   *  Used by the engine for in-session recovery (e.g. OpenRouter ZDR auto-switch),
   *  refreshing the reported model/provider so status surfaces the change. */
  setPrimaryConfig(config: ProviderConfig): void {
    this.subsystem.primary = config;
    this.fallbackIndex = 0;
    const inferred = this.inferProvider(config);
    this.state.currentModel = config.model || inferred?.defaultModel || UNKNOWN_IDENTIFIER;
    this.state.currentProvider = inferred?.provider || config.provider || UNKNOWN_IDENTIFIER;
  }

  updateRateLimits(headers: Record<string, string>): void {
    this.rateLimits.update(headers);
  }

  getRateLimitStatus(): RateLimitStatus {
    return this.rateLimits.getStatus();
  }

  /**
   * Raw per-window rate-limit utilization from the most recent response (fraction
   * in [0, 1] + reset instant per bucket), tracked on every response rather than
   * only when a warning fires. Surfaced to the statusline so a ported script can
   * read `.rate_limit.requests.utilization` (parity: the reference's getRawUtilization).
   */
  getRawUtilization(): RawUtilization {
    return this.rateLimits.getRawUtilization();
  }

  /**
   * Put the active credential into cooldown. When the caller omits an explicit
   * cooldown, prefer the SERVER-DIRECTED delay: the coordinator already ingests
   * each response's headers via {@link updateRateLimits}, so the latest parsed
   * `Retry-After` (ms) is honored verbatim by the pool (clamped, no escalation)
   * instead of the fixed escalating default. This mirrors the reference's
   * getRetryDelay, which uses the server `retry-after` header as the delay and
   * only falls back to exponential backoff when the server did not direct one.
   */
  markCredentialExhausted(cooldownMs?: number): void {
    if (this.credentialPool && this.activeCredentialId) {
      if (cooldownMs === undefined) {
        const serverRetryAfterMs = this.rateLimits.getStatus().retryAfterMs;
        if (serverRetryAfterMs !== null && serverRetryAfterMs > 0) {
          // Authoritative server directive — used verbatim (clamped) by the pool.
          this.credentialPool.markExhausted(this.activeCredentialId, { retryAfterMs: serverRetryAfterMs });
        } else {
          // No server directive: fall back to the pool's escalating default.
          this.credentialPool.markExhausted(this.activeCredentialId);
        }
      } else {
        this.credentialPool.markExhausted(this.activeCredentialId, cooldownMs);
      }
      this.activeCredentialId = null;
    }
  }

  get availableCredentials(): number {
    return this.credentialPool?.availableCount ?? 1;
  }

  private inferProvider(config: ProviderConfig) {
    return inferProviderFromApiKey(config.apiKey)
      || (config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl) : null)
      || inferProviderFromEnv();
  }
}
