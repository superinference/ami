import type { ProviderConfig, ProviderSubsystem } from './types';
import { inferProviderFromApiKey, inferProviderFromBaseUrl, inferProviderFromEnv, isFirstPartyBaseUrl } from './provider';
import { RateLimitTracker } from './rate-limiter';
import type { RateLimitStatus, RawUtilization } from './rate-limiter';
import { CredentialPool } from './credential-pool';
import { ProviderHealthTracker } from './provider-health';
import type { ProviderHealthStats } from './provider-health';

export interface CoordinatorState {
  currentModel: string;
  currentProvider: string;
  usedFallback: boolean;
  requestCount: number;
  errorCount: number;
}

/** A recovery transition emitted when sustained success on a fallback model
 *  lets the coordinator step the active model all the way back to the primary.
 *  Returned by {@link ProviderCoordinator.recordSuccess} so the engine can
 *  announce "returned to <primary> after recovery" — the mirror of the
 *  user-visible fallback notification (parity: Claude Code announces both the
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
  private state: CoordinatorState;
  private fallbackIndex = 0; // 0 = primary; 1..N = position in the fallback chain
  private fallbackSuccessCount = 0;
  private stickyFallback = false;
  private rateLimits = new RateLimitTracker();
  private health = new ProviderHealthTracker();
  private credentialPool: CredentialPool | null = null;
  private activeCredentialId: string | null = null;

  constructor(subsystem: ProviderSubsystem, additionalKeys?: string[]) {
    this.subsystem = subsystem;
    const inferred = this.inferProvider(subsystem.primary);
    this.state = {
      currentModel: subsystem.primary.model || inferred?.defaultModel || 'unknown',
      currentProvider: inferred?.provider || subsystem.primary.provider || 'unknown',
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
    return { ...this.state };
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

  recordError(): boolean {
    this.state.errorCount++;
    this.health.recordError();
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
        const to = this.subsystem.primary.model || 'unknown';
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
    this.state.currentModel = config.model || inferred?.defaultModel || 'unknown';
    this.state.currentProvider = inferred?.provider || config.provider || 'unknown';
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
   * read `.rate_limit.requests.utilization` (parity: Claude's getRawUtilization).
   */
  getRawUtilization(): RawUtilization {
    return this.rateLimits.getRawUtilization();
  }

  /**
   * Put the active credential into cooldown. When the caller omits an explicit
   * cooldown, prefer the SERVER-DIRECTED delay: the coordinator already ingests
   * each response's headers via {@link updateRateLimits}, so the latest parsed
   * `Retry-After` (ms) is honored verbatim by the pool (clamped, no escalation)
   * instead of the fixed escalating default. This mirrors Claude Code's
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
