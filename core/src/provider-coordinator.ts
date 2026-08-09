import type { ProviderConfig, ProviderSubsystem } from './types';
import { inferProviderFromApiKey, inferProviderFromBaseUrl, inferProviderFromEnv } from './provider';
import { RateLimitTracker } from './rate-limiter';
import type { RateLimitStatus } from './rate-limiter';
import { CredentialPool } from './credential-pool';

export interface CoordinatorState {
  currentModel: string;
  currentProvider: string;
  usedFallback: boolean;
  requestCount: number;
  errorCount: number;
}

const FALLBACK_COOLDOWN = 3;

export class ProviderCoordinator {
  private subsystem: ProviderSubsystem;
  private state: CoordinatorState;
  private usingFallback = false;
  private fallbackSuccessCount = 0;
  private rateLimits = new RateLimitTracker();
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

  getConfig(): ProviderConfig {
    const base = this.usingFallback && this.subsystem.fallbackModel
      ? { ...this.subsystem.primary, model: this.subsystem.fallbackModel }
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
    if (this.subsystem.compactionModel) {
      return { ...this.subsystem.primary, model: this.subsystem.compactionModel };
    }
    return this.subsystem.primary;
  }

  getState(): CoordinatorState {
    return { ...this.state };
  }

  recordRequest(): void {
    this.state.requestCount++;
  }

  recordError(): boolean {
    this.state.errorCount++;
    if (!this.usingFallback && this.subsystem.fallbackModel) {
      this.usingFallback = true;
      this.state.usedFallback = true;
      this.state.currentModel = this.subsystem.fallbackModel;
      return true;
    }
    return false;
  }

  recordSuccess(): void {
    if (this.usingFallback) {
      this.fallbackSuccessCount++;
      if (this.fallbackSuccessCount >= FALLBACK_COOLDOWN) {
        this.usingFallback = false;
        this.fallbackSuccessCount = 0;
        this.state.currentModel = this.subsystem.primary.model || 'unknown';
      }
    }
  }

  isAnthropicProvider(): boolean {
    const config = this.getConfig();
    const modelId = config.model || '';
    return modelId.startsWith('claude') ||
      (config.baseUrl?.includes('anthropic.com') ?? false) ||
      config.provider === 'anthropic';
  }

  hasFallback(): boolean {
    return !!this.subsystem.fallbackModel;
  }

  updateRateLimits(headers: Record<string, string>): void {
    this.rateLimits.update(headers);
  }

  getRateLimitStatus(): RateLimitStatus {
    return this.rateLimits.getStatus();
  }

  markCredentialExhausted(cooldownMs?: number): void {
    if (this.credentialPool && this.activeCredentialId) {
      this.credentialPool.markExhausted(this.activeCredentialId, cooldownMs);
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
