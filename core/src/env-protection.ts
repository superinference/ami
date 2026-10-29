/**
 * Pinned environment variables that must not be modified by user-tier code.
 * Based on CC's sec-default plugin behavior.
 */

// Policy and sign-in variables — control where policy and auth are read from
export const POLICY_VARIABLES = new Set([
  'SUPERINFERENCE_MANAGED_SETTINGS_PATH',
  'SUPERINFERENCE_REMOTE_SETTINGS_PATH',
  'SUPERINFERENCE_MOCK_REMOTE_SETTINGS',
  'SUPERINFERENCE_DISABLE_ADMIN_ENV_UNION',
  'SUPERINFERENCE_CONFIG_DIR',
  'SUPERINFERENCE_SECURESTORAGE_CONFIG_DIR',
  'USE_LOCAL_OAUTH',
  'USE_STAGING_OAUTH',
  'SUPERINFERENCE_LOCAL_OAUTH_API_BASE',
]);

// Code-loading variables — can inject arbitrary code into child processes
export const CODE_LOADING_VARIABLES = new Set([
  'LD_PRELOAD',
  'LD_LIBRARY_PATH',
  'LD_AUDIT',
  'DYLD_INSERT_LIBRARIES',
  'DYLD_LIBRARY_PATH',
  'DYLD_FRAMEWORK_PATH',
  'BASH_ENV',
  'ENV',
  'ZDOTDIR',
  'NODE_OPTIONS',
  'PROMPT_COMMAND',
  'PATH',
]);

// Telemetry control variables
export const TELEMETRY_VARIABLES = new Set([
  'DISABLE_TELEMETRY',
  'DO_NOT_TRACK',
  'SUPERINFERENCE_DISABLE_NONESSENTIAL_TRAFFIC',
]);

export const PIN_CATEGORIES = ['policy', 'code_loading', 'telemetry', 'dynamic_policy'] as const;
export type PinCategory = (typeof PIN_CATEGORIES)[number];

export function isPinnedVariable(name: string): boolean {
  const upper = name.toUpperCase();
  return POLICY_VARIABLES.has(upper) || CODE_LOADING_VARIABLES.has(upper) || TELEMETRY_VARIABLES.has(upper);
}

export function getPinCategory(name: string): PinCategory | null {
  const upper = name.toUpperCase();
  if (POLICY_VARIABLES.has(upper)) return 'policy';
  if (CODE_LOADING_VARIABLES.has(upper)) return 'code_loading';
  if (TELEMETRY_VARIABLES.has(upper)) return 'telemetry';
  return null;
}

export function getAllPinnedVariables(): string[] {
  return [...POLICY_VARIABLES, ...CODE_LOADING_VARIABLES, ...TELEMETRY_VARIABLES];
}

export interface EnvSetAttempt {
  variable: string;
  category: PinCategory;
  source: string;
  timestamp: number;
}

export interface EnvCheckResult {
  blocked: boolean;
  variable: string;
  category: PinCategory | null;
  reason: string;
}

export class EnvProtectionGuard {
  private _blockedAttempts: EnvSetAttempt[] = [];
  private _maxAttempts = 1000;
  private _dynamicPins = new Set<string>();

  mergePinnedFromPolicy(keys: string[]): void {
    for (const k of keys) this._dynamicPins.add(k.toUpperCase());
  }

  isDynamicallyPinned(name: string): boolean {
    return this._dynamicPins.has(name.toUpperCase());
  }

  checkAndBlock(variable: string, source: string): EnvCheckResult {
    const category = getPinCategory(variable);
    const isDynamic = this._dynamicPins.has(variable.toUpperCase());
    if (!category && !isDynamic) {
      return { blocked: false, variable, category: null, reason: '' };
    }
    this.checkSet(variable, source);
    const effectiveCategory = category ?? 'dynamic_policy';
    return {
      blocked: true,
      variable: variable.toUpperCase(),
      category: effectiveCategory,
      reason: `Environment variable "${variable}" is pinned (${effectiveCategory}) and cannot be modified`,
    };
  }

  checkSet(variable: string, source: string): EnvSetAttempt | null {
    const category = getPinCategory(variable);
    if (!category) return null;
    const attempt: EnvSetAttempt = {
      variable: variable.toUpperCase(),
      category,
      source,
      timestamp: Date.now(),
    };
    if (this._blockedAttempts.length < this._maxAttempts) {
      this._blockedAttempts.push(attempt);
    }
    return attempt;
  }

  getBlockedAttempts(): readonly EnvSetAttempt[] {
    return this._blockedAttempts;
  }

  getBlockedCount(): number {
    return this._blockedAttempts.length;
  }

  clear(): void {
    this._blockedAttempts = [];
  }
}
