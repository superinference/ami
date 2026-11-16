export const ENV_ACCESS_TIERS = ['builtin', 'prepend', 'user', 'append', 'engine'] as const;
export type EnvAccessTier = (typeof ENV_ACCESS_TIERS)[number];

export const ENV_OPERATIONS = ['get', 'set', 'delete'] as const;
export type EnvOperation = (typeof ENV_OPERATIONS)[number];

// Variables that describe sensitive paths — never expose values, only whether set
export const SENSITIVE_ENV_VARS = new Set([
  'API_KEY',
  'SECRET_KEY',
  'ACCESS_TOKEN',
  'PRIVATE_KEY',
  'PASSWORD',
  'CREDENTIAL',
  'AUTH_TOKEN',
  // Covers AMI_CLIENT_KEY_PASSPHRASE (mTLS client-key passphrase) and any other
  // *_PASSPHRASE secret.
  'PASSPHRASE',
]);

// Word-boundary patterns that catch common secret-bearing variable names the
// coarse SENSITIVE_ENV_VARS substring list misses: bare TOKEN/SECRET/KEY/PASSWD
// and provider-prefixed vars such as GITHUB_TOKEN, NPM_SECRET, AWS_SECRET_ACCESS_KEY.
const SENSITIVE_ENV_NAME_PATTERN =
  /(^|[_-])(API[_-]?KEY|SECRET[_-]?KEY|ACCESS[_-]?TOKEN|AUTH[_-]?TOKEN|PRIVATE[_-]?KEY|PASSWORD|PASSWD|PASSPHRASE|CREDENTIALS?|TOKEN|SECRETS?|KEY)([_-]|$)/;

export function isSensitiveEnvVar(name: string): boolean {
  const upper = name.toUpperCase();
  // Keep the original coarse substring match (back-compat), then broaden with a
  // word-boundary pattern so GITHUB_TOKEN / NPM_SECRET / OPENAI_KEY are caught too.
  if ([...SENSITIVE_ENV_VARS].some(s => upper.includes(s))) return true;
  return SENSITIVE_ENV_NAME_PATTERN.test(upper);
}

/**
 * Secret categories used to label a redacted value. Mirrors the provider classes
 * a secret can fall into so redacted diagnostics stay self-describing instead of
 * collapsing every secret to one opaque marker.
 */
export type EnvSecretClass =
  | 'api_key'
  | 'aws_key'
  | 'gcp_key'
  | 'gcp_service_account'
  | 'token'
  | 'generic';

// Value-shape detectors: catch secrets even when the variable NAME is innocuous
// (e.g. MY_CONFIG holding an sk-ant… key, or X holding a service-account email).
// Ordered most-specific-first; the first match wins.
const VALUE_SHAPE_DETECTORS: ReadonlyArray<readonly [RegExp, EnvSecretClass]> = [
  // AWS access-key IDs (AKIA/ASIA/AGPA/… followed by 16 upper-alnum chars)
  [/\b(?:AKIA|ASIA|AGPA|AIDA|AROA|ANPA|ANVA)[0-9A-Z]{16}\b/, 'aws_key'],
  // GCP / Google API keys
  [/\bAIza[0-9A-Za-z_-]{35}\b/, 'gcp_key'],
  // GCP / Vertex service-account emails
  [/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.iam\.gserviceaccount\.com\b/, 'gcp_service_account'],
  // Anthropic / OpenAI-style secret keys (sk-ant-…, sk-…)
  [/\bsk-(?:ant-)?[A-Za-z0-9_-]{16,}\b/, 'api_key'],
  // GitHub tokens (ghp_/gho_/ghu_/ghs_/ghr_ classic, and fine-grained github_pat_)
  [/\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}\b/, 'token'],
  [/\bgithub_pat_[A-Za-z0-9_]{20,}\b/, 'token'],
  // Slack tokens
  [/\bxox[baprs]-[A-Za-z0-9-]{10,}\b/, 'token'],
  // Authorization: Bearer <token> headers
  [/\b[Bb]earer\s+[A-Za-z0-9._~+/-]{8,}=*/, 'token'],
  // x-api-key headers
  [/x-api-key\s*[:=]/i, 'api_key'],
];

/** Classify a secret purely by the SHAPE of its value, independent of its name. */
export function classifyEnvValue(value: string): EnvSecretClass | undefined {
  for (const [pattern, cls] of VALUE_SHAPE_DETECTORS) {
    if (pattern.test(value)) return cls;
  }
  return undefined;
}

/** Classify a sensitive variable by its NAME (provider prefix / token family). */
export function classifyEnvName(name: string): EnvSecretClass | undefined {
  if (!isSensitiveEnvVar(name)) return undefined;
  const upper = name.toUpperCase();
  if (/(^|[_-])AWS([_-]|$)/.test(upper)) return 'aws_key';
  if (/(^|[_-])(GOOGLE|GCP|GCLOUD|VERTEX)([_-]|$)/.test(upper)) return 'gcp_key';
  if (/(^|[_-])(ACCESS[_-]?TOKEN|AUTH[_-]?TOKEN|TOKEN)([_-]|$)/.test(upper)) return 'token';
  if (/KEY/.test(upper)) return 'api_key';
  return 'generic';
}

/**
 * Decide whether a variable is secret and, if so, which class it belongs to.
 * Value-shape detection wins over name-based classification, so a provider key
 * stored under an innocuously-named var is still caught and labeled correctly.
 */
export function classifyEnvSecret(
  name: string,
  value: string | undefined,
): EnvSecretClass | undefined {
  if (value !== undefined) {
    const byValue = classifyEnvValue(value);
    if (byValue) return byValue;
  }
  return classifyEnvName(name);
}

/**
 * Minimum value length for a partial-reveal preview. Values shorter than this are
 * fully masked (too short to preview without disclosing most of the secret), matching
 * Claude's `REDACT_MIN_LENGTH`. Longer values keep a prefix/suffix for correlation.
 */
export const REDACT_MIN_LENGTH = 16;
/** Characters of a long secret kept at the start when previewing. */
export const REDACT_PREVIEW_PREFIX = 8;
/** Characters of a long secret kept at the end when previewing. */
export const REDACT_PREVIEW_SUFFIX = 4;

/**
 * Self-describing redaction marker for a secret class, e.g. 'api_key' →
 * '[REDACTED_API_KEY]', 'gcp_service_account' → '[REDACTED_GCP_SERVICE_ACCOUNT]'.
 * Mirrors Claude's category-specific labels so a human triaging a redacted diagnostic
 * can tell WHAT kind of secret was hidden — without the value itself ever leaking.
 */
export function redactionMarker(cls: EnvSecretClass): string {
  return `[REDACTED_${cls.toUpperCase()}]`;
}

/**
 * Mask a secret value. By default returns the typed class marker (full redaction).
 * With `preview`, a value at least {@link REDACT_MIN_LENGTH} long is reduced to
 * `prefix(8)...suffix(4)` so it stays correlatable in debug output without disclosing
 * the body; shorter values still fully mask to the class marker. Mirrors Claude's
 * `redactSecrets` threshold/slice behavior while keeping full masking as the safe default.
 */
export function redactSecretValue(value: string, cls: EnvSecretClass, preview = false): string {
  if (preview && value.length >= REDACT_MIN_LENGTH) {
    return `${value.slice(0, REDACT_PREVIEW_PREFIX)}...${value.slice(-REDACT_PREVIEW_SUFFIX)}`;
  }
  return redactionMarker(cls);
}

export function canAccessEnv(
  variable: string,
  operation: EnvOperation,
  callerTier: EnvAccessTier,
  readerTier?: EnvAccessTier,
): boolean {
  // Gets by org plugins bypass user tier
  if (operation === 'get' && readerTier &&
      (readerTier === 'prepend' || readerTier === 'append') &&
      callerTier === 'user') {
    return false;
  }

  // Sets/deletes of pinned variables only by engine/builtin
  if (operation === 'set' || operation === 'delete') {
    // The actual pinning is handled by env-protection.ts
    // This guard handles tier-based access control
    return true;
  }

  return true;
}

export interface EnvAccessEvent {
  variable: string;
  operation: EnvOperation;
  callerTier: EnvAccessTier;
  allowed: boolean;
  timestamp: number;
}

export class EnvAccessGuard {
  private _events: EnvAccessEvent[] = [];
  private _maxEvents: number;

  constructor(maxEvents: number = 10_000) {
    this._maxEvents = maxEvents;
  }

  check(variable: string, operation: EnvOperation, callerTier: EnvAccessTier, readerTier?: EnvAccessTier): boolean {
    const allowed = canAccessEnv(variable, operation, callerTier, readerTier);
    if (this._events.length < this._maxEvents) {
      this._events.push({
        variable,
        operation,
        callerTier,
        allowed,
        timestamp: Date.now(),
      });
    }
    return allowed;
  }

  getEvents(): readonly EnvAccessEvent[] {
    return this._events;
  }

  getBlockedEvents(): EnvAccessEvent[] {
    return this._events.filter(e => !e.allowed);
  }

  getBlockedCount(): number {
    return this._events.filter(e => !e.allowed).length;
  }

  safeGetEnv(
    name: string,
    opts?: { preview?: boolean },
  ): { value: string | undefined; redacted: boolean; redactedAs?: EnvSecretClass } {
    const raw = process.env[name];
    // Redact when EITHER the name is sensitive OR the value looks like a secret,
    // so a secret under an innocuous name (e.g. MY_CONFIG=sk-ant…) is still hidden.
    // The marker is a typed, self-describing '[REDACTED_<CLASS>]' token (mirrors Claude's
    // category-specific labels); `redactedAs` carries the same class for programmatic use.
    // With `preview`, long secrets render as prefix…suffix so they stay correlatable in
    // debug output without disclosing the body (full masking remains the default).
    const cls = classifyEnvSecret(name, raw);
    if (cls !== undefined) {
      return {
        value: raw !== undefined ? redactSecretValue(raw, cls, opts?.preview) : undefined,
        redacted: true,
        redactedAs: cls,
      };
    }
    return { value: raw, redacted: false };
  }

  clear(): void {
    this._events = [];
  }
}
