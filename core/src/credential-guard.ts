import * as os from 'node:os';
import { inspect } from 'node:util';

/**
 * Canonical secret-detection catalog. Each rule pairs a stable snake_case type
 * (used by findCredentials and getCredentialLabel) with its matcher, so the
 * detect / redact / enumerate paths all derive from ONE source and cannot drift
 * (previously CREDENTIAL_PATTERNS and the findCredentials typeMap were two
 * hand-synced lists). Prefixes are high-confidence, gitleaks-derived, so a wide
 * range of provider credentials are caught before text leaves the process.
 */
interface SecretRule {
  type: string;
  pattern: RegExp;
}

const SECRET_RULES: SecretRule[] = [
  // Anthropic must precede the generic openai `sk-` rule.
  { type: 'anthropic_key', pattern: /sk-ant-[a-zA-Z0-9-]{20,}/g },
  { type: 'openai_key', pattern: /sk-[a-zA-Z0-9]{20,}/g },
  { type: 'google_key', pattern: /AIza[a-zA-Z0-9_-]{35}/g },
  // GCP / Vertex AI service-account identity (low-entropy structured text the
  // entropy-gated assigned-secret rule cannot catch).
  { type: 'gcp_service_account', pattern: /[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com/g },
  // GitHub token family: classic PAT, OAuth, fine-grained PAT, app (ghu/ghs),
  // refresh (ghr).
  { type: 'github_pat', pattern: /ghp_[a-zA-Z0-9]{36}/g },
  { type: 'github_oauth', pattern: /gho_[a-zA-Z0-9]{36}/g },
  { type: 'github_fine_pat', pattern: /github_pat_[a-zA-Z0-9_]{22,}/g },
  { type: 'github_app_token', pattern: /gh[us]_[a-zA-Z0-9]{36}/g },
  { type: 'github_refresh_token', pattern: /ghr_[a-zA-Z0-9]{36}/g },
  { type: 'gitlab_pat', pattern: /glpat-[a-zA-Z0-9_-]{20,}/g },
  { type: 'gitlab_deploy_token', pattern: /gldt-[a-zA-Z0-9_-]{20}/g },
  { type: 'slack_bot', pattern: /xoxb-[0-9]+-[a-zA-Z0-9]+/g },
  { type: 'slack_user', pattern: /xoxp-[0-9]+-[a-zA-Z0-9]+/g },
  { type: 'slack_app', pattern: /xapp-\d-[A-Z0-9]+-\d+-[a-z0-9]+/gi },
  // AWS access-key family: long-term (AKIA), temporary/STS session (ASIA),
  // plus A3T*/ABIA/ACCA prefixes. Body is base32 (A-Z,2-7), 16 chars.
  { type: 'aws_access_key', pattern: /(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16}/g },
  { type: 'jwt', pattern: /eyJ[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}/g },
  { type: 'nvidia_key', pattern: /nvapi-[a-zA-Z0-9_-]{20,}/g },
  // Stripe (restricted + secret, test/live/prod); Stripe uses `sk_`/`rk_` with
  // an underscore so it never collides with the openai `sk-` rule above.
  { type: 'stripe_key', pattern: /(?:sk|rk)_(?:test|live|prod)_[a-zA-Z0-9]{10,99}/g },
  { type: 'npm_token', pattern: /npm_[a-zA-Z0-9]{36}/g },
  { type: 'shopify_token', pattern: /shp(?:at|ss)_[a-fA-F0-9]{32}/g },
  { type: 'digitalocean_token', pattern: /d(?:op|oo)_v1_[a-f0-9]{64}/g },
  { type: 'huggingface_token', pattern: /hf_[a-zA-Z]{34}/g },
  { type: 'sentry_token', pattern: /sntryu_[a-f0-9]{64}/g },
  { type: 'grafana_token', pattern: /glc_[A-Za-z0-9+/]{32,400}={0,3}|glsa_[A-Za-z0-9]{32}_[A-Fa-f0-9]{8}/g },
  { type: 'databricks_token', pattern: /dapi[a-f0-9]{32}(?:-\d)?/g },
  { type: 'pulumi_token', pattern: /pul-[a-f0-9]{40}/g },
  { type: 'postman_token', pattern: /PMAK-[a-fA-F0-9]{24}-[a-fA-F0-9]{34}/g },
  { type: 'twilio_key', pattern: /SK[0-9a-fA-F]{32}/g },
  // PEM private-key blocks (RSA / EC / OPENSSH / DSA / PGP / unlabelled).
  { type: 'private_key', pattern: /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----/g },
  // Bearer tokens in Authorization headers / curl examples.
  { type: 'bearer_token', pattern: /Bearer\s+[a-zA-Z0-9_\-.=+/]{20,}/g },
];

const CREDENTIAL_PATTERNS = SECRET_RULES.map(r => r.pattern);

export const REDACT_PLACEHOLDER = '[REDACTED]';

// Words whose canonical capitalization differs from naive Title Case, mirroring
// Claude's ruleIdToLabel special-case table so brand/acronym casing renders
// correctly in UI/introspection surfaces.
const LABEL_SPECIAL_CASE: Record<string, string> = {
  aws: 'AWS', gcp: 'GCP', api: 'API', pat: 'PAT', ad: 'AD', tf: 'TF',
  oauth: 'OAuth', npm: 'NPM', pypi: 'PyPI', jwt: 'JWT', github: 'GitHub',
  gitlab: 'GitLab', openai: 'OpenAI', digitalocean: 'DigitalOcean',
  huggingface: 'HuggingFace', hashicorp: 'HashiCorp', sendgrid: 'SendGrid',
  nvidia: 'NVIDIA',
};

/**
 * Human-readable, brand-correct label for a snake_case credential type (e.g.
 * `github_pat` → "GitHub PAT", `aws_access_key` → "AWS Access Key"). Unknown
 * types fall back to Title Case. Exported so CLI/introspection surfaces can show
 * what KIND of secret was detected without ever revealing the value.
 */
export function getCredentialLabel(type: string): string {
  return type
    .split('_')
    .filter(Boolean)
    .map(part => LABEL_SPECIAL_CASE[part] ?? (part.charAt(0).toUpperCase() + part.slice(1)))
    .join(' ');
}

// ---------------------------------------------------------------------------
// Entropy-based detection for generic assigned secrets
// ---------------------------------------------------------------------------

/** Shannon entropy (bits/char) of a string. Random secrets score high (~4-6); */
export function shannonEntropy(s: string): number {
  if (!s) return 0;
  const freq = new Map<string, number>();
  for (const ch of s) freq.set(ch, (freq.get(ch) ?? 0) + 1);
  let entropy = 0;
  for (const count of freq.values()) {
    const p = count / s.length;
    entropy -= p * Math.log2(p);
  }
  return entropy;
}

/**
 * A token looks like a high-entropy secret: long enough and random enough that
 * it is implausibly a word, path, or identifier. Threshold 3.5 bits/char keeps
 * out repeated/low-variety strings while catching base64/hex secrets.
 */
export function isHighEntropySecret(token: string): boolean {
  return token.length >= 16 && shannonEntropy(token) >= 3.5;
}

// A secret value ASSIGNED to a secret-ish key (`token: x`, `api_key="x"`, …).
// Group 1 = key name, group 2 = separator (incl. opening quote), group 3 = value.
// The lookbehind keeps `secret` from matching the tail of `client_secret`, and the
// value class deliberately excludes quotes/whitespace so a trailing quote is left
// intact. Unlike the old pattern this accepts short values (no `{16,}` floor) so
// a short/opaque token under a secret-named key is caught (see redaction tiers).
const SECRET_ASSIGN_RE =
  /(?<![A-Za-z0-9_])(password|passwd|secret|token|access[_-]?token|auth[_-]?token|authorization|client[_-]?secret|private[_-]?key|credential|api[_-]?key|access[_-]?key)(["']?\s*[:=]\s*["']?)([A-Za-z0-9+/_.\-=]+)/gi;

// Prefixed UPPER_SNAKE env-var secret names that the lowercase SECRET_ASSIGN_RE
// misses because its key lookbehind rejects a leading `PREFIX_` (e.g.
// `AWS_SECRET_ACCESS_KEY`, `GOOGLE_API_KEY`, `GITHUB_TOKEN`, `DB_PASSWORD`). The
// name MUST carry a `…_` prefix before the secret word so a bare `TOKEN=`/
// `SECRET=` is left to SECRET_ASSIGN_RE (no double match). Values here are
// redacted UNCONDITIONALLY — an explicitly secret-named env var is a secret
// regardless of the value's length/entropy, so a short or low-entropy token
// cannot slip through. Case-sensitive (no `i`): matching only UPPER_SNAKE names
// avoids colliding with the lowercase rule and with ordinary identifiers. The
// prefix is a single flat `[A-Z0-9_]*_` class (star-height 1) so there is no
// nested-quantifier backtracking. Group 1 = name, 2 = separator (incl. opening
// quote), 3 = value.
const ENV_SECRET_ASSIGN_RE =
  /(?<![A-Za-z0-9_])([A-Z0-9_]*_(?:SECRET_?KEY|SECRET|TOKEN|PASSWORD|PASSWD|PASSPHRASE|API_?KEY|ACCESS_?KEY|PRIVATE_?KEY|CREDENTIALS?|AUTH))(["']?\s*[:=]\s*["']?)([A-Za-z0-9+/_.\-=]+)/g;

// Field names that are UNCONDITIONALLY secret: the assigned value is redacted
// regardless of length or entropy (a short/opaque session token must not leak just
// because it is low-entropy). The weaker `*_key` family (api_key/access_key) keeps
// the entropy gate below, since those names also appear with non-secret values.
const UNCONDITIONAL_SECRET_KEY =
  /^(?:password|passwd|secret|token|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|credential)$/i;

/** Whether the value assigned to a secret-ish key should be redacted. */
function assignedSecretShouldRedact(key: string, value: string): boolean {
  if (UNCONDITIONAL_SECRET_KEY.test(key)) return true;
  return isHighEntropySecret(value);
}

/**
 * Default minimum length for partial-reveal (fingerprint) mode. Values at least
 * this long keep a short prefix+suffix for diagnosis; shorter values are fully
 * masked so a tiny secret is never partly exposed.
 */
export const REDACT_MIN_LENGTH = 16;

export interface RedactOptions {
  /**
   * Partial-reveal (fingerprint) mode: keep the first 8 and last 4 chars of a
   * value at least `minLength` long, fully masking shorter ones. Off by default
   * so redaction stays fully opaque unless a caller explicitly opts in (e.g. a
   * debug/diagnostic sink where a fingerprint aids correlation).
   */
  partial?: boolean;
  /** Length threshold for partial reveal (default `REDACT_MIN_LENGTH`). */
  minLength?: number;
}

/**
 * Replacement string for a single detected secret. Full `[REDACTED]` by default;
 * in partial mode, a long value becomes `<first8>[REDACTED]<last4>` so its shape
 * can be correlated across logs without revealing the secret. Short values are
 * always fully masked.
 */
export function maskSecretValue(value: string, options: RedactOptions = {}): string {
  const min = options.minLength ?? REDACT_MIN_LENGTH;
  if (options.partial && value.length >= min) {
    return `${value.slice(0, 8)}${REDACT_PLACEHOLDER}${value.slice(-4)}`;
  }
  return REDACT_PLACEHOLDER;
}

export function containsCredential(text: string): boolean {
  if (!text || text.length < 10) return false;
  if (CREDENTIAL_PATTERNS.some(pattern => {
    pattern.lastIndex = 0;
    return pattern.test(text);
  })) return true;
  // Prefixed UPPER_SNAKE env-var secret (unconditional).
  ENV_SECRET_ASSIGN_RE.lastIndex = 0;
  if (ENV_SECRET_ASSIGN_RE.test(text)) return true;
  // Secret value assigned to a secret-ish key.
  SECRET_ASSIGN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = SECRET_ASSIGN_RE.exec(text)) !== null) {
    if (assignedSecretShouldRedact(m[1], m[3])) return true;
  }
  return false;
}

export function redactCredentials(text: string, options: RedactOptions = {}): string {
  if (!text || text.length < 10) return text;
  let result = text;
  for (const pattern of CREDENTIAL_PATTERNS) {
    pattern.lastIndex = 0;
    result = result.replace(
      new RegExp(pattern.source, pattern.flags),
      (match: string) => maskSecretValue(match, options),
    );
  }
  // Prefixed UPPER_SNAKE env-var secrets are unconditionally redacted (keep the
  // name + separator for context). Runs before SECRET_ASSIGN_RE; because the
  // name carries an underscore-joined prefix, the lowercase rule's key lookbehind
  // can never re-match the already-masked assignment, so there is no double pass.
  result = result.replace(
    new RegExp(ENV_SECRET_ASSIGN_RE.source, ENV_SECRET_ASSIGN_RE.flags),
    (_full: string, key: string, sep: string, value: string) =>
      `${key}${sep}${maskSecretValue(value, options)}`,
  );
  // Redact the assigned secret value (keep the key + separator for context).
  result = result.replace(
    new RegExp(SECRET_ASSIGN_RE.source, SECRET_ASSIGN_RE.flags),
    (full: string, key: string, sep: string, value: string) =>
      assignedSecretShouldRedact(key, value) ? `${key}${sep}${maskSecretValue(value, options)}` : full,
  );
  return result;
}

export interface CredentialMatch {
  type: string;
  /** Brand-correct, human-readable label for the type. */
  label: string;
  start: number;
  length: number;
}

export function findCredentials(text: string): CredentialMatch[] {
  if (!text || text.length < 10) return [];
  const found: CredentialMatch[] = [];

  for (const { pattern, type } of SECRET_RULES) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      found.push({ type, label: getCredentialLabel(type), start: match.index, length: match[0].length });
    }
  }

  // Prefixed UPPER_SNAKE env-var secrets (value only, unconditional).
  ENV_SECRET_ASSIGN_RE.lastIndex = 0;
  let envm: RegExpExecArray | null;
  while ((envm = ENV_SECRET_ASSIGN_RE.exec(text)) !== null) {
    const valueStart = envm.index + envm[1].length + envm[2].length;
    found.push({ type: 'env_secret', label: getCredentialLabel('env_secret'), start: valueStart, length: envm[3].length });
  }

  // Secret values assigned to secret-ish keys (value only).
  SECRET_ASSIGN_RE.lastIndex = 0;
  let em: RegExpExecArray | null;
  while ((em = SECRET_ASSIGN_RE.exec(text)) !== null) {
    const key = em[1];
    const value = em[3];
    if (!assignedSecretShouldRedact(key, value)) continue;
    const valueStart = em.index + em[1].length + em[2].length;
    found.push({ type: 'generic_secret', label: getCredentialLabel('generic_secret'), start: valueStart, length: value.length });
  }

  return found.sort((a, b) => a.start - b.start);
}

export class CredentialGuard {
  private _detections: Array<{ type: string; source: string; timestamp: number }> = [];
  private _maxDetections = 1000;

  scan(text: string, source: string): boolean {
    const creds = findCredentials(text);
    if (creds.length === 0) return false;
    for (const cred of creds) {
      if (this._detections.length < this._maxDetections) {
        this._detections.push({ type: cred.type, source, timestamp: Date.now() });
      }
    }
    return true;
  }

  redact(text: string): string {
    return redactCredentials(text);
  }

  getDetections(): ReadonlyArray<{ type: string; source: string; timestamp: number }> {
    return this._detections;
  }

  getDetectionCount(): number {
    return this._detections.length;
  }

  clear(): void {
    this._detections = [];
  }
}

// ---------------------------------------------------------------------------
// Redact-by-construction secret wrapper
// ---------------------------------------------------------------------------

/**
 * A leak-proof-by-construction holder for a secret value (API key, OAuth/refresh
 * token, provisioning secret). The raw value lives in a private field and is only
 * reachable via the single explicit `reveal()` escape hatch — used right at the
 * HTTP/header-injection boundary. Every implicit stringification path
 * (`String(x)`, template literals, `JSON.stringify`, `util.inspect`/`console.log`)
 * returns `REDACT_PLACEHOLDER`, so accidentally logging or serializing a
 * token-bearing object cannot leak it. This complements the after-the-fact text
 * scrubbing done by `redactCredentials`: here the value is never exposed to begin
 * with.
 */
export class RedactedSecret {
  readonly #value: string;

  constructor(value: string) {
    this.#value = value;
  }

  /** The single explicit escape hatch — reveal the raw secret for actual use. */
  reveal(): string {
    return this.#value;
  }

  /** `true` iff the wrapped value is empty, without revealing it. */
  isEmpty(): boolean {
    return this.#value.length === 0;
  }

  toString(): string {
    return REDACT_PLACEHOLDER;
  }

  toJSON(): string {
    return REDACT_PLACEHOLDER;
  }

  [inspect.custom](): string {
    return REDACT_PLACEHOLDER;
  }
}

// ---------------------------------------------------------------------------
// Local-path / PII normalization
// ---------------------------------------------------------------------------

export interface RedactPathsOptions {
  /** Current working directory to normalize to `[CWD]` (default `process.cwd()`). */
  cwd?: string;
  /** Home directory to normalize to `[HOME]` (default `os.homedir()`). */
  home?: string;
}

/**
 * Replace the user's home directory and current working directory with the
 * stable placeholders `[HOME]` / `[CWD]` so file paths in exported/shareable
 * artifacts (session exports, bug reports, diagnostics) don't leak the local
 * username or machine layout. The cwd is substituted first so a cwd nested under
 * home collapses to `[CWD]` rather than `[HOME]/…`. Root (`/`) is never used as a
 * needle, which would otherwise rewrite every path separator.
 */
export function redactPaths(text: string, options: RedactPathsOptions = {}): string {
  if (!text) return text;
  const cwd = options.cwd ?? process.cwd();
  const home = options.home ?? os.homedir();
  let result = text;
  if (cwd && cwd !== '/') result = result.split(cwd).join('[CWD]');
  if (home && home !== '/') result = result.split(home).join('[HOME]');
  return result;
}
