// Split from credential-guard.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/**
 * Canonical secret-detection catalog. Each rule pairs a stable snake_case type
 * (used by findCredentials and getCredentialLabel) with its matcher, so the
 * detect / redact / enumerate paths all derive from ONE source and cannot drift
 * (previously CREDENTIAL_PATTERNS and the findCredentials typeMap were two
 * hand-synced lists). Prefixes are high-confidence, gitleaks-derived, so a wide
 * range of provider credentials are caught before text leaves the process.
 */
export interface SecretRule {
  type: string;
  pattern: RegExp;
  /**
   * When `false`, the rule is matched exactly as written. Otherwise (the
   * default) a zero-width leading word boundary (`LEADING_BOUNDARY`) is prepended
   * at compile time so the distinctive prefix must START a token — a provider key
   * glued to the tail of a longer identifier (`fooAKIA…`, `xghp_…`) is NOT a real
   * credential and is no longer over-redacted (mirrors gitleaks' `\b`-anchored
   * rules). Set `false` only for block-delimited rules (PEM) where a leading
   * boundary is meaningless.
   */
  boundary?: boolean;
}

// Open-ended token bodies are capped with a GENEROUS upper bound (far above any
// real credential length) instead of an unbounded `{n,}`. This caps the scan
// work a single match can perform on malformed/untrusted input (defence-in-depth
// against pathological inputs) while never truncating a real secret — mirroring
// gitleaks' bounded quantifiers (token bodies capped at 4096, PEM blocks at
// 16384). The patterns are star-height 1 (no nested quantifier), so there is no
// catastrophic backtracking regardless.
export const SECRET_RULES: SecretRule[] = [
  // Anthropic must precede the generic openai `sk-` rule.
  { type: 'anthropic_key', pattern: /sk-ant-[a-zA-Z0-9-]{20,4096}/g },
  // OpenAI project/service/admin keys (new `sk-(proj|svcacct|admin)-…T3BlbkFJ…`
  // format) must precede the generic `sk-…` rule — the hyphenated prefix is not
  // matched by openai_key below, so this is the only rule that catches them.
  {
    type: 'openai_project_key',
    pattern:
      /sk-(?:proj|svcacct|admin)-(?:[A-Za-z0-9_-]{74}|[A-Za-z0-9_-]{58})T3BlbkFJ(?:[A-Za-z0-9_-]{74}|[A-Za-z0-9_-]{58})/g,
  },
  // OpenAI standard (non-scoped) user API key — the `sk-<20>T3BlbkFJ<20>` form.
  // Its mandatory `T3BlbkFJ` infix is the structural anchor gitleaks/the reference
  // rely on (the second alternative of the `openai-api-key` rule) for high-
  // confidence OpenAI detection. Precedes the generic `openai_key` rule so a
  // modern user key is typed by its structure; the generic rule is retained as
  // the broad defensive net for legacy/opaque `sk-` tokens.
  { type: 'openai_user_key', pattern: /sk-[a-zA-Z0-9]{20}T3BlbkFJ[a-zA-Z0-9]{20}/g },
  { type: 'openai_key', pattern: /sk-[a-zA-Z0-9]{20,4096}/g },
  { type: 'google_key', pattern: /AIza[a-zA-Z0-9_-]{35}/g },
  // Azure AD client secret — distinctive `…<digit>Q~…` infix (gitleaks
  // azure-ad-client-secret).
  { type: 'azure_ad_secret', pattern: /[a-zA-Z0-9_~.]{3}\dQ~[a-zA-Z0-9_~.-]{31,34}/g },
  // GCP / Vertex AI service-account identity (low-entropy structured text the
  // entropy-gated assigned-secret rule cannot catch).
  { type: 'gcp_service_account', pattern: /[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com/g },
  // GitHub token family: classic PAT, OAuth, fine-grained PAT, app (ghu/ghs),
  // refresh (ghr).
  { type: 'github_pat', pattern: /ghp_[a-zA-Z0-9]{36}/g },
  { type: 'github_oauth', pattern: /gho_[a-zA-Z0-9]{36}/g },
  { type: 'github_fine_pat', pattern: /github_pat_[a-zA-Z0-9_]{22,4096}/g },
  { type: 'github_app_token', pattern: /gh[us]_[a-zA-Z0-9]{36}/g },
  { type: 'github_refresh_token', pattern: /ghr_[a-zA-Z0-9]{36}/g },
  { type: 'gitlab_pat', pattern: /glpat-[a-zA-Z0-9_-]{20,4096}/g },
  { type: 'gitlab_deploy_token', pattern: /gldt-[a-zA-Z0-9_-]{20}/g },
  { type: 'slack_bot', pattern: /xoxb-[0-9]{1,20}-[a-zA-Z0-9]{1,4096}/g },
  { type: 'slack_user', pattern: /xoxp-[0-9]{1,20}-[a-zA-Z0-9]{1,4096}/g },
  { type: 'slack_app', pattern: /xapp-\d-[A-Z0-9]{1,64}-\d{1,20}-[a-z0-9]{1,4096}/gi },
  // AWS access-key family: long-term (AKIA), temporary/STS session (ASIA),
  // plus A3T*/ABIA/ACCA prefixes. Body is base32 (A-Z,2-7), 16 chars.
  { type: 'aws_access_key', pattern: /(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16}/g },
  { type: 'jwt', pattern: /eyJ[a-zA-Z0-9_-]{20,4096}\.[a-zA-Z0-9_-]{20,4096}\.[a-zA-Z0-9_-]{20,4096}/g },
  { type: 'nvidia_key', pattern: /nvapi-[a-zA-Z0-9_-]{20,4096}/g },
  // Stripe (restricted + secret, test/live/prod); Stripe uses `sk_`/`rk_` with
  // an underscore so it never collides with the openai `sk-` rule above.
  { type: 'stripe_key', pattern: /(?:sk|rk)_(?:test|live|prod)_[a-zA-Z0-9]{10,99}/g },
  { type: 'npm_token', pattern: /npm_[a-zA-Z0-9]{36}/g },
  { type: 'shopify_token', pattern: /shp(?:at|ss)_[a-fA-F0-9]{32}/g },
  { type: 'digitalocean_token', pattern: /d(?:op|oo)_v1_[a-f0-9]{64}/g },
  { type: 'huggingface_token', pattern: /hf_[a-zA-Z]{34}/g },
  // SendGrid API token.
  { type: 'sendgrid_token', pattern: /SG\.[a-zA-Z0-9=_.-]{66}/g },
  // PyPI upload token.
  { type: 'pypi_token', pattern: /pypi-AgEIcHlwaS5vcmc[\w-]{50,1000}/g },
  // HashiCorp Terraform Cloud API token (`<14 alnum>.atlasv1.<60-70>`).
  { type: 'hashicorp_token', pattern: /[a-zA-Z0-9]{14}\.atlasv1\.[a-zA-Z0-9_=-]{60,70}/g },
  { type: 'sentry_token', pattern: /sntryu_[a-f0-9]{64}/g },
  // Sentry organization token (`sntrys_…`).
  {
    type: 'sentry_org_token',
    pattern:
      /sntrys_eyJpYXQiO[a-zA-Z0-9+/]{10,200}(?:LCJyZWdpb25fdXJs|InJlZ2lvbl91cmwi|cmVnaW9uX3VybCI6)[a-zA-Z0-9+/]{10,200}={0,2}_[a-zA-Z0-9+/]{43}/g,
  },
  // Grafana: cloud (`glc_`) + service-account (`glsa_`) + API key (base64
  // `eyJrIjoi…`).
  { type: 'grafana_token', pattern: /glc_[A-Za-z0-9+/]{32,400}={0,3}|glsa_[A-Za-z0-9]{32}_[A-Fa-f0-9]{8}/g },
  { type: 'grafana_api_key', pattern: /eyJrIjoi[A-Za-z0-9+/]{70,400}={0,3}/g },
  { type: 'databricks_token', pattern: /dapi[a-f0-9]{32}(?:-\d)?/g },
  { type: 'pulumi_token', pattern: /pul-[a-f0-9]{40}/g },
  { type: 'postman_token', pattern: /PMAK-[a-fA-F0-9]{24}-[a-fA-F0-9]{34}/g },
  { type: 'twilio_key', pattern: /SK[0-9a-fA-F]{32}/g },
  // PEM private-key blocks (RSA / EC / OPENSSH / DSA / PGP / unlabelled). Block
  // delimited by `-----…-----`, so a leading word boundary is meaningless.
  { type: 'private_key', pattern: /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]{1,16384}?-----END [A-Z0-9 ]*PRIVATE KEY-----/g, boundary: false },
  // Bearer tokens in Authorization headers / curl examples.
  { type: 'bearer_token', pattern: /Bearer\s+[a-zA-Z0-9_\-.=+/]{20,4096}/g },
];

// Prepended to every boundary-enabled rule at compile time. A zero-width
// negative lookbehind (does not consume input, so `findCredentials` match offsets
// are unchanged) asserting the distinctive prefix is not glued to a preceding
// alphanumeric — the word-boundary / delimiter anchoring gitleaks relies on to
// avoid over-redacting a prefix that merely appears MID-token.
export const LEADING_BOUNDARY = '(?<![A-Za-z0-9])';

/**
 * Compile-once rule cache. Each rule's RegExp is rebuilt a single time (at module
 * load) with the leading boundary applied, so the detect / redact / enumerate
 * paths all share ONE set of compiled patterns rather than recompiling per call.
 */
export interface CompiledRule {
  type: string;
  re: RegExp;
}

export const REDACT_PLACEHOLDER = '[REDACTED]';
