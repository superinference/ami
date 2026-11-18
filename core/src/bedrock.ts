/**
 * Amazon Bedrock inference-profile resolution.
 *
 * Bedrock exposes models through region-scoped *inference profiles* whose ids
 * carry a region prefix and a version suffix (e.g.
 * `eu.anthropic.claude-opus-4-6-v1:0`). The invoked id must match what the
 * account actually provisioned in its region. AMI lets the user declare the
 * account's profiles via an env knob and then derives the region-aware id by
 * substring-matching the canonical first-party model id against that list —
 * mirroring Claude Code's getBedrockModelStrings canonical-substring matching
 * with a fallback to the plain model id when nothing matches.
 */

export const BEDROCK_INFERENCE_PROFILES_ENV = 'AMI_BEDROCK_INFERENCE_PROFILES';

/**
 * Env knob declaring the account's Bedrock cross-region routing area, applied to
 * the resolved model/profile id when no explicit inference profile matched. One
 * of {@link BEDROCK_REGION_PREFIXES}; any other value is ignored.
 */
export const BEDROCK_REGION_PREFIX_ENV = 'AMI_BEDROCK_REGION';

/**
 * Env var carrying a Bedrock API key (bearer token). When set, Bedrock auth uses
 * this bearer token instead of SigV4 / long-term AWS access keys: its presence
 * skips credential refresh and selects API-key auth. Canonical home for the
 * constant (provider.ts re-exports it so its existing importers are unchanged).
 * Mirrors Claude Code's `AWS_BEARER_TOKEN_BEDROCK` branch (utils/model/bedrock.ts).
 */
export const AWS_BEARER_TOKEN_BEDROCK_ENV = 'AWS_BEARER_TOKEN_BEDROCK';

/**
 * Env var overriding the Bedrock gateway endpoint (a custom / proxy gateway).
 * When unset, the region-derived `bedrock-runtime.<region>.amazonaws.com` endpoint
 * is used instead. Mirrors Claude Code's `ANTHROPIC_BEDROCK_BASE_URL`
 * (utils/model/bedrock.ts) — the single declaration consumed by provider-config.
 */
export const ANTHROPIC_BEDROCK_BASE_URL_ENV = 'ANTHROPIC_BEDROCK_BASE_URL';

/**
 * Bedrock cross-region inference-profile prefixes. A region-scoped profile id
 * carries one of these as its leading dot-segment (e.g.
 * `eu.anthropic.claude-opus-4-6-v1:0`). Mirrors Claude Code's cross-region
 * prefix set (utils/model/bedrock.ts).
 */
export const BEDROCK_REGION_PREFIXES = ['us', 'eu', 'apac', 'global'] as const;
export type BedrockRegionPrefix = (typeof BEDROCK_REGION_PREFIXES)[number];

function isBedrockRegionPrefix(value: string): value is BedrockRegionPrefix {
  return (BEDROCK_REGION_PREFIXES as readonly string[]).includes(value);
}

/**
 * The cross-region inference-profile prefix a profile/model id already carries
 * (its leading dot-segment, case-insensitive), or undefined when it has none.
 * `us.anthropic.claude-opus-4-6-v1:0` → `us`; `anthropic.claude-...` → undefined.
 */
export function extractBedrockRegionPrefix(id: string): BedrockRegionPrefix | undefined {
  const dot = id.indexOf('.');
  if (dot === -1) return undefined;
  const head = id.slice(0, dot).toLowerCase();
  return isBedrockRegionPrefix(head) ? head : undefined;
}

/**
 * Replace-or-add a cross-region prefix on a Bedrock model/profile id. A full ARN
 * is first normalized to its trailing id. If the id already carries a recognized
 * region prefix it is REPLACED (`us.anthropic.claude-...` + `eu` →
 * `eu.anthropic.claude-...`); otherwise the prefix is PREPENDED
 * (`anthropic.claude-...` + `eu` → `eu.anthropic.claude-...`). Mirrors Claude
 * Code's applyBedrockRegionPrefix branch logic (utils/model/bedrock.ts).
 */
export function applyBedrockRegionPrefix(id: string, region: BedrockRegionPrefix): string {
  const base = extractModelIdFromArn(id);
  const existing = extractBedrockRegionPrefix(base);
  if (existing) return `${region}.${base.slice(existing.length + 1)}`;
  return `${region}.${base}`;
}

/**
 * Inherit a Bedrock cross-region inference-profile prefix from a PARENT model onto
 * a sub-agent's resolved model id — a data-residency boundary (parity: Claude
 * Code's subagent model resolution carrying the parent's region prefix,
 * `utils/model/agent.ts`). A sub-agent spawned by a Bedrock session must invoke a
 * profile in the SAME account region as its parent, or the call fails / silently
 * crosses regions. The prefix is inherited ONLY when the sub-agent's own id does
 * not already pin a region (an explicit `eu.anthropic.…` sub-agent model wins) and
 * the parent actually carries one; otherwise the sub-agent id is returned
 * unchanged, so this never rewrites a non-cross-region id or a bare canonical id.
 */
export function inheritBedrockRegionPrefix(
  subagentModel: string,
  parentModel: string | undefined,
): string {
  if (!subagentModel) return subagentModel;
  // The sub-agent already pinned its own region → honor it, do not override.
  if (extractBedrockRegionPrefix(subagentModel)) return subagentModel;
  const parentPrefix = parentModel ? extractBedrockRegionPrefix(parentModel) : undefined;
  if (!parentPrefix) return subagentModel;
  return applyBedrockRegionPrefix(subagentModel, parentPrefix);
}

/**
 * The configured cross-region prefix from {@link BEDROCK_REGION_PREFIX_ENV}
 * (trimmed, case-insensitive), or undefined when unset or not a recognized
 * region. Empty when the account does not use cross-region profiles.
 */
export function getBedrockRegionPrefix(env: NodeJS.ProcessEnv = process.env): BedrockRegionPrefix | undefined {
  const raw = (env[BEDROCK_REGION_PREFIX_ENV] ?? '').trim().toLowerCase();
  return isBedrockRegionPrefix(raw) ? raw : undefined;
}

/**
 * The account's Bedrock inference-profile ids, from the comma-separated
 * `AMI_BEDROCK_INFERENCE_PROFILES` env var (trimmed, empties dropped). Empty
 * when unset.
 */
export function getBedrockInferenceProfiles(env: NodeJS.ProcessEnv = process.env): string[] {
  return (env[BEDROCK_INFERENCE_PROFILES_ENV] ?? '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);
}

/**
 * Strip a Bedrock ARN down to its trailing model/inference-profile id.
 *
 * Handles the three ARN shapes AWS emits — foundation-model, inference-profile,
 * and application-inference-profile — by returning the substring after the last
 * `/` (e.g. `arn:aws:bedrock:us-east-1:123:inference-profile/us.anthropic.
 * claude-opus-4-6-v1:0` → `us.anthropic.claude-opus-4-6-v1:0`). Inputs that are
 * not ARNs (no `arn:` prefix) and ARNs without a `/` are returned unchanged, so
 * a plain model id or first-party id passes through untouched. This mirrors
 * Claude Code's extractModelIdFromArn boundary handling and lets a model copied
 * verbatim as an ARN from the AWS console resolve instead of being sent raw to
 * the gateway.
 */
export function extractModelIdFromArn(id: string): string {
  if (!id.startsWith('arn:')) return id;
  const slash = id.lastIndexOf('/');
  return slash === -1 ? id : id.slice(slash + 1);
}

/**
 * Resolve the Bedrock model/inference-profile id to invoke for a canonical
 * first-party model id. A full Bedrock ARN is first normalized to its trailing
 * id (see `extractModelIdFromArn`). Returns the first profile whose id contains
 * the normalized id (case-insensitive, so `claude-opus-4-6` matches
 * `eu.anthropic.claude-opus-4-6-v1:0`).
 *
 * When no explicit profile matches but a cross-region prefix is configured
 * ({@link BEDROCK_REGION_PREFIX_ENV}) AND the id is provider-namespaced (carries
 * a dot, e.g. `anthropic.claude-...` or `us.anthropic.claude-...`), the region
 * prefix is applied (replace-or-add) so the invoked id targets the account's
 * region. A bare canonical id (no namespace, e.g. `claude-opus-4-6`) is returned
 * unchanged — prefixing it would produce an invalid Bedrock id. Otherwise the
 * normalized id is returned unchanged.
 */
export function resolveBedrockModelId(
  canonicalId: string,
  profiles: string[],
  env: NodeJS.ProcessEnv = process.env,
): string {
  if (!canonicalId) return canonicalId;
  const id = extractModelIdFromArn(canonicalId);
  const needle = id.toLowerCase();
  const match = profiles.find(p => p.toLowerCase().includes(needle));
  if (match) return match;
  const region = getBedrockRegionPrefix(env);
  if (region && id.includes('.')) return applyBedrockRegionPrefix(id, region);
  return id;
}

/** What a Bedrock auth/config env knob controls. */
export type BedrockAuthAffordanceKind = 'auth' | 'endpoint' | 'region' | 'model';

/** One surfaced Bedrock auth/config affordance (env var + what it does). */
export interface BedrockAuthAffordance {
  /** The environment variable name. */
  env: string;
  /** Which concern the knob steers. */
  affects: BedrockAuthAffordanceKind;
  /** One-line human description surfaced to the user and in `ami --introspect`. */
  description: string;
}

/**
 * The Bedrock auth/config affordances AMI actually honors, surfaced so a user can
 * discover the non-default ways to authenticate and target a Bedrock account
 * WITHOUT reading source (parity: Claude Code exposes bearer-token auth, an
 * endpoint override, and region selection in utils/model/bedrock.ts; AMI had the
 * behavior wired but never advertised it). This is the SINGLE declaration consumed
 * by both the `/providers` provider hint and the `ami --introspect` manifest — the
 * behavior itself lives in {@link resolveBedrockGateway} (auth + endpoint),
 * {@link getAWSRegion} (region), and {@link resolveBedrockModelId} (profiles).
 */
export const BEDROCK_AUTH_AFFORDANCES: readonly BedrockAuthAffordance[] = [
  {
    env: 'AWS_ACCESS_KEY_ID',
    affects: 'auth',
    description:
      'SigV4 credentials (add AWS_SESSION_TOKEN for SSO / assume-role); the default auth path when no bearer token is set.',
  },
  {
    env: AWS_BEARER_TOKEN_BEDROCK_ENV,
    affects: 'auth',
    description:
      'Bedrock API-key (bearer-token) auth; when set it takes precedence over AWS access keys and skips SigV4 credential resolution (parity: Claude Code AWS_BEARER_TOKEN_BEDROCK).',
  },
  {
    env: ANTHROPIC_BEDROCK_BASE_URL_ENV,
    affects: 'endpoint',
    description:
      'Override the Bedrock gateway endpoint (custom / proxy gateway); otherwise the region-derived bedrock-runtime endpoint is used (parity: Claude Code ANTHROPIC_BEDROCK_BASE_URL).',
  },
  {
    env: 'AWS_REGION',
    affects: 'region',
    description:
      'Bedrock region (falls back to AWS_DEFAULT_REGION, then us-east-1); determines the default gateway endpoint host.',
  },
  {
    env: BEDROCK_INFERENCE_PROFILES_ENV,
    affects: 'model',
    description:
      'Comma-separated account inference-profile ids; a canonical model id is resolved to the matching region-scoped profile the account provisioned.',
  },
  {
    env: BEDROCK_REGION_PREFIX_ENV,
    affects: 'region',
    description:
      'Cross-region inference-profile prefix (us | eu | apac | global) applied to a provider-namespaced id when no explicit profile matches.',
  },
] as const;

/**
 * A compact, single-line key hint for the Amazon Bedrock entry of the `/providers`
 * readout, composed from {@link BEDROCK_AUTH_AFFORDANCES} so the surfaced guidance
 * stays in lock-step with the honored affordances (no second hand-maintained copy).
 * Leads with the default SigV4/SSO path, then the bearer-token and endpoint-override
 * alternatives — surfacing capabilities AMI had but never told the user about.
 */
export function bedrockProvidersKeyHint(): string {
  const bearer = BEDROCK_AUTH_AFFORDANCES.find(a => a.env === AWS_BEARER_TOKEN_BEDROCK_ENV);
  const endpoint = BEDROCK_AUTH_AFFORDANCES.find(a => a.env === ANTHROPIC_BEDROCK_BASE_URL_ENV);
  return [
    'AKIA... (+ AWS_SESSION_TOKEN for SSO / assume-role)',
    `or set ${bearer?.env} for API-key (bearer-token) auth`,
    `${endpoint?.env} overrides the gateway endpoint`,
  ].join('; ');
}
