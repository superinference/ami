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
