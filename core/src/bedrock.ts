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
 * Resolve the Bedrock model/inference-profile id to invoke for a canonical
 * first-party model id. Returns the first profile whose id contains the
 * canonical id (case-insensitive, so `claude-opus-4-6` matches
 * `eu.anthropic.claude-opus-4-6-v1:0`), or the canonical id unchanged when no
 * profile matches (or no profiles are configured).
 */
export function resolveBedrockModelId(canonicalId: string, profiles: string[]): string {
  if (!canonicalId) return canonicalId;
  const needle = canonicalId.toLowerCase();
  const match = profiles.find(p => p.toLowerCase().includes(needle));
  return match ?? canonicalId;
}
