// OpenRouter Zero-Data-Retention (ZDR) helpers.
//
// If an OpenRouter account requires ZDR, requests to endpoints without a ZDR policy
// fail with "0 endpoints … ZDR violation (account settings)". To stop users hitting
// that, AMI can (a) force ZDR routing per request (provider.zdr), (b) list only
// ZDR-eligible models, and (c) turn the raw 404 into actionable guidance.
//
// Data source: GET https://openrouter.ai/api/v1/endpoints/zdr →
//   { data: [{ model_id: "vendor/model", provider_name, safety_retention, ... }] }
// (auto-updated by OpenRouter as provider data policies change).
//
// Pure + deterministic (the network fetch takes an injectable fetch impl) so every
// branch is unit-testable.

import { stripOpenRouterPrefix } from './provider';
import type { ProviderConfig } from './types';

export const OPENROUTER_ZDR_ENDPOINT = 'https://openrouter.ai/api/v1/endpoints/zdr';
export const OPENROUTER_PRIVACY_URL = 'https://openrouter.ai/settings/privacy';
/** OpenRouter meta-model: its router auto-selects an available endpoint. Combined
 *  with provider.zdr it auto-selects a Zero-Data-Retention endpoint. */
export const OPENROUTER_AUTO_MODEL = 'openrouter/auto';

/** Normalize a model slug for ZDR matching. [FUNCTIONAL] OpenRouter slugs are
 *  canonically lowercase, but the /endpoints/zdr feed and AMI-stored ids can carry
 *  surrounding whitespace or differing case; matching both the parsed set and the
 *  eligibility query through here (trim + lowercase) makes lookups robust to those
 *  cosmetic differences instead of silently missing an eligible model. */
export function normalizeZdrSlug(slug: string): string {
  return slug.trim().toLowerCase();
}

/** Parse the /endpoints/zdr response into a Set of ZDR-eligible `model_id` slugs
 *  (the stripped `vendor/model` form). Tolerant of any malformed shape → empty set.
 *  [FUNCTIONAL] Each id is normalized (trim + lowercase) so the set matches regardless
 *  of feed-side casing/whitespace; entries that normalize to empty are dropped. */
export function parseZdrModelIds(json: unknown): Set<string> {
  const out = new Set<string>();
  const data = (json as { data?: unknown } | null | undefined)?.data;
  if (!Array.isArray(data)) return out;
  for (const entry of data) {
    const id = (entry as { model_id?: unknown })?.model_id;
    if (typeof id === 'string') {
      const norm = normalizeZdrSlug(id);
      if (norm.length > 0) out.add(norm);
    }
  }
  return out;
}

/** Is a model ZDR-eligible? AMI stores OpenRouter ids namespaced
 *  ("openrouter/vendor/model"); the ZDR set uses the bare `vendor/model` slug, so
 *  strip the prefix first. Native single-token routers (openrouter/auto) have no
 *  bare slug and are treated as NOT individually ZDR-listed (routing-dependent). */
export function isZdrEligible(model: string, zdr: ReadonlySet<string>): boolean {
  if (zdr.size === 0) return true; // unknown ZDR set → do not filter anything out
  // [FUNCTIONAL] Normalize the query the same way parseZdrModelIds normalizes the set,
  // so case/whitespace differences in the stored id do not cause a false miss.
  return zdr.has(normalizeZdrSlug(stripOpenRouterPrefix(model)));
}

/** Filter AMI-namespaced model ids to the ZDR-eligible ones. With an empty/unknown
 *  ZDR set, returns the input unchanged (never hide everything on a fetch failure). */
export function filterZdrModels<T extends string>(models: readonly T[], zdr: ReadonlySet<string>): T[] {
  return models.filter((m) => isZdrEligible(m, zdr));
}

export interface ZdrErrorInfo {
  isZdr: boolean;
  /** Actionable guidance for the TUI when isZdr is true. */
  guidance?: string;
}

/** Detect OpenRouter's ZDR-violation 404 (vs a real "model not found") and return
 *  actionable guidance. Keyed on the ZDR/data-policy wording + the "endpoint(s)" noun
 *  so a generic 404 is not misclassified. */
export function classifyZdrError(message: string): ZdrErrorInfo {
  // [FUNCTIONAL] Only trust a real string; a non-string runtime value (e.g. an Error
  // object passed by mistake) must not be regex-coerced into a false ZDR match.
  const m = typeof message === 'string' ? message : '';
  const mentionsZdr = /\bZDR\b|zero[\s-]?data[\s-]?retention|data policy|guardrail/i.test(m);
  // "0 endpoints" already contains "endpoint", so a single /endpoint/i covers both.
  const mentionsEndpoint = /endpoint/i.test(m);
  if (!mentionsZdr || !mentionsEndpoint) return { isZdr: false };
  return {
    isZdr: true,
    guidance:
      'Your OpenRouter account requires Zero-Data-Retention (ZDR), which excludes this ' +
      "model's endpoint — this is an account setting, not a bad model name. Pick a " +
      `ZDR-eligible model (AMI can list them from ${OPENROUTER_ZDR_ENDPOINT}) or adjust ` +
      `your data policy at ${OPENROUTER_PRIVACY_URL}.`,
  };
}

/** Merge ZDR routing (`provider.zdr = true`) into an existing extraBody, preserving
 *  any other extraBody fields and other `provider.*` routing keys. The result is
 *  deep-merged into the request body by the provider's extraBody passthrough, forcing
 *  OpenRouter to route only to Zero-Data-Retention endpoints. */
export function withZdrProviderExtraBody(extraBody: Record<string, unknown> | undefined): Record<string, unknown> {
  const eb = extraBody ?? {};
  // [FUNCTIONAL] Arrays are also `typeof 'object'`; spreading one would corrupt the
  // provider routing block into numeric index keys, so treat an array (or any
  // non-plain-object) provider value as empty — only a real object is merged.
  const existingProvider = (eb.provider && typeof eb.provider === 'object' && !Array.isArray(eb.provider))
    ? eb.provider as Record<string, unknown> : {};
  return { ...eb, provider: { ...existingProvider, zdr: true } };
}

/** Transform a provider config into OpenRouter's ZDR-forced auto-router: the
 *  `openrouter/auto` meta-model (OpenRouter picks an available endpoint) constrained
 *  to ZDR via `provider.zdr`. Used by the engine to auto-recover from a ZDR-violation
 *  404 — the configured model has no ZDR endpoint, so auto-routing picks one that does. */
export function openRouterZdrAutoConfig(base: ProviderConfig): ProviderConfig {
  return { ...base, model: OPENROUTER_AUTO_MODEL, extraBody: withZdrProviderExtraBody(base.extraBody) };
}

/** Fetch + parse the ZDR-eligible model set. Never throws — any network/parse failure
 *  yields an empty set (callers treat empty as "unknown → do not filter"). The fetch
 *  impl is injectable for deterministic unit tests. */
export async function fetchZdrModelIds(
  opts: { apiKey?: string; fetchImpl?: typeof fetch; timeoutMs?: number } = {},
): Promise<Set<string>> {
  const doFetch = opts.fetchImpl ?? fetch;
  // [FUNCTIONAL] Guard the timeout: a non-positive / NaN / non-finite value would make
  // AbortSignal.timeout abort the request immediately (or throw), silently defeating
  // the fetch. Fall back to the 5s default for any invalid value.
  const rawTimeout = opts.timeoutMs;
  const timeoutMs = (typeof rawTimeout === 'number' && Number.isFinite(rawTimeout) && rawTimeout > 0)
    ? rawTimeout : 5000;
  try {
    const headers: Record<string, string> = {};
    if (opts.apiKey) headers.Authorization = `Bearer ${opts.apiKey}`;
    const res = await doFetch(OPENROUTER_ZDR_ENDPOINT, {
      headers,
      signal: AbortSignal.timeout(timeoutMs),
    });
    if (!res.ok) return new Set();
    return parseZdrModelIds(await res.json());
  } catch {
    return new Set();
  }
}
