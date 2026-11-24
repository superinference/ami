import { streamText, generateText, jsonSchema, Output } from 'ai';
import { stripOpenRouterPrefix } from './openrouter-prefix';
export { stripOpenRouterPrefix } from './openrouter-prefix';
import { MODEL_PREFERENCE } from './model-preference';
import type { ToolSet, ToolCallRepairFunction } from 'ai';
import { createOpenAI } from '@ai-sdk/openai';
import { createAnthropic } from '@ai-sdk/anthropic';
import { createGoogleGenerativeAI } from '@ai-sdk/google';
import { createVertexAnthropic } from '@ai-sdk/google-vertex/anthropic';
import { createXai } from '@ai-sdk/xai';
import { createMistral } from '@ai-sdk/mistral';
import { createCohere } from '@ai-sdk/cohere';
import { createDeepSeek } from '@ai-sdk/deepseek';
import { createGroq } from '@ai-sdk/groq';
import { createTogetherAI } from '@ai-sdk/togetherai';
import { createFireworks } from '@ai-sdk/fireworks';
import { createAlibaba } from '@ai-sdk/alibaba';
import { createDeepInfra } from '@ai-sdk/deepinfra';
import { createCerebras } from '@ai-sdk/cerebras';
import { createHuggingFace } from '@ai-sdk/huggingface';
import 'node:zlib';
import { randomUUID } from 'node:crypto';
import { appendFile as fsAppendFile, mkdir as fsMkdir } from 'node:fs/promises';
import 'node:os';
import { join as pathJoin, dirname as pathDirname } from 'node:path';
import { resolveTemperature, fitOutputTokens, getModelMaxOutputTokens, getContextWindow, collectModelRecords, pickAdvertisedContextWindow, huggingfaceRepoCandidates, requiresResponsesAPI, supportsOneMillionContextBeta, normalizeModelStringForAPI } from './model-capabilities';
import { sanitizeToolCallIds, buildConversationCachePoints, healOrphanedToolCalls, stripUnavailableToolReferences, stripEmptyTextBlocks, stripExcessMediaItems, validateImagesForAPI, getPromptCachingEnabled, getStrictToolResultPairing, ToolPairingError } from './provider-transform';

import './utils/tokens';
import { memoizeWithTTLAsync } from './utils/memoize';
import './utils/combined-abort-signal';
import { armUnrefTimer } from './utils/abort';
import { sleep } from './utils/sleep';
import { parseRateLimitHeaders, shouldBackoff, suggestedWaitMs } from './rate-limit-parser';
import { isGatewayRejection, findErrorCode, connectionErrorDiagnostic, isAbort, detectPromptTooLong, parseMockRateLimit, MOCK_RATE_LIMIT_ENV } from './error-classifier';
import { maybeThrowFault } from './fault-injection';
import { getMTLSConfig, getTLSConnectOptions, MTLS_ENV, describeCATrustStore, NODE_EXTRA_CA_CERTS_ENV, SYSTEM_CA_NODE_OPTIONS } from './utils/mtls';
import './ca-certs';
import { resolveBedrockModelId, getBedrockInferenceProfiles, AWS_BEARER_TOKEN_BEDROCK_ENV } from './bedrock';
import { assertValidStsEnvCredentials } from './utils/aws';
import { isEnvTruthy } from './analytics';
import { log, logError, recordDiagnostic } from './logger';
import { buildRequestDumpEntries, getOrCreateDumpState } from './dump-prompts';
import './tools/tool-utils';
import './tools/tool-schema-cache';
import type { ProviderConfig, Message, ToolDefinition, StreamChunk, ThinkingConfig, RetryErrorKind } from './types';
import { CANONICAL_MODEL_EQUIVALENTS, ENV_KEYS, KEY_PREFIXES, PROVIDER_BASE_URLS, extractRetryAfterSeconds, fetchWithTimeout, huggingFaceEndpoint, huggingFaceToken, inferProviderFromApiKey, inferProviderFromBaseUrl, inferProviderFromEnv, isVertexAIConfigured, readHuggingFaceFile, resolveAvailableModel } from './provider-interactive-auth-status';
import type { CanonicalModelConfig, ProviderDescriptor, ProviderName } from './provider-interactive-auth-status';
import { REQUEST_ID_HEADER, appendRequestIdToError, deepMergeRecords, dumpPromptsEnabled, getPromptDumpDir, openAIPromptCacheBody, readDumpSseChunks, resolveBedrockGateway, resolveExtraBodyFromEnv, withEagerToolStreaming, withExtraBody, withExtraHeaders, withGzipCompression } from './provider-with-eager-tool-streaming';
import type { PromptDumpDeps } from './provider-with-eager-tool-streaming';
import { CLAUDE_CODE_BETA, CONTEXT_MANAGEMENT_BETA, EXTENDED_CACHE_TTL_BETA, INTERLEAVED_THINKING_BETA, ONE_MILLION_CONTEXT_BETA, PROMPT_CACHING_SCOPE_BETA, STRUCTURED_OUTPUTS_BETA, TOKEN_EFFICIENT_TOOLS_BETA, buildMetadataProviderOptions, experimentalBetasDisabled, fineGrainedToolStreamingEnabled, getExtraAnthropicBetas, interleavedThinkingDisabled, mergeProviderOptionFragments, modelSupportsStructuredOutputs, resolveMetadataUserIdValue, structuredOutputsEnabled, supportsInterleavedThinking, tokenEfficientToolsEnabled } from './provider-inject-anthropic-cache-control';
import { TOKEN_COUNT_THINKING_BUDGET, buildAnthropicContextManagement, buildProxyConnectOptions, connectOptionsCacheTag, hostMatchesNoProxy, proxyTransportHooks, recordNetworkTrustDiagnostics, requestUrl, withAnthropicBetaHeader } from './provider-record-network-trust-diagnostics';
import type { CountTokensRequest } from './provider-record-network-trust-diagnostics';
import { REDACTED_THINKING_PLACEHOLDER, convertMessages, convertToolsForSDK, extractCacheCreationSplit, extractCacheCreationTokens, extractCachedPromptTokens, extractReasoningTokens, extractRedactedThinkingData, getVertexRegionForModel, hoistSystemPrompt, markLastToolCacheControl, withDynamicBoundaryStrip } from './provider-convert-messages';
import { BASE_DELAY_MS, EMPTY_STREAM_ERROR, MAX_RETRIES, RETRY_HEARTBEAT_INTERVAL_MS, betaSettingsFingerprint, buildAnthropicCacheControl, buildThinkingOptions, isEmptyStreamError, promptTokensForFit, resolveCacheTtl, resolvePromptCacheScope, resolvePromptCacheStrategy, sleepWithRetryHeartbeat, withVertexAnthropicCache } from './provider-build-thinking-options';
import { DEFAULT_REQUEST_TIMEOUT_MS, OVERLOADED_ERROR_STATUS, STREAM_STALL_THRESHOLD_MS, adjustParamsForNonStreaming, buildMockRateLimitChunk, envApiTimeoutMs, extractStatusCode, formatSslError, formatTimeoutError, generateTextToStreamAdapter, identifyGateway, isOverloadedError, isSslError, isStaleConnectionError, nonStreamingFallbackEnabled, parseMaxTokensContextOverflowError, recoverMaxTokensFromContextOverflow, sanitizeErrorMessage } from './provider-generate-text-to-stream-adapter';
import type { QuerySource } from './provider-generate-text-to-stream-adapter';
export { CANONICAL_MODEL_EQUIVALENTS, extractRetryAfterSeconds, getAWSRegion, idleInteractiveAuthStatus } from './provider-interactive-auth-status';
export type { CanonicalModelConfig, InteractiveAuthStatus, ProviderDescriptor } from './provider-interactive-auth-status';
export { EXTRA_BODY_ENV_VARS, EXTRA_METADATA_ENV_VARS, METADATA_USER_ID_ENV_VARS, REQUEST_ID_HEADER, appendRequestIdToError, deepMergeRecords, dumpPromptsEnabled, getPromptDumpDir, isPlainRecord, normalizeToolChoice, openAIPromptCacheBody, resolveBedrockGateway, resolveExtraBodyFromEnv, resolveExtraMetadata, resolveMetadataUserId, withEagerToolStreaming, withExtraBody, withExtraHeaders, withGzipCompression } from './provider-with-eager-tool-streaming';
export type { PromptDumpDeps } from './provider-with-eager-tool-streaming';
export { CLAUDE_CODE_BETA, CONTEXT_MANAGEMENT_BETA, EXTENDED_CACHE_TTL_BETA, INTERLEAVED_THINKING_BETA, ONE_MILLION_CONTEXT_BETA, PROMPT_CACHING_SCOPE_BETA, STRUCTURED_OUTPUTS_BETA, SYSTEM_PROMPT_DYNAMIC_BOUNDARY, TOKEN_EFFICIENT_TOOLS_BETA, buildMetadataProviderOptions, experimentalBetasDisabled, fineGrainedToolStreamingEnabled, getExtraAnthropicBetas, injectAnthropicCacheControl, isSafeBetaToken, mergeBetaTokens, mergeProviderOptionFragments, modelSupportsStructuredOutputs, resolveMetadataUserIdValue, structuredOutputsEnabled, supportsInterleavedThinking, toolDefinitionsToOpenAI, toolUseSummaryLabelEnabled } from './provider-inject-anthropic-cache-control';
export type { AnthropicBetaDescriptor } from './provider-inject-anthropic-cache-control';
export { API_CONTEXT_EDIT_ENV, API_CONTEXT_EDIT_MAX_INPUT_TOKENS_ENV, API_CONTEXT_EDIT_TARGET_INPUT_TOKENS_ENV, DEFAULT_CONTEXT_EDIT_MAX_INPUT_TOKENS, DEFAULT_CONTEXT_EDIT_TARGET_INPUT_TOKENS, NETWORK_TRUST_DIAGNOSTIC_EVENT, TOKEN_COUNT_MAX_TOKENS, TOKEN_COUNT_THINKING_BUDGET, buildAnthropicContextManagement, buildProxyConnectOptions, describeAnthropicBetas, getAddressFamily, proxyResolvesHosts, proxyTransportHooks, recordNetworkTrustDiagnostics, withAnthropicBetaHeader } from './provider-record-network-trust-diagnostics';
export type { CountTokensRequest } from './provider-record-network-trust-diagnostics';
export { ALWAYS_ENABLE_EFFORT_ENV_VARS, REDACTED_THINKING_PLACEHOLDER, alwaysEnableEffort, convertMessages, convertToolsForSDK, extractCacheCreationSplit, extractCacheCreationTokens, extractCachedPromptTokens, extractReasoningTokens, extractRedactedThinkingData, getVertexRegionForModel, hoistSystemPrompt, markLastToolCacheControl, withDynamicBoundaryStrip } from './provider-convert-messages';
export { DISABLE_ADAPTIVE_THINKING_ENV_VARS, DISABLE_THINKING_ENV_VARS, EMPTY_STREAM_ERROR, PROMPT_CACHE_SCOPES, PROMPT_CACHE_SCOPE_ENV, PROMPT_CACHE_STRATEGYS, PROMPT_CACHE_STRATEGY_ENV, PROMPT_CACHE_TTL_ENV, RETRY_HEARTBEAT_INTERVAL_MS, adaptiveThinkingDisabledByEnv, buildAnthropicCacheControl, buildRepairToolCall, buildThinkingOptions, isEmptyStreamError, resolveCacheTtl, resolvePromptCacheScope, resolvePromptCacheStrategy, sleepWithRetryHeartbeat, thinkingDisabledByEnv, withCacheSafeToolDeny, withVertexAnthropicCache } from './provider-build-thinking-options';
export type { PromptCacheScope, PromptCacheStrategy } from './provider-build-thinking-options';
export { CACHE_MISS_WARN_THRESHOLD, CONTEXT_OVERFLOW_SAFETY_BUFFER, CREDENTIAL_PATTERNS, DEFAULT_REQUEST_TIMEOUT_MS, FLOOR_OUTPUT_TOKENS, MAX_NON_STREAMING_TOKENS, MAX_RETRY_AFTER_MS, STREAM_STALL_THRESHOLD_MS, adjustParamsForNonStreaming, envApiTimeoutMs, formatSslError, formatTimeoutError, generateTextToStreamAdapter, identifyGateway, isSslError, isStaleConnectionError, nonStreamingFallbackEnabled, parseMaxTokensContextOverflowError, parseRetryAfterHeader, recoverMaxTokensFromContextOverflow, sanitizeErrorMessage } from './provider-generate-text-to-stream-adapter';
export type { QuerySource } from './provider-generate-text-to-stream-adapter';
export { PromptCacheTracker } from './provider-prompt-cache-tracker';

const NATIVE_SDK_PROVIDERS = new Set<ProviderName>([
  'xai', 'mistral', 'cohere', 'deepseek', 'groq', 'togetherai',
  'fireworks', 'alibaba', 'deepinfra', 'cerebras', 'huggingface',
  'google', 'anthropic',
]);

export function describeProviders(): ProviderDescriptor[] {
  return Object.entries(MODEL_PREFERENCE).map(([id, models]) => ({
    id,
    envVars: ENV_KEYS.filter(e => e.provider === id).map(e => e.env),
    baseUrl: PROVIDER_BASE_URLS[id as ProviderName] || '',
    models,
    nativeSdk: NATIVE_SDK_PROVIDERS.has(id as ProviderName),
    keyPrefixes: KEY_PREFIXES.filter(k => k.provider === id).map(k => k.prefix),
  }));
}

export { resolveAvailableModel, MODEL_PREFERENCE, PROVIDER_BASE_URLS, NATIVE_SDK_PROVIDERS, inferProviderFromBaseUrl, inferProviderFromEnv };

/**
 * Reverse index: any provider-specific model id → its canonical logical key. Built
 * once from {@link CANONICAL_MODEL_EQUIVALENTS} so a configured/overridden id (in
 * ANY provider's naming) resolves back to the logical model. Mirrors the reference's
 * `CANONICAL_ID_TO_KEY`.
 */
export const CANONICAL_ID_TO_KEY: Record<string, string> = (() => {
  const index: Record<string, string> = {};
  for (const [key, config] of Object.entries(CANONICAL_MODEL_EQUIVALENTS)) {
    index[key] = key;
    for (const wireId of Object.values(config)) index[wireId] = key;
  }
  return index;
})();

/**
 * Translate `modelId` (expressed in ANY provider's naming) to the wire string the
 * ACTIVE `provider` requires. Returns `modelId` unchanged when it is not a known
 * canonical id, when `provider` is undefined, or when the provider has no mapping
 * in {@link CanonicalModelConfig} (e.g. a non-the reference provider) — so non-the reference and
 * unknown models always pass through untouched. Idempotent: a model already in the
 * target provider's naming maps to itself.
 */
export function resolveModelForProvider(
  modelId: string,
  provider: ProviderName | string | undefined,
): string {
  if (!provider) return modelId;
  const key = CANONICAL_ID_TO_KEY[modelId];
  if (!key) return modelId;
  const config = CANONICAL_MODEL_EQUIVALENTS[key];
  if (provider in config) {
    return config[provider as keyof CanonicalModelConfig];
  }
  return modelId;
}

/**
 * Shared TTL + single-flight memoizer for the provider `/models` list. Concurrent
 * cold-miss callers for the same endpoint coalesce onto ONE fetch (inFlight
 * dedup); a successful result is cached for the TTL and served stale-while a
 * background refresh runs. A failed fetch THROWS so nothing is cached and the
 * next call retries — the inner fetcher throws on !ok / network error and
 * {@link loadModelRecords} catches it to fall back to `[]`.
 */
const loadModelRecordsCached = memoizeWithTTLAsync(
  async (url: string, headers: Record<string, string>): Promise<Record<string, unknown>[]> => {
    const res = await fetchWithTimeout(url, { headers }, 1500);
    if (!res.ok) throw new Error(`model list fetch failed: ${res.status}`);
    return collectModelRecords(await res.json());
  },
);

/**
 * Host-boundary classification of a first-party API base URL. Parses `baseUrl`
 * with `new URL()` inside a try/catch (returning `false` on a malformed URL — the
 * safe/conservative default) and matches its real `hostname` (case-insensitive)
 * against `domains`: a domain matches when the host equals it OR is a genuine
 * subdomain of it (`host === d || host.endsWith('.' + d)`).
 *
 * This replaces host-spoofable `baseUrl.includes('anthropic.com')` substring
 * tests, which falsely matched lookalikes like `api.anthropic.com.evil.example`,
 * `not-anthropic.com`, and `https://evil.example/?x=anthropic.com` — so AMI would
 * attach official credentials/headers (x-api-key, anthropic-version) and route to
 * the first-party SDK path for an attacker-controlled host. Mirrors the reference's
 * `isFirstPartyAnthropicBaseUrl()` (utils/model/providers.ts) parse-then-exact-host
 * pattern, generalized over an allowlist and permitting genuine subdomains (AMI's
 * established convention — see ProviderCoordinator).
 */
export function isFirstPartyBaseUrl(
  baseUrl: string | undefined,
  domains: readonly string[],
): boolean {
  if (!baseUrl) return false;
  let host: string;
  try {
    host = new URL(baseUrl).hostname.toLowerCase();
  } catch {
    return false;
  }
  return domains.some(d => {
    const dl = d.toLowerCase();
    return host === dl || host.endsWith(`.${dl}`);
  });
}

function modelsEndpoint(config: ProviderConfig): { url: string; headers: Record<string, string> } | null {
  const provider = config.provider ?? '';
  const baseUrl = (config.baseUrl || '').replace(/\/+$/, '');
  const apiKey = config.apiKey || '';
  const officialAnthropic = (provider === 'anthropic' || provider === 'anthropic-vertex')
    && (!baseUrl || baseUrl.includes('anthropic.com'));
  const officialGoogle = (provider === 'google' || provider === 'google-vertex')
    && (!baseUrl || baseUrl.includes('googleapis.com'));

  if (officialAnthropic || isFirstPartyBaseUrl(baseUrl, ['anthropic.com'])) {
    const root = baseUrl.includes('anthropic.com') ? baseUrl : 'https://api.anthropic.com';
    const url = /\/v1$/.test(root) ? `${root}/models` : `${root}/v1/models`;
    return {
      url,
      headers: { 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    };
  }
  if (officialGoogle || isFirstPartyBaseUrl(baseUrl, ['generativelanguage.googleapis.com'])) {
    const root = baseUrl.includes('generativelanguage.googleapis.com')
      ? baseUrl
      : 'https://generativelanguage.googleapis.com/v1beta';
    return {
      url: root.endsWith('/models') ? root : `${root}/models`,
      headers: { 'x-goog-api-key': apiKey },
    };
  }
  if (!baseUrl) return null;
  return {
    url: baseUrl.endsWith('/models') ? baseUrl : `${baseUrl}/models`,
    headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : {},
  };
}

async function loadModelRecords(config: ProviderConfig): Promise<Record<string, unknown>[]> {
  const endpoint = modelsEndpoint(config);
  if (!endpoint) return [];
  try {
    return await loadModelRecordsCached(endpoint.url, endpoint.headers);
  } catch {
    // Non-ok / network failure: nothing was cached, so the caller falls back to
    // the model table and the next call retries the fetch.
    return [];
  }
}

/**
 * Context window from the provider's model list.
 * Returns undefined when the server has no length, so the caller can try a
 * Hugging Face model card, then the proprietary table, then the default.
 */
export async function fetchAdvertisedContextWindow(config: ProviderConfig): Promise<number | undefined> {
  if (!config.model) return undefined;
  const records = await loadModelRecords(config);
  return pickAdvertisedContextWindow(records, config.model);
}

/**
 * Shared TTL + single-flight memoizer for the Hugging Face model-card lookup.
 * Concurrent cold-miss callers for the same (endpoint, repo, token) coalesce onto
 * ONE pair of fetches; a successful length is cached for the TTL. When either
 * file fetch fails at the network level (`'failed'`) the fetcher THROWS so
 * nothing is cached and the next call retries; a missing/length-less repo caches
 * `undefined` so the caller stops re-probing a repo with no published length.
 */
const loadHuggingFaceContextWindow = memoizeWithTTLAsync(
  async (endpoint: string, repo: string, token: string | undefined): Promise<number | undefined> => {
    const headers: Record<string, string> = {};
    if (token) headers.Authorization = `Bearer ${token}`;
    const root = repo.split('/').map(part => encodeURIComponent(part)).join('/');
    const lengths = await Promise.all([
      readHuggingFaceFile(`${endpoint}/${root}/resolve/main/config.json`, headers),
      readHuggingFaceFile(`${endpoint}/${root}/resolve/main/tokenizer_config.json`, headers),
    ]);
    if (lengths.some(length => length === 'failed')) throw new Error('hugging face fetch failed');
    const usable = lengths.filter((length): length is number => typeof length === 'number');
    if (usable.length === 0) return undefined;
    return Math.max(...usable);
  },
);

/**
 * Context length from the Hub model card (`config.json` and
 * `tokenizer_config.json`). A network failure, a missing repo, or no
 * published length returns undefined so the caller can use the table.
 * `HF_ENDPOINT` selects the hub, including an offline mirror.
 */
export async function fetchHuggingFaceContextWindow(repo: string, config: ProviderConfig): Promise<number | undefined> {
  const endpoint = huggingFaceEndpoint();
  const token = huggingFaceToken(config);
  try {
    return await loadHuggingFaceContextWindow(endpoint, repo, token);
  } catch {
    // Transient network failure: nothing cached, retry next call; fall back to
    // the proprietary table for now.
    return undefined;
  }
}

/**
 * Served length, then a Hugging Face model card, then the model table,
 * then the 128k default. An explicit `contextWindow` on the config is kept:
 * provision already stored `max_model_len` there.
 */
export async function resolveContextWindow(config: ProviderConfig): Promise<number> {
  if (typeof config.contextWindow === 'number' && config.contextWindow > 0) {
    return config.contextWindow;
  }
  const records = config.model ? await loadModelRecords(config) : [];
  const fetched = config.model ? pickAdvertisedContextWindow(records, config.model) : undefined;
  if (fetched !== undefined) return fetched;
  for (const repo of huggingfaceRepoCandidates(config.model || '', records)) {
    const card = await fetchHuggingFaceContextWindow(repo, config);
    if (card !== undefined) return card;
  }
  return getContextWindow(config.model || '');
}

// ---------------------------------------------------------------------------
// Model resolution — pick the right AI SDK provider
// ---------------------------------------------------------------------------

// Providers that use native AI SDK packages (not OpenAI-compatible shim)
const NATIVE_PROVIDERS = new Set<ProviderName>([
  'anthropic', 'anthropic-vertex', 'google', 'google-vertex',
  'xai', 'mistral', 'cohere', 'deepseek', 'groq',
  'togetherai', 'fireworks', 'alibaba', 'deepinfra', 'cerebras',
  'huggingface',
]);

// The Bedrock bearer-token env-var name is declared canonically in `./bedrock`
// (alongside the other Bedrock env knobs) and re-exported here so this module's
// existing importers keep resolving it from `provider`.
export { AWS_BEARER_TOKEN_BEDROCK_ENV };

// Providers that always route through OpenAI-compatible endpoint
const OPENAI_COMPAT_PROVIDERS = new Set<ProviderName>([
  'openai', 'perplexity', 'openrouter', 'ollama', 'luma', 'nvidia', 'ai-gateway',
]);

/**
 * Test seam for request-id generation so tests can assert a deterministic id.
 * Defaults to crypto.randomUUID so production behavior is unchanged.
 */
export const requestIdHooks: { generate: () => string } = { generate: randomUUID };

/**
 * Managed cloud gateways that log NO client request-id and whose strict proxies
 * risk 4xx-rejecting an unknown header (inc-4029 class). Mirrors the reference's
 * `getAPIProvider() === 'firstParty'` gate in buildFetch (client.ts:L364-367),
 * which injects the correlation header ONLY on the first-party Anthropic path and
 * never on Bedrock/Vertex/Foundry.
 */
const REQUEST_ID_INCOMPATIBLE_PROVIDERS: ReadonlySet<string> = new Set([
  'amazon-bedrock',
  'anthropic-vertex',
  'google-vertex',
  'azure-openai',
]);

/**
 * True when the resolved endpoint is safe to carry AMI's client request-id
 * header — i.e. a direct first-party-style API rather than a managed cloud
 * gateway (Bedrock/Vertex/Foundry) that drops it and may reject unknown headers.
 */
export function endpointAcceptsClientRequestId(provider: string | undefined): boolean {
  return !(provider !== undefined && REQUEST_ID_INCOMPATIBLE_PROVIDERS.has(provider));
}

/**
 * L6: build the client request-id header for an outgoing API call. Returns a
 * single `{ 'x-superinference-request-id': <uuid> }` entry so logs on both ends
 * can be correlated. Purely ADDITIVE: if the caller/config already set that
 * header (any casing), returns `{}` so the caller's value is never overwritten.
 * Also returns `{}` when `inject` is false (the endpoint is a managed gateway
 * that rejects unknown headers — see {@link endpointAcceptsClientRequestId}).
 */
export function buildRequestIdHeader(
  existingHeaders?: Record<string, string>,
  inject = true,
): Record<string, string> {
  if (!inject) return {};
  if (existingHeaders) {
    for (const key of Object.keys(existingHeaders)) {
      if (key.toLowerCase() === REQUEST_ID_HEADER) return {};
    }
  }
  return { [REQUEST_ID_HEADER]: requestIdHooks.generate() };
}

/**
 * Resolve the client request-id to use for a call: REUSE a caller-provided
 * `x-superinference-request-id` header (any casing) so the id on the wire and the
 * id surfaced on an error are the same, otherwise MINT a fresh one — but only when
 * {@link endpointAcceptsClientRequestId} allows it (managed gateways get no id, as
 * their strict proxies reject unknown headers). Returns undefined when no id should
 * be used, so the gateway path stays header-free and unchanged.
 */
export function resolveClientRequestId(
  config: Pick<ProviderConfig, 'extraHeaders'>,
  provider: string | undefined,
): string | undefined {
  if (config.extraHeaders) {
    for (const [k, v] of Object.entries(config.extraHeaders)) {
      if (k.toLowerCase() === REQUEST_ID_HEADER && v) return v;
    }
  }
  return endpointAcceptsClientRequestId(provider) ? requestIdHooks.generate() : undefined;
}

/**
 * Response-header names a provider/gateway may carry a per-request correlation id
 * under (first match wins). This is the SERVER-returned id surfaced on a `done` /
 * `error` chunk's {@link StreamChunk.responseHeaders} — distinct from the CLIENT
 * id minted via {@link buildRequestIdHeader} ({@link REQUEST_ID_HEADER}), though
 * the latter is in the list so a gateway that echoes it back is still matched.
 * Single source for every caller that needs the generation request-id (title /
 * branch-name aux queries, the cache-preserving prompt-suggestion fork): parity
 * with the reference recording `response.message.id` / the first assistant message's
 * `requestId` for RL-dataset joins. Lower-cased lookup.
 */
export const PROVIDER_RESPONSE_REQUEST_ID_HEADERS = [
  'anthropic-request-id',
  REQUEST_ID_HEADER,
  'x-request-id',
  'request-id',
  'openai-request-id',
] as const;

/**
 * Pull the provider/gateway correlation id out of a response's headers
 * (case-insensitive, first {@link PROVIDER_RESPONSE_REQUEST_ID_HEADERS} match).
 * Returns `undefined` when no header is present or `headers` is omitted. Used to
 * capture the generation request-id of a forked/background LLM run so it can be
 * joined back to telemetry / RL datasets (parity: the reference's `generationRequestId`).
 */
export function extractProviderRequestId(headers?: Record<string, string>): string | undefined {
  if (!headers) return undefined;
  const lower: Record<string, string> = {};
  for (const [k, v] of Object.entries(headers)) lower[k.toLowerCase()] = v;
  for (const name of PROVIDER_RESPONSE_REQUEST_ID_HEADERS) {
    const v = lower[name];
    if (v) return v;
  }
  return undefined;
}

/**
 * Wrap a fetch so each outgoing API request emits a debug trace of its pathname
 * and client request-id to the shared logger (gated by `AMI_DEBUG_LOG_LEVEL` /
 * component filter, like every other `log()` call — no new env knob). The id is
 * generated specifically so TIMEOUTS, which return no server request-id, can
 * still be correlated with server logs. Mirrors the reference's `[API REQUEST]`
 * buildFetch trace (client.ts:L380-383). Never throws — tracing can't perturb the
 * live request. No-op passthrough when `baseFetch` is undefined and no wrapper is
 * otherwise needed is handled by the caller.
 */
export function withRequestIdDebugTrace(
  baseFetch: typeof globalThis.fetch | undefined,
  source: string,
): typeof globalThis.fetch {
  const base = baseFetch ?? globalThis.fetch;
  return (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    let pathname = '<unknown>';
    try {
      const url = requestUrl(input as string | URL | { url?: unknown });
      const headers = new Headers(init?.headers as Record<string, string> | undefined);
      const id = headers.get(REQUEST_ID_HEADER);
      pathname = url ? new URL(url).pathname : '<unknown>';
      log('api', `[API REQUEST] ${pathname}${id ? ` ${REQUEST_ID_HEADER}=${id}` : ''} source=${source}`);
    } catch {
      // Never let tracing crash the fetch.
    }
    // Complete the request→response→error trace triad: forward the provider-SDK
    // transport RESPONSE status and any transport ERROR to the same debug sink the
    // request line uses (routed to stderr when isDebugToStderr()). Mirrors the reference
    // Code's createStderrLogger INFO/ERROR legs (services/api/client.ts:L73-86),
    // adapted to AMI's fetch-wrapper layer so a timeout/5xx/dropped connection is
    // visible in the same trace as its request. A non-ok status is logged at WARN
    // so it surfaces even when the level is raised above `debug`. All logging is
    // best-effort and NEVER perturbs the live request or masks the real error.
    try {
      const res = await base(input, init);
      try {
        log('api', `[API RESPONSE] ${pathname} status=${res.status} source=${source}`, undefined, res.ok ? 'debug' : 'warn');
      } catch {
        // Tracing must not perturb the returned response.
      }
      return res;
    } catch (err) {
      try {
        logError('api', `[API ERROR] ${pathname} source=${source}: ${err instanceof Error ? err.message : String(err)}`);
      } catch {
        // Never let tracing mask the original transport failure.
      }
      throw err;
    }
  }) as typeof globalThis.fetch;
}

/** Default JSONL appender: ensure the directory exists, then append one line. */
/**
 * Per-file serializer for the prompt-dump appends. Each line is mkdir + appendFile (two
 * awaits); issued concurrently, a later line could COMPLETE first, so a `response` line
 * could land in the file before the `init`/`message` lines of the same request. Every
 * append to one file now runs strictly after the previous one settles. Returns the
 * job's own completion; a failed job never blocks later ones.
 */
const dumpWriteChains = new Map<string, Promise<void>>();

export function serializedDumpWrite(filePath: string, job: () => Promise<void>): Promise<void> {
  const prev = dumpWriteChains.get(filePath) ?? Promise.resolve();
  const next = prev.then(job, job);
  const settled = next.catch(() => { /* surfaced to this job's caller only */ });
  dumpWriteChains.set(filePath, settled);
  void settled.then(() => { if (dumpWriteChains.get(filePath) === settled) dumpWriteChains.delete(filePath); });
  return next;
}

function appendDumpLine(filePath: string, line: string): Promise<void> {
  return serializedDumpWrite(filePath, async () => {
    await fsMkdir(pathDirname(filePath), { recursive: true });
    await fsAppendFile(filePath, line + '\n');
  });
}

/**
 * Compose a prompt/response debug-dump wrapper over `fetchImpl`. Returns
 * `fetchImpl` UNCHANGED (same reference) when dumping is disabled, so callers can
 * wrap unconditionally without altering identity-based branching or incurring any
 * overhead. See the section comment above for the two safety invariants.
 */
export function withPromptDump(
  fetchImpl: typeof globalThis.fetch,
  deps: PromptDumpDeps = {},
): typeof globalThis.fetch {
  const enabled = deps.enabled ?? dumpPromptsEnabled();
  if (!enabled) return fetchImpl;

  const dir = deps.dir ?? getPromptDumpDir();
  const sessionId = deps.sessionId ?? String(process.pid);
  const filePath = pathJoin(dir, `prompts-${sessionId}.jsonl`);
  const now = deps.now ?? (() => new Date().toISOString());
  const defer = deps.defer ?? ((fn: () => void) => { setImmediate(fn); });
  const write = deps.write ?? appendDumpLine;

  const safeWrite = (line: string): void => {
    try { void Promise.resolve(write(filePath, line)).catch(() => {}); } catch { /* best effort */ }
  };

  return (async (input: Parameters<typeof globalThis.fetch>[0], init?: RequestInit): Promise<Response> => {
    let timestamp: string | undefined;
    if (init?.method === 'POST' && typeof init.body === 'string') {
      timestamp = now();
      const body = init.body;
      const ts = timestamp;
      // Defer the parse+serialize (body carries the system prompt + every tool
      // schema — can be MBs) so it never blocks the live request. The per-session
      // DumpState deduplicates: the (multi-MB) system/tools blob is emitted ONCE
      // as `init`, then only as `system_update` when it actually drifts, and each
      // turn adds just the new user `message` — not the whole body again.
      defer(() => {
        try {
          const parsed: unknown = JSON.parse(body);
          if (parsed !== null && typeof parsed === 'object') {
            const state = getOrCreateDumpState(sessionId);
            for (const line of buildRequestDumpEntries(state, parsed as Record<string, unknown>, ts)) {
              safeWrite(line);
            }
          }
        } catch { /* ignore non-JSON / malformed body */ }
      });
    }

    const response = await fetchImpl(input, init);

    if (timestamp && response.ok) {
      // clone() so reading the capture stream never consumes the body the caller
      // reads. Fire-and-forget: the live call has already returned to the caller.
      const cloned = response.clone();
      const ts = timestamp;
      void (async () => {
        try {
          const ct = cloned.headers.get('content-type') ?? '';
          let data: unknown;
          if (ct.includes('text/event-stream') && cloned.body) {
            data = { stream: true, chunks: await readDumpSseChunks(cloned.body) };
          } else {
            data = await cloned.json();
          }
          safeWrite(JSON.stringify({ type: 'response', timestamp: ts, data }));
        } catch { /* best effort — never affect the live request */ }
      })();
    }

    return response;
  }) as typeof globalThis.fetch;
}

/**
 * The ONLY `anthropic-beta` tokens the Vertex `countTokens` endpoint accepts —
 * any other beta 400s there. Faithful port of the reference's
 * `VERTEX_COUNT_TOKENS_ALLOWED_BETAS` (constants/betas.ts). Used by
 * {@link filterCountTokensBetas} to PROACTIVELY strip incompatible betas before a
 * Vertex count request, rather than relying on a reactive 400-and-retry.
 */
export const VERTEX_COUNT_TOKENS_ALLOWED_BETAS: ReadonlySet<string> = new Set([
  CLAUDE_CODE_BETA,
  INTERLEAVED_THINKING_BETA,
  CONTEXT_MANAGEMENT_BETA,
]);

/**
 * Filter `betas` to those accepted on the token-counting endpoint for `provider`.
 * On a Vertex path (`anthropic-vertex` / `google-vertex`) only
 * {@link VERTEX_COUNT_TOKENS_ALLOWED_BETAS} survive (every other beta 400s on
 * Vertex `countTokens`); every other provider passes the full set through. This is
 * the proactive endpoint-scoped allowlist the reference applies before dispatch.
 */
export function filterCountTokensBetas(
  betas: string[],
  provider: ProviderName | string | undefined,
): string[] {
  if (provider === 'anthropic-vertex' || provider === 'google-vertex') {
    return betas.filter((b) => VERTEX_COUNT_TOKENS_ALLOWED_BETAS.has(b));
  }
  return betas;
}

/**
 * The single canonical source of truth for the outgoing `anthropic-beta` token
 * set, used by BOTH the native Anthropic and the anthropic-on-vertex provider
 * paths (never computed inline twice). Returns a deduped, order-stable list:
 *
 * - **Always-on** (never stripped): `extended-cache-ttl` (1h TTL, native path
 *   only — vertex injects it separately) and `context-1m` when the model serves
 *   a 1M window.
 * - **Experimental** (stripped by {@link experimentalBetasDisabled}):
 *   interleaved-thinking (model-gated, opt-out via env) and token-efficient-tools
 *   (explicit opt-in).
 * - **User escape hatch**: `ANTHROPIC_BETAS` tokens, appended unconditionally
 *   (bypasses the kill switch, mirroring the reference).
 *
 * When `opts.disableBetas` is set — the gateway-rejection recovery path — the
 * ENTIRE set is dropped (returns `[]`), since a strict proxy that 400s on one
 * unknown `anthropic-beta` token typically rejects every one, not just the
 * extended-cache token. This is the full-coverage complement to the per-token
 * kill switch.
 */
export function computeAnthropicBetas(
  modelId: string,
  opts: { cacheTtl?: '5m' | '1h'; includeCacheTtlBeta?: boolean; disableBetas?: boolean } = {},
): string[] {
  if (opts.disableBetas) return [];
  const betas: string[] = [];
  if (opts.includeCacheTtlBeta && opts.cacheTtl === '1h') betas.push(EXTENDED_CACHE_TTL_BETA);
  // The request-INDEPENDENT portion (model- + settings-gated) is assembled once per
  // (model, settings) and reused via the memoized {@link getModelBaseBetas}; only the
  // per-request 5m/1h cache-TTL beta above and the `disableBetas` kill switch vary
  // per call, so they stay here outside the cache.
  betas.push(...getModelBaseBetas(modelId));
  return [...new Set(betas)];
}

/**
 * Per-(model, settings) cache of the request-INDEPENDENT beta set — the part of
 * {@link computeAnthropicBetas} that does NOT vary per request (everything except
 * the request-specific 5m/1h cache-TTL beta and the `disableBetas` kill switch).
 * Assembling this set touches model-capability checks AND six env predicates
 * (experimental kill switch, interleaved opt-out, token-efficient / structured-output
 * opt-ins, prompt-cache scope, `ANTHROPIC_BETAS`), so it is computed once per
 * (model, settings) and reused on every subsequent request — mirroring the reference's
 * memoized `getAllModelBetas`/`getModelBetas` (utils/betas.ts).
 */
const modelBaseBetasCache = new Map<string, string[]>();

/**
 * The model- + settings-gated beta tokens, memoized by (model, settings). See
 * {@link modelBaseBetasCache}. The returned array is cache-owned and MUST NOT be
 * mutated by callers; {@link computeAnthropicBetas} only ever spreads it into a
 * fresh array, so the cached value is never aliased out.
 */
function getModelBaseBetas(modelId: string): string[] {
  const key = `${modelId}\u0000${betaSettingsFingerprint()}`;
  const cached = modelBaseBetasCache.get(key);
  if (cached) return cached;
  const betas: string[] = [];
  // Cross-session cache scope ('global'/'org') is an explicit operator opt-in via
  // AMI_PROMPT_CACHE_SCOPE; the scoped cache_control the knob produces is rejected
  // without this beta, so it must accompany any scoped request (non-experimental —
  // a deliberate config choice, not a kill-switchable experiment).
  if (resolvePromptCacheScope()) betas.push(PROMPT_CACHING_SCOPE_BETA);
  if (supportsOneMillionContextBeta(modelId)) betas.push(ONE_MILLION_CONTEXT_BETA);
  if (!experimentalBetasDisabled()) {
    if (supportsInterleavedThinking(modelId) && !interleavedThinkingDisabled()) {
      betas.push(INTERLEAVED_THINKING_BETA);
    }
    if (tokenEfficientToolsEnabled()) betas.push(TOKEN_EFFICIENT_TOOLS_BETA);
    // API-enforced structured outputs: model-gated + explicit opt-in. Experimental,
    // so it rides inside the kill-switch guard (stripped when betas are disabled).
    if (modelSupportsStructuredOutputs(modelId) && structuredOutputsEnabled()) {
      betas.push(STRUCTURED_OUTPUTS_BETA);
    }
  }
  betas.push(...getExtraAnthropicBetas());
  const result = [...new Set(betas)];
  modelBaseBetasCache.set(key, result);
  return result;
}

/**
 * Invalidate the memoized per-model beta set ({@link modelBaseBetasCache}). Called
 * from the settings-env re-apply path ({@link reapplySettingsEnv}) alongside the
 * proxy/mTLS and model-capability cache clears, because a re-applied settings `env`
 * block can change the very knobs {@link betaSettingsFingerprint} folds in
 * (`ANTHROPIC_BETAS`, the experimental-betas kill switch, interleaved opt-out, the
 * prompt-cache scope). Mirrors the reference's `clearBetasCaches()` invoked on
 * settings/auth change (utils/betas.ts, auth.ts).
 */
export function clearBetasCaches(): void {
  const cleared = modelBaseBetasCache.size;
  modelBaseBetasCache.clear();
  if (cleared > 0) log('provider', `cleared ${cleared} cached model beta set(s) after settings reconfiguration`);
}

/**
 * Diagnostic: number of (model, settings) beta sets currently memoized. Lets the
 * settings-reconfiguration path (or a test) observe that stale entries were
 * dropped. Mirrors the companion {@link proxyAgentCacheSize}.
 */
export function cachedModelBetaSetCount(): number {
  return modelBaseBetasCache.size;
}

/**
 * Betas that Bedrock accepts ONLY through `extraBodyParams` (the request body),
 * NEVER as `anthropic-beta` headers. Mirrors the reference's
 * `BEDROCK_EXTRA_PARAMS_HEADERS` (constants/betas.ts:L38-42): Bedrock's gateway
 * rejects these as headers, so routing a Claude model via Bedrock must carry them
 * in the body instead. Only the subset AMI computes for a model is sent.
 */
export const BEDROCK_EXTRA_PARAMS_BETAS: ReadonlySet<string> = new Set([
  INTERLEAVED_THINKING_BETA,
  ONE_MILLION_CONTEXT_BETA,
]);

/**
 * Compute the `extraBody` fragment carrying Bedrock's allowed betas for a reference
 * model, or `undefined` when none apply. The betas come from the SAME canonical
 * {@link computeAnthropicBetas} source as the header paths, then are filtered to
 * the Bedrock-extra-params subset and emitted under `anthropic_beta` (Bedrock's
 * body field) rather than a header. Non-the reference Bedrock models get `undefined`.
 */
export function computeBedrockBetaExtraBody(modelId: string): Record<string, unknown> | undefined {
  if (!modelId.toLowerCase().includes('claude')) return undefined;
  const betas = computeAnthropicBetas(modelId, { includeCacheTtlBeta: false })
    .filter(b => BEDROCK_EXTRA_PARAMS_BETAS.has(b));
  if (betas.length === 0) return undefined;
  return { anthropic_beta: betas };
}

/**
 * Count the exact input tokens for a request via Anthropic's `count_tokens`
 * endpoint, or `null` when the count is unavailable (non-OK response, missing
 * `input_tokens`, or a transport error) so the caller can fall back to the
 * heuristic. The outgoing `anthropic-beta` set comes from the single canonical
 * {@link computeAnthropicBetas} source, then is narrowed by
 * {@link filterCountTokensBetas} so a Vertex count never carries a beta that would
 * 400. Mirrors the reference's `countMessagesTokensWithAPI` (thinking budget,
 * dummy-message-when-empty, `input_tokens` extraction, and a bounded single
 * retry on a transient failure — see {@link CountTokensRequest.maxRetries}).
 */
export async function countTokensWithAPI(
  config: Pick<ProviderConfig, 'apiKey' | 'baseUrl' | 'model' | 'provider' | 'disableBetas'>,
  req: CountTokensRequest,
): Promise<number | null> {
  const model = normalizeModelStringForAPI(stripOpenRouterPrefix(config.model || ''));
  if (!model) return null;
  const betas = filterCountTokensBetas(
    computeAnthropicBetas(model, { includeCacheTtlBeta: false, disableBetas: config.disableBetas }),
    config.provider,
  );
  const base = (config.baseUrl || 'https://api.anthropic.com').replace(/\/+$/, '');
  const url = `${base}/v1/messages/count_tokens`;
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    'x-api-key': config.apiKey,
    'anthropic-version': '2023-06-01',
  };
  if (betas.length > 0) headers['anthropic-beta'] = betas.join(',');
  // An empty message list with tools still needs a dummy user message for an
  // accurate tool token count (the reference parity).
  const messages = req.messages.length > 0 ? req.messages : [{ role: 'user', content: 'foo' }];
  const body: Record<string, unknown> = { model, messages };
  if (req.tools && req.tools.length > 0) body.tools = req.tools;
  if (req.thinking) body.thinking = { type: 'enabled', budget_tokens: TOKEN_COUNT_THINKING_BUDGET };
  const fetchImpl = req.fetchImpl ?? globalThis.fetch;
  // Graceful-degradation tier 1: a bounded retry on a *transient* failure before
  // the caller degrades to its heuristic estimate. `maxRetries` extra attempts
  // (default 1) cover a flaky 5xx / connection blip; a deterministic 4xx, a 200
  // with a bad body, an abort, or a non-network throw short-circuits to null.
  const maxRetries = Math.max(0, req.maxRetries ?? 1);
  for (let attempt = 0; ; attempt++) {
    if (req.signal?.aborted) return null;
    try {
      const res = await fetchImpl(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
        ...(req.signal ? { signal: req.signal } : {}),
      });
      if (!res.ok) {
        // Retry only transient statuses (5xx + 408/409/429); a 400/401/403/404 is
        // deterministic and re-sending it just loops, so give up immediately.
        if (attempt < maxRetries && RETRYABLE_STATUS_CODES.has(res.status)) continue;
        return null;
      }
      const data = (await res.json()) as { input_tokens?: unknown };
      return typeof data.input_tokens === 'number' ? data.input_tokens : null;
    } catch (err) {
      // A transport-level blip (ECONNRESET / ETIMEDOUT / socket hang up) is
      // transient and worth one bounded retry; a parse error or an abort is not.
      if (attempt < maxRetries && !req.signal?.aborted && isNetworkError(err)) continue;
      return null;
    }
  }
}

/**
 * Resolve the proxy URL to use for `url` from the standard proxy env vars, or
 * `undefined` when none applies. An https target prefers `HTTPS_PROXY` (falling
 * back to `HTTP_PROXY`); an http target uses `HTTP_PROXY`. Within each pair the
 * lowercase variant is preferred over the uppercase one (`https_proxy` >
 * `HTTPS_PROXY` > `http_proxy` > `HTTP_PROXY`; `no_proxy` > `NO_PROXY`), matching
 * curl/standard-tooling precedence (the reference's `getProxyUrl`/`getNoProxy`). A
 * `NO_PROXY` match (exact host, domain suffix, `*`, `*.domain` glob, a
 * `host:port` entry, or a CIDR / IP range) always wins and returns `undefined`.
 * Pure + exported so it is unit-testable without touching the network.
 */
export function resolveProxyForUrl(env: NodeJS.ProcessEnv, url: string): string | undefined {
  let parsed: URL;
  try { parsed = new URL(url); } catch { return undefined; }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return undefined;
  const proxy = parsed.protocol === 'https:'
    ? (env.https_proxy ?? env.HTTPS_PROXY ?? env.http_proxy ?? env.HTTP_PROXY)
    : (env.http_proxy ?? env.HTTP_PROXY);
  if (!proxy) return undefined;
  const port = parsed.port || (parsed.protocol === 'https:' ? '443' : '80');
  if (hostMatchesNoProxy(env.no_proxy ?? env.NO_PROXY, parsed.hostname, port)) return undefined;
  return proxy;
}

const proxyAgentCache = new Map<string, unknown>();
// eslint-disable-next-line @typescript-eslint/no-explicit-any
let proxyAgentCtorPromise: Promise<(new (opts: any) => unknown) | null> | undefined;

/** Load undici's ProxyAgent lazily; resolves to null when undici is absent. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function loadProxyAgentCtor(): Promise<(new (opts: any) => unknown) | null> {
  if (proxyAgentCtorPromise === undefined) {
    proxyAgentCtorPromise = import('undici')
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      .then(m => (typeof m.ProxyAgent === 'function' ? (m.ProxyAgent as unknown as new (opts: any) => unknown) : null))
      .catch(() => null);
  }
  return proxyAgentCtorPromise;
}

let undiciAgentCtorPromise: Promise<(new (opts: unknown) => unknown) | null> | undefined;

/** Load undici's base Agent lazily (for direct mTLS connections); null when absent. */
function loadUndiciAgentCtor(): Promise<(new (opts: unknown) => unknown) | null> {
  if (undiciAgentCtorPromise === undefined) {
    undiciAgentCtorPromise = import('undici')
      .then(m => (typeof m.Agent === 'function' ? (m.Agent as unknown as new (opts: unknown) => unknown) : null))
      .catch(() => null);
  }
  return undiciAgentCtorPromise;
}

async function getProxyAgent(
  proxyUrl: string,
  tls?: { cert?: string; key?: string; passphrase?: string; ca?: string },
  env: NodeJS.ProcessEnv = process.env,
): Promise<unknown | undefined> {
  const connect = buildProxyConnectOptions(env, tls);
  // Cache key folds in the connect material (client cert, CA bundle size,
  // proxy-side DNS) so an agent built under one trust/DNS config is never reused
  // after a reconfiguration changes it.
  const cacheKey = connect ? `${proxyUrl}\u0000${connectOptionsCacheTag(connect)}` : proxyUrl;
  if (proxyAgentCache.has(cacheKey)) return proxyAgentCache.get(cacheKey) ?? undefined;
  const Ctor = proxyTransportHooks.ProxyAgent ?? await loadProxyAgentCtor();
  // Present the client certificate to the upstream (requestTls) and, for HTTPS
  // proxies, to the proxy itself (connect).
  const agent = Ctor
    ? new Ctor(connect ? { uri: proxyUrl, requestTls: connect, connect } : { uri: proxyUrl })
    : null;
  proxyAgentCache.set(cacheKey, agent);
  if (agent) {
    const caDesc = describeCATrustStore(env, tls?.ca);
    log('provider', `proxy dispatcher created${caDesc ? ` with custom trust store (${caDesc})` : ''}${connect?.lookup ? ', proxy-side DNS resolution ON' : ''}`);
    recordNetworkTrustDiagnostics('proxy', caDesc, { proxySideDns: !!connect?.lookup, clientCert: !!tls?.cert });
  }
  return agent ?? undefined;
}

const mtlsAgentCache = new Map<string, unknown>();

/** An undici Agent that presents the client certificate on direct TLS connections. */
async function getMTLSAgent(
  tls: { cert?: string; key?: string; passphrase?: string; ca?: string },
  env: NodeJS.ProcessEnv = process.env,
): Promise<unknown | undefined> {
  const connect = buildProxyConnectOptions(env, tls) ?? tls;
  const cacheKey = `${tls.cert ?? ''}\u0000${tls.key ?? ''}\u0000${tls.passphrase ?? ''}\u0000${connectOptionsCacheTag(buildProxyConnectOptions(env, tls))}`;
  if (mtlsAgentCache.has(cacheKey)) return mtlsAgentCache.get(cacheKey) ?? undefined;
  const Ctor = proxyTransportHooks.Agent ?? await loadUndiciAgentCtor();
  const agent = Ctor ? new Ctor({ connect }) : null;
  mtlsAgentCache.set(cacheKey, agent);
  if (agent) {
    const caDesc = describeCATrustStore(env, tls.ca);
    log('provider', `mTLS dispatcher created${tls.cert ? ' with client certificate' : ''}${caDesc ? ` and custom trust store (${caDesc})` : ''}`);
    recordNetworkTrustDiagnostics('mtls', caDesc, { clientCert: !!tls.cert });
  }
  return agent ?? undefined;
}

/**
 * Invalidate every cached HTTP dispatcher (proxy + mTLS agents). The agents embed
 * proxy routing, client-cert, and CA-trust material captured at construction, so
 * after a mid-session env mutation (rotated proxy, re-pointed CA, flipped
 * proxy-side-DNS) the stale agents must be dropped and rebuilt against the new
 * env — mirroring the reference's configureGlobalAgents() + cache clears on
 * managed-env reconfiguration. Invoked from the settings-env re-apply path.
 */
export function clearProxyAgentCache(): void {
  const cleared = proxyAgentCache.size + mtlsAgentCache.size;
  proxyAgentCache.clear();
  mtlsAgentCache.clear();
  if (cleared > 0) log('provider', `cleared ${cleared} cached HTTP dispatcher(s) after env reconfiguration`);
}

/**
 * Diagnostic: number of cached HTTP dispatchers (proxy + mTLS agents) currently
 * held. Lets a reconfiguration path (or a test) observe that stale agents were
 * dropped. Part of the proxy-agent lifecycle introspection surface.
 */
export function proxyAgentCacheSize(): number {
  return proxyAgentCache.size + mtlsAgentCache.size;
}

/**
 * Network/TLS env knobs surfaced via `ami --introspect` (repo invariant: every
 * capability is discoverable). Co-located with the transport code that reads them
 * so the manifest and runtime behavior cannot drift. Spread into the CLI feature
 * manifest's `experimentalEnvFlags` (cli/src/index.ts).
 */
export const NETWORK_ENV_FLAGS: ReadonlyArray<{ flag: string; affects: string; description: string }> = [
  {
    flag: 'AMI_PROXY_RESOLVES_HOSTS',
    affects: 'proxy-dns',
    description: 'Truthy to let the proxy resolve DNS: the dispatcher hands the raw hostname to the proxy instead of resolving it locally, for sandboxes where DNS is only reachable through the proxy (also accepts CLAUDE_CODE_PROXY_RESOLVES_HOSTS)',
  },
  {
    flag: MTLS_ENV.ca,
    affects: 'tls-trust',
    description: 'Custom CA bundle (file path or inline PEM) to TRUST for the upstream/proxy; merged WITH the bundled Mozilla roots (never replacing them) on the proxy/mTLS dispatcher so a private-CA endpoint is trusted without losing the default store',
  },
  {
    flag: NODE_EXTRA_CA_CERTS_ENV,
    affects: 'tls-trust',
    description: 'Node ambient extra-CA file; its contents are additionally merged onto the custom proxy/mTLS dispatcher trust store (Node honors it natively for non-dispatcher connections)',
  },
  {
    flag: SYSTEM_CA_NODE_OPTIONS.join(' / '),
    affects: 'tls-trust',
    description: 'Node runtime options (execArgv or NODE_OPTIONS) that opt the proxy/mTLS dispatcher into the OS/system trust store (tls.getCACertificates(\'system\')) in addition to the bundled roots',
  },
];

/**
 * Wrap a fetch so requests are routed through an undici dispatcher when either a
 * proxy env var is set (ProxyAgent) or a client certificate is configured
 * (mutual TLS). Returns `baseFetch` UNCHANGED (same reference, which may be
 * `undefined`) when NEITHER applies — so default behavior is byte-for-byte
 * identical. When undici is unavailable the wrapper is a no-op. A proxied request
 * carries the client cert to the upstream via the ProxyAgent's requestTls; a
 * direct request uses an Agent configured with the client-cert connect options.
 */
export function withProxyDispatcher(
  baseFetch: typeof globalThis.fetch | undefined,
  env: NodeJS.ProcessEnv = process.env,
): typeof globalThis.fetch | undefined {
  const hasProxyEnv = !!(env.HTTPS_PROXY ?? env.https_proxy ?? env.HTTP_PROXY ?? env.http_proxy);
  const mtls = getTLSConnectOptions(getMTLSConfig(env));
  if (!hasProxyEnv && !mtls) return baseFetch;
  const base = baseFetch ?? globalThis.fetch;
  return (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    const callerDispatcher = init && typeof init === 'object' ? (init as { dispatcher?: unknown }).dispatcher : undefined;
    if (!callerDispatcher) {
      const url = requestUrl(input as string | URL | { url?: unknown });
      const proxyUrl = url ? resolveProxyForUrl(env, url) : undefined;
      if (proxyUrl) {
        const agent = await getProxyAgent(proxyUrl, mtls, env);
        if (agent) return base(input, { ...init, dispatcher: agent } as RequestInit);
      } else if (mtls) {
        const agent = await getMTLSAgent(mtls, env);
        if (agent) return base(input, { ...init, dispatcher: agent } as RequestInit);
      }
    }
    return base(input, init);
  }) as typeof globalThis.fetch;
}

/**
 * Wrap a fetch so requests open a FRESH undici connection with HTTP keep-alive
 * effectively disabled (`pipelining:0`, minimal keep-alive timeouts) via a new
 * Agent dispatcher. Used by the stale-socket recovery path: after an
 * ECONNRESET/EPIPE the pooled socket is poisoned, so the next attempt must not
 * reuse it. A fresh Agent is created once per wrapper (i.e. once per retry's
 * resolveModel call), abandoning the old pool. No-op passthrough (never sets a
 * dispatcher) when undici is unavailable or the caller already set one.
 */
export function withFreshConnection(
  baseFetch: typeof globalThis.fetch | undefined,
): typeof globalThis.fetch {
  const base = baseFetch ?? globalThis.fetch;
  let agentPromise: Promise<unknown> | undefined;
  return (async (input: any, init?: any) => { // eslint-disable-line @typescript-eslint/no-explicit-any
    const callerDispatcher = init && typeof init === 'object' ? (init as { dispatcher?: unknown }).dispatcher : undefined;
    if (!callerDispatcher) {
      if (!agentPromise) {
        agentPromise = loadUndiciAgentCtor().then(Ctor =>
          Ctor ? new Ctor({ pipelining: 0, keepAliveTimeout: 1, keepAliveMaxTimeout: 1 }) : null,
        );
      }
      const agent = await agentPromise;
      if (agent) return base(input, { ...init, dispatcher: agent } as RequestInit);
    }
    return base(input, init);
  }) as typeof globalThis.fetch;
}

export function resolveModel(config: ProviderConfig, tools?: ToolDefinition[]) {
  // Env-var extra-body override (AMI_EXTRA_BODY / CLAUDE_CODE_EXTRA_BODY): deep-merge
  // the deploy-time JSON into config.extraBody BEFORE withExtraBody is composed, so it
  // rides every OpenAI-compatible request. Explicit config.extraBody (CLI flag / config
  // file) wins over the env default. No-op when the env is unset.
  const envExtraBody = resolveExtraBodyFromEnv();
  if (envExtraBody) {
    config = { ...config, extraBody: deepMergeRecords(envExtraBody, config.extraBody ?? {}) };
  }
  const { apiKey } = config;
  const fromKey = inferProviderFromApiKey(apiKey);
  const fromUrl = config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl) : null;
  // An explicit endpoint is the request. Ambient credentials (Vertex project,
  // Groq key, …) must not replace it, or a gemini / OpenAI-compatible model
  // is sent to the wrong SDK.
  const inferred = fromKey || fromUrl || (config.baseUrl ? null : inferProviderFromEnv());

  const rawModel = config.model || inferred?.defaultModel || 'gpt-5.6-luna';
  // An `openrouter/...` model id self-selects the OpenRouter provider and base
  // URL (even with no sk-or- key or openrouter.ai URL present), then the prefix
  // is stripped so the bare slug is what reaches OpenRouter's API.
  // An `openrouter/...` id is an explicit request, so it outranks ambient
  // inference (env keys, stray URLs) — but not an explicit config.provider /
  // config.baseUrl, which the caller set on purpose. Precedence:
  // explicit config > openrouter/ model prefix > inferred.
  const isOpenRouterModel = rawModel.startsWith('openrouter/');
  const provider = (config.provider as ProviderName) || (isOpenRouterModel ? 'openrouter' : inferred?.provider);
  // Strip the synthetic [1m]/[2m] context marker before the id reaches any SDK:
  // the marker is a LOCAL knob (context sizing via getContextWindow(config.model),
  // beta selection via supportsOneMillionContextBeta which is family-based, not
  // marker-based) and the endpoint rejects a model id carrying it. Mirrors the reference
  // Code's normalizeModelStringForAPI applied right before the request.
  // Cross-provider model-ID translation: a model selected/overridden under one
  // provider (e.g. the native `claude-opus-4-6`) is translated to the ACTIVE
  // provider's required wire string (e.g. bedrock `anthropic.claude-opus-4-6-…`)
  // via the canonical equivalence table, instead of being sent verbatim and
  // 400'ing as an invalid model. A model that is not a known canonical id, or a
  // provider the model has no mapping for, passes through unchanged.
  const translatedModel = resolveModelForProvider(rawModel, provider);
  const model = normalizeModelStringForAPI(stripOpenRouterPrefix(translatedModel));
  const baseUrl = config.baseUrl || (isOpenRouterModel ? PROVIDER_BASE_URLS.openrouter! : inferred?.defaultBaseUrl || '');
  // Proxy routing (HTTPS_PROXY/HTTP_PROXY/NO_PROXY). A no-op when no proxy env
  // is set, so proxyFetch === globalThis.fetch and behavior is unchanged.
  let proxyFetch = withProxyDispatcher(globalThis.fetch)!;
  let proxyActive = proxyFetch !== globalThis.fetch;
  // Stale keep-alive socket recovery: when a prior attempt failed with
  // ECONNRESET/EPIPE, open a FRESH connection with keep-alive disabled so the
  // retry does not reuse the poisoned pooled socket (mirrors the reference's
  // disableKeepAlive()). Only when no proxy/mTLS agent is already managing the
  // dispatcher; `proxyActive` then marks the fresh transport as a custom base so
  // the composedFetch fallback preserves it.
  if (config.forceFreshConnection && !proxyActive) {
    proxyFetch = withFreshConnection(globalThis.fetch);
    proxyActive = true;
  }
  // Transport-boundary prompt/response capture (AMI_DUMP_PROMPTS). Composed over
  // the innermost transport so EVERY provider branch (which all derive their fetch
  // from proxyFetch/headerFetch) captures the fully-assembled request + raw
  // response. Identity passthrough when disabled, so proxyActive and the composed
  // fallback below stay unaffected in the common case.
  const dumpActive = dumpPromptsEnabled();
  proxyFetch = withPromptDump(proxyFetch, { sessionId: config.promptCacheKey });
  // L6: attach a client-generated request-id header for log correlation /
  // traceability, unless the caller already set one. Additive — merged into the
  // same extra-header fetch wrapper so it rides every outgoing API request. Gated
  // to direct first-party-style endpoints: managed cloud gateways (Bedrock/Vertex/
  // Foundry) drop it and their strict proxies may reject unknown headers, so the
  // header is omitted there (mirrors the reference's firstParty-only injection).
  const injectRequestId = endpointAcceptsClientRequestId(provider);
  const headersWithRequestId = { ...(config.extraHeaders ?? {}), ...buildRequestIdHeader(config.extraHeaders, injectRequestId) };
  const headerFetch = withExtraHeaders(headersWithRequestId, proxyFetch);
  const bodyFetch = withExtraBody(openAIPromptCacheBody(config.extraBody, config.promptCacheKey, provider, baseUrl), headerFetch ?? proxyFetch);
  const composedFetch = bodyFetch ?? headerFetch ?? ((proxyActive || dumpActive) ? proxyFetch : undefined);
  // L22: on the anthropic-vertex path the boundary is consumed by the cache
  // splitter; everywhere else strip it so the model never sees the sentinel.
  const isVertexPath = provider === 'anthropic-vertex' || (model.startsWith('claude') && isVertexAIConfigured());
  const strippedFetch = isVertexPath ? composedFetch : withDynamicBoundaryStrip(composedFetch);
  // Per-request debug trace of pathname + request-id so a timeout (no server
  // request-id) can still be correlated with server logs. Gated by the shared
  // logger's level/component filter — a no-op on disk when debug logging is off.
  const fetch = withRequestIdDebugTrace(strippedFetch, provider ?? 'inferred');

  // Azure AI Foundry — the reference (and other) models over Azure's OpenAI-compatible
  // Foundry endpoint. The explicit provider check is placed BEFORE the
  // claude/anthropic auto-route so a Foundry-hosted `claude-*` model is routed to
  // the Foundry resource rather than api.anthropic.com. The Foundry base URL is
  // per-resource, so it must come from config.baseUrl (or AZURE_AI_FOUNDRY_BASE_URL).
  if (provider === 'azure-foundry') {
    const foundryBase = baseUrl || process.env.AZURE_AI_FOUNDRY_BASE_URL || '';
    return createOpenAI({ apiKey, baseURL: foundryBase || undefined, fetch })(model);
  }

  // Anthropic via Google Vertex AI
  if (provider === 'anthropic-vertex' || (model.startsWith('claude') && isVertexAIConfigured())) {
    const project = process.env.ANTHROPIC_VERTEX_PROJECT_ID || process.env.GOOGLE_VERTEX_PROJECT || '';
    const location = getVertexRegionForModel(model);
    // Prompt caching on anthropic-vertex is OFF by default. withVertexAnthropicCache
    // injects cache_control into the raw body and vertex DOES write caches
    // (cache_creation_input_tokens > 0), but it NEVER reads them back
    // (cache_read_input_tokens stayed 0 across every approach: SDK providerOptions,
    // whole-system injection, and static-prefix injection). A write-without-read is
    // cost-NEGATIVE (1.25x write premium, no read discount), so it is gated behind
    // AMI_VERTEX_PROMPT_CACHE=1 for re-testing when the vertex/SDK read path works.
    // See UI issue #10. Native Anthropic (below) caches normally.
    // Beta headers must also ride on the vertex path, where they cannot be passed to
    // the SDK constructor — inject them at the fetch layer. Same canonical source as the
    // native path, minus extended-cache (withVertexAnthropicCache injects that separately).
    const vertexBetas = computeAnthropicBetas(model, { includeCacheTtlBeta: false, disableBetas: config.disableBetas });
    const hasVertexBetas = vertexBetas.length > 0;
    const vertexBase = hasVertexBetas
      ? withAnthropicBetaHeader(headerFetch ?? proxyFetch, vertexBetas)
      : (headerFetch ?? proxyFetch);
    const vertexCacheFetch = isEnvTruthy(process.env.AMI_VERTEX_PROMPT_CACHE)
      ? withVertexAnthropicCache(vertexBase, config.cacheTtl)
      : (hasVertexBetas ? vertexBase : (headerFetch ?? (proxyActive ? proxyFetch : undefined)));
    return createVertexAnthropic({
      project,
      location,
      ...(vertexCacheFetch ? { fetch: vertexCacheFetch } : {}),
    } as Parameters<typeof createVertexAnthropic>[0])(model);
  }

  // Native Anthropic SDK
  if (provider === 'anthropic' || model.startsWith('claude') || isFirstPartyBaseUrl(baseUrl, ['anthropic.com'])) {
    // Beta tokens for the native path, from the single canonical source: extended-cache
    // (1h TTL) + 1M-context + interleaved-thinking / token-efficient-tools (experimental,
    // kill-switchable) + any ANTHROPIC_BETAS. All ride one comma-separated header; deduped.
    const uniqueBetas = computeAnthropicBetas(model, { cacheTtl: config.cacheTtl, includeCacheTtlBeta: true, disableBetas: config.disableBetas });
    // Fine-grained tool streaming is a first-party-only wire field: apply it ONLY on
    // the direct api.anthropic.com path (no custom gateway), since proxies reject it
    // with a 400. A custom baseUrl means a gateway, so the opt-in is skipped there.
    const firstPartyAnthropic = !baseUrl || isFirstPartyBaseUrl(baseUrl, ['anthropic.com']);
    const nativeBase = withGzipCompression(headerFetch ?? proxyFetch);
    // Fine-grained tool streaming: the GLOBAL env knob opts in every tool, while a
    // per-tool `eagerInputStreaming` flag opts in just those tools even when the
    // global knob is off. First-party path only (proxies/Bedrock/Vertex 400 on it).
    const globalEager = fineGrainedToolStreamingEnabled();
    const perToolEager = tools?.filter((t) => t.eagerInputStreaming).map((t) => t.name) ?? [];
    const eagerFetch = (firstPartyAnthropic && (globalEager || perToolEager.length > 0))
      ? withEagerToolStreaming(nativeBase, globalEager ? undefined : new Set(perToolEager))
      : nativeBase;
    // Provider-native (server-side) context editing: when opted in, inject the
    // `context_management` edit into every request body (cache-preserving) and add
    // the accompanying beta header. No-op when the knob is off.
    const contextManagement = buildAnthropicContextManagement();
    const nativeFetch = contextManagement
      ? (withExtraBody({ context_management: contextManagement }, eagerFetch) ?? eagerFetch)
      : eagerFetch;
    const nativeBetas = contextManagement
      ? [...new Set([...uniqueBetas, CONTEXT_MANAGEMENT_BETA])]
      : uniqueBetas;
    return createAnthropic({
      apiKey,
      baseURL: baseUrl || undefined,
      fetch: nativeFetch,
      ...(nativeBetas.length > 0 ? { headers: { 'anthropic-beta': nativeBetas.join(',') } } : {}),
    })(model);
  }

  // Native Google Generative AI SDK
  if (provider === 'google' || provider === 'google-vertex' || model.startsWith('gemini') ||
      isFirstPartyBaseUrl(baseUrl, ['generativelanguage.googleapis.com']) ||
      baseUrl.includes('aiplatform.googleapis.com')) {
    return createGoogleGenerativeAI({ apiKey, baseURL: baseUrl || undefined })(model);
  }

  // Azure OpenAI uses the OpenAI SDK with azure-specific baseURL
  if (provider === 'azure-openai' || baseUrl.includes('openai.azure.com')) {
    return createOpenAI({ apiKey, baseURL: baseUrl, fetch })(model);
  }

  // Amazon Bedrock — route through OpenAI-compatible if user provides a gateway URL.
  // Resolve the invoked id against the account's inference profiles so a
  // region-scoped profile (e.g. eu.anthropic.<model>-v1:0) is used when provisioned.
  if (provider === 'amazon-bedrock') {
    // Boundary guard: when the setup uses short-term STS credentials
    // (AWS_SESSION_TOKEN exported) but the credential triple is malformed/empty,
    // fail fast here with an actionable message rather than letting a broken
    // credential blob surface as an opaque 403 deep in the request. Skipped for
    // bearer-token auth (AWS_BEARER_TOKEN_BEDROCK), which does not consume the
    // STS env triple. Mirrors the reference's isValidAwsStsOutput boundary check.
    if (!(process.env[AWS_BEARER_TOKEN_BEDROCK_ENV] ?? '').trim()) {
      assertValidStsEnvCredentials(process.env);
    }
    const bedrockModel = resolveBedrockModelId(model, getBedrockInferenceProfiles());
    // Bedrock rejects `anthropic-beta` HEADERS, so route the model's allowed betas
    // (interleaved-thinking / 1M-context) through the request BODY instead, mirroring
    // the reference's BEDROCK_EXTRA_PARAMS_HEADERS set. No-op for non-Claude models.
    const bedrockBetaBody = computeBedrockBetaExtraBody(bedrockModel);
    const bedrockFetch = withExtraBody(bedrockBetaBody, fetch) ?? fetch;
    // Prefer an AWS_BEARER_TOKEN_BEDROCK API key when present and derive the
    // region-scoped OpenAI-compatible endpoint when no explicit gateway is set.
    const gateway = resolveBedrockGateway(config);
    return createOpenAI({ apiKey: gateway.apiKey, baseURL: gateway.baseURL, fetch: bedrockFetch })(bedrockModel);
  }

  // Native Vercel AI SDK providers — each has its own adapter with correct
  // API format, avoiding OpenAI-specific fields like parallel_tool_calls.
  // Only pass baseURL when the user explicitly provided a custom one;
  // skip the default from PROVIDER_BASE_URLS (the native SDK knows its own URL).
  const defaultUrl = PROVIDER_BASE_URLS[provider as ProviderName];
  const userBaseUrl = (baseUrl && baseUrl !== defaultUrl) ? baseUrl : undefined;
  if (provider === 'xai') {
    return createXai({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'mistral') {
    return createMistral({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'cohere') {
    return createCohere({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'deepseek') {
    return createDeepSeek({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'groq') {
    return createGroq({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'togetherai') {
    return createTogetherAI({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'fireworks') {
    return createFireworks({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'alibaba') {
    return createAlibaba({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'deepinfra') {
    return createDeepInfra({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'cerebras') {
    return createCerebras({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }
  if (provider === 'huggingface') {
    return createHuggingFace({ apiKey, baseURL: userBaseUrl, fetch })(model);
  }

  // Remaining OpenAI-compatible providers (openai, openrouter, nvidia, perplexity, ollama, luma, ai-gateway)
  const effectiveBaseUrl = baseUrl || PROVIDER_BASE_URLS[provider as ProviderName] || 'https://api.openai.com/v1';
  const openai = createOpenAI({ baseURL: effectiveBaseUrl, apiKey, fetch });
  // gpt-6+ on OpenAI require /v1/responses for tool use with reasoning
  const isOpenAI = !baseUrl || effectiveBaseUrl.includes('api.openai.com');
  if (isOpenAI && requiresResponsesAPI(model)) {
    return openai.responses(model);
  }
  return openai.chat(model);
}

export { inferProviderFromApiKey };

export async function* streamChatCompletion(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig; repairToolCall?: ToolCallRepairFunction<ToolSet>; streamTextImpl?: typeof streamText; querySource?: QuerySource },
): AsyncGenerator<StreamChunk> {
  // Test seam: the AI SDK streamText entry point. Defaults to the real import so
  // production behavior is unchanged; tests inject a fake that drives the
  // stream-part switch (tool-call / finish-step / error / reasoning) and the
  // generator's catch (abort / MissingToolResults / API errors) deterministically.
  const streamTextFn = options?.streamTextImpl ?? streamText;
  const model = resolveModel(config, tools);
  const inferred = inferProviderFromApiKey(config.apiKey)
    || (config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl) : null)
    || inferProviderFromEnv();
  const modelId = config.model || inferred?.defaultModel || 'gpt-5.6-luna';
  const sdkTools = tools.length > 0 ? convertToolsForSDK(tools) : undefined;

  // Extract system message and pass via `system` parameter to avoid AI SDK warning
  // H3: hoist only the LEADING system prompt into the cached `system` prefix;
  // ephemeral mid-conversation role:'system' nudges stay in-stream as user-role
  // <system-reminder> blocks so they don't bust the prompt cache. (hoistSystemPrompt)
  const { system: systemPrompt, rest: nonSystemMessages } = hoistSystemPrompt(messages);
  const isAnthropic = modelId.startsWith('claude') ||
    isFirstPartyBaseUrl(config.baseUrl, ['anthropic.com']) ||
    config.provider === 'anthropic' ||
    config.provider === 'anthropic-vertex';
  // On vertex, cache_control is injected into the RAW request body by
  // withVertexAnthropicCache (the SDK path double-counts otherwise — Anthropic
  // caps cache_control at 4 blocks). So SDK-level markers apply to NATIVE
  // anthropic only; vertex relies solely on the HTTP injection.
  const isVertexAnthropic = config.provider === 'anthropic-vertex';
  // Honor the DISABLE_PROMPT_CACHING global/per-model kill-switches (getPromptCachingEnabled).
  const promptCachingEnabled = getPromptCachingEnabled(modelId);
  // Global cache-marker strategy (tool_based | system_prompt | none) + the
  // per-request skipCacheWrite one-off suppressor. Both can turn off cache writes;
  // the strategy additionally selects which prefix blocks are marked.
  const cacheStrategy = resolvePromptCacheStrategy();
  const useSdkCacheMarkers = isAnthropic && !isVertexAnthropic && promptCachingEnabled
    && !config.skipCacheWrite && cacheStrategy !== 'none';

  // Repair tool_use/tool_result pairing before dispatch. Strict mode (armed via
  // SUPERINFERENCE_STRICT_TOOL_PAIRING) fails the turn loudly on corruption instead of
  // injecting synthetic placeholders; otherwise the repair is applied and, when it
  // actually mutated the transcript, a diagnostic event is recorded so the root cause of
  // a corrupted transcript is traceable (parity: the reference's tengu_tool_result_pairing_repaired).
  // Defend against stale/removed-tool references before pairing repair: an MCP server
  // may have disconnected, a plugin tool removed, or the tier changed since the turn
  // that issued a tool_call, leaving a transcript that points at a tool name no longer
  // in THIS request's tool set. Anthropic rejects such references, so strip the
  // orphaned tool_call (and its paired tool_result) and emit a diagnostic. Only runs
  // when the request actually carries tools — a deliberately tool-less request keeps
  // its historical tool turns untouched. Parity: the reference's
  // stripUnavailableToolReferencesFromUserMessage.
  const prunedForTools =
    tools.length > 0
      ? stripUnavailableToolReferences(nonSystemMessages, new Set(tools.map(t => t.name)), {
          onStrip: (diag) => {
            recordDiagnostic('warn', 'unavailable_tool_references_stripped', {
              toolCallsDropped: diag.toolCallsDropped,
              toolResultsDropped: diag.toolResultsDropped,
              placeholdersInserted: diag.placeholdersInserted,
              droppedToolNames: diag.droppedToolNames,
            });
          },
        })
      : nonSystemMessages;
  let healed: typeof nonSystemMessages;
  try {
    healed = healOrphanedToolCalls(prunedForTools, {
      strict: getStrictToolResultPairing(),
      onRepair: (diag) => {
        recordDiagnostic('warn', 'tool_result_pairing_repaired', {
          messageCountBefore: diag.messageCountBefore,
          messageCountAfter: diag.messageCountAfter,
          syntheticResultsInserted: diag.syntheticResultsInserted,
          orphanedResultsDropped: diag.orphanedResultsDropped,
          duplicateIdsRemoved: diag.duplicateIdsRemoved,
          leadingOrphanPlaceholderInserted: diag.leadingOrphanPlaceholderInserted,
          messageStructure: diag.messageStructure,
        });
      },
    });
  } catch (err) {
    if (err instanceof ToolPairingError) {
      yield { type: 'error', error: err.message };
      return;
    }
    throw err;
  }
  const sanitized = sanitizeToolCallIds(healed, isAnthropic ? 'anthropic' : '');
  // Proactively cap outbound media so the request can never exceed the API's hard
  // per-request media limit (drops oldest images first).
  const capped = stripExcessMediaItems(sanitized);
  // Proactively validate each surviving image against the API's per-image base64
  // ceiling BEFORE sending, so an oversized image fails fast with a message naming
  // exactly which image is too big — instead of a confusing mid-request 400 that
  // identifies none. Surfaced as a terminal (non-retryable) error chunk, matching
  // AMI's error convention and the reference's validateImagesForAPI boundary check.
  try {
    validateImagesForAPI(capped);
  } catch (err) {
    yield { type: 'error', error: err instanceof Error ? err.message : String(err) };
    return;
  }
  let coreMessages = convertMessages(capped);

  if (isAnthropic) {
    coreMessages = stripEmptyTextBlocks(
      coreMessages as unknown as Array<Record<string, unknown>>,
    ) as unknown as typeof coreMessages;
  }

  // Anthropic prompt caching. Mark the stable prefix so the provider does not
  // re-prefill it every turn. Anthropic allows 4 cache_control markers; we spend
  // them as tools(1) + first-message(1, which caches system+tools before it) +
  // last-two-messages(2). The system prompt stays in the top-level instructions
  // string (this SDK/vertex path rejects a role:'system' message in `messages`),
  // and the breakpoint on the first message caches everything before it including
  // the system+tools blocks. cache_control uses the AI-SDK
  // providerOptions.anthropic.cacheControl shape.
  const cacheControl = buildAnthropicCacheControl(config.cacheTtl);
  let toolsForModel = sdkTools;
  if (useSdkCacheMarkers) {
    const breakpointIndices = new Set<number>();
    if (systemPrompt && coreMessages.length > 0) breakpointIndices.add(0);
    // Emit the TTL/scope-aware breakpoint (`cacheControl`) at each conversation
    // breakpoint index, then apply the per-index breakpoint value below. The
    // system-prompt breakpoint (index 0) is not produced by the helper, so it
    // falls back to the same `cacheControl`.
    const convPoints = buildConversationCachePoints(coreMessages, promptCachingEnabled, cacheControl);
    for (const idx of Object.keys(convPoints)) breakpointIndices.add(Number(idx));
    for (const idx of breakpointIndices) {
      const msg = coreMessages[idx] as Record<string, unknown> | undefined;
      if (!msg) continue;
      const content = msg.content;
      if (Array.isArray(content)) {
        const hasTextPart = content.some((p: Record<string, unknown>) =>
          p.type === 'text' && typeof p.text === 'string' && p.text.trim() !== '');
        if (!hasTextPart) continue;
      } else if (typeof content === 'string' && content.trim() === '') {
        continue;
      }
      const breakpoint = convPoints[idx] ?? cacheControl;
      msg.providerOptions = {
        ...((msg.providerOptions as Record<string, unknown>) || {}),
        anthropic: {
          ...((msg.providerOptions as Record<string, Record<string, unknown>>) || {}).anthropic,
          cacheControl: breakpoint,
        },
      };
    }
    // Cache the TOOLS block via a marker on the last tool (tools precede the
    // messages in Anthropic's cache hierarchy). Additive; does not change which
    // tools are sent. The `system_prompt` strategy deliberately omits this marker
    // (it caches only the system/message prefix), e.g. for setups whose tool set
    // churns every turn and would otherwise bust a tool-block cache every time.
    if (sdkTools && cacheStrategy === 'tool_based') toolsForModel = markLastToolCacheControl(sdkTools, cacheControl);
  }

  try {
    // Runtime fault-injection seam (parity: the reference's bridgeDebug.wrapApiForFaultInjection).
    // When AMI_FAULT_INJECTION has queued a fault for the 'provider' target, throw the
    // classified error HERE — before the real request — so the LIVE retry/backoff/teardown
    // machinery (streamChatCompletionWithRetry → classifyError) actually fires: a transient
    // fault (503) is retried, a fatal fault (403) is surfaced non-retryable. The throw is
    // caught by this generator's catch below and yielded as an error chunk, exactly like a
    // real wire error. No-op / near-zero overhead when unset.
    maybeThrowFault('provider');

    const effectiveTemperature = resolveTemperature(modelId, config.temperature, options?.thinking);

    // OpenAI SDK-native prompt-cache options. The transport-layer extraBody path
    // (openAIPromptCacheBody) already handles prompt_cache_key for Azure and
    // compat servers; SDK providerOptions adds promptCacheOptions (mode/ttl)
    // which the transport layer cannot express.
    const providerName = config.provider || inferred?.provider;
    const oaiHost = config.baseUrl ?? '';
    const isOfficialOpenAI = oaiHost.includes('api.openai.com') ||
      ((providerName === 'openai' || providerName === 'azure-openai') && oaiHost.length === 0);
    const isNativeOpenAI = !isAnthropic && isOfficialOpenAI;
    const openAICacheOpts = isNativeOpenAI && config.promptCacheKey ? {
      providerOptions: {
        openai: {
          promptCacheKey: config.promptCacheKey,
          ...(config.promptCacheOptions ? { promptCacheOptions: config.promptCacheOptions } : {}),
        },
      },
    } : {};

    // Forced/constrained tool selection (parity: the reference's `tool_choice`
    // request parameter). Only meaningful when tools are advertised; the AI SDK
    // rejects a toolChoice with no tools, so it is omitted otherwise. The shape
    // ('auto'|'required'|'none'|{type:'tool',toolName}) maps 1:1 to the SDK option.
    const toolChoiceOpt = (toolsForModel && config.toolChoice)
      ? { toolChoice: config.toolChoice }
      : {};

    // Generation stop strings (parity: the reference's `stop_sequences`). Forwarded
    // verbatim to the AI SDK so the model halts when any sentinel is emitted;
    // omitted when unset/empty so the request is byte-identical for the common case.
    const stopSequencesOpt = (config.stopSequences && config.stopSequences.length > 0)
      ? { stopSequences: config.stopSequences }
      : {};

    // Provider-native structured output (parity: the reference's `outputFormat:
    // { type: 'json_schema', schema }` on auxiliary queries). When a caller sets
    // config.responseFormat, forward the JSON Schema as the AI SDK `output`
    // specification so the SDK constrains the model to emit schema-valid JSON
    // (OpenAI `response_format` json_schema / Anthropic tool-schema) — removing
    // the parse-failure failure mode from JSON-returning auxiliary calls. Omitted
    // when unset so the request is byte-identical for the common free-form case.
    const responseFormatOpt = config.responseFormat
      ? {
          output: Output.object({
            schema: jsonSchema(config.responseFormat.schema),
            ...(config.responseFormat.name ? { name: config.responseFormat.name } : {}),
          }),
        }
      : {};

    // Request metadata user id (Anthropic metadata.user_id / OpenAI user) from
    // config or the AMI_METADATA_USER_ID / CLAUDE_CODE_METADATA_USER_ID env knob,
    // folded together with any AMI_EXTRA_METADATA / CLAUDE_CODE_EXTRA_METADATA JSON.
    // Combine the prompt-cache, metadata, and thinking fragments into ONE
    // providerOptions object so later spreads cannot clobber an earlier one (all
    // three can target providerOptions.{anthropic|openai}).
    const providerOptionsOpt = mergeProviderOptionFragments(
      openAICacheOpts,
      buildMetadataProviderOptions(resolveMetadataUserIdValue(config), isAnthropic),
      buildThinkingOptions(modelId, options?.thinking),
    );

    const result = streamTextFn({
      model,
      instructions: systemPrompt,
      messages: coreMessages,
      tools: toolsForModel,
      ...toolChoiceOpt,
      ...stopSequencesOpt,
      ...responseFormatOpt,
      maxOutputTokens: fitOutputTokens(
        // When the caller omits maxTokens, use the model's conservative DEFAULT
        // budget (the reference's getModelMaxOutputTokens().default) rather than a flat
        // global constant, so each model sends its own sensible default.
        config.maxTokens ?? getModelMaxOutputTokens(modelId).default,
        config.contextWindow ?? getContextWindow(modelId),
        promptTokensForFit(systemPrompt, sanitized, tools),
        modelId,
      ),
      ...(effectiveTemperature !== undefined ? { temperature: effectiveTemperature } : {}),
      ...(options?.repairToolCall ? { repairToolCall: options.repairToolCall } : {}),
      abortSignal,
      maxRetries: 0,
      onError: ({ error }) => {
        const msg = error instanceof Error ? error.message : String(error);
        if (msg.includes('MissingToolResults')) return;
      },
      ...providerOptionsOpt,
    });

    // Track tool call indices so engine.ts can accumulate them by index
    let toolCallIndex = 0;
    // Whether the stream produced ANY assistant output (content / tool-call /
    // reasoning). Drives empty-stream detection after the loop: a stream that
    // ends with neither output nor a finish-step/error is degenerate.
    let producedOutput = false;

    const stream = result.stream;
    for await (const part of stream) {
      switch (part.type) {
        case 'text-delta':
          producedOutput = true;
          yield { type: 'content_delta', text: part.text };
          break;

        case 'tool-call':
          // The AI SDK delivers a complete tool-call (not deltas).
          // We emit a single tool_call_delta with the full payload so
          // engine.ts's accumulator can build the final ToolCallContent.
          producedOutput = true;
          yield {
            type: 'tool_call_delta',
            toolCall: {
              index: toolCallIndex++,
              id: part.toolCallId,
              function: {
                name: part.toolName,
                arguments: JSON.stringify(part.input),
              },
            },
          };
          break;

        case 'finish-step': {
          const cacheSplit = extractCacheCreationSplit(part);
          yield {
            type: 'done',
            finishReason: part.finishReason,
            usage: {
              promptTokens: part.usage.inputTokens ?? 0,
              completionTokens: part.usage.outputTokens ?? 0,
              totalTokens: part.usage.totalTokens ?? (
                (part.usage.inputTokens ?? 0) + (part.usage.outputTokens ?? 0)
              ),
              reasoningTokens: extractReasoningTokens(part.usage),
              cachedPromptTokens: extractCachedPromptTokens(part),
              cacheCreationTokens: extractCacheCreationTokens(part),
              cacheCreation5mTokens: cacheSplit.ephemeral5m,
              cacheCreation1hTokens: cacheSplit.ephemeral1h,
            },
            responseHeaders: (part as { response?: { headers?: Record<string, string> } }).response?.headers,
          };
          return;
        }

        case 'error': {
          const errObj = part.error as { statusCode?: number; responseHeaders?: Record<string, string>; message?: string; type?: string } | undefined;
          const errStatusCode = errObj?.statusCode;
          const errMsg = errStatusCode
            ? `HTTP ${errStatusCode}: ${errObj?.message ?? String(part.error)}`
            : errObj?.message ?? (typeof part.error === 'string' ? part.error : String(part.error));

          // Persist the failure to the error log enriched with the request URL and
          // the server's own message body (AI SDK APICallError carries `url`/
          // `responseBody`). API failures surface here as a stream `error` PART (not
          // a thrown error), so without this the primary provider-error source was
          // never logged with its url/body context. Mirrors the reference's errorLogSink.
          logError('provider', errMsg, { httpError: part.error });

          // Extract Retry-After (seconds) — handles numeric and HTTP-date forms.
          const errRetryAfter = extractRetryAfterSeconds(errObj?.responseHeaders);

          yield {
            type: 'error',
            error: errMsg,
            errorObject: part.error instanceof Error ? part.error : undefined,
            ...(errRetryAfter !== undefined && { retryAfter: errRetryAfter }),
            responseHeaders: errObj?.responseHeaders,
          };
          return;
        }

        case 'reasoning-start':
          // A normal thinking block's reasoning-start carries no text (it arrives via
          // reasoning-delta) and is ignored here. A REDACTED/encrypted thinking block,
          // however, arrives as a reasoning-start carrying providerMetadata.anthropic.
          // redactedData and NEVER emits a reasoning-delta — so without this branch the
          // user would see a silent gap where the model actually reasoned. Surface a
          // placeholder once per redacted block (parity: the reference's redacted-thinking
          // message), reusing the already-wired thinking_delta render path.
          if (extractRedactedThinkingData(part) !== undefined) {
            producedOutput = true;
            yield { type: 'thinking_delta', text: `${REDACTED_THINKING_PLACEHOLDER}\n` };
          }
          break;

        case 'reasoning-delta':
          producedOutput = true;
          yield { type: 'thinking_delta', text: (part as { text?: string }).text ?? '' };
          break;

        default:
          // Ignore other part types (source, file, etc.)
          break;
      }
    }

    // The stream ended without a finish-step or terminal error. If it produced
    // NO assistant output at all — and it was NOT aborted (an abort/idle-timeout
    // ends silently too, but must stay the retry wrapper's timeout path via a
    // bare `done`) — it is a degenerate/empty stream (proxy 200 with a non-SSE
    // body, or a truncated SSE body with no completed content blocks and no
    // stop_reason): surface a retryable sentinel error so
    // streamChatCompletionWithRetry retries instead of yielding a silent, empty
    // `done` turn. If some output was produced but the finish-step was lost, keep
    // the prior behavior and emit `done` (content is already committed upstream).
    if (!producedOutput && !abortSignal.aborted) {
      yield { type: 'error', error: EMPTY_STREAM_ERROR };
      return;
    }
    yield { type: 'done' };
  } catch (err: unknown) {
    if (abortSignal.aborted) {
      yield { type: 'error', error: 'Request aborted' };
    } else if (err instanceof Error && err.message.includes('MissingToolResults')) {
      yield { type: 'done', finishReason: 'tool-calls' };
    } else {
      const baseMsg = err instanceof Error ? err.message : String(err);

      // The AI SDK throws APICallError with statusCode and responseHeaders.
      // Extract these for the retry wrapper.
      const statusCode = (err as { statusCode?: number }).statusCode;
      const responseHeaders = (err as { responseHeaders?: Record<string, string> }).responseHeaders;

      // Format error with HTTP status prefix so extractStatusCode() can find it
      const errorMsg = statusCode ? `HTTP ${statusCode}: ${baseMsg}` : baseMsg;

      // Persist the failure to the error log enriched with the request URL and the
      // server's own message body (AI SDK APICallError carries `url`/`responseBody`),
      // so a logged provider error is self-explanatory — which endpoint, what the
      // server actually said — instead of a bare `HTTP <status>`. `logError` extracts
      // the `[url=…,status=…,body=…]` prefix via `httpErrorContext`; it never writes
      // the raw error object. Mirrors the reference's errorLogSink enrichment.
      logError('provider', errorMsg, { httpError: err });

      // Extract Retry-After (seconds) — handles numeric and HTTP-date forms.
      const retryAfter = extractRetryAfterSeconds(responseHeaders);

      yield {
        type: 'error',
        error: errorMsg,
        errorObject: err instanceof Error ? err : undefined,
        ...(retryAfter !== undefined && { retryAfter }),
        responseHeaders,
      };
    }
  }
}

// ---------------------------------------------------------------------------
/** Test seam for retry-jitter randomness so backoff delays stay deterministic in tests. */
export const retryHooks: { random: () => number } = { random: Math.random };
/**
 * Equal jitter: 50–100% of the computed backoff. De-synchronizes concurrent
 * retries (e.g. in-process subagents sharing one API key that all 429 at once and
 * would otherwise retry in lockstep on a bare 2^n schedule) WITHOUT ever
 * undercutting the backoff floor. Applied only to blind exponential backoff —
 * never to an explicit Retry-After or a server rate-limit-reset window.
 */
function jitteredDelay(delayMs: number): number {
  return Math.round(delayMs * (0.5 + 0.5 * retryHooks.random()));
}

// Retry logic with exponential backoff
// ---------------------------------------------------------------------------

// 408 (request timeout) and 409 (lock timeout/conflict) are transient and worth
// a retry, matching the reference's shouldRetry (parity gap). the reference retries all 5xx
// generically; AMI enumerates the transient server/gateway codes it actually
// sees — 500/502/503 plus 504 (gateway timeout) and 507 (insufficient storage) —
// and the Anthropic-overload 529, while deliberately NOT retrying genuinely
// terminal 5xx like 501 (not implemented) / 505 (version not supported).
const RETRYABLE_STATUS_CODES = new Set([408, 409, 429, 500, 502, 503, 504, 507, 529]);

// Abort-responsive `sleep` + its `SleepOptions` now live in the canonical
// `./utils/sleep` module (parity with the reference's utils/sleep.ts) so provider
// backoff, the engine, and the control plane share ONE implementation instead of
// divergent private copies. Re-exported here for existing importers / callers.
export { sleep } from './utils/sleep';
export type { SleepOptions } from './utils/sleep';

/**
 * Foreground query sources that retry on an overload (HTTP 529). Everything NOT in
 * this set — and only a KNOWN source, never `undefined` — suppresses the 529 retry.
 * New sources default to no-retry: add one here only when a human/turn is blocking
 * on its result (parity: the reference's `FOREGROUND_529_RETRY_SOURCES`).
 */
export const FOREGROUND_529_RETRY_SOURCES: ReadonlySet<QuerySource> = new Set<QuerySource>([
  'repl_main_thread',
  'detached',
  'sdk',
  'agent',
  'compact',
  'side_question',
  'auto_mode',
]);

/**
 * Whether a 529 overload should be retried for the given query source. `undefined`
 * (untagged call paths) → retry, matching the reference's `shouldRetry529`: only a
 * known background source suppresses the overload retry. Does NOT affect 429 or any
 * other status — those always retry per {@link RETRYABLE_STATUS_CODES}.
 */
export function shouldRetry529(querySource: QuerySource | undefined): boolean {
  return querySource === undefined || FOREGROUND_529_RETRY_SOURCES.has(querySource);
}

const NETWORK_ERROR_CODES = new Set([
  'ECONNRESET', 'ECONNREFUSED', 'ECONNABORTED', 'ETIMEDOUT',
  'EPIPE', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH',
]);
export function isNetworkError(err: unknown): boolean {
  // Walk the cause chain: undici/SDK errors wrap the real ETIMEDOUT/ECONNRESET
  // under nested `.cause`, leaving a code-less "Connection error." at the top.
  const code = findErrorCode(err);
  if (code && NETWORK_ERROR_CODES.has(code)) return true;
  const msg = err instanceof Error ? err.message : '';
  return /ECONNRESET|ECONNREFUSED|ECONNABORTED|ETIMEDOUT|socket hang up/i.test(msg);
}

/**
 * Per-attempt timeout (ms) for the non-streaming fallback request (the reference parity:
 * `getNonstreamingFallbackTimeoutMs`): 120s against a remote endpoint, 300s for a
 * local one (a local model has no network RTT but is slower per token). A local
 * endpoint is a loopback/private-host base URL. Overridable via
 * `SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS` (a positive number wins).
 */
export function nonstreamingFallbackTimeoutMs(
  config: ProviderConfig,
  env: NodeJS.ProcessEnv = process.env,
): number {
  const override = Number(env.SUPERINFERENCE_NONSTREAMING_FALLBACK_TIMEOUT_MS);
  if (Number.isFinite(override) && override > 0) return override;
  // Resolve the host and classify it with anchored, backtracking-free checks (no
  // nested quantifiers — ReDoS-safe). A loopback or RFC-1918 private host is local.
  let host = '';
  try { host = new URL(config.baseUrl ?? '').hostname.toLowerCase(); } catch { host = ''; }
  const isLoopback = host === 'localhost' || host === '127.0.0.1' || host === '::1' || host === '0.0.0.0';
  const isPrivate = /^10\./.test(host)
    || /^192\.168\./.test(host)
    || /^172\.(1[6-9]|2[0-9]|3[01])\./.test(host);
  return (isLoopback || isPrivate) ? 300_000 : 120_000;
}

/**
 * Run ONE non-streaming fallback request and yield its result as normal stream
 * chunks. Reuses {@link streamChatCompletion} (so cache markers, betas, metadata,
 * image validation, tool-call pairing, and chunk emission are identical to the
 * streaming path) with the token budget capped ({@link adjustParamsForNonStreaming})
 * and a bounded per-attempt timeout ({@link nonstreamingFallbackTimeoutMs}) linked
 * to the caller's abort signal. The `generateTextImpl` seam lets tests inject a
 * deterministic non-streaming result.
 */
export async function* runNonStreamingFallback(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig; repairToolCall?: ToolCallRepairFunction<ToolSet>; querySource?: QuerySource; generateTextImpl?: typeof generateText },
): AsyncGenerator<StreamChunk> {
  const cappedConfig = adjustParamsForNonStreaming(config);
  const timeoutMs = nonstreamingFallbackTimeoutMs(config);
  const fbAbort = new AbortController();
  const onCallerAbort = () => fbAbort.abort();
  if (abortSignal.aborted) fbAbort.abort();
  else abortSignal.addEventListener('abort', onCallerAbort, { once: true });
  const timer = armUnrefTimer(() => fbAbort.abort(), timeoutMs);
  try {
    yield* streamChatCompletion(cappedConfig, messages, tools, fbAbort.signal, {
      thinking: options?.thinking,
      repairToolCall: options?.repairToolCall,
      querySource: options?.querySource,
      streamTextImpl: generateTextToStreamAdapter(options?.generateTextImpl ?? generateText),
    });
  } finally {
    clearTimeout(timer);
    abortSignal.removeEventListener('abort', onCallerAbort);
  }
}

export async function* streamChatCompletionWithRetry(
  config: ProviderConfig,
  messages: Message[],
  tools: ToolDefinition[],
  abortSignal: AbortSignal,
  options?: { thinking?: ThinkingConfig; repairToolCall?: ToolCallRepairFunction<ToolSet>; requestTimeoutMs?: number; idleTimeoutMs?: number; maxRetries?: number; baseDelayMs?: number; maxDelayMs?: number; maxTotalWaitMs?: number; heartbeatIntervalMs?: number; querySource?: QuerySource; onRetry?: (attempt: number, maxRetries: number, delayMs: number, statusCode: number | null, errorKind: RetryErrorKind) => void; streamImpl?: typeof streamChatCompletion; nowFn?: () => number; generateTextImpl?: typeof generateText },
): AsyncGenerator<StreamChunk> {
  // Test seam: the underlying single-attempt stream. Defaults to the real
  // streamChatCompletion so production behavior is unchanged; tests inject a
  // generator that throws (abort / SSL / network / generic) to drive the catch
  // and network-retry branches, which real transports surface as error chunks
  // rather than thrown errors.
  const baseStreamImpl = options?.streamImpl ?? streamChatCompletion;
  // Dev/test knob (AMI_MOCK_RATE_LIMIT): inject synthetic 429 rate-limit responses
  // ahead of the real request so the whole rate-limit path (classification, the
  // approaching/at-limit warning, Retry-After backoff, the retry loop) can be
  // exercised deterministically without a live provider 429 — parity with the reference
  // Code's shouldProcessMockLimits()/`/mock-limits`. Byte-identical when unset: the
  // real stream impl is used directly. The one-shot counter persists across the
  // retry-loop attempts of THIS call, so the injected 429s drain and the real
  // request runs once the quota is spent.
  const mockRateLimit = parseMockRateLimit(process.env[MOCK_RATE_LIMIT_ENV]);
  let mockRateLimitRemaining = mockRateLimit?.count ?? 0;
  const streamImpl: typeof streamChatCompletion = mockRateLimit
    ? async function* mockRateLimited(c, m, t, sig, o) {
        if (mockRateLimitRemaining > 0) {
          mockRateLimitRemaining--;
          yield buildMockRateLimitChunk(mockRateLimit);
          return;
        }
        yield* baseStreamImpl(c, m, t, sig, o);
      }
    : baseStreamImpl;
  // L4: resolve the configurable DEFAULT prompt-cache TTL (AMI_PROMPT_CACHE_TTL)
  // ONCE, before the retry loop. Done here — not inside streamChatCompletion — so
  // the gateway-rejection recovery below (which clears cacheTtl to drop the
  // extended-cache beta header a proxy rejected) is NOT re-defaulted on the next
  // attempt and the recovery can actually succeed. Byte-identical when the env is
  // unset and when the caller already set config.cacheTtl.
  config = { ...config, cacheTtl: resolveCacheTtl(config.cacheTtl) };

  // Client request-id for log correlation. Resolve/mint it ONCE (stable across all
  // retry attempts) and pin it into extraHeaders so the SAME id rides every wire
  // request (resolveModel's buildRequestIdHeader sees the existing header and does
  // not overwrite) AND can be surfaced on a terminal failure — the only handle for
  // a timeout, where the server returns no response id. Gated to first-party-style
  // endpoints; a managed gateway yields no id and the header stays absent.
  const reqProvider = config.provider
    || inferProviderFromApiKey(config.apiKey)?.provider
    || (config.baseUrl ? inferProviderFromBaseUrl(config.baseUrl)?.provider : undefined)
    || inferProviderFromEnv()?.provider;
  const clientRequestId = resolveClientRequestId(config, reqProvider);
  if (clientRequestId) {
    config = { ...config, extraHeaders: { ...(config.extraHeaders ?? {}), [REQUEST_ID_HEADER]: clientRequestId } };
  }
  // Decorate a terminal ERROR chunk with the correlation id (never an abort, which
  // is user-driven and needs no server lookup). Non-error chunks pass through.
  const withRequestId = (chunk: StreamChunk): StreamChunk =>
    (chunk.type === 'error' && chunk.error !== 'Request aborted')
      ? { ...chunk, error: appendRequestIdToError(chunk.error, clientRequestId) }
      : chunk;

  const envTimeout = envApiTimeoutMs(process.env);
  const timeoutMs = options?.requestTimeoutMs ?? (envTimeout > 0 ? envTimeout : DEFAULT_REQUEST_TIMEOUT_MS);
  const maxRetries = options?.maxRetries ?? MAX_RETRIES;
  const baseDelay = options?.baseDelayMs ?? BASE_DELAY_MS;
  const maxDelay = options?.maxDelayMs ?? 60_000;
  const maxTotalWait = options?.maxTotalWaitMs ?? 0;
  const retryStart = maxTotalWait > 0 ? Date.now() : 0;
  const heartbeatInterval = options?.heartbeatIntervalMs ?? RETRY_HEARTBEAT_INTERVAL_MS;

  // Backoff wait with a live countdown: a long (> heartbeatInterval) delay is sliced
  // and `onRetry` is re-fired with the decremented remaining time on each slice, so
  // the host sees periodic activity (not marked idle mid-wait) and the UI counts down.
  // A short delay sleeps once and fires nothing extra (unchanged). `attempt+1` and
  // `status` mirror the single pre-sleep onRetry call so every event is consistent.
  // `status` is `null` for a connection-level failure that produced NO HTTP response
  // (timeout / network drop), distinct from `0`/a real HTTP status (parity: the reference's
  // SDKAPIRetryMessage `error_status: number | null`).
  const waitBeforeRetry = (delayMs: number, attempt: number, status: number | null, kind: RetryErrorKind): Promise<void> =>
    sleepWithRetryHeartbeat(
      delayMs,
      abortSignal,
      options?.onRetry ? (remainingMs) => options.onRetry!(attempt + 1, maxRetries, remainingMs, status, kind) : undefined,
      heartbeatInterval,
    );

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    if (abortSignal.aborted) {
      yield { type: 'error', error: 'Request aborted' };
      return;
    }

    // Combine caller abort signal with per-request hard timeout
    const requestAbort = new AbortController();
    const timeoutId = armUnrefTimer(() => requestAbort.abort(), timeoutMs);
    const onCallerAbort = () => requestAbort.abort();
    abortSignal.addEventListener('abort', onCallerAbort, { once: true });

    let contentStarted = false;
    let timedOut = false;
    let networkRetry = false;
    let staleConnection = false;
    // Set when the inactivity watchdog (not the hard request-timeout) aborted the
    // stream, so a retry can report `stream_idle` distinctly from `connection_timeout`.
    let idleFired = false;

    // Inactivity watchdog: kill a stream that goes quiet (connection open, no
    // bytes) far sooner than the total request timeout. Reset on every chunk.
    const idleMs = options?.idleTimeoutMs;
    let idleTimer: ReturnType<typeof setTimeout> | undefined;
    // Soft-stall telemetry: distinguish a slow-but-alive stream from a dead one.
    // (1) A half-idle WARNING fires at idleMs/2 — a progress breadcrumb that the
    // stream has gone quiet — BEFORE the hard idle-abort kills it. (2) Inter-chunk
    // gaps past STREAM_STALL_THRESHOLD_MS are counted/summed and logged, so a stream
    // that is slow yet still delivering chunks is diagnosable, separate from the
    // abort. Neither aborts the stream (parity: the reference's streaming idle warning
    // + tengu_streaming_stall / tengu_streaming_stall_summary telemetry).
    let idleWarnTimer: ReturnType<typeof setTimeout> | undefined;
    // Clock seam: defaults to Date.now so production timing is unchanged; tests
    // inject a controllable clock to exercise the stall counter deterministically.
    const now = options?.nowFn ?? Date.now;
    let lastChunkAt = now();
    let stallCount = 0;
    let totalStallMs = 0;
    const resetIdle = () => {
      if (!idleMs) return;
      if (idleTimer) clearTimeout(idleTimer);
      if (idleWarnTimer) clearTimeout(idleWarnTimer);
      idleWarnTimer = armUnrefTimer(() => {
        log('api', `Streaming idle warning: no chunks received for ${Math.round(idleMs / 2 / 1000)}s`, undefined, 'warn');
      }, idleMs / 2);
      idleTimer = armUnrefTimer(() => { idleFired = true; requestAbort.abort(); }, idleMs);
    };
    const trackStall = () => {
      const at = now();
      const gap = at - lastChunkAt;
      lastChunkAt = at;
      if (gap >= STREAM_STALL_THRESHOLD_MS) {
        stallCount += 1;
        totalStallMs += gap;
        log('api', `Streaming stall: ${Math.round(gap / 1000)}s gap between stream events (stall #${stallCount})`, undefined, 'warn');
      }
    };

    try {
    resetIdle();
    for await (const chunk of streamImpl(config, messages, tools, requestAbort.signal, options)) {
      trackStall();
      resetIdle();
      if (chunk.type === 'error' && !contentStarted) {
        const statusCode = extractStatusCode(chunk.error ?? '');

        if (statusCode === 401) {
          yield withRequestId(chunk);
          return;
        }

        // Gateway rejection: a proxy rejected the anthropic-beta header. Drop the
        // ENTIRE beta set on the retry, not just the extended-cache token — a strict
        // proxy that 400s on one unknown beta typically rejects all of them (1M-context,
        // interleaved-thinking, structured-outputs, …). Clearing cacheTtl alone left the
        // other betas on the wire, so the retry 400'd again; `disableBetas` makes
        // computeAnthropicBetas return [] so the next attempt carries no beta header.
        if (statusCode === 400 && attempt < maxRetries && isGatewayRejection((chunk.error ?? '').toLowerCase())) {
          config = { ...config, cacheTtl: undefined, disableBetas: true };
          break; // retry without ANY beta headers
        }

        // Context-overflow 400 auto-recovery: the server reports input + max_tokens
        // exceed the context limit (its accounting disagreed with our preemptive
        // fitOutputTokens). Recompute a SMALLER max_tokens from the server's numbers
        // and retry the SAME request instead of failing the turn, mirroring the reference
        // Code's parseMaxTokensContextOverflowError + maxTokensOverride recovery.
        if (statusCode === 400 && attempt < maxRetries) {
          const overflow = parseMaxTokensContextOverflowError(chunk.error);
          if (overflow) {
            const adjusted = recoverMaxTokensFromContextOverflow(overflow, options?.thinking, config.model);
            // Only retry when the recomputed budget makes real progress (is strictly
            // smaller than what overflowed); otherwise the retry would overflow again.
            if (adjusted !== null && adjusted < overflow.maxTokens) {
              config = { ...config, maxTokens: adjusted };
              if (options?.onRetry) options.onRetry(attempt + 1, maxRetries, 0, 400, 'http');
              break; // retry with the reduced max_tokens
            }
          }
        }

        // Deterministic 400 "prompt is too long: N tokens > M maximum": the INPUT
        // (the prompt itself), not max_tokens, exceeds the context window, so
        // re-sending the SAME request loops — distinct from the recoverable
        // max_tokens-exceed 400 handled just above. Stop immediately with a distinct
        // non-retry diagnostic naming the token counts, even when a gateway sends
        // x-should-retry:true (which would otherwise hammer a deterministic failure).
        // Mirrors the reference's detectPromptTooLong special-casing.
        if (statusCode === 400) {
          const tooLong = detectPromptTooLong(chunk.error ?? '');
          if (tooLong) {
            const counts = tooLong.actualTokens !== undefined && tooLong.limitTokens !== undefined
              ? ` (${tooLong.actualTokens} > ${tooLong.limitTokens} tokens)`
              : '';
            log('api', `API error (prompt too long${counts}): deterministic 400 — not retrying; reduce context (/compact or /clear).`);
            yield withRequestId(chunk);
            return;
          }
        }

        // Honor the API's x-should-retry hint (the SDK retry layer is disabled
        // via maxRetries:0, so we honor it here): 'false' => never retry, even a
        // normally-retryable status (otherwise we hold the turn for minutes
        // re-sending a request the API already said is pointless); 'true' =>
        // retry even a normally-terminal status the API says is worth retrying;
        // absent => fall back to the retryable-status set.
        const shouldRetryHdr = chunk.responseHeaders?.['x-should-retry'];
        // A streamed overload may lose its 529 status; detect it from the body so it
        // is still retried (unless the API explicitly says x-should-retry: false).
        const bodyOverloaded = statusCode === null && isOverloadedError(chunk.error ?? '');
        // An empty/degenerate stream (streamChatCompletion's EMPTY_STREAM_ERROR
        // sentinel) is a transient proxy failure — retry it with backoff, matching
        // the reference's non-streaming-fallback-on-empty-stream behavior.
        const emptyStream = statusCode === null && isEmptyStreamError(chunk.error);
        // Overload (529) foreground/background gating: a 529 (parsed status OR a
        // body-detected overloaded_error) is retried only for foreground sources.
        // A KNOWN background source (summaries, memory extraction, titles, critic)
        // bails immediately — during a capacity cascade each retry is pure gateway
        // amplification and the user never sees it fail (parity: shouldRetry529).
        // Untagged (undefined) sources stay conservative (retry), so existing
        // callers are unaffected. The x-should-retry header still wins when set.
        const is529Overload = statusCode === OVERLOADED_ERROR_STATUS || bodyOverloaded;
        const overloadSuppressed = is529Overload && !shouldRetry529(options?.querySource);
        const retryAllowed = shouldRetryHdr === 'false'
          ? false
          : shouldRetryHdr === 'true'
          ? true
          : overloadSuppressed
          ? false
          : (bodyOverloaded || emptyStream || (statusCode !== null && RETRYABLE_STATUS_CODES.has(statusCode)));
        if (retryAllowed && attempt < maxRetries) {
          if (maxTotalWait > 0 && (Date.now() - retryStart) >= maxTotalWait) {
            yield withRequestId(chunk);
            return;
          }

          // Per-attempt diagnostic (parity: the reference's `API error (attempt X/Y)`
          // logForDebugging): record the status + sanitized reason tied to the attempt
          // number so a human reading the debug log can tell WHY it is retrying
          // (overloaded_error vs 429 vs ECONNRESET vs timeout), not just a number.
          const gateway = identifyGateway(chunk.responseHeaders);
          log('api', `API retry (attempt ${attempt + 1}/${maxRetries}): status=${statusCode ?? (bodyOverloaded ? OVERLOADED_ERROR_STATUS : 'none')} ${gateway ? `gateway=${gateway} ` : ''}${sanitizeErrorMessage(chunk.error ?? '')}`);

          let delayMs: number;
          const retryAfterSeconds = chunk.retryAfter;
          const expBackoff = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
          if (retryAfterSeconds != null) {
            // Honor Retry-After for ANY retryable status (429/500/503/529/…), not
            // just 429 — an overloaded endpoint that sends Retry-After with a 529
            // must be respected rather than hammered with exponential backoff.
            delayMs = Math.max(retryAfterSeconds * 1000, baseDelay);
          } else if (statusCode === 429 && chunk.responseHeaders) {
            // No Retry-After header: when the server's rate-limit headers say we
            // are out of budget, honor the reset window (x-ratelimit-reset-*)
            // instead of a blind exponential backoff. Floored at baseDelay,
            // capped at maxDelay.
            const info = parseRateLimitHeaders(chunk.responseHeaders);
            delayMs = shouldBackoff(info)
              ? Math.min(Math.max(suggestedWaitMs(info, expBackoff), baseDelay), maxDelay)
              : jitteredDelay(expBackoff);
          } else {
            delayMs = jitteredDelay(expBackoff);
          }

          // statusCode is null when the retry was forced by x-should-retry:true on an
          // error with no parsed HTTP status (a connection-level failure that produced
          // no HTTP response); the callback receives `null` for that case so a consumer
          // can distinguish it from a real HTTP status (parity: the reference's
          // SDKAPIRetryMessage `error_status: number | null`). A body-detected overload
          // reports as 529 so the callback sees the overload it is actually retrying.
          const retryStatus = statusCode ?? (bodyOverloaded ? OVERLOADED_ERROR_STATUS : null);
          // Classify the retry cause: a real (or body-detected 529) HTTP status is
          // `http`; a null-status forced retry is a degenerate/empty stream
          // (`stream_idle`) or an otherwise code-less connection drop (`connection_reset`).
          const retryKind: RetryErrorKind = retryStatus !== null
            ? 'http'
            : emptyStream ? 'stream_idle' : 'connection_reset';
          if (options?.onRetry) {
            options.onRetry(attempt + 1, maxRetries, delayMs, retryStatus, retryKind);
          }

          try {
            await waitBeforeRetry(delayMs, attempt, retryStatus, retryKind);
          } catch {
            yield { type: 'error', error: 'Request aborted' };
            return;
          }
          break; // break inner loop to retry
        }

        // Non-streaming fallback (parity: executeNonStreamingRequest). A broken-SSE
        // signal — an empty/non-SSE proxy body (EMPTY_STREAM_ERROR, reached here once
        // the streaming retries are spent) or a 404 on the streaming endpoint
        // (non-retryable, so reached immediately) — is recoverable by ONE
        // non-streaming request. This branch always returns, so it runs at most once.
        if (
          nonStreamingFallbackEnabled(config)
          && (isEmptyStreamError(chunk.error) || statusCode === 404)
        ) {
          let recovered = false;
          for await (const fb of runNonStreamingFallback(config, messages, tools, abortSignal, {
            thinking: options?.thinking,
            repairToolCall: options?.repairToolCall,
            querySource: options?.querySource,
            generateTextImpl: options?.generateTextImpl,
          })) {
            // The fallback's OWN failure (it also errored / stayed empty) must not
            // mask the original error: discard its leading error and surface the
            // original streaming error below instead.
            if (!recovered && fb.type === 'error') break;
            recovered = true;
            yield fb;
          }
          if (recovered) return;
        }

        yield withRequestId(chunk);
        return;
      }

      // Once we yield any content, we're committed to this stream
      if (chunk.type === 'content_delta' || chunk.type === 'tool_call_delta' || chunk.type === 'thinking_delta') {
        contentStarted = true;
      }

      // Detect timeout: if we get a 'done' because the request was aborted by our timeout
      // (not the caller's abort), treat it as a timeout, not a normal completion
      if (chunk.type === 'done' && requestAbort.signal.aborted && !abortSignal.aborted && !chunk.finishReason) {
        timedOut = true;
        break;
      }

      yield chunk;

      if (chunk.type === 'done' || (chunk.type === 'error' && contentStarted)) {
        return;
      }
    }
    } catch (err) {
      // A cancellation here with the caller's signal NOT aborted is our internal
      // idle/timeout abort firing — treat as a timeout. The shared `isAbort`
      // detector (unlike the old `msg.includes('abort')`) will NOT mistake an
      // ECONNABORTED network drop for a cancellation, so that falls through to the
      // network-retry branch below instead of being swallowed as a timeout.
      if (isAbort(err) && !abortSignal.aborted) {
        timedOut = true;
      } else if (requestAbort.signal.aborted && !abortSignal.aborted) {
        timedOut = true;
      } else if (isSslError(err)) {
        // SSL/TLS errors are not transient — fail immediately with actionable hints
        const original = err instanceof Error ? err.message : String(err);
        throw new Error(formatSslError(original));
      } else if (!contentStarted && attempt < maxRetries && isNetworkError(err)) {
        networkRetry = true;
        // Record the root errno + SSL flag for the dropped connection so a
        // code-less "Connection error." is diagnosable from the debug log
        // (the SDK buries the real ETIMEDOUT/ECONNRESET under nested `.cause`).
        const diagnostic = connectionErrorDiagnostic(err);
        if (diagnostic) log('api', diagnostic, undefined, 'error');
        // A STALE pooled keep-alive socket (ECONNRESET/EPIPE) must be dropped, not
        // reused, or the retry grabs the same dead socket — flag the next attempt
        // to open a fresh connection with keep-alive disabled.
        staleConnection = isStaleConnectionError(err);
      } else {
        throw err;
      }
    } finally {
      clearTimeout(timeoutId);
      if (idleTimer) clearTimeout(idleTimer);
      if (idleWarnTimer) clearTimeout(idleWarnTimer);
      // Completion summary when the stream stalled at least once while still alive
      // (parity: the reference's tengu_streaming_stall_summary) — explains why a turn
      // felt slow without conflating it with the hard idle-abort.
      if (stallCount > 0) {
        log('api', `Streaming stall summary: ${stallCount} stall(s), ${Math.round(totalStallMs / 1000)}s total stalled`, undefined, 'warn');
      }
      abortSignal.removeEventListener('abort', onCallerAbort);
    }

    // Also detect timeout when the stream ended gracefully due to abort
    if (!timedOut && requestAbort.signal.aborted && !abortSignal.aborted) {
      timedOut = true;
    }

    if (timedOut) {
      if (contentStarted) {
        yield withRequestId({ type: 'error', error: formatTimeoutError(timeoutMs, true) });
        return;
      }
      // No content yet — treat like a retryable error
      if (attempt < maxRetries) {
        if (maxTotalWait > 0 && (Date.now() - retryStart) >= maxTotalWait) {
          yield withRequestId({ type: 'error', error: `Retry time limit exceeded (${maxTotalWait}ms)` });
          return;
        }
        const timeoutDelay = jitteredDelay(Math.min(baseDelay * Math.pow(2, attempt), maxDelay));
        // No HTTP response was produced (request timed out) — report `null`, not a
        // fabricated status, so the retry event reflects a connection-level failure.
        // Distinguish the inactivity-watchdog abort (`stream_idle`) from the hard
        // request-timeout (`connection_timeout`).
        const timeoutKind: RetryErrorKind = idleFired ? 'stream_idle' : 'connection_timeout';
        if (options?.onRetry) {
          options.onRetry(attempt + 1, maxRetries, timeoutDelay, null, timeoutKind);
        }
        try {
          await waitBeforeRetry(timeoutDelay, attempt, null, timeoutKind);
        } catch {
          yield { type: 'error', error: 'Request aborted' };
          return;
        }
        continue;
      }
      yield withRequestId({ type: 'error', error: formatTimeoutError(timeoutMs, false) });
      return;
    }

    if (networkRetry) {
      if (maxTotalWait > 0 && (Date.now() - retryStart) >= maxTotalWait) {
        yield withRequestId({ type: 'error', error: 'Network error (retry time limit exceeded)' });
        return;
      }
      // Stale keep-alive socket (ECONNRESET/EPIPE): force the next attempt onto a
      // fresh, keep-alive-disabled connection so it cannot reuse the dead socket.
      if (staleConnection) {
        config = { ...config, forceFreshConnection: true };
      }
      const netDelay = jitteredDelay(Math.min(baseDelay * Math.pow(2, attempt), maxDelay));
      // A connection-level network failure (ECONNRESET/ETIMEDOUT/…) produced no HTTP
      // response — report `null`, not a fabricated status, so the retry event reflects
      // that there was no server status to classify.
      if (options?.onRetry) {
        options.onRetry(attempt + 1, maxRetries, netDelay, null, 'connection_reset');
      }
      try {
        await waitBeforeRetry(netDelay, attempt, null, 'connection_reset');
      } catch {
        yield { type: 'error', error: 'Request aborted' };
        return;
      }
      continue;
    }

    // If we got here via 'break' (retry), the inner loop ended without return — continue outer loop
    if (contentStarted) {
      return; // Stream completed or errored mid-content
    }
  }

  yield withRequestId({ type: 'error', error: `Max retries (${maxRetries}) exceeded` });
}
