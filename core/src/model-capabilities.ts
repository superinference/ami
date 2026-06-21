export type ThinkingLevel = 'off' | 'low' | 'medium' | 'high' | 'max';

export interface ModelCapabilities {
  reasoning: boolean;
  defaultThinkingLevel: ThinkingLevel;
  supportsAdaptiveThinking: boolean;
  requiresTemperatureOne: boolean;
  temperatureMustBeUnset: boolean;
  maxThinkingBudget: number;
  maxContextTokens?: number;
  maxOutputTokens?: number;
  supportsVision?: boolean;
  supportsToolUse?: boolean;
}

const CONTEXT_WINDOWS: Record<string, number> = {
  'claude-opus-4': 200000,
  'claude-sonnet-4': 200000,
  'claude-haiku-3.5': 200000,
  'gpt-4o': 128000,
  'gpt-4-turbo': 128000,
  'gpt-4.1': 1047576,
  'o1': 200000,
  'o3': 200000,
  'o4-mini': 200000,
  'gemini-2.0-flash': 1048576,
  'gemini-2.5-pro': 1048576,
  'gemini-2.5-flash': 1048576,
  'gemini-3': 2097152,
  'gpt-4o-mini': 128000,
  'gemini-1.5-pro': 1048576,
  'o1-mini': 128000,
  'o3-mini': 200000,
};

export function getContextWindow(modelId: string): number {
  for (const [prefix, tokens] of Object.entries(CONTEXT_WINDOWS)) {
    if (modelId.startsWith(prefix)) return tokens;
  }
  return 128000;
}

const REASONING_MODELS: Array<{
  pattern: RegExp;
  capabilities: ModelCapabilities;
}> = [
  // Anthropic Claude 4.x — adaptive thinking
  { pattern: /claude-opus-4/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: true, requiresTemperatureOne: true,
      temperatureMustBeUnset: false, maxThinkingBudget: 128000 } },
  { pattern: /claude-sonnet-4/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: true, requiresTemperatureOne: true,
      temperatureMustBeUnset: false, maxThinkingBudget: 128000 } },

  // OpenAI reasoning models — reasoning_effort
  { pattern: /^o1($|-)/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },
  { pattern: /^o3($|-)/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },
  { pattern: /^o4-mini/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: true, maxThinkingBudget: 0 } },

  // Google Gemini 2.5 — thinkingConfig
  { pattern: /gemini-2\.5-pro/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'medium',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 32768 } },
  { pattern: /gemini-2\.5-flash/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'low',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 32768 } },

  // DeepSeek reasoning — inline <think> tags
  { pattern: /deepseek-r1|deepseek-reasoner/,
    capabilities: { reasoning: true, defaultThinkingLevel: 'high',
      supportsAdaptiveThinking: false, requiresTemperatureOne: false,
      temperatureMustBeUnset: false, maxThinkingBudget: 0 } },
];

export function getModelCapabilities(modelId: string): ModelCapabilities | null {
  for (const entry of REASONING_MODELS) {
    if (entry.pattern.test(modelId)) {
      return entry.capabilities;
    }
  }
  return null;
}

export function isReasoningModel(modelId: string): boolean {
  return getModelCapabilities(modelId) !== null;
}

const BUDGET_MAP: Record<ThinkingLevel, number> = {
  off: 0,
  low: 4096,
  medium: 10240,
  high: 32768,
  max: 128000,
};

export function resolveThinkingBudget(level: ThinkingLevel): number {
  return BUDGET_MAP[level] ?? 0;
}

const PROVIDER_SAMPLING_DEFAULTS: Record<string, { temperature?: number; topP?: number; topK?: number }> = {
  'gemini': { temperature: 1.0, topK: 64 },
  'qwen': { temperature: 0.55, topP: 1 },
  'deepseek': { temperature: 0.6 },
};

export function getProviderSamplingDefaults(modelId: string): { temperature?: number; topP?: number; topK?: number } {
  for (const [prefix, defaults] of Object.entries(PROVIDER_SAMPLING_DEFAULTS)) {
    if (modelId.toLowerCase().includes(prefix)) return defaults;
  }
  return {};
}

export function resolveTemperature(
  modelId: string,
  configTemperature: number | undefined,
  thinking: { enabled: boolean } | undefined,
): number | undefined {
  if (!thinking?.enabled) {
    // If no explicit temperature configured, use provider-specific defaults
    if (configTemperature === undefined) {
      const defaults = getProviderSamplingDefaults(modelId);
      return defaults.temperature;
    }
    return configTemperature;
  }

  const caps = getModelCapabilities(modelId);
  if (!caps) {
    if (configTemperature === undefined) {
      const defaults = getProviderSamplingDefaults(modelId);
      return defaults.temperature;
    }
    return configTemperature;
  }

  // Claude: temperature must be omitted (SDK sets it to 1 internally)
  if (caps.requiresTemperatureOne) return undefined;

  // OpenAI o-series: temperature must not be sent at all
  if (caps.temperatureMustBeUnset) return undefined;

  return configTemperature;
}
