import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { safeWriteConfig, validateConfigJson, stripJsoncComments } from '../config';
import { THINKING_LEVELS } from '../model-capabilities';
import { canOverride, MANAGED_ONLY_KEYS, TIER_LOCKED_KEYS } from '../settings-guard';

/**
 * Metadata describing a single supported config setting. This registry is the
 * single source of truth for which keys the config tool may read/write, their
 * type, their enumerated options (if any), nested storage path, and optional
 * write-validation / read-formatting hooks. Mirrors Claude Code's
 * SUPPORTED_SETTINGS registry (tools/ConfigTool/supportedSettings.ts).
 */
export interface SettingConfig {
  type: 'boolean' | 'string' | 'number';
  description: string;
  /** Storage path within the config object. Defaults to key.split('.'). */
  path?: string[];
  /** Enumerated valid string values; rejection lists these back to the caller. */
  options?: readonly string[];
  /** Synchronous per-setting write validation (e.g. non-empty model). */
  validateOnWrite?: (v: unknown) => { valid: boolean; error?: string };
  /** Formats a value for display on read (e.g. undefined → "default"). */
  formatOnRead?: (v: unknown) => unknown;
}

const nonEmptyString = (label: string) => (v: unknown): { valid: boolean; error?: string } =>
  typeof v === 'string' && v.trim().length > 0
    ? { valid: true }
    : { valid: false, error: `${label} must be a non-empty string.` };

const positiveNumber = (label: string) => (v: unknown): { valid: boolean; error?: string } =>
  typeof v === 'number' && Number.isFinite(v) && v > 0
    ? { valid: true }
    : { valid: false, error: `${label} must be a positive number.` };

/**
 * Supported-settings registry (allow-list) for AMI's project config. A key
 * absent from this table is rejected on both get and set so the model cannot
 * silently write arbitrary/typo'd keys into `.ami/config.json`.
 */
export const SUPPORTED_SETTINGS: Record<string, SettingConfig> = {
  model: {
    type: 'string',
    description: 'Override the default model (e.g. "claude-opus-4-8", "gpt-4o").',
    validateOnWrite: nonEmptyString('model'),
  },
  provider: {
    type: 'string',
    description: 'LLM provider id (e.g. "anthropic", "openai", "google").',
    options: ['anthropic', 'openai', 'google', 'openrouter', 'vertex', 'bedrock'],
  },
  baseUrl: {
    type: 'string',
    description: 'Override the provider base URL (OpenAI-compatible endpoints).',
    validateOnWrite: nonEmptyString('baseUrl'),
  },
  permissionMode: {
    type: 'string',
    description: 'Default permission mode for tool usage (security-sensitive).',
    options: ['ask', 'auto-allow', 'deny-all'],
  },
  thinkingLevel: {
    type: 'string',
    description: 'Extended-thinking level.',
    options: THINKING_LEVELS,
  },
  maxEffortLevel: {
    type: 'string',
    description: 'Cap on the reasoning-effort scale for subagents/code review.',
    options: ['low', 'medium', 'high', 'xhigh', 'max'],
  },
  fallbackModel: {
    type: 'string',
    description: 'Model to fall back to on provider errors.',
    validateOnWrite: nonEmptyString('fallbackModel'),
  },
  compactionModel: {
    type: 'string',
    description: 'Model used for context compaction.',
    validateOnWrite: nonEmptyString('compactionModel'),
  },
  maxTurns: {
    type: 'number',
    description: 'Maximum agentic turns before the session stops.',
    validateOnWrite: positiveNumber('maxTurns'),
  },
  tokenBudget: {
    type: 'number',
    description: 'Session token budget.',
    validateOnWrite: positiveNumber('tokenBudget'),
  },
  maxBudgetUsd: {
    type: 'number',
    description: 'Hard USD spend cap; the session stops when exceeded.',
    validateOnWrite: positiveNumber('maxBudgetUsd'),
  },
  skill: {
    type: 'string',
    description: 'Default skill/stance to activate.',
    validateOnWrite: nonEmptyString('skill'),
  },
  cacheTtl: {
    type: 'string',
    description: 'Anthropic prompt-cache TTL.',
    options: ['5m', '1h'],
  },
  safeMode: {
    type: 'boolean',
    description: 'Skip untrusted project-local hooks/MCP/memory/skills.',
  },
  deferTools: {
    type: 'boolean',
    description: 'Defer long-tail tools until tool_search loads them.',
  },
  maxWebSearchesPerSession: {
    type: 'number',
    description: 'Session-wide cap on web_search tool calls.',
    validateOnWrite: positiveNumber('maxWebSearchesPerSession'),
  },
  bashOutputMaxChars: {
    type: 'number',
    description: 'Cap (chars) on inline bash stdout before it is spilled.',
    validateOnWrite: positiveNumber('bashOutputMaxChars'),
  },
  taskOutputMaxChars: {
    type: 'number',
    description: 'Cap (chars) on inline background task output before spill.',
    validateOnWrite: positiveNumber('taskOutputMaxChars'),
  },
  statusLine: {
    type: 'string',
    description: 'Shell command whose stdout renders as a custom status line.',
    validateOnWrite: nonEmptyString('statusLine'),
  },
  'superinference.enforcing': {
    type: 'boolean',
    description: 'Enforce the SuperInference reflection gate.',
  },
  'superinference.useLLMCritic': {
    type: 'boolean',
    description: 'Use an LLM critic in the SuperInference gate.',
  },
};

export function isSupported(key: string): boolean {
  return Object.prototype.hasOwnProperty.call(SUPPORTED_SETTINGS, key);
}

/**
 * Security guard against self-escalation via the model-facing config tool. The
 * config tool mutates `.ami/config.json` — i.e. it writes at the **project**
 * tier. Keys that are managed-only or tier-locked (permissionMode, maxTurns,
 * maxBudgetUsd, maxSessionMs) must not be writable from this path, or the model
 * could lift its own permission mode / budget caps. Reuses the shared
 * settings-guard tier rules (no duplicate policy). Returns a rejection reason
 * when the project tier may not set the key, else null.
 */
export function configToolWriteBlockReason(key: string): string | null {
  // canOverride(project, managed) is false for both MANAGED_ONLY and TIER_LOCKED
  // keys, which is exactly the self-escalation set we must refuse here.
  if (canOverride(key, 'project', 'managed')) return null;
  if (MANAGED_ONLY_KEYS.has(key)) {
    return `"${key}" is a managed-only setting and cannot be changed by the config tool. It is set by your administrator's managed config.`;
  }
  // TIER_LOCKED (security-sensitive) — e.g. permissionMode self-escalation.
  return `"${key}" is a security-sensitive, tier-locked setting and cannot be changed by the config tool (it would let the session escalate its own ${key === 'permissionMode' ? 'permissions' : 'limits'}). Change it at launch (CLI flag / trusted config) instead.`;
}

export function getSettingConfig(key: string): SettingConfig | undefined {
  return SUPPORTED_SETTINGS[key];
}

export function getAllSettingKeys(): string[] {
  return Object.keys(SUPPORTED_SETTINGS);
}

export function getOptionsForSetting(key: string): string[] | undefined {
  const config = SUPPORTED_SETTINGS[key];
  if (!config?.options) return undefined;
  return [...config.options];
}

export function getSettingPath(key: string): string[] {
  const config = SUPPORTED_SETTINGS[key];
  return config?.path ?? key.split('.');
}

/** Reads a (possibly nested) value out of the config object by path. */
function getValueByPath(config: Record<string, unknown>, keyPath: string[]): unknown {
  let current: unknown = config;
  for (const segment of keyPath) {
    if (current && typeof current === 'object' && !Array.isArray(current) && segment in current) {
      current = (current as Record<string, unknown>)[segment];
    } else {
      return undefined;
    }
  }
  return current;
}

/** Writes a (possibly nested) value into the config object by path, in place. */
function setValueByPath(config: Record<string, unknown>, keyPath: string[], value: unknown): void {
  let current = config;
  for (let i = 0; i < keyPath.length - 1; i++) {
    const segment = keyPath[i];
    const existing = current[segment];
    if (!existing || typeof existing !== 'object' || Array.isArray(existing)) {
      current[segment] = {};
    }
    current = current[segment] as Record<string, unknown>;
  }
  current[keyPath[keyPath.length - 1]] = value;
}

/** Deletes a (possibly nested) key, pruning now-empty parent objects. */
function deleteValueByPath(config: Record<string, unknown>, keyPath: string[]): void {
  if (keyPath.length === 1) {
    delete config[keyPath[0]];
    return;
  }
  const parent = getValueByPath(config, keyPath.slice(0, -1));
  if (parent && typeof parent === 'object' && !Array.isArray(parent)) {
    delete (parent as Record<string, unknown>)[keyPath[keyPath.length - 1]];
    if (Object.keys(parent).length === 0) deleteValueByPath(config, keyPath.slice(0, -1));
  }
}

/** Coerce an incoming value to the setting's declared type. */
function coerceValue(type: SettingConfig['type'], value: unknown): { value?: unknown; error?: string } {
  if (type === 'boolean') {
    if (typeof value === 'boolean') return { value };
    if (typeof value === 'string') {
      const lower = value.toLowerCase().trim();
      if (lower === 'true') return { value: true };
      if (lower === 'false') return { value: false };
    }
    return { error: 'requires true or false.' };
  }
  if (type === 'number') {
    if (typeof value === 'number' && Number.isFinite(value)) return { value };
    if (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))) {
      return { value: Number(value) };
    }
    return { error: 'requires a number.' };
  }
  // string
  if (typeof value === 'string') return { value };
  if (typeof value === 'number' || typeof value === 'boolean') return { value: String(value) };
  return { error: 'requires a string.' };
}

function buildDescription(): string {
  const lines = getAllSettingKeys().map((key) => {
    const c = SUPPORTED_SETTINGS[key];
    const opts = c.options ? ` [${c.options.join(' | ')}]` : c.type === 'boolean' ? ' [true | false]' : '';
    return `  - ${key} (${c.type})${opts}: ${c.description}`;
  });
  return (
    'Read or update AMI project configuration in .ami/config.json. ' +
    'Use action="get" [key] to read, or action="set" key=<name> value=<value>. ' +
    'Set value to "default" to reset a key to its built-in default. Supported settings:\n' +
    lines.join('\n')
  );
}

const CONFIG_REL = ['.ami', 'config.json'];

export const configTool: ToolDefinition = {
  name: 'config',
  description: buildDescription(),
  inputSchema: {
    type: 'object',
    properties: {
      action: { type: 'string', enum: ['get', 'set'], description: 'Get current config or set a value.' },
      key: { type: 'string', description: 'Config key (see tool description for the supported list).' },
      value: {
        type: 'string',
        description: 'Value to set (string, boolean, or number accepted; "default" resets the key).',
      },
    },
    required: ['action'],
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const configPath = path.join(context.cwd, ...CONFIG_REL);
    const action = input.action as string;

    if (action === 'get') return handleGet(configPath, input.key as string | undefined);
    if (action === 'set') return handleSet(configPath, input);
    return { output: 'Usage: action="get" [key] or action="set" key=... value=...', isError: true };
  },
};

function readConfigRaw(configPath: string): { config?: Record<string, unknown>; error?: string; absent?: boolean } {
  let raw: string;
  try {
    raw = fs.readFileSync(configPath, 'utf-8');
  } catch {
    return { absent: true };
  }
  const validation = validateConfigJson(stripJsoncComments(raw));
  if (!validation.valid) {
    return { error: validation.error };
  }
  return { config: JSON.parse(stripJsoncComments(raw)) as Record<string, unknown> };
}

function handleGet(configPath: string, key: string | undefined): ToolResult {
  const { config, error, absent } = readConfigRaw(configPath);
  if (absent) return { output: 'No project config found. Create .ami/config.json via action="set".' };
  if (error) {
    return { output: `Config file is corrupt and cannot be read: ${error}`, isError: true };
  }
  if (key) {
    if (!isSupported(key)) {
      return { output: `Unknown setting: "${key}". Supported keys: ${getAllSettingKeys().join(', ')}`, isError: true };
    }
    const settingConfig = getSettingConfig(key)!;
    const value = getValueByPath(config!, getSettingPath(key));
    const display = settingConfig.formatOnRead ? settingConfig.formatOnRead(value) : value;
    return { output: `${key} = ${JSON.stringify(display)}`, metadata: { operation: 'get', setting: key, value: display } };
  }
  return { output: JSON.stringify(config, null, 2), metadata: { operation: 'get' } };
}

function handleSet(configPath: string, input: Record<string, unknown>): ToolResult {
  const key = input.key as string | undefined;
  const value = input.value;
  if (!key) {
    return { output: 'Usage: action="set" key=... value=...', isError: true };
  }
  if (!isSupported(key)) {
    return { output: `Unknown setting: "${key}". Supported keys: ${getAllSettingKeys().join(', ')}`, isError: true };
  }

  // Self-escalation guard: refuse managed-only / tier-locked (security-sensitive)
  // keys before touching the file, so the model cannot raise its own permission
  // mode or budget caps via the config tool.
  const blockReason = configToolWriteBlockReason(key);
  if (blockReason) {
    return { output: `Refusing to write: ${blockReason}`, isError: true };
  }

  // Load existing config; refuse to clobber a corrupt file (don't wipe user config).
  const { config: existing, error: readError, absent } = readConfigRaw(configPath);
  if (readError) {
    return { output: `Refusing to write: existing config is corrupt (${readError}). Fix ${configPath} first.`, isError: true };
  }
  const config: Record<string, unknown> = absent ? {} : existing!;
  const settingConfig = getSettingConfig(key)!;
  const settingPath = getSettingPath(key);
  const previousValue = getValueByPath(config, settingPath);

  // Reset-to-default: value "default" unsets the key so it falls back to the built-in default.
  if (typeof value === 'string' && value.toLowerCase().trim() === 'default') {
    if (previousValue === undefined) {
      return { output: `${key} is already at its default.`, metadata: { operation: 'set', setting: key, previousValue, newValue: undefined } };
    }
    deleteValueByPath(config, settingPath);
    try {
      safeWriteConfig(configPath, config);
    } catch (err) {
      return { output: `Error writing config: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
    return { output: `Reset ${key} to default.`, metadata: { operation: 'set', setting: key, previousValue, newValue: undefined } };
  }

  if (value === undefined) {
    return { output: `Provide a value for "${key}" (or "default" to reset it).`, isError: true };
  }

  // Coerce to the declared type.
  const coerced = coerceValue(settingConfig.type, value);
  if (coerced.error) {
    return { output: `${key} ${coerced.error}`, isError: true };
  }
  const finalValue = coerced.value;

  // Enum/options validation, listing the valid options back to the caller.
  const options = getOptionsForSetting(key);
  if (options && !options.includes(String(finalValue))) {
    return { output: `Invalid value "${value}" for ${key}. Options: ${options.join(', ')}`, isError: true };
  }

  // Per-setting write validation (e.g. non-empty model, positive number).
  if (settingConfig.validateOnWrite) {
    const result = settingConfig.validateOnWrite(finalValue);
    if (!result.valid) {
      return { output: result.error ?? `Invalid value for ${key}.`, isError: true };
    }
  }

  // No-op short-circuit when unchanged.
  if (previousValue === finalValue) {
    return { output: `${key} is already ${JSON.stringify(finalValue)} (no change).`, metadata: { operation: 'set', setting: key, previousValue, newValue: finalValue } };
  }

  setValueByPath(config, settingPath, finalValue);
  try {
    safeWriteConfig(configPath, config);
  } catch (err) {
    return { output: `Error writing config: ${err instanceof Error ? err.message : String(err)}`, isError: true };
  }
  return {
    output: `Set ${key} = ${JSON.stringify(finalValue)}${previousValue !== undefined ? ` (was ${JSON.stringify(previousValue)})` : ''}`,
    metadata: { operation: 'set', setting: key, previousValue, newValue: finalValue },
  };
}
