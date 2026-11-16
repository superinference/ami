import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { updateConfigFile, validateConfigJson, stripJsoncComments, getGlobalConfigPath, type ProjectConfig } from '../config';
import { THINKING_LEVELS } from '../model-capabilities';
import { canOverride, MANAGED_ONLY_KEYS, TIER_LOCKED_KEYS } from '../settings-guard';
import { listProviders, MODEL_ALIASES, resolveModelAlias, validateModel } from '../model-registry';

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
  /**
   * Optional ASYNC per-setting write validation, run after the synchronous
   * checks and only when the write would actually change the value. Mirrors
   * Claude Code's `validateOnWrite` hook (tools/ConfigTool/ConfigTool.ts): a
   * semantic/reachability check that may need I/O — e.g. confirming a `model`
   * id is actually served by the configured provider — so an invalid-but-
   * well-formed value is rejected at write time instead of only failing later
   * at inference. Receives the tool context so a validator can reach injected
   * state (e.g. the resolved provider config). When no such context is
   * available the validator should soft-allow (return valid) rather than block.
   */
  asyncValidateOnWrite?: (v: unknown, context: ToolContext) => Promise<{ valid: boolean; error?: string }>;
  /** Formats a value for display on read (e.g. undefined → "default"). */
  formatOnRead?: (v: unknown) => unknown;
}

/**
 * Async write-validator for model-valued settings (model / fallbackModel /
 * compactionModel). When the running engine has resolved provider credentials
 * (`context._providerConfig`), the requested id is checked against the provider's
 * live/known model list via the shared {@link validateModel} (the SAME check the
 * /model switch path uses — no parallel validation logic), and a bad id is
 * rejected before it is persisted, with the provider's suggestion / available
 * list surfaced so the model can self-correct. Without a provider config (bare
 * engine, offline edit, tests) it soft-allows: the synchronous non-empty guard
 * already ran, and there is no reachable list to check against. `validateModel`
 * itself also soft-allows transient network/API failures, so a blip never blocks
 * a legitimate switch.
 */
async function validateModelOnWrite(
  v: unknown,
  context: ToolContext,
): Promise<{ valid: boolean; error?: string }> {
  const providerConfig = context._providerConfig;
  if (!providerConfig) return { valid: true };
  const modelId = String(v);
  const result = await validateModel(providerConfig, modelId);
  if (result.valid) return { valid: true };
  const parts = [`"${modelId}" is not a valid model for the configured provider.`];
  if (result.suggestion) {
    parts.push(`Did you mean "${result.suggestion}"?`);
  } else if (result.available.length > 0) {
    parts.push(`Available: ${result.available.slice(0, 20).join(', ')}.`);
  }
  if (result.reason) parts.push(result.reason);
  return { valid: false, error: parts.join(' ') };
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
    asyncValidateOnWrite: validateModelOnWrite,
    // An unset model falls back to the built-in default; surface that resolved
    // state on read instead of a bare `undefined` (mirrors Claude's model
    // null -> "default" formatting), so a `get model` on a fresh config is
    // self-explanatory rather than looking empty/broken.
    formatOnRead: (v: unknown) => (v === undefined || v === null ? 'default' : v),
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
    asyncValidateOnWrite: validateModelOnWrite,
  },
  compactionModel: {
    type: 'string',
    description: 'Model used for context compaction.',
    validateOnWrite: nonEmptyString('compactionModel'),
    asyncValidateOnWrite: validateModelOnWrite,
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

/**
 * Builds the `## Model` section appended to the tool description, enumerating the
 * known-good model ids per provider plus the short aliases, so the model has a
 * concrete list of valid values to pick when it wants to change the `model` key
 * (mirrors Claude's generateModelSection, which special-cases the model setting
 * with a per-value listing and a try/catch static fallback). The provider lister
 * is injected so a registry failure degrades to a static fallback line rather
 * than breaking the whole tool description. Uses the SYNCHRONOUS static registry
 * (listProviders) — never a network round-trip — because the description is built
 * once at module load.
 */
export function generateModelSection(
  listFn: () => Array<{ id: string; models: string[] }> = listProviders,
): string {
  try {
    const providers = listFn();
    const providerLines = providers
      .filter((p) => p.models.length > 0)
      .map((p) => `  - ${p.id}: ${p.models.join(', ')}`);
    const aliasLines = MODEL_ALIASES.map((a) => {
      const target = resolveModelAlias(a);
      return target ? `${a} -> ${target}` : a;
    });
    return (
      '\n\n## Model\n' +
      'Set the "model" key to a provider model id below, or one of these aliases, ' +
      'or "default" to use the built-in default.\n' +
      providerLines.join('\n') +
      `\nAliases: ${aliasLines.join(', ')}`
    );
  } catch {
    // Registry resolution failed — degrade to a static pointer at /model so the
    // tool description still renders and the model knows where to find the list.
    return (
      '\n\n## Model\n' +
      'Set the "model" key to any provider model id (run the /model command for the live list), ' +
      'or "default" to use the built-in default.'
    );
  }
}

function buildExamples(): string {
  return (
    '\n\nExamples:\n' +
    '  - Read all settings: action="get"\n' +
    '  - Read one setting: action="get" key="model"\n' +
    '  - Set a model: action="set" key="model" value="claude-opus-4-6"\n' +
    '  - Enable a boolean: action="set" key="safeMode" value="true"\n' +
    '  - Set a number: action="set" key="maxTurns" value="40"\n' +
    '  - Set in the global store: action="set" key="model" value="opus" scope="global"\n' +
    '  - Reset to default: action="set" key="model" value="default"'
  );
}

function buildDescription(): string {
  const lines = getAllSettingKeys().map((key) => {
    const c = SUPPORTED_SETTINGS[key];
    const opts = c.options ? ` [${c.options.join(' | ')}]` : c.type === 'boolean' ? ' [true | false]' : '';
    return `  - ${key} (${c.type})${opts}: ${c.description}`;
  });
  return (
    'Read or update AMI configuration. ' +
    'Use action="get" [key] to read, or action="set" key=<name> value=<value>. ' +
    'Omit value to read a setting; include value to write it. ' +
    'Set value to "default" to reset a key to its built-in default.\n' +
    '## Storage scope\n' +
    'scope="project" (default) reads/writes the project store .ami/config.json (this repo only); ' +
    'scope="global" reads/writes the user-global store ~/.ami/config.json (applies to every project). ' +
    'Choose "global" for machine-wide preferences, "project" for repo-specific settings.\n' +
    'Supported settings:\n' +
    lines.join('\n') +
    generateModelSection() +
    buildExamples()
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
        type: ['string', 'boolean', 'number'],
        description: 'Value to set (string, boolean, or number accepted; "default" resets the key). Omit to read.',
      },
      scope: {
        type: 'string',
        enum: ['project', 'global'],
        description: 'Storage scope: "project" (default, .ami/config.json in this repo) or "global" (~/.ami/config.json, all projects).',
      },
    },
    required: ['action'],
  },
  isReadOnly: false,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const action = input.action as string;

    // Resolve the storage target from the requested scope. 'global' routes to the
    // user-global store (~/.ami/config.json) that the config waterfall reads via
    // loadGlobalConfig; 'project' (default) routes to this repo's .ami/config.json.
    const scope = input.scope === 'global' ? 'global' : 'project';
    let configPath: string;
    if (scope === 'global') {
      const globalPath = getGlobalConfigPath();
      if (!globalPath) {
        return { output: 'Cannot resolve the global config path: no home directory (HOME/USERPROFILE) is set.', isError: true };
      }
      configPath = globalPath;
    } else {
      configPath = path.join(context.cwd, ...CONFIG_REL);
    }

    if (action === 'get') return handleGet(configPath, input.key as string | undefined, scope);
    if (action === 'set') return handleSet(configPath, input, scope, context);
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
  // Strip a leading UTF-8 BOM (e.g. config.json written by PowerShell 5.x or some
  // editors) before parsing so a BOM-prefixed but otherwise valid config is not
  // misreported as corrupt (JSON.parse rejects a leading U+FEFF).
  if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
  const stripped = stripJsoncComments(raw);
  const validation = validateConfigJson(stripped);
  if (!validation.valid) {
    return { error: validation.error };
  }
  return { config: JSON.parse(stripped) as Record<string, unknown> };
}

/**
 * Persist a (possibly nested) mutation to the config file through the guarded
 * functional writer {@link updateConfigFile}: the mutation is re-applied to a
 * FRESH re-read of the file right before the atomic write, so a concurrent writer
 * is not clobbered (lost-update guard) and a present-but-corrupt file is refused
 * rather than overwritten. Returns an error string on refusal / write failure.
 */
function writeConfigMutation(configPath: string, mutate: (cfg: Record<string, unknown>) => void): { error?: string } {
  try {
    const res = updateConfigFile(configPath, cur => {
      const copy = { ...cur } as Record<string, unknown>;
      mutate(copy);
      return copy as ProjectConfig;
    });
    if (res.reason === 'corrupt') {
      return { error: `Refusing to write: existing config is corrupt (${res.diagnostic?.message ?? 'parse error'}). Fix ${configPath} first.` };
    }
    return {};
  } catch (err) {
    return { error: `Error writing config: ${err instanceof Error ? err.message : String(err)}` };
  }
}

function handleGet(configPath: string, key: string | undefined, scope: 'project' | 'global'): ToolResult {
  const { config, error, absent } = readConfigRaw(configPath);
  if (absent) {
    const where = scope === 'global' ? 'global config (~/.ami/config.json)' : 'project config';
    return { output: `No ${where} found. Create it via action="set"${scope === 'global' ? ' scope="global"' : ''}.`, metadata: { operation: 'get', scope } };
  }
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
    return { output: `${key} = ${JSON.stringify(display)}`, metadata: { operation: 'get', setting: key, value: display, scope } };
  }
  return { output: JSON.stringify(config, null, 2), metadata: { operation: 'get', scope } };
}

async function handleSet(configPath: string, input: Record<string, unknown>, scope: 'project' | 'global', context: ToolContext): Promise<ToolResult> {
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
      return { output: `${key} is already at its default.`, metadata: { operation: 'set', setting: key, previousValue, newValue: undefined, scope } };
    }
    const resetWrite = writeConfigMutation(configPath, cfg => deleteValueByPath(cfg, settingPath));
    if (resetWrite.error) {
      return { output: resetWrite.error, isError: true };
    }
    return { output: `Reset ${key} to default.`, metadata: { operation: 'set', setting: key, previousValue, newValue: undefined, scope } };
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

  // No-op short-circuit when unchanged — do this BEFORE the (possibly I/O-bound)
  // async validator so re-setting the current value never triggers a provider
  // round-trip.
  if (previousValue === finalValue) {
    return { output: `${key} is already ${JSON.stringify(finalValue)} (no change).`, metadata: { operation: 'set', setting: key, previousValue, newValue: finalValue, scope } };
  }

  // Async per-setting write validation (e.g. the model id is actually served by
  // the configured provider). Runs last, immediately before persisting, so a
  // semantically-invalid value is rejected instead of committed and failing later.
  if (settingConfig.asyncValidateOnWrite) {
    const asyncResult = await settingConfig.asyncValidateOnWrite(finalValue, context);
    if (!asyncResult.valid) {
      return { output: asyncResult.error ?? `Invalid value for ${key}.`, isError: true };
    }
  }

  const setWrite = writeConfigMutation(configPath, cfg => setValueByPath(cfg, settingPath, finalValue));
  if (setWrite.error) {
    return { output: setWrite.error, isError: true };
  }
  return {
    output: `Set ${key} = ${JSON.stringify(finalValue)}${previousValue !== undefined ? ` (was ${JSON.stringify(previousValue)})` : ''}`,
    metadata: { operation: 'set', setting: key, previousValue, newValue: finalValue, scope },
  };
}
