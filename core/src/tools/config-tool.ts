import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { updateConfigFile, validateConfigJson, validateConfigFileContent, suggestKnownConfigKey, stripJsoncComments, getGlobalConfigPath, type ProjectConfig } from '../config';
import { THINKING_LEVELS } from '../model-capabilities';
import '../settings-guard';
import '../model-registry';
import { isSpecialFileStat } from '../path-safety';
import { CONFIG_REL, buildExamples, coerceValue, configToolWriteBlockReason, deleteValueByPath, generateModelSection, getValueByPath, nonEmptyString, positiveNumber, setValueByPath, validateModelOnWrite } from './config-tool-generate-model-section';
import type { SettingConfig } from './config-tool-generate-model-section';
export { configToolWriteBlockReason, generateModelSection } from './config-tool-generate-model-section';
export type { SettingConfig } from './config-tool-generate-model-section';

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
    // state on read instead of a bare `undefined` (mirrors the reference's model
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
  toolOutputMaxChars: {
    type: 'number',
    description: 'Global default cap (chars) on a tool result (web_fetch, PDF, …) before spill; AMI_TOOL_OUTPUT_MAX_CHARS overrides.',
    validateOnWrite: positiveNumber('toolOutputMaxChars'),
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

function buildDescription(): string {
  const lines = getAllSettingKeys().map((key) => {
    const c = SUPPORTED_SETTINGS[key];
    const opts = c.options ? ` [${c.options.join(' | ')}]` : c.type === 'boolean' ? ' [true | false]' : '';
    return `  - ${key} (${c.type})${opts}: ${c.description}`;
  });
  return (
    'Read or update AMI configuration. ' +
    'Use action="get" [key] to read, action="set" key=<name> value=<value> to write, ' +
    'or action="validate" to check the config file against the schema (lists every bad value/unknown key + valid keys). ' +
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

export const configTool: ToolDefinition = {
  name: 'config',
  description: buildDescription(),
  inputSchema: {
    type: 'object',
    properties: {
      action: { type: 'string', enum: ['get', 'set', 'validate'], description: 'Get current config, set a value, or validate the config file against the schema.' },
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
  // Dynamic read-vs-write classification (parity: the reference's ConfigTool.isReadOnly =
  // `value === undefined`). A config call only mutates on-disk config when it is a
  // `set` that carries a value (a plain `get`, or a `set` with no value — which is a
  // no-op error — reads nothing on disk). Reporting the read case as read-only lets
  // the permission layer auto-allow config reads without a prompt while keeping every
  // actual write on the permission-asking path, matching the reference's auto-allow-reads /
  // ask-on-set affordance exactly. See core/tools/tool-categories.isToolReadOnlyForInput.
  isReadOnlyForInput: (input: Record<string, unknown>): boolean =>
    !(input.action === 'set' && input.value !== undefined),
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
    if (action === 'validate') return handleValidate(configPath, scope);
    return { output: 'Usage: action="get" [key], action="set" key=... value=..., or action="validate".', isError: true };
  },
};

function readConfigRaw(configPath: string): { config?: Record<string, unknown>; error?: string; absent?: boolean; unreadable?: string } {
  // Special-file containment before the read (parity: the reference's safeResolvePath
  // special-file check before a settings read). A config path must be a regular
  // file: `fs.readFileSync` on a FIFO blocks indefinitely waiting for a writer
  // (a hang that looks like a frozen tool), and a socket/char/block device is
  // never a valid config source. `statSync` follows symlinks, so a symlink that
  // ultimately points AT a special file is caught too. A missing/dangling path
  // throws here and is treated as absent (the read below reports it normally).
  try {
    if (isSpecialFileStat(fs.statSync(configPath))) {
      return { unreadable: `Config path ${configPath} is not a regular file (named pipe/socket/device); refusing to read it.` };
    }
  } catch {
    // Does not exist / inaccessible — fall through; the read below classifies it.
  }
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

/**
 * Validate the on-disk config file against the FULL schema and report, structured,
 * every parse/field/unknown-key problem plus the list of valid keys so the model can
 * self-correct the file. Parity: the reference's schema-aware settings validation that
 * returns the per-field error list AND the schema. Read-only. Uses the single canonical
 * {@link validateConfigFileContent} — no parallel validation logic.
 */
function handleValidate(configPath: string, scope: 'project' | 'global'): ToolResult {
  let raw: string;
  try {
    if (isSpecialFileStat(fs.statSync(configPath))) {
      return { output: `Config path ${configPath} is not a regular file; refusing to read it.`, isError: true };
    }
  } catch {
    // Absent / inaccessible — classified by the read below.
  }
  try {
    raw = fs.readFileSync(configPath, 'utf-8');
  } catch {
    const where = scope === 'global' ? 'global config (~/.ami/config.json)' : 'project config (.ami/config.json)';
    return { output: `No ${where} to validate (nothing written yet).`, metadata: { operation: 'validate', scope, valid: true } };
  }
  if (raw.charCodeAt(0) === 0xfeff) raw = raw.slice(1);
  const result = validateConfigFileContent(raw);
  if (result.valid) {
    return { output: `Config at ${configPath} is valid.`, metadata: { operation: 'validate', scope, valid: true } };
  }
  const parts: string[] = [`Config at ${configPath} is invalid:`];
  if (result.parseError) parts.push(`- ${result.parseError}`);
  for (const issue of result.issues) parts.push(`- ${issue.field}: ${issue.message}`);
  for (const k of result.unknownKeys) {
    const suggestion = suggestKnownConfigKey(k);
    parts.push(`- unknown key "${k}"${suggestion ? ` (did you mean "${suggestion}"?)` : ''}`);
  }
  parts.push(`Valid keys: ${Object.keys(result.schema.properties).sort().join(', ')}`);
  return {
    output: parts.join('\n'),
    isError: true,
    metadata: { operation: 'validate', scope, valid: false, issues: result.issues, unknownKeys: result.unknownKeys },
  };
}

function handleGet(configPath: string, key: string | undefined, scope: 'project' | 'global'): ToolResult {
  const { config, error, absent, unreadable } = readConfigRaw(configPath);
  if (unreadable) {
    return { output: unreadable, isError: true };
  }
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
  const { config: existing, error: readError, absent, unreadable } = readConfigRaw(configPath);
  if (unreadable) {
    return { output: `Refusing to write: ${unreadable}`, isError: true };
  }
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
