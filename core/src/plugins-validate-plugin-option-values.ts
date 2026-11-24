// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { parseFrontmatter } from './memory';
import { SKILL_MD_RE } from './plugins-derive-command-description';
import { PLUGIN_OPTIONS_FILE } from './plugins-plugin-user-config-option';
import type { PluginOptionSchema, PluginOptionValues, PluginValidationIssue } from './plugins-plugin-user-config-option';

/**
 * Read a plugin's saved `userConfig` VALUES sidecar at `<dir>/.options.json`,
 * keeping only primitive values (string/number/boolean). Returns `{}` when the
 * file is absent, is not a regular file, is unreadable, or contains invalid JSON.
 *
 * Reads THROUGH a regular-file guard (`statSync().isFile()` before opening) so a
 * hostile bundle that plants a FIFO, directory, socket, or device at the sidecar
 * path can neither block the reader on a pipe nor smuggle a non-object. Canonical
 * reader (the reference parity: loadPluginOptions / pluginOptionsStorage) — the plugin
 * installer and the hook-env plugin-option injection both call this; never
 * re-implement the parse/guard per site.
 */
export function readPluginOptionValues(dir: string): Record<string, string | number | boolean> {
  const file = path.join(dir, PLUGIN_OPTIONS_FILE);
  let st: fs.Stats;
  try {
    // statSync resolves symlinks but does NOT open the node, so it never blocks on
    // a FIFO the way readFileSync would — the whole point of the guard.
    st = fs.statSync(file);
  } catch {
    return {}; // absent / broken symlink / inaccessible
  }
  if (!st.isFile()) return {}; // directory, FIFO, socket, char/block device — never read
  let content: string;
  try {
    content = fs.readFileSync(file, 'utf-8');
  } catch {
    return {};
  }
  try {
    const raw = JSON.parse(content) as unknown;
    if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
      const out: Record<string, string | number | boolean> = {};
      for (const [k, v] of Object.entries(raw as Record<string, unknown>)) {
        if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') out[k] = v;
      }
      return out;
    }
  } catch {
    // corrupt JSON → {} (same fail-open contract as an absent sidecar)
  }
  return {};
}

/**
 * Substitute `${user_config.<key>}` placeholders in a plugin command-hook string
 * (command or an arg) with the plugin's saved `userConfig` values. An unknown or
 * absent key resolves to the empty string so a stale placeholder never leaks the
 * literal `${user_config.x}` onto a command line (the reference parity:
 * substituteUserConfigVariables). Returns the string unchanged when there are no
 * values. Only the `user_config.` namespace is touched — every other `${...}`
 * reference (CLAUDE_PLUGIN_ROOT/DATA, shell env) is left for its own substitution
 * tier, so this composes with {@link substitutePluginRoot}/{@link substitutePluginData}.
 */
export function substituteUserConfigValues(
  text: string,
  values: Record<string, string | number | boolean> | undefined,
): string {
  if (!text || !values) return text;
  return text.replace(/\$\{user_config\.([A-Za-z0-9_]+)\}/g, (_match, key: string) => {
    const v = values[key];
    return v === undefined ? '' : String(v);
  });
}

/**
 * Build the `CLAUDE_PLUGIN_OPTION_<KEY>` (plus the AMI-native `AMI_PLUGIN_OPTION_<KEY>`
 * alias) environment variables that expose a plugin's saved `userConfig` values to
 * its command hooks, so a hook can read configuration from the environment without a
 * `${user_config.X}` placeholder (the reference parity: CLAUDE_PLUGIN_OPTION_<KEY>, utils/hooks.ts).
 * Each key is upper-cased with non-identifier characters replaced by `_` so the value
 * is always shell-referenceable (`$CLAUDE_PLUGIN_OPTION_API_KEY`). Values are stringified.
 * Returns `{}` when there are no values. Sensitive values ARE included — a command hook
 * runs the user's own code, the same trust boundary as the hook reading the sidecar directly.
 */
export function buildPluginOptionEnv(
  values: Record<string, string | number | boolean> | undefined,
): Record<string, string> {
  const env: Record<string, string> = {};
  if (!values) return env;
  for (const [key, value] of Object.entries(values)) {
    const envKey = key.replace(/[^A-Za-z0-9_]/g, '_').toUpperCase();
    const str = String(value);
    env[`CLAUDE_PLUGIN_OPTION_${envKey}`] = str;
    env[`AMI_PLUGIN_OPTION_${envKey}`] = str;
  }
  return env;
}

/**
 * Compute the per-field initial string the config dialog should pre-fill when
 * RECONFIGURING (the reference parity: PluginOptionsDialog's `initialFor`). A
 * `sensitive` field is NEVER pre-filled (the stored secret is not echoed into
 * the input buffer); any other field is pre-filled with the stringified saved
 * value, or `''` when nothing is saved. The returned map is keyed by every
 * option in `schema`.
 */
export function buildPluginOptionPrefill(
  schema: PluginOptionSchema,
  initialValues?: PluginOptionValues,
): Record<string, string> {
  const prefill: Record<string, string> = {};
  for (const key of Object.keys(schema)) {
    if (schema[key]?.sensitive === true) {
      prefill[key] = '';
      continue;
    }
    const v = initialValues?.[key];
    prefill[key] = v === undefined ? '' : String(v);
  }
  return prefill;
}

/**
 * Validate resolved option VALUES against a declared schema (the reference parity: the
 * required/type checks `validateUserConfig` applies to saved values). Reports a
 * human-readable error per offending field:
 *   - a `required` option that is absent / blank / an empty array;
 *   - a `number` option whose value is not a finite number (or is out of a
 *     declared `min`/`max` range);
 *   - a `multiple` string option whose value is not a string array;
 *   - a non-`multiple` scalar option given an array.
 * An absent OPTIONAL option is fine. Returns `{ valid, errors }`.
 */
export function validatePluginOptionValues(
  values: PluginOptionValues,
  schema: PluginOptionSchema,
): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  for (const [key, opt] of Object.entries(schema)) {
    const value = values[key];
    const label = opt.title && opt.title.trim() !== '' ? opt.title : key;
    const absent =
      value === undefined ||
      (typeof value === 'string' && value.trim() === '') ||
      (Array.isArray(value) && value.length === 0);
    if (absent) {
      if (opt.required === true) errors.push(`Missing required value for "${label}".`);
      continue;
    }
    if (opt.multiple === true) {
      if (!Array.isArray(value) || !value.every(v => typeof v === 'string')) {
        errors.push(`"${label}" must be a list of strings.`);
      }
      continue;
    }
    if (Array.isArray(value)) {
      errors.push(`"${label}" must be a single value, not a list.`);
      continue;
    }
    if (opt.type === 'number') {
      const num = typeof value === 'number' ? value : Number(value);
      if (!Number.isFinite(num)) {
        errors.push(`"${label}" must be a number.`);
      } else if (opt.min !== undefined && num < opt.min) {
        errors.push(`"${label}" must be at least ${opt.min}.`);
      } else if (opt.max !== undefined && num > opt.max) {
        errors.push(`"${label}" must be at most ${opt.max}.`);
      }
    }
  }
  return { valid: errors.length === 0, errors };
}

/**
 * Return the subset of a plugin's option schema whose SAVED values are still
 * missing/invalid (the reference parity: pluginOptionsStorage `getUnconfiguredOptions`).
 * Used to decide, after enabling a plugin, whether the user must be prompted —
 * already-configured fields are skipped so a reconfigure never re-asks for a
 * value that already validates. Returns an empty schema when everything is
 * configured.
 */
export function getUnconfiguredPluginOptions(
  schema: PluginOptionSchema,
  saved: PluginOptionValues,
): PluginOptionSchema {
  if (Object.keys(schema).length === 0) return {};
  if (validatePluginOptionValues(saved, schema).valid) return {};
  const unconfigured: PluginOptionSchema = {};
  for (const [key, opt] of Object.entries(schema)) {
    const single = validatePluginOptionValues(
      key in saved ? { [key]: saved[key] } : {},
      { [key]: opt },
    );
    if (!single.valid) unconfigured[key] = opt;
  }
  return unconfigured;
}

/** The action `type` a plugin hook entry may declare (the reference parity: HookConfigType
 *  — mirrors core/core/src/hooks.ts HOOK_CONFIG_TYPES without importing the heavy
 *  HookManager graph into this building-block module). */
export const PLUGIN_HOOK_ACTION_TYPES = ['command', 'prompt', 'http', 'agent'] as const;

/**
 * Structurally validate one hook entry, recursing into the reference matcher form
 * `{ matcher?, hooks: [ { type, command } ] }`. A declared `type` must be a known
 * action type and a `command`-type action must carry a non-empty `command` string
 * (the security hardening of the command itself runs at hook LOAD time via
 * `validateHookCommand`; this is the author-facing STRUCTURAL lint).
 */
export function validateHookAction(obj: unknown, label: string, errors: PluginValidationIssue[]): void {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    errors.push({ path: label, message: 'Hook entry must be an object.' });
    return;
  }
  const o = obj as Record<string, unknown>;
  if (Array.isArray(o.hooks)) {
    o.hooks.forEach((h, i) => validateHookAction(h, `${label}.hooks[${i}]`, errors));
    return;
  }
  if (o.type !== undefined && (typeof o.type !== 'string' || !(PLUGIN_HOOK_ACTION_TYPES as readonly string[]).includes(o.type))) {
    errors.push({ path: `${label}.type`, message: `Hook "type" must be one of: ${PLUGIN_HOOK_ACTION_TYPES.join(', ')}.` });
  }
  if (o.type === 'command' && (typeof o.command !== 'string' || o.command.trim() === '')) {
    errors.push({ path: `${label}.command`, message: 'A "command" hook must declare a non-empty "command" string.' });
  }
}

/**
 * Validate the STRUCTURE of a parsed plugin `hooks.json` (the reference parity:
 * PluginHooksSchema, utils/plugins/validatePlugin.ts L640-711). the reference's loader
 * HARD-parses hooks.json (`.parse()`, not `.safeParse()`) so a malformed file breaks
 * the whole plugin load — surfacing the structural errors here (instead of only
 * catching a JSON syntax error) lets an author fix it before it ships. Accepts both
 * the AMI flat-array form (`[ { event, type, command } ]`) and the reference map-keyed
 * form (`{ EventName: [ { matcher?, hooks: [...] } ] }`, optionally wrapped in a top
 * `hooks` key). Event-NAME membership is intentionally NOT checked — AMI translates
 * the reference's PascalCase event names via normalizeMapKeyedHooks, so a cross-framework
 * manifest must validate. Returns the issues (empty when structurally sound).
 */
export function validatePluginHooks(parsed: unknown): PluginValidationIssue[] {
  const errors: PluginValidationIssue[] = [];
  if (parsed === null || typeof parsed !== 'object') {
    errors.push({ path: 'hooks', message: 'hooks.json must be a JSON object (event → matchers) or an array of hook entries.' });
    return errors;
  }
  if (Array.isArray(parsed)) {
    parsed.forEach((entry, i) => validateHookAction(entry, `hooks[${i}]`, errors));
    return errors;
  }
  const raw = parsed as Record<string, unknown>;
  // the reference map-keyed form may wrap the event map in a top-level `hooks` key.
  const map = raw.hooks && typeof raw.hooks === 'object' && !Array.isArray(raw.hooks)
    ? (raw.hooks as Record<string, unknown>)
    : raw;
  for (const [event, matchers] of Object.entries(map)) {
    if (!Array.isArray(matchers)) {
      errors.push({ path: `hooks.${event}`, message: `Hooks for "${event}" must be an array of hook matchers.` });
      continue;
    }
    matchers.forEach((m, i) => validateHookAction(m, `hooks.${event}[${i}]`, errors));
  }
  return errors;
}

/** readdir that returns [] instead of throwing on a missing/unreadable dir. */
export function safeReaddir(dir: string): string[] {
  try { return fs.readdirSync(dir); } catch { return []; }
}

/** Validate a markdown component file has YAML frontmatter with a name (skills)
 *  or description (agents) and a non-empty body. */
export function validateMarkdownComponent(
  file: string,
  label: string,
  isSkill: boolean,
  errors: PluginValidationIssue[],
  warnings: PluginValidationIssue[],
): void {
  let raw: string;
  try { raw = fs.readFileSync(file, 'utf-8'); } catch (err) {
    errors.push({ path: label, message: `Failed to read: ${err instanceof Error ? err.message : String(err)}` });
    return;
  }
  const { frontmatter, body } = parseFrontmatter(raw);
  if (!body.trim()) {
    errors.push({ path: label, message: 'component body is empty.' });
  }
  if (isSkill) {
    // A subdir SKILL.md can fall back to the dir name, so a missing name is only a
    // warning (SKILL.md matched case-insensitively, the reference parity).
    if (!frontmatter.name && !SKILL_MD_RE.test(path.basename(file))) {
      warnings.push({ path: label, message: 'skill has no "name" frontmatter; the file name will be used.' });
    }
  } else if (!frontmatter.description?.trim()) {
    warnings.push({ path: label, message: 'agent has no "description" frontmatter.' });
  }
}

/**
 * Locate a plugin's `plugin.json` under `root`, in priority order: AMI's
 * `.ami-plugin/`, then the reference's `.claude-plugin/`, then a legacy root-level
 * `plugin.json` (the reference parity: pluginLoader's `legacyManifestPath`). Returns the
 * first existing path, or undefined when the directory ships no manifest. SINGLE
 * source for the lookup order used by both discovery and validation.
 */
export function resolveManifestPath(root: string): string | undefined {
  const candidates = [
    path.join(root, '.ami-plugin', 'plugin.json'),
    path.join(root, '.claude-plugin', 'plugin.json'),
    path.join(root, 'plugin.json'),
  ];
  return candidates.find(p => fs.existsSync(p));
}

/**
 * The kinds of component a plugin can contribute (the reference parity: PluginComponent,
 * types/plugin.ts). Used to tag a {@link PluginError} with the component it concerns.
 */
export const PLUGIN_COMPONENTS = ['commands', 'agents', 'skills', 'hooks', 'output-styles'] as const;

export type PluginComponent = (typeof PLUGIN_COMPONENTS)[number];

/**
 * Typed, discriminated plugin error (the reference parity: types/plugin.ts `PluginError`).
 * Each variant carries the CONTEXTUAL data its kind needs so a consumer can render
 * precise, actionable guidance WITHOUT string-matching an opaque message (which
 * breaks the instant the wording changes). Every variant carries `source` (the
 * plugin/marketplace/path the error came from) and most carry `plugin` (the bundle
 * name). This is the type-safe replacement for the previous free-form string reason.
 */
export type PluginError =
  | { type: 'manifest-parse-error'; source: string; plugin?: string; manifestPath: string; parseError: string }
  | { type: 'manifest-validation-error'; source: string; plugin?: string; manifestPath: string; validationErrors: string[] }
  | { type: 'path-not-found'; source: string; plugin?: string; path: string; component: PluginComponent }
  | { type: 'component-load-failed'; source: string; plugin: string; component: PluginComponent; path: string; reason: string }
  | { type: 'lsp-config-invalid'; source: string; plugin: string; serverName: string; validationError: string }
  | { type: 'mcp-config-invalid'; source: string; plugin: string; serverName: string; validationError: string }
  | { type: 'mcp-server-suppressed-duplicate'; source: string; plugin: string; serverName: string; duplicateOf: string }
  | { type: 'hook-load-failed'; source: string; plugin: string; hookPath: string; reason: string }
  | { type: 'path-traversal-blocked'; source: string; plugin?: string; declaredPath: string; component: PluginComponent }
  | { type: 'generic-error'; source: string; plugin?: string; error: string };

/**
 * Render a {@link PluginError} to a single display string (the reference parity:
 * getPluginErrorMessage). Exhaustive over the union — adding a variant without a
 * case is a compile error — so the message set can never drift out of sync with the
 * type. The SINGLE canonical formatter for every plugin-error surface (installer,
 * `/plugin`, diagnostics).
 */
export function getPluginErrorMessage(error: PluginError): string {
  switch (error.type) {
    case 'manifest-parse-error':
      return `Manifest parse error: ${error.parseError}`;
    case 'manifest-validation-error':
      return `Manifest validation failed: ${error.validationErrors.join(', ')}`;
    case 'path-not-found':
      return `Path not found: ${error.path} (${error.component})`;
    case 'component-load-failed':
      return `${error.component} load failed from ${error.path}: ${error.reason}`;
    case 'lsp-config-invalid':
      return `Plugin "${error.plugin}" has invalid LSP server config for "${error.serverName}": ${error.validationError}`;
    case 'mcp-config-invalid':
      return `Plugin "${error.plugin}" has invalid MCP server config for "${error.serverName}": ${error.validationError}`;
    case 'mcp-server-suppressed-duplicate': {
      const dup = error.duplicateOf.startsWith('plugin:')
        ? `server provided by plugin "${error.duplicateOf.slice('plugin:'.length)}"`
        : `already-configured "${error.duplicateOf}"`;
      return `MCP server "${error.serverName}" skipped — same command/URL as ${dup}`;
    }
    case 'hook-load-failed':
      return `Hook load failed: ${error.reason}`;
    case 'path-traversal-blocked':
      return formatPluginPathTraversalWarning(error.plugin ?? error.source, error.declaredPath, error.component);
    case 'generic-error':
      return error.error;
  }
}

/**
 * The user-facing warning for a plugin-contributed path that escapes its own
 * root (the reference parity: lspPluginIntegration's "Security: Path traversal attempt
 * blocked in plugin …" message). Emitted whenever {@link hardenedResolve} rejects
 * a declared component path because its realpath leaves the plugin dir (a `..`
 * segment that survived manifest validation, or a symlink pointing outside) —
 * surfaced instead of being silently dropped so an author/operator can see the
 * containment violation.
 */
export function formatPluginPathTraversalWarning(plugin: string, declaredPath: string, component?: PluginComponent): string {
  const where = component ? ` (${component})` : '';
  return `Security: path traversal blocked in plugin "${plugin}": "${declaredPath}"${where} escapes the plugin directory and was not loaded.`;
}

/**
 * Stable dedup key for a {@link PluginError} (the reference parity: refresh.ts
 * `errorKey`). A `generic-error` is keyed by its source AND message (two generic
 * errors from the same source but different messages are distinct); every typed
 * variant is keyed by `type:source` (the same kind from the same source is one
 * error regardless of incidental detail).
 */
export function pluginErrorKey(e: PluginError): string {
  return e.type === 'generic-error'
    ? `generic-error:${e.source}:${e.error}`
    : `${e.type}:${e.source}`;
}

/**
 * Merge a freshly-recomputed plugin-error set into the existing one on reload,
 * preserving errors that a plain reload would otherwise strand (the reference parity:
 * refresh.ts `mergePluginErrors`). Errors sourced from the LSP manager
 * (`lsp-manager`) or from an individual plugin (`plugin:<name>`) are PRESERVED
 * across the reload — they are recomputed asynchronously/elsewhere, not by the
 * reload that produced `fresh`, so dropping them would lose live diagnostics.
 * A preserved error is dropped only when `fresh` already carries one with the
 * same {@link pluginErrorKey} (the fresh copy wins). All other existing errors
 * are replaced by `fresh`.
 */
export function mergePluginErrors(existing: PluginError[], fresh: PluginError[]): PluginError[] {
  const preserved = existing.filter(
    e => e.source === 'lsp-manager' || e.source.startsWith('plugin:'),
  );
  const freshKeys = new Set(fresh.map(pluginErrorKey));
  const deduped = preserved.filter(e => !freshKeys.has(pluginErrorKey(e)));
  return [...deduped, ...fresh];
}
