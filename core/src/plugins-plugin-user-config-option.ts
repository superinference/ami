// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';

/**
 * One message-channel declaration from a plugin manifest (the reference parity:
 * PluginManifestChannelsSchema entry). `server` names the MCP server this channel
 * binds to (must match a key in the plugin's {@link PluginBundle.mcpServers});
 * `displayName` is the human-readable name shown in the config dialog (defaults to
 * the server name); `userConfig` declares the fields to prompt for when enabling
 * the plugin, saved values being substituted into the server's env.
 */
export interface PluginChannel {
  server: string;
  displayName?: string;
  userConfig?: PluginOptionSchema;
}

/**
 * The settings sources that can OWN an enabled plugin, lowest precedence first
 * (the reference parity: pluginStartupCheck's `scopeSources`). `managed` (policy) is
 * not user-editable; `flag` is an `--add-dir`/session-only enablement with no
 * write-back. A later source overrides an earlier one for the same plugin, so
 * the resolved owner is the HIGHEST-precedence source that enables it.
 */
export const PLUGIN_EDITABLE_SCOPES = ['managed', 'user', 'project', 'local', 'flag'] as const;

export type ExtendedPluginScope = (typeof PLUGIN_EDITABLE_SCOPES)[number];

/** A scope whose enablement is persisted (everything except the session-only
 *  `flag` scope) — the reference parity: PersistablePluginScope. */
export type PersistablePluginScope = Exclude<ExtendedPluginScope, 'flag'>;

/**
 * Whether an owning scope's enablement should be written back to disk (the reference
 * parity: pluginStartupCheck `isPersistableScope`). The `flag` scope is
 * session-only (`--add-dir`), so it is NOT persistable; every other scope is.
 */
export function isPersistablePluginScope(scope: ExtendedPluginScope): scope is PersistablePluginScope {
  return scope !== 'flag';
}

/** One settings source's `enabledPlugins` map, in precedence order lowest→highest
 *  (the reference parity: the policy>local>project>user>flag resolution, expressed here
 *  as the write-back-ownership order managed<user<project<local<flag). A value of
 *  `true` enables the plugin at that scope, `false` disables it. */
export interface PluginScopeSource {
  scope: ExtendedPluginScope;
  enabledPlugins: Record<string, boolean>;
}

/**
 * Resolve the user-editable scope that OWNS each enabled plugin — i.e. where a
 * toggle should be written back (the reference parity: pluginStartupCheck
 * `getPluginEditableScopes`). Sources are applied in `PluginScopeSource` order
 * (lowest precedence first); for each plugin, a `true` sets the owning scope to
 * that source and a `false` removes it, so the final owner is the highest
 * source that enables it with no higher source disabling it. Only well-formed
 * plugin ids (containing `@`, i.e. `name@marketplace`) are considered. Returns a
 * Map of plugin id → owning scope.
 */
export function resolvePluginEditableScopes(sources: PluginScopeSource[]): Map<string, ExtendedPluginScope> {
  const result = new Map<string, ExtendedPluginScope>();
  for (const { scope, enabledPlugins } of sources) {
    for (const [pluginId, value] of Object.entries(enabledPlugins)) {
      if (!pluginId.includes('@')) continue; // skip invalid id format
      if (value === true) result.set(pluginId, scope);
      else if (value === false) result.delete(pluginId);
    }
  }
  return result;
}

export interface PluginValidationIssue {
  path: string;
  message: string;
}

export interface PluginValidationResult {
  valid: boolean;
  errors: PluginValidationIssue[];
  warnings: PluginValidationIssue[];
}

/** Fields that belong in a marketplace entry, not plugin.json — author confusion. */
export const MARKETPLACE_ONLY_MANIFEST_FIELDS = ['category', 'source', 'tags', 'strict', 'id'];

/** Optional component-path fields a manifest may declare. The single-path forms
 *  (skills/agents/commands/hooks/outputStyles) plus the multi-path `agentsPaths`/
 *  `outputStylesPaths` list forms (the reference parity: loadPluginAgents /
 *  loadPluginOutputStyles) are all traversal-validated. The server-declaration
 *  fields (mcpServers/lspServers) accept inline-record forms too and are validated
 *  separately via {@link validateServerDeclarations}. */
export const COMPONENT_PATH_FIELDS = ['skills', 'agents', 'commands', 'agentsPaths', 'outputStyles', 'outputStylesPaths'];

/** Manifest fields that declare servers either by FILE PATH (traversal-checked) or
 *  by INLINE RECORD (structurally opaque here): mcpServers (the reference parity:
 *  PluginManifestMcpServerSchema) and lspServers (PluginManifestLspServerSchema). */
export const SERVER_DECLARATION_FIELDS = ['mcpServers', 'lspServers'];

/**
 * Flag a single component-path string that would escape the plugin root — an
 * absolute path, or one whose segments include `..`. `label` names the offending
 * field (including an array index / record key for the richer forms).
 */
export function validateComponentPathString(label: string, val: string, errors: PluginValidationIssue[]): void {
  if (path.isAbsolute(val)) {
    errors.push({ path: label, message: `Component path "${val}" must be relative to the plugin root.` });
  } else if (val.split(/[/\\]/).includes('..')) {
    errors.push({ path: label, message: `Component path "${val}" contains ".." and would escape the plugin directory.` });
  }
}

/**
 * Validate a manifest component-path field (skills/agents/commands/hooks/mcpServers)
 * across ALL the forms the reference accepts (schemas.ts L429-497): a single relative-path
 * string, an array of relative-path strings, or (commands) an object mapping a
 * command name → { source, ... } metadata. Every path is traversal-checked via
 * {@link validateComponentPathString}; a non-string array entry is flagged. AMI
 * previously validated only the string form, so an array/record entry with `..` or
 * an absolute path slipped past the traversal gate — this closes that hole.
 */
export function validateComponentPathField(field: string, val: unknown, errors: PluginValidationIssue[]): void {
  if (typeof val === 'string') {
    validateComponentPathString(field, val, errors);
  } else if (Array.isArray(val)) {
    val.forEach((el, i) => {
      if (typeof el !== 'string') {
        errors.push({ path: `${field}[${i}]`, message: `Component path entry must be a string (got ${Array.isArray(el) ? 'array' : typeof el}).` });
      } else {
        validateComponentPathString(`${field}[${i}]`, el, errors);
      }
    });
  } else if (val && typeof val === 'object') {
    // Object-mapping form (the reference parity: commands record name → { source, ... }).
    // Each entry's `source` is a relative command-file path; validate it when present.
    for (const [key, meta] of Object.entries(val as Record<string, unknown>)) {
      if (meta && typeof meta === 'object' && !Array.isArray(meta)) {
        const src = (meta as Record<string, unknown>).source;
        if (typeof src === 'string') validateComponentPathString(`${field}.${key}.source`, src, errors);
      }
    }
  }
}

/**
 * Validate a manifest's optional `requires` availability gate (AMI's declarative
 * analogue of the reference's BuiltinPluginDefinition.isAvailable). Each of `platform`/
 * `env`/`bin`, when present, must be an array of strings; a malformed constraint is
 * flagged with a corrective example so an author can self-correct.
 */
export function validatePluginRequirements(val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: 'requires', message: 'Plugin "requires" must be an object with optional "platform"/"env"/"bin" string arrays.' });
    return;
  }
  const r = val as Record<string, unknown>;
  for (const key of ['platform', 'env', 'bin'] as const) {
    const v = r[key];
    if (v !== undefined && (!Array.isArray(v) || !v.every(x => typeof x === 'string'))) {
      errors.push({ path: `requires.${key}`, message: `Plugin "requires.${key}" must be an array of strings (e.g., ["linux", "darwin"] for platform).` });
    }
  }
}

/**
 * Validate a manifest's optional `settings` object (the reference parity:
 * PluginSettingsSchema — only `agent` is allowlisted, everything else is stripped).
 * `settings` must be an object; a present `agent` must be a string. Non-allowlisted
 * keys are surfaced as warnings (they are silently dropped at load time) so an
 * author learns the plugin cannot set them.
 */
export function validatePluginSettings(val: unknown, errors: PluginValidationIssue[], warnings: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: 'settings', message: 'Plugin "settings" must be an object (only the allowlisted "agent" key is honored).' });
    return;
  }
  const s = val as Record<string, unknown>;
  if (s.agent !== undefined && typeof s.agent !== 'string') {
    errors.push({ path: 'settings.agent', message: 'Plugin "settings.agent" must be a string (the default agent name).' });
  }
  for (const key of Object.keys(s)) {
    if (key !== 'agent') {
      warnings.push({ path: `settings.${key}`, message: `Plugin "settings.${key}" is not an allowlisted plugin setting and is ignored (only "agent" is honored).` });
    }
  }
}

/**
 * Validate a manifest's structured `author` (the reference parity: PluginAuthorSchema).
 * Accepts either a non-empty display-name string or an object `{ name, email?,
 * url? }` whose `name` is a non-empty string and whose optional `email`/`url` are
 * strings. Pushes labelled, actionable errors for every other shape.
 */
export function validatePluginAuthor(author: unknown, errors: PluginValidationIssue[]): void {
  if (author === undefined) return;
  if (typeof author === 'string') {
    if (author.trim() === '') {
      errors.push({ path: 'author', message: 'Plugin "author" cannot be an empty string.' });
    }
    return;
  }
  if (typeof author !== 'object' || author === null || Array.isArray(author)) {
    errors.push({ path: 'author', message: 'Plugin "author" must be a string or an object with a "name" (and optional "email"/"url").' });
    return;
  }
  const a = author as Record<string, unknown>;
  if (typeof a.name !== 'string' || a.name.trim() === '') {
    errors.push({ path: 'author.name', message: 'Author "name" cannot be empty.' });
  }
  if (a.email !== undefined && typeof a.email !== 'string') {
    errors.push({ path: 'author.email', message: 'Author "email" must be a string.' });
  }
  if (a.url !== undefined && typeof a.url !== 'string') {
    errors.push({ path: 'author.url', message: 'Author "url" must be a string.' });
  }
}

/**
 * Validate a manifest's optional `dependencies` — the plugins that must be enabled
 * first (the reference parity: DependencyRefSchema, utils/plugins/schemas.ts L1367). Each
 * entry is either a non-empty ref string (`name` or `name@marketplace`) or an
 * object carrying a non-empty string `name` (AMI also accepts `plugin` as the ref
 * key, matching its resolver) with an optional string `marketplace`. The common
 * dependency resolver already CONSUMES these; this adds the author-facing VALIDATION
 * so a malformed entry surfaces instead of being silently dropped.
 */
export function validateManifestDependencies(deps: unknown, errors: PluginValidationIssue[]): void {
  if (deps === undefined) return;
  if (!Array.isArray(deps)) {
    errors.push({ path: 'dependencies', message: 'Plugin "dependencies" must be an array of plugin refs (a "name"/"name@marketplace" string or a { name, marketplace? } object).' });
    return;
  }
  deps.forEach((dep, i) => {
    if (typeof dep === 'string') {
      if (dep.trim() === '') {
        errors.push({ path: `dependencies[${i}]`, message: 'Dependency ref cannot be an empty string.' });
      }
      return;
    }
    if (!dep || typeof dep !== 'object' || Array.isArray(dep)) {
      errors.push({ path: `dependencies[${i}]`, message: 'Dependency must be a plugin-name string or an object with a "name" (and optional "marketplace").' });
      return;
    }
    const d = dep as Record<string, unknown>;
    const ref = d.name ?? d.plugin;
    if (typeof ref !== 'string' || ref.trim() === '') {
      errors.push({ path: `dependencies[${i}]`, message: 'Dependency object must have a non-empty "name".' });
    }
    if (d.marketplace !== undefined && typeof d.marketplace !== 'string') {
      errors.push({ path: `dependencies[${i}].marketplace`, message: 'Dependency "marketplace" must be a string.' });
    }
  });
}

/**
 * Validate a manifest's optional `commandsMetadata` object-mapping (the reference parity:
 * CommandMetadataSchema, utils/plugins/schemas.ts L385-416). Each value must be an
 * object carrying EXACTLY ONE of `source` (a relative, non-escaping command-file
 * path) or `content` (inline markdown) — the schema `.refine` rejects both/neither —
 * plus optional string `description`/`argumentHint`/`model` and a string-array
 * `allowedTools`. Each message carries the corrective example wording so an author
 * can self-correct.
 */
export function validateCommandsMetadata(val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: 'commandsMetadata', message: 'Plugin "commandsMetadata" must be an object mapping a command name to its { source | content, ... } metadata.' });
    return;
  }
  for (const [key, meta] of Object.entries(val as Record<string, unknown>)) {
    const label = `commandsMetadata.${key}`;
    if (!meta || typeof meta !== 'object' || Array.isArray(meta)) {
      errors.push({ path: label, message: 'Command metadata must be an object with either a "source" (file path) or "content" (inline markdown).' });
      continue;
    }
    const m = meta as Record<string, unknown>;
    const hasSource = typeof m.source === 'string' && m.source.trim() !== '';
    const hasContent = typeof m.content === 'string' && m.content.trim() !== '';
    if (hasSource === hasContent) {
      errors.push({ path: label, message: 'Command must have either "source" (file path) or "content" (inline markdown), but not both.' });
    }
    if (typeof m.source === 'string') validateComponentPathString(`${label}.source`, m.source, errors);
    for (const field of ['description', 'argumentHint', 'model'] as const) {
      if (m[field] !== undefined && typeof m[field] !== 'string') {
        errors.push({ path: `${label}.${field}`, message: `Command metadata "${field}" must be a string.` });
      }
    }
    if (m.allowedTools !== undefined && (!Array.isArray(m.allowedTools) || !m.allowedTools.every(t => typeof t === 'string'))) {
      errors.push({ path: `${label}.allowedTools`, message: 'Command metadata "allowedTools" must be an array of strings (e.g., ["Bash", "Read"]).' });
    }
  }
}

/**
 * The value types a plugin `userConfig` option may declare (the reference parity:
 * PluginUserConfigOptionSchema, utils/plugins/schemas.ts L589 — matches
 * `@anthropic-ai/mcpb`'s McpbUserConfigurationOption). A `directory`/`file` option
 * prompts for a path; `string`/`number`/`boolean` for the obvious scalars.
 */
export const USER_CONFIG_OPTION_TYPES = ['string', 'number', 'boolean', 'directory', 'file'] as const;

export type UserConfigOptionType = (typeof USER_CONFIG_OPTION_TYPES)[number];

/** An option KEY must be a valid identifier — it becomes an env var name at hook
 *  time (the reference parity: CLAUDE_PLUGIN_OPTION_<KEY>). */
export const USER_CONFIG_KEY_RE = /^[A-Za-z_]\w*$/;

/**
 * Validate a manifest's optional `userConfig` — the user-prompted configuration
 * options a plugin needs at enable time (the reference parity: PluginManifestUserConfigSchema
 * + PluginUserConfigOptionSchema, utils/plugins/schemas.ts L575-654). Each entry is
 * keyed by an identifier-safe option key and declares a `type` (one of
 * {@link USER_CONFIG_OPTION_TYPES}), required `title`/`description` strings, and the
 * optional `required`/`default`/`multiple`/`sensitive`/`min`/`max` knobs. Every
 * message carries a corrective example so an author can self-correct.
 */
export function validateUserConfig(val: unknown, errors: PluginValidationIssue[], labelPrefix = 'userConfig'): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: labelPrefix, message: `Plugin "${labelPrefix}" must be an object mapping an option key to its { type, title, description, ... } definition.` });
    return;
  }
  for (const [key, opt] of Object.entries(val as Record<string, unknown>)) {
    const label = `${labelPrefix}.${key}`;
    if (!USER_CONFIG_KEY_RE.test(key)) {
      errors.push({ path: label, message: `Option key "${key}" must be a valid identifier (letters, digits, underscore; no leading digit) — it becomes a CLAUDE_PLUGIN_OPTION_<KEY> env var in hooks.` });
    }
    if (!opt || typeof opt !== 'object' || Array.isArray(opt)) {
      errors.push({ path: label, message: 'Option must be an object with a "type", "title", and "description".' });
      continue;
    }
    const o = opt as Record<string, unknown>;
    if (typeof o.type !== 'string' || !(USER_CONFIG_OPTION_TYPES as readonly string[]).includes(o.type)) {
      errors.push({ path: `${label}.type`, message: `Option "type" must be one of: ${USER_CONFIG_OPTION_TYPES.join(', ')}.` });
    }
    if (typeof o.title !== 'string' || o.title.trim() === '') {
      errors.push({ path: `${label}.title`, message: 'Option "title" must be a non-empty string (the label shown in the config prompt).' });
    }
    if (typeof o.description !== 'string' || o.description.trim() === '') {
      errors.push({ path: `${label}.description`, message: 'Option "description" must be a non-empty string (help text for the config prompt).' });
    }
    for (const flag of ['required', 'multiple', 'sensitive'] as const) {
      if (o[flag] !== undefined && typeof o[flag] !== 'boolean') {
        errors.push({ path: `${label}.${flag}`, message: `Option "${flag}" must be a boolean (true/false).` });
      }
    }
    for (const num of ['min', 'max'] as const) {
      if (o[num] !== undefined && typeof o[num] !== 'number') {
        errors.push({ path: `${label}.${num}`, message: `Option "${num}" must be a number.` });
      }
    }
    if (o.default !== undefined && !isValidUserConfigDefault(o.default)) {
      errors.push({ path: `${label}.default`, message: 'Option "default" must be a string, number, boolean, or array of strings.' });
    }
  }
}

/** A userConfig `default` may be a scalar (string/number/boolean) or, for a
 *  `multiple` string option, an array of strings (the reference parity). */
export function isValidUserConfigDefault(val: unknown): boolean {
  if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') return true;
  return Array.isArray(val) && val.every(v => typeof v === 'string');
}

// ---------------------------------------------------------------------------
// Plugin option VALUES — the runtime side of `userConfig` (the reference parity:
// utils/plugins/pluginOptionsStorage.ts + commands/plugin/PluginOptionsDialog
// /PluginOptionsFlow). The validators above gate the manifest SHAPE; the
// functions below coerce/validate/prefill the user-entered VALUES against a
// declared schema so the config dialog and option storage behave identically
// across modes (TUI/detached) and never silently corrupt a value.
// ---------------------------------------------------------------------------

/**
 * A single plugin `userConfig` option declaration, as the config dialog and
 * option storage consume it (the reference parity: PluginUserConfigOptionSchema). `type`
 * is one of {@link USER_CONFIG_OPTION_TYPES}; `title`/`description` are shown in
 * the prompt; the knobs mirror the manifest schema validated by
 * {@link validatePluginContents}.
 */
export interface PluginUserConfigOption {
  type: UserConfigOptionType;
  title: string;
  description: string;
  required?: boolean;
  default?: string | number | boolean | string[];
  multiple?: boolean;
  sensitive?: boolean;
  min?: number;
  max?: number;
}

/** A declared option schema: option key → its declaration (the reference parity:
 *  PluginOptionSchema / UserConfigSchema). */
export type PluginOptionSchema = Record<string, PluginUserConfigOption>;

/** A resolved option value (the reference parity: a `directory`/`file`/`string`
 *  scalar, a coerced `number`/`boolean`, or a `multiple` string array). */
export type PluginOptionValue = string | number | boolean | string[];

/** Saved/collected option values keyed by option key (the reference parity:
 *  PluginOptionValues / UserConfigValues). */
export type PluginOptionValues = Record<string, PluginOptionValue>;

/** Canonical filename of a plugin's saved `userConfig` VALUES sidecar, co-located
 *  with the plugin bundle root (the reference parity: pluginOptionsStorage's per-plugin
 *  options file). The sole source of truth for the marker — both the installer's
 *  option persistence and the hook-env plugin-option injection resolve it here so
 *  the name is never re-declared. */
export const PLUGIN_OPTIONS_FILE = '.options.json';
