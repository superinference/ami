import * as fs from 'fs';
import * as path from 'path';
import 'crypto';
import { isSafeSegment } from './skill-catalog';
import './utils/string';
import './memory';
import './lsp/config';
import './permissions';
import { SSH_SCP_RE, TRANSPORT_HELPER_RE } from './plugins-is-embedded-shell-command-allowed';
import type { GitSourceValidation } from './plugins-is-embedded-shell-command-allowed';
import { resolveComponentDir, resolvePluginHooksFromManifest } from './plugins-command-definition';
import { COMPONENT_PATH_FIELDS, MARKETPLACE_ONLY_MANIFEST_FIELDS, SERVER_DECLARATION_FIELDS, validateCommandsMetadata, validateComponentPathField, validateManifestDependencies, validatePluginAuthor, validatePluginRequirements, validatePluginSettings, validateUserConfig } from './plugins-plugin-user-config-option';
import type { PluginOptionSchema, PluginOptionValues, PluginValidationIssue, PluginValidationResult } from './plugins-plugin-user-config-option';
import { getPluginErrorMessage, resolveManifestPath } from './plugins-validate-plugin-option-values';
import type { PluginComponent, PluginError } from './plugins-validate-plugin-option-values';
import { errMessage, generatePluginId, isENOENT, isNamespaceSafeName, manifestAuthorName, validatePluginContents } from './plugins-validate-plugin-contents';
import type { PluginLoadFailure } from './plugins-validate-plugin-contents';
import { isPluginAvailable, parseCommandsMetadata, parsePluginRequirements, resolveManifestPathList, resolvePluginManifestHooks } from './plugins-plugin-bundle';
import type { PluginBundle } from './plugins-plugin-bundle';
import { loadPluginSettings, parseInlineMcpServers, parseManifestLspServers, parsePluginChannels, validateManifestHooks, validatePluginChannels, validateServerDeclarations } from './plugins-plugin-capabilities';
import { PLUGIN_KILL_LIST_ENTRY_LIMIT, isSafePluginCloneAuthority } from './plugins-hash-plugin-tree';
import type { PluginKillList, PluginKillListEntry } from './plugins-hash-plugin-tree';
export { COMMAND_SHELLS, GIT_NO_PROMPT_ENV, assertPathWithinBase, buildGitCloneArgs, evaluatePluginHint, formatPluginHintNote, isEmbeddedShellCommandAllowed, isScpLikeGitSource, parseCommandShell, parseGitRef, parsePluginHintTags, parsePluginIdentifier, redactUrlCredentials, resolvePluginDir, stripPluginHintTags } from './plugins-is-embedded-shell-command-allowed';
export type { AmiPluginHint, CommandMetadata, CommandShell, GitSourceValidation, PluginHintGateOptions, PluginRecommendation, PluginRecommendationHint, PluginRequirements, PluginSettings } from './plugins-is-embedded-shell-command-allowed';
export { COMMAND_SOURCES, COMMAND_TYPES, REDACTED_COMMAND_ARGS, buildPluginRecommendation, getCommandDisplayName, isCommandEnabled, isCommandHidden, isCommandModelListable, isImmediateCommand, redactSensitiveCommandArgs, registerCommandAliases } from './plugins-command-definition';
export type { CommandDefinition, CommandSource, CommandType, ManifestHooksOptions, PluginCommandsMetadataContext } from './plugins-command-definition';
export { DEFAULT_COMMAND_DESCRIPTION, VALID_INSTALL_SCOPES, deriveCommandDescription, isInstallScope } from './plugins-derive-command-description';
export type { CommandScanDiagnostics, InstallScope } from './plugins-derive-command-description';
export { PLUGIN_EDITABLE_SCOPES, PLUGIN_OPTIONS_FILE, USER_CONFIG_OPTION_TYPES, isPersistablePluginScope, resolvePluginEditableScopes } from './plugins-plugin-user-config-option';
export type { ExtendedPluginScope, PersistablePluginScope, PluginChannel, PluginOptionSchema, PluginOptionValue, PluginOptionValues, PluginScopeSource, PluginUserConfigOption, PluginValidationIssue, PluginValidationResult, UserConfigOptionType } from './plugins-plugin-user-config-option';
export { PLUGIN_COMPONENTS, buildPluginOptionEnv, buildPluginOptionPrefill, formatPluginPathTraversalWarning, getPluginErrorMessage, getUnconfiguredPluginOptions, mergePluginErrors, pluginErrorKey, readPluginOptionValues, substituteUserConfigValues, validatePluginHooks, validatePluginOptionValues } from './plugins-validate-plugin-option-values';
export type { PluginComponent, PluginError } from './plugins-validate-plugin-option-values';
export { PLUGIN_ID_PREFIXS, generatePluginId, hashPluginSubdirPath, isENOENT, isNamespaceSafeName, loadCommands, normalizePluginSubdirPath, sanitizePluginIdSegment, validatePluginContents } from './plugins-validate-plugin-contents';
export type { PluginIdPrefix, PluginLoadFailure, PluginMcpServer } from './plugins-validate-plugin-contents';
export { isPluginAvailable, mergePluginMcpServers, resolvePluginManifestHooks } from './plugins-plugin-bundle';
export type { PluginAvailabilityContext, PluginBundle } from './plugins-plugin-bundle';
export { PLUGIN_CAPABILITIES, PLUGIN_CAPABILITY_CONSENT, PLUGIN_GATE_ERROR_CODES, PLUGIN_HOST_METHOD_SPECS, canonicalizePluginCapabilities, describePluginConsent, isPluginCapability, parsePluginSettings } from './plugins-plugin-capabilities';
export type { PluginCapability, PluginGateDecision, PluginGateErrorCode, PluginGateSubject, PluginHostMethodSpec } from './plugins-plugin-capabilities';
export { DEFAULT_PLUGIN_TREE_HASH_IGNORE, PLUGIN_COMMIT_PATTERN, PLUGIN_CONTENT_HASH_PATTERN, PLUGIN_EXECUTION_TIERS, PLUGIN_KILL_LIST_ENTRY_LIMIT, computePluginConsentFingerprint, derivePluginExecutionTier, emptyPluginLockfile, findKilledPlugin, gatePluginHostCall, hashPluginTree, parsePluginLockfile, pluginTierAllows, pluginTreeHashMatches, pluginUpdateAccessGrowth, pluginUpdateGrowsAccess, removePluginLock, upsertPluginLock, verifyPluginTreeHash } from './plugins-hash-plugin-tree';
export type { PluginAccessGrowth, PluginAccessProfile, PluginExecutionSubject, PluginExecutionTier, PluginIntegrityResult, PluginKillList, PluginKillListEntry, PluginLockEntry, PluginLockfile, PluginTreeHashOptions } from './plugins-hash-plugin-tree';
/** URL schemes we allow to back a clone. `file:` is a legitimate local-clone URL
 *  (distinct from the `file::` transport helper, which is blocked above); the
 *  dangerous cases are the transport helpers and non-git schemes. */
const ALLOWED_GIT_SCHEMES = new Set(['https:', 'http:', 'ssh:', 'git:', 'file:']);

/**
 * Validate a git repository SOURCE before it is handed to `git clone`.
 *
 * Rejects, in order:
 *  - empty / NUL-bearing sources;
 *  - sources beginning with `-` (git would parse them as flags — argument injection);
 *  - transport-helper sources (`ext::`, `fd::`, `file::`, any `<helper>::…`) that
 *    let git execute arbitrary commands;
 *  - explicit URLs whose scheme is not https/http/ssh/git (notably `file://`).
 *
 * Accepts scp-style SSH remotes (`git@host:path`) and GitHub shorthand
 * (`owner/repo`). Anything else with no scheme is treated as a local path and
 * left for the caller to resolve/existence-check.
 */
export function validateGitSource(source: string): GitSourceValidation {
  if (typeof source !== 'string' || source.trim() === '') {
    return { ok: false, reason: 'Git source is empty.' };
  }
  const s = source.trim();
  if (s.includes('\0')) return { ok: false, reason: 'Git source contains a NUL byte.' };
  if (s.startsWith('-')) {
    return {
      ok: false,
      reason: `Refusing git source beginning with '-' (argument injection): "${s}".`,
    };
  }
  if (TRANSPORT_HELPER_RE.test(s)) {
    const helper = s.slice(0, s.indexOf('::'));
    return {
      ok: false,
      reason: `Refusing git transport-helper source "${helper}::" (arbitrary command execution).`,
    };
  }
  if (SSH_SCP_RE.test(s)) return { ok: true };
  if (s.includes('://')) {
    let parsed: URL;
    try {
      parsed = new URL(s);
    } catch {
      return { ok: false, reason: `Invalid git URL: "${s}".` };
    }
    if (!ALLOWED_GIT_SCHEMES.has(parsed.protocol.toLowerCase())) {
      return {
        ok: false,
        reason: `Unsupported git URL scheme "${parsed.protocol}" (allowed: https, http, ssh, git).`,
      };
    }
    return { ok: true };
  }
  return { ok: true };
}

/** True when `value` parses as an absolute URL (parity with the reference's zod `.url()`
 *  refinement for homepage: a scheme-less `example.com` is rejected). */
function isValidUrl(value: string): boolean {
  try {
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

/**
 * Validate the OPTIONAL metadata fields of a plugin manifest (the reference parity:
 * PluginManifestMetadataSchema, utils/plugins/schemas.ts L251-L320). Every field
 * is optional, but when present must have the right shape; each message carries a
 * concrete example so an author can self-correct. Component-path and name/version
 * checks live in {@link validatePluginManifest}; this covers description, author,
 * homepage, repository, license, and keywords.
 */
function validateManifestMetadata(obj: Record<string, unknown>, errors: PluginValidationIssue[]): void {
  if (obj.description !== undefined && typeof obj.description !== 'string') {
    errors.push({ path: 'description', message: 'Plugin "description" must be a string (a brief, user-facing explanation of what the plugin provides).' });
  }
  validatePluginAuthor(obj.author, errors);
  if (obj.homepage !== undefined && (typeof obj.homepage !== 'string' || !isValidUrl(obj.homepage))) {
    errors.push({ path: 'homepage', message: 'Plugin "homepage" must be a valid URL (e.g., "https://example.com").' });
  }
  if (obj.repository !== undefined && typeof obj.repository !== 'string') {
    errors.push({ path: 'repository', message: 'Plugin "repository" must be a string (the source code repository URL).' });
  }
  if (obj.license !== undefined && typeof obj.license !== 'string') {
    errors.push({ path: 'license', message: 'Plugin "license" must be a string SPDX license identifier (e.g., "MIT", "Apache-2.0").' });
  }
  if (obj.keywords !== undefined && (!Array.isArray(obj.keywords) || !obj.keywords.every(k => typeof k === 'string'))) {
    errors.push({ path: 'keywords', message: 'Plugin "keywords" must be an array of strings (e.g., ["cli", "git"]).' });
  }
  validateManifestDependencies(obj.dependencies, errors);
}

/** Tokens a plugin `boolean` option value coerces to `true` (the reference parity:
 *  isEnvTruthy's allow-list). NOTE: this is a DIFFERENT contract from core's
 *  env-flag {@link isEnvTruthy} (a deny-list) — a plugin boolean option is
 *  truthy ONLY for an explicit affirmative token, so an unrecognized value is
 *  `false`, not `true`. */
const PLUGIN_OPTION_TRUTHY = new Set(['1', 'true', 'yes', 'on']);

/**
 * Coerce a plugin `boolean` option's string input to a boolean (the reference parity:
 * isEnvTruthy applied in buildFinalValues). Truthy only for an explicit
 * affirmative token ({@link PLUGIN_OPTION_TRUTHY}); every other value — including
 * blank and unrecognized text — is `false`.
 */
export function coercePluginOptionBool(value: string): boolean {
  return PLUGIN_OPTION_TRUTHY.has(value.trim().toLowerCase());
}

/**
 * Build the save payload from the raw string inputs the config dialog collected,
 * coercing each by its declared type (the reference parity: PluginOptionsDialog
 * `buildFinalValues`). The behavior-defining edge cases, matched exactly:
 *
 *   - A `sensitive` field left blank on RECONFIGURE (a value already exists in
 *     `initialValues`) is OMITTED — storage only writes keys it receives, so an
 *     omitted key keeps the existing secret instead of wiping it.
 *   - A `number` field left blank is OMITTED (`Number('')` is `0`, not `NaN`, so
 *     keeping it would smuggle a `0` past the required-field check).
 *   - A `number` field whose input is non-numeric falls back to the raw STRING
 *     (so {@link validatePluginOptionValues} can report a meaningful type error
 *     rather than storing `NaN`).
 *   - A `boolean` field is coerced via {@link coercePluginOptionBool}.
 *   - Everything else is stored as the entered string.
 */
export function coercePluginOptionValues(
  fields: string[],
  collected: Record<string, string>,
  schema: PluginOptionSchema,
  initialValues?: PluginOptionValues,
): PluginOptionValues {
  const finalValues: PluginOptionValues = {};
  for (const fieldKey of fields) {
    const opt = schema[fieldKey];
    const value = collected[fieldKey] ?? '';
    if (opt?.sensitive === true && value === '' && initialValues?.[fieldKey] !== undefined) {
      continue;
    }
    if (opt?.type === 'number') {
      if (value.trim() === '') continue;
      const num = Number(value);
      finalValues[fieldKey] = Number.isNaN(num) ? value : num;
    } else if (opt?.type === 'boolean') {
      finalValues[fieldKey] = coercePluginOptionBool(value);
    } else {
      finalValues[fieldKey] = value;
    }
  }
  return finalValues;
}

/**
 * Validate a parsed plugin manifest object. Checks required `name`/`version`
 * strings (with a kebab-case name refinement), validates the optional metadata
 * fields (description/author/homepage/repository/license/keywords) via
 * {@link validateManifestMetadata}, flags any declared component path that
 * contains `..` (would escape the plugin dir) or is absolute, and warns about
 * marketplace-only fields that are silently ignored at runtime.
 */
export function validatePluginManifest(raw: unknown): PluginValidationResult {
  const errors: PluginValidationIssue[] = [];
  const warnings: PluginValidationIssue[] = [];

  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { valid: false, errors: [{ path: 'root', message: 'Manifest must be a JSON object.' }], warnings };
  }
  const obj = raw as Record<string, unknown>;

  if (typeof obj.name !== 'string' || obj.name.trim() === '') {
    errors.push({ path: 'name', message: 'Missing or empty required "name".' });
  } else if (!isSafeSegment(obj.name)) {
    errors.push({ path: 'name', message: `Unsafe plugin name "${obj.name}" (no slashes, "..", or NUL).` });
  } else if (obj.name.includes(' ')) {
    // the reference parity (PluginManifestMetadataSchema name refine): a name is used for
    // namespacing, so a space would break the `plugin:command` lookup — reject it
    // with a corrective kebab-case example rather than silently namespacing on it.
    errors.push({ path: 'name', message: 'Plugin name cannot contain spaces. Use kebab-case (e.g., "my-plugin").' });
  }

  if (obj.version !== undefined && typeof obj.version !== 'string') {
    errors.push({ path: 'version', message: '"version" must be a string.' });
  }

  validateManifestMetadata(obj, errors);
  validateCommandsMetadata(obj.commandsMetadata, errors);
  validateUserConfig(obj.userConfig, errors);
  validatePluginChannels(obj.channels, obj, errors);
  validatePluginRequirements(obj.requires, errors);
  validatePluginSettings(obj.settings, errors, warnings);
  validateManifestHooks(obj.hooks, errors);

  for (const field of COMPONENT_PATH_FIELDS) {
    validateComponentPathField(field, obj[field], errors);
  }

  for (const field of SERVER_DECLARATION_FIELDS) {
    validateServerDeclarations(field, obj[field], errors);
  }

  for (const field of MARKETPLACE_ONLY_MANIFEST_FIELDS) {
    if (field in obj) {
      warnings.push({ path: field, message: `"${field}" belongs in marketplace.json, not plugin.json — it is ignored here.` });
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/**
 * Full plugin validation: manifest + content files. Reads `plugin.json` from the
 * `.ami-plugin`/`.claude-plugin` dir — or a legacy root-level `plugin.json` —
 * under `pluginDir`, runs {@link validatePluginManifest} and
 * {@link validatePluginContents}, and merges the results. A missing manifest is a
 * hard error.
 */
export function validatePlugin(pluginDir: string): PluginValidationResult {
  const manifestPath = resolveManifestPath(pluginDir);
  if (manifestPath !== undefined) {
    let manifestResult: PluginValidationResult;
    try {
      manifestResult = validatePluginManifest(JSON.parse(fs.readFileSync(manifestPath, 'utf-8')));
    } catch (err) {
      manifestResult = { valid: false, errors: [{ path: manifestPath, message: `Invalid plugin.json: ${err instanceof Error ? err.message : String(err)}` }], warnings: [] };
    }
    const contentResult = validatePluginContents(pluginDir);
    return {
      valid: manifestResult.valid && contentResult.valid,
      errors: [...manifestResult.errors, ...contentResult.errors],
      warnings: [...manifestResult.warnings, ...contentResult.warnings],
    };
  }
  return { valid: false, errors: [{ path: pluginDir, message: 'No plugin manifest (.ami-plugin/plugin.json, .claude-plugin/plugin.json, or root plugin.json).' }], warnings: [] };
}

/**
 * Discover local plugin bundles under `dir`, ALSO returning structured load
 * failures (invalid JSON / failed manifest validation) so callers can surface
 * actionable guidance instead of silently dropping a broken plugin. A directory
 * with no manifest at all is not a failure (it simply isn't a plugin).
 *
 * The top-level directory read distinguishes ENOENT (the plugins dir simply does
 * not exist yet — not a failure) from a real I/O error (e.g. ENOTDIR/EACCES),
 * which is surfaced as a {@link PluginLoadFailure} rather than silently yielding
 * an empty list (the reference parity: isENOENT discrimination).
 */
export function discoverPluginsDetailed(dir: string): { bundles: PluginBundle[]; failures: PluginLoadFailure[] } {
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch (err) {
    if (isENOENT(err)) return { bundles: [], failures: [] };
    return { bundles: [], failures: [{ root: dir, reason: `Failed to read plugins directory: ${errMessage(err)}` }] };
  }
  const bundles: PluginBundle[] = [];
  const failures: PluginLoadFailure[] = [];
  for (const entry of entries) {
    const root = path.join(dir, entry);
    let isDir = false;
    try {
      isDir = fs.statSync(root).isDirectory();
    } catch {
      continue;
    }
    if (!isDir) continue;
    // Manifest lookup order: AMI's `.ami-plugin/`, then the reference's
    // `.claude-plugin/`, then a legacy root-level `plugin.json` (the reference parity:
    // pluginLoader's `legacyManifestPath` fallback) so older bundles that ship the
    // manifest at their root still load.
    const manifestPath = resolveManifestPath(root);
    if (manifestPath === undefined) continue; // not a plugin dir — not a failure
    let manifest: Record<string, unknown>;
    try {
      const parsed = JSON.parse(fs.readFileSync(manifestPath, 'utf-8')) as unknown;
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        failures.push({ root, reason: `Invalid plugin.json: manifest must be a JSON object. Check ${manifestPath}.`, error: { type: 'manifest-validation-error', source: root, manifestPath, validationErrors: ['Manifest must be a JSON object.'] } });
        continue;
      }
      manifest = parsed as Record<string, unknown>;
    } catch (err) {
      // CORRUPT (unparseable JSON) is reported distinctly from INVALID (parses but
      // fails the schema) so a front-end can guide the author appropriately.
      failures.push({ root, reason: `Corrupt plugin.json (JSON parse failed): ${errMessage(err)}. Fix the JSON syntax in ${manifestPath}.`, error: { type: 'manifest-parse-error', source: root, manifestPath, parseError: errMessage(err) } });
      continue;
    }
    // Security-relevant manifest errors (unsafe name, '..'/absolute component path)
    // turn a discovered dir into a structured failure rather than a silently-loaded
    // bundle. Lenient fields (missing name/version) still fall back below.
    // A traversal in an ARRAY-element server declaration (mcpServers[i]/lspServers[i])
    // is NOT bundle-dropping: the bad entry degrades gracefully (it is simply not
    // enumerated/loaded) while the rest of the bundle — including its valid inline
    // server entries — still loads, mirroring the list-form handling of
    // commandsPaths/skillsPaths. The single-path OVERRIDE forms (string mcpServers/
    // lspServers, and skills/agents/commands) remain hard errors.
    const hardErrors = validatePluginManifest(manifest).errors.filter(e =>
      /Unsafe plugin name|contains "\.\."|must be relative/.test(e.message) &&
      !/^(?:mcpServers|lspServers)\[\d+\]$/.test(e.path),
    );
    if (hardErrors.length > 0) {
      failures.push({ root, reason: `Invalid plugin.json: ${hardErrors.map(e => `${e.path}: ${e.message}`).join('; ')}`, error: { type: 'manifest-validation-error', source: root, plugin: typeof manifest.name === 'string' ? manifest.name : undefined, manifestPath, validationErrors: hardErrors.map(e => `${e.path}: ${e.message}`) } });
      continue;
    }
    // Discovery is lenient on optional fields (name/version fall back below); the
    // strict schema check is exposed separately via validatePluginManifest for
    // the `/plugin validate` command. A manifest name that is present but unusable
    // for namespacing (spaces / non-ASCII homographs — soft issues that are NOT
    // hard failures above) falls back to the directory name so `plugin:command`
    // lookups never break on an invalid id.
    const manifestName = manifest.name;
    const name = typeof manifestName === 'string' && isNamespaceSafeName(manifestName) ? manifestName : entry;
    const description = typeof manifest.description === 'string' && manifest.description.trim() !== '' ? manifest.description.trim() : undefined;
    const author = manifestAuthorName(manifest.author);
    // Availability gate (the reference parity: BuiltinPluginDefinition.isAvailable). A
    // bundle whose declarative `requires` (platform/env/bin) are not met by the
    // current environment is OMITTED ENTIRELY — not surfaced as a failure, exactly
    // like the reference's getBuiltinPlugins `continue`.
    const requires = parsePluginRequirements(manifest);
    if (!isPluginAvailable(requires)) continue;
    const lsp = parseManifestLspServers(root, manifest);
    // Surface a plugin-contributed component path that escapes the plugin root
    // (a symlink/`..` that survived manifest validation) as a security warning
    // instead of silently dropping it (the reference parity: lspPluginIntegration's
    // path-traversal warning). The bundle still loads without that component.
    const reportViolation = (component: PluginComponent) => (rel: string) => {
      const err: PluginError = { type: 'path-traversal-blocked', source: root, plugin: name, declaredPath: rel, component };
      failures.push({ root, reason: getPluginErrorMessage(err), error: err });
    };
    // Additional/inline hooks declared by the manifest `hooks` field (the reference parity:
    // pluginLoader's string/array/inline forms) merged ON TOP of the standard
    // hooks.json. The standard file seeds the realpath dedup so a manifest entry
    // re-pointing at it is dropped. `strict` is a marketplace/install-context input
    // (not a plugin.json field), so discovery resolves non-strict; the strict path is
    // exercised by callers that own that context via resolvePluginManifestHooks.
    const standardHooks = resolvePluginHooksFromManifest(root, manifest);
    const manifestHooks = resolvePluginManifestHooks(root, manifest, { source: root, pluginName: name, standardHooksFile: standardHooks });
    for (const err of manifestHooks.errors) failures.push({ root, reason: getPluginErrorMessage(err), error: err });
    bundles.push({
      name,
      version: (manifest.version as string | undefined) ?? '0.0.0',
      root,
      description,
      author,
      // Deterministic, collision-resistant id derived from author + name (the reference
      // parity: generateExtensionId). Only set when an author is declared, since
      // the id is anchored on it.
      id: author !== undefined ? generatePluginId(author, name) : undefined,
      defaultEnabled: typeof manifest.defaultEnabled === 'boolean' ? manifest.defaultEnabled : undefined,
      // Component paths: honor a manifest-declared relative override (the reference parity)
      // and fall back to the fixed default directory when none is declared. The
      // override was already vetted against `..`/absolute by the hardErrors gate
      // above; resolveComponentDir additionally hardens it against symlink escape.
      skillsDir: resolveComponentDir(root, manifest, 'skills', 'skills', reportViolation('skills')),
      agentsDir: resolveComponentDir(root, manifest, 'agents', 'agents', reportViolation('agents')),
      commandsDir: resolveComponentDir(root, manifest, 'commands', 'commands', reportViolation('commands')),
      // Hooks: a manifest-declared `hooks` file path overrides the convention of
      // <root>/hooks.json OR <root>/hooks/hooks.json (the reference layout).
      hooksFile: standardHooks,
      // Additional/inline hooks from the manifest `hooks` array/object form (the reference
      // parity), merged on top of the standard hooks file. Kept only when non-empty.
      hookFiles: manifestHooks.hookFiles.length > 0 ? manifestHooks.hookFiles : undefined,
      inlineHooks: manifestHooks.inlineHooks.length > 0 ? manifestHooks.inlineHooks : undefined,
      // MCP servers: a manifest-declared `mcpServers` file path overrides the
      // <root>/.mcp.json convention (the reference layout). Hardened like the rest.
      mcpServersFile: resolveComponentDir(root, manifest, 'mcpServers', '.mcp.json'),
      // Additional command/skill directories declared by the manifest, resolved +
      // hardened (the reference parity: commandsPaths / skillsPaths).
      commandsPaths: resolveManifestPathList(root, manifest, 'commandsPaths'),
      skillsPaths: resolveManifestPathList(root, manifest, 'skillsPaths'),
      // Additional agent directories/files (the reference parity: loadPluginAgents's
      // agentsPaths), resolved + hardened; loaded alongside the primary agents/ dir.
      agentsPaths: resolveManifestPathList(root, manifest, 'agentsPaths'),
      // Per-command metadata object-mapping (the reference parity: commandsMetadata) — kept
      // only when it carries at least one well-formed entry.
      commandsMetadata: parseCommandsMetadata(manifest),
      // Declarative availability requirements this bundle satisfied (see gate above).
      requires,
      // Inline MCP servers declared directly in the manifest (the reference parity:
      // PluginManifestMcpServerSchema inline-record form) — contributed alongside
      // any `.mcp.json` file at mcpServersFile.
      mcpServers: parseInlineMcpServers(manifest),
      // Plugin-contributed LSP servers (the reference parity: lspPluginIntegration): a
      // .lsp.json file path and/or inline manifest records, validated against the
      // canonical LspServerConfig schema.
      lspServersFile: lsp.file,
      lspServers: lsp.servers,
      // Output styles a plugin contributes (the reference parity: loadPluginOutputStyles):
      // the output-styles dir (+ manifest override) and extra outputStylesPaths.
      outputStylesDir: resolveComponentDir(root, manifest, 'outputStyles', 'output-styles', reportViolation('output-styles')),
      outputStylesPaths: resolveManifestPathList(root, manifest, 'outputStylesPaths'),
      // Allowlisted plugin-contributed settings (the reference parity: loadPluginSettings):
      // settings.json over manifest.settings, only the `agent` key survives.
      settings: loadPluginSettings(root, manifest),
      // Message channels the plugin declares (the reference parity: channels) — each binds
      // one of the plugin's MCP servers as a notification channel; kept only when at
      // least one entry carries a non-empty `server`.
      channels: parsePluginChannels(manifest),
    });
  }
  return { bundles, failures };
}

/**
 * Discover local plugin bundles under `dir`. Each immediate subdirectory with a
 * `.ami-plugin/plugin.json` manifest (or `.claude-plugin/plugin.json`
 * fallback) is a bundle; its skills/agents/commands/hooks component paths are
 * path-traversal hardened to its own root. (Thin wrapper over
 * {@link discoverPluginsDetailed} that drops the failure list.)
 */
export function discoverPlugins(dir: string): PluginBundle[] {
  return discoverPluginsDetailed(dir).bundles;
}

/**
 * A STRICTER clone-URL allowlist for plugin sources (Orca parity:
 * `isAllowedPluginGitUrl`). Beyond AMI's {@link validateGitSource} (which also
 * permits `http:`/`git:`/`file:` and bare shorthand), this accepts ONLY an
 * anonymous `https://` URL or an `ssh://` / scp-style `user@host:path` remote, and
 * rejects an embedded password, a `-`-leading user/host (ssh option injection), or a
 * `..` host. Use it to gate a lockfile-pinned or catalog-sourced clone to transports
 * that cannot run a transport-helper or leak credentials.
 */
export function isAllowedPluginCloneUrl(value: string): boolean {
  if (typeof value !== 'string') return false;
  const url = value.trim();
  if (url === '') return false;
  const scp = /^([^\s@/:]+)@([A-Za-z0-9.-]+):[^\s]+$/.exec(url);
  if (scp) {
    return isSafePluginCloneAuthority(scp[1]!, scp[2]!);
  }
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  if (!parsed.hostname || parsed.password || !isSafePluginCloneAuthority(parsed.username, parsed.hostname)) {
    return false;
  }
  if (parsed.protocol === 'https:') {
    return parsed.username.length === 0;
  }
  return parsed.protocol === 'ssh:';
}
/** Publisher/client clocks disagree by minutes, never days (Orca parity). */
export const PLUGIN_KILL_LIST_FUTURE_SKEW_MS = 24 * 60 * 60 * 1000;

/** Exported for coverage of the advisory-URL protocol gate used by kill-list parsing. */
export function isHttpsAdvisoryUrl(v: unknown): v is string {
  if (typeof v !== 'string' || v.length > 2_048) return false;
  try {
    return new URL(v).protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Parse a fetched kill-list (Orca parity: `pluginKillListSchema`), returning null
 * for a malformed envelope (wrong version, missing/invalid `generatedAt`). Parses
 * PER ENTRY and skips a bad one rather than discarding the whole list — one entry a
 * client can't read must not drop every other revocation. Accepts the entry array
 * under `entries` or `plugins`, and the key under `key` or `pluginKey`
 * (forward/back-compat). Dedups by key (first wins), caps at
 * {@link PLUGIN_KILL_LIST_ENTRY_LIMIT}.
 */
export function parsePluginKillList(raw: unknown): PluginKillList | null {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const o = raw as Record<string, unknown>;
  if (o.version !== 1) return null;
  if (typeof o.generatedAt !== 'string' || Number.isNaN(Date.parse(o.generatedAt))) return null;
  const rawEntries = Array.isArray(o.entries)
    ? o.entries
    : Array.isArray(o.plugins)
      ? o.plugins
      : [];
  const byKey = new Map<string, PluginKillListEntry>();
  for (const e of rawEntries.slice(0, PLUGIN_KILL_LIST_ENTRY_LIMIT)) {
    if (!e || typeof e !== 'object' || Array.isArray(e)) continue;
    const entry = e as Record<string, unknown>;
    const rawKey = typeof entry.key === 'string' ? entry.key : typeof entry.pluginKey === 'string' ? entry.pluginKey : '';
    const key = rawKey.trim();
    const reason = typeof entry.reason === 'string' ? entry.reason.trim() : '';
    if (!key || !reason || reason.length > 1_024) continue;
    if (byKey.has(key)) continue;
    const parsed: PluginKillListEntry = { key, reason };
    if (isHttpsAdvisoryUrl(entry.advisoryUrl)) parsed.advisoryUrl = entry.advisoryUrl as string;
    byKey.set(key, parsed);
  }
  return { version: 1, generatedAt: o.generatedAt, entries: [...byKey.values()] };
}

/**
 * Reject a freshly-fetched kill-list whose `generatedAt` is implausibly far in the
 * future (Orca parity: `isPluginKillListTooFarInFuture`) — such a list, if cached,
 * would permanently suppress every later (correctly-timestamped) revocation. Do NOT
 * re-judge an already-accepted cache against a slow device clock.
 */
export function isPluginKillListTooFarInFuture(killList: PluginKillList, now = Date.now()): boolean {
  return Date.parse(killList.generatedAt) > now + PLUGIN_KILL_LIST_FUTURE_SKEW_MS;
}
