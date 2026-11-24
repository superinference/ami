// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { type LspServerConfig } from './lsp/config';
import type { CommandMetadata, PluginRequirements, PluginSettings } from './plugins-is-embedded-shell-command-allowed';
import { canonicalPath, hardenedResolve } from './plugins-command-definition';
import type { ManifestHooksOptions } from './plugins-command-definition';
import type { PluginChannel } from './plugins-plugin-user-config-option';
import type { PluginError } from './plugins-validate-plugin-option-values';
import type { PluginMcpServer } from './plugins-validate-plugin-contents';

/**
 * A discovered plugin bundle: a directory with a `.ami-plugin/plugin.json`
 * (or `.claude-plugin/plugin.json` fallback) manifest that contributes skills/,
 * agents/, commands/, and hooks.json to the registries.
 */
export interface PluginBundle {
  name: string;
  version: string;
  root: string;
  skillsDir?: string;
  agentsDir?: string;
  commandsDir?: string;
  hooksFile?: string;
  /**
   * ADDITIONAL hook files declared by the manifest `hooks` field (the reference parity:
   * pluginLoader's `manifest.hooks` string / string[] forms, L1652-1755) — loaded
   * and merged ON TOP of the standard {@link hooksFile}. Each entry is a resolved,
   * existing, path-traversal-hardened file, DEDUPED by realpath against the standard
   * hooks file and against each other (so a manifest entry that just re-points at
   * `hooks/hooks.json` is dropped, not double-loaded). `undefined` when the manifest
   * declares no additional files (or none survive). Produced by
   * {@link resolvePluginManifestHooks}.
   */
  hookFiles?: string[];
  /**
   * INLINE hook objects declared directly in the manifest `hooks` field (the reference
   * parity: pluginLoader's `manifest.hooks` inline-object form — an entry that is an
   * object rather than a file-path string is merged verbatim). Contributed alongside
   * {@link hooksFile}/{@link hookFiles}. `undefined` when the manifest declares none.
   */
  inlineHooks?: Record<string, unknown>[];
  /**
   * Bundled MCP server config file (the reference parity). A plugin may ship an
   * `.mcp.json` at its root (or a manifest-declared `mcpServers` path) declaring
   * MCP servers it contributes. Path-traversal/symlink hardened like every other
   * component path; `undefined` when the plugin ships none.
   */
  mcpServersFile?: string;
  /**
   * Additional command directories/files declared by the manifest `commandsPaths`
   * (the reference parity: loadPluginCommands's `plugin.commandsPaths`), resolved relative
   * to the plugin root and hardened against `..`/symlink escape. Each entry may be a
   * directory (recursively walked) or a single command file. `undefined` when the
   * manifest declares none.
   */
  commandsPaths?: string[];
  /**
   * Additional skill directories/files declared by the manifest `skillsPaths`
   * (the reference parity: loadPluginCommands's `plugin.skillsPaths`), resolved + hardened
   * exactly like {@link commandsPaths}. `undefined` when the manifest declares none.
   */
  skillsPaths?: string[];
  /**
   * Additional agent directories/files declared by the manifest `agentsPaths`
   * (the reference parity: loadPluginAgents's `plugin.agentsPaths`, L275-335 — each entry a
   * directory OR a single `.md` file), resolved + hardened exactly like
   * {@link commandsPaths}/{@link skillsPaths}. Loaded ALONGSIDE the primary
   * `agents/` dir. `undefined` when the manifest declares none or none survive.
   */
  agentsPaths?: string[];
  /**
   * Per-command metadata declared by the manifest `commandsMetadata` object-mapping
   * (the reference parity: PluginManifestCommandsSchema's object form). Maps a command key
   * to either a `source` file-path override (with description/argumentHint/model/
   * allowedTools overrides applied to the discovered command) OR inline `content`
   * that IS the command (no source file). `undefined` when the manifest declares
   * none or every entry is malformed.
   */
  commandsMetadata?: Record<string, CommandMetadata>;
  /**
   * Manifest `defaultEnabled` (the reference parity: a plugin may ship disabled-by-default).
   * Drives the tri-state enablement fallback (managed policy > explicit user marker >
   * `defaultEnabled` > true). `undefined` when the manifest omits it (treated as true).
   */
  defaultEnabled?: boolean;
  /**
   * Manifest `description` surfaced on the loaded bundle (the reference parity: the
   * plugin manifest's human-readable description) so a `/plugin` listing can show
   * what a bundle provides without re-reading its manifest. `undefined` when the
   * manifest omits it or it is not a non-empty string.
   */
  description?: string;
  /**
   * Manifest author display name, normalized to a string (the reference parity: a
   * manifest `author` may be a bare string or a `{ name, … }` object — both reduce
   * to the display name here). `undefined` when absent/blank. Feeds
   * {@link generatePluginId} for the deterministic bundle {@link PluginBundle.id}.
   */
  author?: string;
  /**
   * Deterministic plugin id derived from `author` + `name` via
   * {@link generatePluginId} (the reference parity: generateExtensionId). Present only
   * when the manifest carries an author, so distinct bundles get a stable,
   * collision-resistant identifier. `undefined` when no author is declared.
   */
  id?: string;
  /**
   * Runtime availability requirements declared by the manifest `requires`
   * (the reference parity: BuiltinPluginDefinition.isAvailable, builtinPlugins.ts
   * L64-68 — a plugin whose environment prerequisites aren't met is omitted
   * ENTIRELY). AMI's bundles come from JSON manifests (no code predicate), so
   * availability is expressed declaratively: a `platform`/`env`/`bin` gate
   * evaluated by {@link isPluginAvailable}. A bundle whose `requires` are not
   * satisfied is dropped from discovery (never surfaced), so this is only ever
   * set on bundles that survived the gate. `undefined` when none are declared.
   */
  requires?: PluginRequirements;
  /**
   * Inline MCP servers declared directly in the manifest `mcpServers` record
   * (the reference parity: PluginManifestMcpServerSchema's inline-record / array-mix
   * forms — schemas.ts L543-572), keyed by server name. These are contributed IN
   * ADDITION to any {@link mcpServersFile} the plugin ships. `undefined` when the
   * manifest declares no inline servers (or only a file path).
   */
  mcpServers?: Record<string, PluginMcpServer>;
  /**
   * Bundled LSP server config file (the reference parity: lspPluginIntegration — a
   * plugin may ship a `.lsp.json` at its root, or declare the path via a manifest
   * `lspServers` string). Path-traversal/symlink hardened. `undefined` when the
   * plugin ships none. Consumed via core's {@link loadLspServersFromFile}.
   */
  lspServersFile?: string;
  /**
   * Inline LSP servers declared directly in the manifest `lspServers` record
   * (the reference parity: PluginManifestLspServerSchema's inline-record / array-mix
   * forms — schemas.ts L738-767), validated against the canonical
   * {@link LspServerConfig} schema. Contributed alongside {@link lspServersFile}.
   * `undefined` when the manifest declares no inline LSP servers.
   */
  lspServers?: Record<string, LspServerConfig>;
  /**
   * Output-styles directory a plugin contributes (the reference parity:
   * loadPluginOutputStyles's `plugin.outputStylesPath`): the `<root>/output-styles`
   * dir, or a manifest-declared `outputStyles` relative override. Path-traversal/
   * symlink hardened. `undefined` when the plugin ships none.
   */
  outputStylesDir?: string;
  /**
   * Additional output-style directories/files declared by the manifest
   * `outputStylesPaths` (the reference parity: loadPluginOutputStyles's
   * `plugin.outputStylesPaths`), resolved + hardened exactly like
   * {@link commandsPaths}. `undefined` when the manifest declares none.
   */
  outputStylesPaths?: string[];
  /**
   * Allowlisted plugin-contributed settings (the reference parity:
   * PluginSettingsSchema.pick({agent}).strip() — pluginLoader.ts L1776-1849). A
   * plugin may contribute a default `agent` via `settings.json` (preferred) or a
   * manifest `settings` object; every other key is stripped. `undefined` when the
   * plugin contributes no allowlisted settings.
   */
  settings?: PluginSettings;
  /**
   * Message channels a plugin declares (the reference parity:
   * PluginManifestChannelsSchema — schemas.ts L656-703). Each channel binds one
   * MCP server (by `server` name, matching a key in {@link mcpServers}) that
   * emits `notifications/.../channel` messages to inject into the conversation
   * (Telegram/Slack/Discord bridges), and may declare per-channel
   * {@link PluginChannel.userConfig} to prompt the user for at enable time. The
   * `server`↔`mcpServers` cross-check runs at LOAD time (the mcpServers field may
   * be a not-yet-read file), so discovery keeps every entry with a non-empty
   * `server`. `undefined` when the manifest declares none (or none survive).
   */
  channels?: PluginChannel[];
}

/**
 * Resolve a manifest's `hooks` field across EVERY form the reference accepts (parity:
 * pluginLoader L1652-1755): a single value, or an array mixing additional-hooks-file
 * path STRINGS with INLINE hook objects. The standard `hooks.json`/`hooks/hooks.json`
 * is loaded automatically (passed as `standardHooksFile`), so `manifest.hooks` only
 * contributes EXTRA sources:
 *  - a string → a path to an additional hooks file, resolved + path-traversal
 *    hardened under `root`; a missing/escaping path yields a `path-not-found`
 *    {@link PluginError}; a file whose realpath duplicates the standard file (or an
 *    earlier entry) is dropped, and under `strict` that duplicate becomes a
 *    `hook-load-failed` error (the reference parity).
 *  - an object → inline hooks, collected verbatim to be merged.
 * Returns the ordered, deduped `hookFiles`, the `inlineHooks`, and any `errors`.
 */
export function resolvePluginManifestHooks(
  root: string,
  manifest: Record<string, unknown>,
  opts: ManifestHooksOptions = {},
): { hookFiles: string[]; inlineHooks: Record<string, unknown>[]; errors: PluginError[] } {
  const hookFiles: string[] = [];
  const inlineHooks: Record<string, unknown>[] = [];
  const errors: PluginError[] = [];
  const raw = manifest.hooks;
  // A manifest `hooks` STRING names the standard override and is already handled by
  // resolvePluginHooksFromManifest — only the ARRAY / inline-OBJECT forms add extra
  // sources here, so a bare string contributes nothing additional.
  if (raw === undefined || typeof raw === 'string') {
    return { hookFiles, inlineHooks, errors };
  }
  const source = opts.source ?? root;
  const plugin = opts.pluginName ?? '';
  const loaded = new Set<string>();
  if (opts.standardHooksFile) loaded.add(canonicalPath(opts.standardHooksFile));
  const entries = Array.isArray(raw) ? raw : [raw];
  for (const entry of entries) {
    if (typeof entry === 'string') {
      if (entry.trim() === '') continue;
      const resolved = hardenedResolve(root, entry);
      if (resolved === undefined) {
        errors.push({ type: 'path-not-found', source, plugin, path: path.join(root, entry), component: 'hooks' });
        continue;
      }
      const canonical = canonicalPath(resolved);
      if (loaded.has(canonical)) {
        if (opts.strict) {
          errors.push({
            type: 'hook-load-failed',
            source,
            plugin,
            hookPath: resolved,
            reason: `Duplicate hooks file: "${entry}" resolves to an already-loaded file (${canonical}). The standard hooks.json is loaded automatically, so manifest.hooks should only reference ADDITIONAL hook files.`,
          });
        }
        continue;
      }
      loaded.add(canonical);
      hookFiles.push(resolved);
    } else if (isPlainObject(entry)) {
      inlineHooks.push(entry);
    }
  }
  return { hookFiles, inlineHooks, errors };
}

/**
 * Canonical dedup key for an MCP server: its stdio `command` + `args`, or its
 * `url`. Two servers with the same key launch the same thing and are duplicates.
 */
export function mcpServerKey(server: PluginMcpServer): string {
  if (typeof server.url === 'string' && server.url.trim() !== '') return `url:${server.url.trim()}`;
  const cmd = typeof server.command === 'string' ? server.command.trim() : '';
  const args = Array.isArray(server.args) ? server.args.map(a => String(a)).join(' ') : '';
  return `cmd:${cmd} ${args}`.trim();
}

/**
 * Merge plugin-contributed MCP servers across bundles, SUPPRESSING duplicates by
 * launch target (the reference parity: types/plugin.ts `mcp-server-suppressed-duplicate`).
 * `existing` names already-configured servers (name → config) that a plugin server
 * must not shadow. Entries are merged in order; the first occurrence of a given
 * {@link mcpServerKey} wins and every later same-target server is dropped with a
 * typed {@link PluginError} naming what it duplicated. Returns the deduped map plus
 * the suppression errors so a consumer can surface the transparency message.
 */
export function mergePluginMcpServers(
  contributions: Array<{ plugin: string; source: string; servers: Record<string, PluginMcpServer> }>,
  existing: Record<string, PluginMcpServer> = {},
): { servers: Record<string, PluginMcpServer>; errors: PluginError[] } {
  const servers: Record<string, PluginMcpServer> = {};
  const errors: PluginError[] = [];
  const keyOwner = new Map<string, string>();
  for (const [name, cfg] of Object.entries(existing)) {
    servers[name] = cfg;
    keyOwner.set(mcpServerKey(cfg), name);
  }
  for (const { plugin, source, servers: contributed } of contributions) {
    for (const [serverName, cfg] of Object.entries(contributed)) {
      const key = mcpServerKey(cfg);
      const owner = keyOwner.get(key);
      if (owner !== undefined) {
        errors.push({ type: 'mcp-server-suppressed-duplicate', source, plugin, serverName, duplicateOf: owner === serverName ? `plugin:${plugin}` : owner });
        continue;
      }
      keyOwner.set(key, serverName);
      servers[serverName] = cfg;
    }
  }
  return { servers, errors };
}

/**
 * Resolve a manifest field declaring ADDITIONAL component locations (the reference parity:
 * `commandsPaths` / `skillsPaths`): a string, or an array of strings, each a
 * relative path hardened against `..`/symlink escape. Returns the existing,
 * contained absolute paths, or undefined when none are declared/survive.
 */
export function resolveManifestPathList(root: string, manifest: Record<string, unknown>, field: string): string[] | undefined {
  const raw = manifest[field];
  const entries = typeof raw === 'string' ? [raw] : Array.isArray(raw) ? raw : [];
  const out: string[] = [];
  for (const e of entries) {
    if (typeof e !== 'string' || e.trim() === '') continue;
    const resolved = hardenedResolve(root, e);
    if (resolved !== undefined) out.push(resolved);
  }
  return out.length > 0 ? out : undefined;
}

/**
 * Parse a manifest's `commandsMetadata` object-mapping into a normalized
 * {@link CommandMetadata} record, keeping only well-formed entries (the reference parity).
 * An entry is kept when it is an object carrying EXACTLY ONE of `source`/`content`;
 * `argument-hint` is accepted as an alias for `argumentHint` (TOML/the reference frontmatter
 * spelling). Returns undefined when the field is absent or no entry survives, so the
 * bundle carries a value only when it actually contributes command metadata.
 */
export function parseCommandsMetadata(manifest: Record<string, unknown>): Record<string, CommandMetadata> | undefined {
  const raw = manifest.commandsMetadata;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  const out: Record<string, CommandMetadata> = {};
  for (const [key, val] of Object.entries(raw as Record<string, unknown>)) {
    if (!val || typeof val !== 'object' || Array.isArray(val)) continue;
    const v = val as Record<string, unknown>;
    const entry: CommandMetadata = {};
    if (typeof v.source === 'string') entry.source = v.source;
    if (typeof v.content === 'string') entry.content = v.content;
    if (typeof v.description === 'string') entry.description = v.description;
    const argHint = v.argumentHint ?? v['argument-hint'];
    if (typeof argHint === 'string') entry.argumentHint = argHint;
    if (typeof v.model === 'string') entry.model = v.model;
    if (Array.isArray(v.allowedTools) && v.allowedTools.every(t => typeof t === 'string')) {
      entry.allowedTools = v.allowedTools as string[];
    }
    const hasSource = entry.source !== undefined && entry.source.trim() !== '';
    const hasContent = entry.content !== undefined && entry.content.trim() !== '';
    if (hasSource === hasContent) continue; // XOR: skip malformed entries at discovery
    out[key] = entry;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Context for evaluating {@link isPluginAvailable}: the current OS platform and
 * environment. Injectable so availability is deterministically unit-testable
 * (defaults to the live `process.platform` / `process.env`).
 */
export interface PluginAvailabilityContext {
  platform?: string;
  env?: Record<string, string | undefined>;
}

/** True when `name` resolves to an executable file on the PATH in `env`. */
export function binOnPath(name: string, env: Record<string, string | undefined>): boolean {
  // An explicit path (contains a separator) is checked directly, not via PATH.
  if (name.includes('/') || name.includes('\\')) {
    try {
      return fs.statSync(name).isFile();
    } catch {
      return false;
    }
  }
  const pathVar = env.PATH ?? env.Path ?? '';
  for (const dir of pathVar.split(path.delimiter)) {
    if (dir === '') continue;
    try {
      if (fs.statSync(path.join(dir, name)).isFile()) return true;
    } catch {
      /* keep scanning */
    }
  }
  return false;
}

/**
 * Evaluate a bundle's declarative {@link PluginRequirements} (the reference parity:
 * BuiltinPluginDefinition.isAvailable — a plugin whose environment prerequisites
 * aren't met is omitted entirely). Returns true when EVERY declared constraint is
 * satisfied: the current `platform` is allowed (when the list is non-empty), every
 * `env` var is set+non-empty, and every `bin` resolves on PATH. `undefined`/empty
 * constraints impose no requirement, so a plugin with no `requires` is always
 * available.
 */
export function isPluginAvailable(requires: PluginRequirements | undefined, ctx: PluginAvailabilityContext = {}): boolean {
  if (requires === undefined) return true;
  const platform = ctx.platform ?? process.platform;
  const env = ctx.env ?? process.env;
  if (Array.isArray(requires.platform) && requires.platform.length > 0 && !requires.platform.includes(platform)) {
    return false;
  }
  if (Array.isArray(requires.env)) {
    for (const name of requires.env) {
      const v = env[name];
      if (typeof v !== 'string' || v === '') return false;
    }
  }
  if (Array.isArray(requires.bin)) {
    for (const name of requires.bin) {
      if (!binOnPath(name, env)) return false;
    }
  }
  return true;
}

/**
 * Parse a manifest's optional `requires` gate into {@link PluginRequirements},
 * keeping only the well-formed string-array constraints. Returns undefined when
 * the field is absent or carries no usable constraint, so a bundle only records a
 * value when it actually declares a requirement.
 */
export function parsePluginRequirements(manifest: Record<string, unknown>): PluginRequirements | undefined {
  const raw = manifest.requires;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  const r = raw as Record<string, unknown>;
  const out: PluginRequirements = {};
  for (const key of ['platform', 'env', 'bin'] as const) {
    const v = r[key];
    if (Array.isArray(v)) {
      const strings = v.filter((x): x is string => typeof x === 'string' && x.trim() !== '');
      if (strings.length > 0) out[key] = strings;
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** True for a plain (non-array) object — an inline server-config record. */
export function isPlainObject(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}
