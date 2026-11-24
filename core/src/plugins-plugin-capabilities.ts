// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import { validateLspServerConfig, type LspServerConfig } from './lsp/config';
import type { PluginSettings } from './plugins-is-embedded-shell-command-allowed';
import { hardenedResolve } from './plugins-command-definition';
import { USER_CONFIG_OPTION_TYPES, isValidUserConfigDefault, validateComponentPathString, validateUserConfig } from './plugins-plugin-user-config-option';
import type { PluginChannel, PluginOptionSchema, PluginUserConfigOption, PluginValidationIssue, UserConfigOptionType } from './plugins-plugin-user-config-option';
import { validateInlineLspServers } from './plugins-validate-plugin-contents';
import type { PluginMcpServer } from './plugins-validate-plugin-contents';
import { isPlainObject } from './plugins-plugin-bundle';

/**
 * Validate a server-declaration field (`mcpServers`/`lspServers`) across the forms
 * the reference accepts (schemas.ts L543-572 / L738-767): a FILE-PATH string, an INLINE
 * RECORD, or an ARRAY mixing file-path strings and inline records. Path strings are
 * traversal-checked via {@link validateComponentPathString} (so a `..`/absolute
 * override is still a hard error for the discovery gate); inline records are opaque
 * here (their per-server shape is validated where the servers are actually loaded).
 */
export function validateServerDeclarations(field: string, val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val === 'string') {
    validateComponentPathString(field, val, errors);
  } else if (Array.isArray(val)) {
    val.forEach((el, i) => {
      if (typeof el === 'string') validateComponentPathString(`${field}[${i}]`, el, errors);
      // An inline-record ARRAY entry is a `name → config` map (the reference parity: the
      // array form mixes file-path strings with inline records). Its structural
      // shape is opaque for MCP, but an inline LSP record is validated with the
      // canonical LSP schema so a bad command/extension surfaces here too.
      else if (field === 'lspServers' && isPlainObject(el)) validateInlineLspServers(`${field}[${i}]`, el, errors);
    });
  } else if (field === 'lspServers' && isPlainObject(val)) {
    // A bare inline LSP record (`name → config`) imposes no path to traverse, but
    // each server config IS validated (the reference parity: PluginManifestLspServerSchema's
    // inline-record form uses LspServerConfigSchema) so corrective, example-bearing
    // messages (command-spaces / file-extension dot) reach `/plugin validate`.
    validateInlineLspServers(field, val, errors);
  }
  // a bare inline MCP record imposes no path to traverse and is structurally opaque here
}

/**
 * Validate a manifest's optional `hooks` field across EVERY form the reference accepts
 * (parity: pluginLoader's `manifest.hooks` — a single additional-hooks-file path
 * STRING, an ARRAY mixing such path strings with INLINE hook objects, or a single
 * inline hook OBJECT). A path string is traversal-checked via
 * {@link validateComponentPathString}; an inline object is structurally opaque here
 * (its event→matcher shape is linted at load time by {@link validatePluginHooks}).
 * A non-string / non-object array entry is flagged. This supersedes the generic
 * component-path check so a legitimate inline-hooks entry is not rejected as "must
 * be a string".
 */
export function validateManifestHooks(val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val === 'string') {
    validateComponentPathString('hooks', val, errors);
  } else if (Array.isArray(val)) {
    val.forEach((el, i) => {
      if (typeof el === 'string') validateComponentPathString(`hooks[${i}]`, el, errors);
      else if (!isPlainObject(el)) {
        errors.push({ path: `hooks[${i}]`, message: `Hooks entry must be a file-path string or an inline hooks object (got ${Array.isArray(el) ? 'array' : typeof el}).` });
      }
    });
  } else if (!isPlainObject(val)) {
    errors.push({ path: 'hooks', message: 'Plugin "hooks" must be a file-path string, an inline hooks object, or an array mixing the two.' });
  }
}

/**
 * The set of inline MCP server names a manifest declares (the reference parity: a
 * channel's `server` must match a key in `mcpServers`). Only the INLINE record /
 * array-of-records forms contribute names here; when `mcpServers` is just a FILE
 * PATH string the names are unknown until the file is read, so this returns
 * `undefined` to signal "defer the cross-check to load time" (matching the reference's
 * schema comment that the server↔mcpServers check lives in mcpPluginIntegration).
 */
export function inlineMcpServerNames(manifest: Record<string, unknown>): Set<string> | undefined {
  const raw = manifest.mcpServers;
  if (typeof raw === 'string') return undefined; // file path — names not yet known
  const names = new Set<string>();
  const absorb = (rec: Record<string, unknown>): void => {
    for (const [name, cfg] of Object.entries(rec)) {
      if (isPlainObject(cfg)) names.add(name);
    }
  };
  if (isPlainObject(raw)) absorb(raw);
  else if (Array.isArray(raw)) {
    // An array may mix file-path strings (names unknown) with inline records.
    if (raw.some(e => typeof e === 'string')) return undefined;
    for (const entry of raw) if (isPlainObject(entry)) absorb(entry);
  } else {
    return undefined; // no inline declaration — nothing to cross-check against
  }
  return names;
}

/**
 * Validate a manifest's optional `channels` — the message-channel declarations a
 * plugin ships (the reference parity: PluginManifestChannelsSchema, utils/plugins/schemas.ts
 * L656-703). `channels` must be an array; each entry is an object with a non-empty
 * `server` string, an optional string `displayName`, and an optional `userConfig`
 * validated exactly like the top-level manifest `userConfig`. When the manifest
 * declares its MCP servers INLINE (so the names are known), each channel's `server`
 * is cross-checked against those keys; when `mcpServers` is only a file path, that
 * cross-check is deferred (the reference parity — resolved at load time, not parse time).
 */
export function validatePluginChannels(val: unknown, manifest: Record<string, unknown>, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (!Array.isArray(val)) {
    errors.push({ path: 'channels', message: 'Plugin "channels" must be an array of channel declarations, each binding an MCP server (e.g., [{ "server": "telegram", "displayName": "Telegram" }]).' });
    return;
  }
  const knownServers = inlineMcpServerNames(manifest);
  val.forEach((entry, i) => {
    const label = `channels[${i}]`;
    if (!isPlainObject(entry)) {
      errors.push({ path: label, message: 'Channel entry must be an object with a "server" (and optional "displayName"/"userConfig").' });
      return;
    }
    const server = entry.server;
    if (typeof server !== 'string' || server.trim() === '') {
      errors.push({ path: `${label}.server`, message: 'Channel "server" must be a non-empty string naming an MCP server declared in this plugin\'s "mcpServers".' });
    } else if (knownServers !== undefined && !knownServers.has(server)) {
      errors.push({ path: `${label}.server`, message: `Channel "server" "${server}" must match a key in this plugin's "mcpServers" (declared: ${[...knownServers].join(', ') || 'none'}).` });
    }
    if (entry.displayName !== undefined && typeof entry.displayName !== 'string') {
      errors.push({ path: `${label}.displayName`, message: 'Channel "displayName" must be a string (the human-readable name shown in the config dialog).' });
    }
    validateUserConfig(entry.userConfig, errors, `${label}.userConfig`);
  });
}

/**
 * Parse inline MCP server declarations from the manifest `mcpServers` field
 * (the reference parity: PluginManifestMcpServerSchema). The field may be a record of
 * `name → config`, or an ARRAY mixing file-path strings (handled separately via
 * {@link PluginBundle.mcpServersFile}) with inline records. Only the inline
 * record entries are collected here; later records override earlier same-named
 * ones. Returns undefined when there are no inline servers.
 */
export function parseInlineMcpServers(manifest: Record<string, unknown>): Record<string, PluginMcpServer> | undefined {
  const raw = manifest.mcpServers;
  const out: Record<string, PluginMcpServer> = {};
  const absorb = (rec: Record<string, unknown>): void => {
    for (const [name, cfg] of Object.entries(rec)) {
      if (isPlainObject(cfg)) out[name] = cfg as PluginMcpServer;
    }
  };
  if (isPlainObject(raw)) {
    absorb(raw);
  } else if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (isPlainObject(entry)) absorb(entry);
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Parse a channel's `userConfig` into a normalized {@link PluginOptionSchema},
 * keeping only well-formed options (an object with a valid `type`/`title`/
 * `description`). Mirrors the manifest `userConfig` shape so the config dialog and
 * option storage consume a channel's fields exactly like top-level ones. Returns
 * undefined when nothing well-formed remains.
 */
export function parseChannelUserConfig(raw: unknown): PluginOptionSchema | undefined {
  if (!isPlainObject(raw)) return undefined;
  const out: PluginOptionSchema = {};
  for (const [key, opt] of Object.entries(raw)) {
    if (!isPlainObject(opt)) continue;
    const type = opt.type;
    if (typeof type !== 'string' || !(USER_CONFIG_OPTION_TYPES as readonly string[]).includes(type)) continue;
    if (typeof opt.title !== 'string' || typeof opt.description !== 'string') continue;
    const entry: PluginUserConfigOption = { type: type as UserConfigOptionType, title: opt.title, description: opt.description };
    if (typeof opt.required === 'boolean') entry.required = opt.required;
    if (typeof opt.multiple === 'boolean') entry.multiple = opt.multiple;
    if (typeof opt.sensitive === 'boolean') entry.sensitive = opt.sensitive;
    if (typeof opt.min === 'number') entry.min = opt.min;
    if (typeof opt.max === 'number') entry.max = opt.max;
    if (isValidUserConfigDefault(opt.default)) entry.default = opt.default as PluginUserConfigOption['default'];
    out[key] = entry;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Parse the manifest `channels` array into normalized {@link PluginChannel}s,
 * keeping only well-formed entries (the reference parity: PluginManifestChannelsSchema).
 * An entry survives when it is an object with a non-empty `server` string; its
 * optional `displayName` (string) and `userConfig` (parsed via
 * {@link parseChannelUserConfig}) are carried when well-formed. Returns undefined
 * when the field is absent or no entry survives, so the bundle records channels
 * only when it actually contributes one. The `server`↔`mcpServers` cross-check is
 * deferred to load time (discovery stays lenient).
 */
export function parsePluginChannels(manifest: Record<string, unknown>): PluginChannel[] | undefined {
  const raw = manifest.channels;
  if (!Array.isArray(raw)) return undefined;
  const out: PluginChannel[] = [];
  for (const entry of raw) {
    if (!isPlainObject(entry)) continue;
    const server = entry.server;
    if (typeof server !== 'string' || server.trim() === '') continue;
    const channel: PluginChannel = { server };
    if (typeof entry.displayName === 'string') channel.displayName = entry.displayName;
    const userConfig = parseChannelUserConfig(entry.userConfig);
    if (userConfig !== undefined) channel.userConfig = userConfig;
    out.push(channel);
  }
  return out.length > 0 ? out : undefined;
}

/**
 * Parse LSP server declarations from the manifest `lspServers` field (the reference
 * parity: PluginManifestLspServerSchema — string path, inline record, or an array
 * mixing both). A string/array-string entry is resolved to a hardened `.lsp.json`
 * FILE path; record entries become inline {@link LspServerConfig}s validated via
 * the canonical {@link validateLspServerConfig}. Malformed inline configs are
 * dropped (discovery stays lenient; strict surfacing is {@link validatePluginManifest}).
 */
export function parseManifestLspServers(
  root: string,
  manifest: Record<string, unknown>,
): { file?: string; servers?: Record<string, LspServerConfig> } {
  const raw = manifest.lspServers;
  let file: string | undefined;
  const servers: Record<string, LspServerConfig> = {};
  const absorbRecord = (rec: Record<string, unknown>): void => {
    for (const [name, cfg] of Object.entries(rec)) {
      try {
        servers[name] = validateLspServerConfig(name, cfg);
      } catch {
        /* lenient at discovery */
      }
    }
  };
  const absorbPath = (p: string): void => {
    const resolved = hardenedResolve(root, p);
    if (resolved !== undefined && file === undefined) file = resolved;
  };
  if (typeof raw === 'string' && raw.trim() !== '') {
    absorbPath(raw);
  } else if (isPlainObject(raw)) {
    absorbRecord(raw);
  } else if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (typeof entry === 'string' && entry.trim() !== '') absorbPath(entry);
      else if (isPlainObject(entry)) absorbRecord(entry);
    }
  } else {
    // No manifest declaration: fall back to the <root>/.lsp.json convention.
    absorbPath('.lsp.json');
  }
  return {
    file,
    servers: Object.keys(servers).length > 0 ? servers : undefined,
  };
}

/**
 * Parse a raw settings object through the plugin allowlist (the reference parity:
 * parsePluginSettings / PluginSettingsSchema.pick({agent}).strip()): only the
 * `agent` key (a non-empty string) survives; every other key is stripped. Returns
 * undefined when nothing allowlisted remains, so a plugin records settings only
 * when it actually contributes one.
 */
export function parsePluginSettings(raw: unknown): PluginSettings | undefined {
  if (!isPlainObject(raw)) return undefined;
  const out: PluginSettings = {};
  if (typeof raw.agent === 'string' && raw.agent.trim() !== '') out.agent = raw.agent;
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Load a plugin's allowlisted settings (the reference parity: loadPluginSettings): a
 * `<root>/settings.json` file takes priority over a manifest `settings` object;
 * only allowlisted keys survive {@link parsePluginSettings}. Returns undefined
 * when neither source yields an allowlisted setting.
 */
export function loadPluginSettings(root: string, manifest: Record<string, unknown>): PluginSettings | undefined {
  const settingsPath = hardenedResolve(root, 'settings.json');
  if (settingsPath !== undefined) {
    try {
      const parsed = JSON.parse(fs.readFileSync(settingsPath, 'utf-8')) as unknown;
      const fromFile = parsePluginSettings(parsed);
      if (fromFile !== undefined) return fromFile;
    } catch {
      /* missing/corrupt settings.json falls through to manifest.settings */
    }
  }
  return parsePluginSettings(manifest.settings);
}

// ===========================================================================
// Plugin least-privilege — closed-enum capabilities, a deny-by-default
// host-call gate, execution tiers, and consent fingerprinting (Orca parity:
// shared/plugins/capabilities/plugin-capability-gate.ts `gatePluginHostCall`,
// contributions/plugin-execution-profile.ts, approval/
// plugin-update-access-growth.ts `pluginUpdateGrowsAccess`). AMI's install-time
// plugin consent was one coarse yes/no yet hooks ran with full host authority;
// these PURE building blocks give a bundle a declared, least-privilege capability
// set that a host call is checked against, a risk-proportional execution tier,
// and a consent fingerprint so a content-only bundle enables with no prompt and an
// update re-prompts ONLY when it widens reach. No I/O, no common/ imports.
// ===========================================================================

/**
 * The CLOSED set of capabilities a plugin bundle may declare and be granted. A
 * host call is denied by default unless its required capability is in the granted
 * set (see {@link gatePluginHostCall}). Deliberately coarse + enumerable (Orca
 * parity: `PluginCapabilityKind`) so consent is a short, auditable list — a
 * capability outside this enum is dropped at parse time ({@link
 * canonicalizePluginCapabilities}), never silently granted.
 */
export const PLUGIN_CAPABILITIES = [
  'read-files',
  'write-files',
  'run-commands',
  'network',
  'read-env',
  'spawn-process',
  'mcp-connect',
] as const;

export type PluginCapability = (typeof PLUGIN_CAPABILITIES)[number];

/** True when `value` is a known {@link PluginCapability}. */
export function isPluginCapability(value: unknown): value is PluginCapability {
  return typeof value === 'string' && (PLUGIN_CAPABILITIES as readonly string[]).includes(value);
}

/**
 * Canonicalize a raw manifest `capabilities` value into a sorted, deduped list of
 * KNOWN {@link PluginCapability}s. Trims + lower-cases each string and DROPS any
 * entry outside the closed enum (deny-by-default: an unrecognized capability is
 * never granted). A non-array input yields an empty grant. The sort makes the
 * result deterministic so it can feed a stable {@link computePluginConsentFingerprint}.
 */
export function canonicalizePluginCapabilities(raw: unknown): PluginCapability[] {
  if (!Array.isArray(raw)) return [];
  const out: PluginCapability[] = [];
  for (const v of raw) {
    const c = typeof v === 'string' ? v.trim().toLowerCase() : '';
    if (isPluginCapability(c) && !out.includes(c)) out.push(c);
  }
  return out.sort();
}

/**
 * The verbatim, human-readable consent string shown for each capability when a
 * plugin asks to be enabled (Orca parity: each capability carries its own consent
 * line). Exhaustive over {@link PLUGIN_CAPABILITIES} — adding a capability without
 * a consent string is a compile error.
 */
export const PLUGIN_CAPABILITY_CONSENT: Record<PluginCapability, string> = {
  'read-files': 'Read files in your workspace',
  'write-files': 'Create, modify, or delete files in your workspace',
  'run-commands': 'Run shell commands on your machine',
  'network': 'Make outbound network requests',
  'read-env': 'Read environment variables (which may include secrets)',
  'spawn-process': 'Spawn and manage background processes',
  'mcp-connect': 'Connect to MCP servers',
};

/** Render the ordered consent lines for a granted capability set (empty when none). */
export function describePluginConsent(capabilities: readonly PluginCapability[]): string[] {
  return canonicalizePluginCapabilities([...capabilities]).map((c) => PLUGIN_CAPABILITY_CONSENT[c]);
}

/** Why a {@link gatePluginHostCall} refused a call. */
export const PLUGIN_GATE_ERROR_CODES = ['unknown_method', 'capability_denied', 'consent_required'] as const;

export type PluginGateErrorCode = (typeof PLUGIN_GATE_ERROR_CODES)[number];

/** The decision of the capability gate: granted, or denied with a code + message. */
export type PluginGateDecision =
  | { granted: true }
  | { granted: false; code: PluginGateErrorCode; error: string };

/** What a host method needs: the {@link PluginCapability} it requires, or null for
 *  an always-safe method (logging, reading the bundle's own manifest). */
export interface PluginHostMethodSpec {
  capability: PluginCapability | null;
}

/**
 * The host-method -> required-capability table (Orca parity: the host API spec
 * table the gate keys off). A method absent from this table is UNKNOWN and always
 * denied, so a plugin can never reach an un-gated host surface by guessing a name.
 */
export const PLUGIN_HOST_METHOD_SPECS: Record<string, PluginHostMethodSpec> = {
  'fs.read': { capability: 'read-files' },
  'fs.list': { capability: 'read-files' },
  'fs.write': { capability: 'write-files' },
  'fs.delete': { capability: 'write-files' },
  'shell.exec': { capability: 'run-commands' },
  'process.spawn': { capability: 'spawn-process' },
  'net.fetch': { capability: 'network' },
  'env.get': { capability: 'read-env' },
  'mcp.connect': { capability: 'mcp-connect' },
  'log': { capability: null },
  'manifest.get': { capability: null },
};

/** The subject a host call is evaluated against. A `null` grant (an unknown,
 *  disabled, or stale-consent plugin) is indistinguishable from an empty grant and
 *  denies every capability-bearing call (Orca parity). */
export interface PluginGateSubject {
  grantedCapabilities: readonly PluginCapability[] | null;
}
