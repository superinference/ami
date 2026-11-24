// Split from manager.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { normalizeMcpNameSegment } from '../tools/mcp-tool';
import { createHash } from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import type { McpServerScope } from './manager-resolve-disabled-mcp-servers';
import type { McpServerConfig } from './manager-mcp-server-config';

/** The plugin-scoped substitution variables resolved AHEAD of `${ENV}` expansion. */
export interface PluginMcpVars {
  /** `${user_config.<key>}` values declared by the plugin's userConfig schema. */
  userConfig?: Record<string, string>;
  /** `${CLAUDE_PLUGIN_ROOT}` — the plugin install dir. */
  pluginRoot?: string;
  /** `${CLAUDE_PLUGIN_DATA}` — the persistent per-plugin data dir. */
  pluginDataDir?: string;
}

/**
 * Substitute the plugin-scoped variable classes in a config string BEFORE the
 * general `${ENV}` tier runs (the reference-parity `resolvePluginMcpEnvironment`,
 * mcpPluginIntegration.ts:L475-490):
 *   - `${user_config.<key>}` → the user-entered value for `<key>` (empty string
 *     when the key is absent, so a stale placeholder never leaks a literal
 *     `${user_config.x}` onto a command line);
 *   - `${CLAUDE_PLUGIN_ROOT}` → `vars.pluginRoot`;
 *   - `${CLAUDE_PLUGIN_DATA}` → `vars.pluginDataDir`.
 * Any OTHER `${...}` reference is left untouched for {@link expandEnvVarsTracked}
 * to resolve, giving plugin/user values precedence over a same-named env var.
 *
 * On Windows the two plugin-PATH substitutions (`CLAUDE_PLUGIN_ROOT` /
 * `CLAUDE_PLUGIN_DATA`) have their backslashes normalized to forward slashes
 * (the reference-parity `substitutePluginVariables`, pluginOptionsStorage.ts:L312-345)
 * so a path like `C:\Users\me\plugin` dropped into a shell command/arg is not
 * mangled by backslash escape interpretation. `user_config` values are NOT
 * normalized — they are opaque user data, not plugin install paths.
 */
export function substitutePluginMcpVars(value: string, vars: PluginMcpVars): string {
  const normalizePath = (p: string): string =>
    process.platform === 'win32' ? p.replace(/\\/g, '/') : p;
  return value.replace(/\$\{(user_config\.[A-Za-z0-9_]+|CLAUDE_PLUGIN_ROOT|CLAUDE_PLUGIN_DATA)\}/g, (_match, ref: string) => {
    if (ref === 'CLAUDE_PLUGIN_ROOT') return normalizePath(vars.pluginRoot ?? '');
    if (ref === 'CLAUDE_PLUGIN_DATA') return normalizePath(vars.pluginDataDir ?? '');
    const key = ref.slice('user_config.'.length);
    return vars.userConfig?.[key] ?? '';
  });
}

/**
 * Scope a plugin/bundle's MCP server names so two different plugins can each ship
 * a server under the same local name (e.g. `github`) without one clobbering the
 * other (the reference-parity `addPluginScopeToServers`, mcpPluginIntegration.ts:L341-360).
 * Each server is renamed to `plugin:${pluginName}:${name}` and tagged with a
 * {@link McpServerConfig.pluginSource} provenance marker. Returns the scoped map
 * plus a `nameMap` (original → scoped) so the caller can carry the plugin's
 * `mcp-auto-allow-patterns` onto the scoped tool names via
 * {@link remapMcpAutoAllowPatterns}. The input map is not mutated (each config is
 * shallow-cloned).
 */
export function scopePluginMcpServers(
  servers: Record<string, McpServerConfig>,
  pluginName: string,
  pluginSource: string,
): { scoped: Record<string, McpServerConfig>; nameMap: Record<string, string> } {
  const scoped: Record<string, McpServerConfig> = {};
  const nameMap: Record<string, string> = {};
  for (const [name, config] of Object.entries(servers)) {
    const scopedName = `plugin:${pluginName}:${name}`;
    scoped[scopedName] = { ...config, pluginSource };
    nameMap[name] = scopedName;
  }
  return { scoped, nameMap };
}

/**
 * Rewrite a plugin's `mcp-auto-allow-patterns` so they still match the per-plugin
 * SCOPED tool names produced by {@link scopePluginMcpServers}. A pattern targets a
 * server via the `mcp__<server>__…` tool-name prefix (or the bare `mcp__<server>`
 * server-granularity form); after scoping, the server segment normalizes
 * differently ({@link normalizeMcpNameSegment} maps the `plugin:<name>:<server>`
 * colons to underscores), so a stale `mcp__github__*` would never match the scoped
 * `mcp__plugin_<name>_github__…`. Each pattern whose server prefix matches a
 * `nameMap` entry is rewritten onto the scoped prefix; every other pattern (and a
 * pattern that does not reference a scoped server) is returned unchanged.
 */
export function remapMcpAutoAllowPatterns(
  patterns: string[],
  nameMap: Record<string, string>,
): string[] {
  const prefixMap: Array<[string, string]> = Object.entries(nameMap).map(
    ([orig, scoped]) => [`mcp__${normalizeMcpNameSegment(orig)}`, `mcp__${normalizeMcpNameSegment(scoped)}`],
  );
  return patterns.map((pattern) => {
    for (const [oldPrefix, newPrefix] of prefixMap) {
      if (pattern.startsWith(oldPrefix + '__')) {
        return newPrefix + pattern.slice(oldPrefix.length);
      }
      if (pattern === oldPrefix || pattern.startsWith(oldPrefix + '*')) {
        return newPrefix + pattern.slice(oldPrefix.length);
      }
    }
    return pattern;
  });
}

/**
 * Strip a leading UTF-8 byte-order mark (U+FEFF) from a string before it is
 * handed to `JSON.parse` (the reference-parity `stripBOM`, utils/json.ts +
 * remoteManagedSettings/syncCacheState.ts). PowerShell 5.x (and some Windows
 * editors) write UTF-8 files WITH a leading BOM, so an `.mcp.json` generated /
 * hand-edited there begins with an invisible `﻿` that makes `JSON.parse`
 * throw ("Unexpected token in JSON at position 0") and silently drops EVERY
 * server the file declared. Removing the BOM first makes such a config load
 * normally. A string without a BOM is returned unchanged (cheap char-code check).
 */
export function stripJsonBom(raw: string): string {
  return raw.charCodeAt(0) === 0xfeff ? raw.slice(1) : raw;
}

/**
 * Deterministic content hash of a raw MCP server config entry, used by
 * {@link McpManager.reloadFromConfig} to detect whether a server's config changed
 * across a config-file reload (the reference-parity config-hash diff). Object keys are
 * sorted recursively so two logically-equal configs that differ only in JSON key
 * order hash identically (a reload of an unchanged-but-reformatted file does not
 * needlessly tear a server down and respawn it).
 */
export function hashMcpServerEntry(entry: unknown): string {
  return createHash('sha256').update(stableStringifyForHash(entry)).digest('hex');
}

/** Stable JSON serialization with recursively sorted object keys (helper for
 *  {@link hashMcpServerEntry}). */
export function stableStringifyForHash(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) return '[' + value.map(stableStringifyForHash).join(',') + ']';
  const obj = value as Record<string, unknown>;
  const keys = Object.keys(obj).sort();
  return '{' + keys.map((k) => JSON.stringify(k) + ':' + stableStringifyForHash(obj[k])).join(',') + '}';
}

/**
 * Discover MCP config files for `cwd`, each tagged with the {@link McpServerScope}
 * it belongs to, in first-wins precedence order. Config files under cwd (the
 * shareable `.ami/mcp.json` and `.mcp.json`, walked up to the FS root) are the
 * `project` scope; the user-global `~/.ami/mcp.json` is the `user` scope. This is
 * the canonical discovery; {@link findMcpConfigPaths} is its paths-only projection.
 */
export function findMcpConfigScopes(cwd: string): Array<{ path: string; scope: McpServerScope }> {
  const entries: Array<{ path: string; scope: McpServerScope }> = [];
  const homeDir = process.env.HOME ?? process.env.USERPROFILE ?? '';

  // Precedence is first-wins (addServer rejects a duplicate name, so the first
  // config to register a server keeps it). Within a scope AMI's own config beats
  // the reference interop one; project scope beats user scope.
  //
  // We intentionally interop only at PROJECT scope (the shareable `.mcp.json` a
  // repo commits), NOT user-global ~/.claude.json: that file aggregates every
  // the reference server a developer ever configured (and a large per-project
  // tree), so auto-loading it would silently spin up unrelated servers. Users
  // who want global servers in AMI put them in ~/.ami/mcp.json.

  // 0. AMI local (machine-private, uncommitted `.ami/mcp.local.json` in cwd) — a
  //    per-user, NOT-committed project override (the reference-parity `local` ConfigScope):
  //    add a server for THIS repo on THIS machine only. Highest precedence
  //    (first-wins) so it overrides a committed project entry of the same name, and
  //    is NEVER approval-gated (the user authored it themselves, like the user scope),
  //    since the engine only treats `scope === 'project'` as untrusted/approval-gated.
  const localConfig = path.join(cwd, '.ami', 'mcp.local.json');
  if (fs.existsSync(localConfig)) entries.push({ path: localConfig, scope: 'local' });

  // 1. AMI project (.ami/mcp.json in cwd).
  const projectConfig = path.join(cwd, '.ami', 'mcp.json');
  if (fs.existsSync(projectConfig)) entries.push({ path: projectConfig, scope: 'project' });

  // 2. the reference project interop — the shareable `.mcp.json`. Walk from cwd UP
  //    to the filesystem root collecting every `.mcp.json` (closer-to-cwd first),
  //    so a monorepo-root config is still found when AMI is launched from a nested
  //    subdirectory (the reference-parity). first-wins precedence means the nearest file's
  //    servers take priority over an ancestor's.
  let dir = path.resolve(cwd);
  while (true) {
    const candidate = path.join(dir, '.mcp.json');
    if (fs.existsSync(candidate)) entries.push({ path: candidate, scope: 'project' });
    const parent = path.dirname(dir);
    if (parent === dir) break;
    dir = parent;
  }

  // 3. AMI user-global (~/.ami/mcp.json).
  const globalConfig = path.join(homeDir, '.ami', 'mcp.json');
  if (fs.existsSync(globalConfig)) entries.push({ path: globalConfig, scope: 'user' });

  return entries;
}

export function findMcpConfigPaths(cwd: string): string[] {
  return findMcpConfigScopes(cwd).map((e) => e.path);
}

/**
 * Env var (and its value) injected into a spawned stdio MCP server's child process
 * when the host session runs in a bypass / auto-allow permission mode, so a server
 * with its own permission layer can skip its prompts and stay non-interactive.
 * Mirrors the reference's `CLAUDE_CHROME_PERMISSION_MODE='skip_all_permission_checks'`
 * injection; the value is kept identical for cross-framework interop with servers
 * that already recognize that sentinel. See {@link McpManager.setBypassPermissions}.
 */
export const MCP_BYPASS_PERMISSION_ENV = 'MCP_PERMISSION_MODE';

export const MCP_BYPASS_PERMISSION_VALUE = 'skip_all_permission_checks';

/**
 * Resolve a non-colliding MCP server name against a set of already-used NORMALIZED
 * names (the reference-parity numbered-suffix dedup, claudeai.ts:L92-L119). Tries the base
 * name first, then appends ` (2)`, ` (3)`, … until the FINAL normalized name
 * (suffix included) is unused — the subtle edge case is that the SUFFIXED name is
 * what gets normalized+checked, so a suffix that itself collides with another
 * server's base name is still caught. Returns the chosen display `name` and its
 * `normalized` form; normalization is {@link normalizeMcpNameSegment} (the same
 * transform the qualified `mcp__…` tool name uses), so two servers can never
 * produce the same model-facing tool prefix. Does NOT mutate `usedNormalizedNames`
 * — the caller adds the returned `normalized` before resolving the next name.
 */
export function resolveMcpServerNameCollision(
  baseName: string,
  usedNormalizedNames: Set<string>,
): { name: string; normalized: string } {
  let name = baseName;
  let normalized = normalizeMcpNameSegment(name);
  let count = 1;
  while (usedNormalizedNames.has(normalized)) {
    count++;
    name = `${baseName} (${count})`;
    normalized = normalizeMcpNameSegment(name);
  }
  return { name, normalized };
}

/**
 * Injectable platform / filesystem seams for desktop-config import (see
 * {@link resolveDesktopMcpConfigPath} / {@link readDesktopMcpServers}). Mirrors the
 * ambient inputs the reference's `getClaudeDesktopConfigPath` reads (platform, USERPROFILE,
 * home dir, file/dir existence), split out so the WSL path-resolution branches are
 * testable on any CI host without a real Windows mount.
 */
export interface DesktopImportEnv {
  /** `process.platform` — only `darwin` (macOS) and `linux`+WSL are supported. */
  platform: NodeJS.Platform;
  /** True when running under WSL (a Windows filesystem is reachable at /mnt/c). */
  isWsl: boolean;
  /** Environment (read for `USERPROFILE` on WSL). */
  env: NodeJS.ProcessEnv;
  /** The current user's home directory (macOS config lives under it). */
  homedir: string;
  /** True when a path exists (defaults to `fs.existsSync`). */
  exists: (p: string) => boolean;
  /** Directory entry names (defaults to `fs.readdirSync`); may throw → treated as empty. */
  readdir: (p: string) => string[];
}

/** True when the current process is running inside WSL (Windows Subsystem for
 *  Linux): WSL sets `WSL_DISTRO_NAME`, and the kernel release string carries a
 *  `microsoft`/`WSL` marker. Used to decide whether a Windows desktop config under
 *  `/mnt/c` is reachable. */
export function detectWsl(): boolean {
  if (process.env.WSL_DISTRO_NAME || process.env.WSL_INTEROP) return true;
  try {
    return /microsoft|wsl/i.test(os.release());
  } catch {
    return false;
  }
}

/** Build the default {@link DesktopImportEnv} from the real process + filesystem. */
export function defaultDesktopImportEnv(): DesktopImportEnv {
  return {
    platform: process.platform,
    isWsl: detectWsl(),
    env: process.env,
    homedir: os.homedir(),
    exists: (p) => fs.existsSync(p),
    readdir: (p) => fs.readdirSync(p),
  };
}
