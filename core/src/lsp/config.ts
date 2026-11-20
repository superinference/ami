/**
 * Configurable LSP server registry.
 *
 * Mirrors the reference's configurable server set (services/lsp/config.ts +
 * LSPServerInstance's ScopedLspServerConfig + the plugin LspServerConfig
 * schema). Which language servers exist, their command/args/env, the workspace
 * folder, per-server init options and limits, and the extension→language map
 * are all data-driven and extensible — a user, project, or plugin can register
 * a new server, override a command, or add a language.
 *
 * core/ must not import from common/ or cli/, so configs are *injected* here
 * (via `registerLspServers`) by the upper layers rather than pulled; the
 * defaults are built in. `loadLspServersFromFile` parses a plugin `.lsp.json`
 * so the (out-of-core) plugin loader can feed validated configs in.
 */
import * as fs from 'fs';
import { expandEnvVarsTracked, substitutePluginMcpVars, type PluginMcpVars } from '../mcp/manager';

/**
 * Communication transport between the client and the language-server process.
 * Only `stdio` is implemented; `socket` is accepted as a configuration value for
 * the reference schema parity but rejected at spawn time with a clear error rather
 * than being silently treated as stdio.
 */
export type LspTransport = 'stdio' | 'socket';

export interface LspServerConfig {
  /** Executable to spawn (required). */
  command: string;
  /**
   * Alternate executables tried in order when `command` is not on PATH, before
   * giving up — an ordered-candidate fallback chain (e.g. a server published both
   * as `pyright-langserver` and `pyright`, or `clangd` vs `clangd-18`). The first
   * entry that resolves to an installed binary is spawned; `command` is always the
   * first preference. Mirrors the reference's alternate-binary fallback (the
   * uvx→pipx→pip→pip3 pattern) so a usable server isn't missed just because the
   * single canonical name is absent. Resolved via {@link firstAvailableBinary}.
   */
  commandAlternatives?: string[];
  /** Arguments passed to the server. */
  args?: string[];
  /**
   * Communication transport mechanism. Defaults to `stdio` when absent. `socket`
   * is a recognized value (schema parity) but unsupported at runtime.
   */
  transport?: LspTransport;
  /** Extra env for the server process (merged over the scrubbed base env). */
  env?: Record<string, string>;
  /** Workspace root; defaults to the request cwd when absent. */
  workspaceFolder?: string;
  /** Server-specific `initializationOptions` (e.g. required by vue). */
  initializationOptions?: Record<string, unknown>;
  /**
   * Server-specific settings pushed to the server via
   * `workspace/didChangeConfiguration` immediately after the initialize
   * handshake completes (parity with the reference's per-server `settings`).
   */
  settings?: unknown;
  /** Milliseconds to wait for `initialize` before giving up. */
  startupTimeout?: number;
  /** Milliseconds to wait for a graceful `shutdown` reply before force-killing. */
  shutdownTimeout?: number;
  /**
   * Whether to respawn the server after a crash. Defaults to `true` (bounded by
   * `maxRestarts`). Set `false` to leave a crashed server down until the session
   * restarts, matching the reference's `restartOnCrash` knob.
   */
  restartOnCrash?: boolean;
  /** Max crash-recovery respawns before refusing to restart. */
  maxRestarts?: number;
  /**
   * Selection priority when MORE THAN ONE configured server (under DIFFERENT keys)
   * claims the same file extension — e.g. a plugin-contributed server that should
   * win over the built-in default, or two servers for the same language. Higher
   * wins; the default is `0`. Ties fall back to a stable iteration order (built-in
   * defaults first, then registered), so overriding a default's extension from a
   * different key requires an explicit higher priority. (Registering under the
   * SAME key already overrides a default outright, independent of priority.)
   * Mirrors the reference's "multiple servers per extension with ordering/priority"
   * (`extensionMap` stores a LIST; selection is explicitly extensible to priority).
   */
  priority?: number;
  /**
   * Lowercased file-extension → languageId map driving extension routing and
   * the `languageId` sent in `didOpen`. Keys include the leading dot (".ts").
   */
  extensionToLanguage?: Record<string, string>;
  /**
   * PyPI package name for a Python-installable server (e.g. `python-lsp-server`,
   * `ruff`). When set and the server's command (+ alternatives) is NOT on PATH,
   * the "command not found" diagnostic gains an actionable bootstrap hint naming
   * the exact install command for the best available Python package manager
   * (`uv tool install` / `pipx install` / `pip install --user`), via
   * {@link detectPythonPackageManager}. AMI never auto-installs it — the hint is
   * surfaced for the user to run. Only meaningful for pip/uv-installable servers;
   * leave unset for npm-based servers (e.g. the built-in pyright/typescript).
   */
  pythonPackage?: string;
}

/**
 * Built-in defaults. The TypeScript server runs via `npx --no-install` so that
 * when `typescript-language-server` is NOT installed, npx refuses rather than
 * silently downloading and executing it from the registry (a supply-chain /
 * arbitrary-code-execution surface). See `assertNoAutoFetch`.
 */
export const DEFAULT_LSP_SERVERS: Record<string, LspServerConfig> = {
  typescript: {
    command: 'npx',
    args: ['--no-install', 'typescript-language-server', '--stdio'],
    extensionToLanguage: {
      '.ts': 'typescript', '.tsx': 'typescriptreact',
      '.js': 'javascript', '.jsx': 'javascriptreact',
      '.mjs': 'javascript', '.cjs': 'javascript',
    },
  },
  python: {
    command: 'pyright-langserver',
    args: ['--stdio'],
    extensionToLanguage: { '.py': 'python', '.pyw': 'python', '.pyi': 'python' },
  },
  go: {
    command: 'gopls',
    args: ['serve'],
    extensionToLanguage: { '.go': 'go' },
  },
  rust: {
    command: 'rust-analyzer',
    args: [],
    extensionToLanguage: { '.rs': 'rust' },
  },
};

/** Externally-registered servers (from config/plugins), merged over defaults. */
let registeredServers: Record<string, LspServerConfig> = {};

/**
 * Register (or override) LSP servers. Keys collide by scoped server name; the
 * last registration wins, and registered servers win over the built-in
 * defaults. Idempotent-friendly: callers pass the full set they manage.
 */
export function registerLspServers(servers: Record<string, LspServerConfig>): void {
  for (const [name, config] of Object.entries(servers)) {
    registeredServers[name] = config;
  }
}

/** Forget all externally-registered servers (defaults remain). Test seam. */
export function resetLspServers(): void {
  registeredServers = {};
}

/**
 * The effective server set: built-in defaults merged with (and overridden by)
 * anything registered via `registerLspServers`.
 */
export function getAllLspServers(): Record<string, LspServerConfig> {
  return { ...DEFAULT_LSP_SERVERS, ...registeredServers };
}

/**
 * Every configured server that handles `ext`, ordered by selection preference:
 * highest `priority` first (default 0), ties broken by stable insertion order so
 * a registered server wins over the built-in default. Returns `[]` when none
 * handle the extension. This is the single ranking used by BOTH the language-key
 * resolver and the `languageId` lookup so they can never disagree on which server
 * owns an extension. Mirrors the reference's per-extension server LIST + ordering.
 */
export function candidatesForExtension(
  ext: string,
  servers: Record<string, LspServerConfig> = getAllLspServers(),
): Array<{ name: string; config: LspServerConfig }> {
  const needle = ext.toLowerCase();
  const matches: Array<{ name: string; config: LspServerConfig; index: number }> = [];
  let index = 0;
  for (const [name, config] of Object.entries(servers)) {
    if (config.extensionToLanguage && needle in config.extensionToLanguage) {
      matches.push({ name, config, index });
    }
    index++;
  }
  matches.sort((a, b) => (b.config.priority ?? 0) - (a.config.priority ?? 0) || a.index - b.index);
  return matches.map(({ name, config }) => ({ name, config }));
}

/**
 * Resolve the language-server KEY for a file extension (with leading dot,
 * lowercased) — the highest-priority server that handles it. Returns null when
 * no configured server handles the extension.
 */
export function resolveLanguageForExtension(
  ext: string,
  servers: Record<string, LspServerConfig> = getAllLspServers(),
): string | null {
  return candidatesForExtension(ext, servers)[0]?.name ?? null;
}

/**
 * The `languageId` to send in `didOpen` for a file extension, e.g.
 * ".tsx" → "typescriptreact". Uses the SAME priority-ranked winner as
 * {@link resolveLanguageForExtension}. Falls back to "plaintext".
 */
export function languageIdForExtension(
  ext: string,
  servers: Record<string, LspServerConfig> = getAllLspServers(),
): string {
  const winner = candidatesForExtension(ext, servers)[0];
  return winner?.config.extensionToLanguage?.[ext.toLowerCase()] ?? 'plaintext';
}

/**
 * Supply-chain guard: refuse configs that would let `npx` auto-fetch a server
 * from the registry at runtime. An npx invocation MUST carry `--no-install`
 * (or `--offline`) so a missing package fails loudly instead of being silently
 * downloaded and executed. Throws on a violating config.
 */
export function assertNoAutoFetch(config: LspServerConfig): void {
  const cmd = config.command.split(/[\\/]/).pop() ?? config.command;
  if (cmd !== 'npx') return;
  const args = config.args ?? [];
  const guarded = args.includes('--no-install') || args.includes('--offline');
  if (!guarded) {
    throw new Error(
      `LSP server command "${config.command} ${args.join(' ')}" would let npx ` +
        `auto-fetch from the registry; add --no-install (or install the server ` +
        `binary locally) to avoid an unprompted supply-chain execution surface.`,
    );
  }
}

/** Minimal structural validation of one server config from untrusted JSON. */
export function validateLspServerConfig(name: string, raw: unknown): LspServerConfig {
  if (typeof raw !== 'object' || raw === null) {
    throw new Error(`LSP server "${name}": config must be an object`);
  }
  const r = raw as Record<string, unknown>;
  if (typeof r.command !== 'string' || r.command.length === 0) {
    throw new Error(`LSP server "${name}": "command" is required and must be a non-empty string`);
  }
  // A command carrying its arguments inline (spaces) is almost always an authoring
  // mistake — the launcher would try to exec the whole string as one program name.
  // the reference parity (LspServerConfigSchema.command refine, utils/plugins/schemas.ts):
  // reject a space-bearing command with a corrective, example-bearing message,
  // unless it is an absolute path (which may legitimately contain spaces, e.g. a
  // macOS app bundle under "/Applications/My App/...").
  if (r.command.includes(' ') && !r.command.startsWith('/')) {
    throw new Error(
      `LSP server "${name}": command should not contain spaces. Use the "args" array for arguments (e.g., { "command": "typescript-language-server", "args": ["--stdio"] }).`,
    );
  }
  if (r.args !== undefined && (!Array.isArray(r.args) || !r.args.every((a) => typeof a === 'string'))) {
    throw new Error(`LSP server "${name}": "args" must be an array of strings`);
  }
  if (
    r.commandAlternatives !== undefined &&
    (!Array.isArray(r.commandAlternatives) ||
      !r.commandAlternatives.every((a) => typeof a === 'string' && a.length > 0))
  ) {
    throw new Error(`LSP server "${name}": "commandAlternatives" must be an array of non-empty strings`);
  }
  if (r.transport !== undefined && r.transport !== 'stdio' && r.transport !== 'socket') {
    throw new Error(`LSP server "${name}": "transport" must be "stdio" or "socket"`);
  }
  if (r.pythonPackage !== undefined && (typeof r.pythonPackage !== 'string' || r.pythonPackage.length === 0)) {
    throw new Error(`LSP server "${name}": "pythonPackage" must be a non-empty string`);
  }
  const config: LspServerConfig = { command: r.command };
  if (r.commandAlternatives) config.commandAlternatives = r.commandAlternatives as string[];
  if (r.args) config.args = r.args as string[];
  if (r.transport === 'stdio' || r.transport === 'socket') config.transport = r.transport;
  if (r.env && typeof r.env === 'object') config.env = r.env as Record<string, string>;
  if (typeof r.workspaceFolder === 'string') config.workspaceFolder = r.workspaceFolder;
  if (r.initializationOptions && typeof r.initializationOptions === 'object') {
    config.initializationOptions = r.initializationOptions as Record<string, unknown>;
  }
  if (r.settings !== undefined) config.settings = r.settings;
  if (typeof r.pythonPackage === 'string' && r.pythonPackage.length > 0) config.pythonPackage = r.pythonPackage;
  if (typeof r.startupTimeout === 'number') config.startupTimeout = r.startupTimeout;
  if (typeof r.shutdownTimeout === 'number') config.shutdownTimeout = r.shutdownTimeout;
  if (typeof r.restartOnCrash === 'boolean') config.restartOnCrash = r.restartOnCrash;
  if (typeof r.maxRestarts === 'number') config.maxRestarts = r.maxRestarts;
  if (typeof r.priority === 'number') config.priority = r.priority;
  if (r.extensionToLanguage && typeof r.extensionToLanguage === 'object') {
    const map: Record<string, string> = {};
    for (const [ext, lang] of Object.entries(r.extensionToLanguage as Record<string, unknown>)) {
      // the reference parity (schemas.ts fileExtension refine): a file-extension key must
      // start with a dot and be at least two characters, so a bare "ts" (which would
      // never match a real file suffix) is rejected with a corrective example.
      if (!ext.startsWith('.') || ext.length < 2) {
        throw new Error(
          `LSP server "${name}": file extension "${ext}" must start with a dot (e.g., ".ts", not "ts").`,
        );
      }
      if (typeof lang === 'string') map[ext.toLowerCase()] = lang;
    }
    config.extensionToLanguage = map;
  }
  return config;
}

/**
 * Parse a plugin `.lsp.json` file into validated server configs. The file maps
 * scoped server name → config. Returns `{}` when the file is absent; throws on
 * malformed JSON or an invalid entry so the caller can surface the error.
 */
export function loadLspServersFromFile(filePath: string): Record<string, LspServerConfig> {
  let text: string;
  try {
    text = fs.readFileSync(filePath, 'utf-8');
  } catch {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    throw new Error(`Invalid JSON in LSP server config "${filePath}": ${(e as Error).message}`);
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error(`LSP server config "${filePath}" must be an object of server configs`);
  }
  const out: Record<string, LspServerConfig> = {};
  for (const [name, raw] of Object.entries(parsed as Record<string, unknown>)) {
    out[name] = validateLspServerConfig(name, raw);
  }
  return out;
}

/**
 * The plugin-scoped resolution context for a plugin-contributed LSP server:
 * the bundle install root (`${CLAUDE_PLUGIN_ROOT}`), the persistent per-plugin
 * data dir (`${CLAUDE_PLUGIN_DATA}`), and the user's saved `userConfig` values
 * (`${user_config.<key>}`). Mirrors the reference's `resolvePluginLspEnvironment`
 * context (lspPluginIntegration.ts:L229-292).
 */
export interface LspPluginEnvironment {
  /** Plugin install dir → `${CLAUDE_PLUGIN_ROOT}` + injected into the server env. */
  pluginRoot: string;
  /** Persistent per-plugin data dir → `${CLAUDE_PLUGIN_DATA}` + injected env. */
  pluginDataDir: string;
  /** `${user_config.<key>}` values from the plugin's userConfig schema (defaults
   *  already folded in by the caller). Absent keys resolve to an empty string. */
  userConfig?: Record<string, string>;
}

/**
 * Resolve variable references in a plugin-contributed LSP server config, matching
 * the reference's `resolvePluginLspEnvironment` (lspPluginIntegration.ts:L229-292).
 *
 * Substitution runs in the SAME precedence order as the MCP path (reusing the
 * canonical {@link substitutePluginMcpVars} + {@link expandEnvVarsTracked} so LSP
 * and MCP never drift): FIRST the plugin-scoped tier (`${CLAUDE_PLUGIN_ROOT}`,
 * `${CLAUDE_PLUGIN_DATA}`, `${user_config.<key>}`), THEN the general `${ENV}` /
 * `$VAR` / `${VAR:-default}` tier — so a plugin/user value wins over a same-named
 * process env var. It resolves `command`, `args`, `workspaceFolder`, and every
 * `env` VALUE, and injects `CLAUDE_PLUGIN_ROOT` / `CLAUDE_PLUGIN_DATA` as literal
 * paths into the server env (an author-declared `env` entry of the same name wins,
 * matching the reference spread order). The two injected path keys are NOT run
 * through expansion — they are already resolved absolute paths.
 *
 * Returns a NEW config (never mutates input) plus the de-duplicated names of
 * referenced env vars that were undefined and had no `:-default`, so the caller
 * can surface a missing-var diagnostic (parity: the reference logs them).
 */
export function resolvePluginLspEnvironment(
  config: LspServerConfig,
  plugin: LspPluginEnvironment,
): { resolved: LspServerConfig; missingVars: string[] } {
  const missing = new Set<string>();
  const vars: PluginMcpVars = {
    userConfig: plugin.userConfig,
    pluginRoot: plugin.pluginRoot,
    pluginDataDir: plugin.pluginDataDir,
  };
  const resolveValue = (value: string): string => {
    const substituted = substitutePluginMcpVars(value, vars);
    const { expanded, missingVars } = expandEnvVarsTracked(substituted);
    for (const m of missingVars) missing.add(m);
    return expanded;
  };

  const resolved: LspServerConfig = { ...config };
  if (resolved.command) resolved.command = resolveValue(resolved.command);
  if (resolved.args) resolved.args = resolved.args.map(resolveValue);

  // Inject the plugin root/data dir as literal paths, then let an author-declared
  // `env` entry of the same name override (reference spread order), and resolve
  // every OTHER env value through the two substitution tiers.
  const resolvedEnv: Record<string, string> = {
    CLAUDE_PLUGIN_ROOT: plugin.pluginRoot,
    CLAUDE_PLUGIN_DATA: plugin.pluginDataDir,
    ...(resolved.env ?? {}),
  };
  for (const [key, value] of Object.entries(resolvedEnv)) {
    if (key !== 'CLAUDE_PLUGIN_ROOT' && key !== 'CLAUDE_PLUGIN_DATA') {
      resolvedEnv[key] = resolveValue(value);
    }
  }
  resolved.env = resolvedEnv;

  if (resolved.workspaceFolder) resolved.workspaceFolder = resolveValue(resolved.workspaceFolder);

  return { resolved, missingVars: [...missing] };
}
