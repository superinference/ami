import { McpClient, McpToolSchema, McpResource, McpPrompt, McpPromptMessage, McpClientState, McpTerminalError, McpSessionExpiredError } from './client';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as path from 'path';
import { log as coreLog } from '../logger';
import { resolveRuntime, buildContainerArgs, isImageAvailable, pullImage, type ContainerConfig } from './container';

const DEFAULT_MCP_DESCRIPTION_LENGTH = 2048;
function getMcpMaxDescriptionLength(): number {
  const env = process.env.SUPERINFERENCE_MAX_MCP_DESCRIPTION_LENGTH;
  if (env) {
    const n = parseInt(env, 10);
    if (!Number.isNaN(n) && n > 0) return n;
  }
  return DEFAULT_MCP_DESCRIPTION_LENGTH;
}

const TRANSIENT_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ENOTFOUND', 'EAI_AGAIN']);
export function isTransientConnectError(err: unknown): boolean {
  if (err instanceof McpSessionExpiredError) return false;
  const code = (err as { code?: string })?.code;
  if (code && TRANSIENT_CODES.has(code)) return true;
  const msg = err instanceof Error ? err.message : String(err);
  return /ECONNREFUSED|ECONNRESET|ETIMEDOUT|EPIPE|socket hang up|connect ENOENT/i.test(msg);
}

export interface McpServerConfig {
  command: string;
  args?: string[];
  env?: Record<string, string>;
  headers?: Record<string, string>;
  requestTimeout?: number;
  connectTimeout?: number;
  autoConnect?: boolean;
  transport?: 'stdio' | 'sse' | 'http';
  url?: string;
  containerConfig?: ContainerConfig;
}

export type McpServerState = 'pending' | 'connecting' | 'ready' | 'error' | 'stopped';

export interface McpServerStatus {
  name: string;
  state: McpClientState | McpServerState;
  serverInfo: { name: string; version: string } | null;
  toolCount: number;
  resourceCount: number;
  uptime: number | null;
  imageAvailable?: boolean;
  containerBacked?: boolean;
  error?: string;
}

export interface PendingServerInfo {
  name: string;
  config: McpServerConfig;
  state: McpServerState;
  error?: string;
  imageAvailable?: boolean;
}

function resolveEnvVars(headers: Record<string, string>): Record<string, string> {
  const resolved: Record<string, string> = {};
  for (const [key, value] of Object.entries(headers)) {
    resolved[key] = value.replace(/\$\{(\w+)\}/g, (_, v) => process.env[v] || '');
  }
  return resolved;
}

export class McpManager extends EventEmitter {
  private clients = new Map<string, McpClient>();
  private configs = new Map<string, McpServerConfig>();
  private _allTools = new Map<string, { serverName: string; schema: McpToolSchema }>();
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private rootPaths: string[] = [];
  private reconnectTimers = new Map<string, ReturnType<typeof setTimeout>>();
  private pendingServers = new Map<string, PendingServerInfo>();
  private serverErrors = new Map<string, string>();

  /** Timestamp (ms) of the last tool call per server name. */
  _lastUsedAt = new Map<string, number>();
  /** Idle timeout in ms — servers unused for this long are stopped. Default 30 min. */
  _idleTimeoutMs = 30 * 60 * 1000;
  private _idleCheckTimer: ReturnType<typeof setInterval> | null = null;

  private _allowedServers: Set<string> | null = null;
  private _deniedServers: string[] = [];

  setRootPaths(paths: string[]): void {
    this.rootPaths = paths;
  }

  setAllowedServers(serverNames: string[]): void {
    this._allowedServers = new Set(serverNames);
    this._deniedServers = [];
  }

  isServerAllowed(serverName: string): boolean {
    if (this._allowedServers === null) return true;
    return this._allowedServers.has(serverName);
  }

  getDeniedServers(): string[] {
    return [...this._deniedServers];
  }

  private enforceAllowlist(name: string): void {
    if (this._allowedServers !== null && !this._allowedServers.has(name)) {
      if (!this._deniedServers.includes(name)) {
        this._deniedServers.push(name);
      }
      throw new Error(`MCP server "${name}" is not in the allowed servers list`);
    }
  }

  addServer(name: string, config: McpServerConfig): void {
    this.enforceAllowlist(name);
    if (this.clients.has(name)) {
      throw new Error(`MCP server '${name}' already registered`);
    }

    let resolvedCommand = config.command || '';
    let resolvedArgs = config.args;

    if (config.containerConfig) {
      const runtime = resolveRuntime(config.containerConfig.runtime);
      if (runtime) {
        resolvedCommand = runtime;
        resolvedArgs = buildContainerArgs(config.containerConfig);
        coreLog('mcp', `container MCP for '${name}': ${runtime} ${resolvedArgs.join(' ')}`);
      } else {
        coreLog('mcp', `no container runtime found for '${name}'`);
      }
    }

    this.configs.set(name, config);
    const client = new McpClient({
      command: resolvedCommand,
      args: resolvedArgs,
      env: config.env,
      headers: config.headers ? resolveEnvVars(config.headers) : undefined,
      requestTimeout: config.requestTimeout ?? (config.containerConfig ? 60000 : 30000),
      connectTimeout: config.connectTimeout ?? (config.containerConfig ? 120000 : 10000),
      rootPaths: this.rootPaths,
    });

    client.on('error', (err) => this.emit('server-error', name, err));
    client.on('close', (code, uptimeMs) => this.emit('server-close', name, code, uptimeMs));
    client.on('ready', () => this.emit('server-ready', name));
    client.on('notification', (method: string, params: unknown) => this.emit('notification', name, method, params));

    client.on('terminal-error', (classified: McpTerminalError) => {
      coreLog('mcp', `Terminal error for ${name}: [${classified.code}] ${classified.message}`);
      this.emit('terminal-error', name, classified);
    });

    client.on('session-expired', () => {
      coreLog('mcp', `Session expired for ${name}, clearing cache and reconnecting`);
      this.invalidateToolCache(name);
      this.emit('session-expired', name);
    });

    client.on('tools-changed', () => {
      coreLog('mcp', `Tool list changed for ${name}, refreshing`);
      this.invalidateToolCache(name);
      this.refreshToolsForServer(name).catch(() => {});
      this.emit('tools-changed', name);
    });

    client.on('resources-changed', () => {
      coreLog('mcp', `Resource list changed for ${name}`);
      this.emit('resources-changed', name);
    });

    client.on('prompts-changed', () => {
      coreLog('mcp', `Prompt list changed for ${name}`);
      this.emit('prompts-changed', name);
    });

    this.clients.set(name, client);
  }

  addPendingServer(name: string, config: McpServerConfig): void {
    this.enforceAllowlist(name);
    if (this.clients.has(name) || this.pendingServers.has(name)) {
      throw new Error(`MCP server '${name}' already registered`);
    }
    let imageAvailable: boolean | undefined;
    if (config.containerConfig) {
      const runtime = resolveRuntime(config.containerConfig.runtime);
      if (runtime) {
        imageAvailable = isImageAvailable(runtime, config.containerConfig.image);
      }
    }
    const info: PendingServerInfo = { name, config, state: 'pending', imageAvailable };
    this.pendingServers.set(name, info);
    this.configs.set(name, config);
    coreLog('mcp', `pending MCP server registered: ${name} (image available: ${imageAvailable ?? 'n/a'})`);
    this.emit('server-state-changed', name, 'pending');
  }

  async ensureConnected(name: string): Promise<void> {
    if (this.clients.has(name)) {
      const client = this.clients.get(name)!;
      if (client.state === 'ready') return;
    }

    const pending = this.pendingServers.get(name);
    if (!pending) {
      if (!this.clients.has(name)) throw new Error(`MCP server '${name}' not found`);
      await this.connectServer(name);
      return;
    }

    pending.state = 'connecting';
    this.serverErrors.delete(name);
    this.emit('server-state-changed', name, 'connecting');
    coreLog('mcp', `on-demand connecting MCP server: ${name}`);

    try {
      if (pending.config.containerConfig) {
        const runtime = resolveRuntime(pending.config.containerConfig.runtime);
        if (runtime && !isImageAvailable(runtime, pending.config.containerConfig.image)) {
          coreLog('mcp', `pulling image for ${name}: ${pending.config.containerConfig.image}`);
          await pullImage(runtime, pending.config.containerConfig.image);
          coreLog('mcp', `image pulled for ${name}`);
        }
      }
      this.pendingServers.delete(name);
      this.addServer(name, pending.config);
      await this.connectServer(name);

      pending.state = 'ready';
      this.emit('server-state-changed', name, 'ready');
      coreLog('mcp', `MCP server connected on-demand: ${name} (${this.getToolsForServer(name).length} tools)`);
    } catch (err) {
      const errMsg = err instanceof Error ? err.message : String(err);
      pending.state = 'error';
      pending.error = errMsg;
      this.serverErrors.set(name, errMsg);
      this.pendingServers.set(name, pending);
      if (this.clients.has(name)) {
        this.clients.get(name)!.disconnect();
        this.clients.delete(name);
      }
      this.emit('server-state-changed', name, 'error');
      coreLog('mcp', `on-demand connect failed for ${name}: ${errMsg}`);
      throw err;
    }
  }

  async stopServer(name: string): Promise<void> {
    const client = this.clients.get(name);
    if (client) {
      client.disconnect();
      this.clients.delete(name);
      this.invalidateToolCache(name);
    }

    const timer = this.reconnectTimers.get(name);
    if (timer) { clearTimeout(timer); this.reconnectTimers.delete(name); }

    const config = this.configs.get(name);
    if (config) {
      this.pendingServers.set(name, { name, config, state: 'stopped' });
    }
    this.serverErrors.delete(name);
    this.emit('server-state-changed', name, 'stopped');
    coreLog('mcp', `MCP server stopped: ${name}`);
  }

  async restartServer(name: string): Promise<void> {
    await this.stopServer(name);
    await this.ensureConnected(name);
  }

  isPending(name: string): boolean {
    return this.pendingServers.has(name) && !this.clients.has(name);
  }

  getServerState(name: string): McpServerState {
    const pending = this.pendingServers.get(name);
    if (pending && !this.clients.has(name)) return pending.state;
    const client = this.clients.get(name);
    if (!client) return 'pending';
    if (client.state === 'ready') return 'ready';
    if (client.state === 'connecting') return 'connecting';
    if (client.state === 'error') return 'error';
    return 'stopped';
  }

  getToolsForServer(name: string): McpToolSchema[] {
    const tools: McpToolSchema[] = [];
    for (const [, entry] of this._allTools) {
      if (entry.serverName === name) tools.push(entry.schema);
    }
    return tools;
  }

  listAllServers(): McpServerStatus[] {
    const result: McpServerStatus[] = [];
    const seen = new Set<string>();

    for (const name of this.clients.keys()) {
      seen.add(name);
      result.push(this.getFullServerStatus(name));
    }
    for (const [name, info] of this.pendingServers) {
      if (seen.has(name)) continue;
      seen.add(name);
      result.push({
        name,
        state: info.state,
        serverInfo: null,
        toolCount: 0,
        resourceCount: 0,
        uptime: null,
        imageAvailable: info.imageAvailable,
        containerBacked: !!info.config.containerConfig,
        error: info.error || this.serverErrors.get(name),
      });
    }
    return result;
  }

  private getFullServerStatus(name: string): McpServerStatus {
    const client = this.clients.get(name);
    const config = this.configs.get(name);
    if (!client) {
      return {
        name, state: 'pending', serverInfo: null, toolCount: 0,
        resourceCount: 0, uptime: null, containerBacked: !!config?.containerConfig,
      };
    }
    return {
      name,
      state: client.state,
      serverInfo: client.serverInfo,
      toolCount: client.tools.length,
      resourceCount: client.resources.length,
      uptime: client.uptime,
      containerBacked: !!config?.containerConfig,
      error: this.serverErrors.get(name),
    };
  }

  removeServer(name: string): void {
    const timer = this.reconnectTimers.get(name);
    if (timer) {
      clearTimeout(timer);
      this.reconnectTimers.delete(name);
    }

    const client = this.clients.get(name);
    if (client) {
      client.disconnect();
      this.clients.delete(name);
    }
    this.pendingServers.delete(name);
    this.serverErrors.delete(name);
    this.configs.delete(name);
    this.invalidateToolCache(name);
  }

  async connectServer(name: string): Promise<void> {
    const client = this.clients.get(name);
    if (!client) throw new Error(`MCP server '${name}' not found`);

    const config = this.configs.get(name);
    const maxRetries = 3;
    let lastErr: unknown;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (config?.transport === 'http' && config.url) {
          await client.connectHTTP(config.url);
        } else if (config?.transport === 'sse' && config.url) {
          await client.connectSSE(config.url);
        } else {
          await client.connect();
        }

        this._lastUsedAt.set(name, Date.now());

        const tools = await client.listTools();
        for (const tool of tools) {
          const key = `${name}:${tool.name}`;
          const schema = { ...tool, description: this.truncateDescription(tool.description || '') };
          const existing = this.findToolByName(tool.name);
          if (existing && existing.serverName !== name) {
            coreLog('mcp', `tool name conflict: "${tool.name}" provided by both "${existing.serverName}" and "${name}" — use qualified name "${key}" to disambiguate`);
          }
          this._allTools.set(key, { serverName: name, schema });
        }
        return;
      } catch (err) {
        lastErr = err;
        if (attempt < maxRetries && isTransientConnectError(err)) {
          const delay = 1000 * Math.pow(2, attempt);
          coreLog('mcp', `transient connect error for ${name} (attempt ${attempt + 1}/${maxRetries + 1}): ${err instanceof Error ? err.message : err}, retrying in ${delay}ms`);
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        throw err;
      }
    }
    throw lastErr;
  }

  async connectAll(): Promise<Map<string, Error | null>> {
    const results = new Map<string, Error | null>();
    const promises = Array.from(this.clients.keys()).map(async (name) => {
      try {
        await this.connectServer(name);
        results.set(name, null);
      } catch (err) {
        results.set(name, err instanceof Error ? err : new Error(String(err)));
      }
    });
    await Promise.all(promises);
    return results;
  }

  async callTool(qualifiedName: string, args: Record<string, unknown> = {}, options?: { signal?: AbortSignal }): Promise<unknown> {
    const colonIdx = qualifiedName.indexOf(':');
    if (colonIdx === -1) {
      const entry = this.findToolByName(qualifiedName);
      if (!entry) throw new Error(`MCP tool '${qualifiedName}' not found`);
      this._lastUsedAt.set(entry.serverName, Date.now());
      const client = this.clients.get(entry.serverName);
      if (!client) throw new Error(`MCP server '${entry.serverName}' not connected`);
      return client.callTool(qualifiedName, args, options);
    }

    const serverName = qualifiedName.slice(0, colonIdx);
    const toolName = qualifiedName.slice(colonIdx + 1);
    this._lastUsedAt.set(serverName, Date.now());
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server '${serverName}' not found`);
    return client.callTool(toolName, args, options);
  }

  async listResources(serverFilter?: string): Promise<Array<McpResource & { server: string }>> {
    const results: Array<McpResource & { server: string }> = [];
    for (const [name, client] of this.clients) {
      if (serverFilter && name !== serverFilter) continue;
      try {
        const resources = await client.listResources();
        for (const r of resources) {
          results.push({ ...r, server: name });
        }
      } catch (err) {
        if (err instanceof McpSessionExpiredError) {
          coreLog('mcp', `Session expired for ${name} during listResources, scheduling reconnect`);
          this.emit('session-expired', name);
        }
      }
    }
    return results;
  }

  async readResource(serverName: string, uri: string): Promise<unknown> {
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server "${serverName}" not connected`);
    try {
      return await client.readResource(uri);
    } catch (err) {
      if (err instanceof McpSessionExpiredError) {
        coreLog('mcp', `Session expired for ${serverName} during readResource`);
        this.emit('session-expired', serverName);
      }
      throw err;
    }
  }

  async listPrompts(serverFilter?: string): Promise<Array<McpPrompt & { server: string }>> {
    const results: Array<McpPrompt & { server: string }> = [];
    for (const [name, client] of this.clients) {
      if (serverFilter && name !== serverFilter) continue;
      if (client.state !== 'ready') continue;
      try {
        const prompts = await client.listPrompts();
        for (const p of prompts) {
          results.push({ ...p, server: name });
        }
      } catch {}
    }
    return results;
  }

  async getPrompt(serverName: string, promptName: string, args: Record<string, string> = {}): Promise<{ description?: string; messages: McpPromptMessage[] }> {
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server "${serverName}" not connected`);
    return client.getPrompt(promptName, args);
  }

  getAllTools(): Array<{ serverName: string; schema: McpToolSchema }> {
    return Array.from(this._allTools.values());
  }

  getToolConflicts(): Array<{ toolName: string; servers: string[] }> {
    const byName = new Map<string, string[]>();
    for (const entry of this._allTools.values()) {
      const list = byName.get(entry.schema.name) ?? [];
      list.push(entry.serverName);
      byName.set(entry.schema.name, list);
    }
    const conflicts: Array<{ toolName: string; servers: string[] }> = [];
    for (const [toolName, servers] of byName) {
      if (servers.length > 1) conflicts.push({ toolName, servers });
    }
    return conflicts;
  }

  getServerStatus(name: string): McpServerStatus | null {
    const client = this.clients.get(name);
    if (!client) return null;
    return {
      name,
      state: client.state,
      serverInfo: client.serverInfo,
      toolCount: client.tools.length,
      resourceCount: client.resources.length,
      uptime: client.uptime,
    };
  }

  listServers(): McpServerStatus[] {
    return Array.from(this.clients.keys()).map(name => this.getServerStatus(name)!);
  }

  startHealthChecks(intervalMs: number = 30000): void {
    if (this.healthCheckInterval) return;
    this.healthCheckInterval = setInterval(async () => {
      for (const [name, client] of this.clients) {
        if (client.state !== 'ready') continue;
        try {
          await client.ping();
        } catch (err) {
          coreLog('mcp', `Health check failed for ${name}, attempting reconnect`);
          this.scheduleReconnect(name);
        }
      }
    }, intervalMs);
    this.healthCheckInterval.unref();
  }

  stopHealthChecks(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  getToolAnnotations(qualifiedName: string): { readOnlyHint?: boolean; destructiveHint?: boolean; openWorldHint?: boolean } {
    const entry = this._allTools.get(qualifiedName) ?? this.findToolByName(qualifiedName);
    if (entry?.schema.annotations) {
      const a = entry.schema.annotations;
      return {
        readOnlyHint: a.readOnlyHint ?? undefined,
        destructiveHint: a.destructiveHint ?? undefined,
        openWorldHint: a.openWorldHint ?? undefined,
      };
    }

    const toolName = qualifiedName.includes(':') ? qualifiedName.slice(qualifiedName.indexOf(':') + 1) : qualifiedName;
    const readOnlyPrefixes = ['get', 'list', 'search', 'find', 'read', 'view', 'show', 'describe', 'fetch', 'query'];
    const destructivePrefixes = ['delete', 'remove', 'drop', 'destroy', 'purge', 'clear', 'truncate'];
    const openWorldPrefixes = ['web', 'fetch', 'http', 'curl', 'download', 'upload', 'send', 'post'];

    const lower = toolName.toLowerCase();
    const isReadOnly = readOnlyPrefixes.some(p => lower.startsWith(p));
    const isDestructive = destructivePrefixes.some(p => lower.startsWith(p));
    const isOpenWorld = openWorldPrefixes.some(p => lower.startsWith(p));

    return {
      readOnlyHint: isReadOnly || undefined,
      destructiveHint: isDestructive || undefined,
      openWorldHint: isOpenWorld || undefined,
    };
  }

  truncateDescription(description: string, maxLength: number = getMcpMaxDescriptionLength()): string {
    if (description.length <= maxLength) return description;
    return description.slice(0, maxLength - 3) + '...';
  }

  startIdleChecker(intervalMs = 60_000): void {
    this.stopIdleChecker();
    this._idleCheckTimer = setInterval(() => {
      this._evictIdleServers();
    }, intervalMs);
    this._idleCheckTimer.unref();
  }

  stopIdleChecker(): void {
    if (this._idleCheckTimer !== null) {
      clearInterval(this._idleCheckTimer);
      this._idleCheckTimer = null;
    }
  }

  _evictIdleServers(): string[] {
    const now = Date.now();
    const evicted: string[] = [];
    for (const [name] of this.clients) {
      const lastUsed = this._lastUsedAt.get(name);
      if (lastUsed !== undefined && now - lastUsed > this._idleTimeoutMs) {
        coreLog('mcp', `Idle timeout: stopping server '${name}' (idle ${Math.round((now - lastUsed) / 1000)}s)`);
        this.stopServer(name).catch(() => {});
        this._lastUsedAt.delete(name);
        evicted.push(name);
      }
    }
    return evicted;
  }

  disconnectAll(): void {
    this.stopHealthChecks();
    this.stopIdleChecker();
    for (const timer of this.reconnectTimers.values()) {
      clearTimeout(timer);
    }
    this.reconnectTimers.clear();
    for (const client of this.clients.values()) {
      client.disconnect();
    }
    this._allTools.clear();
    this.pendingServers.clear();
    this.serverErrors.clear();
    this._lastUsedAt.clear();
    this._deniedServers = [];
  }

  loadFromConfig(configPath: string): void {
    if (!fs.existsSync(configPath)) return;
    try {
      const raw = fs.readFileSync(configPath, 'utf-8');
      const stripped = raw.replace(/^\s*\/\/.*$/gm, '');
      const config = JSON.parse(stripped);
      const servers = config.mcpServers ?? config;
      if (typeof servers !== 'object') return;

      for (const [name, serverConfig] of Object.entries(servers)) {
        const cfg = serverConfig as McpServerConfig;
        if (!cfg.command && !cfg.url) continue;
        const expanded = this.expandConfigEnv(cfg);
        this.addServer(name, expanded);
      }
    } catch {
      // Invalid config — skip silently
    }
  }

  private expandConfigEnv(config: McpServerConfig): McpServerConfig {
    const result = { ...config };

    if (result.command) {
      result.command = expandEnvVars(result.command);
    }
    if (result.args) {
      result.args = result.args.map(a => expandEnvVars(a));
    }
    if (result.url) {
      result.url = expandEnvVars(result.url);
    }
    if (result.env) {
      const expanded: Record<string, string> = {};
      for (const [k, v] of Object.entries(result.env)) {
        expanded[k] = expandEnvVars(v);
      }
      result.env = expanded;
    }

    return result;
  }

  private invalidateToolCache(serverName: string): void {
    for (const [toolKey, entry] of this._allTools) {
      if (entry.serverName === serverName) this._allTools.delete(toolKey);
    }
  }

  private async refreshToolsForServer(name: string): Promise<void> {
    const client = this.clients.get(name);
    if (!client || client.state !== 'ready') return;

    try {
      const tools = await client.listTools();
      this.invalidateToolCache(name);
      for (const tool of tools) {
        const key = `${name}:${tool.name}`;
        const schema = { ...tool, description: this.truncateDescription(tool.description || '') };
        const existing = this.findToolByName(tool.name);
        if (existing && existing.serverName !== name) {
          coreLog('mcp', `tool name conflict: "${tool.name}" provided by both "${existing.serverName}" and "${name}" — use qualified name "${key}" to disambiguate`);
        }
        this._allTools.set(key, { serverName: name, schema });
      }
    } catch (err) {
      coreLog('mcp', `Failed to refresh tools for ${name}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  private scheduleReconnect(name: string): void {
    const existingTimer = this.reconnectTimers.get(name);
    if (existingTimer) {
      clearTimeout(existingTimer);
    }

    const client = this.clients.get(name);
    if (!client) return;

    const timer = setTimeout(async () => {
      this.reconnectTimers.delete(name);
      try {
        await client.reconnect();
        coreLog('mcp', `Reconnected to ${name}`);
        await this.refreshToolsForServer(name);
      } catch (err) {
        coreLog('mcp', `Reconnect failed for ${name}: ${err instanceof Error ? err.message : String(err)}`);
      }
    }, 1000);
    timer.unref();

    this.reconnectTimers.set(name, timer);
  }

  private findToolByName(name: string): { serverName: string; schema: McpToolSchema } | undefined {
    for (const entry of this._allTools.values()) {
      if (entry.schema.name === name) return entry;
    }
    return undefined;
  }

}

export const MAX_MCP_TOOLS_PER_SERVER = 200;
export const MAX_MCP_TOOL_NAME_LENGTH = 128;

const ALLOWED_SCHEMA_KEYS = new Set([
  'type', 'properties', 'required', 'additionalProperties',
  'description', 'title', '$schema', 'definitions', '$defs',
  'allOf', 'anyOf', 'oneOf', 'not', 'if', 'then', 'else',
  'enum', 'const', 'default', 'examples',
]);

export function validateMcpToolSchema(schema: unknown): { valid: boolean; error?: string } {
  if (schema === null || schema === undefined) {
    return { valid: false, error: 'Schema is null or undefined' };
  }
  if (typeof schema !== 'object' || Array.isArray(schema)) {
    return { valid: false, error: 'Schema must be a non-array object' };
  }
  const s = schema as Record<string, unknown>;
  if (s.type !== 'object') {
    return { valid: false, error: `Schema root type must be 'object', got '${String(s.type)}'` };
  }
  if (s.properties !== undefined) {
    if (typeof s.properties !== 'object' || s.properties === null || Array.isArray(s.properties)) {
      return { valid: false, error: 'Schema properties must be a non-array object' };
    }
  }
  if (s.required !== undefined) {
    if (!Array.isArray(s.required)) {
      return { valid: false, error: 'Schema required must be an array' };
    }
    for (const item of s.required) {
      if (typeof item !== 'string') {
        return { valid: false, error: `Schema required items must be strings, got ${typeof item}` };
      }
    }
    if (s.properties !== undefined) {
      const props = s.properties as Record<string, unknown>;
      for (const key of s.required as string[]) {
        if (!(key in props)) {
          return { valid: false, error: `Required key '${key}' is not defined in properties` };
        }
      }
    }
  }
  return { valid: true };
}

export function sanitizeMcpToolSchema(schema: unknown): object {
  const validation = validateMcpToolSchema(schema);
  if (!validation.valid) {
    return { type: 'object', properties: {} };
  }
  const s = schema as Record<string, unknown>;
  const cleaned: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(s)) {
    if (ALLOWED_SCHEMA_KEYS.has(key)) {
      cleaned[key] = value;
    }
  }
  return cleaned;
}

export function expandEnvVars(value: string): string {
  return value.replace(/\$\{([^}]+)\}|\$([A-Z_][A-Z0-9_]*)/gi, (_, braced, bare) => {
    const name = braced || bare;
    return process.env[name] ?? '';
  });
}

export function findMcpConfigPaths(cwd: string): string[] {
  const paths: string[] = [];
  const homeDir = process.env.HOME ?? process.env.USERPROFILE ?? '';

  const projectConfig = path.join(cwd, '.superinference', 'mcp.json');
  if (fs.existsSync(projectConfig)) paths.push(projectConfig);

  const globalConfig = path.join(homeDir, '.superinference', 'mcp.json');
  if (fs.existsSync(globalConfig)) paths.push(globalConfig);

  return paths;
}
