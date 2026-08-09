import { McpClient, McpToolSchema, McpResource, McpPrompt, McpPromptMessage, McpClientState, McpTerminalError, McpSessionExpiredError } from './client';
import { EventEmitter } from 'events';
import * as fs from 'fs';
import * as path from 'path';
import { log as coreLog } from '../logger';

export interface McpServerConfig {
  command: string;
  args?: string[];
  env?: Record<string, string>;
  requestTimeout?: number;
  connectTimeout?: number;
  autoConnect?: boolean;
  transport?: 'stdio' | 'sse' | 'http';
  url?: string;
}

export interface McpServerStatus {
  name: string;
  state: McpClientState;
  serverInfo: { name: string; version: string } | null;
  toolCount: number;
  resourceCount: number;
  uptime: number | null;
}

export class McpManager extends EventEmitter {
  private clients = new Map<string, McpClient>();
  private configs = new Map<string, McpServerConfig>();
  private _allTools = new Map<string, { serverName: string; schema: McpToolSchema }>();
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private rootPaths: string[] = [];
  private reconnectTimers = new Map<string, ReturnType<typeof setTimeout>>();

  setRootPaths(paths: string[]): void {
    this.rootPaths = paths;
  }

  addServer(name: string, config: McpServerConfig): void {
    if (this.clients.has(name)) {
      throw new Error(`MCP server '${name}' already registered`);
    }
    this.configs.set(name, config);
    const client = new McpClient({
      command: config.command || '',
      args: config.args,
      env: config.env,
      requestTimeout: config.requestTimeout,
      connectTimeout: config.connectTimeout,
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
    this.configs.delete(name);
    this.invalidateToolCache(name);
  }

  async connectServer(name: string): Promise<void> {
    const client = this.clients.get(name);
    if (!client) throw new Error(`MCP server '${name}' not found`);

    const config = this.configs.get(name);
    if (config?.transport === 'http' && config.url) {
      await client.connectHTTP(config.url);
    } else if (config?.transport === 'sse' && config.url) {
      await client.connectSSE(config.url);
    } else {
      await client.connect();
    }

    const tools = await client.listTools();
    for (const tool of tools) {
      const key = `${name}:${tool.name}`;
      const schema = { ...tool, description: this.truncateDescription(tool.description || '') };
      this._allTools.set(key, { serverName: name, schema });
    }
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
      const client = this.clients.get(entry.serverName);
      if (!client) throw new Error(`MCP server '${entry.serverName}' not connected`);
      return client.callTool(qualifiedName, args, options);
    }

    const serverName = qualifiedName.slice(0, colonIdx);
    const toolName = qualifiedName.slice(colonIdx + 1);
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server '${serverName}' not found`);
    return client.callTool(toolName, args, options);
  }

  async listResources(serverFilter?: string): Promise<Array<{uri: string; name?: string; mimeType?: string; server: string}>> {
    const results: Array<{uri: string; name?: string; mimeType?: string; server: string}> = [];
    for (const [name, client] of this.clients) {
      if (serverFilter && name !== serverFilter) continue;
      try {
        const resources = await client.listResources();
        for (const r of resources) {
          results.push({ ...r, server: name });
        }
      } catch {}
    }
    return results;
  }

  async readResource(serverName: string, uri: string): Promise<unknown> {
    const client = this.clients.get(serverName);
    if (!client) throw new Error(`MCP server "${serverName}" not connected`);
    return client.readResource(uri);
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

  truncateDescription(description: string, maxLength: number = 2048): string {
    if (description.length <= maxLength) return description;
    return description.slice(0, maxLength - 3) + '...';
  }

  disconnectAll(): void {
    this.stopHealthChecks();
    for (const timer of this.reconnectTimers.values()) {
      clearTimeout(timer);
    }
    this.reconnectTimers.clear();
    for (const client of this.clients.values()) {
      client.disconnect();
    }
    this._allTools.clear();
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
