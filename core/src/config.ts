import * as fs from 'fs';
import * as path from 'path';
import { log as coreLog } from './logger';

/**
 * Project-level configuration that can be stored in
 * `.superinference/config.json` (project) or `~/.superinference/config.json`
 * (global).  The file format is JSONC — single-line (//) and block comments
 * are stripped before parsing.
 *
 * Priority chain (highest to lowest):
 *   CLI args  >  env vars  >  project config  >  global config
 */
export interface ProjectConfig {
  model?: string;
  provider?: 'openai' | 'anthropic' | 'anthropic-vertex' | 'google' | 'ollama' | 'openrouter';
  baseUrl?: string;
  permissionMode?: 'ask' | 'auto-allow' | 'deny-all';
  thinkingLevel?: string;
  fallbackModel?: string;
  compactionModel?: string;
  maxTurns?: number;
  tokenBudget?: number;
  persona?: string;
}

// ---------------------------------------------------------------------------
// JSONC helpers
// ---------------------------------------------------------------------------

/**
 * Strip single-line (//) and block comments from a JSONC string so that
 * it can be parsed by the standard JSON.parse.
 *
 * Uses a context-aware parser to avoid corrupting URLs or other content
 * inside JSON string literals.
 */
export function stripJsoncComments(input: string): string {
  let result = '';
  let i = 0;
  let inString = false;
  let escape = false;
  while (i < input.length) {
    const ch = input[i];
    if (escape) { result += ch; escape = false; i++; continue; }
    if (inString) {
      if (ch === '\\') { escape = true; result += ch; i++; continue; }
      if (ch === '"') { inString = false; }
      result += ch; i++; continue;
    }
    if (ch === '"') { inString = true; result += ch; i++; continue; }
    if (ch === '/' && i + 1 < input.length) {
      if (input[i + 1] === '/') {
        // Skip to end of line
        while (i < input.length && input[i] !== '\n') i++;
        continue;
      }
      if (input[i + 1] === '*') {
        i += 2;
        while (i < input.length - 1 && !(input[i] === '*' && input[i + 1] === '/')) i++;
        i += 2;
        continue;
      }
    }
    result += ch; i++;
  }
  return result;
}

// ---------------------------------------------------------------------------
// Loaders
// ---------------------------------------------------------------------------

/**
 * Load a project-level config from `<cwd>/.superinference/config.json`.
 * Returns `null` when the file does not exist.
 */
export function loadProjectConfig(cwd: string): ProjectConfig | null {
  const configPath = path.join(cwd, '.superinference', 'config.json');
  return loadConfigFile(configPath);
}

/**
 * Load the global config from `~/.superinference/config.json`.
 * Returns `null` when the file does not exist.
 */
export function loadGlobalConfig(): ProjectConfig | null {
  const home = process.env.HOME || process.env.USERPROFILE || '';
  if (!home) return null;
  const configPath = path.join(home, '.superinference', 'config.json');
  return loadConfigFile(configPath);
}

function loadConfigFile(configPath: string): ProjectConfig | null {
  try {
    if (!fs.existsSync(configPath)) return null;
    const raw = fs.readFileSync(configPath, 'utf-8');
    const stripped = stripJsoncComments(raw);
    const parsed = JSON.parse(stripped) as ProjectConfig;
    coreLog('config', `loaded ${configPath}`);
    return parsed;
  } catch (err) {
    coreLog('config', `failed to load ${configPath}: ${err}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// Merge
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ConfigService — hot-reload config watcher
// ---------------------------------------------------------------------------

export type ConfigChangeListener = (config: ProjectConfig, prev: ProjectConfig) => void;

export class ConfigService {
  private config: ProjectConfig;
  private configPath: string;
  private watcher: fs.FSWatcher | null = null;
  private listeners: ConfigChangeListener[] = [];
  private debounceTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(cwd: string) {
    this.configPath = path.join(cwd, '.superinference', 'config.json');
    this.config = loadConfigFile(this.configPath) ?? {};
  }

  get(): ProjectConfig {
    return { ...this.config };
  }

  onChange(listener: ConfigChangeListener): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter(l => l !== listener);
    };
  }

  watch(): void {
    if (this.watcher) return;

    const dir = path.dirname(this.configPath);
    try {
      if (!fs.existsSync(dir)) return;

      this.watcher = fs.watch(dir, (eventType, filename) => {
        if (filename !== 'config.json') return;

        if (this.debounceTimer) clearTimeout(this.debounceTimer);
        this.debounceTimer = setTimeout(() => {
          this.reload();
        }, 100);
      });

      this.watcher.on('error', () => {
        this.stop();
      });
    } catch {
      // Directory doesn't exist or not watchable
    }
  }

  reload(): boolean {
    const prev = this.config;
    const next = loadConfigFile(this.configPath);
    if (!next) return false;

    const changed = JSON.stringify(prev) !== JSON.stringify(next);
    if (changed) {
      this.config = next;
      for (const listener of this.listeners) {
        try { listener(next, prev); } catch (err) { coreLog('config', 'config listener error', { error: String(err) }); }
      }
    }
    return changed;
  }

  stop(): void {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    if (this.watcher) {
      this.watcher.close();
      this.watcher = null;
    }
  }
}

/**
 * Merge configuration sources in priority order.
 *
 * Priority (highest first):
 *   1. `cliArgs`       — flags passed on the command line
 *   2. `envVars`       — values derived from environment variables
 *   3. `projectConfig` — `.superinference/config.json` in the project
 *   4. `globalConfig`  — `~/.superinference/config.json`
 *
 * Only defined (non-undefined) values participate in the merge.
 */
export function mergeConfigs(
  cliArgs: Partial<ProjectConfig>,
  envVars: Partial<ProjectConfig>,
  projectConfig: ProjectConfig | null,
  globalConfig: ProjectConfig | null,
): ProjectConfig {
  const layers: Array<Partial<ProjectConfig>> = [
    globalConfig ?? {},
    projectConfig ?? {},
    envVars,
    cliArgs,
  ];

  const merged: ProjectConfig = {};

  for (const layer of layers) {
    if (layer.model !== undefined) merged.model = layer.model;
    if (layer.provider !== undefined) merged.provider = layer.provider;
    if (layer.baseUrl !== undefined) merged.baseUrl = layer.baseUrl;
    if (layer.permissionMode !== undefined) merged.permissionMode = layer.permissionMode;
    if (layer.thinkingLevel !== undefined) merged.thinkingLevel = layer.thinkingLevel;
    if (layer.fallbackModel !== undefined) merged.fallbackModel = layer.fallbackModel;
    if (layer.compactionModel !== undefined) merged.compactionModel = layer.compactionModel;
    if (layer.maxTurns !== undefined) merged.maxTurns = layer.maxTurns;
    if (layer.tokenBudget !== undefined) merged.tokenBudget = layer.tokenBudget;
    if (layer.persona !== undefined) merged.persona = layer.persona;
  }

  return merged;
}
