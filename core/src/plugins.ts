import * as fs from 'fs';
import * as path from 'path';

/**
 * A discovered plugin bundle: a directory with a `.superinference-plugin/plugin.json`
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
}

/**
 * A slash-command definition loaded from a TOML file in a plugin's commands/ dir.
 */
export interface CommandDefinition {
  name: string;
  description: string;
  prompt: string;
  filePath: string;
}

/**
 * Resolve a component path within a plugin root, rejecting any path that escapes
 * the root via `..` or a symlink (realpath containment check). Returns the path
 * only if it exists and stays inside the plugin — otherwise null.
 */
function hardenedResolve(root: string, rel: string): string | undefined {
  const target = path.join(root, rel);
  try {
    const realTarget = fs.realpathSync(target);
    const realRoot = fs.realpathSync(root);
    if (realTarget === realRoot || realTarget.startsWith(realRoot + path.sep)) return target;
    return undefined; // escapes the plugin root — reject
  } catch {
    return undefined; // does not exist / unreadable
  }
}

/**
 * Minimal TOML string parser for the simple `key = "value"` format used by
 * command definition files. Handles backslash escapes (\n, \t, \\, \").
 */
function parseTomlString(content: string, key: string): string | undefined {
  const lines = content.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const prefix = key + ' = "';
    const idx = line.indexOf(prefix);
    if (idx !== -1) {
      const value = line.slice(idx + prefix.length);
      let result = '';
      let escaped = false;
      for (const ch of value) {
        if (escaped) {
          if (ch === 'n') result += '\n';
          else if (ch === 't') result += '\t';
          else result += ch;
          escaped = false;
        } else if (ch === '\\') {
          escaped = true;
        } else if (ch === '"') {
          return result;
        } else {
          result += ch;
        }
      }
      return result; // No closing quote found, return what we have
    }
  }
  return undefined;
}

/**
 * Load TOML command definitions from a plugin's commands/ directory.
 * Each `.toml` file defines a slash command with `description` and `prompt` fields.
 */
export function loadCommands(dir: string): CommandDefinition[] {
  let entries: string[];
  try {
    entries = fs.readdirSync(dir).filter(f => f.endsWith('.toml'));
  } catch {
    return [];
  }
  const commands: CommandDefinition[] = [];
  for (const file of entries) {
    const filePath = path.join(dir, file);
    try {
      const content = fs.readFileSync(filePath, 'utf-8');
      const name = path.basename(file, '.toml');
      const desc = parseTomlString(content, 'description');
      const prompt = parseTomlString(content, 'prompt');
      if (desc && prompt) {
        commands.push({ name, description: desc, prompt, filePath });
      }
    } catch {
      // Skip malformed command files
    }
  }
  return commands;
}

/**
 * Discover local plugin bundles under `dir`. Each immediate subdirectory with a
 * `.superinference-plugin/plugin.json` manifest (or `.claude-plugin/plugin.json`
 * fallback) is a bundle; its skills/agents/commands/hooks component paths are
 * path-traversal hardened to its own root.
 */
export function discoverPlugins(dir: string): PluginBundle[] {
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return [];
  }
  const bundles: PluginBundle[] = [];
  for (const entry of entries) {
    const root = path.join(dir, entry);
    try {
      if (!fs.statSync(root).isDirectory()) continue;
      // Claude Code compatibility: check .claude-plugin/plugin.json as fallback
      let manifestPath = path.join(root, '.superinference-plugin', 'plugin.json');
      if (!fs.existsSync(manifestPath)) {
        manifestPath = path.join(root, '.claude-plugin', 'plugin.json');
      }
      if (!fs.existsSync(manifestPath)) continue;
      const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8')) as { name?: string; version?: string };
      bundles.push({
        name: manifest.name ?? entry,
        version: manifest.version ?? '0.0.0',
        root,
        skillsDir: hardenedResolve(root, 'skills'),
        agentsDir: hardenedResolve(root, 'agents'),
        commandsDir: hardenedResolve(root, 'commands'),
        hooksFile: hardenedResolve(root, 'hooks.json'),
      });
    } catch {
      // Skip a malformed bundle rather than failing all discovery.
    }
  }
  return bundles;
}
