import * as fs from 'fs';
import * as path from 'path';

/**
 * A discovered plugin bundle: a directory with a `.superinference-plugin/plugin.json`
 * manifest that contributes skills/, agents/, and hooks.json to the registries.
 */
export interface PluginBundle {
  name: string;
  version: string;
  root: string;
  skillsDir?: string;
  agentsDir?: string;
  hooksFile?: string;
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
 * Discover local plugin bundles under `dir`. Each immediate subdirectory with a
 * `.superinference-plugin/plugin.json` manifest is a bundle; its skills/agents/
 * hooks component paths are path-traversal hardened to its own root.
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
      const manifestPath = path.join(root, '.superinference-plugin', 'plugin.json');
      if (!fs.existsSync(manifestPath)) continue;
      const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf-8')) as { name?: string; version?: string };
      bundles.push({
        name: manifest.name ?? entry,
        version: manifest.version ?? '0.0.0',
        root,
        skillsDir: hardenedResolve(root, 'skills'),
        agentsDir: hardenedResolve(root, 'agents'),
        hooksFile: hardenedResolve(root, 'hooks.json'),
      });
    } catch {
      // Skip a malformed bundle rather than failing all discovery.
    }
  }
  return bundles;
}
