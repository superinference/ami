import * as fs from 'fs';
import * as path from 'path';
import { isSafeSegment } from './skill-catalog';

// ---------------------------------------------------------------------------
// Git source safety — SHARED validators reused by the plugin installer AND the
// marketplace manager (common/) so there is a SINGLE source-format gate for
// every `git clone`. Blocks argument injection, transport-helper RCE, and
// non-allowlisted URL schemes; provides the fail-closed clone environment and
// a `--`-terminated argv builder.
// ---------------------------------------------------------------------------

/**
 * Environment that prevents `git` from hanging on, or being hijacked by,
 * interactive credential / host-key prompts. Spread over `process.env` for every
 * clone/fetch so an untrusted source can never stall the process waiting on stdin
 * nor pop an askpass GUI. Fail-closed: no prompt ⇒ the clone just fails.
 */
export const GIT_NO_PROMPT_ENV = {
  GIT_TERMINAL_PROMPT: '0', // no terminal credential prompt
  GIT_ASKPASS: '', // disable askpass GUI programs
  GIT_SSH_COMMAND: 'ssh -oBatchMode=yes', // ssh never prompts (host key / password)
} as const;

export interface GitSourceValidation {
  ok: boolean;
  reason?: string;
}

/** scp-style SSH remote, e.g. `git@github.com:owner/repo.git`. */
const SSH_SCP_RE = /^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+:.+/;
/** GitHub shorthand `owner/repo` (single separator, no scheme, no host). */
const GITHUB_SHORTHAND_RE = /^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/;
/** A git transport-helper prefix, e.g. `ext::`, `fd::`, `file::` — these can run
 *  arbitrary programs and MUST be rejected. Matches `<scheme>::` but NOT a normal
 *  URL `<scheme>://` (single colon before the slashes). */
const TRANSPORT_HELPER_RE = /^[a-zA-Z][a-zA-Z0-9+.-]*::/;
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

/**
 * Split a `url#ref` source into its clone URL and optional branch/tag ref so a
 * source can be pinned to a specific ref (e.g. `owner/repo#v1.2.0`). A leading
 * `#` or empty fragment yields no ref.
 */
export function parseGitRef(source: string): { url: string; ref?: string } {
  const hash = source.indexOf('#');
  if (hash > 0) {
    const ref = source.slice(hash + 1).trim();
    return { url: source.slice(0, hash), ref: ref || undefined };
  }
  return { url: source };
}

/**
 * Build the argv for a shallow `git clone`, always inserting `--` before the
 * untrusted url/dest so they can never be misinterpreted as flags, and pinning
 * to `ref` via `--branch` when one is given (works for branches and tags).
 */
export function buildGitCloneArgs(url: string, dest: string, ref?: string): string[] {
  const args = ['clone', '--depth', '1'];
  if (ref) args.push('--branch', ref);
  args.push('--', url, dest);
  return args;
}

/**
 * Resolve `relativeOrName` under `baseDir` and assert the result stays inside
 * `baseDir` (a trailing-separator compare so `/foo/bar` can't pass for
 * `/foo/barbaz`). Throws on any `..`/absolute escape. Used before every
 * destructive filesystem op on a caller-supplied plugin/marketplace path.
 */
export function assertPathWithinBase(baseDir: string, relativeOrName: string): string {
  const resolved = path.resolve(baseDir, relativeOrName);
  const base = path.resolve(baseDir);
  if (resolved !== base && !resolved.startsWith(base + path.sep)) {
    throw new Error(
      `Path traversal detected: "${relativeOrName}" would escape "${baseDir}".`,
    );
  }
  return resolved;
}

/**
 * Resolve the install directory for a single-segment plugin `name` under
 * `pluginsDir`, rejecting unsafe segments (`.`, `..`, slashes, NUL, overlong)
 * AND double-checking containment. Returns the absolute dir. Throws on any
 * unsafe name so name-based destructive ops (remove/disable/enable) can never
 * be tricked into touching a path outside the plugins dir.
 */
export function resolvePluginDir(pluginsDir: string, name: string): string {
  if (!isSafeSegment(name)) {
    throw new Error(`Unsafe plugin name "${name}".`);
  }
  return assertPathWithinBase(pluginsDir, name);
}

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
 * A plugin's hooks file is at <root>/hooks.json (AMI's original layout) or
 * <root>/hooks/hooks.json (the Claude Code convention — used by the impeccable
 * plugin). Prefer the root file, fall back to the subdir. hardenedResolve returns
 * undefined for a nonexistent/escaping path, so this naturally yields the first
 * one that actually exists (or undefined).
 */
function resolvePluginHooksFile(root: string): string | undefined {
  return hardenedResolve(root, 'hooks.json') ?? hardenedResolve(root, path.join('hooks', 'hooks.json'));
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

// ---------------------------------------------------------------------------
// Plugin manifest validation — a dependency-free validator for a plugin.json
// (and the component paths it may declare). Surfaces structured errors (hard
// failures) + warnings (author foot-guns) with actionable guidance, including
// path-traversal flagging for any `..` in a declared component path.
// ---------------------------------------------------------------------------

export interface PluginValidationIssue {
  path: string;
  message: string;
}

export interface PluginValidationResult {
  valid: boolean;
  errors: PluginValidationIssue[];
  warnings: PluginValidationIssue[];
}

/** Fields that belong in a marketplace entry, not plugin.json — author confusion. */
const MARKETPLACE_ONLY_MANIFEST_FIELDS = ['category', 'source', 'tags', 'strict', 'id'];
/** Optional component-path fields a manifest may declare. */
const COMPONENT_PATH_FIELDS = ['skills', 'agents', 'commands', 'hooks'];

/**
 * Validate a parsed plugin manifest object. Checks required `name`/`version`
 * strings, flags any declared component path that contains `..` (would escape
 * the plugin dir) or is absolute, and warns about marketplace-only fields that
 * are silently ignored at runtime.
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
  }

  if (obj.version !== undefined && typeof obj.version !== 'string') {
    errors.push({ path: 'version', message: '"version" must be a string.' });
  }

  for (const field of COMPONENT_PATH_FIELDS) {
    const val = obj[field];
    if (typeof val !== 'string') continue;
    if (path.isAbsolute(val)) {
      errors.push({ path: field, message: `Component path "${val}" must be relative to the plugin root.` });
    } else if (val.split(/[/\\]/).includes('..')) {
      errors.push({ path: field, message: `Component path "${val}" contains ".." and would escape the plugin directory.` });
    }
  }

  for (const field of MARKETPLACE_ONLY_MANIFEST_FIELDS) {
    if (field in obj) {
      warnings.push({ path: field, message: `"${field}" belongs in marketplace.json, not plugin.json — it is ignored here.` });
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/** A plugin directory that failed to load, with an actionable reason. */
export interface PluginLoadFailure {
  root: string;
  reason: string;
}

/**
 * Discover local plugin bundles under `dir`, ALSO returning structured load
 * failures (invalid JSON / failed manifest validation) so callers can surface
 * actionable guidance instead of silently dropping a broken plugin. A directory
 * with no manifest at all is not a failure (it simply isn't a plugin).
 */
export function discoverPluginsDetailed(dir: string): { bundles: PluginBundle[]; failures: PluginLoadFailure[] } {
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch {
    return { bundles: [], failures: [] };
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
    // Claude Code compatibility: check .claude-plugin/plugin.json as fallback
    let manifestPath = path.join(root, '.ami-plugin', 'plugin.json');
    if (!fs.existsSync(manifestPath)) {
      manifestPath = path.join(root, '.claude-plugin', 'plugin.json');
    }
    if (!fs.existsSync(manifestPath)) continue; // not a plugin dir — not a failure
    let manifest: Record<string, unknown>;
    try {
      const parsed = JSON.parse(fs.readFileSync(manifestPath, 'utf-8')) as unknown;
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        failures.push({ root, reason: `Invalid plugin.json: manifest must be a JSON object. Check ${manifestPath}.` });
        continue;
      }
      manifest = parsed as Record<string, unknown>;
    } catch (err) {
      failures.push({ root, reason: `Invalid plugin.json: ${err instanceof Error ? err.message : String(err)}. Fix the JSON syntax in ${manifestPath}.` });
      continue;
    }
    // Discovery is lenient on optional fields (name/version fall back below); the
    // strict schema check is exposed separately via validatePluginManifest for
    // the `/plugin validate` command.
    bundles.push({
      name: (manifest.name as string | undefined) ?? entry,
      version: (manifest.version as string | undefined) ?? '0.0.0',
      root,
      skillsDir: hardenedResolve(root, 'skills'),
      agentsDir: hardenedResolve(root, 'agents'),
      commandsDir: hardenedResolve(root, 'commands'),
      // Hooks live at <root>/hooks.json OR, per the Claude Code convention
      // (used by e.g. the impeccable plugin), <root>/hooks/hooks.json.
      hooksFile: resolvePluginHooksFile(root),
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
