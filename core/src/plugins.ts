import * as fs from 'fs';
import * as path from 'path';
import { isSafeSegment } from './skill-catalog';
import { parseFrontmatter } from './memory';

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
 * Redact embedded credentials from any URL(s) inside a string so a clone source
 * or a git error message can be logged/surfaced without leaking a token or
 * password. Rewrites the `user:pass@` (and bare `user@`) userinfo of every
 * `scheme://…` URL to `***`. The SINGLE canonical redactor for the installer and
 * the marketplace manager (no per-site re-implementation).
 */
export function redactUrlCredentials(text: string): string {
  if (typeof text !== 'string' || text === '') return text;
  // Match scheme://userinfo@host — replace only the userinfo portion.
  return text.replace(/([a-zA-Z][a-zA-Z0-9+.-]*:\/\/)([^/@\s]+)@/g, '$1***@');
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
 * Beyond the required name/description/prompt, a command may declare the same
 * behavioral knobs as a Claude Code command (all optional).
 */
export interface CommandDefinition {
  name: string;
  description: string;
  prompt: string;
  filePath: string;
  /** Hint text for the argument string (e.g. "<file-path>"). */
  argumentHint?: string;
  /** Tool allowlist this command's prompt may use. */
  allowedTools?: string[];
  /** Model override for running the command. */
  model?: string;
  /** Thinking effort level for the command. */
  effort?: string;
  /** Trigger hint for auto/model invocation. */
  whenToUse?: string;
  /** Command version string. */
  version?: string;
  /** If true, the model may not invoke this command via the skill/command tool. */
  disableModelInvocation?: boolean;
  /** If false, the user may not invoke it via /name (default true). */
  userInvocable?: boolean;
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
        const cmd: CommandDefinition = { name, description: desc, prompt, filePath };
        // Optional behavioral knobs (Claude-parity). Each is omitted when absent.
        const argumentHint = parseTomlString(content, 'argument-hint');
        if (argumentHint) cmd.argumentHint = argumentHint;
        const model = parseTomlString(content, 'model');
        if (model) cmd.model = model;
        const effort = parseTomlString(content, 'effort');
        if (effort) cmd.effort = effort;
        const whenToUse = parseTomlString(content, 'when-to-use') ?? parseTomlString(content, 'when_to_use');
        if (whenToUse) cmd.whenToUse = whenToUse;
        const version = parseTomlString(content, 'version');
        if (version) cmd.version = version;
        const allowedTools = parseTomlStringArray(content, 'allowed-tools');
        if (allowedTools) cmd.allowedTools = allowedTools;
        const disableModelInvocation = parseTomlBool(content, 'disable-model-invocation');
        if (disableModelInvocation !== undefined) cmd.disableModelInvocation = disableModelInvocation;
        const userInvocable = parseTomlBool(content, 'user-invocable');
        if (userInvocable !== undefined) cmd.userInvocable = userInvocable;
        commands.push(cmd);
      }
    } catch {
      // Skip malformed command files
    }
  }
  return commands;
}

/** Parse a TOML `key = true|false` boolean, or undefined when absent/other. */
function parseTomlBool(content: string, key: string): boolean | undefined {
  const m = content.match(new RegExp(`^\\s*${key.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\$&')}\\s*=\\s*(true|false)\\b`, 'm'));
  return m ? m[1] === 'true' : undefined;
}

/** Parse a TOML `key = ["a", "b"]` string array, or undefined when absent. */
function parseTomlStringArray(content: string, key: string): string[] | undefined {
  const m = content.match(new RegExp(`^\\s*${key.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\$&')}\\s*=\\s*\\[([^\\]]*)\\]`, 'm'));
  if (!m) return undefined;
  const inner = m[1]!.trim();
  if (!inner) return [];
  return inner
    .split(',')
    .map(s => s.trim().replace(/^["']|["']$/g, ''))
    .filter(s => s.length > 0);
}

// ---------------------------------------------------------------------------
// Plugin manifest validation — a dependency-free validator for a plugin.json
// (and the component paths it may declare). Surfaces structured errors (hard
// failures) + warnings (author foot-guns) with actionable guidance, including
// path-traversal flagging for any `..` in a declared component path.
// ---------------------------------------------------------------------------

/**
 * Installable plugin scopes, in Claude-Code order. `user` installs into the
 * cross-project global plugins dir; `project` into the project's committed
 * `.ami/plugins`; `local` into the project dir but intended to stay un-committed
 * (local-only). Default is `user`.
 */
export const VALID_INSTALL_SCOPES = ['user', 'project', 'local'] as const;
export type InstallScope = (typeof VALID_INSTALL_SCOPES)[number];

/** Whether `scope` is a valid install scope (for arg validation at the CLI edge). */
export function isInstallScope(scope: string): scope is InstallScope {
  return (VALID_INSTALL_SCOPES as readonly string[]).includes(scope);
}

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

/**
 * Validate a plugin's CONTENT files (skills/, agents/, commands/, hooks.json)
 * under `pluginDir`, complementing {@link validatePluginManifest} (which only
 * checks plugin.json). Mirrors Claude Code's `validatePluginContents`: every
 * declared component is structurally checked and any component path that escapes
 * the plugin root (via `..` or a symlink) is flagged. Returns aggregated
 * errors/warnings labelled by the offending relative path. Missing component dirs
 * are fine (a plugin need not ship every kind).
 */
export function validatePluginContents(pluginDir: string): PluginValidationResult {
  const errors: PluginValidationIssue[] = [];
  const warnings: PluginValidationIssue[] = [];

  // skills/ — subdir form needs a SKILL.md with a name; flat *.md needs a body.
  const skillsDir = hardenedResolve(pluginDir, 'skills');
  if (skillsDir === undefined && fs.existsSync(path.join(pluginDir, 'skills'))) {
    errors.push({ path: 'skills', message: 'skills/ path escapes the plugin directory (symlink or ..).' });
  } else if (skillsDir) {
    for (const entry of safeReaddir(skillsDir)) {
      const entryPath = path.join(skillsDir, entry);
      let isDir = false;
      try { isDir = fs.statSync(entryPath).isDirectory(); } catch { continue; }
      if (isDir) {
        const skillMd = path.join(entryPath, 'SKILL.md');
        if (!fs.existsSync(skillMd)) {
          warnings.push({ path: `skills/${entry}`, message: 'skill subdirectory has no SKILL.md and will be ignored.' });
          continue;
        }
        validateMarkdownComponent(skillMd, `skills/${entry}/SKILL.md`, true, errors, warnings);
      } else if (entry.endsWith('.md')) {
        validateMarkdownComponent(entryPath, `skills/${entry}`, true, errors, warnings);
      }
    }
  }

  // agents/ — each *.md needs a non-empty description + body.
  const agentsDir = hardenedResolve(pluginDir, 'agents');
  if (agentsDir === undefined && fs.existsSync(path.join(pluginDir, 'agents'))) {
    errors.push({ path: 'agents', message: 'agents/ path escapes the plugin directory (symlink or ..).' });
  } else if (agentsDir) {
    for (const entry of safeReaddir(agentsDir)) {
      if (!entry.endsWith('.md')) continue;
      validateMarkdownComponent(path.join(agentsDir, entry), `agents/${entry}`, false, errors, warnings);
    }
  }

  // commands/ — each *.toml must define description + prompt.
  const commandsDir = hardenedResolve(pluginDir, 'commands');
  if (commandsDir === undefined && fs.existsSync(path.join(pluginDir, 'commands'))) {
    errors.push({ path: 'commands', message: 'commands/ path escapes the plugin directory (symlink or ..).' });
  } else if (commandsDir) {
    for (const entry of safeReaddir(commandsDir)) {
      if (!entry.endsWith('.toml')) continue;
      const file = path.join(commandsDir, entry);
      let content: string;
      try { content = fs.readFileSync(file, 'utf-8'); } catch (err) {
        errors.push({ path: `commands/${entry}`, message: `Failed to read: ${err instanceof Error ? err.message : String(err)}` });
        continue;
      }
      if (!parseTomlString(content, 'description') || !parseTomlString(content, 'prompt')) {
        errors.push({ path: `commands/${entry}`, message: 'command TOML must define both "description" and "prompt".' });
      }
    }
  }

  // hooks.json (root) OR hooks/hooks.json (Claude Code) — must be valid JSON.
  const hooksFile = resolvePluginHooksFile(pluginDir);
  if (hooksFile) {
    try {
      JSON.parse(fs.readFileSync(hooksFile, 'utf-8'));
    } catch (err) {
      errors.push({ path: path.relative(pluginDir, hooksFile), message: `Invalid hooks JSON: ${err instanceof Error ? err.message : String(err)}` });
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/** readdir that returns [] instead of throwing on a missing/unreadable dir. */
function safeReaddir(dir: string): string[] {
  try { return fs.readdirSync(dir); } catch { return []; }
}

/** Validate a markdown component file has YAML frontmatter with a name (skills)
 *  or description (agents) and a non-empty body. */
function validateMarkdownComponent(
  file: string,
  label: string,
  isSkill: boolean,
  errors: PluginValidationIssue[],
  warnings: PluginValidationIssue[],
): void {
  let raw: string;
  try { raw = fs.readFileSync(file, 'utf-8'); } catch (err) {
    errors.push({ path: label, message: `Failed to read: ${err instanceof Error ? err.message : String(err)}` });
    return;
  }
  const { frontmatter, body } = parseFrontmatter(raw);
  if (!body.trim()) {
    errors.push({ path: label, message: 'component body is empty.' });
  }
  if (isSkill) {
    // A subdir SKILL.md can fall back to the dir name, so a missing name is only a warning.
    if (!frontmatter.name && path.basename(file) !== 'SKILL.md') {
      warnings.push({ path: label, message: 'skill has no "name" frontmatter; the file name will be used.' });
    }
  } else if (!frontmatter.description?.trim()) {
    warnings.push({ path: label, message: 'agent has no "description" frontmatter.' });
  }
}

/**
 * Full plugin validation: manifest + content files. Reads `plugin.json` from the
 * `.ami-plugin`/`.claude-plugin` dir under `pluginDir`, runs
 * {@link validatePluginManifest} and {@link validatePluginContents}, and merges
 * the results. A missing manifest is a hard error.
 */
export function validatePlugin(pluginDir: string): PluginValidationResult {
  for (const marker of ['.ami-plugin', '.claude-plugin']) {
    const manifestPath = path.join(pluginDir, marker, 'plugin.json');
    if (!fs.existsSync(manifestPath)) continue;
    let manifestResult: PluginValidationResult;
    try {
      manifestResult = validatePluginManifest(JSON.parse(fs.readFileSync(manifestPath, 'utf-8')));
    } catch (err) {
      manifestResult = { valid: false, errors: [{ path: manifestPath, message: `Invalid plugin.json: ${err instanceof Error ? err.message : String(err)}` }], warnings: [] };
    }
    const contentResult = validatePluginContents(pluginDir);
    return {
      valid: manifestResult.valid && contentResult.valid,
      errors: [...manifestResult.errors, ...contentResult.errors],
      warnings: [...manifestResult.warnings, ...contentResult.warnings],
    };
  }
  return { valid: false, errors: [{ path: pluginDir, message: 'No plugin manifest (.ami-plugin/plugin.json or .claude-plugin/plugin.json).' }], warnings: [] };
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
      // CORRUPT (unparseable JSON) is reported distinctly from INVALID (parses but
      // fails the schema) so a front-end can guide the author appropriately.
      failures.push({ root, reason: `Corrupt plugin.json (JSON parse failed): ${err instanceof Error ? err.message : String(err)}. Fix the JSON syntax in ${manifestPath}.` });
      continue;
    }
    // Security-relevant manifest errors (unsafe name, '..'/absolute component path)
    // turn a discovered dir into a structured failure rather than a silently-loaded
    // bundle. Lenient fields (missing name/version) still fall back below.
    const hardErrors = validatePluginManifest(manifest).errors.filter(e =>
      /Unsafe plugin name|contains "\.\."|must be relative/.test(e.message),
    );
    if (hardErrors.length > 0) {
      failures.push({ root, reason: `Invalid plugin.json: ${hardErrors.map(e => `${e.path}: ${e.message}`).join('; ')}` });
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
