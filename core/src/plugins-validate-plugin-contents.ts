// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { isSafeSegment } from './skill-catalog';
import { parseFrontmatter } from './memory';
import { validateLspServerConfig } from './lsp/config';
import { hardenedResolve, resolvePluginHooksFile } from './plugins-command-definition';
import type { CommandDefinition, PluginCommandsMetadataContext } from './plugins-command-definition';
import { SKILL_MD_RE, applyCommandsMetadata, commandBaseName, hasExt, isDuplicateComponentPath, parseCommandFile, parseTomlString } from './plugins-derive-command-description';
import type { CommandScanDiagnostics } from './plugins-derive-command-description';
import type { PluginValidationIssue, PluginValidationResult } from './plugins-plugin-user-config-option';
import { safeReaddir, validateMarkdownComponent, validatePluginHooks } from './plugins-validate-plugin-option-values';
import type { PluginError } from './plugins-validate-plugin-option-values';

/**
 * Surface a command-directory scan failure through the caller's
 * {@link CommandScanDiagnostics} (the reference parity: the `Failed to scan ${label}
 * directory ${dir}` debug log). A no-op when no `onScanError` sink is wired, so the
 * default (diagnostics-less) load stays silent exactly as before.
 */
export function reportScanError(dir: string, error: unknown, diag: CommandScanDiagnostics | undefined): void {
  if (!diag?.onScanError) return;
  const label = diag.logLabel ?? 'plugin';
  diag.onScanError({ dir, error, label, message: `Failed to scan ${label} directory ${dir}: ${errMessage(error)}` });
}

export function walkCommandDir(dir: string, subPath: string[], pluginName: string | undefined, out: CommandDefinition[], loaded: Set<string>, diag?: CommandScanDiagnostics): void {
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    // Missing/unreadable dir — nothing to add. Emit a diagnostic (the reference parity:
    // walkPluginMarkdown's debug log) so one bad directory is traceable instead of
    // a silent no-op, then continue the load.
    reportScanError(dir, err, diag);
    return;
  }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      walkCommandDir(full, [...subPath, entry.name], pluginName, out, loaded, diag);
      continue;
    }
    if (!entry.isFile()) continue;
    const base = commandBaseName(entry.name);
    if (base === null) continue;
    // Realpath dedup: skip a file already loaded via another source/symlink
    // (the reference parity: isDuplicatePath) so it never yields a duplicate command.
    if (isDuplicateComponentPath(full, loaded)) continue;
    // The looked-up `name` keeps its original composition: optional plugin-name
    // segment, then the sub-path, then the base. The `namespace` field tracks ONLY
    // the on-disk sub-path (the reference parity: walkPluginMarkdown), so the plugin-name
    // prefix is not conflated with the folder structure.
    const nameSegs = [...(pluginName ? [pluginName] : []), ...subPath, base];
    const name = nameSegs.join(':');
    const cmd = parseCommandFile(full, name);
    if (cmd) {
      cmd.namespace = subPath;
      // Provenance defaults to 'plugin' for a bundle walk (a pluginName was given)
      // unless the file already declared an explicit source. The human bundle name
      // is stamped separately (the reference parity: pluginInfo.pluginManifest.name) so a
      // UI can annotate the command's origin with the actual plugin name.
      if (pluginName) {
        cmd.pluginName = pluginName;
        if (cmd.source === undefined) cmd.source = 'plugin';
      }
      out.push(cmd);
    }
  }
}

/**
 * Load command definitions from a plugin's commands/ directory, recursing into
 * nested subdirectories (the reference parity). Each `.toml` or `.md` file defines a
 * slash command; a prompt is required (it is what the command runs — the TOML
 * `prompt` key or the markdown body), while `description` is optional and falls
 * back to {@link DEFAULT_COMMAND_DESCRIPTION}. A command in a subdirectory is
 * namespaced by its sub-path with `:` separators (e.g. `commands/git/commit.toml`
 * → `git:commit`). When `pluginName` is given, it is prepended as the first
 * segment so command names are unique across plugins (`pluginName:git:commit`).
 *
 * `extraPaths` (the reference parity: a manifest's `commandsPaths`) are ADDITIONAL command
 * locations outside the primary `commands/` dir — each entry may be a directory
 * (walked recursively) or a single command file (`.toml`/`.md`), discovered under
 * the same namespacing rules. They are loaded AFTER `dir` so a duplicate name from
 * an extra path does not shadow the canonical command (callers dedup by name).
 *
 * `diagnostics` (the reference parity: walkPluginMarkdown's `logLabel` + swallowed-with-a-
 * debug-log scan failure) lets a caller trace a bad/unreadable command directory —
 * a readdir/stat failure is reported through `onScanError` (labelled by `logLabel`,
 * default `'plugin'`) instead of silently contributing zero commands.
 */
export function loadCommands(dir: string, pluginName?: string, extraPaths?: string[], metadata?: PluginCommandsMetadataContext, diagnostics?: CommandScanDiagnostics): CommandDefinition[] {
  const commands: CommandDefinition[] = [];
  const plugin = pluginName && pluginName.trim() !== '' ? pluginName.trim() : undefined;
  // Tracks the canonical realpath of every file loaded across ALL sources (the
  // primary dir + every extra path) so a file reachable through more than one
  // source is loaded exactly once (the reference parity: loadPluginCommands' loadedPaths).
  const loaded = new Set<string>();
  walkCommandDir(dir, [], plugin, commands, loaded, diagnostics);
  for (const extra of extraPaths ?? []) {
    let stat: fs.Stats;
    try {
      stat = fs.statSync(extra);
    } catch (err) {
      reportScanError(extra, err, diagnostics); // missing/unreadable extra path — trace + skip (the reference parity)
      continue;
    }
    if (stat.isDirectory()) {
      walkCommandDir(extra, [], plugin, commands, loaded, diagnostics);
    } else if (stat.isFile() && commandBaseName(path.basename(extra)) !== null && !isDuplicateComponentPath(extra, loaded)) {
      const base = commandBaseName(path.basename(extra))!;
      const name = [...(plugin ? [plugin] : []), base].join(':');
      const cmd = parseCommandFile(extra, name);
      if (cmd) {
        cmd.namespace = [];
        if (plugin) {
          cmd.pluginName = plugin;
          if (cmd.source === undefined) cmd.source = 'plugin';
        }
        commands.push(cmd);
      }
    }
  }
  if (metadata) applyCommandsMetadata(commands, metadata);
  return commands;
}

/**
 * Validate every inline LSP server config in a manifest `lspServers` record
 * (`name → config`) through the canonical {@link validateLspServerConfig} (the
 * SINGLE source of LSP-config validation, so the manifest linter and the loader
 * can never disagree). A thrown validation error — e.g. a command that carries
 * its arguments inline, or a file-extension key missing its leading dot — is
 * captured as a labelled {@link PluginValidationIssue} so the author-facing
 * `/plugin validate` surfaces the same corrective, example-bearing message the
 * loader would hit.
 */
export function validateInlineLspServers(label: string, rec: Record<string, unknown>, errors: PluginValidationIssue[]): void {
  for (const [serverName, cfg] of Object.entries(rec)) {
    try {
      validateLspServerConfig(serverName, cfg);
    } catch (err) {
      errors.push({ path: `${label}.${serverName}`, message: errMessage(err) });
    }
  }
}

/**
 * Validate a plugin's CONTENT files (skills/, agents/, commands/, hooks.json)
 * under `pluginDir`, complementing {@link validatePluginManifest} (which only
 * checks plugin.json). Mirrors the reference's `validatePluginContents`: every
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
        // SKILL.md is matched case-insensitively (the reference parity), so a subdir
        // shipping `skill.md`/`Skill.md` on a case-sensitive filesystem is not
        // spuriously reported as missing its manifest.
        const skillMdName = safeReaddir(entryPath).find(f => SKILL_MD_RE.test(f));
        if (skillMdName === undefined) {
          warnings.push({ path: `skills/${entry}`, message: 'skill subdirectory has no SKILL.md and will be ignored.' });
          continue;
        }
        validateMarkdownComponent(path.join(entryPath, skillMdName), `skills/${entry}/${skillMdName}`, true, errors, warnings);
      } else if (hasExt(entry, '.md')) {
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
      if (!hasExt(entry, '.md')) continue;
      validateMarkdownComponent(path.join(agentsDir, entry), `agents/${entry}`, false, errors, warnings);
    }
  }

  // commands/ — each *.toml must define description + prompt.
  const commandsDir = hardenedResolve(pluginDir, 'commands');
  if (commandsDir === undefined && fs.existsSync(path.join(pluginDir, 'commands'))) {
    errors.push({ path: 'commands', message: 'commands/ path escapes the plugin directory (symlink or ..).' });
  } else if (commandsDir) {
    for (const entry of safeReaddir(commandsDir)) {
      const isMd = hasExt(entry, '.md');
      if (!hasExt(entry, '.toml') && !isMd) continue;
      const file = path.join(commandsDir, entry);
      let content: string;
      try { content = fs.readFileSync(file, 'utf-8'); } catch (err) {
        errors.push({ path: `commands/${entry}`, message: `Failed to read: ${err instanceof Error ? err.message : String(err)}` });
        continue;
      }
      if (isMd) {
        // Markdown command: the body IS the prompt (the reference format), so a non-empty
        // body is what makes the command runnable; description is optional.
        if (!parseFrontmatter(content).body.trim()) {
          errors.push({ path: `commands/${entry}`, message: 'markdown command must have a non-empty body (the prompt).' });
        }
      } else if (!parseTomlString(content, 'description') || !parseTomlString(content, 'prompt')) {
        errors.push({ path: `commands/${entry}`, message: 'command TOML must define both "description" and "prompt".' });
      }
    }
    // Affordance warning (the reference parity: loadPluginCommands warns when a commands
    // directory yields nothing) — a commands/ dir that loads no runnable command
    // (recursively) is almost always an author mistake, so name the expected
    // format instead of silently contributing zero commands.
    if (loadCommands(commandsDir).length === 0) {
      warnings.push({ path: 'commands', message: 'commands/ directory has no loadable commands — expected .toml or .md command files (optionally nested in subdirectories).' });
    }
  }

  // hooks.json (root) OR hooks/hooks.json (the reference) — must be valid JSON AND
  // structurally sound (the reference parity: PluginHooksSchema). A bad hooks.json hard-breaks
  // the plugin at load time, so both the JSON syntax and the schema are linted here.
  const hooksFile = resolvePluginHooksFile(pluginDir);
  if (hooksFile) {
    const rel = path.relative(pluginDir, hooksFile);
    let parsedHooks: unknown;
    try {
      parsedHooks = JSON.parse(fs.readFileSync(hooksFile, 'utf-8'));
    } catch (err) {
      errors.push({ path: rel, message: `Invalid hooks JSON: ${err instanceof Error ? err.message : String(err)}` });
      parsedHooks = undefined;
    }
    if (parsedHooks !== undefined) {
      for (const issue of validatePluginHooks(parsedHooks)) {
        errors.push({ path: `${rel}:${issue.path}`, message: issue.message });
      }
    }
  }

  // .mcp.json (the reference) — bundled MCP servers must be valid JSON; a symlink/..
  // escape is flagged exactly like the other component paths.
  const mcpFile = hardenedResolve(pluginDir, '.mcp.json');
  if (mcpFile === undefined && fs.existsSync(path.join(pluginDir, '.mcp.json'))) {
    errors.push({ path: '.mcp.json', message: '.mcp.json path escapes the plugin directory (symlink or ..).' });
  } else if (mcpFile) {
    try {
      JSON.parse(fs.readFileSync(mcpFile, 'utf-8'));
    } catch (err) {
      errors.push({ path: '.mcp.json', message: `Invalid MCP servers JSON: ${err instanceof Error ? err.message : String(err)}` });
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/**
 * A plugin directory that failed to load, with an actionable reason. `error`
 * carries the TYPED {@link PluginError} (the reference parity) so a consumer can branch on
 * the failure kind instead of string-matching `reason`; `reason` is its
 * human-readable rendering (via {@link getPluginErrorMessage} for typed failures)
 * and remains the display fallback.
 */
export interface PluginLoadFailure {
  root: string;
  reason: string;
  error?: PluginError;
}

/** Matches any non-ASCII (potential homograph) character — a plugin name used for
 *  `plugin:command` namespacing must stay ASCII to avoid lookalike impersonation
 *  (the reference parity: NON_ASCII_PATTERN, utils/plugins/schemas.ts). */
export const NON_ASCII_NAME_RE = /[^ -~]/;

/**
 * True when `name` is safe to use verbatim as a plugin's namespace segment: it
 * passes the path-safety segment check (no slashes, `..`, or NUL), contains no
 * whitespace, and is ASCII-only (the reference parity: the plugin-name schema refinement
 * rejects spaces/path separators, and the homograph guard rejects non-ASCII).
 * A manifest name that fails this is not fatal — discovery falls back to the
 * directory name so the bundle still loads under a usable, namespacing-safe id.
 */
export function isNamespaceSafeName(name: string): boolean {
  return isSafeSegment(name) && !/\s/.test(name) && !NON_ASCII_NAME_RE.test(name);
}

/**
 * True when `err` is a filesystem "no such file or directory" error (errno code
 * `ENOENT`), matching the reference's `isENOENT` guard (utils/errors.ts). Used to
 * distinguish an EXPECTED-absent path (a plugin that simply ships none of an
 * optional component — swallow it) from a REAL I/O error (EACCES/ENOTDIR/… —
 * surface it) so a genuine misconfiguration is not silently discarded.
 */
export function isENOENT(err: unknown): boolean {
  return !!err && typeof err === 'object' && (err as NodeJS.ErrnoException).code === 'ENOENT';
}

/** Compact, non-throwing message for a caught error (used in load-failure reasons). */
export function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Sanitize one segment of a plugin/extension id (the reference parity: the `sanitize`
 * pipeline in utils/dxt/helpers.ts `generateExtensionId`), applied IN ORDER:
 * lower-case → collapse whitespace runs to `-` → drop any char outside
 * `[a-z0-9-_.]` → collapse `-` runs → trim leading/trailing `-`. Deterministic:
 * the same input always yields the same segment.
 */
export function sanitizePluginIdSegment(str: string): string {
  return str
    .toLowerCase()
    .replace(/\s+/g, '-')
    .replace(/[^a-z0-9-_.]/g, '')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/** Prefixes a deterministic plugin id may carry (the reference parity: local unpacked vs
 *  packaged `.dxt`/`.mcpb` bundle). */
export const PLUGIN_ID_PREFIXS = ['local.unpacked', 'local.dxt'] as const;

export type PluginIdPrefix = (typeof PLUGIN_ID_PREFIXS)[number];

/**
 * Generate a deterministic plugin/extension id from an author name and a plugin
 * name (the reference parity: generateExtensionId, utils/dxt/helpers.ts). Each part is
 * run through {@link sanitizePluginIdSegment}; an optional `prefix` is prepended.
 * The result is `${author}.${name}` (or `${prefix}.${author}.${name}`) — stable
 * across runs so the same plugin always resolves to the same id.
 */
export function generatePluginId(authorName: string, name: string, prefix?: PluginIdPrefix): string {
  const author = sanitizePluginIdSegment(authorName);
  const sanitizedName = sanitizePluginIdSegment(name);
  return prefix ? `${prefix}.${author}.${sanitizedName}` : `${author}.${sanitizedName}`;
}

/**
 * Normalize a plugin's git-subdir path EXACTLY as the reference's version/cache-key
 * encoder does (the reference parity: calculatePluginVersion's git-subdir branch,
 * pluginVersioning.ts L63-88), byte-for-byte: (1) backslash → forward slash, (2)
 * strip ONE leading `./`, (3) strip ALL trailing `/`. The result is what gets
 * hashed into a git-subdir plugin's version, so the normalization must stay
 * identical across every producer of that version (no second spelling).
 */
export function normalizePluginSubdirPath(subdirPath: string): string {
  return subdirPath
    .replace(/\\/g, '/')
    .replace(/^\.\//, '')
    .replace(/\/+$/, '');
}

/**
 * Hash a plugin's git-subdir path into the short digest the reference folds into a
 * git-subdir plugin's version/cache key (the reference parity: calculatePluginVersion
 * L63-88 — `createHash('sha256').update(normPath).digest('hex').substring(0, 8)`).
 * Two plugins living at different subdirs of the SAME monorepo commit share a
 * SHA, so without this suffix they collide at `cache/<m>/<p>/<sha>/` and serve
 * each other's trees; the path hash disambiguates them. The path is normalized
 * via {@link normalizePluginSubdirPath} first so the digest is stable across
 * `\`/`/`, a `./` prefix, and trailing slashes. Pure + deterministic.
 */
export function hashPluginSubdirPath(subdirPath: string): string {
  const norm = normalizePluginSubdirPath(subdirPath);
  return crypto.createHash('sha256').update(norm).digest('hex').substring(0, 8);
}

/**
 * Normalize a manifest `author` (string or `{ name, … }` object — the reference parity)
 * to a non-empty display-name string, or undefined when absent/blank/malformed.
 */
export function manifestAuthorName(author: unknown): string | undefined {
  if (typeof author === 'string') return author.trim() !== '' ? author.trim() : undefined;
  if (author && typeof author === 'object' && !Array.isArray(author)) {
    const name = (author as Record<string, unknown>).name;
    if (typeof name === 'string' && name.trim() !== '') return name.trim();
  }
  return undefined;
}

/**
 * A plugin-contributed MCP server declaration keyed by name. The dedup key is its
 * launch target — a stdio `command` (+ args) or a `url` — so two plugins shipping
 * the same server (or one duplicating an already-configured server) collapse to one.
 */
export interface PluginMcpServer {
  command?: string;
  args?: string[];
  url?: string;
  [k: string]: unknown;
}
