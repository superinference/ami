// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { escapeRegExp } from './utils/string';
import { parseFrontmatter, parseFrontmatterList } from './memory';
import { parseCommandShell } from './plugins-is-embedded-shell-command-allowed';
import type { CommandMetadata } from './plugins-is-embedded-shell-command-allowed';
import { COMMAND_SOURCES, COMMAND_TYPES, hardenedResolve, unescapeTomlBasic } from './plugins-command-definition';
import type { CommandDefinition, CommandSource, CommandType, PluginCommandsMetadataContext } from './plugins-command-definition';

/**
 * Minimal TOML string parser for the `key = "value"` format used by command
 * definition files. Beyond the original single-line basic string it is an
 * ADDITIVE SUPERSET that matches real TOML / the reference command files:
 *  - the key must start the line (after optional indentation) so a value is never
 *    picked up from inside another line;
 *  - full-line `#` comments are ignored (a commented-out `key = "…"` no longer
 *    shadows the real assignment below it);
 *  - multi-line basic strings `key = """ … """` are supported (the newline that
 *    immediately follows the opening delimiter is trimmed, per the TOML spec),
 *    so a multi-line `prompt`/`description` is no longer parsed as empty and
 *    silently dropped.
 * Backslash escapes (\n, \t, \\, \") are decoded in both forms.
 */
export function parseTomlString(content: string, key: string): string | undefined {
  const lines = content.split('\n');
  const keyRe = new RegExp(
    '^\\s*' + escapeRegExp(key) + '\\s*=\\s*(.*)$',
  );
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!;
    if (line.trimStart().startsWith('#')) continue; // full-line TOML comment
    const m = line.match(keyRe);
    if (!m) continue;
    const rest = m[1]!;
    // Multi-line basic string: key = """ … """ (possibly closing on the same line).
    if (rest.startsWith('"""')) {
      const opener = rest.slice(3);
      const sameClose = opener.indexOf('"""');
      if (sameClose !== -1) return unescapeTomlBasic(opener.slice(0, sameClose), false);
      const collected: string[] = [];
      // TOML trims a newline immediately after the opening delimiter; keep any
      // text that trails """ on the opening line.
      if (opener !== '') collected.push(opener);
      for (let j = i + 1; j < lines.length; j++) {
        const l = lines[j]!;
        const close = l.indexOf('"""');
        if (close !== -1) {
          collected.push(l.slice(0, close));
          return unescapeTomlBasic(collected.join('\n'), false);
        }
        collected.push(l);
      }
      return undefined; // unterminated multi-line string → treat as absent
    }
    // Single-line basic string: key = "value".
    if (rest.startsWith('"')) return unescapeTomlBasic(rest.slice(1), true);
    return undefined; // not a quoted-string value
  }
  return undefined;
}

/**
 * Labelled fallback description for a command whose TOML omits `description`.
 * the reference parity: a command is identified by its (required) prompt body; the
 * description is a user-facing affordance only, so a missing one falls back to a
 * generic label rather than silently dropping the whole command.
 */
export const DEFAULT_COMMAND_DESCRIPTION = 'Plugin command';

/**
 * Derive a user-facing command description from its markdown body / TOML prompt
 * when no explicit `description` is declared (the reference parity:
 * `extractDescriptionFromMarkdown`, utils/markdownConfigLoader.ts). Returns the
 * first non-empty line with any leading markdown header prefix (`#`, `##`, …)
 * stripped, truncated to 100 characters (`...` suffix past 97). Falls back to
 * `fallback` only when `content` has no non-empty line — so a description-less
 * command reflects what it actually does instead of a generic label.
 */
export function deriveCommandDescription(content: string, fallback: string = DEFAULT_COMMAND_DESCRIPTION): string {
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (trimmed === '') continue;
    const headerMatch = trimmed.match(/^#+\s+(.+)$/);
    const text = headerMatch ? headerMatch[1]! : trimmed;
    return text.length > 100 ? text.slice(0, 97) + '...' : text;
  }
  return fallback;
}

/**
 * Apply the optional behavioral knobs (the reference-parity) onto `cmd`, reading each
 * from whichever format the command file is in. `getStr` returns a string knob by
 * key (TOML quoted value or markdown-frontmatter value); `getBool` a boolean;
 * `getList` a string array. Each knob is omitted from `cmd` when absent. A single
 * applier keeps the `.toml` and `.md` loaders in exact knob parity.
 */
export function applyCommandKnobs(
  cmd: CommandDefinition,
  getStr: (key: string) => string | undefined,
  getBool: (key: string) => boolean | undefined,
  getList: (key: string) => string[] | undefined,
): void {
  const argumentHint = getStr('argument-hint');
  if (argumentHint) cmd.argumentHint = argumentHint;
  const model = getStr('model');
  if (model) cmd.model = model;
  const effort = getStr('effort');
  if (effort) cmd.effort = effort;
  const whenToUse = getStr('when-to-use') ?? getStr('when_to_use');
  if (whenToUse) cmd.whenToUse = whenToUse;
  const version = getStr('version');
  if (version) cmd.version = version;
  const displayName = getStr('name');
  if (displayName) cmd.displayName = displayName;
  const allowedTools = getList('allowed-tools');
  if (allowedTools) cmd.allowedTools = allowedTools;
  const disableModelInvocation = getBool('disable-model-invocation');
  if (disableModelInvocation !== undefined) cmd.disableModelInvocation = disableModelInvocation;
  const userInvocable = getBool('user-invocable');
  if (userInvocable !== undefined) cmd.userInvocable = userInvocable;
  const progressMessage = getStr('progress-message') ?? getStr('progressMessage');
  if (progressMessage) cmd.progressMessage = progressMessage;
  const argNames = getList('arg-names') ?? getList('argNames');
  if (argNames) cmd.argNames = argNames;
  const kind = getStr('kind');
  if (kind) cmd.kind = kind;
  const aliases = getList('aliases');
  if (aliases) cmd.aliases = aliases;
  const supportsNonInteractive = getBool('supports-non-interactive') ?? getBool('supportsNonInteractive');
  if (supportsNonInteractive !== undefined) cmd.supportsNonInteractive = supportsNonInteractive;
  // Embedded-shell selector (the reference parity: parseShellFrontmatter) — the shell a
  // command's `!`-block execution runs in. Validated to COMMAND_SHELLS; an
  // unrecognized value degrades to undefined (caller defaults to bash).
  const shell = parseCommandShell(getStr('shell'));
  if (shell) cmd.shell = shell;
  // Immediate dispatch (the reference parity: Command.immediate) — run without the
  // deferred/confirm path. Declarative boolean, read via isImmediateCommand.
  const immediate = getBool('immediate') ?? getBool('is-immediate');
  if (immediate !== undefined) cmd.immediate = immediate;
  // Enablement / visibility / sensitivity gates (the reference parity: CommandBase
  // isEnabled/isHidden/isSensitive). Declarative booleans an author sets to ship a
  // command disabled, hidden from the catalog, or with its args redacted from history.
  const isEnabled = getBool('enabled') ?? getBool('is-enabled');
  if (isEnabled !== undefined) cmd.isEnabled = isEnabled;
  const isHidden = getBool('hidden') ?? getBool('is-hidden');
  if (isHidden !== undefined) cmd.isHidden = isHidden;
  const isSensitive = getBool('sensitive') ?? getBool('is-sensitive');
  if (isSensitive !== undefined) cmd.isSensitive = isSensitive;
  // Plugin recommendation (the reference parity: PluginHintMenu) — a command may suggest
  // installing a plugin. Both the plugin name AND the marketplace are required; a
  // command declaring only one of them produces no recommendation.
  const recPlugin = getStr('recommend-plugin') ?? getStr('recommend_plugin');
  const recMarketplace = getStr('recommend-marketplace') ?? getStr('recommend_marketplace');
  if (recPlugin && recMarketplace) {
    const recDesc = getStr('recommend-plugin-description') ?? getStr('recommend_plugin_description');
    cmd.recommendsPlugin = {
      pluginName: recPlugin,
      marketplaceName: recMarketplace,
      ...(recDesc ? { pluginDescription: recDesc } : {}),
    };
  }
  // Provenance/type discriminators: honor a declared value only when it is a
  // recognized enum member (an unknown string is ignored rather than written
  // through, so a consumer can trust the field's type).
  const source = getStr('source');
  if (source && (COMMAND_SOURCES as readonly string[]).includes(source)) cmd.source = source as CommandSource;
  const type = getStr('type');
  if (type && (COMMAND_TYPES as readonly string[]).includes(type)) cmd.type = type as CommandType;
}

/**
 * Parse a single `.toml` command file into a {@link CommandDefinition} with the
 * given (already namespaced) `name`, or undefined when the file has no `prompt`
 * (a command needs a prompt to do anything) or cannot be read. The description is
 * user-facing only and falls back to {@link DEFAULT_COMMAND_DESCRIPTION}.
 */
export function parseTomlCommandFile(content: string, filePath: string, name: string): CommandDefinition | undefined {
  const desc = parseTomlString(content, 'description');
  const prompt = parseTomlString(content, 'prompt');
  // A command needs a prompt to do anything; the description is user-facing
  // only, so a prompt-only command loads with a description DERIVED from its
  // prompt (the reference parity) instead of being silently discarded or labelled
  // generically.
  if (!prompt) return undefined;
  // A command FILE injects its body/prompt, so it is a `'prompt'` command (the reference
  // parity: Command.type) unless the file explicitly declares another type. Record
  // whether the description was author-specified (the reference parity:
  // hasUserSpecifiedDescription) so the model-facing listing can require an explicit
  // one for plugin/MCP commands (see {@link isCommandModelListable}).
  const cmd: CommandDefinition = { name, description: desc || deriveCommandDescription(prompt), prompt, filePath, type: 'prompt' };
  if (desc) cmd.hasUserSpecifiedDescription = true;
  applyCommandKnobs(
    cmd,
    key => parseTomlString(content, key),
    key => parseTomlBool(content, key),
    key => parseTomlStringArray(content, key),
  );
  return cmd;
}

/**
 * Parse a single markdown command file (the reference's native command format:
 * YAML frontmatter + a markdown body that IS the prompt) into a
 * {@link CommandDefinition}, or undefined when the body is empty / unreadable.
 * `allowed-tools` accepts the comma-separated or single-value frontmatter form;
 * booleans accept `true`/`false`. A missing description is DERIVED from the body's
 * first line via {@link deriveCommandDescription} (the reference parity).
 */
export function parseMarkdownCommandFile(content: string, filePath: string, name: string): CommandDefinition | undefined {
  const { frontmatter, body } = parseFrontmatter(content);
  const prompt = body.trim();
  if (!prompt) return undefined; // a command with no body does nothing — skip
  const desc = frontmatter.description?.trim();
  const cmd: CommandDefinition = { name, description: desc || deriveCommandDescription(prompt), prompt, filePath, type: 'prompt' };
  if (desc) cmd.hasUserSpecifiedDescription = true; // explicit vs derived (the reference parity)
  applyCommandKnobs(
    cmd,
    key => { const v = frontmatter[key]; return typeof v === 'string' && v.trim() !== '' ? v.trim() : undefined; },
    key => {
      const v = frontmatter[key]?.trim().toLowerCase();
      return v === 'true' ? true : v === 'false' ? false : undefined;
    },
    key => {
      const v = frontmatter[key];
      if (v === undefined) return undefined;
      const list = parseFrontmatterList(v);
      return list;
    },
  );
  return cmd;
}

/**
 * Parse a single command file into a {@link CommandDefinition} with the given
 * (already namespaced) `name`, dispatching on extension: `.toml` uses the TOML
 * `key = "value"` format; `.md` uses the reference's YAML-frontmatter + body format
 * (the body is the prompt). Returns undefined for an unreadable file or one with
 * no usable prompt.
 */
export function parseCommandFile(filePath: string, name: string): CommandDefinition | undefined {
  let content: string;
  try {
    content = fs.readFileSync(filePath, 'utf-8');
  } catch {
    return undefined; // unreadable command file — skip
  }
  return hasExt(filePath, '.md')
    ? parseMarkdownCommandFile(content, filePath, name)
    : parseTomlCommandFile(content, filePath, name);
}

/** Command file extensions AMI loads: TOML (AMI's original) and the reference's `.md`. */
export const COMMAND_FILE_EXTS = ['.toml', '.md'] as const;

/** Case-insensitive `.md`/SKILL.md matching (the reference parity: walkPluginMarkdown
 *  uses `.toLowerCase().endsWith('.md')` and `/^skill\.md$/i`), so a `Deploy.TOML`
 *  / `Review.MD` / `skill.md` is discovered the same as its canonical casing. */
export const SKILL_MD_RE = /^skill\.md$/i;

/** True when `name` ends with `ext` (lower-cased) regardless of the on-disk case. */
export function hasExt(name: string, ext: string): boolean {
  return name.toLowerCase().endsWith(ext);
}

/** Strip a recognized command-file extension (case-insensitively) from
 *  `fileName`, preserving the base name's original casing, or null if none. */
export function commandBaseName(fileName: string): string | null {
  for (const ext of COMMAND_FILE_EXTS) {
    if (hasExt(fileName, ext)) return fileName.slice(0, fileName.length - ext.length);
  }
  return null;
}

/**
 * Diagnostics for a command-directory scan (the reference parity: walkPluginMarkdown's
 * `logLabel` option + swallowed-with-a-debug-log readdir failure). Lets a caller
 * trace a bad/unreadable plugin directory instead of the load silently contributing
 * zero commands — core stays logger-free by surfacing the failure through a
 * callback the consumer (skill-manager / plugin-installer) wires to its logger.
 */
export interface CommandScanDiagnostics {
  /** Context label for scan-failure messages (the reference parity: logLabel, default `'plugin'`). */
  logLabel?: string;
  /**
   * Invoked once per directory that cannot be scanned (readdir/stat failure).
   * Receives a ready-to-log `message` plus the structured `dir`/`error`/`label` so
   * a consumer can either log the message verbatim or build its own.
   */
  onScanError?: (info: { dir: string; error: unknown; label: string; message: string }) => void;
}

/**
 * Recursively walk a commands directory, collecting every `.toml` or `.md`
 * command file. The `namespace` accumulates the sub-path from the commands root
 * to the file (the reference parity: `getCommandNameFromFile`), and `pluginName` — when
 * the walk was started for a named plugin bundle — is prepended as the top
 * namespace segment so command names are globally unique ACROSS plugins
 * (`${pluginName}:${namespace}:${base}`). Two bundles each shipping `deploy.toml`
 * therefore register as `pluginA:deploy` / `pluginB:deploy` instead of silently
 * colliding. A direct `loadCommands(dir)` with no plugin name keeps the bare
 * `namespace:base` form (back-compat for non-bundle callers/tests).
 * Symlinked directories are NOT followed (readdir Dirents report a symlink as
 * `isDirectory() === false`), which both prevents walk loops and keeps discovery
 * contained to the plugin's own tree — consistent with the realpath hardening
 * used everywhere else in this module.
 */
/**
 * Canonical-path dedup for a command/skill file already collected during a load
 * (the reference parity: fsOperations `isDuplicatePath`/`safeResolvePath` used by
 * loadPluginCommands). Resolves `filePath` to its realpath (falling back to the
 * raw path when it can't be resolved — ENOENT/broken symlink) and records it;
 * returns true when that canonical path was ALREADY loaded, so a file reachable
 * through two sources (e.g. the primary `commands/` dir and a manifest
 * `commandsPaths` entry, or a symlink pointing back into the tree) is loaded
 * exactly once instead of producing duplicate command definitions.
 */
export function isDuplicateComponentPath(filePath: string, loaded: Set<string>): boolean {
  let resolved: string;
  try {
    resolved = fs.realpathSync(filePath);
  } catch {
    resolved = filePath;
  }
  if (loaded.has(resolved)) return true;
  loaded.add(resolved);
  return false;
}

/**
 * Copy the overridable fields of a {@link CommandMetadata} entry onto `cmd`
 * (the reference parity: the metadata-override frontmatter merge in loadPluginCommands).
 * Each override is applied only when it is a non-empty string / non-empty array so
 * a blank override never blanks out the command's own value.
 */
export function applyCommandMetadataOverride(cmd: CommandDefinition, meta: CommandMetadata): void {
  // A manifest-declared `description` is author-specified (the reference parity:
  // hasUserSpecifiedDescription), so it also flips the explicit-description flag the
  // model-listing gate consults.
  if (typeof meta.description === 'string' && meta.description.trim() !== '') {
    cmd.description = meta.description;
    cmd.hasUserSpecifiedDescription = true;
  }
  if (typeof meta.argumentHint === 'string' && meta.argumentHint.trim() !== '') cmd.argumentHint = meta.argumentHint;
  if (typeof meta.model === 'string' && meta.model.trim() !== '') cmd.model = meta.model;
  if (Array.isArray(meta.allowedTools) && meta.allowedTools.length > 0) cmd.allowedTools = meta.allowedTools;
}

/**
 * Apply a plugin manifest's `commandsMetadata` object-mapping to an already-walked
 * command list (the reference parity: loadPluginCommands L516-668). Each entry is keyed by
 * the command name it defines and carries EXACTLY ONE of `source`/`content`:
 *  - `source`: resolve the file path (relative to the plugin root, hardened against
 *    `..`/symlink escape), match the discovered command loaded from it, rename that
 *    command to the metadata key, and apply the field overrides.
 *  - `content`: parse the inline markdown as a brand-new `'prompt'` command under the
 *    metadata key (no source file) and apply the field overrides.
 * An entry with both or neither of source/content is skipped (the schema rejects it).
 */
export function applyCommandsMetadata(commands: CommandDefinition[], ctx: PluginCommandsMetadataContext): void {
  for (const [key, meta] of Object.entries(ctx.entries)) {
    if (!meta || typeof meta !== 'object') continue;
    const hasSource = typeof meta.source === 'string' && meta.source.trim() !== '';
    const hasContent = typeof meta.content === 'string' && meta.content.trim() !== '';
    if (hasSource === hasContent) continue; // XOR (parity: CommandMetadataSchema.refine)
    if (hasSource) {
      const abs = hardenedResolve(ctx.root, meta.source!);
      if (abs === undefined) continue; // source escapes the root / does not exist
      const match = commands.find(c => path.resolve(c.filePath) === path.resolve(abs));
      if (match) {
        match.name = key;
        match.namespace = [];
        applyCommandMetadataOverride(match, meta);
      }
    } else {
      // Inline-content command: the markdown IS the command (the reference parity). A
      // virtual filePath marks it as having no on-disk source.
      const cmd = parseMarkdownCommandFile(meta.content!, `<inline:${key}>`, key);
      if (cmd) {
        cmd.namespace = [];
        cmd.source = 'plugin';
        applyCommandMetadataOverride(cmd, meta);
        commands.push(cmd);
      }
    }
  }
}

/** Parse a TOML `key = true|false` boolean, or undefined when absent/other. */
export function parseTomlBool(content: string, key: string): boolean | undefined {
  const m = content.match(new RegExp(`^\\s*${escapeRegExp(key)}\\s*=\\s*(true|false)\\b`, 'm'));
  return m ? m[1] === 'true' : undefined;
}

/** Parse a TOML `key = ["a", "b"]` string array, or undefined when absent. */
export function parseTomlStringArray(content: string, key: string): string[] | undefined {
  const m = content.match(new RegExp(`^\\s*${escapeRegExp(key)}\\s*=\\s*\\[([^\\]]*)\\]`, 'm'));
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
 * Installable plugin scopes, in the reference order. `user` installs into the
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
