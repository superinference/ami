// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import type { CommandMetadata, CommandShell, PluginRecommendation, PluginRecommendationHint } from './plugins-is-embedded-shell-command-allowed';

/**
 * A slash-command definition loaded from a TOML file in a plugin's commands/ dir.
 * Beyond the required name/description/prompt, a command may declare the same
 * behavioral knobs as a reference command (all optional).
 */
export interface CommandDefinition {
  name: string;
  description: string;
  prompt: string;
  filePath: string;
  /**
   * Whether {@link description} was EXPLICITLY authored (TOML `description` /
   * markdown-frontmatter `description`) versus auto-derived from the prompt/body's
   * first line via {@link deriveCommandDescription} (the reference parity:
   * Command.hasUserSpecifiedDescription). A derived description is a usable
   * affordance for the USER, but the model-facing listing of plugin/MCP commands
   * requires an author-specified one (see {@link isCommandModelListable}) so an
   * un-described plugin command is not silently advertised to the model. `true`
   * only when the file carried a non-empty description; omitted otherwise.
   */
  hasUserSpecifiedDescription?: boolean;
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
  /** User-facing display name (the reference's `name` frontmatter → `userFacingName()`),
   *  distinct from the namespaced lookup `name`. Omitted when unset. */
  displayName?: string;
  /**
   * Subdirectory path (relative to the commands root) this command was discovered
   * under, with each path segment as an element (the reference parity:
   * walkPluginMarkdown's `namespace` array). Empty for a top-level command.
   * Carried alongside `name` so a UI can group commands by their folder without
   * re-parsing the `:`-joined name — and distinct from `name` so the plugin-name
   * prefix is NOT conflated with the on-disk folder structure.
   */
  namespace?: string[];
  /**
   * User-facing in-progress hint shown while a prompt/skill command runs (the reference
   * parity: Command.progressMessage, e.g. "committing changes"). Purely an
   * affordance — omitted when the command file declares none.
   */
  progressMessage?: string;
  /**
   * Named positional arguments this command accepts (the reference parity:
   * Command.argNames). Surfaced in suggestion descriptions and mutually
   * exclusive with a single free-form {@link argumentHint}.
   */
  argNames?: string[];
  /**
   * Command kind tag (the reference parity: Command.kind, e.g. `'workflow'`). Drives the
   * suggestion `workflow` affordance; omitted for an ordinary command.
   */
  kind?: string;
  /**
   * Alternate invocation names for this command (the reference parity:
   * CommandBase.aliases) — each resolves to the same command as the primary name.
   */
  aliases?: string[];
  /**
   * Provenance of the command definition (the reference parity: Command.source). A
   * plugin-discovered command defaults to `'plugin'`; a bare-directory load leaves
   * it unset unless the file declares one.
   */
  source?: CommandSource;
  /**
   * The NAME of the plugin bundle this command was discovered in (the reference parity:
   * Command.pluginInfo.pluginManifest.name, consumed by formatDescriptionWithSource
   * to render `(<pluginName>) <description>` rather than a generic `(plugin)` tag).
   * Stamped during a bundle walk (when {@link loadCommands} is called with a plugin
   * name), SEPARATE from the `:`-namespaced lookup {@link name} so a UI can show the
   * human bundle name without re-parsing the prefix. `undefined` for a bare
   * (non-bundle) directory load.
   */
  pluginName?: string;
  /**
   * Command behavior discriminator (the reference parity: Command.type). Plugin/user
   * command FILES are always `'prompt'` commands (the body/`prompt` is injected);
   * the field is set so a consumer can branch without re-deriving it.
   */
  type?: CommandType;
  /**
   * Whether this command is usable from the non-interactive/detached runner
   * (the reference parity: Command.supportsNonInteractive). Omitted (treated as allowed)
   * unless the file opts out with `supports-non-interactive = false`.
   */
  supportsNonInteractive?: boolean;
  /**
   * Declarative enablement gate (the reference parity: CommandBase.isEnabled, a
   * `() => boolean` defaulting to true — set ONLY for conditional enablement such
   * as feature flags / env checks). AMI commands are data, so the dynamic predicate
   * reduces to a declared boolean: a command file sets `enabled = false` (or
   * `is-enabled = false`) to ship DISABLED — omitted from the user-facing command
   * list and never dispatched. Evaluated via {@link isCommandEnabled}; omitted
   * (treated as enabled) when the file declares nothing.
   */
  isEnabled?: boolean;
  /**
   * Hide this command from the typeahead/help catalog while keeping it invocable by
   * exact name (the reference parity: CommandBase.isHidden, defaults to false). A command
   * file opts in with `hidden = true` (or `is-hidden = true`). Evaluated via
   * {@link isCommandHidden}; omitted (treated as visible) otherwise.
   */
  isHidden?: boolean;
  /**
   * When true, this command's invocation ARGUMENTS are redacted from the persisted
   * conversation history (the reference parity: CommandBase.isSensitive — "args are
   * redacted from the conversation history"). A command file opts in with
   * `sensitive = true` (or `is-sensitive = true`). Applied via
   * {@link redactSensitiveCommandArgs}; omitted (treated as non-sensitive) otherwise.
   */
  isSensitive?: boolean;
  /**
   * A plugin this command recommends installing (the reference parity: PluginHintMenu). A
   * command file declares `recommend-plugin` + `recommend-marketplace` (and an
   * optional `recommend-plugin-description`); both the plugin name and marketplace
   * are required, so a command declaring only one yields no recommendation.
   * Surfaced with provenance via {@link buildPluginRecommendation}. Omitted when the
   * command declares none.
   */
  recommendsPlugin?: PluginRecommendation;
  /**
   * The shell a command's embedded `!`-block execution runs in (the reference parity:
   * parseShellFrontmatter / loadPluginCommands's `shell` passed to
   * executeShellCommandsInPrompt). A command file declares `shell = "bash"` /
   * `shell: powershell`; the value is validated to {@link COMMAND_SHELLS} via
   * {@link parseCommandShell} so an unrecognized value DEGRADES to undefined (the
   * in-prompt executor then defaults to bash — same graceful-degradation contract
   * as {@link effort}). The embedded shell runs scoped to this command's
   * {@link allowedTools} (the reference's `alwaysAllowRules.command` injection). Omitted
   * when the file declares none or an invalid value.
   */
  shell?: CommandShell;
  /**
   * Whether this command executes IMMEDIATELY rather than through the normal
   * deferred/confirm dispatch path (the reference parity: Command.immediate, e.g.
   * commands/bridge/index.ts's `immediate: true`). A command file opts in with
   * `immediate = true` (or `is-immediate = true`). Evaluated via
   * {@link isImmediateCommand}; omitted (treated as deferred) otherwise.
   */
  immediate?: boolean;
}

/**
 * Whether a command runs immediately rather than through the deferred/confirm
 * dispatch path (the reference parity: Command.immediate, defaulting to false). A command
 * is deferred unless its file explicitly declared `immediate = true`.
 */
export function isImmediateCommand(cmd: Pick<CommandDefinition, 'immediate'>): boolean {
  return cmd.immediate === true;
}

/**
 * Resolve a command's plugin recommendation WITH provenance (the reference parity:
 * assembling PluginHintMenu's `Props`). Returns the command's
 * {@link CommandDefinition.recommendsPlugin} stamped with `sourceCommand` (the
 * command's user-facing name via {@link getCommandDisplayName}), or undefined when
 * the command recommends no plugin — so a caller can render the hint without
 * re-deriving the trigger.
 */
export function buildPluginRecommendation(
  cmd: Pick<CommandDefinition, 'name' | 'displayName' | 'recommendsPlugin'>,
): PluginRecommendationHint | undefined {
  const rec = cmd.recommendsPlugin;
  if (!rec) return undefined;
  return {
    pluginName: rec.pluginName,
    marketplaceName: rec.marketplaceName,
    ...(rec.pluginDescription !== undefined ? { pluginDescription: rec.pluginDescription } : {}),
    sourceCommand: getCommandDisplayName(cmd),
  };
}

/**
 * Context for applying a manifest's {@link CommandMetadata} record while loading
 * commands: the plugin `root` (to resolve each entry's relative `source` against)
 * and the `entries` record itself (command key → metadata).
 */
export interface PluginCommandsMetadataContext {
  root: string;
  entries: Record<string, CommandMetadata>;
}

/** Provenance of a command definition (the reference parity: Command.source). */
export const COMMAND_SOURCES = ['builtin', 'plugin', 'user', 'project'] as const;

export type CommandSource = (typeof COMMAND_SOURCES)[number];

/** Behavior discriminator of a command (the reference parity: Command.type). */
export const COMMAND_TYPES = ['prompt', 'local', 'local-jsx'] as const;

export type CommandType = (typeof COMMAND_TYPES)[number];

/**
 * Whether a command is enabled (the reference parity: isCommandEnabled, types/command.ts
 * L213-216 — `cmd.isEnabled?.() ?? true`). AMI's declarative analogue: a command is
 * enabled unless its file explicitly declared `enabled = false`. Disabled commands
 * are dropped from the user-facing command list and never dispatched.
 */
export function isCommandEnabled(cmd: Pick<CommandDefinition, 'isEnabled'>): boolean {
  return cmd.isEnabled !== false;
}

/**
 * Whether a command is hidden from the typeahead/help catalog (the reference parity:
 * CommandBase.isHidden, defaults to false). A hidden command stays invocable by
 * exact name but is omitted from suggestion/help listings.
 */
export function isCommandHidden(cmd: Pick<CommandDefinition, 'isHidden'>): boolean {
  return cmd.isHidden === true;
}

/**
 * Whether a plugin/MCP-contributed command should surface in the MODEL-facing
 * command listing (the reference parity: getSkillToolCommands, commands.ts L563-581 — the
 * `hasUserSpecifiedDescription || whenToUse` gate). Skills / bundled / legacy
 * commands always list because an auto-derived first-line description is acceptable
 * for them; a plugin- or MCP-sourced command instead requires an EXPLICIT,
 * author-specified description (or a `whenToUse` trigger) before it is advertised to
 * the model — an auto-derived description is not enough, so an un-described plugin
 * command is kept user-invocable but hidden from the model listing. Every command in
 * AMI's plugin command registry is plugin-contributed, so this is the gate its
 * model-facing listing applies.
 */
export function isCommandModelListable(
  cmd: Pick<CommandDefinition, 'hasUserSpecifiedDescription' | 'whenToUse'>,
): boolean {
  return (
    cmd.hasUserSpecifiedDescription === true ||
    (typeof cmd.whenToUse === 'string' && cmd.whenToUse.trim() !== '')
  );
}

/**
 * Resolve a command's user-facing display name (the reference parity: getCommandName /
 * userFacingName, types/command.ts L208-211 — `cmd.userFacingName?.() ?? cmd.name`).
 * AMI carries the override as the declared {@link CommandDefinition.displayName};
 * falls back to the (possibly namespaced) lookup `name`.
 */
export function getCommandDisplayName(cmd: Pick<CommandDefinition, 'name' | 'displayName'>): string {
  const display = cmd.displayName;
  return typeof display === 'string' && display.trim() !== '' ? display : cmd.name;
}

/** Placeholder written in place of a sensitive command's arguments in history. */
export const REDACTED_COMMAND_ARGS = '[redacted]';

/**
 * Redact a sensitive command's invocation arguments before they are persisted to
 * conversation history (the reference parity: CommandBase.isSensitive — "args are redacted
 * from the conversation history"). Returns `argString` unchanged for a
 * non-sensitive command (or when there are no args to redact); otherwise replaces
 * the whole argument payload with {@link REDACTED_COMMAND_ARGS} so a secret passed
 * to e.g. `/login <token>` never lands in the transcript. The SINGLE canonical
 * redactor for the history-recording path.
 */
export function redactSensitiveCommandArgs(cmd: Pick<CommandDefinition, 'isSensitive'>, argString: string): string {
  if (cmd.isSensitive !== true) return argString;
  return argString.trim() === '' ? argString : REDACTED_COMMAND_ARGS;
}

/**
 * Register a command's declared {@link CommandDefinition.aliases} into `index`
 * with COLLISION AVOIDANCE (the reference parity: the conditional-alias pattern in
 * commands/branch/index.ts — an alias like `fork` is registered only when it would
 * not shadow an existing name). An alias is added only when it is a non-empty
 * string that does NOT already resolve to a primary command name or a previously
 * claimed alias, so the first claimant of a name always wins and a later plugin's
 * alias can never silently clobber it. Mutates and returns `index`; `primaryNames`
 * is the set of already-registered canonical command names to protect.
 */
export function registerCommandAliases(
  cmd: CommandDefinition,
  index: Map<string, CommandDefinition>,
  primaryNames: ReadonlySet<string>,
): Map<string, CommandDefinition> {
  for (const raw of cmd.aliases ?? []) {
    if (typeof raw !== 'string') continue;
    const alias = raw.trim();
    if (alias === '' || alias === cmd.name) continue; // empty / self-alias — nothing to add
    if (primaryNames.has(alias) || index.has(alias)) continue; // collision — keep the first claimant
    index.set(alias, cmd);
  }
  return index;
}

/**
 * Resolve a component path within a plugin root, rejecting any path that escapes
 * the root via `..` or a symlink (realpath containment check). Returns the path
 * only if it exists and stays inside the plugin — otherwise null.
 */
export function hardenedResolve(root: string, rel: string, onViolation?: (rel: string) => void): string | undefined {
  const target = path.join(root, rel);
  try {
    const realTarget = fs.realpathSync(target);
    const realRoot = fs.realpathSync(root);
    if (realTarget === realRoot || realTarget.startsWith(realRoot + path.sep)) return target;
    // Escapes the plugin root — reject AND, when a reporter is supplied, surface
    // it as a security warning instead of a silent drop (the reference parity:
    // lspPluginIntegration's path-traversal warning).
    onViolation?.(rel);
    return undefined;
  } catch {
    return undefined; // does not exist / unreadable
  }
}

/**
 * A plugin's hooks file is at <root>/hooks.json (AMI's original layout) or
 * <root>/hooks/hooks.json (the reference convention — used by the impeccable
 * plugin). Prefer the root file, fall back to the subdir. hardenedResolve returns
 * undefined for a nonexistent/escaping path, so this naturally yields the first
 * one that actually exists (or undefined).
 */
export function resolvePluginHooksFile(root: string): string | undefined {
  return hardenedResolve(root, 'hooks.json') ?? hardenedResolve(root, path.join('hooks', 'hooks.json'));
}

/**
 * Read a manifest field as a non-empty relative path string, or undefined when it
 * is absent, blank, or a non-string (array / object-mapping forms are not yet
 * supported and fall back to the default). A declared override has already passed
 * the manifest `..`/absolute safety gate before this runs.
 */
export function manifestPathOverride(manifest: Record<string, unknown>, field: string): string | undefined {
  const v = manifest[field];
  return typeof v === 'string' && v.trim() !== '' ? v : undefined;
}

/**
 * Resolve a plugin component directory, honoring a manifest-declared relative
 * override (the reference parity) and otherwise auto-detecting the fixed default dir.
 * Either path is hardened against `..`/symlink escape by {@link hardenedResolve},
 * which yields undefined for a nonexistent or escaping path.
 */
export function resolveComponentDir(
  root: string,
  manifest: Record<string, unknown>,
  field: string,
  defaultRel: string,
  onViolation?: (rel: string) => void,
): string | undefined {
  return hardenedResolve(root, manifestPathOverride(manifest, field) ?? defaultRel, onViolation);
}

/**
 * Resolve a plugin's hooks file: a manifest-declared `hooks` path override wins;
 * otherwise fall back to the <root>/hooks.json or <root>/hooks/hooks.json
 * convention via {@link resolvePluginHooksFile}.
 */
export function resolvePluginHooksFromManifest(root: string, manifest: Record<string, unknown>): string | undefined {
  const override = manifestPathOverride(manifest, 'hooks');
  return override ? hardenedResolve(root, override) : resolvePluginHooksFile(root);
}

/** Options for {@link resolvePluginManifestHooks}. `strict` turns a duplicate hook
 *  file into a {@link PluginError} (the reference parity: the `strict` flag the marketplace/
 *  install context passes into the loader); `standardHooksFile` is the automatically-
 *  loaded `hooks.json`/`hooks/hooks.json` whose realpath seeds the dedup set so a
 *  manifest entry pointing back at it is never double-loaded. */
export interface ManifestHooksOptions {
  source?: string;
  pluginName?: string;
  strict?: boolean;
  standardHooksFile?: string;
}

/** The canonical realpath of `file`, falling back to the raw path when it cannot be
 *  resolved (ENOENT / broken symlink) — the SINGLE spelling used for hook-file dedup
 *  (the reference parity: loadedHookPaths keyed by realpath). */
export function canonicalPath(file: string): string {
  try {
    return fs.realpathSync(file);
  } catch {
    return file;
  }
}

/**
 * Decode the backslash escapes (\n, \t, \\, \") inside a TOML basic-string body.
 * When `stopAtQuote` is set (single-line basic string) an unescaped `"` ends the
 * value; otherwise (a multi-line `"""…"""` body, already delimiter-stripped) the
 * whole input is decoded and inner `"` characters are kept verbatim.
 */
export function unescapeTomlBasic(value: string, stopAtQuote: boolean): string {
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
    } else if (ch === '"' && stopAtQuote) {
      return result;
    } else {
      result += ch;
    }
  }
  return result; // No closing quote found, return what we have
}
