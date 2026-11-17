import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { isSafeSegment } from './skill-catalog';
import { parseFrontmatter, parseFrontmatterList } from './memory';
import { validateLspServerConfig, type LspServerConfig } from './lsp/config';

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

/**
 * True when `source` is an scp-style SSH git remote (`user@host:path`). git clone
 * accepts this form verbatim, so a caller that otherwise expands bare shorthand to
 * a GitHub URL must NOT rewrite it — doing so produces a broken
 * `https://github.com/git@host:…` URL. Shares the single SSH_SCP_RE so the
 * accept-and-clone paths can never diverge from validateGitSource.
 */
export function isScpLikeGitSource(source: string): boolean {
  return typeof source === 'string' && SSH_SCP_RE.test(source.trim());
}
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
  /**
   * Bundled MCP server config file (Claude parity). A plugin may ship an
   * `.mcp.json` at its root (or a manifest-declared `mcpServers` path) declaring
   * MCP servers it contributes. Path-traversal/symlink hardened like every other
   * component path; `undefined` when the plugin ships none.
   */
  mcpServersFile?: string;
  /**
   * Additional command directories/files declared by the manifest `commandsPaths`
   * (Claude parity: loadPluginCommands's `plugin.commandsPaths`), resolved relative
   * to the plugin root and hardened against `..`/symlink escape. Each entry may be a
   * directory (recursively walked) or a single command file. `undefined` when the
   * manifest declares none.
   */
  commandsPaths?: string[];
  /**
   * Additional skill directories/files declared by the manifest `skillsPaths`
   * (Claude parity: loadPluginCommands's `plugin.skillsPaths`), resolved + hardened
   * exactly like {@link commandsPaths}. `undefined` when the manifest declares none.
   */
  skillsPaths?: string[];
  /**
   * Additional agent directories/files declared by the manifest `agentsPaths`
   * (Claude parity: loadPluginAgents's `plugin.agentsPaths`, L275-335 — each entry a
   * directory OR a single `.md` file), resolved + hardened exactly like
   * {@link commandsPaths}/{@link skillsPaths}. Loaded ALONGSIDE the primary
   * `agents/` dir. `undefined` when the manifest declares none or none survive.
   */
  agentsPaths?: string[];
  /**
   * Per-command metadata declared by the manifest `commandsMetadata` object-mapping
   * (Claude parity: PluginManifestCommandsSchema's object form). Maps a command key
   * to either a `source` file-path override (with description/argumentHint/model/
   * allowedTools overrides applied to the discovered command) OR inline `content`
   * that IS the command (no source file). `undefined` when the manifest declares
   * none or every entry is malformed.
   */
  commandsMetadata?: Record<string, CommandMetadata>;
  /**
   * Manifest `defaultEnabled` (Claude parity: a plugin may ship disabled-by-default).
   * Drives the tri-state enablement fallback (managed policy > explicit user marker >
   * `defaultEnabled` > true). `undefined` when the manifest omits it (treated as true).
   */
  defaultEnabled?: boolean;
  /**
   * Manifest `description` surfaced on the loaded bundle (Claude parity: the
   * plugin manifest's human-readable description) so a `/plugin` listing can show
   * what a bundle provides without re-reading its manifest. `undefined` when the
   * manifest omits it or it is not a non-empty string.
   */
  description?: string;
  /**
   * Manifest author display name, normalized to a string (Claude parity: a
   * manifest `author` may be a bare string or a `{ name, … }` object — both reduce
   * to the display name here). `undefined` when absent/blank. Feeds
   * {@link generatePluginId} for the deterministic bundle {@link PluginBundle.id}.
   */
  author?: string;
  /**
   * Deterministic plugin id derived from `author` + `name` via
   * {@link generatePluginId} (Claude parity: generateExtensionId). Present only
   * when the manifest carries an author, so distinct bundles get a stable,
   * collision-resistant identifier. `undefined` when no author is declared.
   */
  id?: string;
  /**
   * Runtime availability requirements declared by the manifest `requires`
   * (Claude parity: BuiltinPluginDefinition.isAvailable, builtinPlugins.ts
   * L64-68 — a plugin whose environment prerequisites aren't met is omitted
   * ENTIRELY). AMI's bundles come from JSON manifests (no code predicate), so
   * availability is expressed declaratively: a `platform`/`env`/`bin` gate
   * evaluated by {@link isPluginAvailable}. A bundle whose `requires` are not
   * satisfied is dropped from discovery (never surfaced), so this is only ever
   * set on bundles that survived the gate. `undefined` when none are declared.
   */
  requires?: PluginRequirements;
  /**
   * Inline MCP servers declared directly in the manifest `mcpServers` record
   * (Claude parity: PluginManifestMcpServerSchema's inline-record / array-mix
   * forms — schemas.ts L543-572), keyed by server name. These are contributed IN
   * ADDITION to any {@link mcpServersFile} the plugin ships. `undefined` when the
   * manifest declares no inline servers (or only a file path).
   */
  mcpServers?: Record<string, PluginMcpServer>;
  /**
   * Bundled LSP server config file (Claude parity: lspPluginIntegration — a
   * plugin may ship a `.lsp.json` at its root, or declare the path via a manifest
   * `lspServers` string). Path-traversal/symlink hardened. `undefined` when the
   * plugin ships none. Consumed via core's {@link loadLspServersFromFile}.
   */
  lspServersFile?: string;
  /**
   * Inline LSP servers declared directly in the manifest `lspServers` record
   * (Claude parity: PluginManifestLspServerSchema's inline-record / array-mix
   * forms — schemas.ts L738-767), validated against the canonical
   * {@link LspServerConfig} schema. Contributed alongside {@link lspServersFile}.
   * `undefined` when the manifest declares no inline LSP servers.
   */
  lspServers?: Record<string, LspServerConfig>;
  /**
   * Output-styles directory a plugin contributes (Claude parity:
   * loadPluginOutputStyles's `plugin.outputStylesPath`): the `<root>/output-styles`
   * dir, or a manifest-declared `outputStyles` relative override. Path-traversal/
   * symlink hardened. `undefined` when the plugin ships none.
   */
  outputStylesDir?: string;
  /**
   * Additional output-style directories/files declared by the manifest
   * `outputStylesPaths` (Claude parity: loadPluginOutputStyles's
   * `plugin.outputStylesPaths`), resolved + hardened exactly like
   * {@link commandsPaths}. `undefined` when the manifest declares none.
   */
  outputStylesPaths?: string[];
  /**
   * Allowlisted plugin-contributed settings (Claude parity:
   * PluginSettingsSchema.pick({agent}).strip() — pluginLoader.ts L1776-1849). A
   * plugin may contribute a default `agent` via `settings.json` (preferred) or a
   * manifest `settings` object; every other key is stripped. `undefined` when the
   * plugin contributes no allowlisted settings.
   */
  settings?: PluginSettings;
  /**
   * Message channels a plugin declares (Claude parity:
   * PluginManifestChannelsSchema — schemas.ts L656-703). Each channel binds one
   * MCP server (by `server` name, matching a key in {@link mcpServers}) that
   * emits `notifications/.../channel` messages to inject into the conversation
   * (Telegram/Slack/Discord bridges), and may declare per-channel
   * {@link PluginChannel.userConfig} to prompt the user for at enable time. The
   * `server`↔`mcpServers` cross-check runs at LOAD time (the mcpServers field may
   * be a not-yet-read file), so discovery keeps every entry with a non-empty
   * `server`. `undefined` when the manifest declares none (or none survive).
   */
  channels?: PluginChannel[];
}

/**
 * One message-channel declaration from a plugin manifest (Claude parity:
 * PluginManifestChannelsSchema entry). `server` names the MCP server this channel
 * binds to (must match a key in the plugin's {@link PluginBundle.mcpServers});
 * `displayName` is the human-readable name shown in the config dialog (defaults to
 * the server name); `userConfig` declares the fields to prompt for when enabling
 * the plugin, saved values being substituted into the server's env.
 */
export interface PluginChannel {
  server: string;
  displayName?: string;
  userConfig?: PluginOptionSchema;
}

/**
 * Declarative runtime-availability gate for a plugin bundle (Claude parity:
 * BuiltinPluginDefinition.isAvailable). AMI bundles are described by JSON, so the
 * predicate is expressed as data a manifest can carry and {@link isPluginAvailable}
 * evaluates: the current OS must be in `platform` (when listed), every name in
 * `env` must be a set, non-empty environment variable, and every name in `bin`
 * must resolve on `PATH`. An omitted/empty constraint imposes no requirement.
 */
export interface PluginRequirements {
  /** Allowed `process.platform` values (e.g. `["linux","darwin"]`). */
  platform?: string[];
  /** Environment variables that must be set and non-empty. */
  env?: string[];
  /** Executables that must resolve on `PATH`. */
  bin?: string[];
}

/**
 * The allowlisted subset of settings a plugin may contribute (Claude parity:
 * PluginSettingsSchema.pick({ agent: true }).strip()). Only `agent` (a default
 * agent name) is honored; any other key an author writes is dropped at parse time.
 */
export interface PluginSettings {
  /** Default agent this plugin contributes to the settings cascade. */
  agent?: string;
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
  /** User-facing display name (Claude's `name` frontmatter → `userFacingName()`),
   *  distinct from the namespaced lookup `name`. Omitted when unset. */
  displayName?: string;
  /**
   * Subdirectory path (relative to the commands root) this command was discovered
   * under, with each path segment as an element (Claude parity:
   * walkPluginMarkdown's `namespace` array). Empty for a top-level command.
   * Carried alongside `name` so a UI can group commands by their folder without
   * re-parsing the `:`-joined name — and distinct from `name` so the plugin-name
   * prefix is NOT conflated with the on-disk folder structure.
   */
  namespace?: string[];
  /**
   * User-facing in-progress hint shown while a prompt/skill command runs (Claude
   * parity: Command.progressMessage, e.g. "committing changes"). Purely an
   * affordance — omitted when the command file declares none.
   */
  progressMessage?: string;
  /**
   * Named positional arguments this command accepts (Claude parity:
   * Command.argNames). Surfaced in suggestion descriptions and mutually
   * exclusive with a single free-form {@link argumentHint}.
   */
  argNames?: string[];
  /**
   * Command kind tag (Claude parity: Command.kind, e.g. `'workflow'`). Drives the
   * suggestion `workflow` affordance; omitted for an ordinary command.
   */
  kind?: string;
  /**
   * Alternate invocation names for this command (Claude parity:
   * CommandBase.aliases) — each resolves to the same command as the primary name.
   */
  aliases?: string[];
  /**
   * Provenance of the command definition (Claude parity: Command.source). A
   * plugin-discovered command defaults to `'plugin'`; a bare-directory load leaves
   * it unset unless the file declares one.
   */
  source?: CommandSource;
  /**
   * Command behavior discriminator (Claude parity: Command.type). Plugin/user
   * command FILES are always `'prompt'` commands (the body/`prompt` is injected);
   * the field is set so a consumer can branch without re-deriving it.
   */
  type?: CommandType;
  /**
   * Whether this command is usable from the non-interactive/detached runner
   * (Claude parity: Command.supportsNonInteractive). Omitted (treated as allowed)
   * unless the file opts out with `supports-non-interactive = false`.
   */
  supportsNonInteractive?: boolean;
  /**
   * Declarative enablement gate (Claude parity: CommandBase.isEnabled, a
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
   * exact name (Claude parity: CommandBase.isHidden, defaults to false). A command
   * file opts in with `hidden = true` (or `is-hidden = true`). Evaluated via
   * {@link isCommandHidden}; omitted (treated as visible) otherwise.
   */
  isHidden?: boolean;
  /**
   * When true, this command's invocation ARGUMENTS are redacted from the persisted
   * conversation history (Claude parity: CommandBase.isSensitive — "args are
   * redacted from the conversation history"). A command file opts in with
   * `sensitive = true` (or `is-sensitive = true`). Applied via
   * {@link redactSensitiveCommandArgs}; omitted (treated as non-sensitive) otherwise.
   */
  isSensitive?: boolean;
}

/**
 * Per-command metadata an author may declare in a plugin manifest's
 * `commandsMetadata` object-mapping (Claude parity: CommandMetadataSchema,
 * utils/plugins/schemas.ts L385-416). An entry either points at a command file via
 * `source` OR carries inline markdown `content` — EXACTLY ONE (the schema `.refine`
 * rejects both/neither) — and may override the command's user-facing description /
 * argument hint / model / allowed-tools.
 */
export interface CommandMetadata {
  /** Relative path (to the plugin root) of the command file this entry overrides. */
  source?: string;
  /** Inline markdown that IS the command (frontmatter + body), when there is no file. */
  content?: string;
  /** Description override for the command. */
  description?: string;
  /** Argument-hint override for the command. */
  argumentHint?: string;
  /** Model override for the command. */
  model?: string;
  /** Allowed-tools override for the command. */
  allowedTools?: string[];
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

/** Provenance of a command definition (Claude parity: Command.source). */
export const COMMAND_SOURCES = ['builtin', 'plugin', 'user', 'project'] as const;
export type CommandSource = (typeof COMMAND_SOURCES)[number];
/** Behavior discriminator of a command (Claude parity: Command.type). */
export const COMMAND_TYPES = ['prompt', 'local', 'local-jsx'] as const;
export type CommandType = (typeof COMMAND_TYPES)[number];

/**
 * Whether a command is enabled (Claude parity: isCommandEnabled, types/command.ts
 * L213-216 — `cmd.isEnabled?.() ?? true`). AMI's declarative analogue: a command is
 * enabled unless its file explicitly declared `enabled = false`. Disabled commands
 * are dropped from the user-facing command list and never dispatched.
 */
export function isCommandEnabled(cmd: Pick<CommandDefinition, 'isEnabled'>): boolean {
  return cmd.isEnabled !== false;
}

/**
 * Whether a command is hidden from the typeahead/help catalog (Claude parity:
 * CommandBase.isHidden, defaults to false). A hidden command stays invocable by
 * exact name but is omitted from suggestion/help listings.
 */
export function isCommandHidden(cmd: Pick<CommandDefinition, 'isHidden'>): boolean {
  return cmd.isHidden === true;
}

/**
 * Resolve a command's user-facing display name (Claude parity: getCommandName /
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
 * conversation history (Claude parity: CommandBase.isSensitive — "args are redacted
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
 * with COLLISION AVOIDANCE (Claude parity: the conditional-alias pattern in
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
function hardenedResolve(root: string, rel: string, onViolation?: (rel: string) => void): string | undefined {
  const target = path.join(root, rel);
  try {
    const realTarget = fs.realpathSync(target);
    const realRoot = fs.realpathSync(root);
    if (realTarget === realRoot || realTarget.startsWith(realRoot + path.sep)) return target;
    // Escapes the plugin root — reject AND, when a reporter is supplied, surface
    // it as a security warning instead of a silent drop (Claude parity:
    // lspPluginIntegration's path-traversal warning).
    onViolation?.(rel);
    return undefined;
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
 * Read a manifest field as a non-empty relative path string, or undefined when it
 * is absent, blank, or a non-string (array / object-mapping forms are not yet
 * supported and fall back to the default). A declared override has already passed
 * the manifest `..`/absolute safety gate before this runs.
 */
function manifestPathOverride(manifest: Record<string, unknown>, field: string): string | undefined {
  const v = manifest[field];
  return typeof v === 'string' && v.trim() !== '' ? v : undefined;
}

/**
 * Resolve a plugin component directory, honoring a manifest-declared relative
 * override (Claude parity) and otherwise auto-detecting the fixed default dir.
 * Either path is hardened against `..`/symlink escape by {@link hardenedResolve},
 * which yields undefined for a nonexistent or escaping path.
 */
function resolveComponentDir(
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
function resolvePluginHooksFromManifest(root: string, manifest: Record<string, unknown>): string | undefined {
  const override = manifestPathOverride(manifest, 'hooks');
  return override ? hardenedResolve(root, override) : resolvePluginHooksFile(root);
}

/**
 * Decode the backslash escapes (\n, \t, \\, \") inside a TOML basic-string body.
 * When `stopAtQuote` is set (single-line basic string) an unescaped `"` ends the
 * value; otherwise (a multi-line `"""…"""` body, already delimiter-stripped) the
 * whole input is decoded and inner `"` characters are kept verbatim.
 */
function unescapeTomlBasic(value: string, stopAtQuote: boolean): string {
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

/**
 * Minimal TOML string parser for the `key = "value"` format used by command
 * definition files. Beyond the original single-line basic string it is an
 * ADDITIVE SUPERSET that matches real TOML / Claude-Code command files:
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
function parseTomlString(content: string, key: string): string | undefined {
  const lines = content.split('\n');
  const keyRe = new RegExp(
    '^\\s*' + key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*=\\s*(.*)$',
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
 * Claude parity: a command is identified by its (required) prompt body; the
 * description is a user-facing affordance only, so a missing one falls back to a
 * generic label rather than silently dropping the whole command.
 */
export const DEFAULT_COMMAND_DESCRIPTION = 'Plugin command';

/**
 * Derive a user-facing command description from its markdown body / TOML prompt
 * when no explicit `description` is declared (Claude parity:
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
 * Apply the optional behavioral knobs (Claude-parity) onto `cmd`, reading each
 * from whichever format the command file is in. `getStr` returns a string knob by
 * key (TOML quoted value or markdown-frontmatter value); `getBool` a boolean;
 * `getList` a string array. Each knob is omitted from `cmd` when absent. A single
 * applier keeps the `.toml` and `.md` loaders in exact knob parity.
 */
function applyCommandKnobs(
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
  // Enablement / visibility / sensitivity gates (Claude parity: CommandBase
  // isEnabled/isHidden/isSensitive). Declarative booleans an author sets to ship a
  // command disabled, hidden from the catalog, or with its args redacted from history.
  const isEnabled = getBool('enabled') ?? getBool('is-enabled');
  if (isEnabled !== undefined) cmd.isEnabled = isEnabled;
  const isHidden = getBool('hidden') ?? getBool('is-hidden');
  if (isHidden !== undefined) cmd.isHidden = isHidden;
  const isSensitive = getBool('sensitive') ?? getBool('is-sensitive');
  if (isSensitive !== undefined) cmd.isSensitive = isSensitive;
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
function parseTomlCommandFile(content: string, filePath: string, name: string): CommandDefinition | undefined {
  const desc = parseTomlString(content, 'description');
  const prompt = parseTomlString(content, 'prompt');
  // A command needs a prompt to do anything; the description is user-facing
  // only, so a prompt-only command loads with a description DERIVED from its
  // prompt (Claude parity) instead of being silently discarded or labelled
  // generically.
  if (!prompt) return undefined;
  // A command FILE injects its body/prompt, so it is a `'prompt'` command (Claude
  // parity: Command.type) unless the file explicitly declares another type.
  const cmd: CommandDefinition = { name, description: desc || deriveCommandDescription(prompt), prompt, filePath, type: 'prompt' };
  applyCommandKnobs(
    cmd,
    key => parseTomlString(content, key),
    key => parseTomlBool(content, key),
    key => parseTomlStringArray(content, key),
  );
  return cmd;
}

/**
 * Parse a single markdown command file (Claude Code's native command format:
 * YAML frontmatter + a markdown body that IS the prompt) into a
 * {@link CommandDefinition}, or undefined when the body is empty / unreadable.
 * `allowed-tools` accepts the comma-separated or single-value frontmatter form;
 * booleans accept `true`/`false`. A missing description is DERIVED from the body's
 * first line via {@link deriveCommandDescription} (Claude parity).
 */
function parseMarkdownCommandFile(content: string, filePath: string, name: string): CommandDefinition | undefined {
  const { frontmatter, body } = parseFrontmatter(content);
  const prompt = body.trim();
  if (!prompt) return undefined; // a command with no body does nothing — skip
  const desc = frontmatter.description?.trim();
  const cmd: CommandDefinition = { name, description: desc || deriveCommandDescription(prompt), prompt, filePath, type: 'prompt' };
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
 * `key = "value"` format; `.md` uses Claude Code's YAML-frontmatter + body format
 * (the body is the prompt). Returns undefined for an unreadable file or one with
 * no usable prompt.
 */
function parseCommandFile(filePath: string, name: string): CommandDefinition | undefined {
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

/** Command file extensions AMI loads: TOML (AMI's original) and Claude's `.md`. */
const COMMAND_FILE_EXTS = ['.toml', '.md'] as const;

/** Case-insensitive `.md`/SKILL.md matching (Claude parity: walkPluginMarkdown
 *  uses `.toLowerCase().endsWith('.md')` and `/^skill\.md$/i`), so a `Deploy.TOML`
 *  / `Review.MD` / `skill.md` is discovered the same as its canonical casing. */
const SKILL_MD_RE = /^skill\.md$/i;

/** True when `name` ends with `ext` (lower-cased) regardless of the on-disk case. */
function hasExt(name: string, ext: string): boolean {
  return name.toLowerCase().endsWith(ext);
}

/** Strip a recognized command-file extension (case-insensitively) from
 *  `fileName`, preserving the base name's original casing, or null if none. */
function commandBaseName(fileName: string): string | null {
  for (const ext of COMMAND_FILE_EXTS) {
    if (hasExt(fileName, ext)) return fileName.slice(0, fileName.length - ext.length);
  }
  return null;
}

/**
 * Diagnostics for a command-directory scan (Claude parity: walkPluginMarkdown's
 * `logLabel` option + swallowed-with-a-debug-log readdir failure). Lets a caller
 * trace a bad/unreadable plugin directory instead of the load silently contributing
 * zero commands — core stays logger-free by surfacing the failure through a
 * callback the consumer (skill-manager / plugin-installer) wires to its logger.
 */
export interface CommandScanDiagnostics {
  /** Context label for scan-failure messages (Claude parity: logLabel, default `'plugin'`). */
  logLabel?: string;
  /**
   * Invoked once per directory that cannot be scanned (readdir/stat failure).
   * Receives a ready-to-log `message` plus the structured `dir`/`error`/`label` so
   * a consumer can either log the message verbatim or build its own.
   */
  onScanError?: (info: { dir: string; error: unknown; label: string; message: string }) => void;
}

/**
 * Surface a command-directory scan failure through the caller's
 * {@link CommandScanDiagnostics} (Claude parity: the `Failed to scan ${label}
 * directory ${dir}` debug log). A no-op when no `onScanError` sink is wired, so the
 * default (diagnostics-less) load stays silent exactly as before.
 */
function reportScanError(dir: string, error: unknown, diag: CommandScanDiagnostics | undefined): void {
  if (!diag?.onScanError) return;
  const label = diag.logLabel ?? 'plugin';
  diag.onScanError({ dir, error, label, message: `Failed to scan ${label} directory ${dir}: ${errMessage(error)}` });
}

/**
 * Recursively walk a commands directory, collecting every `.toml` or `.md`
 * command file. The `namespace` accumulates the sub-path from the commands root
 * to the file (Claude parity: `getCommandNameFromFile`), and `pluginName` — when
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
 * (Claude parity: fsOperations `isDuplicatePath`/`safeResolvePath` used by
 * loadPluginCommands). Resolves `filePath` to its realpath (falling back to the
 * raw path when it can't be resolved — ENOENT/broken symlink) and records it;
 * returns true when that canonical path was ALREADY loaded, so a file reachable
 * through two sources (e.g. the primary `commands/` dir and a manifest
 * `commandsPaths` entry, or a symlink pointing back into the tree) is loaded
 * exactly once instead of producing duplicate command definitions.
 */
function isDuplicateComponentPath(filePath: string, loaded: Set<string>): boolean {
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

function walkCommandDir(dir: string, subPath: string[], pluginName: string | undefined, out: CommandDefinition[], loaded: Set<string>, diag?: CommandScanDiagnostics): void {
  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch (err) {
    // Missing/unreadable dir — nothing to add. Emit a diagnostic (Claude parity:
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
    // (Claude parity: isDuplicatePath) so it never yields a duplicate command.
    if (isDuplicateComponentPath(full, loaded)) continue;
    // The looked-up `name` keeps its original composition: optional plugin-name
    // segment, then the sub-path, then the base. The `namespace` field tracks ONLY
    // the on-disk sub-path (Claude parity: walkPluginMarkdown), so the plugin-name
    // prefix is not conflated with the folder structure.
    const nameSegs = [...(pluginName ? [pluginName] : []), ...subPath, base];
    const name = nameSegs.join(':');
    const cmd = parseCommandFile(full, name);
    if (cmd) {
      cmd.namespace = subPath;
      // Provenance defaults to 'plugin' for a bundle walk (a pluginName was given)
      // unless the file already declared an explicit source.
      if (pluginName && cmd.source === undefined) cmd.source = 'plugin';
      out.push(cmd);
    }
  }
}

/**
 * Load command definitions from a plugin's commands/ directory, recursing into
 * nested subdirectories (Claude parity). Each `.toml` or `.md` file defines a
 * slash command; a prompt is required (it is what the command runs — the TOML
 * `prompt` key or the markdown body), while `description` is optional and falls
 * back to {@link DEFAULT_COMMAND_DESCRIPTION}. A command in a subdirectory is
 * namespaced by its sub-path with `:` separators (e.g. `commands/git/commit.toml`
 * → `git:commit`). When `pluginName` is given, it is prepended as the first
 * segment so command names are unique across plugins (`pluginName:git:commit`).
 *
 * `extraPaths` (Claude parity: a manifest's `commandsPaths`) are ADDITIONAL command
 * locations outside the primary `commands/` dir — each entry may be a directory
 * (walked recursively) or a single command file (`.toml`/`.md`), discovered under
 * the same namespacing rules. They are loaded AFTER `dir` so a duplicate name from
 * an extra path does not shadow the canonical command (callers dedup by name).
 *
 * `diagnostics` (Claude parity: walkPluginMarkdown's `logLabel` + swallowed-with-a-
 * debug-log scan failure) lets a caller trace a bad/unreadable command directory —
 * a readdir/stat failure is reported through `onScanError` (labelled by `logLabel`,
 * default `'plugin'`) instead of silently contributing zero commands.
 */
export function loadCommands(dir: string, pluginName?: string, extraPaths?: string[], metadata?: PluginCommandsMetadataContext, diagnostics?: CommandScanDiagnostics): CommandDefinition[] {
  const commands: CommandDefinition[] = [];
  const plugin = pluginName && pluginName.trim() !== '' ? pluginName.trim() : undefined;
  // Tracks the canonical realpath of every file loaded across ALL sources (the
  // primary dir + every extra path) so a file reachable through more than one
  // source is loaded exactly once (Claude parity: loadPluginCommands' loadedPaths).
  const loaded = new Set<string>();
  walkCommandDir(dir, [], plugin, commands, loaded, diagnostics);
  for (const extra of extraPaths ?? []) {
    let stat: fs.Stats;
    try {
      stat = fs.statSync(extra);
    } catch (err) {
      reportScanError(extra, err, diagnostics); // missing/unreadable extra path — trace + skip (Claude parity)
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
        if (plugin && cmd.source === undefined) cmd.source = 'plugin';
        commands.push(cmd);
      }
    }
  }
  if (metadata) applyCommandsMetadata(commands, metadata);
  return commands;
}

/**
 * Copy the overridable fields of a {@link CommandMetadata} entry onto `cmd`
 * (Claude parity: the metadata-override frontmatter merge in loadPluginCommands).
 * Each override is applied only when it is a non-empty string / non-empty array so
 * a blank override never blanks out the command's own value.
 */
function applyCommandMetadataOverride(cmd: CommandDefinition, meta: CommandMetadata): void {
  if (typeof meta.description === 'string' && meta.description.trim() !== '') cmd.description = meta.description;
  if (typeof meta.argumentHint === 'string' && meta.argumentHint.trim() !== '') cmd.argumentHint = meta.argumentHint;
  if (typeof meta.model === 'string' && meta.model.trim() !== '') cmd.model = meta.model;
  if (Array.isArray(meta.allowedTools) && meta.allowedTools.length > 0) cmd.allowedTools = meta.allowedTools;
}

/**
 * Apply a plugin manifest's `commandsMetadata` object-mapping to an already-walked
 * command list (Claude parity: loadPluginCommands L516-668). Each entry is keyed by
 * the command name it defines and carries EXACTLY ONE of `source`/`content`:
 *  - `source`: resolve the file path (relative to the plugin root, hardened against
 *    `..`/symlink escape), match the discovered command loaded from it, rename that
 *    command to the metadata key, and apply the field overrides.
 *  - `content`: parse the inline markdown as a brand-new `'prompt'` command under the
 *    metadata key (no source file) and apply the field overrides.
 * An entry with both or neither of source/content is skipped (the schema rejects it).
 */
function applyCommandsMetadata(commands: CommandDefinition[], ctx: PluginCommandsMetadataContext): void {
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
      // Inline-content command: the markdown IS the command (Claude parity). A
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

/**
 * The settings sources that can OWN an enabled plugin, lowest precedence first
 * (Claude parity: pluginStartupCheck's `scopeSources`). `managed` (policy) is
 * not user-editable; `flag` is an `--add-dir`/session-only enablement with no
 * write-back. A later source overrides an earlier one for the same plugin, so
 * the resolved owner is the HIGHEST-precedence source that enables it.
 */
export const PLUGIN_EDITABLE_SCOPES = ['managed', 'user', 'project', 'local', 'flag'] as const;
export type ExtendedPluginScope = (typeof PLUGIN_EDITABLE_SCOPES)[number];
/** A scope whose enablement is persisted (everything except the session-only
 *  `flag` scope) — Claude parity: PersistablePluginScope. */
export type PersistablePluginScope = Exclude<ExtendedPluginScope, 'flag'>;

/**
 * Whether an owning scope's enablement should be written back to disk (Claude
 * parity: pluginStartupCheck `isPersistableScope`). The `flag` scope is
 * session-only (`--add-dir`), so it is NOT persistable; every other scope is.
 */
export function isPersistablePluginScope(scope: ExtendedPluginScope): scope is PersistablePluginScope {
  return scope !== 'flag';
}

/** One settings source's `enabledPlugins` map, in precedence order lowest→highest
 *  (Claude parity: the policy>local>project>user>flag resolution, expressed here
 *  as the write-back-ownership order managed<user<project<local<flag). A value of
 *  `true` enables the plugin at that scope, `false` disables it. */
export interface PluginScopeSource {
  scope: ExtendedPluginScope;
  enabledPlugins: Record<string, boolean>;
}

/**
 * Resolve the user-editable scope that OWNS each enabled plugin — i.e. where a
 * toggle should be written back (Claude parity: pluginStartupCheck
 * `getPluginEditableScopes`). Sources are applied in `PluginScopeSource` order
 * (lowest precedence first); for each plugin, a `true` sets the owning scope to
 * that source and a `false` removes it, so the final owner is the highest
 * source that enables it with no higher source disabling it. Only well-formed
 * plugin ids (containing `@`, i.e. `name@marketplace`) are considered. Returns a
 * Map of plugin id → owning scope.
 */
export function resolvePluginEditableScopes(sources: PluginScopeSource[]): Map<string, ExtendedPluginScope> {
  const result = new Map<string, ExtendedPluginScope>();
  for (const { scope, enabledPlugins } of sources) {
    for (const [pluginId, value] of Object.entries(enabledPlugins)) {
      if (!pluginId.includes('@')) continue; // skip invalid id format
      if (value === true) result.set(pluginId, scope);
      else if (value === false) result.delete(pluginId);
    }
  }
  return result;
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
/** Optional component-path fields a manifest may declare. The single-path forms
 *  (skills/agents/commands/hooks/outputStyles) plus the multi-path `agentsPaths`/
 *  `outputStylesPaths` list forms (Claude parity: loadPluginAgents /
 *  loadPluginOutputStyles) are all traversal-validated. The server-declaration
 *  fields (mcpServers/lspServers) accept inline-record forms too and are validated
 *  separately via {@link validateServerDeclarations}. */
const COMPONENT_PATH_FIELDS = ['skills', 'agents', 'commands', 'hooks', 'agentsPaths', 'outputStyles', 'outputStylesPaths'];

/** Manifest fields that declare servers either by FILE PATH (traversal-checked) or
 *  by INLINE RECORD (structurally opaque here): mcpServers (Claude parity:
 *  PluginManifestMcpServerSchema) and lspServers (PluginManifestLspServerSchema). */
const SERVER_DECLARATION_FIELDS = ['mcpServers', 'lspServers'];

/**
 * Flag a single component-path string that would escape the plugin root — an
 * absolute path, or one whose segments include `..`. `label` names the offending
 * field (including an array index / record key for the richer forms).
 */
function validateComponentPathString(label: string, val: string, errors: PluginValidationIssue[]): void {
  if (path.isAbsolute(val)) {
    errors.push({ path: label, message: `Component path "${val}" must be relative to the plugin root.` });
  } else if (val.split(/[/\\]/).includes('..')) {
    errors.push({ path: label, message: `Component path "${val}" contains ".." and would escape the plugin directory.` });
  }
}

/**
 * Validate a manifest component-path field (skills/agents/commands/hooks/mcpServers)
 * across ALL the forms Claude accepts (schemas.ts L429-497): a single relative-path
 * string, an array of relative-path strings, or (commands) an object mapping a
 * command name → { source, ... } metadata. Every path is traversal-checked via
 * {@link validateComponentPathString}; a non-string array entry is flagged. AMI
 * previously validated only the string form, so an array/record entry with `..` or
 * an absolute path slipped past the traversal gate — this closes that hole.
 */
function validateComponentPathField(field: string, val: unknown, errors: PluginValidationIssue[]): void {
  if (typeof val === 'string') {
    validateComponentPathString(field, val, errors);
  } else if (Array.isArray(val)) {
    val.forEach((el, i) => {
      if (typeof el !== 'string') {
        errors.push({ path: `${field}[${i}]`, message: `Component path entry must be a string (got ${Array.isArray(el) ? 'array' : typeof el}).` });
      } else {
        validateComponentPathString(`${field}[${i}]`, el, errors);
      }
    });
  } else if (val && typeof val === 'object') {
    // Object-mapping form (Claude parity: commands record name → { source, ... }).
    // Each entry's `source` is a relative command-file path; validate it when present.
    for (const [key, meta] of Object.entries(val as Record<string, unknown>)) {
      if (meta && typeof meta === 'object' && !Array.isArray(meta)) {
        const src = (meta as Record<string, unknown>).source;
        if (typeof src === 'string') validateComponentPathString(`${field}.${key}.source`, src, errors);
      }
    }
  }
}

/**
 * Validate a server-declaration field (`mcpServers`/`lspServers`) across the forms
 * Claude accepts (schemas.ts L543-572 / L738-767): a FILE-PATH string, an INLINE
 * RECORD, or an ARRAY mixing file-path strings and inline records. Path strings are
 * traversal-checked via {@link validateComponentPathString} (so a `..`/absolute
 * override is still a hard error for the discovery gate); inline records are opaque
 * here (their per-server shape is validated where the servers are actually loaded).
 */
function validateServerDeclarations(field: string, val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val === 'string') {
    validateComponentPathString(field, val, errors);
  } else if (Array.isArray(val)) {
    val.forEach((el, i) => {
      if (typeof el === 'string') validateComponentPathString(`${field}[${i}]`, el, errors);
      // An inline-record ARRAY entry is a `name → config` map (Claude parity: the
      // array form mixes file-path strings with inline records). Its structural
      // shape is opaque for MCP, but an inline LSP record is validated with the
      // canonical LSP schema so a bad command/extension surfaces here too.
      else if (field === 'lspServers' && isPlainObject(el)) validateInlineLspServers(`${field}[${i}]`, el, errors);
    });
  } else if (field === 'lspServers' && isPlainObject(val)) {
    // A bare inline LSP record (`name → config`) imposes no path to traverse, but
    // each server config IS validated (Claude parity: PluginManifestLspServerSchema's
    // inline-record form uses LspServerConfigSchema) so corrective, example-bearing
    // messages (command-spaces / file-extension dot) reach `/plugin validate`.
    validateInlineLspServers(field, val, errors);
  }
  // a bare inline MCP record imposes no path to traverse and is structurally opaque here
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
function validateInlineLspServers(label: string, rec: Record<string, unknown>, errors: PluginValidationIssue[]): void {
  for (const [serverName, cfg] of Object.entries(rec)) {
    try {
      validateLspServerConfig(serverName, cfg);
    } catch (err) {
      errors.push({ path: `${label}.${serverName}`, message: errMessage(err) });
    }
  }
}

/**
 * Validate a manifest's optional `requires` availability gate (AMI's declarative
 * analogue of Claude's BuiltinPluginDefinition.isAvailable). Each of `platform`/
 * `env`/`bin`, when present, must be an array of strings; a malformed constraint is
 * flagged with a corrective example so an author can self-correct.
 */
function validatePluginRequirements(val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: 'requires', message: 'Plugin "requires" must be an object with optional "platform"/"env"/"bin" string arrays.' });
    return;
  }
  const r = val as Record<string, unknown>;
  for (const key of ['platform', 'env', 'bin'] as const) {
    const v = r[key];
    if (v !== undefined && (!Array.isArray(v) || !v.every(x => typeof x === 'string'))) {
      errors.push({ path: `requires.${key}`, message: `Plugin "requires.${key}" must be an array of strings (e.g., ["linux", "darwin"] for platform).` });
    }
  }
}

/**
 * Validate a manifest's optional `settings` object (Claude parity:
 * PluginSettingsSchema — only `agent` is allowlisted, everything else is stripped).
 * `settings` must be an object; a present `agent` must be a string. Non-allowlisted
 * keys are surfaced as warnings (they are silently dropped at load time) so an
 * author learns the plugin cannot set them.
 */
function validatePluginSettings(val: unknown, errors: PluginValidationIssue[], warnings: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: 'settings', message: 'Plugin "settings" must be an object (only the allowlisted "agent" key is honored).' });
    return;
  }
  const s = val as Record<string, unknown>;
  if (s.agent !== undefined && typeof s.agent !== 'string') {
    errors.push({ path: 'settings.agent', message: 'Plugin "settings.agent" must be a string (the default agent name).' });
  }
  for (const key of Object.keys(s)) {
    if (key !== 'agent') {
      warnings.push({ path: `settings.${key}`, message: `Plugin "settings.${key}" is not an allowlisted plugin setting and is ignored (only "agent" is honored).` });
    }
  }
}

/** True when `value` parses as an absolute URL (parity with Claude's zod `.url()`
 *  refinement for homepage: a scheme-less `example.com` is rejected). */
function isValidUrl(value: string): boolean {
  try {
    new URL(value);
    return true;
  } catch {
    return false;
  }
}

/**
 * Validate a manifest's structured `author` (Claude parity: PluginAuthorSchema).
 * Accepts either a non-empty display-name string or an object `{ name, email?,
 * url? }` whose `name` is a non-empty string and whose optional `email`/`url` are
 * strings. Pushes labelled, actionable errors for every other shape.
 */
function validatePluginAuthor(author: unknown, errors: PluginValidationIssue[]): void {
  if (author === undefined) return;
  if (typeof author === 'string') {
    if (author.trim() === '') {
      errors.push({ path: 'author', message: 'Plugin "author" cannot be an empty string.' });
    }
    return;
  }
  if (typeof author !== 'object' || author === null || Array.isArray(author)) {
    errors.push({ path: 'author', message: 'Plugin "author" must be a string or an object with a "name" (and optional "email"/"url").' });
    return;
  }
  const a = author as Record<string, unknown>;
  if (typeof a.name !== 'string' || a.name.trim() === '') {
    errors.push({ path: 'author.name', message: 'Author "name" cannot be empty.' });
  }
  if (a.email !== undefined && typeof a.email !== 'string') {
    errors.push({ path: 'author.email', message: 'Author "email" must be a string.' });
  }
  if (a.url !== undefined && typeof a.url !== 'string') {
    errors.push({ path: 'author.url', message: 'Author "url" must be a string.' });
  }
}

/**
 * Validate the OPTIONAL metadata fields of a plugin manifest (Claude parity:
 * PluginManifestMetadataSchema, utils/plugins/schemas.ts L251-L320). Every field
 * is optional, but when present must have the right shape; each message carries a
 * concrete example so an author can self-correct. Component-path and name/version
 * checks live in {@link validatePluginManifest}; this covers description, author,
 * homepage, repository, license, and keywords.
 */
function validateManifestMetadata(obj: Record<string, unknown>, errors: PluginValidationIssue[]): void {
  if (obj.description !== undefined && typeof obj.description !== 'string') {
    errors.push({ path: 'description', message: 'Plugin "description" must be a string (a brief, user-facing explanation of what the plugin provides).' });
  }
  validatePluginAuthor(obj.author, errors);
  if (obj.homepage !== undefined && (typeof obj.homepage !== 'string' || !isValidUrl(obj.homepage))) {
    errors.push({ path: 'homepage', message: 'Plugin "homepage" must be a valid URL (e.g., "https://example.com").' });
  }
  if (obj.repository !== undefined && typeof obj.repository !== 'string') {
    errors.push({ path: 'repository', message: 'Plugin "repository" must be a string (the source code repository URL).' });
  }
  if (obj.license !== undefined && typeof obj.license !== 'string') {
    errors.push({ path: 'license', message: 'Plugin "license" must be a string SPDX license identifier (e.g., "MIT", "Apache-2.0").' });
  }
  if (obj.keywords !== undefined && (!Array.isArray(obj.keywords) || !obj.keywords.every(k => typeof k === 'string'))) {
    errors.push({ path: 'keywords', message: 'Plugin "keywords" must be an array of strings (e.g., ["cli", "git"]).' });
  }
  validateManifestDependencies(obj.dependencies, errors);
}

/**
 * Validate a manifest's optional `dependencies` — the plugins that must be enabled
 * first (Claude parity: DependencyRefSchema, utils/plugins/schemas.ts L1367). Each
 * entry is either a non-empty ref string (`name` or `name@marketplace`) or an
 * object carrying a non-empty string `name` (AMI also accepts `plugin` as the ref
 * key, matching its resolver) with an optional string `marketplace`. The common
 * dependency resolver already CONSUMES these; this adds the author-facing VALIDATION
 * so a malformed entry surfaces instead of being silently dropped.
 */
function validateManifestDependencies(deps: unknown, errors: PluginValidationIssue[]): void {
  if (deps === undefined) return;
  if (!Array.isArray(deps)) {
    errors.push({ path: 'dependencies', message: 'Plugin "dependencies" must be an array of plugin refs (a "name"/"name@marketplace" string or a { name, marketplace? } object).' });
    return;
  }
  deps.forEach((dep, i) => {
    if (typeof dep === 'string') {
      if (dep.trim() === '') {
        errors.push({ path: `dependencies[${i}]`, message: 'Dependency ref cannot be an empty string.' });
      }
      return;
    }
    if (!dep || typeof dep !== 'object' || Array.isArray(dep)) {
      errors.push({ path: `dependencies[${i}]`, message: 'Dependency must be a plugin-name string or an object with a "name" (and optional "marketplace").' });
      return;
    }
    const d = dep as Record<string, unknown>;
    const ref = d.name ?? d.plugin;
    if (typeof ref !== 'string' || ref.trim() === '') {
      errors.push({ path: `dependencies[${i}]`, message: 'Dependency object must have a non-empty "name".' });
    }
    if (d.marketplace !== undefined && typeof d.marketplace !== 'string') {
      errors.push({ path: `dependencies[${i}].marketplace`, message: 'Dependency "marketplace" must be a string.' });
    }
  });
}

/**
 * Validate a manifest's optional `commandsMetadata` object-mapping (Claude parity:
 * CommandMetadataSchema, utils/plugins/schemas.ts L385-416). Each value must be an
 * object carrying EXACTLY ONE of `source` (a relative, non-escaping command-file
 * path) or `content` (inline markdown) — the schema `.refine` rejects both/neither —
 * plus optional string `description`/`argumentHint`/`model` and a string-array
 * `allowedTools`. Each message carries the corrective example wording so an author
 * can self-correct.
 */
function validateCommandsMetadata(val: unknown, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: 'commandsMetadata', message: 'Plugin "commandsMetadata" must be an object mapping a command name to its { source | content, ... } metadata.' });
    return;
  }
  for (const [key, meta] of Object.entries(val as Record<string, unknown>)) {
    const label = `commandsMetadata.${key}`;
    if (!meta || typeof meta !== 'object' || Array.isArray(meta)) {
      errors.push({ path: label, message: 'Command metadata must be an object with either a "source" (file path) or "content" (inline markdown).' });
      continue;
    }
    const m = meta as Record<string, unknown>;
    const hasSource = typeof m.source === 'string' && m.source.trim() !== '';
    const hasContent = typeof m.content === 'string' && m.content.trim() !== '';
    if (hasSource === hasContent) {
      errors.push({ path: label, message: 'Command must have either "source" (file path) or "content" (inline markdown), but not both.' });
    }
    if (typeof m.source === 'string') validateComponentPathString(`${label}.source`, m.source, errors);
    for (const field of ['description', 'argumentHint', 'model'] as const) {
      if (m[field] !== undefined && typeof m[field] !== 'string') {
        errors.push({ path: `${label}.${field}`, message: `Command metadata "${field}" must be a string.` });
      }
    }
    if (m.allowedTools !== undefined && (!Array.isArray(m.allowedTools) || !m.allowedTools.every(t => typeof t === 'string'))) {
      errors.push({ path: `${label}.allowedTools`, message: 'Command metadata "allowedTools" must be an array of strings (e.g., ["Bash", "Read"]).' });
    }
  }
}

/**
 * The value types a plugin `userConfig` option may declare (Claude parity:
 * PluginUserConfigOptionSchema, utils/plugins/schemas.ts L589 — matches
 * `@anthropic-ai/mcpb`'s McpbUserConfigurationOption). A `directory`/`file` option
 * prompts for a path; `string`/`number`/`boolean` for the obvious scalars.
 */
export const USER_CONFIG_OPTION_TYPES = ['string', 'number', 'boolean', 'directory', 'file'] as const;
export type UserConfigOptionType = (typeof USER_CONFIG_OPTION_TYPES)[number];
/** An option KEY must be a valid identifier — it becomes an env var name at hook
 *  time (Claude parity: CLAUDE_PLUGIN_OPTION_<KEY>). */
const USER_CONFIG_KEY_RE = /^[A-Za-z_]\w*$/;

/**
 * Validate a manifest's optional `userConfig` — the user-prompted configuration
 * options a plugin needs at enable time (Claude parity: PluginManifestUserConfigSchema
 * + PluginUserConfigOptionSchema, utils/plugins/schemas.ts L575-654). Each entry is
 * keyed by an identifier-safe option key and declares a `type` (one of
 * {@link USER_CONFIG_OPTION_TYPES}), required `title`/`description` strings, and the
 * optional `required`/`default`/`multiple`/`sensitive`/`min`/`max` knobs. Every
 * message carries a corrective example so an author can self-correct.
 */
function validateUserConfig(val: unknown, errors: PluginValidationIssue[], labelPrefix = 'userConfig'): void {
  if (val === undefined) return;
  if (typeof val !== 'object' || val === null || Array.isArray(val)) {
    errors.push({ path: labelPrefix, message: `Plugin "${labelPrefix}" must be an object mapping an option key to its { type, title, description, ... } definition.` });
    return;
  }
  for (const [key, opt] of Object.entries(val as Record<string, unknown>)) {
    const label = `${labelPrefix}.${key}`;
    if (!USER_CONFIG_KEY_RE.test(key)) {
      errors.push({ path: label, message: `Option key "${key}" must be a valid identifier (letters, digits, underscore; no leading digit) — it becomes a CLAUDE_PLUGIN_OPTION_<KEY> env var in hooks.` });
    }
    if (!opt || typeof opt !== 'object' || Array.isArray(opt)) {
      errors.push({ path: label, message: 'Option must be an object with a "type", "title", and "description".' });
      continue;
    }
    const o = opt as Record<string, unknown>;
    if (typeof o.type !== 'string' || !(USER_CONFIG_OPTION_TYPES as readonly string[]).includes(o.type)) {
      errors.push({ path: `${label}.type`, message: `Option "type" must be one of: ${USER_CONFIG_OPTION_TYPES.join(', ')}.` });
    }
    if (typeof o.title !== 'string' || o.title.trim() === '') {
      errors.push({ path: `${label}.title`, message: 'Option "title" must be a non-empty string (the label shown in the config prompt).' });
    }
    if (typeof o.description !== 'string' || o.description.trim() === '') {
      errors.push({ path: `${label}.description`, message: 'Option "description" must be a non-empty string (help text for the config prompt).' });
    }
    for (const flag of ['required', 'multiple', 'sensitive'] as const) {
      if (o[flag] !== undefined && typeof o[flag] !== 'boolean') {
        errors.push({ path: `${label}.${flag}`, message: `Option "${flag}" must be a boolean (true/false).` });
      }
    }
    for (const num of ['min', 'max'] as const) {
      if (o[num] !== undefined && typeof o[num] !== 'number') {
        errors.push({ path: `${label}.${num}`, message: `Option "${num}" must be a number.` });
      }
    }
    if (o.default !== undefined && !isValidUserConfigDefault(o.default)) {
      errors.push({ path: `${label}.default`, message: 'Option "default" must be a string, number, boolean, or array of strings.' });
    }
  }
}

/** A userConfig `default` may be a scalar (string/number/boolean) or, for a
 *  `multiple` string option, an array of strings (Claude parity). */
function isValidUserConfigDefault(val: unknown): boolean {
  if (typeof val === 'string' || typeof val === 'number' || typeof val === 'boolean') return true;
  return Array.isArray(val) && val.every(v => typeof v === 'string');
}

/**
 * The set of inline MCP server names a manifest declares (Claude parity: a
 * channel's `server` must match a key in `mcpServers`). Only the INLINE record /
 * array-of-records forms contribute names here; when `mcpServers` is just a FILE
 * PATH string the names are unknown until the file is read, so this returns
 * `undefined` to signal "defer the cross-check to load time" (matching Claude's
 * schema comment that the server↔mcpServers check lives in mcpPluginIntegration).
 */
function inlineMcpServerNames(manifest: Record<string, unknown>): Set<string> | undefined {
  const raw = manifest.mcpServers;
  if (typeof raw === 'string') return undefined; // file path — names not yet known
  const names = new Set<string>();
  const absorb = (rec: Record<string, unknown>): void => {
    for (const [name, cfg] of Object.entries(rec)) {
      if (isPlainObject(cfg)) names.add(name);
    }
  };
  if (isPlainObject(raw)) absorb(raw);
  else if (Array.isArray(raw)) {
    // An array may mix file-path strings (names unknown) with inline records.
    if (raw.some(e => typeof e === 'string')) return undefined;
    for (const entry of raw) if (isPlainObject(entry)) absorb(entry);
  } else {
    return undefined; // no inline declaration — nothing to cross-check against
  }
  return names;
}

/**
 * Validate a manifest's optional `channels` — the message-channel declarations a
 * plugin ships (Claude parity: PluginManifestChannelsSchema, utils/plugins/schemas.ts
 * L656-703). `channels` must be an array; each entry is an object with a non-empty
 * `server` string, an optional string `displayName`, and an optional `userConfig`
 * validated exactly like the top-level manifest `userConfig`. When the manifest
 * declares its MCP servers INLINE (so the names are known), each channel's `server`
 * is cross-checked against those keys; when `mcpServers` is only a file path, that
 * cross-check is deferred (Claude parity — resolved at load time, not parse time).
 */
function validatePluginChannels(val: unknown, manifest: Record<string, unknown>, errors: PluginValidationIssue[]): void {
  if (val === undefined) return;
  if (!Array.isArray(val)) {
    errors.push({ path: 'channels', message: 'Plugin "channels" must be an array of channel declarations, each binding an MCP server (e.g., [{ "server": "telegram", "displayName": "Telegram" }]).' });
    return;
  }
  const knownServers = inlineMcpServerNames(manifest);
  val.forEach((entry, i) => {
    const label = `channels[${i}]`;
    if (!isPlainObject(entry)) {
      errors.push({ path: label, message: 'Channel entry must be an object with a "server" (and optional "displayName"/"userConfig").' });
      return;
    }
    const server = entry.server;
    if (typeof server !== 'string' || server.trim() === '') {
      errors.push({ path: `${label}.server`, message: 'Channel "server" must be a non-empty string naming an MCP server declared in this plugin\'s "mcpServers".' });
    } else if (knownServers !== undefined && !knownServers.has(server)) {
      errors.push({ path: `${label}.server`, message: `Channel "server" "${server}" must match a key in this plugin's "mcpServers" (declared: ${[...knownServers].join(', ') || 'none'}).` });
    }
    if (entry.displayName !== undefined && typeof entry.displayName !== 'string') {
      errors.push({ path: `${label}.displayName`, message: 'Channel "displayName" must be a string (the human-readable name shown in the config dialog).' });
    }
    validateUserConfig(entry.userConfig, errors, `${label}.userConfig`);
  });
}

// ---------------------------------------------------------------------------
// Plugin option VALUES — the runtime side of `userConfig` (Claude parity:
// utils/plugins/pluginOptionsStorage.ts + commands/plugin/PluginOptionsDialog
// /PluginOptionsFlow). The validators above gate the manifest SHAPE; the
// functions below coerce/validate/prefill the user-entered VALUES against a
// declared schema so the config dialog and option storage behave identically
// across modes (TUI/detached) and never silently corrupt a value.
// ---------------------------------------------------------------------------

/**
 * A single plugin `userConfig` option declaration, as the config dialog and
 * option storage consume it (Claude parity: PluginUserConfigOptionSchema). `type`
 * is one of {@link USER_CONFIG_OPTION_TYPES}; `title`/`description` are shown in
 * the prompt; the knobs mirror the manifest schema validated by
 * {@link validatePluginContents}.
 */
export interface PluginUserConfigOption {
  type: UserConfigOptionType;
  title: string;
  description: string;
  required?: boolean;
  default?: string | number | boolean | string[];
  multiple?: boolean;
  sensitive?: boolean;
  min?: number;
  max?: number;
}

/** A declared option schema: option key → its declaration (Claude parity:
 *  PluginOptionSchema / UserConfigSchema). */
export type PluginOptionSchema = Record<string, PluginUserConfigOption>;
/** A resolved option value (Claude parity: a `directory`/`file`/`string`
 *  scalar, a coerced `number`/`boolean`, or a `multiple` string array). */
export type PluginOptionValue = string | number | boolean | string[];
/** Saved/collected option values keyed by option key (Claude parity:
 *  PluginOptionValues / UserConfigValues). */
export type PluginOptionValues = Record<string, PluginOptionValue>;

/** Tokens a plugin `boolean` option value coerces to `true` (Claude parity:
 *  isEnvTruthy's allow-list). NOTE: this is a DIFFERENT contract from core's
 *  env-flag {@link isEnvTruthy} (a deny-list) — a plugin boolean option is
 *  truthy ONLY for an explicit affirmative token, so an unrecognized value is
 *  `false`, not `true`. */
const PLUGIN_OPTION_TRUTHY = new Set(['1', 'true', 'yes', 'on']);

/**
 * Coerce a plugin `boolean` option's string input to a boolean (Claude parity:
 * isEnvTruthy applied in buildFinalValues). Truthy only for an explicit
 * affirmative token ({@link PLUGIN_OPTION_TRUTHY}); every other value — including
 * blank and unrecognized text — is `false`.
 */
export function coercePluginOptionBool(value: string): boolean {
  return PLUGIN_OPTION_TRUTHY.has(value.trim().toLowerCase());
}

/**
 * Build the save payload from the raw string inputs the config dialog collected,
 * coercing each by its declared type (Claude parity: PluginOptionsDialog
 * `buildFinalValues`). The behavior-defining edge cases, matched exactly:
 *
 *   - A `sensitive` field left blank on RECONFIGURE (a value already exists in
 *     `initialValues`) is OMITTED — storage only writes keys it receives, so an
 *     omitted key keeps the existing secret instead of wiping it.
 *   - A `number` field left blank is OMITTED (`Number('')` is `0`, not `NaN`, so
 *     keeping it would smuggle a `0` past the required-field check).
 *   - A `number` field whose input is non-numeric falls back to the raw STRING
 *     (so {@link validatePluginOptionValues} can report a meaningful type error
 *     rather than storing `NaN`).
 *   - A `boolean` field is coerced via {@link coercePluginOptionBool}.
 *   - Everything else is stored as the entered string.
 */
export function coercePluginOptionValues(
  fields: string[],
  collected: Record<string, string>,
  schema: PluginOptionSchema,
  initialValues?: PluginOptionValues,
): PluginOptionValues {
  const finalValues: PluginOptionValues = {};
  for (const fieldKey of fields) {
    const opt = schema[fieldKey];
    const value = collected[fieldKey] ?? '';
    if (opt?.sensitive === true && value === '' && initialValues?.[fieldKey] !== undefined) {
      continue;
    }
    if (opt?.type === 'number') {
      if (value.trim() === '') continue;
      const num = Number(value);
      finalValues[fieldKey] = Number.isNaN(num) ? value : num;
    } else if (opt?.type === 'boolean') {
      finalValues[fieldKey] = coercePluginOptionBool(value);
    } else {
      finalValues[fieldKey] = value;
    }
  }
  return finalValues;
}

/**
 * Compute the per-field initial string the config dialog should pre-fill when
 * RECONFIGURING (Claude parity: PluginOptionsDialog's `initialFor`). A
 * `sensitive` field is NEVER pre-filled (the stored secret is not echoed into
 * the input buffer); any other field is pre-filled with the stringified saved
 * value, or `''` when nothing is saved. The returned map is keyed by every
 * option in `schema`.
 */
export function buildPluginOptionPrefill(
  schema: PluginOptionSchema,
  initialValues?: PluginOptionValues,
): Record<string, string> {
  const prefill: Record<string, string> = {};
  for (const key of Object.keys(schema)) {
    if (schema[key]?.sensitive === true) {
      prefill[key] = '';
      continue;
    }
    const v = initialValues?.[key];
    prefill[key] = v === undefined ? '' : String(v);
  }
  return prefill;
}

/**
 * Validate resolved option VALUES against a declared schema (Claude parity: the
 * required/type checks `validateUserConfig` applies to saved values). Reports a
 * human-readable error per offending field:
 *   - a `required` option that is absent / blank / an empty array;
 *   - a `number` option whose value is not a finite number (or is out of a
 *     declared `min`/`max` range);
 *   - a `multiple` string option whose value is not a string array;
 *   - a non-`multiple` scalar option given an array.
 * An absent OPTIONAL option is fine. Returns `{ valid, errors }`.
 */
export function validatePluginOptionValues(
  values: PluginOptionValues,
  schema: PluginOptionSchema,
): { valid: boolean; errors: string[] } {
  const errors: string[] = [];
  for (const [key, opt] of Object.entries(schema)) {
    const value = values[key];
    const label = opt.title && opt.title.trim() !== '' ? opt.title : key;
    const absent =
      value === undefined ||
      (typeof value === 'string' && value.trim() === '') ||
      (Array.isArray(value) && value.length === 0);
    if (absent) {
      if (opt.required === true) errors.push(`Missing required value for "${label}".`);
      continue;
    }
    if (opt.multiple === true) {
      if (!Array.isArray(value) || !value.every(v => typeof v === 'string')) {
        errors.push(`"${label}" must be a list of strings.`);
      }
      continue;
    }
    if (Array.isArray(value)) {
      errors.push(`"${label}" must be a single value, not a list.`);
      continue;
    }
    if (opt.type === 'number') {
      const num = typeof value === 'number' ? value : Number(value);
      if (!Number.isFinite(num)) {
        errors.push(`"${label}" must be a number.`);
      } else if (opt.min !== undefined && num < opt.min) {
        errors.push(`"${label}" must be at least ${opt.min}.`);
      } else if (opt.max !== undefined && num > opt.max) {
        errors.push(`"${label}" must be at most ${opt.max}.`);
      }
    }
  }
  return { valid: errors.length === 0, errors };
}

/**
 * Return the subset of a plugin's option schema whose SAVED values are still
 * missing/invalid (Claude parity: pluginOptionsStorage `getUnconfiguredOptions`).
 * Used to decide, after enabling a plugin, whether the user must be prompted —
 * already-configured fields are skipped so a reconfigure never re-asks for a
 * value that already validates. Returns an empty schema when everything is
 * configured.
 */
export function getUnconfiguredPluginOptions(
  schema: PluginOptionSchema,
  saved: PluginOptionValues,
): PluginOptionSchema {
  if (Object.keys(schema).length === 0) return {};
  if (validatePluginOptionValues(saved, schema).valid) return {};
  const unconfigured: PluginOptionSchema = {};
  for (const [key, opt] of Object.entries(schema)) {
    const single = validatePluginOptionValues(
      key in saved ? { [key]: saved[key] } : {},
      { [key]: opt },
    );
    if (!single.valid) unconfigured[key] = opt;
  }
  return unconfigured;
}

/**
 * Validate a parsed plugin manifest object. Checks required `name`/`version`
 * strings (with a kebab-case name refinement), validates the optional metadata
 * fields (description/author/homepage/repository/license/keywords) via
 * {@link validateManifestMetadata}, flags any declared component path that
 * contains `..` (would escape the plugin dir) or is absolute, and warns about
 * marketplace-only fields that are silently ignored at runtime.
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
  } else if (obj.name.includes(' ')) {
    // Claude parity (PluginManifestMetadataSchema name refine): a name is used for
    // namespacing, so a space would break the `plugin:command` lookup — reject it
    // with a corrective kebab-case example rather than silently namespacing on it.
    errors.push({ path: 'name', message: 'Plugin name cannot contain spaces. Use kebab-case (e.g., "my-plugin").' });
  }

  if (obj.version !== undefined && typeof obj.version !== 'string') {
    errors.push({ path: 'version', message: '"version" must be a string.' });
  }

  validateManifestMetadata(obj, errors);
  validateCommandsMetadata(obj.commandsMetadata, errors);
  validateUserConfig(obj.userConfig, errors);
  validatePluginChannels(obj.channels, obj, errors);
  validatePluginRequirements(obj.requires, errors);
  validatePluginSettings(obj.settings, errors, warnings);

  for (const field of COMPONENT_PATH_FIELDS) {
    validateComponentPathField(field, obj[field], errors);
  }

  for (const field of SERVER_DECLARATION_FIELDS) {
    validateServerDeclarations(field, obj[field], errors);
  }

  for (const field of MARKETPLACE_ONLY_MANIFEST_FIELDS) {
    if (field in obj) {
      warnings.push({ path: field, message: `"${field}" belongs in marketplace.json, not plugin.json — it is ignored here.` });
    }
  }

  return { valid: errors.length === 0, errors, warnings };
}

/** The action `type` a plugin hook entry may declare (Claude parity: HookConfigType
 *  — mirrors core/core/src/hooks.ts HOOK_CONFIG_TYPES without importing the heavy
 *  HookManager graph into this building-block module). */
const PLUGIN_HOOK_ACTION_TYPES = ['command', 'prompt', 'http', 'agent'] as const;

/**
 * Structurally validate one hook entry, recursing into the Claude matcher form
 * `{ matcher?, hooks: [ { type, command } ] }`. A declared `type` must be a known
 * action type and a `command`-type action must carry a non-empty `command` string
 * (the security hardening of the command itself runs at hook LOAD time via
 * `validateHookCommand`; this is the author-facing STRUCTURAL lint).
 */
function validateHookAction(obj: unknown, label: string, errors: PluginValidationIssue[]): void {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) {
    errors.push({ path: label, message: 'Hook entry must be an object.' });
    return;
  }
  const o = obj as Record<string, unknown>;
  if (Array.isArray(o.hooks)) {
    o.hooks.forEach((h, i) => validateHookAction(h, `${label}.hooks[${i}]`, errors));
    return;
  }
  if (o.type !== undefined && (typeof o.type !== 'string' || !(PLUGIN_HOOK_ACTION_TYPES as readonly string[]).includes(o.type))) {
    errors.push({ path: `${label}.type`, message: `Hook "type" must be one of: ${PLUGIN_HOOK_ACTION_TYPES.join(', ')}.` });
  }
  if (o.type === 'command' && (typeof o.command !== 'string' || o.command.trim() === '')) {
    errors.push({ path: `${label}.command`, message: 'A "command" hook must declare a non-empty "command" string.' });
  }
}

/**
 * Validate the STRUCTURE of a parsed plugin `hooks.json` (Claude parity:
 * PluginHooksSchema, utils/plugins/validatePlugin.ts L640-711). Claude's loader
 * HARD-parses hooks.json (`.parse()`, not `.safeParse()`) so a malformed file breaks
 * the whole plugin load — surfacing the structural errors here (instead of only
 * catching a JSON syntax error) lets an author fix it before it ships. Accepts both
 * the AMI flat-array form (`[ { event, type, command } ]`) and the Claude map-keyed
 * form (`{ EventName: [ { matcher?, hooks: [...] } ] }`, optionally wrapped in a top
 * `hooks` key). Event-NAME membership is intentionally NOT checked — AMI translates
 * Claude's PascalCase event names via normalizeMapKeyedHooks, so a cross-framework
 * manifest must validate. Returns the issues (empty when structurally sound).
 */
export function validatePluginHooks(parsed: unknown): PluginValidationIssue[] {
  const errors: PluginValidationIssue[] = [];
  if (parsed === null || typeof parsed !== 'object') {
    errors.push({ path: 'hooks', message: 'hooks.json must be a JSON object (event → matchers) or an array of hook entries.' });
    return errors;
  }
  if (Array.isArray(parsed)) {
    parsed.forEach((entry, i) => validateHookAction(entry, `hooks[${i}]`, errors));
    return errors;
  }
  const raw = parsed as Record<string, unknown>;
  // Claude map-keyed form may wrap the event map in a top-level `hooks` key.
  const map = raw.hooks && typeof raw.hooks === 'object' && !Array.isArray(raw.hooks)
    ? (raw.hooks as Record<string, unknown>)
    : raw;
  for (const [event, matchers] of Object.entries(map)) {
    if (!Array.isArray(matchers)) {
      errors.push({ path: `hooks.${event}`, message: `Hooks for "${event}" must be an array of hook matchers.` });
      continue;
    }
    matchers.forEach((m, i) => validateHookAction(m, `hooks.${event}[${i}]`, errors));
  }
  return errors;
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
        // SKILL.md is matched case-insensitively (Claude parity), so a subdir
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
        // Markdown command: the body IS the prompt (Claude format), so a non-empty
        // body is what makes the command runnable; description is optional.
        if (!parseFrontmatter(content).body.trim()) {
          errors.push({ path: `commands/${entry}`, message: 'markdown command must have a non-empty body (the prompt).' });
        }
      } else if (!parseTomlString(content, 'description') || !parseTomlString(content, 'prompt')) {
        errors.push({ path: `commands/${entry}`, message: 'command TOML must define both "description" and "prompt".' });
      }
    }
    // Affordance warning (Claude parity: loadPluginCommands warns when a commands
    // directory yields nothing) — a commands/ dir that loads no runnable command
    // (recursively) is almost always an author mistake, so name the expected
    // format instead of silently contributing zero commands.
    if (loadCommands(commandsDir).length === 0) {
      warnings.push({ path: 'commands', message: 'commands/ directory has no loadable commands — expected .toml or .md command files (optionally nested in subdirectories).' });
    }
  }

  // hooks.json (root) OR hooks/hooks.json (Claude Code) — must be valid JSON AND
  // structurally sound (Claude parity: PluginHooksSchema). A bad hooks.json hard-breaks
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

  // .mcp.json (Claude Code) — bundled MCP servers must be valid JSON; a symlink/..
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
    // A subdir SKILL.md can fall back to the dir name, so a missing name is only a
    // warning (SKILL.md matched case-insensitively, Claude parity).
    if (!frontmatter.name && !SKILL_MD_RE.test(path.basename(file))) {
      warnings.push({ path: label, message: 'skill has no "name" frontmatter; the file name will be used.' });
    }
  } else if (!frontmatter.description?.trim()) {
    warnings.push({ path: label, message: 'agent has no "description" frontmatter.' });
  }
}

/**
 * Locate a plugin's `plugin.json` under `root`, in priority order: AMI's
 * `.ami-plugin/`, then Claude Code's `.claude-plugin/`, then a legacy root-level
 * `plugin.json` (Claude parity: pluginLoader's `legacyManifestPath`). Returns the
 * first existing path, or undefined when the directory ships no manifest. SINGLE
 * source for the lookup order used by both discovery and validation.
 */
function resolveManifestPath(root: string): string | undefined {
  const candidates = [
    path.join(root, '.ami-plugin', 'plugin.json'),
    path.join(root, '.claude-plugin', 'plugin.json'),
    path.join(root, 'plugin.json'),
  ];
  return candidates.find(p => fs.existsSync(p));
}

/**
 * Full plugin validation: manifest + content files. Reads `plugin.json` from the
 * `.ami-plugin`/`.claude-plugin` dir — or a legacy root-level `plugin.json` —
 * under `pluginDir`, runs {@link validatePluginManifest} and
 * {@link validatePluginContents}, and merges the results. A missing manifest is a
 * hard error.
 */
export function validatePlugin(pluginDir: string): PluginValidationResult {
  const manifestPath = resolveManifestPath(pluginDir);
  if (manifestPath !== undefined) {
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
  return { valid: false, errors: [{ path: pluginDir, message: 'No plugin manifest (.ami-plugin/plugin.json, .claude-plugin/plugin.json, or root plugin.json).' }], warnings: [] };
}

/**
 * The kinds of component a plugin can contribute (Claude parity: PluginComponent,
 * types/plugin.ts). Used to tag a {@link PluginError} with the component it concerns.
 */
export const PLUGIN_COMPONENTS = ['commands', 'agents', 'skills', 'hooks', 'output-styles'] as const;
export type PluginComponent = (typeof PLUGIN_COMPONENTS)[number];

/**
 * Typed, discriminated plugin error (Claude parity: types/plugin.ts `PluginError`).
 * Each variant carries the CONTEXTUAL data its kind needs so a consumer can render
 * precise, actionable guidance WITHOUT string-matching an opaque message (which
 * breaks the instant the wording changes). Every variant carries `source` (the
 * plugin/marketplace/path the error came from) and most carry `plugin` (the bundle
 * name). This is the type-safe replacement for the previous free-form string reason.
 */
export type PluginError =
  | { type: 'manifest-parse-error'; source: string; plugin?: string; manifestPath: string; parseError: string }
  | { type: 'manifest-validation-error'; source: string; plugin?: string; manifestPath: string; validationErrors: string[] }
  | { type: 'path-not-found'; source: string; plugin?: string; path: string; component: PluginComponent }
  | { type: 'component-load-failed'; source: string; plugin: string; component: PluginComponent; path: string; reason: string }
  | { type: 'lsp-config-invalid'; source: string; plugin: string; serverName: string; validationError: string }
  | { type: 'mcp-config-invalid'; source: string; plugin: string; serverName: string; validationError: string }
  | { type: 'mcp-server-suppressed-duplicate'; source: string; plugin: string; serverName: string; duplicateOf: string }
  | { type: 'hook-load-failed'; source: string; plugin: string; hookPath: string; reason: string }
  | { type: 'path-traversal-blocked'; source: string; plugin?: string; declaredPath: string; component: PluginComponent }
  | { type: 'generic-error'; source: string; plugin?: string; error: string };

/**
 * Render a {@link PluginError} to a single display string (Claude parity:
 * getPluginErrorMessage). Exhaustive over the union — adding a variant without a
 * case is a compile error — so the message set can never drift out of sync with the
 * type. The SINGLE canonical formatter for every plugin-error surface (installer,
 * `/plugin`, diagnostics).
 */
export function getPluginErrorMessage(error: PluginError): string {
  switch (error.type) {
    case 'manifest-parse-error':
      return `Manifest parse error: ${error.parseError}`;
    case 'manifest-validation-error':
      return `Manifest validation failed: ${error.validationErrors.join(', ')}`;
    case 'path-not-found':
      return `Path not found: ${error.path} (${error.component})`;
    case 'component-load-failed':
      return `${error.component} load failed from ${error.path}: ${error.reason}`;
    case 'lsp-config-invalid':
      return `Plugin "${error.plugin}" has invalid LSP server config for "${error.serverName}": ${error.validationError}`;
    case 'mcp-config-invalid':
      return `Plugin "${error.plugin}" has invalid MCP server config for "${error.serverName}": ${error.validationError}`;
    case 'mcp-server-suppressed-duplicate': {
      const dup = error.duplicateOf.startsWith('plugin:')
        ? `server provided by plugin "${error.duplicateOf.slice('plugin:'.length)}"`
        : `already-configured "${error.duplicateOf}"`;
      return `MCP server "${error.serverName}" skipped — same command/URL as ${dup}`;
    }
    case 'hook-load-failed':
      return `Hook load failed: ${error.reason}`;
    case 'path-traversal-blocked':
      return formatPluginPathTraversalWarning(error.plugin ?? error.source, error.declaredPath, error.component);
    case 'generic-error':
      return error.error;
  }
}

/**
 * The user-facing warning for a plugin-contributed path that escapes its own
 * root (Claude parity: lspPluginIntegration's "Security: Path traversal attempt
 * blocked in plugin …" message). Emitted whenever {@link hardenedResolve} rejects
 * a declared component path because its realpath leaves the plugin dir (a `..`
 * segment that survived manifest validation, or a symlink pointing outside) —
 * surfaced instead of being silently dropped so an author/operator can see the
 * containment violation.
 */
export function formatPluginPathTraversalWarning(plugin: string, declaredPath: string, component?: PluginComponent): string {
  const where = component ? ` (${component})` : '';
  return `Security: path traversal blocked in plugin "${plugin}": "${declaredPath}"${where} escapes the plugin directory and was not loaded.`;
}

/**
 * Stable dedup key for a {@link PluginError} (Claude parity: refresh.ts
 * `errorKey`). A `generic-error` is keyed by its source AND message (two generic
 * errors from the same source but different messages are distinct); every typed
 * variant is keyed by `type:source` (the same kind from the same source is one
 * error regardless of incidental detail).
 */
export function pluginErrorKey(e: PluginError): string {
  return e.type === 'generic-error'
    ? `generic-error:${e.source}:${e.error}`
    : `${e.type}:${e.source}`;
}

/**
 * Merge a freshly-recomputed plugin-error set into the existing one on reload,
 * preserving errors that a plain reload would otherwise strand (Claude parity:
 * refresh.ts `mergePluginErrors`). Errors sourced from the LSP manager
 * (`lsp-manager`) or from an individual plugin (`plugin:<name>`) are PRESERVED
 * across the reload — they are recomputed asynchronously/elsewhere, not by the
 * reload that produced `fresh`, so dropping them would lose live diagnostics.
 * A preserved error is dropped only when `fresh` already carries one with the
 * same {@link pluginErrorKey} (the fresh copy wins). All other existing errors
 * are replaced by `fresh`.
 */
export function mergePluginErrors(existing: PluginError[], fresh: PluginError[]): PluginError[] {
  const preserved = existing.filter(
    e => e.source === 'lsp-manager' || e.source.startsWith('plugin:'),
  );
  const freshKeys = new Set(fresh.map(pluginErrorKey));
  const deduped = preserved.filter(e => !freshKeys.has(pluginErrorKey(e)));
  return [...deduped, ...fresh];
}

/**
 * A plugin directory that failed to load, with an actionable reason. `error`
 * carries the TYPED {@link PluginError} (Claude parity) so a consumer can branch on
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
 *  (Claude parity: NON_ASCII_PATTERN, utils/plugins/schemas.ts). */
const NON_ASCII_NAME_RE = /[^ -~]/;

/**
 * True when `name` is safe to use verbatim as a plugin's namespace segment: it
 * passes the path-safety segment check (no slashes, `..`, or NUL), contains no
 * whitespace, and is ASCII-only (Claude parity: the plugin-name schema refinement
 * rejects spaces/path separators, and the homograph guard rejects non-ASCII).
 * A manifest name that fails this is not fatal — discovery falls back to the
 * directory name so the bundle still loads under a usable, namespacing-safe id.
 */
export function isNamespaceSafeName(name: string): boolean {
  return isSafeSegment(name) && !/\s/.test(name) && !NON_ASCII_NAME_RE.test(name);
}

/**
 * True when `err` is a filesystem "no such file or directory" error (errno code
 * `ENOENT`), matching Claude Code's `isENOENT` guard (utils/errors.ts). Used to
 * distinguish an EXPECTED-absent path (a plugin that simply ships none of an
 * optional component — swallow it) from a REAL I/O error (EACCES/ENOTDIR/… —
 * surface it) so a genuine misconfiguration is not silently discarded.
 */
export function isENOENT(err: unknown): boolean {
  return !!err && typeof err === 'object' && (err as NodeJS.ErrnoException).code === 'ENOENT';
}

/** Compact, non-throwing message for a caught error (used in load-failure reasons). */
function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Sanitize one segment of a plugin/extension id (Claude parity: the `sanitize`
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

/** Prefixes a deterministic plugin id may carry (Claude parity: local unpacked vs
 *  packaged `.dxt`/`.mcpb` bundle). */
export type PluginIdPrefix = 'local.unpacked' | 'local.dxt';

/**
 * Generate a deterministic plugin/extension id from an author name and a plugin
 * name (Claude parity: generateExtensionId, utils/dxt/helpers.ts). Each part is
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
 * Normalize a plugin's git-subdir path EXACTLY as Claude Code's version/cache-key
 * encoder does (Claude parity: calculatePluginVersion's git-subdir branch,
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
 * Hash a plugin's git-subdir path into the short digest Claude Code folds into a
 * git-subdir plugin's version/cache key (Claude parity: calculatePluginVersion
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
 * Normalize a manifest `author` (string or `{ name, … }` object — Claude parity)
 * to a non-empty display-name string, or undefined when absent/blank/malformed.
 */
function manifestAuthorName(author: unknown): string | undefined {
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

/**
 * Canonical dedup key for an MCP server: its stdio `command` + `args`, or its
 * `url`. Two servers with the same key launch the same thing and are duplicates.
 */
function mcpServerKey(server: PluginMcpServer): string {
  if (typeof server.url === 'string' && server.url.trim() !== '') return `url:${server.url.trim()}`;
  const cmd = typeof server.command === 'string' ? server.command.trim() : '';
  const args = Array.isArray(server.args) ? server.args.map(a => String(a)).join(' ') : '';
  return `cmd:${cmd} ${args}`.trim();
}

/**
 * Merge plugin-contributed MCP servers across bundles, SUPPRESSING duplicates by
 * launch target (Claude parity: types/plugin.ts `mcp-server-suppressed-duplicate`).
 * `existing` names already-configured servers (name → config) that a plugin server
 * must not shadow. Entries are merged in order; the first occurrence of a given
 * {@link mcpServerKey} wins and every later same-target server is dropped with a
 * typed {@link PluginError} naming what it duplicated. Returns the deduped map plus
 * the suppression errors so a consumer can surface the transparency message.
 */
export function mergePluginMcpServers(
  contributions: Array<{ plugin: string; source: string; servers: Record<string, PluginMcpServer> }>,
  existing: Record<string, PluginMcpServer> = {},
): { servers: Record<string, PluginMcpServer>; errors: PluginError[] } {
  const servers: Record<string, PluginMcpServer> = {};
  const errors: PluginError[] = [];
  const keyOwner = new Map<string, string>();
  for (const [name, cfg] of Object.entries(existing)) {
    servers[name] = cfg;
    keyOwner.set(mcpServerKey(cfg), name);
  }
  for (const { plugin, source, servers: contributed } of contributions) {
    for (const [serverName, cfg] of Object.entries(contributed)) {
      const key = mcpServerKey(cfg);
      const owner = keyOwner.get(key);
      if (owner !== undefined) {
        errors.push({ type: 'mcp-server-suppressed-duplicate', source, plugin, serverName, duplicateOf: owner === serverName ? `plugin:${plugin}` : owner });
        continue;
      }
      keyOwner.set(key, serverName);
      servers[serverName] = cfg;
    }
  }
  return { servers, errors };
}

/**
 * Resolve a manifest field declaring ADDITIONAL component locations (Claude parity:
 * `commandsPaths` / `skillsPaths`): a string, or an array of strings, each a
 * relative path hardened against `..`/symlink escape. Returns the existing,
 * contained absolute paths, or undefined when none are declared/survive.
 */
function resolveManifestPathList(root: string, manifest: Record<string, unknown>, field: string): string[] | undefined {
  const raw = manifest[field];
  const entries = typeof raw === 'string' ? [raw] : Array.isArray(raw) ? raw : [];
  const out: string[] = [];
  for (const e of entries) {
    if (typeof e !== 'string' || e.trim() === '') continue;
    const resolved = hardenedResolve(root, e);
    if (resolved !== undefined) out.push(resolved);
  }
  return out.length > 0 ? out : undefined;
}

/**
 * Parse a manifest's `commandsMetadata` object-mapping into a normalized
 * {@link CommandMetadata} record, keeping only well-formed entries (Claude parity).
 * An entry is kept when it is an object carrying EXACTLY ONE of `source`/`content`;
 * `argument-hint` is accepted as an alias for `argumentHint` (TOML/Claude frontmatter
 * spelling). Returns undefined when the field is absent or no entry survives, so the
 * bundle carries a value only when it actually contributes command metadata.
 */
function parseCommandsMetadata(manifest: Record<string, unknown>): Record<string, CommandMetadata> | undefined {
  const raw = manifest.commandsMetadata;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  const out: Record<string, CommandMetadata> = {};
  for (const [key, val] of Object.entries(raw as Record<string, unknown>)) {
    if (!val || typeof val !== 'object' || Array.isArray(val)) continue;
    const v = val as Record<string, unknown>;
    const entry: CommandMetadata = {};
    if (typeof v.source === 'string') entry.source = v.source;
    if (typeof v.content === 'string') entry.content = v.content;
    if (typeof v.description === 'string') entry.description = v.description;
    const argHint = v.argumentHint ?? v['argument-hint'];
    if (typeof argHint === 'string') entry.argumentHint = argHint;
    if (typeof v.model === 'string') entry.model = v.model;
    if (Array.isArray(v.allowedTools) && v.allowedTools.every(t => typeof t === 'string')) {
      entry.allowedTools = v.allowedTools as string[];
    }
    const hasSource = entry.source !== undefined && entry.source.trim() !== '';
    const hasContent = entry.content !== undefined && entry.content.trim() !== '';
    if (hasSource === hasContent) continue; // XOR: skip malformed entries at discovery
    out[key] = entry;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Context for evaluating {@link isPluginAvailable}: the current OS platform and
 * environment. Injectable so availability is deterministically unit-testable
 * (defaults to the live `process.platform` / `process.env`).
 */
export interface PluginAvailabilityContext {
  platform?: string;
  env?: Record<string, string | undefined>;
}

/** True when `name` resolves to an executable file on the PATH in `env`. */
function binOnPath(name: string, env: Record<string, string | undefined>): boolean {
  // An explicit path (contains a separator) is checked directly, not via PATH.
  if (name.includes('/') || name.includes('\\')) {
    try {
      return fs.statSync(name).isFile();
    } catch {
      return false;
    }
  }
  const pathVar = env.PATH ?? env.Path ?? '';
  for (const dir of pathVar.split(path.delimiter)) {
    if (dir === '') continue;
    try {
      if (fs.statSync(path.join(dir, name)).isFile()) return true;
    } catch {
      /* keep scanning */
    }
  }
  return false;
}

/**
 * Evaluate a bundle's declarative {@link PluginRequirements} (Claude parity:
 * BuiltinPluginDefinition.isAvailable — a plugin whose environment prerequisites
 * aren't met is omitted entirely). Returns true when EVERY declared constraint is
 * satisfied: the current `platform` is allowed (when the list is non-empty), every
 * `env` var is set+non-empty, and every `bin` resolves on PATH. `undefined`/empty
 * constraints impose no requirement, so a plugin with no `requires` is always
 * available.
 */
export function isPluginAvailable(requires: PluginRequirements | undefined, ctx: PluginAvailabilityContext = {}): boolean {
  if (requires === undefined) return true;
  const platform = ctx.platform ?? process.platform;
  const env = ctx.env ?? process.env;
  if (Array.isArray(requires.platform) && requires.platform.length > 0 && !requires.platform.includes(platform)) {
    return false;
  }
  if (Array.isArray(requires.env)) {
    for (const name of requires.env) {
      const v = env[name];
      if (typeof v !== 'string' || v === '') return false;
    }
  }
  if (Array.isArray(requires.bin)) {
    for (const name of requires.bin) {
      if (!binOnPath(name, env)) return false;
    }
  }
  return true;
}

/**
 * Parse a manifest's optional `requires` gate into {@link PluginRequirements},
 * keeping only the well-formed string-array constraints. Returns undefined when
 * the field is absent or carries no usable constraint, so a bundle only records a
 * value when it actually declares a requirement.
 */
function parsePluginRequirements(manifest: Record<string, unknown>): PluginRequirements | undefined {
  const raw = manifest.requires;
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return undefined;
  const r = raw as Record<string, unknown>;
  const out: PluginRequirements = {};
  for (const key of ['platform', 'env', 'bin'] as const) {
    const v = r[key];
    if (Array.isArray(v)) {
      const strings = v.filter((x): x is string => typeof x === 'string' && x.trim() !== '');
      if (strings.length > 0) out[key] = strings;
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/** True for a plain (non-array) object — an inline server-config record. */
function isPlainObject(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Parse inline MCP server declarations from the manifest `mcpServers` field
 * (Claude parity: PluginManifestMcpServerSchema). The field may be a record of
 * `name → config`, or an ARRAY mixing file-path strings (handled separately via
 * {@link PluginBundle.mcpServersFile}) with inline records. Only the inline
 * record entries are collected here; later records override earlier same-named
 * ones. Returns undefined when there are no inline servers.
 */
function parseInlineMcpServers(manifest: Record<string, unknown>): Record<string, PluginMcpServer> | undefined {
  const raw = manifest.mcpServers;
  const out: Record<string, PluginMcpServer> = {};
  const absorb = (rec: Record<string, unknown>): void => {
    for (const [name, cfg] of Object.entries(rec)) {
      if (isPlainObject(cfg)) out[name] = cfg as PluginMcpServer;
    }
  };
  if (isPlainObject(raw)) {
    absorb(raw);
  } else if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (isPlainObject(entry)) absorb(entry);
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Parse a channel's `userConfig` into a normalized {@link PluginOptionSchema},
 * keeping only well-formed options (an object with a valid `type`/`title`/
 * `description`). Mirrors the manifest `userConfig` shape so the config dialog and
 * option storage consume a channel's fields exactly like top-level ones. Returns
 * undefined when nothing well-formed remains.
 */
function parseChannelUserConfig(raw: unknown): PluginOptionSchema | undefined {
  if (!isPlainObject(raw)) return undefined;
  const out: PluginOptionSchema = {};
  for (const [key, opt] of Object.entries(raw)) {
    if (!isPlainObject(opt)) continue;
    const type = opt.type;
    if (typeof type !== 'string' || !(USER_CONFIG_OPTION_TYPES as readonly string[]).includes(type)) continue;
    if (typeof opt.title !== 'string' || typeof opt.description !== 'string') continue;
    const entry: PluginUserConfigOption = { type: type as UserConfigOptionType, title: opt.title, description: opt.description };
    if (typeof opt.required === 'boolean') entry.required = opt.required;
    if (typeof opt.multiple === 'boolean') entry.multiple = opt.multiple;
    if (typeof opt.sensitive === 'boolean') entry.sensitive = opt.sensitive;
    if (typeof opt.min === 'number') entry.min = opt.min;
    if (typeof opt.max === 'number') entry.max = opt.max;
    if (isValidUserConfigDefault(opt.default)) entry.default = opt.default as PluginUserConfigOption['default'];
    out[key] = entry;
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Parse the manifest `channels` array into normalized {@link PluginChannel}s,
 * keeping only well-formed entries (Claude parity: PluginManifestChannelsSchema).
 * An entry survives when it is an object with a non-empty `server` string; its
 * optional `displayName` (string) and `userConfig` (parsed via
 * {@link parseChannelUserConfig}) are carried when well-formed. Returns undefined
 * when the field is absent or no entry survives, so the bundle records channels
 * only when it actually contributes one. The `server`↔`mcpServers` cross-check is
 * deferred to load time (discovery stays lenient).
 */
function parsePluginChannels(manifest: Record<string, unknown>): PluginChannel[] | undefined {
  const raw = manifest.channels;
  if (!Array.isArray(raw)) return undefined;
  const out: PluginChannel[] = [];
  for (const entry of raw) {
    if (!isPlainObject(entry)) continue;
    const server = entry.server;
    if (typeof server !== 'string' || server.trim() === '') continue;
    const channel: PluginChannel = { server };
    if (typeof entry.displayName === 'string') channel.displayName = entry.displayName;
    const userConfig = parseChannelUserConfig(entry.userConfig);
    if (userConfig !== undefined) channel.userConfig = userConfig;
    out.push(channel);
  }
  return out.length > 0 ? out : undefined;
}

/**
 * Parse LSP server declarations from the manifest `lspServers` field (Claude
 * parity: PluginManifestLspServerSchema — string path, inline record, or an array
 * mixing both). A string/array-string entry is resolved to a hardened `.lsp.json`
 * FILE path; record entries become inline {@link LspServerConfig}s validated via
 * the canonical {@link validateLspServerConfig}. Malformed inline configs are
 * dropped (discovery stays lenient; strict surfacing is {@link validatePluginManifest}).
 */
function parseManifestLspServers(
  root: string,
  manifest: Record<string, unknown>,
): { file?: string; servers?: Record<string, LspServerConfig> } {
  const raw = manifest.lspServers;
  let file: string | undefined;
  const servers: Record<string, LspServerConfig> = {};
  const absorbRecord = (rec: Record<string, unknown>): void => {
    for (const [name, cfg] of Object.entries(rec)) {
      try {
        servers[name] = validateLspServerConfig(name, cfg);
      } catch {
        /* lenient at discovery */
      }
    }
  };
  const absorbPath = (p: string): void => {
    const resolved = hardenedResolve(root, p);
    if (resolved !== undefined && file === undefined) file = resolved;
  };
  if (typeof raw === 'string' && raw.trim() !== '') {
    absorbPath(raw);
  } else if (isPlainObject(raw)) {
    absorbRecord(raw);
  } else if (Array.isArray(raw)) {
    for (const entry of raw) {
      if (typeof entry === 'string' && entry.trim() !== '') absorbPath(entry);
      else if (isPlainObject(entry)) absorbRecord(entry);
    }
  } else {
    // No manifest declaration: fall back to the <root>/.lsp.json convention.
    absorbPath('.lsp.json');
  }
  return {
    file,
    servers: Object.keys(servers).length > 0 ? servers : undefined,
  };
}

/**
 * Parse a raw settings object through the plugin allowlist (Claude parity:
 * parsePluginSettings / PluginSettingsSchema.pick({agent}).strip()): only the
 * `agent` key (a non-empty string) survives; every other key is stripped. Returns
 * undefined when nothing allowlisted remains, so a plugin records settings only
 * when it actually contributes one.
 */
export function parsePluginSettings(raw: unknown): PluginSettings | undefined {
  if (!isPlainObject(raw)) return undefined;
  const out: PluginSettings = {};
  if (typeof raw.agent === 'string' && raw.agent.trim() !== '') out.agent = raw.agent;
  return Object.keys(out).length > 0 ? out : undefined;
}

/**
 * Load a plugin's allowlisted settings (Claude parity: loadPluginSettings): a
 * `<root>/settings.json` file takes priority over a manifest `settings` object;
 * only allowlisted keys survive {@link parsePluginSettings}. Returns undefined
 * when neither source yields an allowlisted setting.
 */
function loadPluginSettings(root: string, manifest: Record<string, unknown>): PluginSettings | undefined {
  const settingsPath = hardenedResolve(root, 'settings.json');
  if (settingsPath !== undefined) {
    try {
      const parsed = JSON.parse(fs.readFileSync(settingsPath, 'utf-8')) as unknown;
      const fromFile = parsePluginSettings(parsed);
      if (fromFile !== undefined) return fromFile;
    } catch {
      /* missing/corrupt settings.json falls through to manifest.settings */
    }
  }
  return parsePluginSettings(manifest.settings);
}

/**
 * Discover local plugin bundles under `dir`, ALSO returning structured load
 * failures (invalid JSON / failed manifest validation) so callers can surface
 * actionable guidance instead of silently dropping a broken plugin. A directory
 * with no manifest at all is not a failure (it simply isn't a plugin).
 *
 * The top-level directory read distinguishes ENOENT (the plugins dir simply does
 * not exist yet — not a failure) from a real I/O error (e.g. ENOTDIR/EACCES),
 * which is surfaced as a {@link PluginLoadFailure} rather than silently yielding
 * an empty list (Claude parity: isENOENT discrimination).
 */
export function discoverPluginsDetailed(dir: string): { bundles: PluginBundle[]; failures: PluginLoadFailure[] } {
  let entries: string[];
  try {
    entries = fs.readdirSync(dir);
  } catch (err) {
    if (isENOENT(err)) return { bundles: [], failures: [] };
    return { bundles: [], failures: [{ root: dir, reason: `Failed to read plugins directory: ${errMessage(err)}` }] };
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
    // Manifest lookup order: AMI's `.ami-plugin/`, then Claude Code's
    // `.claude-plugin/`, then a legacy root-level `plugin.json` (Claude parity:
    // pluginLoader's `legacyManifestPath` fallback) so older bundles that ship the
    // manifest at their root still load.
    const manifestPath = resolveManifestPath(root);
    if (manifestPath === undefined) continue; // not a plugin dir — not a failure
    let manifest: Record<string, unknown>;
    try {
      const parsed = JSON.parse(fs.readFileSync(manifestPath, 'utf-8')) as unknown;
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        failures.push({ root, reason: `Invalid plugin.json: manifest must be a JSON object. Check ${manifestPath}.`, error: { type: 'manifest-validation-error', source: root, manifestPath, validationErrors: ['Manifest must be a JSON object.'] } });
        continue;
      }
      manifest = parsed as Record<string, unknown>;
    } catch (err) {
      // CORRUPT (unparseable JSON) is reported distinctly from INVALID (parses but
      // fails the schema) so a front-end can guide the author appropriately.
      failures.push({ root, reason: `Corrupt plugin.json (JSON parse failed): ${errMessage(err)}. Fix the JSON syntax in ${manifestPath}.`, error: { type: 'manifest-parse-error', source: root, manifestPath, parseError: errMessage(err) } });
      continue;
    }
    // Security-relevant manifest errors (unsafe name, '..'/absolute component path)
    // turn a discovered dir into a structured failure rather than a silently-loaded
    // bundle. Lenient fields (missing name/version) still fall back below.
    const hardErrors = validatePluginManifest(manifest).errors.filter(e =>
      /Unsafe plugin name|contains "\.\."|must be relative/.test(e.message),
    );
    if (hardErrors.length > 0) {
      failures.push({ root, reason: `Invalid plugin.json: ${hardErrors.map(e => `${e.path}: ${e.message}`).join('; ')}`, error: { type: 'manifest-validation-error', source: root, plugin: typeof manifest.name === 'string' ? manifest.name : undefined, manifestPath, validationErrors: hardErrors.map(e => `${e.path}: ${e.message}`) } });
      continue;
    }
    // Discovery is lenient on optional fields (name/version fall back below); the
    // strict schema check is exposed separately via validatePluginManifest for
    // the `/plugin validate` command. A manifest name that is present but unusable
    // for namespacing (spaces / non-ASCII homographs — soft issues that are NOT
    // hard failures above) falls back to the directory name so `plugin:command`
    // lookups never break on an invalid id.
    const manifestName = manifest.name;
    const name = typeof manifestName === 'string' && isNamespaceSafeName(manifestName) ? manifestName : entry;
    const description = typeof manifest.description === 'string' && manifest.description.trim() !== '' ? manifest.description.trim() : undefined;
    const author = manifestAuthorName(manifest.author);
    // Availability gate (Claude parity: BuiltinPluginDefinition.isAvailable). A
    // bundle whose declarative `requires` (platform/env/bin) are not met by the
    // current environment is OMITTED ENTIRELY — not surfaced as a failure, exactly
    // like Claude's getBuiltinPlugins `continue`.
    const requires = parsePluginRequirements(manifest);
    if (!isPluginAvailable(requires)) continue;
    const lsp = parseManifestLspServers(root, manifest);
    // Surface a plugin-contributed component path that escapes the plugin root
    // (a symlink/`..` that survived manifest validation) as a security warning
    // instead of silently dropping it (Claude parity: lspPluginIntegration's
    // path-traversal warning). The bundle still loads without that component.
    const reportViolation = (component: PluginComponent) => (rel: string) => {
      const err: PluginError = { type: 'path-traversal-blocked', source: root, plugin: name, declaredPath: rel, component };
      failures.push({ root, reason: getPluginErrorMessage(err), error: err });
    };
    bundles.push({
      name,
      version: (manifest.version as string | undefined) ?? '0.0.0',
      root,
      description,
      author,
      // Deterministic, collision-resistant id derived from author + name (Claude
      // parity: generateExtensionId). Only set when an author is declared, since
      // the id is anchored on it.
      id: author !== undefined ? generatePluginId(author, name) : undefined,
      defaultEnabled: typeof manifest.defaultEnabled === 'boolean' ? manifest.defaultEnabled : undefined,
      // Component paths: honor a manifest-declared relative override (Claude parity)
      // and fall back to the fixed default directory when none is declared. The
      // override was already vetted against `..`/absolute by the hardErrors gate
      // above; resolveComponentDir additionally hardens it against symlink escape.
      skillsDir: resolveComponentDir(root, manifest, 'skills', 'skills', reportViolation('skills')),
      agentsDir: resolveComponentDir(root, manifest, 'agents', 'agents', reportViolation('agents')),
      commandsDir: resolveComponentDir(root, manifest, 'commands', 'commands', reportViolation('commands')),
      // Hooks: a manifest-declared `hooks` file path overrides the convention of
      // <root>/hooks.json OR <root>/hooks/hooks.json (the Claude Code layout).
      hooksFile: resolvePluginHooksFromManifest(root, manifest),
      // MCP servers: a manifest-declared `mcpServers` file path overrides the
      // <root>/.mcp.json convention (Claude Code layout). Hardened like the rest.
      mcpServersFile: resolveComponentDir(root, manifest, 'mcpServers', '.mcp.json'),
      // Additional command/skill directories declared by the manifest, resolved +
      // hardened (Claude parity: commandsPaths / skillsPaths).
      commandsPaths: resolveManifestPathList(root, manifest, 'commandsPaths'),
      skillsPaths: resolveManifestPathList(root, manifest, 'skillsPaths'),
      // Additional agent directories/files (Claude parity: loadPluginAgents's
      // agentsPaths), resolved + hardened; loaded alongside the primary agents/ dir.
      agentsPaths: resolveManifestPathList(root, manifest, 'agentsPaths'),
      // Per-command metadata object-mapping (Claude parity: commandsMetadata) — kept
      // only when it carries at least one well-formed entry.
      commandsMetadata: parseCommandsMetadata(manifest),
      // Declarative availability requirements this bundle satisfied (see gate above).
      requires,
      // Inline MCP servers declared directly in the manifest (Claude parity:
      // PluginManifestMcpServerSchema inline-record form) — contributed alongside
      // any `.mcp.json` file at mcpServersFile.
      mcpServers: parseInlineMcpServers(manifest),
      // Plugin-contributed LSP servers (Claude parity: lspPluginIntegration): a
      // .lsp.json file path and/or inline manifest records, validated against the
      // canonical LspServerConfig schema.
      lspServersFile: lsp.file,
      lspServers: lsp.servers,
      // Output styles a plugin contributes (Claude parity: loadPluginOutputStyles):
      // the output-styles dir (+ manifest override) and extra outputStylesPaths.
      outputStylesDir: resolveComponentDir(root, manifest, 'outputStyles', 'output-styles', reportViolation('output-styles')),
      outputStylesPaths: resolveManifestPathList(root, manifest, 'outputStylesPaths'),
      // Allowlisted plugin-contributed settings (Claude parity: loadPluginSettings):
      // settings.json over manifest.settings, only the `agent` key survives.
      settings: loadPluginSettings(root, manifest),
      // Message channels the plugin declares (Claude parity: channels) — each binds
      // one of the plugin's MCP servers as a notification channel; kept only when at
      // least one entry carries a non-empty `server`.
      channels: parsePluginChannels(manifest),
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
