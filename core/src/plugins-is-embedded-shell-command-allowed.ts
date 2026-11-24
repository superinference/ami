// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import { isSafeSegment } from './skill-catalog';
import { permissionRuleValueFromString, isBashPrefixTool, matchBashPattern } from './permissions';

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
export const SSH_SCP_RE = /^[a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+:.+/;

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
export const GITHUB_SHORTHAND_RE = /^[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+$/;

/** A git transport-helper prefix, e.g. `ext::`, `fd::`, `file::` — these can run
 *  arbitrary programs and MUST be rejected. Matches `<scheme>::` but NOT a normal
 *  URL `<scheme>://` (single colon before the slashes). */
export const TRANSPORT_HELPER_RE = /^[a-zA-Z][a-zA-Z0-9+.-]*::/;

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
 * Declarative runtime-availability gate for a plugin bundle (the reference parity:
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
 * The allowlisted subset of settings a plugin may contribute (the reference parity:
 * PluginSettingsSchema.pick({ agent: true }).strip()). Only `agent` (a default
 * agent name) is honored; any other key an author writes is dropped at parse time.
 */
export interface PluginSettings {
  /** Default agent this plugin contributes to the settings cascade. */
  agent?: string;
}

/**
 * A command's declaration that it recommends installing a plugin (the reference parity:
 * PluginHintMenu's Props — a `<sourceCommand>` command suggests installing
 * `<pluginName>` from `<marketplaceName>`). A command/skill file declares
 * `recommend-plugin`/`recommend-marketplace` (+ optional `recommend-plugin-description`)
 * so the frontend can surface a command-driven plugin recommendation. Both the
 * plugin name and the marketplace are required for the recommendation to be usable.
 */
export interface PluginRecommendation {
  /** The plugin the command recommends installing. */
  pluginName: string;
  /** The marketplace the plugin is installed from. */
  marketplaceName: string;
  /** Optional human-readable blurb shown in the recommendation. */
  pluginDescription?: string;
}

/**
 * A fully-resolved plugin recommendation with its PROVENANCE (the reference parity:
 * PluginHintMenu's full `Props`, incl. `sourceCommand`). {@link buildPluginRecommendation}
 * stamps the originating command's user-facing name onto a command's
 * {@link PluginRecommendation} so the frontend can render
 * "the <sourceCommand> command suggests installing <pluginName> from <marketplaceName>".
 */
export interface PluginRecommendationHint extends PluginRecommendation {
  /** The command that triggered the recommendation (its user-facing name). */
  sourceCommand: string;
}

/**
 * The shells a command's embedded `!`-block execution may declare (the reference parity:
 * FrontmatterShell). `bash` is the default a caller falls back to when a command
 * declares none or an unrecognized value.
 */
export const COMMAND_SHELLS = ['bash', 'powershell'] as const;

export type CommandShell = (typeof COMMAND_SHELLS)[number];

/**
 * Validate a declared `shell` value to a {@link CommandShell} (the reference parity:
 * parseShellFrontmatter, frontmatterParser.ts L351-370). Returns the normalized
 * shell for a recognized value (trimmed + lower-cased so `"PowerShell"` is
 * accepted), or undefined for an absent / blank / non-string / unrecognized value —
 * the caller then defaults to bash, matching how `effort` degrades rather than
 * failing the command load.
 */
export function parseCommandShell(value: unknown): CommandShell | undefined {
  if (typeof value !== 'string') return undefined;
  const normalized = value.trim().toLowerCase();
  if (normalized === '') return undefined;
  return (COMMAND_SHELLS as readonly string[]).includes(normalized)
    ? (normalized as CommandShell)
    : undefined;
}

/**
 * Decide whether an embedded prompt-shell command — a `` !`cmd` `` / ` ```! block``` `
 * placeholder expanded in a plugin/slash command's prompt BEFORE it reaches the
 * model — is auto-permitted by the command's OWN declared {@link
 * CommandDefinition.allowedTools} (the reference parity: loadPluginCommands injects the
 * command's parsed allowed-tools into `toolPermissionContext.alwaysAllowRules.command`
 * before `executeShellCommandsInPrompt` runs, so only the command-declared shell is
 * auto-run — loadPluginCommands.ts L378-398 + promptShellExecution.ts's
 * `hasPermissionsToUseTool` gate). The embedded shell is thus scoped to the command:
 * it auto-runs an embedded command only when one of the command's own Bash allow-tools
 * covers it.
 *
 * A rule grants a command when it is a Bash-class tool rule (`Bash`/`bash` and its
 * aliases, via {@link isBashPrefixTool}): a bare `Bash` (no parenthesized content)
 * grants every command; `Bash(<pattern>)` grants commands matched by
 * {@link matchBashPattern} under the `allow` action (the `cmd:*` prefix form and `*`
 * wildcards). Non-Bash entries (`Read`, `Edit`, MCP tools, …) never grant a shell
 * command. Returns `false` for an empty/undefined/blank command or allow-list (a
 * command that declares no Bash allow-tools auto-runs nothing — fail-closed).
 */
export function isEmbeddedShellCommandAllowed(
  command: string,
  allowedTools: readonly string[] | undefined,
): boolean {
  const cmd = command.trim();
  if (cmd === '' || !allowedTools || allowedTools.length === 0) return false;
  for (const rule of allowedTools) {
    if (typeof rule !== 'string' || rule.trim() === '') continue;
    const { toolName, ruleContent } = permissionRuleValueFromString(rule.trim());
    if (!isBashPrefixTool(toolName)) continue;
    // A bare `Bash` rule (no parenthesized content) grants every command.
    if (ruleContent === undefined) return true;
    if (matchBashPattern(ruleContent, cmd, 'allow')) return true;
  }
  return false;
}

/**
 * The INPUT hint schema (the reference parity: `ClaudeCodeHint`, utils/plugins/
 * hintRecommendation.ts L41-47) a CLI/SDK emits on a tool's **stderr** inside an
 * `<ami-hint type="plugin" value="name@marketplace" sourceCommand="…" />` tag.
 * The Bash tool detects the tag, strips it from the model-visible output, and
 * turns it into the OUTPUT {@link PluginRecommendationHint} via
 * {@link evaluatePluginHint}. Distinct from `PluginRecommendationHint`: this is
 * the raw wire form a tool emits; the recommendation is the resolved, provenance-
 * stamped shape a frontend renders. `value` is a `name@marketplace` identifier.
 */
export interface AmiPluginHint {
  /** `name@marketplace` plugin identifier the emitting tool recommends. */
  value: string;
  /** The command/tool whose output produced the hint (its stderr). */
  sourceCommand?: string;
  /** Hint discriminator — only `'plugin'` is recognized. */
  type: string;
}

/** Matches one `<ami-hint …>` / `<ami-hint …/>` open tag (greedy to the first `>`). */
export const AMI_HINT_OPEN_RE = /<ami-hint\b[^>]*>/gi;

/** Matches the optional `</ami-hint>` close tag of the non-self-closing form. */
export const AMI_HINT_CLOSE_RE = /<\/ami-hint\s*>/gi;

/** Matches a single `key="value"` attribute inside a tag. */
export const AMI_HINT_ATTR_RE = /([A-Za-z][\w-]*)\s*=\s*"([^"]*)"/g;

/**
 * Parse every `<ami-hint type="plugin" value="…" sourceCommand="…" />` tag out of
 * a tool's stderr into {@link AmiPluginHint}s (the reference parity: the Bash/PowerShell
 * tool's `<claude-code-hint>` stderr detection). A tag missing `type` or `value`
 * is skipped. The raw tag text is NOT modified here — use {@link stripPluginHintTags}
 * to remove it from model-visible output.
 */
export function parsePluginHintTags(stderr: string): AmiPluginHint[] {
  const hints: AmiPluginHint[] = [];
  if (!stderr.includes('<ami-hint')) return hints;
  AMI_HINT_OPEN_RE.lastIndex = 0;
  let tag: RegExpExecArray | null;
  while ((tag = AMI_HINT_OPEN_RE.exec(stderr)) !== null) {
    const attrs: Record<string, string> = {};
    AMI_HINT_ATTR_RE.lastIndex = 0;
    let a: RegExpExecArray | null;
    while ((a = AMI_HINT_ATTR_RE.exec(tag[0])) !== null) attrs[a[1]] = a[2];
    if (!attrs.type || !attrs.value) continue;
    hints.push({
      value: attrs.value,
      type: attrs.type,
      ...(attrs.sourceCommand ? { sourceCommand: attrs.sourceCommand } : {}),
    });
  }
  return hints;
}

/**
 * Remove every `<ami-hint …/>` tag from a tool's stderr so the internal plugin-hint
 * protocol never leaks into the model-visible output (parity: the reference's Bash tool
 * strips the `<claude-code-hint>` tag before surfacing stderr). Also collapses the
 * blank gap a stripped standalone-line tag leaves behind.
 */
export function stripPluginHintTags(stderr: string): string {
  if (!stderr.includes('<ami-hint')) return stderr;
  return stderr
    .replace(AMI_HINT_OPEN_RE, '')
    .replace(AMI_HINT_CLOSE_RE, '')
    // A tag that occupied its own line leaves an empty line — collapse it.
    .replace(/\n[ \t]*\n+/g, '\n');
}

/**
 * Split a `name@marketplace` plugin identifier (the reference parity: hintRecommendation's
 * identifier parse) into its parts, or return `null` when it is malformed (no `@`,
 * empty name, or empty marketplace). `@` may appear in the name (e.g. a scoped
 * `@org/pkg@market`) — the LAST `@` separates the marketplace suffix.
 */
export function parsePluginIdentifier(value: string): { pluginName: string; marketplaceName: string } | null {
  const at = value.lastIndexOf('@');
  if (at <= 0 || at === value.length - 1) return null;
  const pluginName = value.slice(0, at).trim();
  const marketplaceName = value.slice(at + 1).trim();
  if (!pluginName || !marketplaceName) return null;
  return { pluginName, marketplaceName };
}

/** Sync-gate inputs for {@link evaluatePluginHint} (the reference parity: maybeRecordPluginHint's cheap pre-store checks). */
export interface PluginHintGateOptions {
  /**
   * When provided, ONLY hints whose marketplace is in this set pass (the reference's
   * official-marketplace filter). Omitted = no marketplace restriction.
   */
  officialMarketplaces?: ReadonlySet<string>;
  /** Already-installed plugin identifiers (`name@marketplace`) to skip (the reference's already-installed check). */
  installedPlugins?: ReadonlySet<string>;
  /**
   * Per-session identifiers already surfaced (the reference's per-session dedup /
   * show-once). MUTATED: a hint that passes is added so it won't surface twice.
   */
  seen?: Set<string>;
}

/**
 * The cheap SYNCHRONOUS gate (the reference parity: `maybeRecordPluginHint` L65-135) that
 * turns a raw {@link AmiPluginHint} into a resolved {@link PluginRecommendationHint},
 * or `null` when it fails any pre-store check: wrong type, unparseable
 * `name@marketplace` identifier, non-official marketplace, already installed, or
 * already shown this session. Deliberately does NO network/marketplace-cache
 * lookup — that expensive resolve is deferred to the async layer so a shell tool
 * never awaits I/O just to strip a stderr line.
 */
export function evaluatePluginHint(
  hint: AmiPluginHint,
  opts: PluginHintGateOptions = {},
): PluginRecommendationHint | null {
  if (hint.type !== 'plugin') return null;
  const id = parsePluginIdentifier(hint.value);
  if (!id) return null;
  if (opts.officialMarketplaces && !opts.officialMarketplaces.has(id.marketplaceName)) return null;
  const key = `${id.pluginName}@${id.marketplaceName}`;
  if (opts.installedPlugins?.has(key)) return null;
  if (opts.seen) {
    if (opts.seen.has(key)) return null;
    opts.seen.add(key);
  }
  return {
    pluginName: id.pluginName,
    marketplaceName: id.marketplaceName,
    sourceCommand: hint.sourceCommand ?? '',
  };
}

/**
 * Render a resolved {@link PluginRecommendationHint} as the one-line install
 * suggestion surfaced in a tool's output (the reference parity: PluginHintMenu wording
 * "the <sourceCommand> command suggests installing <pluginName> from
 * <marketplaceName>"). The `sourceCommand` clause is omitted when unknown.
 */
export function formatPluginHintNote(rec: PluginRecommendationHint): string {
  const lead = rec.sourceCommand
    ? `the '${rec.sourceCommand}' command suggests installing ${rec.pluginName}`
    : `suggested plugin: ${rec.pluginName}`;
  const desc = rec.pluginDescription ? ` — ${rec.pluginDescription}` : '';
  return `${lead} from ${rec.marketplaceName}${desc}`;
}

/**
 * Per-command metadata an author may declare in a plugin manifest's
 * `commandsMetadata` object-mapping (the reference parity: CommandMetadataSchema,
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
