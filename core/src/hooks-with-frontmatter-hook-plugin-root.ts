// Split from hooks.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { sanitizeHookOutput } from './hooks-session-context';
import type { HookConfigType } from './hooks-session-context';
import type { StopHookDisplay } from './hooks-stop-hook-summary';
import { HOOK_PROGRESS_INTERVAL_MS, shouldEmitHookLifecycle } from './hooks-command-hook-config';
import type { AgentHookConfig, CommandHookConfig, HookLifecycleOutcome, HttpHookConfig, PromptHookConfig } from './hooks-command-hook-config';

/**
 * Stamp a bundle/base `pluginRoot` onto a skill's / agent's invocation-scoped
 * frontmatter {@link HookConfigEntry} list so its `command` hooks can resolve
 * `${CLAUDE_PLUGIN_ROOT}` (and the `$AMI_PLUGIN_ROOT` alias) in both the command
 * text and the hook's environment when the sub-engine runs them — parity with how
 * PLUGIN-FILE command hooks already carry their bundle root (the `pluginRoot` field
 * set at hooks-file load). Previously a frontmatter hook declared inside a SKILL.md
 * / agent `.md` (registered via {@link HookManager.registerConfigEntries}) had NO
 * plugin root, so a `${CLAUDE_PLUGIN_ROOT}/scripts/x.sh` hook silently failed to
 * resolve its bundled asset path.
 *
 * Returns a NEW array with the relevant command hooks shallow-cloned (never mutates
 * the cached definition's `hooks`): a `command` hook that does NOT already declare
 * its own `pluginRoot` gets `root`; a hook that already has one, every non-command
 * hook, and malformed entries are passed through untouched. A blank/`undefined`
 * `root`, or an empty list, returns the input unchanged. The resolved root comes
 * from {@link agentPluginRoot} (agents) / `skillPluginRoot` (skills) in `skills.ts`.
 */
export function withFrontmatterHookPluginRoot(
  hooks: readonly HookConfigEntry[] | undefined,
  root: string | undefined,
): HookConfigEntry[] | undefined {
  if (!hooks || hooks.length === 0) return hooks as HookConfigEntry[] | undefined;
  const trimmed = typeof root === 'string' ? root.trim() : '';
  if (trimmed.length === 0) return hooks as HookConfigEntry[];
  return hooks.map((entry) => {
    const hook = entry?.hook;
    if (
      hook &&
      hook.type === 'command' &&
      (typeof hook.pluginRoot !== 'string' || hook.pluginRoot.trim().length === 0)
    ) {
      return { ...entry, hook: { ...hook, pluginRoot: trimmed } };
    }
    return entry;
  });
}

/**
 * Poll a running hook's incremental output on a fixed interval and deliver each
 * CHANGED snapshot to `onProgress` (the reference's `startHookProgressInterval`):
 * gated by {@link shouldEmitHookLifecycle} (a gated-off event never polls and gets
 * a no-op stop), deduped on the combined `output` so unchanged polls are dropped,
 * and timer-unref'd so a pending poll never keeps the process alive. Returns a stop
 * closure that clears the interval. `intervalMs` defaults to
 * {@link HOOK_PROGRESS_INTERVAL_MS}. Pure of the HookManager so it is unit-testable
 * in isolation; {@link HookManager.streamHookProgress} wires it to the lifecycle
 * channel.
 */
export function startHookProgressInterval(params: {
  event: string;
  allEnabled: boolean;
  getOutput: () => Promise<{ stdout: string; stderr: string; output: string }>;
  onProgress: (o: { stdout: string; stderr: string; output: string }) => void;
  intervalMs?: number;
}): () => void {
  if (!shouldEmitHookLifecycle(params.event, params.allEnabled)) return () => {};
  let lastEmitted = '';
  const interval = setInterval(() => {
    void params.getOutput().then(({ stdout, stderr, output }) => {
      if (output === lastEmitted) return;
      lastEmitted = output;
      params.onProgress({ stdout, stderr, output });
    });
  }, params.intervalMs ?? HOOK_PROGRESS_INTERVAL_MS);
  if (typeof interval.unref === 'function') interval.unref();
  return () => clearInterval(interval);
}

export interface HookConfigEntry {
  event: string;
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig;
  matcher?: string;
  _source?: 'project' | 'user' | 'config';
  /** Name of the plugin bundle that contributed this hook, when it came from a
   *  plugin (the reference's per-hook `pluginName`/`pluginId` provenance). Absent
   *  for filesystem (.ami/.claude/config) hooks. Retained so a hook can be
   *  attributed to — and audited per — the plugin that installed it. */
  pluginName?: string;
  /** Absolute base directory of the plugin bundle that contributed this hook, when
   *  it came from a plugin. Unlike {@link CommandHookConfig.pluginRoot} (set only on
   *  command hooks for `${CLAUDE_PLUGIN_ROOT}` substitution), this is carried for
   *  EVERY plugin hook type so a disabled/uninstalled plugin's hooks can be pruned by
   *  root at runtime ({@link HookManager.pruneRemovedPluginHooks}, gh-36995). Absent
   *  for filesystem/config hooks. */
  pluginRoot?: string;
}

export interface HookConfigFile {
  hooks: HookConfigEntry[];
}

/**
 * Options for {@link HookManager.registerConfigEntries} — parity with the reference's
 * registerFrontmatterHooks `isAgent` + `sourceName` parameters.
 */
export interface FrontmatterHookOptions {
  /** When true the entries came from a SUBAGENT's own frontmatter: a `stop` hook is
   *  remapped to `subagentStop`, because a completing subagent fires SubagentStop (not
   *  Stop), mirroring the reference's `isAgent` conversion. Skills leave `stop` as-is. */
  isAgent?: boolean;
  /** Human-readable origin (e.g. `agent 'reviewer'` / `skill 'deploy'`) used only in
   *  the debug/introspection log attributing the registered hooks + the Stop→SubagentStop
   *  conversion to their source (the reference's registerFrontmatterHooks `sourceName`). */
  sourceName?: string;
}

/**
 * A single file-loaded hook, retained for introspection (the reference's per-hook
 * ViewHookMode details): reveals the ACTUAL configured command / prompt / url —
 * not just an aggregate count — so `/hooks` and `--introspect` can show what each
 * hook runs, its matcher, and which settings tier contributed it.
 */
export interface HookDescriptor {
  event: string;
  type: HookConfigType;
  /** The matcher scoping this hook ('' / absent = all targets). */
  matcher?: string;
  /** Settings tier that contributed the hook (project | user | config | …). */
  source: string;
  /** The hook's command, prompt, or URL (sanitized of secret-looking tokens). */
  content: string;
  /** Plugin bundle name when the hook came from a plugin (the reference's per-hook
   *  plugin attribution). Absent for filesystem/config hooks. */
  pluginName?: string;
  /** The hook author's friendly status label (the reference `statusMessage`), when
   *  set — surfaced in progress/audit views instead of the raw command. */
  statusMessage?: string;
}

/**
 * Per-hook-type display label for the hook's primary content field — the reference's
 * ViewHookMode label resolution: command→'Command', http→'URL', and BOTH prompt
 * and agent hooks reuse the 'Prompt' label (an agent hook is a prompt-driven
 * verification agent). Pure; used by {@link HookManager.getHookAuditView}.
 */
export function hookContentLabel(type: HookConfigType): string {
  switch (type) {
    case 'command': return 'Command';
    case 'http': return 'URL';
    case 'prompt':
    case 'agent': return 'Prompt';
  }
}

/**
 * Human-readable text to show WHILE a hook runs / in an audit row (the reference's
 * getHookDisplayText): the author-supplied `statusMessage` when present, else the
 * hook's own content (command/prompt/url). Never leaks a secret the content
 * sanitizer already stripped. Pure; used by {@link HookManager.getHookAuditView}.
 */
export function getHookDisplayText(d: { statusMessage?: string; content: string }): string {
  const sm = d.statusMessage?.trim();
  return sm ? sm : d.content;
}

/**
 * Payload for the `hookProgress` / `hookResolved` events a {@link HookManager}
 * emits around every config-registered command/prompt/http/agent hook run
 * (the reference's hook_progress affordance). `hookProgress` fires when the hook
 * starts; `hookResolved` adds `success` when it settles. A frontend subscribes
 * to render a live "Running <event> hook…" indicator so a long-running hook does
 * not look like a hang; it is also an operator-facing introspection signal.
 */
export interface HookProgressEvent {
  /** The lifecycle event the hook fired for (e.g. 'preToolUse'). */
  event: string;
  /** The hook's type — command | prompt | http | agent. */
  hookType: HookConfigType;
  /** Human-readable label: statusMessage when set, else the hook's content. */
  label: string;
  /** Present only on `hookResolved`: whether the hook ran successfully. */
  success?: boolean;
  /** Stable per-hook id (the reference `hookId`) correlating this hook's
   *  `hookProgress` and `hookResolved` emissions. From {@link computeHookId}. */
  hookId?: string;
  /** Human-facing hook name (the reference `hookName`) — the display label. */
  hookName?: string;
  /** Present only on `hookResolved`: the 3-way terminal classification
   *  (success | error | cancelled), from {@link classifyHookOutcome}. */
  outcome?: HookLifecycleOutcome;
}

/**
 * Canonical per-source-tier label set (the reference's three
 * `hookSource*DisplayString` helpers in hooksSettings.ts), so the three UI forms —
 * a full description WITH the backing file path, a Title-Case section HEADER, and a
 * terse INLINE badge — can never drift from one another. One table; every form reads
 * from it. `display` is the medium provenance form retained for
 * {@link hookSourceDisplayString}. Paths use AMI's own `.ami`/`~/.ami` settings tree.
 */
export const HOOK_SOURCE_LABELS: Record<string, { display: string; description: string; header: string; inline: string }> = {
  project: { display: 'Project settings', description: 'Project settings (.ami/settings.json)', header: 'Project Settings', inline: 'Project' },
  user: { display: 'User settings', description: 'User settings (~/.ami/settings.json)', header: 'User Settings', inline: 'User' },
  config: { display: 'Project config', description: 'Project config (.ami/config.json)', header: 'Project Config', inline: 'Config' },
  managed: { display: 'Managed policy', description: 'Managed policy (administrator-enforced settings)', header: 'Managed Policy', inline: 'Managed' },
  plugin: { display: 'Plugin', description: 'Plugin hooks (installed plugin bundle)', header: 'Plugin Hooks', inline: 'Plugin' },
};

/** Append `(pluginName)` to a base label for a plugin-contributed hook so the user
 *  sees WHICH plugin installed it (the reference's per-hook plugin attribution). */
export function withPluginSuffix(base: string, pluginName?: string): string {
  return pluginName ? `${base} (${pluginName})` : base;
}

/**
 * Human-readable provenance string for a hook's source tier, with the reference's
 * plugin special-case: a plugin-contributed hook appends `(pluginName)` so the
 * user sees WHICH plugin installed it (SelectHookMode /
 * hookSourceDescriptionDisplayString). Pure; used by
 * {@link HookManager.getHookAuditView}.
 */
export function hookSourceDisplayString(source: string, pluginName?: string): string {
  return withPluginSuffix(HOOK_SOURCE_LABELS[source]?.display ?? source, pluginName);
}

/**
 * Full source description INCLUDING the backing file path (the reference's
 * `hookSourceDescriptionDisplayString`): e.g. `Project settings (.ami/settings.json)`,
 * with `(pluginName)` appended for plugin hooks. For a detail/inspector view that
 * tells the user exactly which file a hook came from. Pure; used by
 * {@link HookManager.getHookAuditView}.
 */
export function hookSourceDescription(source: string, pluginName?: string): string {
  return withPluginSuffix(HOOK_SOURCE_LABELS[source]?.description ?? source, pluginName);
}

/**
 * Title-Case section HEADER for grouping hooks by source (the reference's
 * `hookSourceHeaderDisplayString`): e.g. `Project Settings`, `Plugin Hooks`. For a
 * grouped list header. The plugin name is appended when present so distinct plugins
 * get distinct headers. Pure; used by {@link HookManager.getHookAuditView}.
 */
export function hookSourceHeader(source: string, pluginName?: string): string {
  return withPluginSuffix(HOOK_SOURCE_LABELS[source]?.header ?? source, pluginName);
}

/**
 * Terse INLINE badge for a hook source (the reference's
 * `hookSourceInlineDisplayString`): a single word — `Project`, `User`, `Config`,
 * `Managed`, `Plugin` — for a compact inline column. The plugin name is NOT appended
 * (the badge stays short). Pure; used by {@link HookManager.getHookAuditView}.
 */
export function hookSourceInline(source: string): string {
  return HOOK_SOURCE_LABELS[source]?.inline ?? source;
}

/** A single hook's full audit row (the reference's ViewHookMode): identity, matcher,
 *  typed content with its display label, plugin attribution, and the human-readable
 *  source + display text. */
export interface HookAuditRow {
  event: string;
  type: HookConfigType;
  matcher?: string;
  source: string;
  /** Human-readable source description incl. `(pluginName)` for plugin hooks. */
  sourceLabel: string;
  /** Full source description WITH the backing file path (e.g. `Project settings
   *  (.ami/settings.json)`) — for a detail/inspector view. */
  sourceDescription: string;
  /** Title-Case section header (e.g. `Project Settings`) — for grouped list headers. */
  sourceHeader: string;
  /** Terse single-word badge (e.g. `Project`) — for a compact inline column. */
  sourceInline: string;
  pluginName?: string;
  /** Per-type content label ('Command' | 'Prompt' | 'URL'). */
  contentLabel: string;
  /** The sanitized command / prompt / url. */
  content: string;
  /** What to show while the hook runs (statusMessage || content). */
  displayText: string;
}

/** One matcher group under an event in the introspection view. */
export interface HookEventMatcherGroup {
  /** Raw matcher ('' = all). */
  matcher: string;
  /** Display label ('(all)' for the empty matcher). */
  label: string;
  /** Hooks registered under this matcher. */
  count: number;
  /** Distinct settings tiers contributing hooks under this matcher. */
  sources: string[];
  /** Distinct plugin bundle names that contributed a hook under this matcher
   *  (the reference's per-matcher `pluginHook`/`pluginName` attribution in
   *  groupHooksByEventAndMatcher). Present only when a plugin owns at least one
   *  hook in the group, so the `/hooks` view can show WHICH plugin bound a matcher,
   *  not just that a `plugin` tier is involved. */
  pluginNames?: string[];
}

/** Per-active-event introspection record (the reference's SelectEventMode row):
 *  count + human-readable summary/description + matcher metadata + the matcher
 *  groups (sorted by source priority). */
export interface HookEventDescription {
  event: string;
  count: number;
  summary?: string;
  description?: string;
  matcherField?: string;
  matcherValues?: string[];
  supportsMatcher: boolean;
  matchers: HookEventMatcherGroup[];
}

/** Human-readable content for a hook config (its command / prompt / url),
 *  sanitized of secret-looking tokens before it is surfaced for introspection. */
export function describeHookContent(
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
): string {
  switch (hook.type) {
    case 'command': return sanitizeHookOutput(hook.args ? hook.args.join(' ') : hook.command);
    case 'prompt':
    case 'agent': return sanitizeHookOutput(hook.prompt);
    case 'http': return hook.url;
  }
}

/** Derive the Stop-hook run-summary display descriptor (command / promptText) from
 *  a hook config, so a Stop hook can be attributed to its backing config in the
 *  per-hook summary (the reference's StopHookInfo.command / promptText). Command and
 *  HTTP hooks surface a `command` identifier (the shell command / URL); prompt and
 *  agent hooks surface `promptText`. Content is sanitized of secret-looking tokens
 *  (reusing {@link describeHookContent}) before it is retained for display. */
export function stopHookDisplayFromConfig(
  hook: CommandHookConfig | PromptHookConfig | HttpHookConfig | AgentHookConfig,
): StopHookDisplay {
  const content = describeHookContent(hook);
  switch (hook.type) {
    case 'command':
    case 'http': return { command: content };
    case 'prompt':
    case 'agent': return { promptText: content };
  }
}

/** Hook-matcher-only tool-name aliases layered OVER the shared permission alias
 *  map. The plan-exit tool is named `ExitPlanMode`/`ExitPlanModeV2` in the reference
 *  but `plan_mode` in AMI, so a reference-format hook matcher of `ExitPlanMode`
 *  must gate AMI's plan-mode tool. Kept hook-local (not in PERMISSION_TOOL_ALIASES)
 *  because the permission layer does not expose a plan-exit tool name. */
export const HOOK_MATCHER_EXTRA_ALIASES: Record<string, string> = {
  ExitPlanMode: 'plan_mode',
  ExitPlanModeV2: 'plan_mode',
};

/**
 * Scan a hook's RAW stdout for the FIRST line that is a self-contained JSON object
 * (its trimmed text starts with `{` and `JSON.parse`s to a plain object) whose
 * object does NOT carry an `async` key, returning that parsed object — or
 * `undefined` when no such line exists.
 *
 * Parity with the reference's `checkForAsyncHookResponses` stdout line-scan
 * (utils/hooks/AsyncHookRegistry.ts L183-212): a command hook may interleave
 * ordinary log/diagnostic lines with its single JSON decision line, so a
 * whole-blob `JSON.parse(stdout)` fails even though a valid decision IS present
 * (e.g. a wrapper that prints `[hook] starting` before echoing its decision).
 * Scanning line-by-line for the first non-`async` JSON object recovers the
 * decision instead of silently dropping it and treating everything as plain text.
 * The `async` carve-out skips the `{ async: true, asyncTimeout }` background-mode
 * marker, which is the opt-in detach signal, not the synchronous decision. Pure;
 * never throws.
 */
export function firstHookJsonLine(stdout: string): Record<string, unknown> | undefined {
  if (!stdout) return undefined;
  for (const rawLine of stdout.split('\n')) {
    const line = rawLine.trim();
    if (line.length === 0 || line[0] !== '{') continue;
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      continue; // not a complete JSON object on this line — keep scanning
    }
    if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) continue;
    // Skip the background-mode marker ({ async: true, asyncTimeout }); it is the
    // detach opt-in, NOT the synchronous decision the reference's line-scan looks for.
    if ('async' in (parsed as Record<string, unknown>)) continue;
    return parsed as Record<string, unknown>;
  }
  return undefined;
}

export type EngineFactory = (config: any) => { submit(prompt: string): AsyncIterable<{ type: string; text?: string; error?: string; toolName?: string; input?: Record<string, unknown>; exitReason?: string }>; shutdown?(): void };

// ---------------------------------------------------------------------------
// Hook-driven prompt-elicitation protocol (the reference parity:
// types/hooks.ts promptRequestSchema / PromptResponse + utils/hooks.ts
// stdout line-scan → stdin answer). A command hook can pause and ask the user
// to pick from a labelled list of options by emitting a `PromptRequest` JSON
// line on stdout; the engine answers with a `PromptResponse` line on stdin and
// strips the request line from the hook's captured stdout (content match, so no
// index drift). The `prompt` key is the discriminator (mirroring the
// `{async:true}` async-output discriminator) and carries the request id.
// ---------------------------------------------------------------------------

/** One selectable option offered by a hook prompt (the reference's
 *  `{key,label,description?}`). `key` is returned to the hook; `label` is shown
 *  to the user; `description` is an optional secondary line. */
export interface PromptRequestOption {
  key: string;
  label: string;
  description?: string;
}
