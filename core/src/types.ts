import type { SuperInferenceConfig } from './superinference/types';
import type { ToolCapabilityCeiling } from './tools/scoped-permissions';

/**
 * Type-branded session id: a nominal `string` subtype so a raw string can't be silently
 * passed where a persisted session id is expected. It stays assignable TO `string`
 * (reads / pass-through / Map keys need no cast); only MINTING one from a raw string goes
 * through {@link asSessionId}. The brand symbol is unexported, so the type is truly
 * nominal. (round-2 "type-branded ids".)
 */
declare const SESSION_ID_BRAND: unique symbol;
export type SessionId = string & { readonly [SESSION_ID_BRAND]: true };
/** Mint a {@link SessionId} from a raw string (the single branding boundary). */
export function asSessionId(id: string): SessionId { return id as SessionId; }

/**
 * Type-branded agent id — a nominal `string` subtype so a free-text teammate name
 * cannot be silently passed where a real agent id is expected (parity: Claude's
 * `AgentId` in types/ids.ts). {@link asAgentId} mints one unconditionally (the
 * pass-through boundary); {@link toAgentId} VALIDATES the format first and returns
 * `null` for a non-conforming string, which is what discriminates a genuine agent id
 * from an arbitrary name at the mint boundary.
 */
declare const AGENT_ID_BRAND: unique symbol;
export type AgentId = string & { readonly [AGENT_ID_BRAND]: true };
/** Mint an {@link AgentId} from a raw string without validation (pass-through boundary). */
export function asAgentId(id: string): AgentId { return id as AgentId; }
/** Validate and brand an agent id; returns `null` when `id` is not a well-formed
 *  agent id (empty, or containing whitespace / disallowed characters). */
export function toAgentId(id: string): AgentId | null {
  return /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(id) ? (id as AgentId) : null;
}

export const OUTPUT_FORMATS = ['json', 'jsonl', 'text'] as const;
export type OutputFormat = (typeof OUTPUT_FORMATS)[number];

export const INPUT_FORMATS = ['text', 'stream-json'] as const;
export type InputFormat = (typeof INPUT_FORMATS)[number];

export const CHAT_MODES = ['ask', 'edit', 'agent'] as const;
export type ChatMode = (typeof CHAT_MODES)[number];

/**
 * User-selectable theme SETTINGS — the canonical vocabulary accepted by `/theme`
 * and the `AMI_THEME` env var (parity: Claude's `THEME_SETTINGS`, as distinct from
 * the resolved `THEME_NAMES`). The CLI's `resolveThemeByName` turns each of these
 * into a concrete palette at render time:
 *   - `default` / `auto` — follow the terminal (auto-detect scheme + color tier);
 *     `auto` is the Claude-parity alias of the historical AMI `default`.
 *   - `dark` / `light` — force a scheme.
 *   - `dark-daltonized` / `light-daltonized` — deuteranopia/protanopia-safe
 *     ("colorblind-friendly") palettes.
 *   - `dark-ansi` / `light-ansi` — force the coarse 16-color (ANSI-only) palette
 *     even on a truecolor terminal (low-color SSH sessions / recordings).
 * Every member is honored by `resolveThemeByName`; a drift guard (cli
 * repl-theme-settings-parity test) asserts the enum and the resolver stay 1:1.
 */
export const THEMES = [
  'default',
  'auto',
  'dark',
  'light',
  'dark-daltonized',
  'light-daltonized',
  'dark-ansi',
  'light-ansi',
] as const;
export type Theme = (typeof THEMES)[number];

/**
 * Terminal notification channels for turn-completion alerts, selectable per
 * terminal family (parity: Claude's NOTIFICATION_CHANNELS). `auto` picks the best
 * mechanism for the detected terminal; `notifications_disabled` turns alerts off.
 * The runtime array is the single source of truth — {@link NotificationChannel}
 * is derived from it so the enum and the type can never drift.
 */
export const NOTIFICATION_CHANNELS = [
  'auto',
  'iterm2',
  'iterm2_with_bell',
  'terminal_bell',
  'kitty',
  'ghostty',
  'notifications_disabled',
] as const;
export type NotificationChannel = (typeof NOTIFICATION_CHANNELS)[number];

// The built-in output-style NAMES (the string overlays live in common's
// output-style-manager BUILTIN_OUTPUT_STYLES). `learning` is a first-class built-in —
// accepted by the engine, /output-style, and --output-style, and documented in HELP —
// so it MUST be advertised here too (audit #5: the manifest was under-advertising it).
// A gate (output-style-manager test) asserts these names === the manager's built-in set.
export const OUTPUT_STYLES = ['default', 'concise', 'explanatory', 'learning'] as const;
export type OutputStyle = (typeof OUTPUT_STYLES)[number];

/**
 * Single source of truth for the "explanatory" teaching-callout contract. The
 * `explanatory` output-style overlay instructs the model to open each callout with
 * a line beginning with this header; the TUI renderer keys on the SAME constant to
 * style the block. Keeping one constant prevents the emission↔render drift that
 * AGENTS.md warns about for dual prompt paths.
 */
export const INSIGHT_CALLOUT_HEADER = '★ Insight';

export interface TextContent {
  type: 'text';
  text: string;
}

export interface ImageContent {
  type: 'image';
  source: {
    type: 'base64';
    media_type: string;
    data: string;
  };
}

export interface ToolCallFunction {
  name: string;
  arguments: string;
}

export interface ToolCallContent {
  id: string;
  type: 'function';
  function: ToolCallFunction;
}

/**
 * Provenance of an inbound user/queued message (parity: Claude's `MessageOrigin`),
 * which drives per-origin framing — e.g. a `task-notification`/`coordinator`/`channel`
 * message gets an untrusted-source warning that a plain `user` message does not.
 */
export type MessageOrigin =
  | { kind: 'user' }
  | { kind: 'task-notification' }
  | { kind: 'coordinator' }
  | { kind: 'teammate'; name?: string }
  | { kind: 'channel'; source: string; user?: string; chatId?: string };

/**
 * Optional transcript-tree / provenance metadata persisted on a message so subagent
 * sidechains form a resumable tree and each message records its origin (parity:
 * Claude's TranscriptMessage `parentUuid`/`logicalParentUuid`/`isSidechain`/`agentId`/
 * `promptId` plus swarm `teamName`/`agentName` identity used to detect a resumed teammate).
 */
export interface TranscriptMeta {
  /** Stable id of this message. */
  uuid?: string;
  /** Parent message id (threading within a session). */
  parentUuid?: string;
  /** Logical parent, preserved across session breaks/resumes. */
  logicalParentUuid?: string;
  /** True when this message belongs to a subagent sidechain. */
  isSidechain?: boolean;
  /** Owning agent id, for per-agent transcript records. */
  agentId?: string;
  /** Correlates to the OTel `prompt.id` for this turn. */
  promptId?: string;
  /** Swarm team name — present on the first message of a resumed teammate session. */
  teamName?: string;
  /** Swarm agent name — present on the first message of a resumed teammate session. */
  agentName?: string;
  /** Render this message ONLY in verbose/transcript mode while STILL sending it to
   *  the model — a third visibility state distinct from {@link UserMessage.isMeta}
   *  (which hides it from the model entirely). Consumers treat
   *  `isMeta || isVisibleInTranscriptOnly` as "synthetic" for display. Parity:
   *  Claude's `createUserMessage({ isVisibleInTranscriptOnly })` + `shouldShowUserMessage`. */
  isVisibleInTranscriptOnly?: boolean;
}

export interface UserMessage extends TranscriptMeta {
  role: 'user';
  content: string | Array<TextContent | ImageContent>;
  /** Engine-injected ephemeral control message (turn-local nudge). Excluded from
   *  the persisted session transcript so --resume is not polluted; still sent to
   *  the model during the live turn. */
  isMeta?: boolean;
  /** Provenance of this message, driving per-origin framing. Parity: `MessageOrigin`. */
  origin?: MessageOrigin;
  /** Permission mode active when this prompt was submitted, preserved per turn for
   *  audit/replay. Parity: Claude's per-message `permissionMode`. */
  permissionMode?: string;
  /** Ids of inline pasted-image placeholders (e.g. `[Image #1]`) in this message's
   *  text, correlating each placeholder with the image content block appended to the
   *  message. Set only when non-empty (else omitted). Parity: Claude's
   *  `processTextPrompt({ imagePasteIds })` user-message field. */
  imagePasteIds?: number[];
}

export interface AssistantMessage extends TranscriptMeta {
  role: 'assistant';
  content: string | null;
  tool_calls?: ToolCallContent[];
  /** Model that produced this assistant message (stamped per turn, incl. architect/
   *  fallback/provider switches), for the transcript model-attribution label.
   *  Parity: Claude's MessageModel. */
  model?: string;
}

export interface ToolMessage extends TranscriptMeta {
  role: 'tool';
  tool_call_id: string;
  content: string;
  /** Marks this tool result as a failure so the model clearly sees the tool errored
   *  (translated by `convertMessages` to the provider's `tool_result` `is_error`
   *  flag, e.g. the AI-SDK `error-text` output variant). Set on the synthetic
   *  placeholders `healOrphanedToolCalls` emits for unanswered/interrupted tool
   *  calls so they surface as errors, not ordinary tool output. Parity: Claude's
   *  synthetic tool_result `is_error: true`. */
  isError?: boolean;
}

export interface SystemMessage extends TranscriptMeta {
  role: 'system';
  content: string;
  /** Engine-injected ephemeral control message (turn-local nudge). Excluded from
   *  the persisted session transcript so --resume is not polluted; still sent to
   *  the model during the live turn. */
  isMeta?: boolean;
  /** Severity level: 'info' messages are suppressed unless verbose; 'warning'/'error'
   *  change the styling (color/dot). Parity: Claude's SystemMessage `level`. */
  level?: 'info' | 'warning' | 'error';
}

export type Message = UserMessage | AssistantMessage | ToolMessage | SystemMessage;

export interface JsonSchemaProperty {
  /** JSON Schema type. A string for a single type, or an array of types for a
   *  union (e.g. `['string','boolean','number']`) — parity with Claude's
   *  `union([...])` value parameters. Consumers that branch on a single type
   *  must handle the array form (see synthetic-output / workflow-runtime). */
  type: string | string[];
  description?: string;
  enum?: string[];
  default?: unknown;
  items?: JsonSchemaProperty;
  properties?: Record<string, JsonSchemaProperty>;
  required?: string[];
  additionalProperties?: boolean | JsonSchemaProperty;
  [key: string]: unknown;
}

export interface ToolInputSchema {
  type: 'object';
  properties: Record<string, JsonSchemaProperty>;
  required?: string[];
  /** When false, the schema rejects keys not declared in {@link properties}
   *  (parity: Claude Code's `z.strictObject`). Omitted => permissive, as before. */
  additionalProperties?: boolean;
}

/** One hunk of a structured diff (parity: Claude's FileEdit `structuredPatch`),
 *  so a frontend can render a file mutation per-hunk instead of parsing text. */
export interface DiffHunk {
  oldStart: number;
  oldLines: number;
  newStart: number;
  newLines: number;
  lines: string[];
}

/** VCS-aware per-edit change summary attached to a file-mutating tool result's
 *  {@link ToolResult.metadata} as `gitDiff`. Parity: Claude's FileEditTool
 *  `gitDiffSchema` output field (filename, status, additions/deletions/changes
 *  counts, unified patch) — lets a programmatic caller read change magnitude and
 *  the patch without re-parsing the human `output` string. */
export interface GitDiffSummary {
  /** The edited file's path. */
  filename: string;
  /** `'added'` for a newly created (or previously empty) file, else `'modified'`. */
  status: 'modified' | 'added';
  /** Count of added lines. */
  additions: number;
  /** Count of removed lines. */
  deletions: number;
  /** `additions + deletions` — total changed lines (Claude's `changes`). */
  changes: number;
  /** Unified diff text (bounded by the edit diff cap). */
  patch: string;
}

/**
 * Live, structured progress for a long-running shell/task tool, carried so a
 * frontend can render a "(elapsed · N lines)" affordance without re-parsing the
 * streamed text. Consolidates Claude's shell/task progress schemas — the
 * `onProgress(lastLines, allLines, totalLines, totalBytes, isIncomplete)` callback
 * (utils/Shell.ts `ExecOptions`, utils/task/TaskOutput.ts `ProgressCallback`) plus
 * the UI timer fields (tools/PowerShellTool/UI.tsx, components/BashModeProgress.tsx:
 * `elapsedTimeSeconds` / `timeoutMs`).
 */
export interface StreamingToolProgress {
  /** The tail of the output captured so far (most recent lines). */
  lastLines: string;
  /** All output captured so far (may be truncated to a memory cap). */
  allLines: string;
  /** Total number of output lines seen (may exceed what `allLines` retains). */
  totalLines: number;
  /** Total number of output bytes seen. */
  totalBytes: number;
  /** Whether `allLines`/`lastLines` is a truncated view of the full output. */
  isIncomplete: boolean;
  /** Wall-clock seconds the command has been running (UI timer field). */
  elapsedTimeSeconds?: number;
  /** The command's timeout budget in ms, for a "will abort at" affordance. */
  timeoutMs?: number;
}

export interface ToolResult {
  output: string;
  isError?: boolean;
  /** Free-form structured hints attached to a result. Well-known keys the bash
   *  tool sets (parity with Claude Code's shell result fields):
   *  - `preSpawnError`: `'cwd-missing' | 'spawn-failure'` — the command never ran
   *    because the shell could not be spawned (a deleted working directory, or a
   *    pre-exec spawn failure). Distinguishes a setup failure from a command that
   *    ran and exited non-zero or timed out, mirroring Claude's dedicated
   *    `preSpawnError` result field (`ShellCommand.createFailedCommand`).
   *  - `assistantAutoBackgrounded`: `true` when a long foreground command was
   *    auto-moved to the background in assistant/detached mode so the main agent
   *    stays responsive (Claude's `shouldAutoBackground` / assistant blocking
   *    budget). Accompanied by `backgrounded: true` and the `taskId`.
   *  - `pluginHints`: `PluginRecommendationHint[]` — install suggestions parsed
   *    from an `<ami-hint type="plugin" .../>` tag a command emitted on stderr
   *    (set only when `AMI_PLUGIN_HINTS` is enabled; Claude's stderr-hint plugin
   *    recommendation).
   *
   *  Keys the file_edit tool sets on a SUCCESSFUL edit (parity with Claude Code's
   *  FileEditTool `outputSchema`), so a programmatic caller reads them without
   *  re-parsing the text `output`:
   *  - `filePath`: `string` — the edited path.
   *  - `editMode`: `'single' | 'multiple'` — whether one edit or an `edits[]` batch
   *    was applied (Claude's IDEDiffConfig `editMode` discriminator), letting a
   *    diff/permission UI branch on single-vs-batch presentation.
   *  - `replaceAll`: `boolean` — whether every occurrence was replaced.
   *  - `userModified`: `boolean` — whether the applied bytes diverged from the
   *    model's proposal via an interactive accept-with-edits path. Always `false`
   *    in AMI (the permission layer is allow/deny, with no in-prompt diff editor);
   *    emitted to match Claude's output contract.
   *  - `gitDiff`: {@link GitDiffSummary} — change counts + unified patch (omitted on
   *    the diff-cost-capped success paths, where no per-line diff is computed). */
  metadata?: Record<string, unknown>;
  /** Structured per-hunk diff for a file-mutating result, carried alongside the
   *  text `output` for programmatic rendering. Parity: Claude's `structuredPatch`. */
  structuredPatch?: DiffHunk[];
  /** The tool call was canceled before/while running (e.g. the surrounding batch
   *  was aborted) — distinct from a tool that ran and errored. Parity: Claude's
   *  `CANCEL_MESSAGE` content marker. Drives a dedicated "canceled" render state
   *  via {@link classifyToolResultState}. */
  canceled?: boolean;
  /** The user actively rejected this tool call at a permission prompt (deny), so it
   *  never ran. Parity: Claude's `REJECT_MESSAGE` content marker. Distinct from an
   *  execution error. Drives the "rejected" render state. */
  rejected?: boolean;
  /** The tool call was interrupted (user pressed stop / server abort) with no real
   *  result. Parity: Claude's `INTERRUPT_MESSAGE_FOR_TOOL_USE`. Distinct from a tool
   *  that produced an error output. Drives the "interrupted" render state. */
  interrupted?: boolean;
  /** Severity of a failing result, for tools that distinguish a RECOVERABLE
   *  validation outcome from a FATAL error. `'ask'` marks a soft failure the model is
   *  expected to fix and retry (e.g. a file_edit whose `old_string` was not found, the
   *  file was not read first, the edit is a no-op, or the file changed since it was
   *  read) — the call still carries `isError: true`, but a frontend/transcript can
   *  present it as an "ask and retry" prompt rather than a hard stop. `'error'` (or
   *  absent) is a plain fatal error. Parity: Claude's FileEditTool validation
   *  `behavior: 'ask' | 'error'`. Classified by {@link classifyToolResultState}. */
  behavior?: 'ask' | 'error';
  /** Final streamed-progress snapshot for a long-running shell/task tool, so a
   *  transcript can show the "(elapsed · N lines)" affordance on the completed
   *  call. See {@link StreamingToolProgress}. */
  progress?: StreamingToolProgress;
}

/** The collapse "kind" a single tool call contributes to a consecutive
 *  read/search group. Parity: Claude's `getToolSearchOrReadInfo` discriminator
 *  (`utils/collapseReadSearch.ts`). `absorbed` tool calls (e.g. `tool_search`)
 *  belong to the group visually but increment no count. */
export type ToolGroupKind =
  | 'search'
  | 'read'
  | 'list'
  | 'repl'
  | 'bash'
  | 'gitOpBash'
  | 'mcpCall'
  | 'memoryRead'
  | 'memorySearch'
  | 'memoryWrite'
  | 'absorbed'
  | 'none';

/** Aggregated per-kind counts for a collapsed run of consecutive read/search
 *  tool calls, so a frontend can render one summary row ("Read 4 files · Searched
 *  3×") instead of a wall of individual rows. Parity: Claude's
 *  `CollapsedReadSearchGroup` schema (`components/messages/CollapsedReadSearchContent.tsx`).
 *  Built by {@link aggregateToolGroupCounts}. */
export interface ToolGroupCounts {
  searchCount: number;
  readCount: number;
  listCount: number;
  replCount: number;
  bashCount: number;
  /** Bash commands recognised as git operations (commit/push/branch/PR); surfaced
   *  separately so they are not double-counted against {@link bashCount}. */
  gitOpBashCount: number;
  mcpCallCount: number;
  memoryReadCount: number;
  memorySearchCount: number;
  memoryWriteCount: number;
  /** Distinct MCP server names seen in the group (leaf names, dedup, first-seen order). */
  mcpServerNames: string[];
}

/** Parsed form of a `<channel source="…" user="…" chat_id="…">…</channel>`
 *  wrapper that an external channel (e.g. a chat/MCP bridge) prepends to an
 *  inbound user message. Parity: Claude's channel-message schema
 *  (`components/messages/UserChannelMessage.tsx`, `services/mcp/channelNotification.ts`).
 *  `source` is mandatory and always written first; every other attribute
 *  (`user`, `chat_id`, `thread_ts`, …) is optional and carried in {@link attrs}. */
export interface ChannelMessageMeta {
  /** Originating channel/server name, e.g. `slack` or `plugin:slack-channel:slack`. */
  source: string;
  /** Convenience accessor for the common `user` attribute (also present in {@link attrs}). */
  user?: string;
  /** All parsed attributes except `source` (includes `user`, `chat_id`, …). */
  attrs: Record<string, string>;
  /** The channel message body (inner text of the wrapper, trimmed of the single
   *  framing newline on each side). */
  content: string;
}

/** Per-host outbound-network allow/deny lists for the sandbox. `deniedHosts`
 *  takes precedence over `allowedHosts`. Proxy ports route the container's
 *  outbound traffic through a host proxy (parity: Claude's SandboxNetworkConfig). */
export interface SandboxNetworkConfig {
  /** Hostnames/domains permitted for outbound network (allowlist). */
  allowedHosts?: string[];
  /** Hostnames/domains blocked for outbound network (denylist; wins over allowedHosts). */
  deniedHosts?: string[];
  /** Unix-socket paths to allow (macOS only; ignored where the backend can't filter by path). */
  allowUnixSockets?: string[];
  /** Allow ALL Unix sockets (disables socket blocking entirely). */
  allowAllUnixSockets?: boolean;
  /** Allow the sandboxed process to bind local ports. */
  allowLocalBinding?: boolean;
  /** Host port the sandbox routes outbound HTTP(S) through. 1..65535. */
  httpProxyPort?: number;
  /** Host port the sandbox routes outbound traffic through (SOCKS). 1..65535. */
  socksProxyPort?: number;
  /** ENTERPRISE managed-policy lockdown: when true the sandbox outbound allowlist
   *  is forced to {@link managedAllowedHosts} ONLY — any `allowedHosts` derived
   *  from user settings or folded-in `web_fetch` allow rules is IGNORED, so a
   *  lower tier can never widen managed egress. `deniedHosts` still applies (deny
   *  wins), and an empty/absent managed list means NO outbound host is allowed
   *  (full egress block). Mirrors Claude's
   *  `shouldAllowManagedSandboxDomainsOnly()` / `allowManagedDomainsOnly`.
   *  Additive: undefined/false leaves the normal allowlist behavior unchanged. */
  allowManagedDomainsOnly?: boolean;
  /** The admin/managed-provided outbound allowlist honored (and used EXCLUSIVELY)
   *  when {@link allowManagedDomainsOnly} is set. Ignored otherwise. Mirrors the
   *  policy-tier `sandbox.network.allowedDomains` Claude uses under managed-only. */
  managedAllowedHosts?: string[];
}

/** Granular filesystem carve-outs layered on top of the project-only writable
 *  mount (parity: Claude's SandboxFilesystemConfig). Deny wins over allow, and
 *  `allowRead` re-opens specific paths inside a `denyRead` region. */
export interface SandboxFilesystemConfig {
  /** Extra absolute paths writable inside the sandbox (merged with Edit allow rules). */
  allowWrite?: string[];
  /** Extra absolute paths denied for writing (merged with Edit deny rules; wins over allowWrite). */
  denyWrite?: string[];
  /** Extra absolute paths denied for reading (merged with Read deny rules). */
  denyRead?: string[];
  /** Paths re-allowed for reading within a denyRead region (precedence over denyRead). */
  allowRead?: string[];
  /** ENTERPRISE managed-policy lockdown: when true the read RE-allow carve-outs are
   *  forced to {@link managedAllowRead} ONLY — any `allowRead` from user settings is
   *  IGNORED, so a lower tier cannot re-open a managed-denied read path. `denyRead`
   *  still applies. Mirrors Claude's `allowManagedReadPathsOnly`
   *  (sandbox-adapter's `shouldAllowManagedReadPathsOnly()` gate on non-policy
   *  `allowRead`). Additive: undefined/false leaves `allowRead` behavior unchanged. */
  allowManagedReadPathsOnly?: boolean;
  /** The admin/managed-provided read RE-allow paths honored (and used EXCLUSIVELY)
   *  when {@link allowManagedReadPathsOnly} is set. Ignored otherwise. */
  managedAllowRead?: string[];
}

/** Custom seccomp syscall-filter configuration for the sandbox backend.
 *  Parity: Claude's `sandbox.seccomp.bpfPath` / `sandbox.seccomp.applyPath`
 *  (bubblewrap sandbox-runtime). AMI sandboxes via Docker, so the ADDITIVE,
 *  architecture-native analog is a Docker seccomp JSON profile: when
 *  {@link profilePath} is set it is passed to the container as
 *  `--security-opt seccomp=<path>`, replacing Docker's default profile with the
 *  operator-supplied one for tighter (or custom) syscall filtering. Unset keeps
 *  Docker's hardened default profile. */
export interface SandboxSeccompConfig {
  /** Absolute path to a Docker seccomp JSON profile applied to every sandbox
   *  container via `--security-opt seccomp=<path>`. Unset → Docker's default
   *  seccomp profile is kept. */
  profilePath?: string;
}

/** Policy controlling whether/how bash commands run inside an isolation backend.
 *  `mode: 'container'` forces every bash command through a container whose only
 *  writable mount is the project dir; the host filesystem is unreachable. When
 *  the backend is unavailable the bash tool fails closed (refuses to run on the
 *  host) when `failIfUnavailable` is set, otherwise it warns and runs on the
 *  host. `mode: 'off'` (or undefined) runs on the host as before. */
export interface SandboxPolicy {
  mode: 'off' | 'container';
  /** Global off-switch for the lightweight host (unshare) auto-sandbox. When set
   *  to exactly `false`, the non-container branch NEVER wraps a command with the
   *  unshare backend even when a trigger pattern matches — the config-level
   *  analogue of the {@link import('./tools/tool-gates').DISABLE_HOST_SANDBOX_ENV}
   *  env knob. Default (unset/true) keeps the auto-wrap on. Parity: Claude Code's
   *  `SandboxManager.isSandboxingEnabled()`. Does NOT affect `mode: 'container'`,
   *  which is a separate, stronger boundary. */
  enabled?: boolean;
  /** Container image (must contain the project toolchain). Default: node:26. */
  image?: string;
  /** The single writable mount (absolute). Defaults to the engine launch cwd. */
  projectDir?: string;
  /** Allow network inside the container (needed for npm/git). Default: true. */
  allowNetwork?: boolean;
  /** Per-host outbound network allow/deny lists + proxy ports. */
  network?: SandboxNetworkConfig;
  /** Granular filesystem read/write carve-outs. */
  filesystem?: SandboxFilesystemConfig;
  /** Custom seccomp syscall-filter profile for the container backend. */
  seccomp?: SandboxSeccompConfig;
  /** OPT-IN: auto-approve the bash tool (skip the permission prompt) in the
   *  prompt-driven modes (ask/acceptEdits) when `mode === 'container'`, because the
   *  container — not the prompt — is the safety boundary (the host filesystem is not
   *  mounted). Honored by {@link import('./permissions').PermissionManager} only when
   *  set to exactly `true`; default (unset/false) keeps the normal bash prompt
   *  (Claude-parity: enterprise `sandbox.autoAllowBashIfSandboxed`). Hardline blocks,
   *  `dangerouslyDisableSandbox`, and explicit deny rules still win. */
  autoAllowBashIfSandboxed?: boolean;
  /** Hard gate on the `dangerouslyDisableSandbox` escape hatch. When false, the
   *  parameter is ignored entirely and EVERY command must run sandboxed. Default: true. */
  allowUnsandboxedCommands?: boolean;
  /** Fail closed at startup if the sandbox is enabled but cannot start (missing
   *  backend / unsupported platform). When false (default) a warning is shown and
   *  commands run unsandboxed. Managed-deployment hard gate. */
  failIfUnavailable?: boolean;
  /** Per-command map of sandbox-violation strings to suppress (command → allowlisted violations). */
  ignoreViolations?: Record<string, string[]>;
  /** Restrict sandboxing (and auto-allow) to specific platforms (e.g. ['linux']). */
  enabledPlatforms?: string[];
  /** Whether a bash command that cannot be sandboxed may still run UNsandboxed.
   *  Default (unset/true) keeps the warn-and-run-on-host behavior; `false` makes
   *  the host sandbox a hard gate — a command that would otherwise degrade to the
   *  weaker ulimits-only path (no namespace isolation) is REFUSED instead. Mirrors
   *  Claude Code's `areUnsandboxedCommandsAllowed()` / `, fallback allowed` status.
   *  Security-weakening when true; surfaced by {@link import('./tools/bash-sandbox').describeSandboxSecurityTradeoffs}. */
  allowUnsandboxedFallback?: boolean;
  /** OPT-IN security relaxation for NESTED sandbox environments (inside an outer
   *  container / CI runner where the user-namespace UID map is already consumed):
   *  when true the host unshare backend drops the `--map-root-user` flag that fails
   *  in a nested user namespace, allowing a (weaker) sandbox to still start. Mirrors
   *  Claude Code's `enableWeakerNestedSandbox`. **Reduces security** — see
   *  {@link import('./tools/bash-sandbox').describeSandboxSecurityTradeoffs}. Default false. */
  enableWeakerNestedSandbox?: boolean;
  /** OPT-IN security relaxation of NETWORK isolation: when true the host unshare
   *  backend does NOT bind `/dev/null` over the default-denied Unix domain sockets,
   *  so a local trust/CA service (e.g. a MITM proxy's cert validator) remains
   *  reachable. Mirrors Claude Code's `enableWeakerNetworkIsolation`. **Reduces
   *  security** — opens a potential data-exfiltration vector; see
   *  {@link import('./tools/bash-sandbox').describeSandboxSecurityTradeoffs}. Default false. */
  enableWeakerNetworkIsolation?: boolean;
  /** Config-level override for the ripgrep binary used by the search tools (grep/glob),
   *  taking precedence over the `USE_BUILTIN_RIPGREP`/`AMI_RIPGREP_PATH` env waterfall.
   *  `command` is the binary to spawn (an absolute path or a bare name resolved via PATH);
   *  `args` are extra flags prepended to every ripgrep invocation. Consumed by
   *  {@link import('./tools/ripgrep-config').resolveRipgrepConfig}. Parity: Claude Code's
   *  sandbox `ripgrep: { command, args }` setting. */
  ripgrep?: { command: string; args?: string[] };
}

export interface ToolContext {
  cwd: string;
  /** Absolute paths/dirs OUTSIDE the launch cwd that the permission layer has already
   *  granted read access to for this call — via `/add-dir`, `--context-file`/`--image`,
   *  an approved out-of-workspace prompt, or auto-allow/yolo. The core path guards treat
   *  these as in-bounds in ADDITION to cwd, so a skill loaded from an external folder
   *  (e.g. ~/.ami/plugins/<name>) can read its own bundled resources. Populated by the
   *  tool executor from PermissionManager.getAllowedReadRoots() right AFTER the per-call
   *  permission check (which is what asks/denies/auto-allows and registers the root), so
   *  it reflects the just-granted dir. Absent/empty → confined to cwd only (the default
   *  for bare engines and tests with no permission manager). */
  allowedReadRoots?: string[];
  /** File-READ deny GLOBS from the permission layer (PermissionManager.getFileReadIgnorePatterns()).
   *  The search tools (grep, glob) EXCLUDE any file matching one of these so a denied file's
   *  matching lines — or even its path — never leak into results, in every output mode. Populated
   *  by the tool executor from the permission manager right after the per-call check (same seam as
   *  allowedReadRoots). Absent/empty → only the hardcoded VCS exclusions apply (bare engines/tests
   *  with no permission manager). Mirrors Claude Code's read-deny → `--glob !pattern` injection. */
  readIgnorePatterns?: string[];
  /** Env vars exported into the `bash` tool's subprocess for the active skill — notably
   *  CLAUDE_SKILL_DIR (and CLAUDE_PLUGIN_ROOT for plugin-packaged skills), so imported
   *  Claude-Code skills that shell out via `$CLAUDE_SKILL_DIR/scripts/...` work (issue #22). */
  skillEnv?: Record<string, string>;
  abortSignal: AbortSignal;
  /** Early-wake hook for the `sleep` tool: returns true when inbound user input is
   *  queued (e.g. a mid-turn message pushed via injectMidTurnUserMessage). When
   *  present, `sleep` polls it and resolves early so the agent reacts promptly
   *  instead of blocking the full duration. Parity: Claude's SleepTool polls
   *  `hasCommandsInQueue()` and wakes within ~1s of an inbound channel message. */
  hasQueuedInput?: () => boolean;
  onProgress?: (data: string) => void;
  /** Tracks file paths that have been read during this session.
   *  Used by file_edit to enforce read-before-write. */
  filesRead?: Set<string>;
  /** Subset of {@link filesRead} read only as a PARTIAL view (offset/limit or a
   *  size-truncated read). A partial read does NOT satisfy the read-before-OVERWRITE
   *  guard — overwriting the whole file after seeing only part silently clobbers the
   *  unseen remainder. Parity: Claude's `readTimestamp.isPartialView`. */
  filesReadPartial?: Set<string>;
  /** Session id for session/agent-scoped tool state (e.g. todo_write store, T2).
   *  Populated by the engine; absent in bare/test contexts (falls back to global). */
  sessionId?: string;
  /** Sub-agent id, when the tool runs inside a forked sub-engine. Combined with
   *  sessionId to key per-agent tool state (T2). */
  agentId?: string;
  /** Operator-configured command prefixes exempt from the bash sandbox (M11).
   *  Threaded to shouldUseSandbox; undefined → the hardcoded defaults only. */
  sandboxExcludedCommands?: string[];
  processManager?: import('./process-manager').ProcessManager;
  _providerConfig?: ProviderConfig;
  /** The engine's persisted {@link EngineConfig.subagentDefaultModel} knob, forwarded so
   *  the task tool can apply it as the default model for a spawn that names none (tri-state:
   *  string = force that model, `null` = follow the leader, absent = provider-aware fallback). */
  _subagentDefaultModel?: string | null;
  _permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  _allowLocalhostForTesting?: boolean;
  _engineFactory?: (config: EngineConfig) => { submit(prompt: string): AsyncIterable<EngineEvent>; shutdown?(): void };
  _skillManager?: import('./skills').SkillManagerLike;
  _hookManager?: import('./hooks').HookManager;
  _mcpManager?: any;
  _parentMessages?: Message[];
  _engineAddSystemReminder?: (msg: string) => void;
  _engineSetActiveSkill?: (name: string | undefined) => void;
  /** Signal the engine to load a deferred tool into the active set (tool_search select). */
  _loadDeferredTool?: (name: string) => void;
  /** Relay a child (sub-engine) event up to the parent engine's event stream so
   *  foreground subagents are visible live. Injected by the tool executor (which
   *  drains it onto the parent generator); absent → no relay (back-compat). */
  _emitChild?: (event: EngineEvent) => void;
  /** Per-tool output budgets (e.g. inline bash stdout cap before spill).
   *  `fileReadMaxTokens`/`fileReadMaxSizeBytes` are per-call overrides for the
   *  Read tool's output-token budget and pre-read size gate; each falls through
   *  to its env override (AMI_FILE_READ_MAX_OUTPUT_TOKENS /
   *  AMI_FILE_READ_MAX_SIZE_BYTES) and then the hardcoded default, validated so a
   *  malformed value can never disable the cap (see tools/file-read-limits.ts). */
  outputBudget?: { bashMaxChars?: number; taskTailLines?: number; taskMaxChars?: number; fileReadMaxTokens?: number; fileReadMaxSizeBytes?: number };
  /** Parent engine's thinking config, inherited by subagents unless overridden. */
  _parentThinking?: ThinkingConfig;
  /** Optional sub-agent handoff safety classifier (parity: Claude's classifyHandoffIfNeeded).
   *  When provided, it reviews an autonomous (yolo/auto) sub-agent's tool calls before the
   *  result is handed back to the parent; omitted, the task tool uses its built-in
   *  dangerous-command heuristic. See tools/handoff-review.ts. */
  _handoffClassifier?: (
    actions: Array<{ tool: string; command?: string }>,
  ) => { flagged: boolean; reason?: string; unavailable?: boolean }
    | Promise<{ flagged: boolean; reason?: string; unavailable?: boolean }>;
  /** Parent engine's permission rules, forwarded to subagents so deny rules are inherited. */
  _parentPermissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Parent engine's LIVE permission mode (the resolved stance, incl. yolo/propagation).
   *  Forwarded so the task tool can CLAMP a spawned subagent's permission mode: a custom
   *  (non-built-in) agent definition may tighten but never ESCALATE past the parent's
   *  stance. Parity: Claude's runAgent permission clamp + source-trust gate. */
  _parentPermissionMode?: string;
  /** Session-wide web-search budget with token-bucket refill to stop runaway loops. */
  _webSearchBudget?: { count: number; max: number; lastRefillTime?: number; refillRate?: number };
  /** Test-only dependency seam for web_search (mock HTTP getter / SSRF validator /
   *  search-URL builder). Never set in production; typed `unknown` here and cast at
   *  the web-search call site to avoid a types.ts -> tools/web-search import cycle. */
  _webSearchHooks?: unknown;
  /** Test-only dependency seam for web_fetch's HTTP getter. Never set in
   *  production; typed `unknown` (cast at the web-fetch call site) to avoid a
   *  types.ts -> tools/web-utils import cycle. */
  _webFetchHttpGet?: unknown;
  detachedMode?: boolean;
  /** Bash isolation policy. When mode==='container', every bash command runs in
   *  a container (project-only writable); fail-closed if the backend is absent. */
  sandbox?: SandboxPolicy;
  /** Current subagent nesting depth (0 = top-level). */
  _agentDepth?: number;
  /** Maximum subagent nesting depth (default 5). */
  _maxAgentDepth?: number;
  /** Shared mutable counter: total agents spawned this session (never decreases). */
  _agentSpawnCount?: { value: number };
  /** Max total agents allowed in a single session (default 50). */
  _maxAgentsPerSession?: number;
  /** Max concurrently running agents (default 10). */
  _maxConcurrentAgents?: number;
  /** Session-lifetime subagent budget (honors CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION
   *  and related env vars). task.ts consults isLifetimeBudgetExhausted() to block
   *  spawns past the per-session cap; depth/concurrency stay governed by the
   *  _agentDepth/_maxConcurrentAgents logic above. */
  subagentLimiter?: import('./subagent-limiter').SubagentLimiter;
  /** True when the user EDITED this tool call's input at the permission prompt before
   *  approving it (the executed input differs from the model's original). Threaded by the
   *  permission layer so a tool can tailor its behavior/telemetry to a human-corrected call.
   *  Parity: Claude's toolUseContext `userModified` (services/tools/toolExecution.ts). */
  userModified?: boolean;
}

/** Result of a tool's structured pre-execution {@link ToolDefinition.validateInput}
 *  (parity: Claude's `ValidationResult`): either success, or a machine-readable
 *  `errorCode` plus a model-facing `message`, produced BEFORE the permission check
 *  and before the tool body runs. */
export type ToolValidationResult =
  | { result: true }
  | { result: false; message: string; errorCode: number };

/** Fine-grained read/search/list classification of a single tool invocation
 *  (parity: Claude's `isSearchOrReadCommand(input)`), used to drive concise,
 *  action-specific transcript summaries. */
export interface ToolAccessClassification {
  isSearch: boolean;
  isRead: boolean;
  isList: boolean;
}

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: ToolInputSchema;
  isReadOnly: boolean;
  /** Per-invocation read-only classification that overrides the static `isReadOnly`
   *  flag for THIS specific call. A few tools are read-only for some inputs and
   *  writable for others — e.g. the config tool only reads on `get` but mutates on
   *  `set`. When present, the permission layer trusts this verdict so a read
   *  auto-allows without a prompt while a write stays on the permission-asking path.
   *  Parity: Claude's `Tool.isReadOnly(input)`. */
  isReadOnlyForInput?: (input: Record<string, unknown>) => boolean;
  /** Whether this tool may run CONCURRENTLY with other calls in the same turn's
   *  batch. Either a static flag OR — like {@link isReadOnlyForInput} — an
   *  input-aware predicate, so a tool that is parallel-safe for some inputs and
   *  must serialize for others (e.g. a read that targets a file another call in the
   *  batch writes) can decide per call. When absent, the executor falls back to the
   *  per-invocation read-only verdict (read-only ⇒ concurrency-safe). Resolve a tool
   *  call's verdict with {@link isToolConcurrencySafe}. Parity: Claude's
   *  `Tool.isConcurrencySafe(input)` (toolOrchestration / StreamingToolExecutor). */
  isConcurrencySafe?: boolean | ((input: Record<string, unknown>) => boolean);
  /** True for tools originating from MCP servers — used for auto-deferral when schema tokens exceed the context budget. */
  isMcp?: boolean;
  /** True when the tool's effects reach OUTSIDE the local environment / are
   *  non-deterministic (a remote fetch, a web search, a message send, an
   *  external-API MCP tool). Distinct from {@link isReadOnly}: a tool can be
   *  read-only yet open-world (a remote fetch mutates nothing locally but still
   *  depends on live external state). Lets renderers tag a call (an `[open-world]`
   *  badge) and lets metadata/introspection surface that a result is non-cacheable /
   *  non-reproducible. Absent / `false` = closed-world (fully local, deterministic).
   *  Parity: Claude's `Tool.isOpenWorld()` (MCPTool defaults false, overridden
   *  per-MCP-tool from the server's `annotations.openWorldHint`). */
  isOpenWorld?: boolean;
  /** Extra capability keywords (3-10 words) weighted by `tool_search`, using terms
   *  NOT already in the name/description (e.g. 'jupyter' for notebook_edit) so a
   *  deferred tool is findable by synonym. Parity: Claude's `Tool.searchHint`. */
  searchHint?: string;
  /** When true, keep this (native) tool OUT of the default advertised set and surface
   *  it on demand via `tool_search` — the per-tool opt-in equivalent of MCP schema-size
   *  auto-deferral. Parity: Claude's `Tool.shouldDefer`. */
  shouldDefer?: boolean;
  /** Per-tool self-declared opt-OUT of deferral: force-KEEP this tool in the initial
   *  advertised set (full schema) even when it would otherwise be deferred behind
   *  `tool_search` (e.g. an MCP tool subject to schema-size auto-deferral). An MCP
   *  server sets this on a tool via `_meta['anthropic/alwaysLoad'] === true`. Checked
   *  before the `isMcp` auto-defer rule. Parity: Claude's `Tool.alwaysLoad`
   *  (ToolSearchTool/prompt.ts — `if (tool.alwaysLoad === true) return false`). */
  alwaysLoad?: boolean;
  /** Input-aware irreversible-operation predicate (delete / overwrite / send). Absent
   *  or `false` = not destructive. Lets the permission layer add an extra guard for ANY
   *  tool, not just bash. Parity: Claude's `Tool.isDestructive(input)`. */
  isDestructive?: (input: Record<string, unknown>) => boolean;
  /** Runtime availability gate — when it returns false the tool is dropped from the
   *  advertised set (feature flag / env condition). Parity: Claude's `Tool.isEnabled()`. */
  isEnabled?: (context?: ToolContext) => boolean;
  /** Per-tool cap (chars) on result size before spill-to-disk, overriding the
   *  engine-global default; `Infinity` = never spill. Parity: `maxResultSizeChars`. */
  maxResultSizeChars?: number;
  /** Alternate names this tool also answers to, for legacy/compat dispatch
   *  (e.g. `KillShell` → task_stop). Parity: Claude's tool `aliases`. */
  aliases?: string[];
  /** Force the provider to constrain generated arguments to exactly match
   *  {@link inputSchema} (OpenAI/Anthropic strict/structured tool mode). Parity: strict. */
  strict?: boolean;
  /** JSON Schema describing the tool's structured result — used to validate a
   *  persisted/resumed tool result before rendering it. Parity: Claude's `Tool.outputSchema`. */
  outputSchema?: ToolInputSchema;
  /** What happens to an in-flight call when the user submits a new message:
   *  `'cancel'` aborts and discards the result; `'block'` (default) runs to completion
   *  while the message waits. Parity: Claude's `Tool.interruptBehavior()`. */
  interruptBehavior?: 'cancel' | 'block';
  /** Structured pre-execution validation run BEFORE the permission check; returns a
   *  machine-readable {@link ToolValidationResult} so the model gets a clean, schema-derived
   *  correction without entering the tool body. Parity: Claude's `Tool.validateInput`. */
  validateInput?: (input: Record<string, unknown>, context: ToolContext) => Promise<ToolValidationResult>;
  /** Compact human-readable activity phrase for progress UIs (e.g. "Reading src/foo.ts")
   *  computed from the input. Parity: Claude's `Tool.getActivityDescription(input)`. */
  getActivityDescription?: (input: Record<string, unknown>) => string;
  /** Fine-grained per-invocation read/search/list classification driving transcript
   *  summaries. Parity: Claude's `Tool.isSearchOrReadCommand(input)`. */
  isSearchOrReadCommand?: (input: Record<string, unknown>) => ToolAccessClassification;
  /** Compact representation of this tool use fed to an auto-mode security classifier
   *  (`''` = skip). Parity: Claude's `Tool.toAutoClassifierInput(input)`. */
  toAutoClassifierInput?: (input: Record<string, unknown>) => string;
  /** Long-form proactive-use / when-to-use (and when-NOT-to-use) guidance carried
   *  alongside the terse `description` and surfaced to the model. Parity: TodoWrite prompt. */
  prompt?: string;
  /** Tool-authored concise transcript label for a call, preferred by renderers over the
   *  generic field-sniffer/JSON fallback. Parity: Claude's `renderToolUseMessage`. */
  renderUseMessage?: (input: Record<string, unknown>) => string;
  /** Input-derived display name for a tool call, so a renderer can show a human-friendly
   *  header (e.g. "Creating worktree") computed from the arguments instead of the raw tool
   *  name. Empty string => fall back to {@link name}. Parity: Claude's `Tool.userFacingName(input)`
   *  (components/tasks/renderToolActivity.tsx). */
  userFacingName?: (input: Record<string, unknown>) => string;
  /** When true, this tool is a transparent wrapper: renderers show only its progress/result
   *  and suppress the tool-name header row (e.g. a passthrough/forwarding tool). Parity:
   *  Claude's `Tool.isTransparentWrapper` (components/messages/AssistantToolUseMessage.tsx). */
  isTransparentWrapper?: boolean;
  /** Per-tool predicate deciding whether a result string is truncated (e.g. exceeded a
   *  line-count cap), so a renderer can show a "truncated" affordance without re-deriving
   *  the tool's own limit. Parity: Claude's `Tool.isResultTruncated(output)` (MCPTool). */
  isResultTruncated?: (output: string) => boolean;
  /** Semantic equality of two parsed inputs for this tool, used to dedup repeated identical
   *  calls (order-insensitive / field-normalized where the generic deep-equal is too strict).
   *  Absent => callers fall back to structural equality. Parity: Claude's
   *  `Tool.inputsEquivalent(a, b)`. */
  inputsEquivalent?: (a: Record<string, unknown>, b: Record<string, unknown>) => boolean;
  /** Per-tool opt-in to Anthropic fine-grained tool streaming (`eager_input_streaming`):
   *  the model streams this tool's input JSON as it is generated instead of only after the
   *  tool_use block completes, lowering latency for tools with long arguments. Unlike the
   *  process-wide `SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING` env knob (which opts in every
   *  tool), this flag opts in ONLY this tool even when the global knob is off. Applied only
   *  on the first-party `api.anthropic.com` path (proxies/Bedrock/Vertex 400 on the field).
   *  Parity: Claude's per-tool `eager_input_streaming` on the cached tool schema. */
  eagerInputStreaming?: boolean;
  execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult>;
}

export interface ProviderConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
  maxTokens?: number;
  temperature?: number;
  provider?: string;
  /** Anthropic prompt-cache TTL. '1h' requires the extended-cache beta; default is the 5-minute ephemeral cache. */
  cacheTtl?: '5m' | '1h';
  /**
   * Actual context window size reported by the serving backend (e.g. vLLM
   * max_model_len). When set, overrides the theoretical value from
   * getContextWindow() for headroom calculations and system prompt sizing.
   */
  contextWindow?: number;
  /**
   * Provider-specific request-body fields merged verbatim into every
   * OpenAI-compatible request (e.g. `chat_template_kwargs` for vLLM-served
   * Qwen/Granite reasoning control). Deep-merged into the AI SDK request body
   * at the transport layer, so it survives SDK schema changes and works for any
   * current or future OpenAI-compatible parameter. Ignored by native
   * Anthropic/Google providers.
   */
  extraBody?: Record<string, unknown>;
  /**
   * Stable id for OpenAI prompt-cache routing. The same conversation must send
   * the same key on every turn. Official OpenAI hosts only; other
   * OpenAI-compatible servers never see `prompt_cache_key`.
   */
  promptCacheKey?: string;
  /**
   * OpenAI SDK-native prompt-cache options. `mode` controls whether caching is
   * implicit (automatic prefix matching, default) or explicit. `ttl` sets the
   * server-side cache lifetime ('30m' or '12h'). Only effective on official
   * OpenAI hosts; ignored by other providers.
   */
  promptCacheOptions?: { mode?: 'implicit' | 'explicit'; ttl?: '30m' | '12h' };
  /** Extra headers merged into every LLM API request. Used for gateway hint
   *  headers (request class, agent type, compaction state). */
  extraHeaders?: Record<string, string>;
  fallbackModel?: string;
  /**
   * Internal recovery flag set by `streamChatCompletionWithRetry` after a STALE
   * keep-alive socket error (ECONNRESET/EPIPE): forces the next attempt to open a
   * FRESH connection with HTTP keep-alive disabled, so the retry does not grab the
   * same poisoned pooled socket. Not user-facing; off by default (pooling stays on).
   */
  forceFreshConnection?: boolean;
  /**
   * Force or constrain the model's tool selection for this request. `'auto'` lets
   * the model decide (the API default), `'required'` forces it to call SOME tool,
   * `'none'` forbids tool calls, and `{ type: 'tool', toolName }` pins one specific
   * tool. Mirrors Claude Code's `tool_choice` request parameter (services/api/claude.ts).
   * Threaded into the AI SDK `toolChoice` option; omitted (model default) when unset.
   */
  toolChoice?: 'auto' | 'required' | 'none' | { type: 'tool'; toolName: string };
  /**
   * Generation stop strings. When the model emits ANY of these sequences the
   * provider halts generation at that point (the sequence itself is not included
   * in the output). Useful for bounded classifier / validator outputs and format
   * fencing — a caller can terminate on a delimiter (e.g. `['</answer>']`).
   * Forwarded verbatim as the AI SDK `stopSequences` option; omitted (no stop
   * strings) when unset or empty. Mirrors Claude Code's `stop_sequences` request
   * parameter (utils/sideQuery.ts).
   */
  stopSequences?: string[];
  /**
   * Stable user/session identifier forwarded to the provider as the request
   * metadata user id — Anthropic `metadata.user_id`, OpenAI `user`. A non-lock-in
   * input parameter that drives abuse detection and can influence rate-limit
   * bucketing. Mirrors Claude Code's `getAPIMetadata().user_id` (services/api/claude.ts).
   * Populated from this field or the AMI_METADATA_USER_ID / CLAUDE_CODE_METADATA_USER_ID
   * env knob ({@link resolveMetadataUserId}); unset ⇒ no metadata is sent.
   *
   * `extra` carries arbitrary correlation fields (device/session ids, deployment
   * tags) merged into the forwarded metadata value, mirroring Claude Code's
   * `CLAUDE_CODE_EXTRA_METADATA` JSON knob (also populatable via AMI_EXTRA_METADATA /
   * CLAUDE_CODE_EXTRA_METADATA). When present, the value becomes a JSON object
   * `{ ...extra, user_id? }` so a gateway can parse every field.
   */
  metadata?: { userId?: string; extra?: Record<string, unknown> };
  /**
   * Suppress prompt-cache WRITES for this one request. Set on fire-and-forget /
   * one-off auxiliary model calls (compaction summaries, insight extraction) whose
   * prefix no future turn will ever read back — writing a fresh cache entry for them
   * is pure cost with no hit. The read path is unaffected; only the Anthropic
   * `cache_control` write markers are dropped. Mirrors Claude Code's `skipCacheWrite`.
   */
  skipCacheWrite?: boolean;
  /**
   * Strip ALL `anthropic-beta` tokens from the outgoing request. Set internally by
   * {@link streamChatCompletionWithRetry} after a gateway/proxy rejects the beta
   * header with a 400 — the first retry drops the ENTIRE beta set (not just the
   * extended-cache token), since a strict proxy that rejects one unknown beta
   * typically rejects every one. Mirrors Claude Code's gateway beta-rejection
   * fallback covering all beta headers. Not user-facing; off by default.
   */
  disableBetas?: boolean;
}

export const STREAM_CHUNK_TYPES = ['content_delta', 'tool_call_delta', 'thinking_delta', 'done', 'error'] as const;
export type StreamChunkType = (typeof STREAM_CHUNK_TYPES)[number];

export interface StreamChunk {
  type: StreamChunkType;
  text?: string;
  toolCall?: {
    index: number;
    id?: string;
    function?: {
      name?: string;
      arguments?: string;
    };
  };
  finishReason?: string;
  usage?: {
    promptTokens: number;
    completionTokens: number;
    totalTokens: number;
    reasoningTokens?: number;
    cachedPromptTokens?: number;
    cacheCreationTokens?: number;
    /** Cache-WRITE tokens billed at the 5-minute ephemeral TTL (parity:
     *  `cache_creation.ephemeral_5m_input_tokens`). */
    cacheCreation5mTokens?: number;
    /** Cache-WRITE tokens billed at the 1-hour ephemeral TTL (parity:
     *  `cache_creation.ephemeral_1h_input_tokens`). */
    cacheCreation1hTokens?: number;
    /** API service tier reported for this request (parity: `service_tier`). */
    serviceTier?: ServiceTier;
    /** Server-side web-search requests reported in this chunk's usage
     *  (parity: `server_tool_use.web_search_requests`). */
    webSearchRequests?: number;
    /** Server-side web-fetch requests reported in this chunk's usage
     *  (parity: `server_tool_use.web_fetch_requests`). */
    webFetchRequests?: number;
  };
  error?: string;
  /** The original Error object from the SDK or provider, preserved for structured classification. */
  errorObject?: Error;
  /** Present on error chunks — the Retry-After value in seconds from the response header, if any. */
  retryAfter?: number;
  /** Response headers from the API call (available on 'done' and 'error' chunks). */
  responseHeaders?: Record<string, string>;
}

/** Per-request API service tier the model call ran under (parity: Claude's usage
 *  `service_tier`, default 'standard'). A throughput/latency class reflected per
 *  request for cost/latency attribution. */
export const SERVICE_TIERS = ['standard', 'priority', 'batch'] as const;
export type ServiceTier = (typeof SERVICE_TIERS)[number];

export interface UsageStats {
  promptTokens: number;
  completionTokens: number;
  reasoningTokens: number;
  totalTokens: number;
  totalCost: number;
  requestCount: number;
  toolCallCount: number;
  turnCount: number;
  /** Prompt tokens served from cache. */
  cachedPromptTokens?: number;
  /** Prompt tokens that were not cached. */
  uncachedPromptTokens?: number;
  /** Fraction of prompt tokens served from cache (0..1). */
  cacheHitRate?: number;
  /** Estimated dollar savings from prompt caching. */
  cachedCostSavings?: number;
  /** API service tier the request(s) ran under (parity: Claude usage `service_tier`,
   *  default 'standard'). Carried for cost/latency attribution. */
  serviceTier?: ServiceTier;
  /** Count of server-side web-search requests folded into this usage record
   *  (parity: Claude `server_tool_use.web_search_requests`). */
  webSearchRequests?: number;
  /** Count of server-side web-fetch requests folded into this usage record
   *  (parity: Claude `server_tool_use.web_fetch_requests`). */
  webFetchRequests?: number;
}

import type { ThinkingLevel } from './model-capabilities';
export type { ThinkingLevel } from './model-capabilities';

export interface ThinkingConfig {
  enabled: boolean;
  level: ThinkingLevel;
  budgetTokens?: number;
  type?: 'enabled' | 'disabled';
}

/**
 * Every reason the engine loop can terminate a submit() and emit its final
 * `done` event. This runtime array is the single source of truth: the
 * `ExitReason` union below is derived from it, so adding a variant here adds it
 * to the type (and vice versa is impossible). Tests introspect this value to
 * guarantee every variant is covered.
 */
export const EXIT_REASONS = [
  'completed',
  'max_turns',
  'error',
  'abort',
  'session_timeout',
  'max_steps',
  'budget_exceeded',
  'hook_denied',
  'superinference_stop',
  'consecutive_errors',
  'escalation_terminate',
  'no_edit_stall',
  'keepalive_exhausted',
  'context_too_small',
  'usage_limit_watchdog',
] as const;

export type ExitReason = (typeof EXIT_REASONS)[number];

export const ENGINE_EVENT_TYPES = [
  'text_delta', 'thinking_delta', 'tool_call_generating', 'tool_use_start',
  'tool_use_progress', 'tool_use_result', 'turn_complete', 'usage_update',
  'error', 'retry_attempt', 'error_classified', 'provider_changed',
  'checkpoint_created', 'analytics_summary', 'superinference_state',
  'suggest_file_save', 'user_question', 'session_title', 'memory_warning',
  'plan_mode_changed', 'task_updated', 'skill_activated',
  'subagent_start', 'subagent_progress', 'subagent_result', 'todo_update',
  'mcp_resource_update', 'mcp_polling_update', 'mcp_connection_changed',
  'external_import_blocked', 'peer_message', 'background_task_complete', 'done',
] as const;

/** A single todo item as rendered to the user (T1/T5). `id` is optional
 *  (Claude-Code parity); `activeForm` is the present-continuous label shown while
 *  the item is in progress. */
export interface TodoItemView {
  id?: string;
  content: string;
  status: 'pending' | 'in_progress' | 'completed' | 'cancelled';
  activeForm?: string;
}

/**
 * One selectable option in an AskUserQuestion prompt (parity: Claude's
 * `QuestionOption`). Single source of truth for the option shape, shared by the
 * `user_question` {@link EngineEvent} and the {@link EngineConfig.onUserQuestion}
 * callback so the carried fields can never drift between emit and handler.
 */
export interface UserQuestionOption {
  label: string;
  description: string;
  /** Markdown preview rendered in a bordered box when this option is focused
   *  (side-by-side preview panel). Parity: Claude's `option.preview`. */
  preview?: string;
  /** When true the option is shown but not selectable (dimmed; Enter/number/space
   *  refuse to act on it). Parity: Claude's `option.disabled`. */
  disabled?: boolean;
}

/** Three-state terminal outcome of a (sub)agent/task, distinguishing a clean finish
 *  from an error from a user/abort stop (parity: Claude's task_notification `status`:
 *  'completed' | 'failed' | 'stopped'). Richer than a bare success boolean. */
export const TASK_TERMINAL_STATUS = ['completed', 'failed', 'stopped'] as const;
export type TaskTerminalStatus = (typeof TASK_TERMINAL_STATUS)[number];

/** Live machine-readable usage snapshot for a running (sub)agent, carried on progress
 *  and terminal events (parity: Claude's task_progress usage `{ total_tokens, tool_uses,
 *  duration_ms }`). */
export interface TaskUsageSnapshot {
  /** Total tokens consumed by the (sub)agent so far. */
  totalTokens: number;
  /** Number of tool calls the (sub)agent has made so far. */
  toolUses: number;
  /** Wall-clock milliseconds elapsed since the (sub)agent started. */
  durationMs: number;
}

/** Persisted per-skill usage statistic (parity: Claude Code's global-config
 *  `skillUsage` map, `utils/suggestions/skillUsageTracking.ts`). Keyed by skill name
 *  in the global config, each entry records how many times the skill was invoked and
 *  when it was last used (epoch ms). Lets a skill's frecency ranking survive across
 *  sessions instead of resetting to zero every launch (AMI previously tracked this
 *  only in-memory, session-scoped). */
export interface SkillUsageStat {
  /** Total number of times this skill has been invoked (monotonic, persisted). */
  usageCount: number;
  /** Epoch-ms timestamp of the most recent invocation. */
  lastUsedAt: number;
}

/** Fold one invocation of `name` into a persisted {@link SkillUsageStat} map at time
 *  `now`, returning a NEW map (the input is never mutated). The single canonical
 *  increment+stamp formula so every persistence call site agrees — bumps `usageCount`
 *  and refreshes `lastUsedAt`. A malformed/absent prior entry is treated as a zero
 *  baseline. Parity: Claude's `skillUsage[name] = { usageCount: prev+1, lastUsedAt: now }`. */
export function mergeSkillUsageStat(
  record: Record<string, SkillUsageStat> | undefined,
  name: string,
  now: number = Date.now(),
): Record<string, SkillUsageStat> {
  const prev = record?.[name];
  const prevCount = typeof prev?.usageCount === 'number' && Number.isFinite(prev.usageCount) && prev.usageCount > 0
    ? prev.usageCount
    : 0;
  return { ...(record ?? {}), [name]: { usageCount: prevCount + 1, lastUsedAt: now } };
}

/** The operating MODE a session ran under, persisted so `--resume` can reconcile a
 *  resumed session against the live environment (parity: Claude Code's
 *  `coordinatorMode.ts` `SessionMode` / `matchSessionMode`). `'coordinator'` is the
 *  pure-orchestrator stance (SUPERINFERENCE_COORDINATOR_MODE); `'normal'` is the
 *  default implementer stance. Distinct from permission mode and sandbox mode. */
export const SESSION_OPERATING_MODES = ['normal', 'coordinator'] as const;
export type SessionOperatingMode = (typeof SESSION_OPERATING_MODES)[number];

/** Outcome of reconciling a resumed session's stored operating mode against the live
 *  mode (see {@link reconcileSessionMode}). `changed` is true only when the caller must
 *  flip the live mode; `target` is the mode the session should run under; `warning` is a
 *  human-readable notice to surface when (and only when) a flip occurred. */
export interface SessionModeReconciliation {
  changed: boolean;
  target: SessionOperatingMode;
  warning?: string;
}

/**
 * Reconcile a resumed session's STORED operating mode against the CURRENT (live) mode,
 * returning what the caller should do. Pure — it mutates nothing and reads no globals, so
 * every resume path can share one decision formula. Branch logic mirrors Claude Code's
 * `matchSessionMode` (coordinator/coordinatorMode.ts):
 *   - `stored === undefined` → no-op (`changed:false`), back-compat for sessions saved
 *     before operating-mode tracking existed (they carry no mode to match).
 *   - `stored === current` → no-op; the live mode already matches the resumed session.
 *   - mismatch → `changed:true`, `target = stored` (the resumed session wins), and a
 *     `warning` describing the correction ("Entered/Exited coordinator mode to match
 *     resumed session.") so a silently-wrong mode on resume is surfaced + corrected.
 */
export function reconcileSessionMode(
  stored: SessionOperatingMode | undefined,
  current: SessionOperatingMode,
): SessionModeReconciliation {
  if (stored === undefined || stored === current) {
    return { changed: false, target: current };
  }
  const warning = stored === 'coordinator'
    ? 'Entered coordinator mode to match resumed session.'
    : 'Exited coordinator mode to match resumed session.';
  return { changed: true, target: stored, warning };
}

export type EngineEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_generating'; toolName: string; toolIndex: number; argsDelta: string }
  | { type: 'tool_use_start'; toolName: string; toolCallId: string; input: Record<string, unknown> }
  | { type: 'tool_use_progress'; toolCallId: string; data: string }
  | { type: 'tool_use_result'; toolCallId: string; toolName: string; output: string; isError: boolean }
  | { type: 'turn_complete'; content: string | null; toolCalls: ToolCallContent[]; finishReason?: string }
  | { type: 'usage_update'; stats: UsageStats }
  | { type: 'error'; error: string }
  // `statusCode` is `null` for a connection-level failure that produced NO HTTP
  // response (timeout / network drop), distinct from a real HTTP status such as 429
  // or 503 (parity: Claude's SDKAPIRetryMessage `error_status: number | null`).
  | { type: 'retry_attempt'; attempt: number; maxRetries: number; delayMs: number; statusCode: number | null }
  | { type: 'error_classified'; category: string; retryable: boolean; willRetry: boolean; message?: string }
  | { type: 'provider_changed'; provider: string; model: string; reason: string }
  | { type: 'checkpoint_created'; checkpointId: string; files: string[] }
  | { type: 'analytics_summary'; summary: Record<string, number> }
  | { type: 'superinference_state'; state: { value: number; entropy: number; eig: number; step: number; ppv: number }; stopReason: { type: string; detail: string } }
  | { type: 'suggest_file_save'; content: string; suggestedPath: string; lineCount: number }
  | { type: 'user_question'; toolCallId: string; question: string; options: UserQuestionOption[]; allowFreeText: boolean; header?: string; multiSelect?: boolean; answers?: string[]; annotations?: Record<string, unknown>; defaultIndex?: number }
  | { type: 'session_title'; title: string }
  | { type: 'memory_warning'; rssBytes: number; thresholdBytes: number; recoverySteps: string[] }
  | { type: 'plan_mode_changed'; enabled: boolean }
  | { type: 'task_updated'; taskId: number; status: string; subject: string }
  | { type: 'skill_activated'; skill: string; description: string; hasMcpServers: boolean; source: 'model' | 'user' }
  | { type: 'subagent_start'; nodeId: string; depth: number; agentName?: string; model?: string; prompt: string; color?: string;
      /** The resolved sub-agent KIND this task was spawned as (`subagent_type`): a built-in
       *  mode (`explore`/`plan`/`general`) or a named agent definition. Distinct from
       *  {@link agentName} (a per-spawn display/identity name) — two agents can share a name
       *  but differ in type. Lets a roster surface the classification at a glance. Parity:
       *  Claude's `task_started` event `task_type` (sdkEventQueue.ts). Omitted when unset. */
      taskType?: string }
  | { type: 'subagent_progress'; nodeId: string; depth: number; activity: 'text' | 'thinking' | 'tool'; detail: string; usage?: TaskUsageSnapshot; lastToolName?: string }
  | { type: 'subagent_result'; nodeId: string; depth: number; success: boolean; summary: string; tokens: number; status?: TaskTerminalStatus; toolUses?: number; durationMs?: number; outputPath?: string }
  | { type: 'todo_update'; todos: TodoItemView[] }
  /** An MCP server pushed a resource-update (`notifications/resources/updated`) or
   *  resource-list-change (`notifications/resources/list_changed`) notification.
   *  Carries server provenance so the user has an audit trail of WHICH server
   *  changed WHAT (and why, for a list change). Parity: Claude's
   *  `UserResourceUpdateMessage`. `uri` is empty for a whole-list change. */
  | { type: 'mcp_resource_update'; server: string; uri: string; reason?: string }
  /** A polled MCP tool's output changed between polls. Carries server + tool
   *  provenance so the user has an audit trail of WHICH polled tool (on WHICH server)
   *  produced new output. The engine also injects a model-facing
   *  `<mcp-polling-update type="tool" …>` user turn. Parity: Claude's
   *  `UserResourceUpdateMessage` polling branch. */
  | { type: 'mcp_polling_update'; server: string; tool: string; reason?: string }
  /** An MCP server's connection state changed mid-session in a user-relevant way:
   *  `'lost'` (a ready server dropped), `'recovered'` (it came back after a drop), or
   *  `'failed'` (reconnection was abandoned). Carries server provenance + an optional
   *  `detail` (the raw state / attempt count) so a dropped tool/MCP connection is
   *  surfaced proactively instead of silently vanishing. Parity: Claude's IDE/MCP
   *  connection status indicator (`useIDEStatusIndicator`). */
  | { type: 'mcp_connection_changed'; server: string; state: 'lost' | 'recovered' | 'failed'; detail?: string }
  /** A project instruction file (CLAUDE.md / AGENTS.md) `@import`ed a file OUTSIDE
   *  the workspace trust boundary, so its content was stripped and replaced with an
   *  inline `[import blocked …]` placeholder. The engine detects this silently via the
   *  `instructionsLoaded` hook with `loadReason:'import-blocked-outside-workspace'`.
   *  This event surfaces it so an interactive user sees WHICH external paths were
   *  dropped instead of acting on instruction files that were quietly truncated.
   *  `paths` lists every blocked import drained together. Parity: Claude Code's
   *  `ClaudeMdExternalIncludesDialog` (enumerated external-import warning). */
  | { type: 'external_import_blocked'; paths: string[] }
  /** An inbound peer/teammate message arrived on this session's mailbox and was
   *  relayed into the conversation. The engine already injects it as a fenced
   *  `<peer-message>` user turn for the model, but the TUI renders from EngineEvents,
   *  not the message list — so without this event the human never sees that a peer
   *  spoke. Carries the sender (`from`), the relayed `body`, and the optional
   *  per-message `summary`/roster `color` the sender stamped. Parity: Claude Code's
   *  dedicated `UserTeammateMessage` transcript renderer. */
  | { type: 'peer_message'; from: string; body: string; summary?: string; color?: string }
  /** A backgrounded task (bash/agent/monitor) reached a terminal state. The engine
   *  already injects a collapsed completion note for the model, and this event surfaces
   *  each completion distinctly to the TUI/detached frontend with its status-aware
   *  `summary` phrase (`completed (exit code: 0)` / `failed with exit code 1` /
   *  `was stopped`) so a finished/failed/killed background task is visible inline
   *  instead of only in the model's context. Parity: Claude Code's shell/local-agent
   *  completion notification. `outputFile` is the on-disk path the task's captured
   *  output was streamed to (the `${taskId}.output` spool), so a frontend can offer a
   *  "view full output" affordance that reads the complete transcript instead of only
   *  the collapsed summary. Parity: Claude Code's task terminal notification
   *  `output_file` field (`utils/sdkEventQueue.ts`). Omitted only on a legacy direct
   *  emit that never recorded a spool path. */
  | { type: 'background_task_complete'; taskId: string; status: string; summary: string; outputFile?: string }
  | { type: 'done'; totalTurns: number; exitReason: ExitReason };

/**
 * Natural-language auto-mode (YOLO) rule configuration (parity: Claude's
 * `settings.autoMode` / `AutoModeConfig`): plain-English allow / soft-deny rules
 * plus an environment description that are substituted into the security-classifier
 * prompt, an optional dedicated classifier model, and a two-stage strategy selector.
 */
export interface AutoModeConfig {
  /** Plain-English descriptions of autonomous actions to permit. */
  allow?: string[];
  /** Plain-English descriptions of actions to soft-block (ask instead of run). */
  soft_deny?: string[];
  /** Plain-English description of the runtime environment, for the classifier. */
  environment?: string[];
  /** Dedicated model for the security classifier (falls back to the main-loop model). */
  model?: string;
  /** Classifier strategy: tool_use (false/undefined), both stages (true), stage-1-only
   *  ('fast'), or stage-2-only ('thinking'). Parity: `twoStageClassifier`. */
  twoStageClassifier?: boolean | 'fast' | 'thinking';
}

export interface EngineConfig {
  provider: ProviderConfig;
  cwd: string;
  tools?: ToolDefinition[];
  systemPrompt?: string;
  maxTurns?: number;
  maxTurnsCeiling?: number;
  /** Max time (ms) to wait for MCP servers to connect before the first submit proceeds (default 3000). */
  mcpStartupBudgetMs?: number;
  /**
   * Auto-connect to a code-editor-launched MCP endpoint at startup (Claude-parity
   * `autoConnectIde`). One input to the auto-connect waterfall — a supported editor
   * terminal, an inherited `AMI_SSE_PORT`, or a truthy `AMI_AUTO_CONNECT_IDE` also
   * enable it; an explicit falsy `AMI_AUTO_CONNECT_IDE` is a hard opt-out. Only
   * fires when the editor actually exported its SSE port.
   */
  autoConnectIde?: boolean;
  /** Session-wide cap on web_search tool calls (default 200) to stop runaway loops. */
  maxWebSearchesPerSession?: number;
  /** Cap (chars) on inline bash stdout before it is spilled to a file. */
  bashOutputMaxChars?: number;
  /** Cap (chars) on inline background task output before it is spilled to a file. */
  taskOutputMaxChars?: number;
  /** Inactivity watchdog (ms) for a stalled-but-open stream, distinct from the total request timeout. */
  streamIdleTimeoutMs?: number;
  /** RSS threshold (bytes) that emits a one-time memory_warning with recovery steps (default 1.5 GiB). */
  memoryWarningRssBytes?: number;
  /** Independent ABSOLUTE RSS ceiling (bytes) at which the session is treated as
   *  CRITICAL — escalating the memory_warning straight to its top tier regardless of
   *  the (possibly lower) {@link memoryWarningRssBytes} warning threshold. This is a
   *  second, absolute threshold rather than a value derived from the warning one, so
   *  an operator can keep an early soft warning while only declaring a hard-critical
   *  state at a true memory ceiling. Parity: Claude Code's useMemoryUsage two
   *  independent absolute thresholds (HIGH 1.5 GiB / CRITICAL 2.5 GiB). Default 2.5 GiB. */
  memoryCriticalRssBytes?: number;
  /** Injectable RSS reader for tests (defaults to process.memoryUsage().rss). */
  memoryUsageReader?: () => number;
  /** Defer long-tail tools from the per-turn catalog until tool_search loads them, saving prompt tokens. */
  deferTools?: boolean;
  /** Extra tool names to always keep loaded when deferTools is on (beyond the essential set + tool_search). */
  alwaysLoadTools?: string[];
  /** MCP server names whose tools bypass the MCP schema-size deferral threshold. */
  alwaysLoadMcpServers?: string[];
  abortController?: AbortController;
  /**
   * Tool names to exclude from this engine's toolset. Used for hook-spawned
   * sub-engines (prompt/agent hooks) so a verification hook cannot recursively
   * spawn subagents (`task`) or enter the interactive `plan_mode`
   * (Claude Code's ALL_AGENT_DISALLOWED_TOOLS).
   */
  disallowedTools?: string[];
  onPermissionRequest?: (toolName: string, input: Record<string, unknown>) => Promise<boolean>;
  /** Centralized permission prompt handler. Both CLI and VSCode implement this interface. */
  permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  /**
   * Name of a registered tool to DELEGATE every non-interactive ("ask") permission decision
   * to (parity: Claude Code's `--permission-prompt-tool <name>`). In a headless/detached run,
   * instead of auto-denying an `ask`, the engine invokes this tool with
   * `{ tool_name, input, tool_use_id }` and parses its JSON result into an allow/deny decision
   * (see core `parsePermissionPromptToolResult`). The named tool is removed from the advertised
   * toolset so the model cannot call it directly. Absent → the standard headless policy applies.
   */
  permissionPromptToolName?: string;
  /**
   * Resolve an MCP server elicitation request (2025-06-18). Interactive
   * frontends prompt the user; absent this handler the engine declines, so a
   * detached run never blocks. Receives the server name and the request.
   */
  onMcpElicitation?: (serverName: string, request: import('./mcp').McpElicitationRequest) => Promise<import('./mcp').McpElicitationResult>;
  /**
   * Approve/deny an MCP server sampling request (run an LLM turn on the server's
   * behalf) before the engine executes the model call. Return false to refuse.
   * Absent this handler the engine refuses sampling (safe default), per the
   * spec's human-in-the-loop requirement.
   */
  onMcpSamplingApproval?: (serverName: string, request: import('./mcp').McpSamplingRequest) => Promise<boolean>;
  /**
   * Interactive OAuth 2.1 consent leg for HTTP MCP servers (open browser +
   * capture the redirect code). Interactive frontends provide it; detached runs
   * omit it so the manager's headless default refuses with guidance.
   */
  onMcpOAuthAuthorize?: import('./mcp').AuthorizeFn;
  /**
   * MCP servers to block, by name (case-insensitive) or normalized upstream URL.
   * Default-open: an empty/absent list blocks nothing. A URL entry blocks every
   * configured server pointing at that upstream, regardless of its name.
   */
  deniedMcpServers?: string[];
  /**
   * M14 — opt-in gate for PROJECT-tier MCP servers (configs under cwd:
   * `.ami/mcp.json` + `.mcp.json`). Default OFF/undefined: project MCP
   * servers auto-connect exactly as before. When true, a NEW (not-yet-approved)
   * project server is registered PENDING instead of auto-connecting; the user
   * approves it via `/mcp approve`, and the approval persists to
   * `<cwd>/.ami/approved-mcp.json`. User/global-tier configs are never
   * gated.
   */
  mcpRequireApproval?: boolean;
  /** Maximum token budget for context management. Defaults to 100 000. */
  tokenBudget?: number;
  /** Resume a specific session. When set, the engine loads its messages on init. */
  sessionId?: string;
  /** Forked sub-agent id. When set (by the `task` tool for a sub-engine), the engine
   *  threads it into every tool's {@link ToolContext.agentId} so file-reading tools use
   *  an ISOLATED per-agent file cache (clone-on-fork / release-on-finish) rather than the
   *  directory-shared one. Absent for the top-level engine. */
  agentId?: string;
  /**
   * Seed the engine's conversation with these messages at construction (only when
   * no `sessionId` restore supplied any). Used by a `fork` sub-agent to inherit the
   * parent's REAL message history (user turns, assistant tool_use/thinking, tool
   * results) as a context-continuing clone instead of a lossy text summary, which
   * both preserves full structured context and maximizes prompt-cache prefix
   * sharing. Routed through the same validation/healing as a session restore so a
   * trailing/orphaned tool_use cannot produce an API-invalid transcript. Parity:
   * Claude Code's `buildForkedMessages` + `filterIncompleteToolCalls` (runAgent.ts).
   */
  initialMessages?: Message[];
  /** Override the default session directory (~/.ami/projects/<cwd-slug>/sessions/). */
  sessionDir?: string;
  /** Permission mode: 'ask' (default), 'auto-allow', or 'deny-all'. */
  permissionMode?: string;
  /** Bash isolation policy forwarded to every ToolContext. When mode==='container',
   *  all bash commands run inside a container (project-only writable), fail-closed.
   *  Undefined = host execution (the default for programmatic/test use; the CLI sets
   *  container mode for real runs). */
  sandbox?: SandboxPolicy;
  /** Operator-configured command prefixes exempt from the bash sandbox (M11),
   *  forwarded to every ToolContext. Additive to the built-in defaults. */
  sandboxExcludedCommands?: string[];
  /**
   * Managed killswitch mirrored from the merged config's
   * `disableBypassPermissionsMode`. When true the engine arms
   * `PermissionManager.setDisableBypass(true)` so bypass/auto-allow modes stay
   * forbidden for the whole session (startup, Shift+Tab, `/permissions`, resume,
   * and live config reloads). Managed-tier only; see {@link ProjectConfig}.
   */
  disableBypassPermissionsMode?: boolean;
  /**
   * Managed hook kill-switch mirrored from the merged config's `disableAllHooks`.
   * When true the engine calls `HookManager.setDisableAllHooks(true, true)` before
   * loading hooks, so ZERO hooks register/run for the whole session. Managed-tier
   * only; see {@link ProjectConfig}. Parity: Claude Code's `disableAllHooks`.
   */
  disableAllHooks?: boolean;
  /**
   * Managed hook lockdown mirrored from the merged config's `allowManagedHooksOnly`.
   * When true the engine calls `HookManager.setAllowManagedHooksOnly(true)` so only
   * admin/managed-sourced hooks load (user/project/plugin hooks blocked). Managed-tier
   * only; see {@link ProjectConfig}. Parity: Claude Code's `allowManagedHooksOnly`.
   */
  allowManagedHooksOnly?: boolean;
  /** Permission rules evaluated in order; first match wins. */
  permissionRules?: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  /** Least-privilege capability ceiling for a forked/background sub-engine (parity:
   *  Claude's forked-agent `canUseTool`). A HARD cap enforced ahead of every mode —
   *  confines the sub-agent to read-only bash and/or writes within a directory even
   *  under auto-allow. Set by the skill tool when a forked skill declares a sandbox;
   *  undefined for the main engine (inert). */
  capabilityCeiling?: ToolCapabilityCeiling;
  /** Extra absolute paths/dirs the user has granted read access to OUTSIDE the launch
   *  cwd (e.g. dirs of `--context-file`/`--image` targets, or `--add-dir`). The tools are
   *  confined to the launch cwd; these roots widen that boundary to user-pointed resources. */
  allowedReadRoots?: string[];
  /** Model to fall back to on errors (e.g., 'gemini-2.0-flash' as fallback for 'gemini-1.5-pro'). */
  fallbackModel?: string;
  /** Ordered fallback model chain (up to 3), stepped through on provider errors. */
  fallbackModels?: string[];
  /** Cheap/fast model for auxiliary tasks (compaction, follow-up suggestions). Falls back to primary model if not set. */
  compactionModel?: string;
  /** Architect model for dual-model (plan + execute) mode. When set, the engine runs a
   *  reasoning phase with this model before each tool-execution phase with the primary model. */
  architectModel?: string;
  /** Extended thinking/reasoning configuration. */
  thinking?: ThinkingConfig;
  /** Cap the reasoning effort level (e.g. 'medium' prevents 'high'/'max'). */
  maxEffortLevel?: string;
  /** SuperInference belief tracking. shouldStop() ends the session only when enforcing is true. */
  superinference?: SuperInferenceConfig;
  /** Lifecycle hooks for post-sampling, stop, and error events. */
  hooks?: import('./hooks').HookManager;
  /** Admin/settings policy for HTTP hooks — URL allowlist + header env-var
   *  allowlist. Undefined leaves HTTP hooks unrestricted (SSRF guard + non-2xx
   *  handling always apply regardless). */
  httpHookPolicy?: import('./hooks').HttpHookPolicy;
  /** Invocation-scoped hooks a forked skill or spawned subagent contributes from its
   *  OWN definition's frontmatter `hooks` field (parity with Claude Code's
   *  registerFrontmatterHooks). Registered on this (sub-)engine's HookManager AFTER
   *  the file-loaded hooks, so they layer on top for the lifetime of the sub-engine
   *  and are torn down when it shuts down. Threaded by the skill/task tools from
   *  SkillDefinition.hooks / AgentDefinition.hooks. */
  frontmatterHooks?: import('./hooks').HookConfigEntry[];
  /** True when {@link frontmatterHooks} come from a SUBAGENT's own frontmatter (set by
   *  the task tool): a `stop` hook is remapped to `subagentStop` on registration, since a
   *  completing subagent fires SubagentStop (parity: Claude Code registerFrontmatterHooks
   *  `isAgent`). Left false/undefined for forked-skill hooks, which keep `stop`. */
  frontmatterHooksIsAgent?: boolean;
  /** Human-readable origin of {@link frontmatterHooks} (e.g. `agent 'reviewer'` /
   *  `skill 'deploy'`) for the attribution + Stop→SubagentStop conversion debug log. */
  frontmatterHooksSource?: string;
  /** Stance skill name (--skill) — a skill pinned to govern the whole session/run:
   *  its body becomes the system-prompt overlay, plus its tool-scoping, bash
   *  auto-allow, MCP servers, and thinking level. Replaces the old persona. */
  skill?: string;
  /** Named output style overlay appended to the system prompt (e.g. 'concise'). */
  outputStyle?: string;
  /** Managed allowlist of selectable models; non-listed models are blocked. */
  enforceAvailableModels?: string[];
  /** Merged project-level configuration (from .ami/config.json). */
  projectConfig?: import('./config').ProjectConfig;
  /** Maximum number of tool-call steps before forcing a summary. When reached,
   *  the engine injects an assistant prefill requesting a summary and stops
   *  processing further tool calls. Default: unlimited. */
  maxSteps?: number;
  /** When true, the engine operates in read-only "plan" mode: writable tools
   *  are blocked, and the system prompt instructs the model to only read,
   *  search, and describe its plan. */
  planMode?: boolean;
  /** Handler for AskUserQuestion tool. The UI implements this to show
   *  the question and return the user's answer. Options carry optional per-option
   *  `preview`/`disabled` and an optional `defaultIndex` to preselect/focus a
   *  recommended option. */
  onUserQuestion?: (question: string, options: UserQuestionOption[], allowFreeText: boolean, multiSelect?: boolean, defaultIndex?: number) => Promise<string>;
  /** When true, the engine generates a session title via LLM after the first turn. */
  enableTitleGeneration?: boolean;
  /** When true, the engine is running in non-interactive (detached / --prompt) mode. */
  detachedMode?: boolean;
  /** Task classification for detached mode. 'bugfix' injects the full bug-fix
   *  overlay (mandatory tool calls, bug-fix loop, edit-before-complete). Omit or
   *  set to 'generic' for a lightweight overlay that lets the model answer
   *  directly when no tools are needed. */
  taskType?: 'generic' | 'bugfix';
  /** Current subagent nesting depth — propagated to child ToolContext. */
  agentDepth?: number;
  /** Maximum subagent nesting depth (default 5). */
  maxAgentDepth?: number;
  /** Maximum USD budget for the session. If set, the engine stops when total cost exceeds this amount. */
  maxBudgetUsd?: number;
  /** Wall-clock session limit in milliseconds. If set, the engine stops when elapsed time exceeds this. */
  maxSessionMs?: number;
  /** Chat mode — controls tool availability and autonomy level. */
  mode?: 'ask' | 'edit' | 'agent';
  /** Per-request timeout in milliseconds for the API provider. Default: 300 000 (5 min). */
  requestTimeoutMs?: number;
  /** @internal Override provider-level max retries. Set for testing and by the
   *  unattended-retry mode (persistent retry through long outages). */
  _providerMaxRetries?: number;
  /** Unattended/persistent retry mode for long detached runs. When on, the
   *  provider keeps retrying transient failures through a long outage window
   *  (per-attempt delay capped, total window bounded) instead of giving up after
   *  the default ~5 attempts. Opt-in via SUPERINFERENCE_UNATTENDED_RETRY=1
   *  (CLAUDE_CODE_UNATTENDED_RETRY accepted for compatibility) or project config.
   *  Mode-agnostic: identical in interactive and detached runs. */
  unattendedRetry?: boolean;
  /** @internal Override provider-level base delay in ms (for testing). */
  _providerBaseDelayMs?: number;
  /** Maximum tool iterations before prompting to continue. Default 200. */
  maxToolIterations?: number;
  /** Safe mode: skip loading untrusted project-local config — no project hooks
   *  (.ami/hooks.json), no MCP server auto-connect, and no project
   *  memory/skills in the system prompt. For running in untrusted repos. */
  safeMode?: boolean;
  /** Bare mode: strip system prompt to minimal identity + date. No persona,
   *  memory, skills, project config, or tool instructions. For benchmarking
   *  and raw LLM interaction. */
  bareMode?: boolean;
  /** Behavior when tool iteration limit is reached: 'stop' or 'confirm'. Default 'confirm'. */
  toolIterationBehavior?: 'stop' | 'confirm';
  /** Minimum estimated tokens before idle compaction will run (default 200 000). */
  idleCompactionMinTokens?: number;
  /** Milliseconds of inactivity after a turn before proactive compaction fires (default 3 240 000 = 54 min). */
  idleCompactionDelayMs?: number;
  /** Maximum milliseconds to wait for a usage-limit retry-after in detached mode.
   *  If the Retry-After header exceeds this, the session terminates instead of
   *  waiting indefinitely. Default: 300 000 (5 min). Set 0 to disable. */
  maxUsageLimitWaitMs?: number;
  /** Maximum delay cap for exponential backoff on overloaded (529) retries, in ms.
   *  Prevents unbounded waits. Default: 60 000 (1 min). Env: SUPERINFERENCE_OVERLOADED_RETRY_MAX_DELAY_MS. */
  overloadedRetryMaxDelayMs?: number;
  /** Base delay for overloaded (529) retries, in ms. Default: 1000.
   *  Env: SUPERINFERENCE_OVERLOADED_RETRY_BASE_DELAY_MS. */
  overloadedRetryBaseDelayMs?: number;
  /** Maximum total wall-clock time for all retry attempts, in ms. If total retry
   *  time exceeds this, stop retrying. Default: 0 (no limit).
   *  Env: SUPERINFERENCE_RETRY_MAX_TOTAL_WAIT_MS. */
  retryMaxTotalWaitMs?: number;
  /** Maximum retries specifically for non-streaming timeout fallbacks.
   *  Env: SUPERINFERENCE_NONSTREAMING_TIMEOUT_RETRIES. */
  nonstreamingTimeoutRetries?: number;
  /** Maximum transient error retries (connection resets, 502/503/529). Default: 5.
   *  Env: SUPERINFERENCE_MAX_TRANSIENT_RETRIES. */
  maxTransientRetries?: number;
  /** Number of CONSECUTIVE capacity-overload errors (HTTP 503/529, "overloaded")
   *  the engine rides out on the SAME model before switching to a fallback model.
   *  An overload is usually transient, so switching models on the first 529 is
   *  premature; the engine retries up to this many times first, then falls back.
   *  A genuine model-unavailable (e.g. `model_not_found`) is unaffected and still
   *  falls back immediately. Default: 3. Env: SUPERINFERENCE_MAX_529_RETRIES
   *  (CLAUDE_CODE_MAX_529_RETRIES accepted as an alias). Parity: Claude Code's
   *  MAX_529_RETRIES threshold before FallbackTriggeredError. */
  max529RetriesBeforeFallback?: number;
  /** Maximum delay cap for transient error backoff, in ms. Default: 30 000 (30s).
   *  Env: SUPERINFERENCE_TRANSIENT_RETRY_MAX_DELAY_MS. */
  transientRetryMaxDelayMs?: number;
  /** Enable gateway hint headers on LLM requests (x-superinference-request-class,
   *  x-superinference-agent-type, x-superinference-compaction).
   *  Env: SUPERINFERENCE_GATEWAY_HINT_HEADERS=1. */
  gatewayHintHeaders?: boolean;
  /** @internal Override the chat-completion stream implementation (for testing).
   *  Defaults to the real streamChatCompletionWithRetry. Injecting a generator
   *  that emits thinking-only chunks or throws mid-iteration drives the engine's
   *  thinking/abort/error stream branches that mock SSE transports cannot reach.
   *  Typed as `unknown` (cast at the common/engine.ts call site) to avoid a
   *  types.ts -> provider.ts import cycle. */
  _streamChatImpl?: unknown;

  // -------------------------------------------------------------------------
  // Parity config knobs (Claude Code settings.json / CLI flags). All optional;
  // each is consumed by its owning subsystem (prompt assembly, permissions,
  // memory, MCP, sandbox, UI). Added here as the single config surface so the
  // CLI resolver and `ami --introspect` thread them to the engine.
  // -------------------------------------------------------------------------
  /** Append text to the END of the assembled system prompt instead of replacing it
   *  (keeps the full default prompt). Parity: `--append-system-prompt`. */
  appendSystemPrompt?: string;
  /** Suppress writing the session transcript/state to disk for the whole run
   *  (ephemeral session). Parity: `sessionPersistenceDisabled` / `--no-save`. */
  sessionPersistenceDisabled?: boolean;
  /** Composer editing mode: modal 'vim' (Esc toggles normal/insert) or 'normal'
   *  (default). Parity: `editorMode`. */
  editorMode?: 'vim' | 'normal';
  /** Render full (untruncated) long API-error text + extra diagnostics. Parity: `verbose`. */
  verbose?: boolean;
  /** Separate, typically-stronger reviewer/advisor model that reviews the conversation
   *  mid-session. Parity: `advisorModel`. */
  advisorModel?: string;
  /** Natural-language auto-mode (YOLO) rule configuration. Parity: `settings.autoMode`. */
  autoMode?: AutoModeConfig;
  /** Default install/update channel. Parity: `autoUpdatesChannel`. */
  autoUpdatesChannel?: 'latest' | 'stable';
  /** Remembered responses for env-detected API keys (truncated), so a detected key is
   *  prompted once then trusted/rejected. Parity: `customApiKeyResponses`. */
  customApiKeyResponses?: { approved: string[]; rejected: string[] };
  /** Minutes-away threshold after which an idle-return prompt is shown. Parity: IdleReturnDialog. */
  idleReturnPromptMinutes?: number;
  /** Enable automatic session-memory extraction/consolidation (default true). Parity:
   *  `isAutoMemoryEnabled` / `autoDreamEnabled`. */
  autoMemoryEnabled?: boolean;
  /** Run session-memory extraction only every N eligible turns. Parity: extraction throttle. */
  sessionMemoryInterval?: number;
  /** Master switch for time-based idle microcompact (default off). Parity: timeBasedMCConfig.enabled. */
  idleMicrocompactEnabled?: boolean;
  /** Idle-gap threshold (minutes) before microcompact fires, tuned against the server
   *  prompt-cache TTL. Parity: `gapThresholdMinutes`. */
  idleMicrocompactThresholdMinutes?: number;
  /** Enable file checkpointing/history (default true); false disables snapshots.
   *  Parity: `fileCheckpointingEnabled`. */
  fileCheckpointingEnabled?: boolean;
  /** Ignore all on-disk/discovered MCP configs and use ONLY explicitly-passed servers.
   *  Parity: `--strict-mcp-config`. */
  strictMcpConfig?: boolean;
  /** Persisted per-server disable list (opt-out without removing). Parity: `disabledMcpServers`. */
  disabledMcpServers?: string[];
  /** Persisted per-server enable list for default-disabled servers. Parity: `enabledMcpServers`. */
  enabledMcpServers?: string[];
  /** Cap on output tokens for this run/fork (sizes the request `max_tokens`). Parity:
   *  `maxOutputTokens`. */
  maxOutputTokens?: number;
  /** Show rotating capability tips (default true). Parity: `spinnerTipsEnabled`. */
  spinnerTipsEnabled?: boolean;
  /** Disable fenced-code syntax highlighting while keeping UI color. Parity:
   *  `syntaxHighlightingDisabled`. */
  syntaxHighlightingDisabled?: boolean;
  /** Reduce/disable animations for accessibility (persistent, vs env-only). Parity:
   *  `prefersReducedMotion`. */
  prefersReducedMotion?: boolean;
  /** Enable AI-generated inline prompt suggestions. Parity: `promptSuggestionEnabled`. */
  promptSuggestionEnabled?: boolean;
  /** Retention window (days) for stale worktrees/files cleanup. Parity: `cleanupPeriodDays`. */
  cleanupPeriodDays?: number;
  /** Enable the LLM-generated permission explainer (default true). Parity:
   *  `permissionExplainerEnabled`. */
  permissionExplainerEnabled?: boolean;
  /** Directory (relative to project root) where plan .md files are written. Parity:
   *  `plansDirectory`. */
  plansDirectory?: string;
  /** Capture a heap dump when the RSS memory-warning threshold is crossed (not just a
   *  warning). Reuses {@link memoryWarningRssBytes} as the trigger level. Parity:
   *  heapDumpService `auto-1.5GB`. */
  heapDumpOnMemoryWarning?: boolean;
  /** Apply /model, /thinking, /fast immediately to an in-flight turn rather than
   *  deferring to turn end. Parity: `shouldInferenceConfigCommandBeImmediate`. */
  immediateInferenceConfig?: boolean;
  /** Backend that runs teammates/subagents; 'auto' resolves against the live
   *  environment. Parity: `teammateMode`. */
  teammateMode?: 'auto' | 'tmux' | 'in-process';
  /** Persisted default model for spawned teammates/subagents, consulted only when a
   *  spawn names no model from a higher-priority source. Tri-state (parity:
   *  `teammateDefaultModel` / getDefaultTeammateModel): a model id/alias string forces
   *  that default; `null` means "Default → follow the leader" (keep the parent's exact
   *  model); omitted falls through to the provider-aware hardcoded fallback. */
  subagentDefaultModel?: string | null;
  /** Map of canonical model id → provider-specific model string (e.g. a Bedrock
   *  inference-profile ARN), layered on top of the resolved model. Parity: `modelOverrides`. */
  modelOverrides?: Record<string, string>;
  /** Terminal notification channel for turn-completion alerts. Parity: NOTIFICATION_CHANNELS. */
  notificationChannel?: NotificationChannel;
  /** Plugin ids never suggested for LSP recommendations. Parity: `lspRecommendationNeverPlugins`. */
  lspRecommendationNeverPlugins?: string[];
  /** Hard off-switch for LSP-plugin recommendations. Parity: `lspRecommendationDisabled`. */
  lspRecommendationDisabled?: boolean;
  /** Running count of LSP recommendations the user has ignored. Once it reaches
   *  {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT}, recommendations auto-disable
   *  (see {@link isLspRecommendationDisabled}). Parity: `lspRecommendationIgnoredCount`
   *  (utils/plugins/lspRecommendation.ts incrementIgnoredCount/isLspRecommendationsDisabled). */
  lspRecommendationIgnoredCount?: number;
  /** Skip the code-block picker on `/copy` and always copy the full response. Parity:
   *  `copyFullResponse`. */
  copyFullResponse?: boolean;
  /** Execute tool calls as they stream in during the assistant message (vs waiting for the
   *  whole message). Runtime gate. Parity: `streamingToolExecution` (query/config.ts). */
  streamingToolExecution?: boolean;
  /** Show a one-line preview of each teammate's latest message in the teammate spinner tree.
   *  Parity: `showTeammateMessagePreview`. */
  showTeammateMessagePreview?: boolean;
  /** Opt-in consent flag that suppresses the auto-mode (YOLO) permission-opt-in dialog on
   *  subsequent runs once the user has accepted it. Parity: `skipAutoPermissionPrompt`
   *  (components/AutoModeOptInDialog.tsx). */
  skipAutoPermissionPrompt?: boolean;
}

// ---------------------------------------------------------------------------
// LSP-plugin recommendation gating (parity: utils/plugins/lspRecommendation.ts)
// ---------------------------------------------------------------------------

/** After this many ignored LSP recommendations, suggestions auto-disable.
 *  Parity: lspRecommendation.ts `MAX_IGNORED_COUNT = 5`. */
export const MAX_LSP_RECOMMENDATION_IGNORED_COUNT = 5;

/**
 * Whether LSP-plugin recommendations are currently suppressed for a config.
 * Disabled when the user explicitly turned them off, OR they have ignored at
 * least {@link MAX_LSP_RECOMMENDATION_IGNORED_COUNT} recommendations. Pure —
 * mirrors Claude's `isLspRecommendationsDisabled()`.
 */
export function isLspRecommendationDisabled(
  config: Pick<EngineConfig, 'lspRecommendationDisabled' | 'lspRecommendationIgnoredCount'>,
): boolean {
  return (
    config.lspRecommendationDisabled === true ||
    (config.lspRecommendationIgnoredCount ?? 0) >= MAX_LSP_RECOMMENDATION_IGNORED_COUNT
  );
}

// ---------------------------------------------------------------------------
// Inline prompt-suggestion default resolution (parity:
// services/PromptSuggestion/promptSuggestion.ts `shouldEnablePromptSuggestion`)
// ---------------------------------------------------------------------------

/** Resolved inputs for {@link shouldEnablePromptSuggestion}. The caller resolves
 *  the env override (via the shared env truthy/falsy helpers) and interactivity
 *  so this stays a pure, dependency-free decision function. */
export interface PromptSuggestionResolution {
  /** Persisted/config toggle ({@link EngineConfig.promptSuggestionEnabled}).
   *  Defaults to ON when omitted. */
  configEnabled?: boolean;
  /** A set env override: `true`/`false` force the outcome; `undefined` defers to
   *  the config toggle. Parity: `CLAUDE_CODE_ENABLE_PROMPT_SUGGESTION`. */
  envOverride?: boolean;
  /** Print/piped/SDK sessions never get inline suggestions. Parity:
   *  `getIsNonInteractiveSession()`. */
  nonInteractive?: boolean;
}

/**
 * Decide whether AI-generated inline prompt suggestions are enabled, matching
 * Claude's precedence: an explicit env override wins; otherwise non-interactive
 * sessions are always off; otherwise the config toggle decides (default ON).
 */
export function shouldEnablePromptSuggestion(resolution: PromptSuggestionResolution = {}): boolean {
  if (resolution.envOverride !== undefined) return resolution.envOverride;
  if (resolution.nonInteractive) return false;
  return resolution.configEnabled ?? true;
}

// ---------------------------------------------------------------------------
// DI subsystem interfaces — typed groups of related EngineConfig fields
// ---------------------------------------------------------------------------

export interface ProviderSubsystem {
  primary: ProviderConfig;
  fallbackModel?: string;
  /** Ordered fallback chain (normalized from fallbackModels + fallbackModel, deduped, capped). */
  fallbackModels?: string[];
  compactionModel?: string;
  /** Architect model for dual-model (plan + execute) mode. */
  architectModel?: string;
  thinking?: ThinkingConfig;
}

export interface PermissionSubsystem {
  mode: string;
  rules: Array<{ tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' }>;
  promptHandler?: import('./permissions').PermissionPromptHandler;
}

export interface SessionSubsystem {
  sessionId?: string;
  sessionDir?: string;
  tokenBudget: number;
  maxTurns: number;
  maxSteps?: number;
}

export function buildSubsystems(config: EngineConfig): {
  provider: ProviderSubsystem;
  permissions: PermissionSubsystem;
  session: SessionSubsystem;
} {
  return {
    provider: {
      primary: config.provider,
      fallbackModel: config.fallbackModel,
      fallbackModels: (() => {
        const chain = [...(config.fallbackModels ?? []), ...(config.fallbackModel ? [config.fallbackModel] : [])]
          .filter((m): m is string => !!m);
        return [...new Set(chain)].slice(0, 3);
      })(),
      compactionModel: config.compactionModel,
      architectModel: config.architectModel,
      thinking: config.thinking,
    },
    permissions: {
      mode: config.permissionMode ?? 'ask',
      rules: config.permissionRules ?? [],
      promptHandler: config.permissionPromptHandler,
    },
    session: {
      sessionId: config.sessionId,
      sessionDir: config.sessionDir,
      tokenBudget: config.tokenBudget ?? 100_000,
      maxTurns: (config.maxTurns && config.maxTurns > 0 && Number.isFinite(config.maxTurns))
        ? config.maxTurns
        : 100,
      maxSteps: config.maxSteps,
    },
  };
}

export interface EngineSubmitOptions {
  systemPromptOverride?: string;
  /** Text appended to the END of the active system prompt (default / override / agent)
   *  for this turn, preserving the full prompt rather than replacing it. Parity:
   *  Claude's per-call `appendSystemPrompt`. */
  appendSystemPrompt?: string;
  contextFiles?: ContextFile[];
  /** JSON Schema for structured output. When provided, the model output is validated against this schema. */
  outputSchema?: Record<string, unknown>;
  /** Max retries for schema validation (default 3). */
  maxStructuredRetries?: number;
  /**
   * Image attachments for this user turn (e.g. drag-dropped or path-referenced
   * files). Attached to the user message as multimodal content when the active
   * model supports vision; otherwise ignored with a note.
   */
  images?: ImageContent[];
}

export interface ContextFile {
  path: string;
  content: string;
  language?: string;
}

// ---------------------------------------------------------------------------
// EngineConfig validation & sanitization
// ---------------------------------------------------------------------------

/** Canonical bounded-integer/number knob validator. Pushes a warning and returns
 *  `false` (invalid) when `value` is out of the hard [min,max] range or (when
 *  `integer`) non-integer; pushes a soft advisory (still valid) past `softMax`.
 *  Single source for every numeric EngineConfig/sandbox knob check so the range
 *  logic is never re-implemented per site (dedup). */
function validateNumericKnob(
  value: number,
  name: string,
  opts: { min: number; max?: number; softMax?: number; integer?: boolean },
  warnings: string[],
): boolean {
  if (opts.integer && !Number.isInteger(value)) {
    warnings.push(`${name} must be an integer`);
    return false;
  }
  if (!Number.isFinite(value) || value < opts.min) {
    warnings.push(`${name} must be >= ${opts.min}`);
    return false;
  }
  if (opts.max !== undefined && value > opts.max) {
    warnings.push(`${name} must be <= ${opts.max}`);
    return false;
  }
  if (opts.softMax !== undefined && value > opts.softMax) {
    warnings.push(`${name} exceeds ${opts.softMax} — likely misconfigured`);
  }
  return true;
}

/** True when `n` is a usable TCP port (integer in 1..65535). */
function isValidPort(n: number): boolean {
  return Number.isInteger(n) && n >= 1 && n <= 65535;
}

/** Validate a {@link SandboxPolicy}: mode enum, proxy ports, host/path list
 *  shapes, and cross-field sanity (fail-closed only meaningful when enabled).
 *  Pushes warnings and returns `false` on a hard error. Reached by
 *  {@link validateEngineConfig}. */
function validateSandboxPolicy(sandbox: SandboxPolicy, warnings: string[]): boolean {
  let valid = true;
  if (sandbox.mode !== 'off' && sandbox.mode !== 'container') {
    warnings.push(`sandbox.mode must be 'off' or 'container'`);
    valid = false;
  }
  const net = sandbox.network;
  if (net) {
    for (const port of [['httpProxyPort', net.httpProxyPort], ['socksProxyPort', net.socksProxyPort]] as const) {
      if (port[1] !== undefined && !isValidPort(port[1])) {
        warnings.push(`sandbox.network.${port[0]} must be a port in 1-65535`);
        valid = false;
      }
    }
    for (const [field, list] of [['allowedHosts', net.allowedHosts], ['deniedHosts', net.deniedHosts], ['managedAllowedHosts', net.managedAllowedHosts]] as const) {
      if (list && list.some((h) => typeof h !== 'string' || h.trim() === '')) {
        warnings.push(`sandbox.network.${field} entries must be non-empty strings`);
        valid = false;
      }
    }
  }
  const fs = sandbox.filesystem;
  if (fs) {
    for (const [field, list] of [['allowWrite', fs.allowWrite], ['denyWrite', fs.denyWrite], ['denyRead', fs.denyRead], ['allowRead', fs.allowRead], ['managedAllowRead', fs.managedAllowRead]] as const) {
      if (list && list.some((p) => typeof p !== 'string' || p.trim() === '')) {
        warnings.push(`sandbox.filesystem.${field} entries must be non-empty paths`);
        valid = false;
      }
    }
  }
  if (sandbox.seccomp?.profilePath !== undefined) {
    if (typeof sandbox.seccomp.profilePath !== 'string' || sandbox.seccomp.profilePath.trim() === '') {
      warnings.push('sandbox.seccomp.profilePath must be a non-empty path');
      valid = false;
    }
  }
  if (sandbox.failIfUnavailable && sandbox.mode === 'off') {
    warnings.push('sandbox.failIfUnavailable has no effect when sandbox.mode is off');
  }
  if (sandbox.ripgrep !== undefined) {
    if (typeof sandbox.ripgrep.command !== 'string' || sandbox.ripgrep.command.trim() === '') {
      warnings.push('sandbox.ripgrep.command must be a non-empty string');
      valid = false;
    }
    if (sandbox.ripgrep.args && sandbox.ripgrep.args.some((a) => typeof a !== 'string' || a.trim() === '')) {
      warnings.push('sandbox.ripgrep.args entries must be non-empty strings');
      valid = false;
    }
  }
  return valid;
}

export function validateEngineConfig(config: Partial<EngineConfig>): { valid: boolean; warnings: string[] } {
  const warnings: string[] = [];
  let valid = true;

  if (config.maxTurns !== undefined) {
    if (!validateNumericKnob(config.maxTurns, 'maxTurns', { min: 1, softMax: 10000, integer: true }, warnings)) valid = false;
  }
  if (config.maxSessionMs !== undefined) {
    if (config.maxSessionMs <= 0) { warnings.push('maxSessionMs must be positive'); valid = false; }
    else if (config.maxSessionMs > 86_400_000) { warnings.push('maxSessionMs exceeds 24 hours'); valid = false; }
    else if (config.maxSessionMs > 3_600_000) { warnings.push('maxSessionMs exceeds 1 hour — are you sure?'); }
  }
  if (config.maxBudgetUsd !== undefined) {
    if (config.maxBudgetUsd <= 0) { warnings.push('maxBudgetUsd must be positive'); valid = false; }
    else if (config.maxBudgetUsd > 1000) { warnings.push('maxBudgetUsd exceeds $1000'); valid = false; }
    else if (config.maxBudgetUsd > 100) { warnings.push('maxBudgetUsd exceeds $100 — are you sure?'); }
  }
  if (config.maxAgentDepth !== undefined) {
    if (!Number.isInteger(config.maxAgentDepth) || config.maxAgentDepth < 1 || config.maxAgentDepth > 20) {
      warnings.push('maxAgentDepth must be 1-20');
      valid = false;
    } else if (config.maxAgentDepth > 10) {
      warnings.push('maxAgentDepth > 10 — deep nesting may be expensive');
    }
  }
  if (config.maxUsageLimitWaitMs !== undefined && config.maxUsageLimitWaitMs < 0) {
    warnings.push('maxUsageLimitWaitMs must be non-negative');
    valid = false;
  }
  if (config.requestTimeoutMs !== undefined) {
    if (config.requestTimeoutMs < 1000 || config.requestTimeoutMs > 600_000) {
      warnings.push('requestTimeoutMs must be 1000-600000');
      valid = false;
    } else if (config.requestTimeoutMs > 300_000) {
      warnings.push('requestTimeoutMs > 5min may cause timeouts elsewhere');
    }
  }
  if (config.tokenBudget !== undefined && config.tokenBudget <= 0) {
    warnings.push('tokenBudget must be positive');
    valid = false;
  }
  if (config.maxWebSearchesPerSession !== undefined) {
    if (!validateNumericKnob(config.maxWebSearchesPerSession, 'maxWebSearchesPerSession', { min: 1, softMax: 10_000, integer: true }, warnings)) valid = false;
  }
  if (config.bashOutputMaxChars !== undefined) {
    if (!validateNumericKnob(config.bashOutputMaxChars, 'bashOutputMaxChars', { min: 1, softMax: 10_000_000, integer: true }, warnings)) valid = false;
  }
  if (config.taskOutputMaxChars !== undefined) {
    if (!validateNumericKnob(config.taskOutputMaxChars, 'taskOutputMaxChars', { min: 1, softMax: 10_000_000, integer: true }, warnings)) valid = false;
  }
  if (config.lspRecommendationIgnoredCount !== undefined) {
    if (!validateNumericKnob(config.lspRecommendationIgnoredCount, 'lspRecommendationIgnoredCount', { min: 0, integer: true }, warnings)) valid = false;
  }
  if (config.sandbox !== undefined) {
    if (!validateSandboxPolicy(config.sandbox, warnings)) valid = false;
  }

  return { valid, warnings };
}

export function sanitizeEngineConfig(config: Partial<EngineConfig>): Partial<EngineConfig> {
  const out = { ...config };
  if (out.maxTurns !== undefined) out.maxTurns = Math.max(1, Math.min(10000, Math.round(out.maxTurns)));
  if (out.maxSessionMs !== undefined) out.maxSessionMs = Math.max(1, Math.min(86_400_000, out.maxSessionMs));
  if (out.maxBudgetUsd !== undefined) out.maxBudgetUsd = Math.max(0.01, Math.min(1000, out.maxBudgetUsd));
  if (out.maxAgentDepth !== undefined) out.maxAgentDepth = Math.max(1, Math.min(20, Math.round(out.maxAgentDepth)));
  if (out.maxUsageLimitWaitMs !== undefined) out.maxUsageLimitWaitMs = Math.max(0, out.maxUsageLimitWaitMs);
  if (out.requestTimeoutMs !== undefined) out.requestTimeoutMs = Math.max(1000, Math.min(600_000, out.requestTimeoutMs));
  if (out.tokenBudget !== undefined) out.tokenBudget = Math.max(1, out.tokenBudget);
  if (out.maxWebSearchesPerSession !== undefined) out.maxWebSearchesPerSession = Math.max(1, Math.round(out.maxWebSearchesPerSession));
  if (out.bashOutputMaxChars !== undefined) out.bashOutputMaxChars = Math.max(1, Math.round(out.bashOutputMaxChars));
  if (out.taskOutputMaxChars !== undefined) out.taskOutputMaxChars = Math.max(1, Math.round(out.taskOutputMaxChars));
  if (out.lspRecommendationIgnoredCount !== undefined) out.lspRecommendationIgnoredCount = Math.max(0, Math.round(out.lspRecommendationIgnoredCount));
  if (out.sandbox !== undefined) out.sandbox = sanitizeSandboxPolicy(out.sandbox);
  return out;
}

/** Drop invalid proxy ports and normalize host/path lists (trim, drop empties,
 *  dedupe). Returns a new policy; never mutates the input. */
function sanitizeSandboxPolicy(sandbox: SandboxPolicy): SandboxPolicy {
  const cleanList = (list?: string[]): string[] | undefined => {
    if (!list) return undefined;
    const cleaned = [...new Set(list.map((s) => (typeof s === 'string' ? s.trim() : '')).filter((s) => s !== ''))];
    return cleaned.length ? cleaned : undefined;
  };
  const out: SandboxPolicy = { ...sandbox };
  if (sandbox.network) {
    const net = { ...sandbox.network };
    if (net.httpProxyPort !== undefined && !isValidPort(net.httpProxyPort)) delete net.httpProxyPort;
    if (net.socksProxyPort !== undefined && !isValidPort(net.socksProxyPort)) delete net.socksProxyPort;
    net.allowedHosts = cleanList(net.allowedHosts);
    net.deniedHosts = cleanList(net.deniedHosts);
    net.allowUnixSockets = cleanList(net.allowUnixSockets);
    net.managedAllowedHosts = cleanList(net.managedAllowedHosts);
    out.network = net;
  }
  if (sandbox.filesystem) {
    const fsc = { ...sandbox.filesystem };
    fsc.allowWrite = cleanList(fsc.allowWrite);
    fsc.denyWrite = cleanList(fsc.denyWrite);
    fsc.denyRead = cleanList(fsc.denyRead);
    fsc.allowRead = cleanList(fsc.allowRead);
    fsc.managedAllowRead = cleanList(fsc.managedAllowRead);
    out.filesystem = fsc;
  }
  if (sandbox.seccomp) {
    const profilePath = typeof sandbox.seccomp.profilePath === 'string' ? sandbox.seccomp.profilePath.trim() : '';
    out.seccomp = profilePath ? { profilePath } : {};
  }
  if (sandbox.enabledPlatforms) out.enabledPlatforms = cleanList(sandbox.enabledPlatforms);
  if (sandbox.ripgrep) {
    const command = typeof sandbox.ripgrep.command === 'string' ? sandbox.ripgrep.command.trim() : '';
    if (command) {
      const args = cleanList(sandbox.ripgrep.args);
      out.ripgrep = args ? { command, args } : { command };
    } else {
      delete out.ripgrep;
    }
  }
  return out;
}
