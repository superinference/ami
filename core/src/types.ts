import { relative, isAbsolute } from 'node:path';
import type { SuperInferenceConfig } from './superinference/types';
import type { ToolCapabilityCeiling } from './tools/scoped-permissions';
import type { RateLimitTrackerOptions } from './rate-limiter';

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

/**
 * Detached/headless output formats accepted by `--output-format` (parity: Claude's
 * `OutputFormat`). `json` / `jsonl` / `text` are machine/human batch encodings;
 * `streamlined` is a distillation-resistant text mode that keeps the final response
 * text intact, collapses every tool call into a single cumulative tool-use summary
 * line, and drops thinking + system/init noise — so a downstream model (or operator)
 * reading the transcript gets the signal without the per-tool-call bulk (parity:
 * Claude's `streamlined` output-format value / `createStreamlinedTransformer`). The
 * valid set is derived from this const everywhere (config-resolver validation,
 * completions, settings-validation, `--introspect outputFormats`) so there is no
 * drift when a format is added.
 */
export const OUTPUT_FORMATS = ['json', 'jsonl', 'text', 'streamlined'] as const;
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
 * Human-readable labels for each {@link Theme} value — the descriptive text a
 * frontend shows beside the bare machine name in a theme picker/listing (parity:
 * Claude Code's `ThemePicker` option labels, e.g. `auto` → "Auto (match terminal)",
 * `dark-daltonized` → "Dark mode (colorblind-friendly)"). The machine value stays
 * the selection key; the label is purely descriptive, so the `/theme` listing can
 * tell a user what each option DOES ("match terminal", "colorblind-friendly",
 * "ANSI colors only") instead of exposing only the terse id. Keyed by every member
 * of {@link THEMES} so the map and the enum can never drift.
 */
export const THEME_LABELS: Record<Theme, string> = {
  'default': 'Default (match terminal)',
  'auto': 'Auto (match terminal)',
  'dark': 'Dark mode',
  'light': 'Light mode',
  'dark-daltonized': 'Dark mode (colorblind-friendly)',
  'light-daltonized': 'Light mode (colorblind-friendly)',
  'dark-ansi': 'Dark mode (ANSI colors only)',
  'light-ansi': 'Light mode (ANSI colors only)',
};

/**
 * The descriptive label for a theme name. A recognized {@link Theme} resolves to
 * its {@link THEME_LABELS} entry; any unknown string passes through unchanged so a
 * caller can label an arbitrary/custom value without a lookup miss.
 */
export function describeTheme(theme: string): string {
  return (THEME_LABELS as Record<string, string>)[theme] ?? theme;
}

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

/**
 * Closed allowlist of image media types the provider vision APIs accept (parity:
 * Claude's imageResizer sniff targets — PNG/JPEG/GIF/WebP). A declared type outside
 * this set (e.g. the WSL-clipboard `image/bmp`, or a spoofed `image/svg+xml`) must be
 * rejected or normalized before it reaches the provider. Single source of truth for
 * {@link isSupportedImageMediaType} and the mint-boundary normalization. */
export const IMAGE_MEDIA_TYPES = ['image/png', 'image/jpeg', 'image/gif', 'image/webp'] as const;
export type ImageMediaType = (typeof IMAGE_MEDIA_TYPES)[number];

/** True when `mt` is one of the provider-accepted {@link IMAGE_MEDIA_TYPES}. Used at
 *  the image-mint boundary to reject/normalize a declared media type that no vision
 *  API will accept. */
export function isSupportedImageMediaType(mt: string | undefined | null): mt is ImageMediaType {
  return typeof mt === 'string' && (IMAGE_MEDIA_TYPES as readonly string[]).includes(mt);
}

export interface ImageContent {
  type: 'image';
  source: {
    type: 'base64';
    media_type: string;
    data: string;
  };
}

/**
 * A document (e.g. PDF) carried natively in a user message so the model reads it
 * directly rather than through a base64 text blob. Mirrors Claude's third content
 * block `{ type: 'document', source: { type: 'base64', media_type: 'application/pdf',
 * data } }` emitted by the Read tool for PDFs (tools/FileReadTool.ts). Distinct from
 * {@link ImageContent}: a document is a whole-file attachment with its own media type
 * (`application/pdf`, …), serialized to the provider as an AI-SDK `file` part, and is
 * gated on {@link modelSupportsDocuments} rather than vision support. */
export interface DocumentContent {
  type: 'document';
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
  content: string | Array<TextContent | ImageContent | DocumentContent>;
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
  /** Marks a message that carries hook-produced additional context (a SessionStart/
   *  Setup `additionalContexts[]` aggregation — Claude's `hook_additional_context`
   *  attachment). By default such a message is injected for the live turn but
   *  FILTERED OUT of the saved transcript (so --resume re-derives fresh context by
   *  re-running the hooks); the operator can opt in to persist it across resume via
   *  `AMI_SAVE_HOOK_ADDITIONAL_CONTEXT` (Claude-compat
   *  `CLAUDE_CODE_SAVE_HOOK_ADDITIONAL_CONTEXT`). The persistence decision lives in
   *  the single `isLoggableMessage` save-filter (session.ts). */
  hookAdditionalContext?: boolean;
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
  /** Optional semantic subtype classifying WHY this system message was emitted, so a
   *  renderer can present a category distinctly (e.g. a proactive `'suggestion'` like
   *  "Skill \"x\" updated with improvements." rendered as a hint rather than a plain
   *  system line). Absent ⇒ a generic system message. Check with
   *  {@link isSuggestionMessage}. Parity: Claude's `createSystemMessage` subtype
   *  argument (`'suggestion'`). */
  kind?: SystemMessageKind;
}

/** Semantic subtype of a {@link SystemMessage}. `'suggestion'` tags a proactive,
 *  dismissible hint (e.g. a skill-improvement confirmation) so the UI can style it
 *  apart from ordinary system output. Parity: Claude's `createSystemMessage`
 *  message-subtype parameter. */
export type SystemMessageKind = 'suggestion';

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
   *  - `backgroundedByUser`: `true` when the model EXPLICITLY requested
   *    `run_in_background: true` (as opposed to an auto-move on timeout, which sets
   *    `assistantAutoBackgrounded`). Accompanied by `backgrounded: true` and the
   *    `taskId`. Parity: Claude's `ExecResult.backgroundedByUser`.
   *  - `outputFilePath` / `outputFileSize`: set when stdout overflowed the inline
   *    cap and the FULL output was persisted to disk — the absolute path and its
   *    byte size, so a programmatic caller reads them without re-parsing the spill
   *    banner. Parity: Claude's `ExecResult.outputFilePath` / `outputFileSize`.
   *  - `pluginHints`: `PluginRecommendationHint[]` — install suggestions parsed
   *    from an `<ami-hint type="plugin" .../>` tag a command emitted on stderr
   *    (set only when `AMI_PLUGIN_HINTS` is enabled; Claude's stderr-hint plugin
   *    recommendation).
   *  - `sedSimulatedEdit`: `true` when a `sed -i` in-place substitution was applied
   *    as an in-memory FILE EDIT (write of the `applySedSubstitution` result)
   *    instead of running the real `sed` binary, accompanied by `filePath`. This
   *    guarantees the written bytes equal the before → after diff the permission
   *    prompt previewed — parity with Claude Code's `_simulatedSedEdit` exactness
   *    guarantee (SedEditPermissionRequest).
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
   *    the diff-cost-capped success paths, where no per-line diff is computed).
   *
   *  Keys the lsp tool sets on a successful query (parity with Claude Code's LSPTool
   *  `formatResult` structured return), so a programmatic caller reads the hit count
   *  without re-parsing the formatted text:
   *  - `resultCount`: `number` — total hits for the operation. For a hierarchical
   *    `documentSymbol` result this is the RECURSIVE nested-symbol total (a class
   *    plus its methods), matching Claude's `countSymbols`; for other ops it is the
   *    count of valid locations/symbols/calls (1/0 for `hover`).
   *  - `fileCount`: `number` — number of DISTINCT files the hits span. */
  metadata?: Record<string, unknown>;
  /** Structured per-hunk diff for a file-mutating result, carried alongside the
   *  text `output` for programmatic rendering. Parity: Claude's `structuredPatch`. */
  structuredPatch?: DiffHunk[];
  /** The edit's per-hunk {@link structuredPatch} was deliberately OMITTED (left
   *  `[]`) because the file exceeded the diff-scan cap — the change happened but
   *  its location could not be rendered. Parity: Claude's readEditContext
   *  `truncated: true` signal, which lets a replay/introspection surface say
   *  "edit location not shown — file too large to scan" instead of mistaking an
   *  empty patch for "no change". Only set on a SUCCESSFUL, size-capped edit. */
  structuredPatchTruncated?: boolean;
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
  /** Emit-time ISO-8601 timestamp stamped when the tool actually RAN, on the
   *  originating process. Carried separately from the surrounding message so a
   *  resumed/replayed session can render a per-result timestamp. Deliberately
   *  OPTIONAL: pre-timestamp results replayed from an older transcript omit it and
   *  must still render — never assume it is present. Populate/read via
   *  {@link resolveSentAt}. Parity: Claude's BriefTool output `sentAt`. */
  sentAt?: string;
  /** The USER-facing payload, decoupled from the model-facing `output`. When a tool
   *  wants the model to receive only a terse receipt (e.g. "Message delivered to
   *  user.") while the human sees the full message/attachments, it sets the rich
   *  content here and a short receipt in `output`. Renderers show this to the user;
   *  the model context keeps `output`. Selected via {@link selectUserFacingOutput} /
   *  {@link selectModelFacingOutput}. Absent → both audiences see `output`. Parity:
   *  Claude's BriefTool `output.data` (UI) vs `mapToolResultToToolResultBlockParam`
   *  model receipt. */
  userFacingContent?: string;
  /** When true, a frontend MUST NOT render this tool result in the transcript — the
   *  tool declares its own output redundant because a higher-level lifecycle message
   *  already reports the same thing (e.g. a batched shutdown/cleanup summary covers a
   *  per-item cleanup result). The model STILL receives `output` in its context; only
   *  the user-facing render is suppressed, so this never hides information from the
   *  model. Absent / `false` ⇒ render normally. Honor it uniformly via
   *  {@link shouldRenderToolResult} rather than reading the flag directly. Parity:
   *  Claude's `renderToolResultMessage` returning `null` to suppress a redundant
   *  tool-result ("Suppress cleanup result — the batched shutdown message covers this",
   *  TeamDeleteTool/UI.tsx). */
  suppressRender?: boolean;
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
   *  prompt-driven modes (ask/acceptEdits) for commands protected by the sandbox.
   *  Two arms, both honored by {@link import('./permissions').PermissionManager}
   *  only when set to exactly `true` (default unset/false keeps the normal bash
   *  prompt): in CONTAINER mode (`mode === 'container'`) EVERY command is
   *  auto-allowed because the container — not the prompt — is the safety boundary
   *  (the host filesystem is not mounted); in HOST (non-container) mode only the
   *  commands that WILL actually be wrapped by the host (unshare) auto-sandbox are
   *  auto-allowed (per {@link import('./tools/bash-sandbox').commandWillBeHostSandboxed}),
   *  while sandbox-incompatible commands fall through to a regular permission prompt —
   *  Claude Code's "auto-allow" sandbox mode (try sandboxed, else regular
   *  permissions). Parity: enterprise `sandbox.autoAllowBashIfSandboxed`. Hardline
   *  blocks, `dangerouslyDisableSandbox`, explicit deny rules, and (in host mode)
   *  sensitive-path reads/writes still prompt. */
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
   *  `args` are extra flags prepended to every ripgrep invocation; `argv0` is the explicit
   *  argv[0] handed to the child (for a multi-call / busybox-style binary that dispatches on
   *  argv[0]==='rg' while `command` is the wrapper path). Consumed by
   *  {@link import('./tools/ripgrep-config').resolveRipgrepConfig}. Parity: Claude Code's
   *  sandbox `ripgrep: { command, args, argv0 }` setting. */
  ripgrep?: { command: string; args?: string[]; argv0?: string };
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
  /** Force the per-call permission check to run even when a hook / auto-approval path
   *  would otherwise bypass it (a forked/untrusted sub-agent context that must not
   *  inherit the parent's auto-grants). When `true`, callers MUST route through the
   *  normal permission flow regardless of any auto-approve verdict. Resolve the final
   *  decision with {@link shouldForcePermissionCheck}. Parity: Claude's forked-agent
   *  `requireCanUseTool` override (utils/forkedAgent.ts). */
  requireCanUseTool?: boolean;
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
  /** Forwarded {@link EngineConfig.maxSkillProgressMessages}: caps how many of a
   *  forked skill's nested tool-use steps the skill tool surfaces live to the parent
   *  stream (the rest are summarized as "+K more tool use"). Undefined / negative /
   *  non-finite → surface every nested event (AMI's full live-stream default). */
  _maxSkillProgressMessages?: number;
  _permissionPromptHandler?: import('./permissions').PermissionPromptHandler;
  _allowLocalhostForTesting?: boolean;
  _engineFactory?: (config: EngineConfig) => { submit(prompt: string): AsyncIterable<EngineEvent>; shutdown?(): void };
  _skillManager?: import('./skills').SkillManagerLike;
  _hookManager?: import('./hooks').HookManager;
  _mcpManager?: any;
  _parentMessages?: Message[];
  /**
   * Parent engine's ALREADY-RENDERED system prompt (byte-exact), threaded into a
   * `fork`-mode child so the child reuses the parent's cached prompt prefix VERBATIM
   * instead of re-deriving the stance from scratch. Re-rendering a fork child's
   * stance can diverge from the parent (e.g. a cold→warm feature flag, a differently
   * ordered skill block) and bust the provider prompt cache at the first differing
   * byte — so a cache-identical fork pays full input-token cost. Parity with Claude
   * Code's FORK_AGENT threading `toolUseContext.renderedSystemPrompt` instead of
   * calling `getSystemPrompt()` again (forkSubagent.ts:L44-L71). Only consumed by the
   * fork branch of the task tool; other modes keep their own authoritative prompt. */
  _parentSystemPrompt?: string;
  /**
   * The ORDERED names of the parent engine's exact tool array, threaded into a
   * `fork`-mode child so it inherits the parent's EXACT tool pool (same tools, same
   * order) rather than a freshly-rebuilt general pool. Rebuilding the pool under a
   * child's permission mode can reorder tools or change a tool-def's serialization,
   * which busts the prompt cache at the first differing tool. Parity with Claude
   * Code's `useExactTools` / `tools:['*']` fork inheritance (forkSubagent.ts:L44-L71,
   * resumeAgent.ts:L183-L190). The fork branch selects from the live tool registry in
   * this order; the universal subagent denylist/depth gate still applies afterwards. */
  _parentToolNames?: string[];
  _engineAddSystemReminder?: (msg: string) => void;
  /**
   * Activate (or, with `undefined`, clear) the inline skill named `name`. The
   * optional `scopedAllowRules` are the scoped command auto-allow rules the skill
   * contributes from its frontmatter `allowed-tools` (e.g. `Bash(git:*)` →
   * `{tool:'bash', pattern:'git:*', action:'allow'}`), computed via the canonical
   * {@link scopedToolAllowRules}. The engine applies them to the SESSION permission
   * context for as long as the inline skill stays active and removes them when it is
   * deactivated or replaced — parity with Claude Code's security-review command,
   * which injects a command's allowed-tools into `alwaysAllowRules.command` for the
   * duration of that command. Decoupled from `isStanceSkill`, so a plain inline
   * skill (or a skill-backed slash command) can pre-approve its own scoped commands
   * without being promoted to a pinned stance.
   */
  _engineSetActiveSkill?: (
    name: string | undefined,
    scopedAllowRules?: Array<{ tool: string; pattern?: string; action: 'allow' }>,
  ) => void;
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
  /** Test-only dependency seam for the `advisor` tool's reviewer stream. Never set
   *  in production (the tool falls through to the real streamChatCompletion); typed
   *  `unknown` and cast at the advisor call site to avoid a types.ts -> provider
   *  import coupling. */
  _advisorStreamImpl?: unknown;
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
  /** Whether the tool's effects reach OUTSIDE the local environment / are
   *  non-deterministic (a remote fetch, a web search, a message send, an
   *  external-API MCP tool) — the uniform SSRF/egress-surface marker. Distinct from
   *  {@link isReadOnly}: a tool can be read-only yet open-world (a remote fetch
   *  mutates nothing locally but still depends on live external state). Lets
   *  renderers tag a call (an `[open-world]` badge) and lets metadata/introspection
   *  surface that a result is non-cacheable / non-reproducible.
   *
   *  Either a plain `boolean` (the whole tool is open-world) OR an INPUT-AWARE
   *  predicate `(input) => boolean` so a single invocation can declare egress for
   *  THIS input only (e.g. a tool that reaches the network only for certain
   *  arguments). Absent / `false` = closed-world (fully local, deterministic).
   *  Resolve uniformly — never read this field directly — via
   *  {@link resolveToolOpenWorld}, which handles both forms and a throwing
   *  predicate. Parity: Claude's `Tool.isOpenWorld?(input)` (the reference consults
   *  it input-aware as `tool.isOpenWorld?.(input)`; MCPTool defaults false,
   *  overridden per-MCP-tool from the server's `annotations.openWorldHint`). */
  isOpenWorld?: boolean | ((input: Record<string, unknown>) => boolean);
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
  /** The wall-clock timeout budget (ms) this invocation will be killed at, resolved from
   *  the input (and any env/default knobs). Lets the executor surface a "N/budget elapsed"
   *  affordance on a long-running call so the user can see how close it is to its timeout
   *  boundary. Return `undefined` when the tool has no bounded budget. Parity: Claude's
   *  `timeoutMs` threaded through ShellProgressMessage / BashToolResultMessage
   *  (tools/BashTool/UI.tsx, tools/PowerShellTool/UI.tsx). */
  getProgressBudgetMs?: (input: Record<string, unknown>) => number | undefined;
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
  /** Per-tool opt-in to GROUPED rendering: when `true`, consecutive calls to this
   *  tool are collapsed into one summary row by a group-aware renderer instead of
   *  each call drawing its own header. Absent / `false` ⇒ render each call
   *  individually. Resolve with {@link shouldRenderGroupedToolUse}. Parity: Claude's
   *  per-tool `renderGroupedToolUse` opt-in (utils/groupToolUses.ts). */
  renderGroupedToolUse?: boolean;
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
  /**
   * Opt in to the NON-STREAMING request fallback. When a streaming attempt fails
   * in a way that signals a broken SSE transport — a gateway/proxy that returns a
   * 200 with a non-SSE body (the empty-stream sentinel) or a 404 on the streaming
   * endpoint — `streamChatCompletionWithRetry` makes ONE bounded, token-capped
   * non-streaming request (AI SDK `generateText`) and replays its result as the
   * normal stream, recovering a turn a proxy would otherwise have killed. Mirrors
   * Claude Code's executeNonStreamingRequest fallback (services/api/claude.ts).
   * Also enabled process-wide via SUPERINFERENCE_NONSTREAMING_FALLBACK /
   * CLAUDE_CODE_NONSTREAMING_FALLBACK. Off by default (streaming-only retry is
   * unchanged); an explicit falsy config value overrides a truthy env knob.
   */
  nonStreamingFallback?: boolean;
  /**
   * Provider-native structured output: constrain the model response to a JSON
   * Schema so the returned text is GUARANTEED schema-valid JSON instead of being
   * coaxed by a "respond with ONLY JSON" prompt and then parsed best-effort.
   * Threaded into the AI SDK `output: Output.object({ schema })` option, which
   * each provider maps to its native structured-output mechanism (OpenAI
   * `response_format: { type: 'json_schema' }`, Anthropic tool-schema). Set on
   * auxiliary/LLM-judge calls (e.g. the critic's {approved,score,reason} verdict)
   * so a formatting miss can never masquerade as a substantive negative verdict.
   * Omitted (free-form text) when unset. Mirrors Claude Code's `outputFormat:
   * { type: 'json_schema', schema }` on auxiliary queries (commands/rename/
   * generateSessionName.ts → services/api/claude.ts output_config.format).
   */
  responseFormat?: {
    type: 'json_schema';
    /** JSON Schema object the model output must satisfy. */
    schema: Record<string, unknown>;
    /** Optional schema name forwarded to providers that use it for LLM guidance. */
    name?: string;
  };
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
  'suggest_file_save', 'user_question', 'session_title', 'session_init', 'memory_warning',
  'plan_mode_changed', 'task_updated', 'skill_activated',
  'subagent_start', 'subagent_progress', 'subagent_result', 'todo_update',
  'mcp_resource_update', 'mcp_polling_update', 'mcp_connection_changed',
  'external_import_blocked', 'peer_message', 'background_task_complete',
  'hook_response', 'max_turns_reached', 'classifier_checking',
  'skill_improvement', 'done',
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
  /** Controls whether this option's DESCRIPTION column renders dimmed. Descriptions
   *  are dimmed by default (the secondary-context look); set `false` to keep a
   *  particular option's description at full brightness so a load-bearing note
   *  (e.g. a warning or the recommended choice's rationale) is not visually
   *  demoted. Parity: Claude's CustomSelect per-option `dimDescription` override
   *  (which defaults to dimmed but can be turned off per option). */
  dimDescription?: boolean;
}

/**
 * A per-question annotation the user attached to their AskUserQuestion answer,
 * relayed back to the model as structured context (parity: Claude's AskUserQuestion
 * per-option preview/notes annotation value). Keyed by question text in the
 * `user_question` {@link EngineEvent}'s `annotations` map. Typed (rather than an
 * opaque `unknown`) so the preview the user actually chose and any free-text notes
 * are discriminable by both the renderer and the model-facing result builder.
 */
export interface UserQuestionAnnotation {
  /** The preview content of the option the user selected, when the question used
   *  previews — lets the model see WHICH concrete artifact the user picked. */
  preview?: string;
  /** Free-text notes the user added alongside their selection. */
  notes?: string;
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

/**
 * The kind of live activity a relayed `subagent_progress` event describes. The
 * base kinds (`text`/`thinking`/`tool`) carry the sub-agent's streamed prose,
 * reasoning, and tool calls; `error`/`retry` surface failure + retry liveness on
 * the roster the moment they happen (parity: Claude Code's AgentTool relaying a
 * sub-agent's error/retry so the roster reflects it instead of appearing frozen).
 * `waiting` surfaces that the sub-agent is BLOCKED on an interactive
 * approval/question (a permission `ask`, an `AskUserQuestion`, or plan-mode
 * consent) so the roster shows "awaiting approval" instead of freezing on the
 * child's last tool/text row — parity: Claude Code's swarmWorkerHandler
 * `pendingWorkerRequest` "waiting for leader approval" affordance + the
 * AssistantToolUseMessage hook-progress "waiting" subtree.
 */
export type SubagentActivity = 'text' | 'thinking' | 'tool' | 'error' | 'retry' | 'waiting';

/**
 * Why a request is being retried, carried on the `retry_attempt` {@link EngineEvent}
 * so a frontend can render a connection/timeout/idle retry distinctly from an
 * HTTP-status retry (which has a real `statusCode`). Parity: Claude's withRetry cause
 * classification (services/api/withRetry.ts) distinguishing HTTP errors from
 * connection resets, request timeouts, and idle-stream aborts.
 *   - `http`               — the server returned a real HTTP status (429/500/503/529…).
 *   - `connection_reset`   — a connection-level network failure (ECONNRESET/EPIPE/…) with no HTTP response.
 *   - `connection_timeout` — the request hard-timeout fired before any response.
 *   - `stream_idle`        — the inactivity watchdog aborted a quiet-but-open stream.
 */
export type RetryErrorKind = 'http' | 'connection_reset' | 'connection_timeout' | 'stream_idle';

export type EngineEvent =
  | { type: 'text_delta'; text: string }
  | { type: 'thinking_delta'; text: string }
  | { type: 'tool_call_generating'; toolName: string; toolIndex: number; argsDelta: string }
  | { type: 'tool_use_start'; toolName: string; toolCallId: string; input: Record<string, unknown> }
  | { type: 'tool_use_progress'; toolCallId: string; data: string;
      /** Live wall-clock seconds the underlying command/task has been running, so a
       *  frontend can show a ticking "(1m3s · N lines)" affordance on a slow in-flight
       *  call without re-parsing `data`. Revealed only past the
       *  {@link MIN_SLOW_PROGRESS_REVEAL_SECONDS} threshold (see
       *  {@link shouldRevealSlowProgress}) so brief commands don't flicker a timer.
       *  Parity: Claude's bash_progress/powershell_progress `elapsedTimeSeconds`
       *  (components/messages/CollapsedReadSearchContent.tsx). Omitted when unmeasured. */
      elapsedSeconds?: number;
      /** Total output lines seen so far for the running command, for the live
       *  "· N lines" counter. Parity: Claude's shell-progress `totalLines`. Omitted
       *  when the tool does not count lines. */
      totalLines?: number }
  | { type: 'tool_use_result'; toolCallId: string; toolName: string; output: string; isError: boolean;
      /** The frontend should ABSORB this result from the transcript (render nothing)
       *  rather than echoing a confirmation row, while the model still receives the
       *  `output` in context. Set only for a routed / async-request `send_message`
       *  control send (see {@link shouldSuppressSendMessageTranscript}) — the parity
       *  of Claude Code's SendMessageTool renderToolResultMessage null-render branches
       *  (tools/SendMessageTool/UI.tsx). Absent/false → render normally. */
      suppressTranscript?: boolean;
      /** Per-hunk structured diff of a file-mutating result, forwarded from the
       *  tool's {@link ToolResult.structuredPatch} so a consumer (the session
       *  journal, a replay/introspection surface) can record WHERE an edit landed
       *  without re-parsing `output`. Absent for non-mutating tools. */
      structuredPatch?: DiffHunk[];
      /** The structured patch was omitted because the file exceeded the diff-scan
       *  cap (forwarded from {@link ToolResult.structuredPatchTruncated}) — the
       *  edit succeeded but its location could not be rendered. */
      structuredPatchTruncated?: boolean }
  | { type: 'turn_complete'; content: string | null; toolCalls: ToolCallContent[]; finishReason?: string }
  | { type: 'usage_update'; stats: UsageStats }
  | { type: 'error'; error: string }
  // `statusCode` is `null` for a connection-level failure that produced NO HTTP
  // response (timeout / network drop), distinct from a real HTTP status such as 429
  // or 503 (parity: Claude's SDKAPIRetryMessage `error_status: number | null`). It is
  // now OPTIONAL: a connection/timeout/idle retry may omit it entirely and carry only
  // `errorKind`, so a frontend never has to render a meaningless "HTTP null".
  // `errorKind` discriminates WHY the retry fired (parity: Claude's withRetry cause
  // classification) so connection/timeout/idle retries render distinctly from an
  // HTTP-status retry even when no status is present.
  | { type: 'retry_attempt'; attempt: number; maxRetries: number; delayMs: number; statusCode?: number | null; errorKind?: RetryErrorKind }
  // `retryAfterMs` (rate-limit category only): the server-directed wait the engine
  // derived from the `Retry-After` header / 429 body ("try again in Ns"), so a UI
  // can show WHEN the limit resets (parity: Claude's getLimitReachedText "· resets
  // <time>"), not just a bare "[RATE_LIMITED]". Absent when no hint was provided.
  | { type: 'error_classified'; category: string; retryable: boolean; willRetry: boolean; message?: string; retryAfterMs?: number }
  | { type: 'provider_changed'; provider: string; model: string; reason: string }
  | { type: 'checkpoint_created'; checkpointId: string; files: string[] }
  | { type: 'analytics_summary'; summary: Record<string, number> }
  | { type: 'superinference_state'; state: { value: number; entropy: number; eig: number; step: number; ppv: number }; stopReason: { type: string; detail: string } }
  | { type: 'suggest_file_save'; content: string; suggestedPath: string; lineCount: number }
  | { type: 'user_question'; toolCallId: string; question: string; options: UserQuestionOption[]; allowFreeText: boolean; header?: string; multiSelect?: boolean; answers?: string[]; annotations?: Record<string, UserQuestionAnnotation>; defaultIndex?: number;
      /** Dimmed secondary context shown beside the prompt title: a terse summary of
       *  the TOOL INPUT that triggered this question (e.g. the command/file a
       *  permission-style ask refers to), so the user sees WHAT is being asked about
       *  without it crowding the main `question`. `null`/empty are normalized to
       *  undefined by {@link sanitizeToolInputSummary}. Parity: Claude's PromptDialog
       *  `toolInputSummary` rendered in the `titleRight` slot. */
      toolInputSummary?: string | null }
  | { type: 'session_title'; title: string }
  /** A live per-session handshake snapshot emitted ONCE as the FIRST event of a
   *  fresh session (before the first turn runs), so a remote-control / SDK /
   *  detached consumer can obtain the session's resolved state in one message
   *  instead of reconstructing it from later deltas: the `sessionId`, launch
   *  `cwd`, active `model` + `permissionMode`, the resolved `tools` (names), the
   *  live `mcpServers` ([{name,status}]) and the running `version`. Emitted from a
   *  SINGLE engine site so the TUI and the detached/SDK runner receive an IDENTICAL
   *  shape (the TUI↔CLI parity invariant). Parity: Claude Code's `system/init`
   *  snapshot message (utils/messages/systemInit.ts `buildSystemInitMessage`). */
  | { type: 'session_init'; sessionId: string; cwd: string; model: string; permissionMode: string; tools: string[]; mcpServers: { name: string; status: string }[]; version: string }
  /** Progressive memory-pressure warning. `level` is the escalation tier the live
   *  RSS reading entered — `1` (high / soft warning), `2` (critical), `3` (severe —
   *  the warning×2 escalation ceiling OR the independent absolute CRITICAL ceiling).
   *  The tier lets a frontend surface DISTINCT severity wording ('High' vs 'Critical'
   *  vs 'Severe') and escalate the notification kind at the top tier, rather than a
   *  single flat "high memory" message. Parity: Claude Code's `useMemoryUsage`
   *  high/critical status drives distinct notification severity. Always `>= 1` because
   *  the event is only emitted when the tier rises above 0. */
  | { type: 'memory_warning'; level: 1 | 2 | 3; rssBytes: number; thresholdBytes: number; recoverySteps: string[] }
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
  | { type: 'subagent_progress'; nodeId: string; depth: number; activity: SubagentActivity; detail: string; usage?: TaskUsageSnapshot; lastToolName?: string;
      /** A concise mid-turn progress summary for the sub-agent's work so far (a forked
       *  summarizer phrase emitted periodically), distinct from the per-step `detail`
       *  activity line — lets a roster show "what this agent is accomplishing" rather
       *  than only its latest raw action. Parity: Claude's task progress `summary`
       *  field alongside `last_tool_name` (utils/task/sdkProgress.ts). Omitted until a
       *  summary has been computed. */
      summary?: string }
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
  /** A backgrounded (async) hook finished. The engine's `hookRewake` channel only
   *  surfaces a DENY/exit-2 or an error as a flattened `[hookRewake:…]` system string
   *  for the MODEL; this event is the USER/introspection-facing completion RECORD for
   *  EVERY async hook — including a clean success — classified by `outcome`
   *  (`'success'` | `'error'` | `'cancelled'`), with the command hook's `exitCode` and
   *  SEPARATELY-captured `stdout`/`stderr` rather than one merged blob, plus the
   *  installing plugin's provenance when the hook came from a bundle. Emitted once per
   *  async hook from a SINGLE engine site. Parity: Claude Code's
   *  `finalizeHook`/`emitHookResponse` structured async-hook response
   *  (utils/hooks/AsyncHookRegistry.ts). */
  | { type: 'hook_response'; event: string; outcome: 'success' | 'error' | 'cancelled'; exitCode?: number; stdout?: string; stderr?: string; pluginName?: string; pluginId?: string }
  /** The engine reached its per-submit turn cap (`maxTurns`) without the model
   *  signaling completion. This is a NORMAL, expected stop BOUNDARY — not a failure —
   *  so it is surfaced as its own informational event (carrying the configured
   *  `maxTurns` and the `turnCount` actually run) instead of a generic `error`, letting
   *  a frontend render the limit neutrally rather than as a stream/engine failure. The
   *  terminal `done` event still carries `exitReason: 'max_turns'` for exit-code
   *  mapping. Parity: Claude Code's `max_turns_reached` informational attachment
   *  (query.ts) — an expected boundary the UI renders neutrally, distinct from an error. */
  | { type: 'max_turns_reached'; maxTurns: number; turnCount: number }
  /** The autonomous-mode safety classifier is ACTIVELY evaluating an about-to-be
   *  auto-approved tool call (`checking:true`), or has just finished (`checking:false`).
   *  On the autonomous auto-approve path (`auto-allow` / `bypassPermissions` / screened
   *  `auto` / inherited plan-bypass) a model-backed classifier runs a real async query
   *  before a tool call is silently allowed (see
   *  {@link PermissionManager.setAutoModeSafetyClassifier}); until it resolves the user
   *  has no signal that an approval is not instantaneous/unchecked. This event brackets
   *  that async check so a frontend can surface an in-flight "safety check in progress"
   *  indicator and the engine's {@link ToolConfirmationService} can track the checking
   *  state (parity: Claude Code's `CLASSIFIER_CHECKING` set +
   *  `setClassifierChecking`/`clearClassifierChecking`/`isClassifierChecking`/
   *  `subscribeClassifierChecking` in `classifierApprovals.ts`). Emitted only when a
   *  classifier is installed and actually consulted for this call. */
  | { type: 'classifier_checking'; toolName: string; checking: boolean }
  /** The skill-improvement lifecycle surfaced a user/introspection-facing signal.
   *  AMI can write learned corrections back into a project `SKILL.md` (the `/skill
   *  improve` flow + `SkillImprovementSurvey` apply affordance); this event makes
   *  that otherwise-silent feature OBSERVABLE so the TUI, a diagnostic, and
   *  `ami --introspect` can see WHEN it fired — mirroring Claude Code's structured
   *  `skill_improvement_detected` signal (utils/hooks/skillImprovement.ts) and its
   *  survey appeared/responded lifecycle events (hooks/useSkillImprovementSurvey.ts),
   *  minus the vendor telemetry routing. `phase` discriminates the moment:
   *    - `'detected'` — the classifier proposed `updateCount` edits for `skillName`
   *      (the once-per-detection signal; `applied` omitted);
   *    - `'responded'` — the user acted on a proposal: `applied` is `true` when the
   *      SKILL.md was actually rewritten, `false` when the suggestion was dismissed.
   *  `updateCount` is the number of proposed edits (carried on both phases so a
   *  frontend can show "N improvements"). Emitted from a SINGLE engine site
   *  ({@link drainSkillImprovementNotices}) so the TUI and the detached/SDK runner
   *  receive an identical shape (the TUI↔CLI parity invariant). */
  | { type: 'skill_improvement'; phase: 'detected' | 'responded'; skillName: string; updateCount: number; applied?: boolean }
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
  /** Serialize the proposed tool call handed to the classifier as a compact JSONL dict
   *  line (`{"bash":"ls"}` for a tool call, the tool's input projected to its most
   *  salient value) instead of the default verbose human sentence. JSON escaping keeps
   *  hostile tool input from breaking out of its string context to forge another
   *  transcript line. Off (undefined/false) → the default descriptive rendering. Parity:
   *  Claude's `AutoModeConfig.jsonlTranscript` / `isJsonlTranscriptEnabled` →
   *  `toCompactBlock` ({@code {"Bash":"ls"}} vs {@code `Bash ls`}). */
  jsonlTranscript?: boolean;
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
  /**
   * Rate-limit tracker tuning, threaded to the {@link ProviderCoordinator}'s
   * {@link RateLimitTracker}: `warnThreshold` / `errorThreshold` utilization
   * fractions and the proactive `earlyWarningConfigs` ladder (an empty array
   * disables proactive "approaching limit" warnings). Undefined = provider
   * defaults ({@link DEFAULT_EARLY_WARNING_CONFIGS}).
   */
  rateLimit?: RateLimitTrackerOptions;
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
  /** Managed allowlist of selectable models; non-listed models are blocked.
   *
   *  UNSET-vs-EMPTY semantics (parity: Claude Code modelAllowlist.ts L103-108):
   *  - UNSET (`undefined`) ⇒ NO restriction — any model may be selected.
   *  - DEFINED but EMPTY (`[]`) ⇒ HARD LOCKDOWN — EVERY user-initiated model switch
   *    (`/model`, `/architect <model>`) is blocked; the admin has disallowed all
   *    user model selection. This is the opposite of unset and is honored by the
   *    user-switch gate (`isUserModelSwitchAllowed` / `isManagedModelLockdown`).
   *  - NON-EMPTY ⇒ only the listed family aliases / version prefixes / full ids are
   *    selectable (family-aware match via `isModelAllowed`).
   *  The non-user DEFAULT slots (startup model, fallback/compaction) deliberately
   *  stay lenient (empty ⇒ off) so they always resolve to some model. */
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
  /** The agent type/name this engine runs as when forked (the `subagent_type` or the
   *  built-in mode). Combined with {@link agentIsBuiltIn} to derive the agent
   *  query-source provenance tag (`agent:builtin:<type>` / `agent:default` /
   *  `agent:custom`) stamped on this engine's turn/api-call analytics events so local
   *  usage introspection can be segmented by agent category. Absent on the main thread.
   *  Parity: Claude's getQuerySourceForAgent (promptCategory.ts). */
  agentType?: string;
  /** Whether {@link agentType} names a BUILT-IN agent identity (vs a user-defined/custom
   *  one). Non-built-in collapses the provenance tag to `agent:custom` so a custom agent
   *  name never leaks into analytics. */
  agentIsBuiltIn?: boolean;
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
  /** @internal Override the auto-mode safety-classifier RUNNER (for testing). Lets a
   *  test drive the full engine→classifier→permission path with a deterministic
   *  verdict instead of a live model call. Defaults to the model-backed runner built
   *  from {@link autoMode} + the provider. Typed `unknown` (cast at the engine call
   *  site) to avoid a types.ts -> common import. Only consulted when {@link autoMode}
   *  is set. */
  _autoModeClassifierRunner?: unknown;

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
  /** Opt-in cap on how many of a forked skill's nested tool-use steps are surfaced
   *  LIVE to the parent stream. When set to a non-negative integer N, only the first
   *  N `tool_use_start` steps (and their progress/result rows) of a forked skill are
   *  forwarded as child events; further tool-use steps are suppressed from the live
   *  stream and summarized as a "+K more tool use" line appended to the skill's
   *  result. `0` surfaces the result only. Default (unset / negative / non-finite)
   *  keeps AMI's full live streaming — every nested event is surfaced (verbose).
   *  Parity: Claude Code's SkillTool `MAX_PROGRESS_MESSAGES_TO_SHOW` (=3) + `verbose`
   *  flag, adapted to AMI's live-stream model (opt-in cap; verbose is the default). */
  maxSkillProgressMessages?: number;
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
  /** Explicit column width for the inline diff preview shown in a file-edit permission
   *  prompt. When unset, the width is derived from the verbose flag by
   *  {@link resolveDiffPreviewWidth} ({@link VERBOSE_DIFF_PREVIEW_WIDTH} vs
   *  {@link COMPACT_DIFF_PREVIEW_WIDTH}). Parity: Claude's
   *  `NotebookEditPermissionRequest` `verbose ? 120 : 80` diff width. */
  diffPreviewWidth?: number;
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
// w88 parity primitives — pure, dependency-free building blocks for the typed
// schema additions above (event fields, ToolResult/ToolDefinition/SystemMessage
// fields). Each mirrors a specific Claude Code behavior; display/rendering stays
// in the cli layer, these only derive/validate data.
// ---------------------------------------------------------------------------

/** Minimum wall-clock seconds an in-flight command must have been running before a
 *  live "(elapsed · N lines)" progress suffix is revealed, so brief commands never
 *  flicker a timer. Parity: Claude's 2-second slow-command reveal threshold
 *  (components/messages/CollapsedReadSearchContent.tsx). */
export const MIN_SLOW_PROGRESS_REVEAL_SECONDS = 2;

/** Whether a running tool call has been slow enough to reveal its live elapsed/line
 *  progress affordance. `undefined`/unmeasured elapsed ⇒ not revealed. Pure; mirrors
 *  Claude's `elapsedTimeSeconds >= 2` gate for the ticking progress suffix. */
export function shouldRevealSlowProgress(
  elapsedSeconds: number | undefined,
  thresholdSeconds: number = MIN_SLOW_PROGRESS_REVEAL_SECONDS,
): boolean {
  return typeof elapsedSeconds === 'number' && elapsedSeconds >= thresholdSeconds;
}

/** Normalize a {@link EngineEvent}'s `toolInputSummary` (a `user_question` field): a
 *  `null`, `undefined`, or whitespace-only value collapses to `undefined` so a
 *  renderer can treat "no summary" uniformly; otherwise the trimmed text is returned.
 *  Parity: Claude's PromptDialog `toolInputSummary?: string | null` handling. */
export function sanitizeToolInputSummary(summary: string | null | undefined): string | undefined {
  if (summary == null) return undefined;
  const trimmed = summary.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/** Resolve a {@link ToolResult.sentAt} emit timestamp: keep an already-stamped value
 *  (so a resumed/replayed result shows its ORIGINAL time), otherwise stamp `nowIso`
 *  (defaulting to the current clock). This enforces the "optional for replay, stamped
 *  once at execution" invariant in one place. Parity: Claude's BriefTool
 *  `sentAt = new Date().toISOString()` captured at execution and preserved on resume. */
export function resolveSentAt(existing: string | undefined, nowIso: string = new Date().toISOString()): string {
  return existing ?? nowIso;
}

/** The MODEL-facing output for a {@link ToolResult}: always the terse `output`
 *  (the model never re-ingests a decoupled user-facing payload). Parity: Claude's
 *  `mapToolResultToToolResultBlockParam` returning only the receipt. */
export function selectModelFacingOutput(result: Pick<ToolResult, 'output'>): string {
  return result.output;
}

/** The USER-facing output for a {@link ToolResult}: the decoupled
 *  {@link ToolResult.userFacingContent} when set, else the plain `output`. Parity:
 *  Claude's BriefTool `output.data` (UI) vs the model receipt. */
export function selectUserFacingOutput(result: Pick<ToolResult, 'output' | 'userFacingContent'>): string {
  return result.userFacingContent ?? result.output;
}

/** Whether a tool opts into grouped (collapsed) rendering of consecutive calls.
 *  Absent field ⇒ `false` (render each call individually). Parity: Claude's per-tool
 *  `renderGroupedToolUse` opt-in. */
export function shouldRenderGroupedToolUse(tool: Pick<ToolDefinition, 'renderGroupedToolUse'>): boolean {
  return tool.renderGroupedToolUse === true;
}

/** Resolve a tool's open-world (SSRF/egress-surface) status for a SPECIFIC input,
 *  the single place {@link ToolDefinition.isOpenWorld} is consulted so both forms —
 *  a plain `boolean` and Claude's input-aware `isOpenWorld?(input)` predicate — are
 *  handled uniformly. A throwing predicate degrades to `false` (closed-world), never
 *  propagating, mirroring the reference's safe `tool.isOpenWorld?.(input) || false`
 *  consult (cli/print.ts, MCPToolDetailView). Absent field ⇒ `false`.
 *  `input` defaults to `{}` for a tool-level query (the badge/introspection case,
 *  parity with Claude's `tool.isOpenWorld?.({})`). */
export function resolveToolOpenWorld(
  tool: Pick<ToolDefinition, 'isOpenWorld'>,
  input: Record<string, unknown> = {},
): boolean {
  const flag = tool.isOpenWorld;
  if (typeof flag === 'function') {
    try {
      return flag(input) === true;
    } catch {
      return false;
    }
  }
  return flag === true;
}

/** Whether a {@link SystemMessage} is a proactive `'suggestion'` (e.g. a
 *  skill-improvement confirmation), so a renderer can style it apart from ordinary
 *  system output. Parity: Claude's `'suggestion'` message subtype. */
export function isSuggestionMessage(message: Pick<SystemMessage, 'kind'>): boolean {
  return message.kind === 'suggestion';
}

/** Final permission-check decision for a tool call given an upstream auto-approval
 *  verdict and a {@link ToolContext.requireCanUseTool} override: the override FORCES a
 *  real check (returns `true`) even when `autoApproved` is `true`; otherwise an
 *  auto-approval skips the check (returns `false`). Parity: Claude's forked-agent
 *  `requireCanUseTool` that re-asserts the permission gate past hook auto-approval. */
export function shouldForcePermissionCheck(opts: {
  requireCanUseTool?: boolean;
  autoApproved?: boolean;
}): boolean {
  if (opts.requireCanUseTool === true) return true;
  return opts.autoApproved !== true;
}

/** Persisted metadata attached to a conversation-summary (compaction) marker so it is
 *  re-rendered inline in history: how many messages were summarized, the direction of
 *  the summary, and the optional user focus/context note. Validate raw/persisted input
 *  with {@link validateSummarizeMetadata}. Parity: Claude's `summarizeMetadata`
 *  ({messagesSummarized, direction, userContext}) on the summary user message. */
export interface SummarizeMetadata {
  /** Number of prior messages folded into the summary. */
  messagesSummarized: number;
  /** Which end of the transcript was summarized. Parity: Claude's `direction`. */
  direction?: 'before' | 'after';
  /** Optional free-text focus/context the user supplied to steer the summary
   *  (re-rendered as `Context: "…"`). */
  userContext?: string;
}

/** Validate + normalize a raw/persisted {@link SummarizeMetadata} object (e.g. loaded
 *  from a resumed session). Returns a clean metadata object, or `null` when the shape
 *  is unusable (missing/negative/non-integer count). Pure guard — no display. */
export function validateSummarizeMetadata(raw: unknown): SummarizeMetadata | null {
  if (typeof raw !== 'object' || raw === null) return null;
  const o = raw as Record<string, unknown>;
  const count = o.messagesSummarized;
  if (typeof count !== 'number' || !Number.isInteger(count) || count < 0) return null;
  const meta: SummarizeMetadata = { messagesSummarized: count };
  if (o.direction === 'before' || o.direction === 'after') meta.direction = o.direction;
  if (typeof o.userContext === 'string') {
    const ctx = o.userContext.trim();
    if (ctx.length > 0) meta.userContext = ctx;
  }
  return meta;
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
  /**
   * Rate-limit tracker tuning (warn/error thresholds + proactive early-warning
   * ladders). Threaded from {@link EngineConfig.rateLimit} so the coordinator's
   * {@link RateLimitTracker} is configurable rather than always defaulting;
   * undefined = provider defaults.
   */
  rateLimit?: RateLimitTrackerOptions;
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
      rateLimit: config.rateLimit,
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
  /**
   * Document attachments (e.g. PDFs) for this user turn. Attached to the user
   * message as native `document` content blocks when the active model supports
   * documents ({@link modelSupportsDocuments}); otherwise ignored with a note so
   * the model can tell the user. Parity: Claude's `document` content block emitted
   * by the Read tool for PDFs.
   */
  documents?: DocumentContent[];
  /**
   * Per-turn total output-token budget for this submit. When set, the engine sizes
   * the request `max_tokens` against the remaining budget and decrements it across
   * context-manager compaction boundaries ({@link nextTaskBudgetRemaining}) so a long,
   * multi-compaction turn stays under the ceiling. Parity: Claude's query-time
   * `taskBudget: { total }` with server-side `task_budget.remaining` countdown
   * (query.ts beta `task-budgets-2026-03-13`).
   */
  taskBudget?: TaskBudget;
}

/**
 * A per-turn output-token budget carried on {@link EngineSubmitOptions.taskBudget}.
 * `total` is the ceiling for the whole turn; the engine tracks a separate remaining
 * figure that is decremented at each compaction boundary. Parity: Claude's
 * `OutputConfig.task_budget` ({ total, remaining }).
 */
export interface TaskBudget {
  /** Total output-token ceiling for the turn. */
  total: number;
}

/**
 * Compute the remaining output-token budget after a context-manager compaction.
 * Mirrors Claude's per-compact carryover
 * `remaining = max(0, (remaining ?? total) - preCompactContextTokens)` (query.ts):
 * the first compaction subtracts from `total`, each subsequent one from the prior
 * remaining, and the result never goes negative. Pure.
 */
export function nextTaskBudgetRemaining(
  budget: TaskBudget,
  preCompactContextTokens: number,
  currentRemaining?: number,
): number {
  const base = currentRemaining ?? budget.total;
  return Math.max(0, base - Math.max(0, preCompactContextTokens));
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
  if (config.maxSkillProgressMessages !== undefined) {
    if (!validateNumericKnob(config.maxSkillProgressMessages, 'maxSkillProgressMessages', { min: 0, integer: true }, warnings)) valid = false;
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
  if (out.maxSkillProgressMessages !== undefined) out.maxSkillProgressMessages = Math.max(0, Math.round(out.maxSkillProgressMessages));
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

// ---------------------------------------------------------------------------
// w96 parity primitives — pure, dependency-light building blocks. Each mirrors a
// specific Claude Code behavior; display/rendering stays in the cli layer, these
// only derive/validate data.
// ---------------------------------------------------------------------------

/** Resolved inputs for {@link isInferenceConfigImmediate}. Keeps the decision a
 *  pure, side-effect-free function — the caller resolves the privileged-user flag
 *  and any experiment override from the environment. */
export interface InferenceConfigImmediacy {
  /** Persisted/config toggle ({@link EngineConfig.immediateInferenceConfig}). */
  configEnabled?: boolean;
  /** Privileged internal user. Parity: `process.env.USER_TYPE === 'ant'`. */
  privilegedUser?: boolean;
  /** Experiment / feature-flag override. Parity:
   *  `getFeatureValue_CACHED_MAY_BE_STALE('tengu_immediate_model_command')`. */
  experimentEnabled?: boolean;
}

/**
 * Whether inference-config commands (`/model`, `/thinking`, `/fast`) should apply
 * immediately to an in-flight turn rather than deferring to turn end. Matches
 * Claude's `shouldInferenceConfigCommandBeImmediate()`: a privileged user OR an
 * enabled experiment forces it on; otherwise the persisted config toggle decides
 * (default OFF). Pure.
 */
export function isInferenceConfigImmediate(r: InferenceConfigImmediacy = {}): boolean {
  return r.privilegedUser === true || r.experimentEnabled === true || r.configEnabled === true;
}

// --- Diff-preview width (parity: NotebookEditPermissionRequest `verbose ? 120 : 80`) ---

/** Diff-preview column width used in verbose mode. Parity: Claude's `120`. */
export const VERBOSE_DIFF_PREVIEW_WIDTH = 120;
/** Diff-preview column width used in compact (non-verbose) mode. Parity: Claude's `80`. */
export const COMPACT_DIFF_PREVIEW_WIDTH = 80;

/**
 * Column width for the inline diff preview in a file-edit permission prompt. An
 * explicit {@link EngineConfig.diffPreviewWidth} override wins; otherwise the width
 * follows the verbose flag ({@link VERBOSE_DIFF_PREVIEW_WIDTH} vs
 * {@link COMPACT_DIFF_PREVIEW_WIDTH}). Parity: Claude's `verbose ? 120 : 80`. Pure.
 */
export function resolveDiffPreviewWidth(
  verbose: boolean,
  config?: Pick<EngineConfig, 'diffPreviewWidth'>,
): number {
  const override = config?.diffPreviewWidth;
  if (typeof override === 'number' && Number.isFinite(override) && override > 0) return override;
  return verbose ? VERBOSE_DIFF_PREVIEW_WIDTH : COMPACT_DIFF_PREVIEW_WIDTH;
}

// --- Symlink disclosure in the edit-approval prompt (parity: ShowInIDEPrompt.tsx) ---

/** Informed-consent disclosure describing that a file-edit target is a symlink
 *  pointing somewhere other than the literal edit path. Parity: Claude's
 *  `ShowInIDEPrompt` `symlinkTarget` note. */
export interface SymlinkEditDisclosure {
  /** Realpath the edited symlink resolves to. */
  target: string;
  /** True when the resolved target is OUTSIDE the working directory. */
  outsideWorkspace: boolean;
  /** Pre-rendered warning note for the permission prompt. */
  note: string;
}

/**
 * Build the symlink-target disclosure for a file-edit permission prompt. Returns
 * `undefined` when there is no symlink target or it resolves to the same path being
 * edited (not a redirecting symlink). Otherwise mirrors Claude's wording: a target
 * that escapes the working directory ⇒
 * `"This will modify <target> (outside working directory) via a symlink"`, an
 * in-workspace target ⇒ `"Symlink target: <target>"`. Pure.
 */
export function describeSymlinkEdit(
  editPath: string,
  symlinkTarget: string | null | undefined,
  cwd: string,
): SymlinkEditDisclosure | undefined {
  if (!symlinkTarget || symlinkTarget === editPath) return undefined;
  const rel = relative(cwd, symlinkTarget);
  const outsideWorkspace = rel === '' ? false : rel.startsWith('..') || isAbsolute(rel);
  const note = outsideWorkspace
    ? `This will modify ${symlinkTarget} (outside working directory) via a symlink`
    : `Symlink target: ${symlinkTarget}`;
  return { target: symlinkTarget, outsideWorkspace, note };
}

// --- Worker/subagent attribution badge (parity: WorkerBadge.tsx) ---

/** Identity of a non-primary swarm worker/subagent requesting a permission, so a
 *  prompt can render a colored `● @name` badge. Parity: Claude's `WorkerBadge`
 *  props ({ name, color }). */
export interface WorkerAttribution {
  /** Display name of the requesting worker (rendered as `@name`). */
  name: string;
  /** Theme/ANSI color for the badge dot + name. */
  color?: string;
}

/**
 * Whether a permission prompt should show a worker-attribution badge. The badge is
 * shown ONLY when the request originates from a named, non-primary subagent — the
 * primary session never badges itself. Parity: Claude renders `WorkerBadge` only for
 * swarm workers. Pure.
 */
export function shouldShowWorkerBadge(
  requestingAgent: WorkerAttribution | undefined,
  isPrimary: boolean,
): requestingAgent is WorkerAttribution {
  return !isPrimary && !!requestingAgent && requestingAgent.name.trim() !== '';
}

// --- In-REPL startup notification queue (parity: context/notifications.tsx) ---

/** Default auto-dismiss timeout for a {@link ReplNotification}. Parity: Claude's
 *  `DEFAULT_TIMEOUT_MS = 8000`. */
export const DEFAULT_NOTIFICATION_TIMEOUT_MS = 8000;

/** Priority tier of a {@link ReplNotification}. Parity: Claude's notification
 *  `Priority`. */
export type NotificationPriority = 'low' | 'medium' | 'high' | 'immediate';

/** A transient in-REPL startup notification. Parity: Claude's `Notification`
 *  (context/notifications.tsx) minus the React-specific JSX variant. */
export interface ReplNotification {
  /** Stable identity; a later notification with the same key folds over the earlier. */
  key: string;
  /** Message text to display. */
  text: string;
  /** Display priority; `'immediate'` bypasses the queue to the front. */
  priority: NotificationPriority;
  /** Auto-dismiss timeout (ms); defaults to {@link DEFAULT_NOTIFICATION_TIMEOUT_MS}. */
  timeoutMs?: number;
  /** Keys of queued/displayed notifications this one invalidates (removes). */
  invalidates?: string[];
}

/** Effective auto-dismiss timeout for a notification. Parity: `timeoutMs ?? DEFAULT_TIMEOUT_MS`. */
export function resolveNotificationTimeout(n: Pick<ReplNotification, 'timeoutMs'>): number {
  return n.timeoutMs ?? DEFAULT_NOTIFICATION_TIMEOUT_MS;
}

/**
 * Compute the next notification queue after an incoming notification arrives,
 * mirroring Claude's queue semantics: the incoming notification's `invalidates`
 * keys are dropped from the queue; an existing entry with the SAME key is folded
 * (replaced) by the incoming one; an `'immediate'` notification jumps to the front,
 * any other is appended. Pure — returns a new array, never mutates `existing`.
 */
export function planNotificationQueue(
  existing: readonly ReplNotification[],
  incoming: ReplNotification,
): ReplNotification[] {
  const invalid = new Set(incoming.invalidates ?? []);
  const kept = existing.filter((n) => n.key !== incoming.key && !invalid.has(n.key));
  return incoming.priority === 'immediate' ? [incoming, ...kept] : [...kept, incoming];
}

// --- Slash-command lifecycle beacon (parity: utils/commandLifecycle.ts) ---

/**
 * Lifecycle phase of a single slash-command invocation. Parity: Claude Code's
 * `commandLifecycle.ts` `CommandLifecycleState` (`'started' | 'completed'`), the
 * lightweight signal a UI/observer consumes to track in-flight command
 * invocations (by {@link SlashCommandLifecycleEvent.uuid}) and render affordances
 * while a command runs.
 */
export type SlashCommandLifecycleState = 'started' | 'completed';

/** The canonical analytics-event TYPE AMI records each slash-command lifecycle
 *  transition under, so observers (the usage view, remote-control, a `/` spinner)
 *  query ONE event name via `getEventsByType`. Parity: Claude's per-command
 *  started/completed notification, routed through AMI's analytics surface. */
export const SLASH_COMMAND_LIFECYCLE_EVENT = 'slash_command_lifecycle';

/**
 * Structured payload for one slash-command lifecycle transition. `uuid` pairs the
 * `'started'` and `'completed'` records of a single invocation so an observer can
 * measure in-flight commands and their duration; `command` is the
 * display-redacted command line (never the raw argument text). Parity: Claude's
 * `notifyCommandLifecycle(uuid, state)`.
 */
export interface SlashCommandLifecycleEvent {
  uuid: string;
  command: string;
  state: SlashCommandLifecycleState;
}

/**
 * Build the structured payload for a slash-command lifecycle transition. Pure —
 * the single constructor both the `'started'` and `'completed'` emissions use so
 * the two records of one invocation are shaped identically (only `state` differs)
 * and can never drift. Returned as a fresh plain object safe to hand to
 * `analytics.log`.
 */
export function buildSlashCommandLifecycleEvent(
  uuid: string,
  command: string,
  state: SlashCommandLifecycleState,
): SlashCommandLifecycleEvent {
  return { uuid, command, state };
}
