// Split from types.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).

/**
 * Type-branded session id: a nominal `string` subtype so a raw string can't be silently
 * passed where a persisted session id is expected. It stays assignable TO `string`
 * (reads / pass-through / Map keys need no cast); only MINTING one from a raw string goes
 * through {@link asSessionId}. The brand symbol is unexported, so the type is truly
 * nominal. (round-2 "type-branded ids".)
 */
export declare const SESSION_ID_BRAND: unique symbol;

export type SessionId = string & { readonly [SESSION_ID_BRAND]: true };

/** Mint a {@link SessionId} from a raw string (the single branding boundary). */
export function asSessionId(id: string): SessionId { return id as SessionId; }

/**
 * Type-branded agent id — a nominal `string` subtype so a free-text teammate name
 * cannot be silently passed where a real agent id is expected (parity: the reference's
 * `AgentId` in types/ids.ts). {@link asAgentId} mints one unconditionally (the
 * pass-through boundary); {@link toAgentId} VALIDATES the format first and returns
 * `null` for a non-conforming string, which is what discriminates a genuine agent id
 * from an arbitrary name at the mint boundary.
 */
export declare const AGENT_ID_BRAND: unique symbol;

export type AgentId = string & { readonly [AGENT_ID_BRAND]: true };

/** Mint an {@link AgentId} from a raw string without validation (pass-through boundary). */
export function asAgentId(id: string): AgentId { return id as AgentId; }

/** Validate and brand an agent id; returns `null` when `id` is not a well-formed
 *  agent id (empty, or containing whitespace / disallowed characters). */
export function toAgentId(id: string): AgentId | null {
  return /^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(id) ? (id as AgentId) : null;
}

/**
 * Detached/headless output formats accepted by `--output-format` (parity: the reference's
 * `OutputFormat`). `json` / `jsonl` / `text` are machine/human batch encodings;
 * `streamlined` is a distillation-resistant text mode that keeps the final response
 * text intact, collapses every tool call into a single cumulative tool-use summary
 * line, and drops thinking + system/init noise — so a downstream model (or operator)
 * reading the transcript gets the signal without the per-tool-call bulk (parity:
 * the reference's `streamlined` output-format value / `createStreamlinedTransformer`). The
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
 * and the `AMI_THEME` env var (parity: the reference's `THEME_SETTINGS`, as distinct from
 * the resolved `THEME_NAMES`). The CLI's `resolveThemeByName` turns each of these
 * into a concrete palette at render time:
 *   - `default` / `auto` — follow the terminal (auto-detect scheme + color tier);
 *     `auto` is the reference-parity alias of the historical AMI `default`.
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
 * the reference's `ThemePicker` option labels, e.g. `auto` → "Auto (match terminal)",
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
 * terminal family (parity: the reference's NOTIFICATION_CHANNELS). `auto` picks the best
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
 * the reference's imageResizer sniff targets — PNG/JPEG/GIF/WebP). A declared type outside
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
 * directly rather than through a base64 text blob. Mirrors the reference's third content
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
 * Provenance of an inbound user/queued message (parity: the reference's `MessageOrigin`),
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
 * the reference's TranscriptMessage `parentUuid`/`logicalParentUuid`/`isSidechain`/`agentId`/
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
   *  the reference's `createUserMessage({ isVisibleInTranscriptOnly })` + `shouldShowUserMessage`. */
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
   *  audit/replay. Parity: the reference's per-message `permissionMode`. */
  permissionMode?: string;
  /** Ids of inline pasted-image placeholders (e.g. `[Image #1]`) in this message's
   *  text, correlating each placeholder with the image content block appended to the
   *  message. Set only when non-empty (else omitted). Parity: the reference's
   *  `processTextPrompt({ imagePasteIds })` user-message field. */
  imagePasteIds?: number[];
  /** Marks a message that carries hook-produced additional context (a SessionStart/
   *  Setup `additionalContexts[]` aggregation — the reference's `hook_additional_context`
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
   *  Parity: the reference's MessageModel. */
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
   *  calls so they surface as errors, not ordinary tool output. Parity: the reference's
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
   *  change the styling (color/dot). Parity: the reference's SystemMessage `level`. */
  level?: 'info' | 'warning' | 'error';
  /** Optional semantic subtype classifying WHY this system message was emitted, so a
   *  renderer can present a category distinctly (e.g. a proactive `'suggestion'` like
   *  "Skill \"x\" updated with improvements." rendered as a hint rather than a plain
   *  system line). Absent ⇒ a generic system message. Check with
   *  {@link isSuggestionMessage}. Parity: the reference's `createSystemMessage` subtype
   *  argument (`'suggestion'`). */
  kind?: SystemMessageKind;
}

/** Semantic subtype of a {@link SystemMessage}. `'suggestion'` tags a proactive,
 *  dismissible hint (e.g. a skill-improvement confirmation) so the UI can style it
 *  apart from ordinary system output. Parity: the reference's `createSystemMessage`
 *  message-subtype parameter. */
export type SystemMessageKind = 'suggestion';

export type Message = UserMessage | AssistantMessage | ToolMessage | SystemMessage;

export interface JsonSchemaProperty {
  /** JSON Schema type. A string for a single type, or an array of types for a
   *  union (e.g. `['string','boolean','number']`) — parity with the reference's
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
   *  (parity: the reference's `z.strictObject`). Omitted => permissive, as before. */
  additionalProperties?: boolean;
}

/** One hunk of a structured diff (parity: the reference's FileEdit `structuredPatch`),
 *  so a frontend can render a file mutation per-hunk instead of parsing text. */
export interface DiffHunk {
  oldStart: number;
  oldLines: number;
  newStart: number;
  newLines: number;
  lines: string[];
}

/** VCS-aware per-edit change summary attached to a file-mutating tool result's
 *  {@link ToolResult.metadata} as `gitDiff`. Parity: the reference's FileEditTool
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
  /** `additions + deletions` — total changed lines (the reference's `changes`). */
  changes: number;
  /** Unified diff text (bounded by the edit diff cap). */
  patch: string;
}
