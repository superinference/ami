// Split from types.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { DiffHunk, ToolInputSchema } from './types-transcript-meta';
import type { ToolContext } from './types';

/**
 * Live, structured progress for a long-running shell/task tool, carried so a
 * frontend can render a "(elapsed · N lines)" affordance without re-parsing the
 * streamed text. Consolidates the reference's shell/task progress schemas — the
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
   *  tool sets (parity with the reference's shell result fields):
   *  - `preSpawnError`: `'cwd-missing' | 'spawn-failure'` — the command never ran
   *    because the shell could not be spawned (a deleted working directory, or a
   *    pre-exec spawn failure). Distinguishes a setup failure from a command that
   *    ran and exited non-zero or timed out, mirroring the reference's dedicated
   *    `preSpawnError` result field (`ShellCommand.createFailedCommand`).
   *  - `assistantAutoBackgrounded`: `true` when a long foreground command was
   *    auto-moved to the background in assistant/detached mode so the main agent
   *    stays responsive (the reference's `shouldAutoBackground` / assistant blocking
   *    budget). Accompanied by `backgrounded: true` and the `taskId`.
   *  - `backgroundedByUser`: `true` when the model EXPLICITLY requested
   *    `run_in_background: true` (as opposed to an auto-move on timeout, which sets
   *    `assistantAutoBackgrounded`). Accompanied by `backgrounded: true` and the
   *    `taskId`. Parity: the reference's `ExecResult.backgroundedByUser`.
   *  - `outputFilePath` / `outputFileSize`: set when stdout overflowed the inline
   *    cap and the FULL output was persisted to disk — the absolute path and its
   *    byte size, so a programmatic caller reads them without re-parsing the spill
   *    banner. Parity: the reference's `ExecResult.outputFilePath` / `outputFileSize`.
   *  - `pluginHints`: `PluginRecommendationHint[]` — install suggestions parsed
   *    from an `<ami-hint type="plugin" .../>` tag a command emitted on stderr
   *    (set only when `AMI_PLUGIN_HINTS` is enabled; the reference's stderr-hint plugin
   *    recommendation).
   *  - `sedSimulatedEdit`: `true` when a `sed -i` in-place substitution was applied
   *    as an in-memory FILE EDIT (write of the `applySedSubstitution` result)
   *    instead of running the real `sed` binary, accompanied by `filePath`. This
   *    guarantees the written bytes equal the before → after diff the permission
   *    prompt previewed — parity with the reference's `_simulatedSedEdit` exactness
   *    guarantee (SedEditPermissionRequest).
   *
   *  Keys the file_edit tool sets on a SUCCESSFUL edit (parity with the reference's
   *  FileEditTool `outputSchema`), so a programmatic caller reads them without
   *  re-parsing the text `output`:
   *  - `filePath`: `string` — the edited path.
   *  - `editMode`: `'single' | 'multiple'` — whether one edit or an `edits[]` batch
   *    was applied (the reference's IDEDiffConfig `editMode` discriminator), letting a
   *    diff/permission UI branch on single-vs-batch presentation.
   *  - `replaceAll`: `boolean` — whether every occurrence was replaced.
   *  - `userModified`: `boolean` — whether the applied bytes diverged from the
   *    model's proposal via an interactive accept-with-edits path. Always `false`
   *    in AMI (the permission layer is allow/deny, with no in-prompt diff editor);
   *    emitted to match the reference's output contract.
   *  - `gitDiff`: {@link GitDiffSummary} — change counts + unified patch (omitted on
   *    the diff-cost-capped success paths, where no per-line diff is computed).
   *
   *  Keys the lsp tool sets on a successful query (parity with the reference's LSPTool
   *  `formatResult` structured return), so a programmatic caller reads the hit count
   *  without re-parsing the formatted text:
   *  - `resultCount`: `number` — total hits for the operation. For a hierarchical
   *    `documentSymbol` result this is the RECURSIVE nested-symbol total (a class
   *    plus its methods), matching the reference's `countSymbols`; for other ops it is the
   *    count of valid locations/symbols/calls (1/0 for `hover`).
   *  - `fileCount`: `number` — number of DISTINCT files the hits span. */
  metadata?: Record<string, unknown>;
  /** Structured per-hunk diff for a file-mutating result, carried alongside the
   *  text `output` for programmatic rendering. Parity: the reference's `structuredPatch`. */
  structuredPatch?: DiffHunk[];
  /** The edit's per-hunk {@link structuredPatch} was deliberately OMITTED (left
   *  `[]`) because the file exceeded the diff-scan cap — the change happened but
   *  its location could not be rendered. Parity: the reference's readEditContext
   *  `truncated: true` signal, which lets a replay/introspection surface say
   *  "edit location not shown — file too large to scan" instead of mistaking an
   *  empty patch for "no change". Only set on a SUCCESSFUL, size-capped edit. */
  structuredPatchTruncated?: boolean;
  /** The tool call was canceled before/while running (e.g. the surrounding batch
   *  was aborted) — distinct from a tool that ran and errored. Parity: the reference's
   *  `CANCEL_MESSAGE` content marker. Drives a dedicated "canceled" render state
   *  via {@link classifyToolResultState}. */
  canceled?: boolean;
  /** The user actively rejected this tool call at a permission prompt (deny), so it
   *  never ran. Parity: the reference's `REJECT_MESSAGE` content marker. Distinct from an
   *  execution error. Drives the "rejected" render state. */
  rejected?: boolean;
  /** The tool call was interrupted (user pressed stop / server abort) with no real
   *  result. Parity: the reference's `INTERRUPT_MESSAGE_FOR_TOOL_USE`. Distinct from a tool
   *  that produced an error output. Drives the "interrupted" render state. */
  interrupted?: boolean;
  /** Severity of a failing result, for tools that distinguish a RECOVERABLE
   *  validation outcome from a FATAL error. `'ask'` marks a soft failure the model is
   *  expected to fix and retry (e.g. a file_edit whose `old_string` was not found, the
   *  file was not read first, the edit is a no-op, or the file changed since it was
   *  read) — the call still carries `isError: true`, but a frontend/transcript can
   *  present it as an "ask and retry" prompt rather than a hard stop. `'error'` (or
   *  absent) is a plain fatal error. Parity: the reference's FileEditTool validation
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
   *  {@link resolveSentAt}. Parity: the reference's BriefTool output `sentAt`. */
  sentAt?: string;
  /** The USER-facing payload, decoupled from the model-facing `output`. When a tool
   *  wants the model to receive only a terse receipt (e.g. "Message delivered to
   *  user.") while the human sees the full message/attachments, it sets the rich
   *  content here and a short receipt in `output`. Renderers show this to the user;
   *  the model context keeps `output`. Selected via {@link selectUserFacingOutput} /
   *  {@link selectModelFacingOutput}. Absent → both audiences see `output`. Parity:
   *  the reference's BriefTool `output.data` (UI) vs `mapToolResultToToolResultBlockParam`
   *  model receipt. */
  userFacingContent?: string;
  /** When true, a frontend MUST NOT render this tool result in the transcript — the
   *  tool declares its own output redundant because a higher-level lifecycle message
   *  already reports the same thing (e.g. a batched shutdown/cleanup summary covers a
   *  per-item cleanup result). The model STILL receives `output` in its context; only
   *  the user-facing render is suppressed, so this never hides information from the
   *  model. Absent / `false` ⇒ render normally. Honor it uniformly via
   *  {@link shouldRenderToolResult} rather than reading the flag directly. Parity:
   *  the reference's `renderToolResultMessage` returning `null` to suppress a redundant
   *  tool-result ("Suppress cleanup result — the batched shutdown message covers this",
   *  TeamDeleteTool/UI.tsx). */
  suppressRender?: boolean;
}

/** The collapse "kind" a single tool call contributes to a consecutive
 *  read/search group. Parity: the reference's `getToolSearchOrReadInfo` discriminator
 *  (`utils/collapseReadSearch.ts`). `absorbed` tool calls (e.g. `tool_search`)
 *  belong to the group visually but increment no count. */
export const TOOL_GROUP_KINDS = ['search', 'read', 'list', 'repl', 'bash', 'gitOpBash', 'mcpCall', 'memoryRead', 'memorySearch', 'memoryWrite', 'absorbed', 'none'] as const;

export type ToolGroupKind = (typeof TOOL_GROUP_KINDS)[number];

/** Aggregated per-kind counts for a collapsed run of consecutive read/search
 *  tool calls, so a frontend can render one summary row ("Read 4 files · Searched
 *  3×") instead of a wall of individual rows. Parity: the reference's
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
 *  inbound user message. Parity: the reference's channel-message schema
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
 *  outbound traffic through a host proxy (parity: the reference's SandboxNetworkConfig). */
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
   *  (full egress block). Mirrors the reference's
   *  `shouldAllowManagedSandboxDomainsOnly()` / `allowManagedDomainsOnly`.
   *  Additive: undefined/false leaves the normal allowlist behavior unchanged. */
  allowManagedDomainsOnly?: boolean;
  /** The admin/managed-provided outbound allowlist honored (and used EXCLUSIVELY)
   *  when {@link allowManagedDomainsOnly} is set. Ignored otherwise. Mirrors the
   *  policy-tier `sandbox.network.allowedDomains` the reference uses under managed-only. */
  managedAllowedHosts?: string[];
}

/** Granular filesystem carve-outs layered on top of the project-only writable
 *  mount (parity: the reference's SandboxFilesystemConfig). Deny wins over allow, and
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
   *  still applies. Mirrors the reference's `allowManagedReadPathsOnly`
   *  (sandbox-adapter's `shouldAllowManagedReadPathsOnly()` gate on non-policy
   *  `allowRead`). Additive: undefined/false leaves `allowRead` behavior unchanged. */
  allowManagedReadPathsOnly?: boolean;
  /** The admin/managed-provided read RE-allow paths honored (and used EXCLUSIVELY)
   *  when {@link allowManagedReadPathsOnly} is set. Ignored otherwise. */
  managedAllowRead?: string[];
}

/** Custom seccomp syscall-filter configuration for the sandbox backend.
 *  Parity: the reference's `sandbox.seccomp.bpfPath` / `sandbox.seccomp.applyPath`
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

/** Result of a tool's structured pre-execution {@link ToolDefinition.validateInput}
 *  (parity: the reference's `ValidationResult`): either success, or a machine-readable
 *  `errorCode` plus a model-facing `message`, produced BEFORE the permission check
 *  and before the tool body runs. */
export type ToolValidationResult =
  | { result: true }
  | { result: false; message: string; errorCode: number };

/** Fine-grained read/search/list classification of a single tool invocation
 *  (parity: the reference's `isSearchOrReadCommand(input)`), used to drive concise,
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
   *  Parity: the reference's `Tool.isReadOnly(input)`. */
  isReadOnlyForInput?: (input: Record<string, unknown>) => boolean;
  /** Whether this tool may run CONCURRENTLY with other calls in the same turn's
   *  batch. Either a static flag OR — like {@link isReadOnlyForInput} — an
   *  input-aware predicate, so a tool that is parallel-safe for some inputs and
   *  must serialize for others (e.g. a read that targets a file another call in the
   *  batch writes) can decide per call. When absent, the executor falls back to the
   *  per-invocation read-only verdict (read-only ⇒ concurrency-safe). Resolve a tool
   *  call's verdict with {@link isToolConcurrencySafe}. Parity: the reference's
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
   *  predicate. Parity: the reference's `Tool.isOpenWorld?(input)` (the reference consults
   *  it input-aware as `tool.isOpenWorld?.(input)`; MCPTool defaults false,
   *  overridden per-MCP-tool from the server's `annotations.openWorldHint`). */
  isOpenWorld?: boolean | ((input: Record<string, unknown>) => boolean);
  /** Extra capability keywords (3-10 words) weighted by `tool_search`, using terms
   *  NOT already in the name/description (e.g. 'jupyter' for notebook_edit) so a
   *  deferred tool is findable by synonym. Parity: the reference's `Tool.searchHint`. */
  searchHint?: string;
  /** When true, keep this (native) tool OUT of the default advertised set and surface
   *  it on demand via `tool_search` — the per-tool opt-in equivalent of MCP schema-size
   *  auto-deferral. Parity: the reference's `Tool.shouldDefer`. */
  shouldDefer?: boolean;
  /** Per-tool self-declared opt-OUT of deferral: force-KEEP this tool in the initial
   *  advertised set (full schema) even when it would otherwise be deferred behind
   *  `tool_search` (e.g. an MCP tool subject to schema-size auto-deferral). An MCP
   *  server sets this on a tool via `_meta['anthropic/alwaysLoad'] === true`. Checked
   *  before the `isMcp` auto-defer rule. Parity: the reference's `Tool.alwaysLoad`
   *  (ToolSearchTool/prompt.ts — `if (tool.alwaysLoad === true) return false`). */
  alwaysLoad?: boolean;
  /** Input-aware irreversible-operation predicate (delete / overwrite / send). Absent
   *  or `false` = not destructive. Lets the permission layer add an extra guard for ANY
   *  tool, not just bash. Parity: the reference's `Tool.isDestructive(input)`. */
  isDestructive?: (input: Record<string, unknown>) => boolean;
  /** Runtime availability gate — when it returns false the tool is dropped from the
   *  advertised set (feature flag / env condition). Parity: the reference's `Tool.isEnabled()`. */
  isEnabled?: (context?: ToolContext) => boolean;
  /** Per-tool cap (chars) on result size before spill-to-disk, overriding the
   *  engine-global default; `Infinity` = never spill. Parity: `maxResultSizeChars`. */
  maxResultSizeChars?: number;
  /** Alternate names this tool also answers to, for legacy/compat dispatch
   *  (e.g. `KillShell` → task_stop). Parity: the reference's tool `aliases`. */
  aliases?: string[];
  /** Force the provider to constrain generated arguments to exactly match
   *  {@link inputSchema} (OpenAI/Anthropic strict/structured tool mode). Parity: strict. */
  strict?: boolean;
  /** JSON Schema describing the tool's structured result — used to validate a
   *  persisted/resumed tool result before rendering it. Parity: the reference's `Tool.outputSchema`. */
  outputSchema?: ToolInputSchema;
  /** What happens to an in-flight call when the user submits a new message:
   *  `'cancel'` aborts and discards the result; `'block'` (default) runs to completion
   *  while the message waits. Parity: the reference's `Tool.interruptBehavior()`. */
  interruptBehavior?: 'cancel' | 'block';
  /** Structured pre-execution validation run BEFORE the permission check; returns a
   *  machine-readable {@link ToolValidationResult} so the model gets a clean, schema-derived
   *  correction without entering the tool body. Parity: the reference's `Tool.validateInput`. */
  validateInput?: (input: Record<string, unknown>, context: ToolContext) => Promise<ToolValidationResult>;
  /** Compact human-readable activity phrase for progress UIs (e.g. "Reading src/foo.ts")
   *  computed from the input. Parity: the reference's `Tool.getActivityDescription(input)`. */
  getActivityDescription?: (input: Record<string, unknown>) => string;
  /** Fine-grained per-invocation read/search/list classification driving transcript
   *  summaries. Parity: the reference's `Tool.isSearchOrReadCommand(input)`. */
  isSearchOrReadCommand?: (input: Record<string, unknown>) => ToolAccessClassification;
  /** The wall-clock timeout budget (ms) this invocation will be killed at, resolved from
   *  the input (and any env/default knobs). Lets the executor surface a "N/budget elapsed"
   *  affordance on a long-running call so the user can see how close it is to its timeout
   *  boundary. Return `undefined` when the tool has no bounded budget. Parity: the reference's
   *  `timeoutMs` threaded through ShellProgressMessage / BashToolResultMessage
   *  (tools/BashTool/UI.tsx, tools/PowerShellTool/UI.tsx). */
  getProgressBudgetMs?: (input: Record<string, unknown>) => number | undefined;
  /** Compact representation of this tool use fed to an auto-mode security classifier
   *  (`''` = skip). Parity: the reference's `Tool.toAutoClassifierInput(input)`. */
  toAutoClassifierInput?: (input: Record<string, unknown>) => string;
  /** Long-form proactive-use / when-to-use (and when-NOT-to-use) guidance carried
   *  alongside the terse `description` and surfaced to the model. Parity: TodoWrite prompt. */
  prompt?: string;
  /** Tool-authored concise transcript label for a call, preferred by renderers over the
   *  generic field-sniffer/JSON fallback. Parity: the reference's `renderToolUseMessage`. */
  renderUseMessage?: (input: Record<string, unknown>) => string;
  /** Input-derived display name for a tool call, so a renderer can show a human-friendly
   *  header (e.g. "Creating worktree") computed from the arguments instead of the raw tool
   *  name. Empty string => fall back to {@link name}. Parity: the reference's `Tool.userFacingName(input)`
   *  (components/tasks/renderToolActivity.tsx). */
  userFacingName?: (input: Record<string, unknown>) => string;
  /** When true, this tool is a transparent wrapper: renderers show only its progress/result
   *  and suppress the tool-name header row (e.g. a passthrough/forwarding tool). Parity:
   *  the reference's `Tool.isTransparentWrapper` (components/messages/AssistantToolUseMessage.tsx). */
  isTransparentWrapper?: boolean;
  /** Per-tool predicate deciding whether a result string is truncated (e.g. exceeded a
   *  line-count cap), so a renderer can show a "truncated" affordance without re-deriving
   *  the tool's own limit. Parity: the reference's `Tool.isResultTruncated(output)` (MCPTool). */
  isResultTruncated?: (output: string) => boolean;
  /** Semantic equality of two parsed inputs for this tool, used to dedup repeated identical
   *  calls (order-insensitive / field-normalized where the generic deep-equal is too strict).
   *  Absent => callers fall back to structural equality. Parity: the reference's
   *  `Tool.inputsEquivalent(a, b)`. */
  inputsEquivalent?: (a: Record<string, unknown>, b: Record<string, unknown>) => boolean;
  /** Per-tool opt-in to Anthropic fine-grained tool streaming (`eager_input_streaming`):
   *  the model streams this tool's input JSON as it is generated instead of only after the
   *  tool_use block completes, lowering latency for tools with long arguments. Unlike the
   *  process-wide `SUPERINFERENCE_FINE_GRAINED_TOOL_STREAMING` env knob (which opts in every
   *  tool), this flag opts in ONLY this tool even when the global knob is off. Applied only
   *  on the first-party `api.anthropic.com` path (proxies/Bedrock/Vertex 400 on the field).
   *  Parity: the reference's per-tool `eager_input_streaming` on the cached tool schema. */
  eagerInputStreaming?: boolean;
  /** Per-tool opt-in to GROUPED rendering: when `true`, consecutive calls to this
   *  tool are collapsed into one summary row by a group-aware renderer instead of
   *  each call drawing its own header. Absent / `false` ⇒ render each call
   *  individually. Resolve with {@link shouldRenderGroupedToolUse}. Parity: the reference's
   *  per-tool `renderGroupedToolUse` opt-in (utils/groupToolUses.ts). */
  renderGroupedToolUse?: boolean;
  execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult>;
}
