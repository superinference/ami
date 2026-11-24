// Split from skills.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { HookConfigEntry } from './hooks';
import type { McpServerConfig } from './mcp/manager';
import type { ThinkingLevel } from './model-capabilities';
import { isSafeRelativePath } from './skill-catalog';
import type { SkillKind } from './skills-validate-skill-args';
import { DEFAULT_SKILL_PROGRESS_MESSAGE, defaultProgressMessage, requiredEnvSatisfied } from './skills-agent-definition';
import type { AgentDefinition } from './skills-agent-definition';

export interface SkillDefinition {
  name: string;
  description: string;
  content: string;        // Markdown body (the skill instructions)
  filePath: string;       // Where the skill file lives ('<builtin>' for built-ins)
  version?: string;       // Author-declared version string (frontmatter `version`), surfaced in /skills
  whenToUse?: string;     // Hint for auto-activation
  paths?: string[];       // File glob patterns that auto-activate this skill
  model?: string;         // Override model for this skill
  userInvocable?: boolean; // Can user invoke via /skill-name (default true)
  allowedTools?: string[]; // Restrict which tools this skill can use
  disallowedTools?: string[]; // Tools to exclude (blacklist, applied after allowedTools)
  argumentHint?: string;  // Hint text for the argument (e.g., "<file-path>")
  argumentNames?: string[]; // Declared positional argument names (frontmatter `arguments`), bound to $name placeholders
  /** Alternate names that ALSO resolve to this skill (parity with the reference's bundled
   *  skill `aliases`). Registered as secondary lookup keys so `getSkill`/`getSkillContent`
   *  and the skill tool accept any alias without a second definition. Never shadows a
   *  real skill name. Parsed via {@link parseSkillAliases}. */
  aliases?: string[];
  /** Runtime availability predicate (parity with the reference's bundled skill `isEnabled`).
   *  When present and it returns `false`, the skill is hidden from listings/context and
   *  cannot be invoked — letting a dependency/feature-gated skill ship but stay dormant
   *  until its precondition holds. Absent ⇒ always enabled. Evaluated via
   *  {@link isSkillEnabled} (a throwing predicate is treated as disabled). */
  isEnabled?: () => boolean;
  /** DECLARATIVE environment/feature-flag gate (parity with the reference's bundled skills
   *  that register conditionally on an environment/config flag, e.g. `stuck.ts`'s
   *  env-gated registration and `index.ts`'s feature-flag-gated bundled set). The
   *  skill is available ONLY when EVERY named environment variable is set to a
   *  non-empty value; an unset/blank var hides the skill from listings/context and
   *  the skill tool — exactly like a `false` {@link isEnabled}. Unlike `isEnabled`
   *  (a JS predicate a disk/plugin skill cannot author in YAML), this is a plain
   *  string list declarable in SKILL.md frontmatter (`required-env`). Evaluated by
   *  {@link isSkillEnabled} (which honors it ALONGSIDE `isEnabled`), so no consumer
   *  needs a second gate. Absent/empty ⇒ no env requirement. */
  requiredEnv?: string[];
  /** When true, invoking this skill with no argument is an error that returns
   *  {@link missingArgumentMessage} (or a generated help line) instead of running an
   *  argument-less skill body (parity with the reference's required-argument skills, e.g.
   *  `/batch`'s MISSING_INSTRUCTION_MESSAGE). */
  requiresArgument?: boolean;
  /** Custom help/examples message returned when a {@link requiresArgument} skill is
   *  invoked without an argument. Blank/absent ⇒ a generated usage line. */
  missingArgumentMessage?: string;
  /** When true, the skill may only run inside a git working tree — invoking it from a
   *  non-git cwd returns an up-front, explanatory precondition message instead of
   *  letting a later worktree/PR step fail with a low-level error (parity with the reference
   *  Code's `getIsGit()` guard in worktree-spawning commands like `/batch`). Checked by
   *  the skill tool via {@link isGitRepo}; declarative metadata (reviewed-safe). */
  requiresGitRepo?: boolean;
  /** Least-privilege sandbox for a FORKED skill (parity with the reference's forked-agent
   *  `createAutoMemCanUseTool`): when true, the forked sub-engine's `bash` is capped to
   *  read-only commands via a {@link ToolCapabilityCeiling} — a HARD cap enforced ahead
   *  of the sub-engine's permission mode. Declarative metadata; frontmatter
   *  `read-only-bash: true`. No effect on an inline skill. */
  sandboxReadOnlyBash?: boolean;
  /** Directories a FORKED skill's file writes are confined to (parity with the reference's
   *  autoMem write-path confinement, writes only within the memory dir). When set, the
   *  forked sub-engine denies edit/write outside these roots regardless of mode.
   *  Relative entries resolve against the skill's run cwd. Frontmatter
   *  `confine-writes-to:` (comma/newline list). Declarative metadata. */
  sandboxWriteRoots?: string[];
  /** Programmatic dynamic-prompt builder (parity with the reference's bundled-skill
   *  `getPromptForCommand(args)`, e.g. `/batch`, `/claude-api`). When present, the
   *  skill body is COMPUTED per invocation from the raw argument string (plus any
   *  runtime state the builder closes over) instead of the static {@link content}
   *  markdown — letting a skill validate args, lazy-load large reference material,
   *  or branch on the environment. May be sync or async. The content resolver
   *  prefers it over `content`; resolve via {@link resolveSkillPrompt}. */
  getPromptForCommand?: (args: string) => string | Promise<string>;
  /** Supporting resource files bundled WITH the skill (parity with the reference's bundled
   *  `SKILL_FILES`: a map of skill-relative path → inlined file content). The skill
   *  body may reference these companion docs/examples; read one safely (with
   *  path-traversal rejection) via {@link getSkillFile}. */
  files?: Record<string, string>;
  /** Affordance text shown while the skill is executing (parity with the reference's
   *  command/skill `progressMessage`, which defaults to `'running'`). Resolved —
   *  with the default fallback — via {@link skillProgressMessage}. */
  progressMessage?: string;
  /** When true, invoking this skill/command JUMPS the message queue and executes
   *  at once instead of waiting for the next stop point (parity with the reference's
   *  command `immediate`). Checked via {@link isSkillImmediate}. */
  immediate?: boolean;
  /** The shell a skill/command's inline `!`-blocks run under (parity with the reference's
   *  `shell:` frontmatter). Absent ⇒ the executor defaults to bash — resolve with
   *  the fallback via {@link shellForExecution}. Parsed/validated via
   *  {@link parseShellFrontmatter}. */
  shell?: FrontmatterShell;
  /** Author-defined named template variables substituted into the skill body:
   *  every `{{key}}` token in {@link content} is replaced with `templateVars[key]`
   *  (parity with the reference's bundled-skill `SKILL_MODEL_VARS`) so centrally-maintained
   *  wording updates in one place. An unknown token is left intact. Applied via
   *  {@link applySkillTemplateVariables}. */
  templateVars?: Record<string, string>;
  /** When true, the skill's invocation ARGUMENTS are sensitive and must be masked
   *  (as `***`) wherever the invocation is echoed — the permission-prompt summary,
   *  transcript, and conversation history (parity with the reference's command
   *  `isSensitive`). Resolve a display-safe argument string via
   *  {@link redactSkillArgs}. */
  isSensitive?: boolean;
  /** Hide the skill from user-facing help/typeahead listings (parity with the reference's
   *  command `isHidden`). A hidden skill is STILL invocable by its exact name; it is
   *  only omitted from the `/skills` catalog. Checked via {@link isSkillHidden}. */
  isHidden?: boolean;
  /** Overrides the name shown for the skill in listings (parity with the reference's
   *  command `userFacingName()`) WITHOUT changing the canonical lookup `name`.
   *  Resolved — with the `name` fallback — via {@link skillUserFacingName}. */
  userFacingName?: string;
  /** Base directory exported to the skill's `!`-blocks / hooks as
   *  `CLAUDE_PLUGIN_ROOT` (and the `AMI_PLUGIN_ROOT` alias) — parity with the reference's
   *  PromptCommand `skillRoot`. Lets a plugin/disk skill reference its bundle files
   *  by an absolute `${CLAUDE_PLUGIN_ROOT}/...` path regardless of the session cwd.
   *  Resolved — with a fallback to the skill's own on-disk directory — via
   *  {@link skillPluginRoot}; the substitution itself is done by
   *  `substitutePluginRoot` (hooks.ts). Already a {@link SAFE_SKILL_PROPERTIES}
   *  member (declarative metadata, not injectable auto-run code). */
  skillRoot?: string;
  /** Discriminator badging a workflow-backed command/skill in help/typeahead
   *  (parity with the reference's command `kind: 'workflow'`). Purely a classification
   *  surfaced by the listing; absent ⇒ a plain skill. Read via {@link skillKind}. */
  kind?: SkillKind;
  /** When true, the skill is UNAVAILABLE in a non-interactive (detached / `--prompt`)
   *  run (parity with the reference's command `disableNonInteractive`). An explicit
   *  {@link supportsNonInteractive} overrides it. Gated via
   *  {@link isSkillAvailableNonInteractive}. */
  disableNonInteractive?: boolean;
  /** Tri-state non-interactive opt-in (parity with the reference's command
   *  `supportsNonInteractive`). `false` ⇒ unavailable in a non-interactive run;
   *  `true` ⇒ always available regardless of {@link disableNonInteractive}; absent ⇒
   *  defer to {@link disableNonInteractive}. Gated via
   *  {@link isSkillAvailableNonInteractive}. */
  supportsNonInteractive?: boolean;
  /** Invocation-scoped lifecycle hooks declared in the skill's frontmatter (parity
   *  with the reference's PromptCommand `hooks` + registerFrontmatterHooks). When a
   *  FORKED skill carries these, they are registered on its sub-engine for the
   *  duration of the invocation and torn down when the fork finishes — letting an
   *  individual skill contribute e.g. a preToolUse guard scoped to its own run
   *  instead of the global/plugin-only hooks AMI previously supported. Parsed from
   *  SKILL.md frontmatter via {@link parseFrontmatterHooks} (hooks.ts) and stripped
   *  for untrusted (installed/plugin) tiers by the SkillManager, mirroring how a
   *  self-granted `auto-allow` permissionMode is refused. */
  hooks?: HookConfigEntry[];
  context?: 'inline' | 'fork'; // Execution mode: inline injects as system message, fork spawns subagent
  /** Named agent type a FORKED skill runs as (parity with the reference's PromptCommand
   *  `agent?: string`). When set, the forked sub-engine is bound to this agent's
   *  system prompt (and its model/maxTurns as a fallback when the skill does not
   *  override them) instead of a generic agent-agnostic sub-engine — so a fork can
   *  carry a specific persona/budget. Resolved via {@link SkillManagerLike.getAgent};
   *  an unknown agent name is a hard error at invocation. Ignored for inline skills. */
  agent?: string;
  disableModelInvocation?: boolean; // If true, LLM cannot invoke this skill via the skill tool
  effort?: string;         // Thinking effort level for sub-engine (e.g., "low", "medium", "high")
  /** Permission mode for this skill's FORKED sub-engine. Absent → the forked run
   *  defaults to 'ask' (parity with task.ts's sub-agent fallback) so forked-skill
   *  tool calls stay gated by the parent's deny/ask rules + prompt handler instead
   *  of the previous hard-coded 'auto-allow' bypass. A trusted skill may opt into
   *  'auto-allow' explicitly (mirrors AgentDefinition.permissionMode). A trusted
   *  skill may also pin the read-only 'plan' mode so its fork investigates without
   *  mutating. The valid set is {@link AGENT_PERMISSION_MODES}. */
  permissionMode?: 'auto-allow' | 'ask' | 'deny-all' | 'plan';

  // --- Stance capabilities (used when the skill is pinned as the session/run
  // stance via --skill) — a skill then governs the run the way a persona used to:
  // its body is the system-prompt overlay, plus the fields below. ---
  autoAllowPatterns?: string[];       // bash command globs auto-allowed while pinned
  mcpServers?: Record<string, McpServerConfig>; // MCP servers launched when pinned (e.g. kali)
  mcpAutoAllowPatterns?: string[];    // MCP tool-name globs auto-allowed while pinned
  mcpToolGuidance?: string;           // extra system-prompt guidance for the pinned MCP tools
  defaultThinkingLevel?: ThinkingLevel; // thinking level applied when pinned (unless user overrides)
}

/**
 * A skill is a *stance* when it carries stance capabilities — fields that change
 * the session's tools, permissions, MCP servers, or thinking level when the skill
 * is in effect (as opposed to a plain inline/fork skill or a workflow-doc skill
 * that only contributes instructions). Stance skills are the ones that can be
 * pinned via `--skill` OR dynamically activated by the model mid-session: when the
 * user's request matches a stance (e.g. "run the kali mcp to portscan" → pentest),
 * the engine switches into it, enabling its tooling (e.g. the Kali MCP server).
 *
 * This is the single discriminator that separates pinnable/activatable stances
 * (pentest, sre, research, swebench) from `disable-model-invocation` workflow docs
 * (review-changes-workflow, migration-workflow, …) that must NOT be activated as a
 * stance.
 */
export function isStanceSkill(skill: SkillDefinition): boolean {
  return !!(
    (skill.autoAllowPatterns && skill.autoAllowPatterns.length > 0) ||
    (skill.mcpServers && Object.keys(skill.mcpServers).length > 0) ||
    (skill.mcpAutoAllowPatterns && skill.mcpAutoAllowPatterns.length > 0) ||
    skill.defaultThinkingLevel
  );
}

/**
 * Structural view of the SkillManager (which lives in common/). The core skill
 * and task tools reach their manager through `ToolContext._skillManager`, so core
 * depends only on this shape — never on the common/ implementation. This keeps
 * the dependency direction (common → core) intact.
 */
export interface SkillManagerLike {
  getSkill(name: string): SkillDefinition | null;
  getSkillContent(name: string, args?: Record<string, string>): string | null;
  listSkills(): SkillDefinition[];
  getAgent(name: string): AgentDefinition | null;
  listAgents(): AgentDefinition[];
  findMatchingSkills?(filePaths: string[]): SkillDefinition[];
}

/**
 * True when a skill is currently available (parity with the reference's bundled skill
 * `isEnabled` + its env/feature-flag-gated registration). A skill is enabled when
 * BOTH gates pass: (1) its declarative {@link SkillDefinition.requiredEnv} — every
 * named env var is set non-empty (evaluated via {@link requiredEnvSatisfied}
 * against `env`, defaulting to `process.env`) — AND (2) its {@link
 * SkillDefinition.isEnabled} predicate (absent ⇒ enabled; a predicate that THROWS
 * is treated as DISABLED so a broken gate hides the skill rather than crashing the
 * loader/listing). Every call site (listings, context, the skill tool, agent
 * preload) therefore honors the env gate with no additional wiring.
 */
export function isSkillEnabled(
  skill: Pick<SkillDefinition, 'isEnabled' | 'requiredEnv'>,
  env: Record<string, string | undefined> = process.env,
): boolean {
  if (!requiredEnvSatisfied(skill.requiredEnv, env)) return false;
  if (!skill.isEnabled) return true;
  try {
    return skill.isEnabled() === true;
  } catch {
    return false;
  }
}

/**
 * Build the error message returned when a skill that declares
 * {@link SkillDefinition.requiresArgument} is invoked with no argument. Returns the
 * author's {@link SkillDefinition.missingArgumentMessage} verbatim when it is a
 * non-empty string; otherwise a generated usage line that includes the skill's
 * `argumentHint` when present (parity with the reference's required-argument skills, which
 * return a custom help/examples message, e.g. `/batch`).
 */
export function missingSkillArgumentMessage(
  skill: Pick<SkillDefinition, 'name' | 'argumentHint' | 'missingArgumentMessage'>,
): string {
  if (typeof skill.missingArgumentMessage === 'string' && skill.missingArgumentMessage.trim().length > 0) {
    return skill.missingArgumentMessage;
  }
  const hint = skill.argumentHint && skill.argumentHint.trim().length > 0
    ? ` (expected: ${skill.argumentHint.trim()})`
    : '';
  return `Skill "${skill.name}" requires an argument${hint}. Provide one and try again.`;
}

/**
 * Safely read a companion file bundled with a skill (parity with the reference's bundled
 * `SKILL_FILES` lookup). Returns the inlined content for `relPath` when it exists
 * in {@link SkillDefinition.files}, or `null` when there are no bundled files, the
 * path is not a string, the path is unsafe (absolute or containing `..`
 * traversal — rejected via {@link isSafeRelativePath}), or no such entry exists.
 * Never throws and never resolves outside the declared bundle.
 */
export function getSkillFile(
  skill: Pick<SkillDefinition, 'files'>,
  relPath: string,
): string | null {
  if (!skill.files || typeof relPath !== 'string') return null;
  const normalized = relPath.replace(/\\/g, '/').replace(/^\.\//, '');
  if (!isSafeRelativePath(normalized)) return null;
  const content = skill.files[normalized];
  return typeof content === 'string' ? content : null;
}

/** The entry's own trimmed {@link SkillDefinition.progressMessage} when it is a
 *  non-empty string, otherwise `undefined` — the shared declared-value lookup both
 *  {@link skillProgressMessage} and {@link progressMessageForKind} default from, so
 *  the two never diverge on what counts as an author-declared message. */
export function declaredProgressMessage(
  entry: Pick<SkillDefinition, 'progressMessage'>,
): string | undefined {
  return typeof entry.progressMessage === 'string' && entry.progressMessage.trim().length > 0
    ? entry.progressMessage.trim()
    : undefined;
}

/**
 * The progress/affordance message to show while a skill executes (parity with
 * the reference's command `progressMessage`). Returns the skill's own trimmed
 * {@link SkillDefinition.progressMessage} when it is a non-empty string, otherwise
 * {@link DEFAULT_SKILL_PROGRESS_MESSAGE}.
 */
export function skillProgressMessage(
  skill: Pick<SkillDefinition, 'progressMessage'>,
): string {
  return declaredProgressMessage(skill) ?? DEFAULT_SKILL_PROGRESS_MESSAGE;
}

/**
 * The progress/affordance message for a skill OR command entry, defaulting by its
 * KIND when no custom message is declared (parity with the reference's
 * loadPluginCommands `progressMessage: isSkill ? 'loading' : 'running'`). An
 * author-declared {@link SkillDefinition.progressMessage} always wins; otherwise a
 * skill-sourced entry (`isSkill = true`) falls back to `'loading'` and a
 * command-sourced entry to `'running'` — the single resolver the loader uses so the
 * two kinds advertise distinct default affordances.
 */
export function progressMessageForKind(
  entry: Pick<SkillDefinition, 'progressMessage'>,
  isSkill: boolean,
): string {
  return declaredProgressMessage(entry) ?? defaultProgressMessage(isSkill);
}

// ---------------------------------------------------------------------------
// Agent skill preloading — assemble the system-prompt block for the skills an
// agent definition declares via its `skills` allowlist (parity with the reference's
// runAgent skill preloading: inline each named skill's body into the spawned
// agent's context AND report which skills loaded vs. which names could not be
// resolved). Pure core helper; the task tool (core) wires it into the spawn path.
// ---------------------------------------------------------------------------

/** Heading introducing the inlined bodies of an agent's preloaded skills. */
export const PRELOADED_SKILLS_HEADING = '# Preloaded Skills';

/** Outcome of {@link buildPreloadedSkillsBlock}. */
export interface PreloadedSkillsResult {
  /** The assembled system-prompt block to append to the agent's prompt, or `''`
   *  when no requested skill resolved to enabled, non-empty content. */
  block: string;
  /** Canonical names of the skills that were preloaded, in request order (the
   *  "which skill is loading" metadata). */
  loaded: string[];
  /** Requested skill names that did NOT resolve to an enabled, non-empty skill
   *  (missing, disabled via {@link isSkillEnabled}, or blank-bodied) — surfaced as
   *  a load/spawn warning instead of silently dropped. */
  missing: string[];
}

// ---------------------------------------------------------------------------
// Skill-improvement suggestions — a structured descriptor for applying concrete,
// section-scoped edits to a named skill (parity with the reference's
// SkillImprovementSuggestion / SkillUpdate schema). Pure data + validation; the
// apply/survey flow (model call + file write) lives in the upper layers.
// ---------------------------------------------------------------------------

/**
 * One concrete, section-scoped edit to apply to a skill definition (parity with
 * the reference's `SkillUpdate`). `section` names the part of the skill to change,
 * `change` is the edit to integrate, and `reason` is the (optional) rationale.
 */
export interface SkillUpdate {
  section: string;
  change: string;
  reason: string;
}

/**
 * A structured suggestion to improve a named skill (parity with the reference's
 * `SkillImprovementSuggestion`): the target skill plus the list of edits to apply.
 */
export interface SkillImprovementSuggestion {
  skillName: string;
  updates: SkillUpdate[];
}

/**
 * Parse/normalize a raw (possibly model-produced or injected) value into a clean
 * list of {@link SkillUpdate}s. Accepts only an array of objects; each entry must
 * carry a non-empty `section` AND `change` (both trimmed) to be kept — entries
 * missing either are dropped so a malformed suggestion can never produce a
 * no-op/garbage edit. `reason` is optional and defaults to `''`.
 */
export function parseSkillUpdates(value: unknown): SkillUpdate[] {
  if (!Array.isArray(value)) return [];
  const out: SkillUpdate[] = [];
  for (const entry of value) {
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) continue;
    const e = entry as Record<string, unknown>;
    const section = typeof e.section === 'string' ? e.section.trim() : '';
    const change = typeof e.change === 'string' ? e.change.trim() : '';
    if (section.length === 0 || change.length === 0) continue;
    const reason = typeof e.reason === 'string' ? e.reason.trim() : '';
    out.push({ section, change, reason });
  }
  return out;
}

/**
 * True when `value` is a well-formed {@link SkillImprovementSuggestion}: a
 * non-empty `skillName` and at least one valid {@link SkillUpdate} (as decided by
 * {@link parseSkillUpdates}). Used to gate whether an improvement survey/apply
 * flow should run at all.
 */
export function isValidSkillImprovement(value: unknown): value is SkillImprovementSuggestion {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  const v = value as Record<string, unknown>;
  if (typeof v.skillName !== 'string' || v.skillName.trim().length === 0) return false;
  return parseSkillUpdates(v.updates).length > 0;
}

/**
 * Render a list of {@link SkillUpdate}s into the `- <section>: <change>` bullet
 * list the reference feeds the editor model when applying an improvement
 * (`applySkillImprovement`). Entries are normalized via {@link parseSkillUpdates}
 * first so invalid updates are excluded.
 */
export function formatSkillUpdates(updates: SkillUpdate[]): string {
  return parseSkillUpdates(updates)
    .map((u) => `- ${u.section}: ${u.change}`)
    .join('\n');
}

/**
 * The shells accepted in a skill/command's `shell:` frontmatter for `!`-block
 * execution (parity with the reference's `FrontmatterShell`).
 */
export const FRONTMATTER_SHELLS = ['bash', 'powershell'] as const;

export type FrontmatterShell = (typeof FRONTMATTER_SHELLS)[number];
