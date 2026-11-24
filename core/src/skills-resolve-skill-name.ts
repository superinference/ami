// Split from skills.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { estimateTokens } from './utils/tokens';
import { parseMemoryScope } from './skills-validate-skill-args';
import type { AgentMemoryScope, SkillKind } from './skills-validate-skill-args';
import { REDACTED_SKILL_ARGS } from './skills-agent-definition';
import type { AgentDefinition } from './skills-agent-definition';
import { FRONTMATTER_SHELLS, PRELOADED_SKILLS_HEADING, isSkillEnabled } from './skills-skill-definition';
import type { FrontmatterShell, PreloadedSkillsResult, SkillDefinition } from './skills-skill-definition';

// ---------------------------------------------------------------------------
// Dynamic prompt / companion-file / progress-message resolution — building
// blocks that let a skill compute its body at invocation time, ship supporting
// resource files, and advertise an in-progress affordance. Pure core helpers;
// the SkillManager (common/) and the skill tool wire them into the content path.
// ---------------------------------------------------------------------------

/**
 * Resolve a skill's prompt/body for a given argument string (parity with the reference's
 * bundled-skill `getPromptForCommand(args)`). When the skill declares a
 * {@link SkillDefinition.getPromptForCommand} builder, it is invoked (awaited) with
 * the raw `args` string and its return value becomes the body — allowing per-call
 * validation, lazy content loading, or environment branching. A builder that
 * returns a non-string (malformed) yields `''`.
 *
 * GRACEFUL DEGRADATION (parity with the reference's bundled claude-api skill, whose
 * dynamic prompt builder wraps its project-environment detection in a try/catch and
 * falls back to the static reference when detection throws): when the builder THROWS
 * — e.g. its environment/feature detection fails — the static
 * {@link SkillDefinition.content} is returned instead of rejecting, so a broken
 * dynamic prompt degrades to the author's fallback body rather than crashing the
 * skill-content path. With no builder, the static content is returned unchanged.
 */
export async function resolveSkillPrompt(
  skill: Pick<SkillDefinition, 'content' | 'getPromptForCommand' | 'templateVars'>,
  args: string = '',
): Promise<string> {
  if (typeof skill.getPromptForCommand === 'function') {
    try {
      const out = await skill.getPromptForCommand(args);
      return applySkillTemplateVariables(typeof out === 'string' ? out : '', skill.templateVars);
    } catch {
      // Builder failed (e.g. environment detection threw) — fall back to the static
      // body so the skill still resolves to its author-provided default.
      return applySkillTemplateVariables(skill.content, skill.templateVars);
    }
  }
  return applySkillTemplateVariables(skill.content, skill.templateVars);
}

/**
 * Display-safe form of a skill's raw argument string. When the skill declares
 * {@link SkillDefinition.isSensitive} and `rawArgs` is a non-empty string, returns
 * {@link REDACTED_SKILL_ARGS} so the arguments are never echoed into a permission
 * prompt, transcript, or history (parity with the reference's `isSensitive` arg masking).
 * A non-sensitive skill — or an empty/whitespace argument — returns `rawArgs`
 * unchanged.
 */
export function redactSkillArgs(
  skill: Pick<SkillDefinition, 'isSensitive'>,
  rawArgs: string,
): string {
  if (skill.isSensitive && typeof rawArgs === 'string' && rawArgs.trim().length > 0) {
    return REDACTED_SKILL_ARGS;
  }
  return rawArgs;
}

/**
 * True when a skill is HIDDEN from user-facing help/typeahead listings (parity
 * with the reference's command `isHidden`). A hidden skill stays invocable by its exact
 * name; it is only omitted from catalog listings such as `/skills`.
 */
export function isSkillHidden(skill: Pick<SkillDefinition, 'isHidden'>): boolean {
  return skill.isHidden === true;
}

/**
 * The name to DISPLAY for a skill in listings (parity with the reference's command
 * `userFacingName()`): the trimmed {@link SkillDefinition.userFacingName} when it
 * is a non-empty string, otherwise the canonical {@link SkillDefinition.name}.
 */
export function skillUserFacingName(
  skill: Pick<SkillDefinition, 'name' | 'userFacingName'>,
): string {
  if (typeof skill.userFacingName === 'string' && skill.userFacingName.trim().length > 0) {
    return skill.userFacingName.trim();
  }
  return skill.name;
}

/**
 * Whether a skill may be invoked in the CURRENT interactivity mode (parity with
 * the reference's command `disableNonInteractive` / `supportsNonInteractive` gating). In
 * an interactive run every skill is available. In a non-interactive (detached /
 * `--prompt`) run a skill is unavailable when it declares
 * `supportsNonInteractive === false`, OR it declares `disableNonInteractive` and
 * does not explicitly opt back in via `supportsNonInteractive === true`.
 */
export function isSkillAvailableNonInteractive(
  skill: Pick<SkillDefinition, 'disableNonInteractive' | 'supportsNonInteractive'>,
  nonInteractive: boolean,
): boolean {
  if (!nonInteractive) return true;
  if (skill.supportsNonInteractive === true) return true;
  if (skill.supportsNonInteractive === false) return false;
  return skill.disableNonInteractive !== true;
}

/**
 * Resolve the base directory a skill exports to its `!`-blocks / hooks as
 * `CLAUDE_PLUGIN_ROOT` (parity with the reference's PromptCommand `skillRoot`). Returns
 * the skill's explicit {@link SkillDefinition.skillRoot} when it is a non-empty
 * string; otherwise falls back to the directory of its real on-disk
 * {@link SkillDefinition.filePath} (a `<builtin>`/in-memory sentinel or a bare
 * filename yields no directory). Returns `undefined` when neither is available, so
 * a caller can skip the `${CLAUDE_PLUGIN_ROOT}` substitution. Never throws.
 */
export function skillPluginRoot(
  skill: Pick<SkillDefinition, 'skillRoot' | 'filePath'>,
): string | undefined {
  if (typeof skill.skillRoot === 'string' && skill.skillRoot.trim().length > 0) {
    return skill.skillRoot.trim();
  }
  const fp = skill.filePath;
  if (typeof fp !== 'string' || fp.startsWith('<') || !fp.includes('/')) return undefined;
  const slash = fp.lastIndexOf('/');
  const dir = fp.slice(0, slash);
  return dir.length > 0 ? dir : undefined;
}

/**
 * The classification of a command/skill for help/typeahead badging (parity with
 * the reference's command `kind`). Returns the declared {@link SkillDefinition.kind}
 * (currently only `'workflow'`) or `undefined` for a plain skill.
 */
export function skillKind(skill: Pick<SkillDefinition, 'kind'>): SkillKind | undefined {
  return skill.kind;
}

/**
 * Estimate the context-budget cost (in tokens) of a skill's FRONTMATTER only —
 * name + description + whenToUse (parity with the reference's
 * `estimateSkillFrontmatterTokens`). The full body is loaded only on invocation,
 * so the listing cost is just the always-present metadata. Reuses the canonical
 * {@link estimateTokens} heuristic so there is a single token estimator.
 */
export function estimateSkillFrontmatterTokens(
  skill: Pick<SkillDefinition, 'name' | 'description' | 'whenToUse'>,
): number {
  const text = [skill.name, skill.description, skill.whenToUse]
    .filter((v): v is string => typeof v === 'string' && v.length > 0)
    .join(' ');
  return estimateTokens(text);
}

/**
 * Build the preloaded-skills prompt block for an agent's `skills` allowlist
 * (parity with the reference's runAgent skill preloading). For each requested name,
 * `resolve` is consulted (the SkillManager's name/alias lookup); a skill that
 * resolves, is {@link isSkillEnabled}, and has a non-empty body is inlined under a
 * `## <name>` sub-heading, and its canonical name recorded in `loaded`. Any name
 * that does not resolve to such a skill is recorded in `missing`. The returned
 * `block` is empty when nothing loaded so the caller appends nothing. Duplicate
 * requested names resolve once each (the caller's allowlist is already de-duped by
 * {@link parseAgentSkills}); a skill is never inlined twice for the same request.
 */
export function buildPreloadedSkillsBlock(
  skillNames: readonly string[],
  resolve: (name: string) => SkillDefinition | null,
): PreloadedSkillsResult {
  const loaded: string[] = [];
  const missing: string[] = [];
  const parts: string[] = [];
  const seen = new Set<string>();
  for (const name of skillNames) {
    const skill = resolve(name);
    const body = typeof skill?.content === 'string' ? skill.content.trim() : '';
    if (skill && isSkillEnabled(skill) && body.length > 0) {
      if (seen.has(skill.name)) continue; // never inline the same skill twice
      seen.add(skill.name);
      loaded.push(skill.name);
      parts.push(`## ${skill.name}\n\n${body}`);
    } else {
      missing.push(name);
    }
  }
  const block = parts.length > 0 ? `${PRELOADED_SKILLS_HEADING}\n\n${parts.join('\n\n')}` : '';
  return { block, loaded, missing };
}

/**
 * Resolve a requested skill name against a candidate set using the SAME ordered,
 * multi-strategy lookup the reference's runAgent `resolveSkillName` uses for agent
 * `skills:` frontmatter preloading — so a plugin-namespaced skill still resolves
 * when its bare name is unavailable (e.g. taken by another plugin's skill of the
 * same name, so only the `plugin:name` form is registered):
 *
 *   1. EXACT match on a candidate's canonical `name`, its {@link
 *      SkillDefinition.userFacingName}, or one of its {@link SkillDefinition.aliases}.
 *   2. PLUGIN-QUALIFIED match: when `opts.pluginPrefix` is given, try the
 *      fully-qualified `${pluginPrefix}:${requested}` form against name/aliases
 *      (an agent preferring skills from its own plugin).
 *   3. SUFFIX match on `:${requested}` against a candidate's name or an alias — a
 *      plugin-namespaced skill (`someplugin:requested`) whose bare name is not
 *      registered.
 *
 * Earlier strategies win; within a strategy the first matching candidate (in the
 * provided order) wins. Matching is exact/case-sensitive, mirroring how skill
 * names are registered. A blank/non-string request, or no match, yields `null`.
 * This is the WHICH-skill resolver; {@link buildPreloadedSkillsBlock} then decides
 * whether the resolved skill is enabled and non-empty before inlining it.
 */
export function resolveSkillName(
  requested: string,
  candidates: readonly SkillDefinition[],
  opts?: { pluginPrefix?: string },
): SkillDefinition | null {
  if (typeof requested !== 'string') return null;
  const want = requested.trim();
  if (want.length === 0) return null;

  // Strategy 1 — exact name / userFacingName / alias.
  for (const c of candidates) {
    if (
      c.name === want ||
      c.userFacingName === want ||
      (c.aliases?.includes(want) ?? false)
    ) {
      return c;
    }
  }

  // Strategy 2 — fully-qualified with the agent's plugin prefix.
  const prefix = opts?.pluginPrefix?.trim();
  if (prefix) {
    const qualified = `${prefix}:${want}`;
    for (const c of candidates) {
      if (c.name === qualified || (c.aliases?.includes(qualified) ?? false)) {
        return c;
      }
    }
  }

  // Strategy 3 — suffix match on ":requested" (plugin-namespaced skill).
  const suffix = `:${want}`;
  for (const c of candidates) {
    if (c.name.endsWith(suffix) || (c.aliases?.some((a) => a.endsWith(suffix)) ?? false)) {
      return c;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Additive skill/agent config knobs + display formatters — pure core building
// blocks mirroring the reference capabilities AMI's skills library was missing:
// immediate-queue flag, per-file shell selection, a human tool-name joiner, a
// progressive argument hint, an admin-trusted source gate, author template
// variables, and the built-in-agent discriminator. The SkillManager (common/),
// task tool, and `/skills`/`/agents` listings wire these into the real paths.
// ---------------------------------------------------------------------------

/**
 * True when a skill/command declares {@link SkillDefinition.immediate} (parity
 * with the reference's command `immediate`): it should bypass the message queue and run
 * at once rather than waiting for the next stop point. Only an explicit `true`
 * counts — a truthy-but-not-`true` value never silently jumps the queue.
 */
export function isSkillImmediate(skill: Pick<SkillDefinition, 'immediate'>): boolean {
  return skill.immediate === true;
}

/** The shell used when a skill/command declares none (parity with the reference, which
 *  defaults `!`-block execution to bash). */
export const DEFAULT_SHELL: FrontmatterShell = 'bash';

/**
 * Parse + validate a raw frontmatter `shell:` value (parity with the reference's
 * `parseShellFrontmatter`). Absent/null/empty ⇒ `undefined` (the caller defaults
 * to bash via {@link shellForExecution}); a recognized value (case-insensitive,
 * trimmed) ⇒ that shell; an unrecognized value ⇒ `undefined` so a garbled shell
 * silently falls back to bash rather than failing the skill load.
 */
export function parseShellFrontmatter(value: unknown): FrontmatterShell | undefined {
  if (value === undefined || value === null) return undefined;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === '') return undefined;
  return (FRONTMATTER_SHELLS as readonly string[]).includes(normalized)
    ? (normalized as FrontmatterShell)
    : undefined;
}

/**
 * Resolve the shell a skill/command's `!`-blocks execute under, applying the
 * {@link DEFAULT_SHELL} fallback when none was declared (parity with the reference's
 * caller-side bash default).
 */
export function shellForExecution(shell: FrontmatterShell | undefined): FrontmatterShell {
  return shell ?? DEFAULT_SHELL;
}

/**
 * Format a tool-name list for human display (parity with the reference's
 * `getToolsDisplay`, ConfirmStep): `undefined` ⇒ `'All tools'` (unset means no
 * restriction), `[]` ⇒ `'None'`, one name ⇒ that name (or `'None'` when blank), a
 * pair ⇒ `'a and b'`, and three+ ⇒ an Oxford-comma list `'a, b, and c'`.
 */
export function formatToolNameList(toolNames: readonly string[] | undefined): string {
  if (toolNames === undefined) return 'All tools';
  if (toolNames.length === 0) return 'None';
  if (toolNames.length === 1) return toolNames[0] || 'None';
  if (toolNames.length === 2) return toolNames.join(' and ');
  return `${toolNames.slice(0, -1).join(', ')}, and ${toolNames[toolNames.length - 1]}`;
}

/**
 * Compute a progressive argument hint showing only the arg names NOT yet filled
 * (parity with the reference's `generateProgressiveArgumentHint`): for `argNames` and
 * the args typed so far, returns e.g. `'[arg2] [arg3]'`, or `undefined` once
 * every positional arg is filled (or over-filled) so the hint disappears.
 */
export function progressiveArgumentHint(
  argNames: readonly string[],
  typedArgs: readonly string[],
): string | undefined {
  const remaining = argNames.slice(typedArgs.length);
  if (remaining.length === 0) return undefined;
  return remaining.map((name) => `[${name}]`).join(' ');
}

/**
 * Substitute author-defined `{{key}}` template variables into a skill body
 * (parity with the reference's bundled-skill `SKILL_MODEL_VARS` substitution). Every
 * `{{key}}` whose `key` is present in `vars` is replaced with its value (an
 * empty-string mapping is honored); an unknown token is left intact. With no
 * `vars` the content is returned unchanged. This is DISTINCT from the invocation
 * `$name`/`$ARGUMENTS` positional substitution (owned by the SkillManager): it is
 * a skill-owned, centrally-maintained wording map, not user-supplied args.
 */
export function applySkillTemplateVariables(
  content: string,
  vars: Record<string, string> | undefined,
): string {
  if (!vars || typeof content !== 'string') return content;
  return content.replace(/\{\{(\w+)\}\}/g, (match, key: string) =>
    Object.prototype.hasOwnProperty.call(vars, key) ? vars[key] : match,
  );
}

/**
 * True when an agent definition is a BUILT-IN agent (parity with the reference's
 * `isBuiltIn = source === 'built-in'`). The discriminator that gates built-in
 * vs. custom treatment — e.g. editability of the definition and the extra
 * custom-agent tool-denylist that applies only to non-built-in agents.
 */
export function isBuiltInAgent(agent: Pick<AgentDefinition, 'source'>): boolean {
  return agent.source === 'built-in';
}

/**
 * Detect a mismatch between an agent's on-disk {@link AgentDefinition.fileName} and
 * its resolved canonical {@link AgentDefinition.name} (parity with the reference's
 * agentFileUtils filename-vs-agentType handling). The extension is stripped from the
 * filename before comparison. Returns the filename-derived type (the base name) when
 * it differs from `name`, so the loader can surface an actionable "file X declares
 * agent Y" warning instead of silently masking the divergence; returns `null` when
 * there is no filename, the filename has no base, or the two already agree. Never
 * throws.
 */
export function agentFileNameMismatch(
  agent: Pick<AgentDefinition, 'name' | 'fileName'>,
): string | null {
  const fn = agent.fileName;
  if (typeof fn !== 'string' || fn.trim().length === 0) return null;
  // Strip a single trailing extension (`.md`), keeping the base name's casing.
  const base = fn.replace(/\.[^./\\]+$/, '').trim();
  if (base.length === 0 || base === agent.name) return null;
  return base;
}

// ---------------------------------------------------------------------------
// Agent-listing display affordances — surface a sub-agent's persistent-memory
// scope and its scoped skill allowlist in the `/agents` listing (parity with
// the reference's `formatAgent` memory dimension + AgentDetail `getMemoryScopeDisplay`
// / skills-list collapse). Pure core formatters; the `/agents` slash-command data
// already carries `memory` + `skills`, and the CLI listing renderer wires these in.
// ---------------------------------------------------------------------------

/**
 * Human-readable label for an agent's persistent-memory {@link AgentMemoryScope}
 * (parity with the reference's `getMemoryScopeDisplay`): `user` / `project` / `local`
 * each map to a descriptive, location-aware phrase. Returns `undefined` for an
 * unset/unknown scope so a memory-less agent surfaces no memory affordance.
 */
export function agentMemoryScopeDisplay(scope: AgentMemoryScope | undefined): string | undefined {
  switch (scope) {
    case 'user': return 'User (shared across all projects)';
    case 'project': return 'Project (shared with your team)';
    case 'local': return 'Local (this project only, not shared)';
    default: return undefined;
  }
}

/**
 * The short memory dimension appended to an agent's listing line (parity with
 * the reference's `formatAgent`, which appends `${agent.memory} memory` only when the
 * agent declares a memory scope). Returns e.g. `'project memory'`, or `''` when the
 * agent declares no (or an unrecognized) memory scope so no dangling affordance is
 * shown.
 */
export function formatAgentMemoryDimension(agent: Pick<AgentDefinition, 'memory'>): string {
  const scope = parseMemoryScope(agent.memory);
  return scope ? `${scope} memory` : '';
}
