// Split from skill.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { dirname } from 'node:path';
import { type SkillDefinition, type SkillManagerLike, SAFE_SKILL_PROPERTIES } from '../skills';
import { normalizeToolName, wildcardMatch } from '../permissions';
import type { ToolCategory } from './tool-categories';

/**
 * The outcome of resolving a requested skill name against the manager.
 * - `skill`/`canonicalName` identify the resolved skill (the name the manager
 *   recognizes — pass it to `getSkillContent`).
 * - `ambiguous` lists the qualified candidates when an UNqualified request
 *   matches several directory/plugin-scoped skills and cannot be disambiguated.
 */
export interface SkillResolution {
  skill: SkillDefinition | null;
  canonicalName: string;
  ambiguous?: string[];
}

/**
 * Resolve a requested skill name to a registered skill, mirroring the reference's
 * Skill-tool name resolution as an additive superset:
 *
 * 1. **Exact match** — handles unscoped names and fully-qualified `plugin:skill`
 *    / directory-scoped `dir:name` forms that the manager stores verbatim.
 * 2. **Qualifier fallback** — a qualified request (`X:Y`) whose prefix is not
 *    registered falls back to the bare skill segment `Y` (so `web:deploy`
 *    resolves `deploy` when only the unscoped skill exists).
 * 3. **Most-specific-wins** — an UNqualified request `Y` resolves to a unique
 *    scoped variant `*:Y` when exactly one exists; when several scoped skills
 *    match it is reported as ambiguous so the caller can qualify it rather than
 *    silently guessing.
 *
 * Pure + exported for testability. Takes only the subset of the manager it needs.
 */
export function resolveSkillTarget(
  requested: string,
  manager: Pick<SkillManagerLike, 'getSkill' | 'listSkills'>,
): SkillResolution {
  const name = requested.trim().replace(/^\/+/, '');

  const exact = manager.getSkill(name);
  if (exact) return { skill: exact, canonicalName: name };

  if (name.includes(':')) {
    const bare = name.slice(name.lastIndexOf(':') + 1);
    if (bare && bare !== name) {
      const bareMatch = manager.getSkill(bare);
      if (bareMatch) return { skill: bareMatch, canonicalName: bare };
    }
    return { skill: null, canonicalName: name };
  }

  const suffix = `:${name}`;
  const scoped = manager.listSkills().filter(s => s.name.endsWith(suffix));
  if (scoped.length === 1) return { skill: scoped[0], canonicalName: scoped[0].name };
  if (scoped.length > 1) {
    return { skill: null, canonicalName: name, ambiguous: scoped.map(s => s.name) };
  }
  return { skill: null, canonicalName: name };
}

/** A permission rule keyed by tool name (the subset the skill gate consults). */
export type SkillPermissionRule = { tool: string; pattern?: string; action: 'allow' | 'deny' | 'ask' };

/** The per-skill permission decision: an EXPLICIT operator rule matched the skill
 *  name, or `undefined` when no `Skill(...)` rule applies (fall through to the
 *  generic skill-tool permission gate, which still enforces the default-ask). */
export const SKILL_PERMISSION_DECISIONS = ['allow', 'deny', 'ask'] as const;

export type SkillPermissionDecision = (typeof SKILL_PERMISSION_DECISIONS)[number];

/** Does a `Skill(...)` rule's pattern match this skill name? A rule with no
 *  pattern (`{tool:'skill', action:'deny'}`) matches EVERY skill; otherwise the
 *  pattern is a glob so exact names and `name:*` prefix scopes both work. Reuses
 *  the canonical {@link wildcardMatch} — no local matcher. */
export function skillRuleMatches(pattern: string | undefined, skillName: string): boolean {
  if (pattern === undefined || pattern === '') return true;
  return wildcardMatch(pattern, skillName);
}

/**
 * Evaluate per-skill permission rules keyed by skill NAME, mirroring the reference's
 * SkillTool.checkPermissions (deny rules first, then allow rules, with exact and
 * `name:*` prefix matching). This is the per-skill deny/allow/ask surface AMI
 * previously lacked — an operator can now `deny: ["Skill(dangerous)"]` or scope
 * `allow: ["Skill(project:*)"]` which model-invokable skills may auto-run.
 *
 * Returns `undefined` when NO `Skill(...)` rule matches, so the default behavior is
 * unchanged: the generic skill-tool permission gate (the `skill` tool is
 * non-read-only) still provides the reference's default-ask. Only an explicit matching
 * rule overrides that. Pure + exported for testability.
 */
export function evaluateSkillPermission(
  skillName: string,
  rules: ReadonlyArray<SkillPermissionRule> | undefined,
): SkillPermissionDecision | undefined {
  if (!rules || rules.length === 0) return undefined;
  const skillRules = rules.filter(r => normalizeToolName(r.tool) === 'skill');
  if (skillRules.length === 0) return undefined;
  // Deny wins over allow wins over ask (defense-in-depth, the reference's order).
  for (const order of ['deny', 'allow', 'ask'] as const) {
    for (const r of skillRules) {
      if (r.action === order && skillRuleMatches(r.pattern, skillName)) return order;
    }
  }
  return undefined;
}

/**
 * Return the skill's property keys that are OUTSIDE {@link SAFE_SKILL_PROPERTIES}
 * AND carry a meaningful value (not undefined/null, not an empty array/object).
 * An empty result means the skill uses only reviewed-safe properties and may
 * auto-run. Mirrors the reference's `skillHasOnlySafeProperties` value check.
 * Pure + exported for testability.
 */
export function unsafeSkillProperties(skill: SkillDefinition): string[] {
  const offending: string[] = [];
  for (const key of Object.keys(skill)) {
    if (SAFE_SKILL_PROPERTIES.has(key)) continue;
    const value = (skill as unknown as Record<string, unknown>)[key];
    if (value === undefined || value === null) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0) continue;
    offending.push(key);
  }
  return offending;
}

/** True when the skill carries only reviewed-safe properties (auto-run eligible). */
export function skillHasOnlySafeProperties(skill: SkillDefinition): boolean {
  return unsafeSkillProperties(skill).length === 0;
}

/**
 * A diagnostic produced while resolving a skill's tool restriction list — surfaced
 * so a misconfigured `allowed-tools`/`disallowed-tools` (a typo, a disconnected MCP
 * tool, or a whitelist that resolves to nothing) is VISIBLE instead of silently
 * degrading the subagent's toolset. Mirrors the reference's validation/selected-count
 * affordance (ToolSelector drops unresolved names but still surfaces the count).
 */
export interface SkillToolFilterWarning {
  kind: 'unknown-tool' | 'empty-toolset';
  /** The offending allow/deny entry (verbatim), when `kind === 'unknown-tool'`. */
  entry?: string;
  /** Which restriction list the entry came from. */
  list?: 'allowed' | 'disallowed';
  message: string;
}

/** The parsed form of a single allow/deny list entry: either a tool-CLASS token
 *  (expands to every tool in that category) or a single normalized tool NAME. */
export type ParsedSkillToolEntry = { category: ToolCategory } | { name: string };

/** Sentinel `filePath` values that are NOT real on-disk locations (built-ins and
 *  in-memory/test skills), so no base-directory pointer can be derived from them. */
export function isRealSkillPath(filePath: string | undefined): filePath is string {
  return !!filePath && !filePath.startsWith('<') && filePath.includes('/');
}

/**
 * The directory a disk-based skill lives in — where its sibling reference files
 * (scripts, templates, docs the SKILL.md refers to) sit — or `null` for a built-in
 * or in-memory skill that ships no on-disk tree. Parity with the reference's automatic
 * "Base directory for this skill" pointer. Pure + exported for testability.
 */
export function skillBaseDir(skill: SkillDefinition | null | undefined): string | null {
  const fp = skill?.filePath;
  if (!isRealSkillPath(fp)) return null;
  const dir = dirname(fp);
  return dir && dir !== '.' ? dir : null;
}

/**
 * Prepend a "Base directory for this skill: <dir>" pointer to a disk-based skill's
 * content so the model can Read/Grep the skill's sibling reference files on demand
 * without the author having to template `${SKILL_DIR}` by hand (parity with the reference
 * Code's bundled/disk skill base-dir injection). No-op for built-in/in-memory
 * skills and idempotent (never double-prepends). Pure + exported for testability.
 */
export function withSkillBaseDir(content: string, skill: SkillDefinition | null | undefined): string {
  const dir = skillBaseDir(skill);
  if (!dir || content.includes('Base directory for this skill:')) return content;
  return `Base directory for this skill: ${dir}\n\n${content}`;
}

/**
 * Build the skill-activation byline, surfacing the sandboxing a skill applies at the
 * moment it activates (parity with the reference's renderToolResultMessage byline): the
 * number of tools the skill is scoped to (when it restricts its toolset) and the
 * model it is pinned to (when non-default). A `·`-joined one-liner; the base message
 * is always first. Pure + exported for testability.
 */
export function skillActivationByline(
  base: string,
  allowedToolCount: number | undefined,
  model: string | undefined,
): string {
  const parts = [base];
  if (allowedToolCount !== undefined) {
    parts.push(`${allowedToolCount} tool${allowedToolCount === 1 ? '' : 's'} allowed`);
  }
  if (model) parts.push(`model ${model}`);
  return parts.join(' · ');
}

/** Hard cap on how much forked-skill output is echoed back into the context. */
export const SKILL_MAX_RESULT_CHARS = 100_000;

export function capSkillOutput(text: string): string {
  if (text.length <= SKILL_MAX_RESULT_CHARS) return text;
  return text.slice(0, SKILL_MAX_RESULT_CHARS) + '\n[... skill output truncated ...]';
}
