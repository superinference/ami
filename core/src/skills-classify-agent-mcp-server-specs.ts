// Split from skills.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { McpServerConfig } from './mcp/manager';
import type { AgentDefinition } from './skills-agent-definition';
import { applySkillTemplateVariables } from './skills-resolve-skill-name';

/**
 * Resolve the model string to DISPLAY for an agent row/detail (parity with the reference
 * Code's `resolveAgentModelDisplay`, agentDisplay.ts): the agent's own declared
 * {@link AgentDefinition.model} when set, otherwise the `defaultModel` the caller
 * supplies as the effective subagent default (AMI's equivalent of the reference's
 * `getDefaultSubagentModel()` — typically the session's current model a
 * no-model/inheriting subagent runs under). The literal `'inherit'` sentinel is
 * surfaced verbatim (so the listing can say an agent inherits the parent model
 * rather than printing a concrete name), and a blank/absent result yields
 * `undefined` so the row shows no `· <model>` affordance at all. Pure: the caller
 * passes the default rather than core reaching into session config.
 */
export function resolveAgentModelDisplay(
  agent: Pick<AgentDefinition, 'model'>,
  defaultModel?: string,
): string | undefined {
  const raw = typeof agent.model === 'string' && agent.model.trim().length > 0
    ? agent.model.trim()
    : (typeof defaultModel === 'string' && defaultModel.trim().length > 0 ? defaultModel.trim() : undefined);
  if (raw === undefined) return undefined;
  return raw === 'inherit' ? 'inherit' : raw;
}

/**
 * Collapse an agent's scoped-skills allowlist for display (parity with the reference
 * Code's AgentDetail skills list: `skills.length > 10 ? '${n} skills' :
 * skills.join(', ')`). An agent that declares many skills shows a `'N skills'`
 * count instead of an overwhelming comma list; `undefined`/empty yields `''`.
 */
export function formatAgentSkillsList(skills: readonly string[] | undefined): string {
  if (!skills || skills.length === 0) return '';
  if (skills.length > 10) return `${skills.length} skills`;
  return skills.join(', ');
}

// ---------------------------------------------------------------------------
// Relevance-filtered bundled-doc inlining — select only the companion docs that
// match a detected language (plus shared docs) and splice them into a skill's
// context as deterministically-ordered `<doc path="…">…</doc>` blocks (parity
// with the reference's bundled-skill `getFilesForLanguage` + `buildInlineReference`,
// e.g. the claude-api skill). Pure core building blocks over {@link
// SkillDefinition.files}; avoids injecting every bundled doc verbatim.
// ---------------------------------------------------------------------------

/**
 * Select the subset of a skill's bundled {@link SkillDefinition.files} keys that
 * are relevant to a detected language (parity with the reference's
 * `getFilesForLanguage`): a file is relevant when its key is under `${lang}/` or
 * the always-shared `shared/` prefix. Returns `[]` for no files or a blank
 * language. Order follows the files map's own key order (callers sort when
 * inlining via {@link buildInlineReference}).
 */
export function getFilesForLanguage(
  files: Record<string, string> | undefined,
  lang: string,
): string[] {
  if (!files || typeof lang !== 'string' || lang.trim().length === 0) return [];
  const prefix = `${lang.trim()}/`;
  return Object.keys(files).filter((p) => p.startsWith(prefix) || p.startsWith('shared/'));
}

/** Strip HTML comments from markdown, nesting-safe (parity with the reference's
 *  `processContent` comment pass). Looped so a `<!-- a <!-- b --> c -->` resolves. */
export function stripHtmlComments(md: string): string {
  let out = md;
  let prev: string;
  do {
    prev = out;
    out = out.replace(/<!--[\s\S]*?-->\n?/g, '');
  } while (out !== prev);
  return out;
}

/**
 * Assemble the inline-reference block for a set of a skill's bundled-doc keys
 * (parity with the reference's `buildInlineReference`). Keys are sorted
 * deterministically; each existing file's content is comment-stripped, has its
 * `{{key}}` author template variables applied (via
 * {@link applySkillTemplateVariables}), trimmed, and wrapped in a
 * `<doc path="…">…</doc>` tag. A requested key with no matching file is skipped
 * (never emits an empty tag). Returns `''` when nothing resolved.
 */
export function buildInlineReference(
  files: Record<string, string> | undefined,
  filePaths: readonly string[],
  templateVars?: Record<string, string>,
): string {
  if (!files) return '';
  const sections: string[] = [];
  for (const filePath of [...filePaths].sort()) {
    const md = files[filePath];
    if (typeof md !== 'string') continue; // skip missing keys
    const processed = applySkillTemplateVariables(stripHtmlComments(md), templateVars).trim();
    sections.push(`<doc path="${filePath}">\n${processed}\n</doc>`);
  }
  return sections.join('\n\n');
}

// ---------------------------------------------------------------------------
// Required-MCP-server readiness — the pure decision a subagent-spawn precondition
// loop needs (parity with the reference's AgentTool requiredMcpServers gate, which
// polls pending servers, early-exits on a failed required server, and errors when
// a required server is missing). The polling/timeout loop lives in the task tool;
// this classifies the CURRENT server-state snapshot so the loop can decide to
// proceed, keep waiting, or abort.
// ---------------------------------------------------------------------------

/** One required-MCP-server readiness classification over a state snapshot. */
export interface McpServerReadiness {
  /** True when every required server is connected (safe to spawn). */
  ready: boolean;
  /** Required patterns whose matching server(s) are still connecting — the loop
   *  should keep polling while these remain. */
  pending: string[];
  /** Required patterns whose every matching server has failed — the loop should
   *  early-exit (no point waiting). */
  failed: string[];
  /** Required patterns with no matching server at all — an actionable "missing
   *  required MCP server" error. */
  missing: string[];
}

/**
 * Classify a required-MCP-server precondition against a snapshot of live server
 * states (parity with the reference's AgentTool requiredMcpServers gate). Each
 * required entry is matched case-insensitively as a substring against the server
 * names (consistent with {@link hasRequiredMcpServers}); a requirement is
 * satisfied when ANY matching server is `'connected'`, `failed` when EVERY match
 * has `'failed'`, `missing` when no server matches, otherwise `pending`. With no
 * requirements the result is immediately {@link McpServerReadiness.ready}.
 */
export function requiredMcpServerReadiness(
  required: readonly string[] | undefined,
  serverStates: Readonly<Record<string, string>>,
): McpServerReadiness {
  const pending: string[] = [];
  const failed: string[] = [];
  const missing: string[] = [];
  if (!required || required.length === 0) {
    return { ready: true, pending, failed, missing };
  }
  const entries = Object.entries(serverStates);
  for (const pattern of required) {
    const pat = pattern.toLowerCase();
    const matches = entries.filter(([name]) => name.toLowerCase().includes(pat));
    if (matches.length === 0) { missing.push(pattern); continue; }
    if (matches.some(([, state]) => state === 'connected')) continue; // satisfied
    if (matches.every(([, state]) => state === 'failed')) { failed.push(pattern); continue; }
    pending.push(pattern); // at least one still connecting
  }
  const ready = pending.length === 0 && failed.length === 0 && missing.length === 0;
  return { ready, pending, failed, missing };
}

// ---------------------------------------------------------------------------
// Per-agent MCP server specs — classify an agent's frontmatter `mcpServers` into
// the REFERENCED (by name, shared with the parent, NOT torn down) vs INLINE
// (agent-specific single-key object, connected fresh and torn down on finish)
// halves (parity with the reference's AgentTool `initializeAgentMcpServers`, which
// per spec either looks up an existing config by name — a memoized/shared client —
// or treats a single-key `{ [name]: config }` object as a newly-created client that
// is the ONLY kind cleaned up when the agent completes). Agent MCP servers are
// ADDITIVE to the parent's clients. Pure core building block over AMI's
// {@link McpServerConfig}; the task tool's agent-spawn path wires the referenced
// lookup + inline connect/cleanup from this classification.
// ---------------------------------------------------------------------------

/**
 * One agent-frontmatter MCP server spec (parity with the reference's
 * `AgentDefinition.mcpServers` element): either a STRING that REFERENCES an
 * already-configured server by name (shared with the parent context, memoized, and
 * NOT torn down when the agent finishes), or an INLINE single-key
 * `{ [name]: config }` object (an agent-specific server connected fresh for the
 * agent and torn down when it finishes).
 */
export type AgentMcpServerSpec = string | Record<string, McpServerConfig>;

/** The raw frontmatter value an agent's `mcpServers`/`mcp-servers` may carry: the
 *  existing AMI inline map form (`{ name: config }`), OR the reference's array of
 *  referenced-by-name strings and/or inline single-key objects. */
export type RawAgentMcpServers =
  | Record<string, McpServerConfig>
  | AgentMcpServerSpec[]
  | undefined;

/** Structured classification of an agent's MCP server specs. */
export interface ClassifiedAgentMcpServers {
  /** Server names referenced by STRING — resolved from the parent/global MCP config,
   *  shared (memoized) with the parent, and NOT cleaned up when the agent finishes.
   *  In input order, de-duplicated. */
  referenced: string[];
  /** INLINE agent-specific servers (single-key objects, or the inline-map form's
   *  entries) — connected fresh for the agent and torn down when it finishes. In
   *  input order; a later duplicate server name replaces an earlier one (last-wins). */
  inline: Array<{ name: string; config: McpServerConfig }>;
  /** Specs that could not be classified (a blank/whitespace reference name, an empty
   *  or multi-key inline object, or a non-string/non-object element) paired with a
   *  human-readable reason — surfaced as a load/spawn warning instead of silently
   *  connecting a garbage server. */
  invalid: Array<{ index: number; reason: string }>;
}

/**
 * Classify an agent's frontmatter MCP server specs into the referenced vs inline
 * halves (parity with the reference's `initializeAgentMcpServers` per-spec handling).
 *
 * Accepts both the existing AMI INLINE-MAP form (`{ name: config }` — every entry is
 * treated as an inline, agent-specific server) and the reference's ARRAY form (each
 * element a referenced-by-name string or a single-key inline object), so a loader can
 * normalize either shape through one canonical classifier:
 *   - a non-empty STRING element → a {@link ClassifiedAgentMcpServers.referenced} name
 *     (shared with the parent, not cleaned up); a blank/whitespace string is `invalid`.
 *   - a single-key `{ [name]: config }` object whose `config` is a non-null object →
 *     an {@link ClassifiedAgentMcpServers.inline} server (cleaned up on finish); an
 *     empty object, a multi-key object, or a non-object config is `invalid`.
 *   - anything else (number/boolean/null/array element) → `invalid`.
 *
 * `referenced` is de-duplicated in input order; `inline` keeps input order with a
 * later duplicate server name replacing an earlier one (last-wins, matching the
 * inline-map's natural key semantics). Deep per-field config validation is left to
 * the caller (common's `validateMcpServerConfigShape`); this stays a dependency-light
 * core classifier that only distinguishes the two cleanup regimes + obvious garbage.
 */
export function classifyAgentMcpServerSpecs(raw: RawAgentMcpServers): ClassifiedAgentMcpServers {
  const referenced: string[] = [];
  const inline: Array<{ name: string; config: McpServerConfig }> = [];
  const invalid: Array<{ index: number; reason: string }> = [];
  const seenRef = new Set<string>();
  const inlineIndex = new Map<string, number>(); // server name → position in `inline`

  const addInline = (name: string, config: McpServerConfig): void => {
    const at = inlineIndex.get(name);
    if (at !== undefined) {
      inline[at] = { name, config }; // last-wins on a repeated inline server name
      return;
    }
    inlineIndex.set(name, inline.length);
    inline.push({ name, config });
  };

  if (!raw) return { referenced, inline, invalid };

  // Normalize the inline-MAP form into an array of single-key objects so both shapes
  // share the one classification loop below (order follows the map's key order).
  const specs: unknown[] = Array.isArray(raw)
    ? raw
    : Object.entries(raw).map(([name, config]) => ({ [name]: config }));

  specs.forEach((spec, index) => {
    if (typeof spec === 'string') {
      const name = spec.trim();
      if (name.length === 0) {
        invalid.push({ index, reason: 'blank MCP server reference name' });
        return;
      }
      if (!seenRef.has(name)) {
        seenRef.add(name);
        referenced.push(name);
      }
      return;
    }
    if (spec === null || typeof spec !== 'object' || Array.isArray(spec)) {
      invalid.push({ index, reason: 'expected a server name string or a single-key { name: config } object' });
      return;
    }
    const entries = Object.entries(spec as Record<string, unknown>);
    if (entries.length !== 1) {
      invalid.push({ index, reason: `inline MCP server must have exactly one key (got ${entries.length})` });
      return;
    }
    const [name, config] = entries[0]!;
    if (name.trim().length === 0) {
      invalid.push({ index, reason: 'blank inline MCP server name' });
      return;
    }
    if (config === null || typeof config !== 'object' || Array.isArray(config)) {
      invalid.push({ index, reason: `inline MCP server "${name}" has a non-object config` });
      return;
    }
    addInline(name, config as McpServerConfig);
  });

  return { referenced, inline, invalid };
}

// ---------------------------------------------------------------------------
// Named positional-argument substitution — bind a skill/command's declared
// `arguments` names to their positional token values in the body (parity with
// the reference's `substituteArguments`, which replaces BARE `$name` as well as
// braced `${name}`). AMI previously bound only `${name}`; a bare `$foo` was left
// literal. Single canonical substitution shared by the SkillManager content path.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Path/argument-scoped tool allowlist → permission ALLOW rules — the
// least-privilege half AMI's name-only skill/agent tool filter was missing
// (parity with the reference's path-scoped allowlists, e.g. a /statusline-spawned
// subagent bound to `allowedTools: ['Read(~/**)', 'Edit(~/.claude/settings.json)']`
// so it can only read the home tree and edit that single file). `filterToolsForSkill`
// (core/core/src/tools/skill.ts) already enforces WHICH tools a forked skill gets by
// NAME; this derives the WHICH-ARGUMENTS half as additive permission rules the forked
// sub-engine pre-grants. Pure core building block; the skill tool wires it into the
// spawned sub-engine's `permissionRules`.
// ---------------------------------------------------------------------------

/**
 * Parse a skill/command `allowedTools` list into the ARGUMENT-scoped permission
 * ALLOW rules it implies (parity with the reference's path-scoped tool allowlists).
 *
 * For every entry of the form `Tool(scope)` whose scope is a REAL restriction, an
 * `{ tool, pattern, action: 'allow' }` rule is produced so the forked subagent can
 * pre-grant exactly that scoped operation (e.g. `Edit(~/.claude/settings.json)` →
 * allow the file-edit tool only for that path). Entries that are NOT an argument
 * scope yield no rule — the name-only filter alone governs them:
 * - a bare name (`Edit`, no parens),
 * - a category token (`read-only`, `edit`, … — these never carry parens),
 * - an empty or bare-`*` scope (`Edit()`, `Read(*)`) — unrestricted,
 * - a non-string or malformed (unterminated-paren) entry.
 *
 * `normalize` maps the authored tool name to the canonical runtime tool name — the
 * caller passes the shared `normalizeToolName` alias map so `Read`/`Edit` resolve to
 * `file_read`/`file_edit`; it defaults to identity to keep this a dependency-free
 * core helper. Results are de-duplicated by tool+scope and preserve input order.
 *
 * These are ADDITIVE allow rules only: a matching DENY unconditionally wins in the
 * permission engine, so prepending them ahead of inherited parent rules safely scopes
 * the subagent UP to the author's allowlist without ever relaxing a parent deny.
 */
export function scopedToolAllowRules(
  allowedTools: readonly string[] | undefined,
  normalize: (name: string) => string = (n) => n,
): Array<{ tool: string; pattern: string; action: 'allow' }> {
  if (!allowedTools || allowedTools.length === 0) return [];
  const rules: Array<{ tool: string; pattern: string; action: 'allow' }> = [];
  const seen = new Set<string>();
  for (const raw of allowedTools) {
    if (typeof raw !== 'string') continue;
    const entry = raw.trim();
    const open = entry.indexOf('(');
    if (open <= 0 || !entry.endsWith(')')) continue; // no scope (or no tool name) → name filter governs
    const base = entry.slice(0, open).trim();
    if (base.length === 0) continue;
    const scope = entry.slice(open + 1, -1).trim();
    if (scope.length === 0 || scope === '*') continue; // unrestricted scope → no rule
    const tool = normalize(base);
    const key = `${tool}\u0000${scope}`;
    if (seen.has(key)) continue;
    seen.add(key);
    rules.push({ tool, pattern: scope, action: 'allow' });
  }
  return rules;
}

// ---------------------------------------------------------------------------
// Bash-granting tool-allowlist detection — recognize when a skill/command's tool
// allowlist pre-grants the bash tool (parity with the reference's TrustDialog
// `hasSkillsBash` / `hasSlashCommandBash`, which treat a project-shipped skill or
// slash command whose `allowedTools` include Bash as a workspace-trust signal: such
// a capability can auto-execute shell the moment the folder is trusted). The single
// canonical bash-grant matcher the safe-mode settings scanner AND the per-file
// workspace trust audit both delegate to, so "does this allow a shell" is decided
// in exactly one place (replacing two diverged ad-hoc matchers).
// ---------------------------------------------------------------------------

/**
 * True when a single tool-allowlist / permission `allow` entry GRANTS the bash tool
 * (parity with the reference's bash-permission detectors). Matches the bare tool name
 * (`bash` / `Bash`), a scoped rule (`Bash(git:*)` / `bash(npm:*)`), and the
 * space-prefixed `bash <...>` form — all case-insensitively. A non-string entry, a
 * category token (`read-only`, `edit`, …), or any non-bash tool yields `false`.
 * Canonical: the safe-mode settings scanner and the workspace trust audit both
 * delegate here so the bash-grant definition never diverges between them.
 */
export function isBashToolAllowRule(rule: unknown): boolean {
  if (typeof rule !== 'string') return false;
  return /^bash(\s|\(|$)/i.test(rule.trim());
}

/**
 * True when a skill/command's `allowedTools` list pre-grants the bash tool — i.e.
 * ANY entry satisfies {@link isBashToolAllowRule} (parity with the reference's
 * `hasSkillsBash` / `hasSlashCommandBash` trust signals, which flag a project-shipped
 * capability whose allowlist includes Bash). An absent/empty allowlist declares no
 * explicit skill-scoped bash grant and yields `false` (bash availability is then
 * governed by the normal tool set, not a capability-authored allowlist).
 */
export function allowlistGrantsBash(allowedTools: readonly string[] | undefined): boolean {
  if (!allowedTools || allowedTools.length === 0) return false;
  return allowedTools.some(isBashToolAllowRule);
}

// ---------------------------------------------------------------------------
// MCP-derived tool allowlists — compute a skill/command's allowed MCP tool names
// from a connected server's ADVERTISED tool set instead of hand-listing them
// (parity with the reference's bundled claude-in-chrome skill, whose allowlist is
// `BROWSER_TOOLS.map(t => `mcp__<server>__${t.name}`)` — derived from the MCP
// server's enumerated tools, so it never drifts when the server adds/removes a
// tool). Pure core building blocks over AMI's `mcp__<server>__<tool>` naming
// convention; the skill tool's `filterToolsForSkill` wires the `mcp__<server>__*`
// server-wildcard expansion into the real toolset filter.
// ---------------------------------------------------------------------------

/**
 * The model-facing namespace prefix a server's MCP tools carry
 * (`mcp__<server>__`) — the single source for building and recognizing qualified
 * MCP tool names, consistent with AMI's `mcp__<server>__<tool>` convention.
 */
export function mcpToolNamespacePrefix(serverName: string): string {
  return `mcp__${serverName}__`;
}
