// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as path from 'path';
import * as os from 'os';
import { expandPath } from './utils/path';
import { isManagedPermissionSource, permissionRuleSourceDisplayString } from './permissions-permission-mode-aliases';
import type { PermissionRule, PermissionRuleSource, PermissionUpdateDestination } from './permissions-permission-mode-aliases';
import { normalizeCaseForComparison } from './permissions-permission-verdict';
import { SKILL_CONFIG_FOLDER_NAMES } from './permissions-validate-permission-update';
import type { PermissionUpdate } from './permissions-validate-permission-update';

/**
 * Plain-language, one-line preview of what a {@link PermissionRule} does, for
 * showing the user — under a pattern input while they type, and in the "Rule
 * added" confirmation — what they are about to grant/deny BEFORE it takes effect.
 * Parity: the reference's `PermissionRuleDescription`. Reads behavior + tool + scope +
 * (optionally) origin tier, e.g.:
 *   `Allow bash commands matching "git status"`
 *   `Deny edits to files matching "/etc/**" (from enterprise managed settings)`
 *   `Ask before any web_fetch call`
 */
export function describeRule(rule: PermissionRule): string {
  const verb = rule.action === 'allow' ? 'Allow' : rule.action === 'deny' ? 'Deny' : 'Ask before';
  const tool = rule.tool === '*' ? 'any tool' : rule.tool;
  const origin = rule.source ? ` (from ${permissionRuleSourceDisplayString(rule.source)})` : '';
  // Natural-language prompt rules describe an action in prose, not a glob — render
  // them as the description so the preview reads naturally.
  const promptDescription = extractPromptDescription(rule.pattern);
  if (promptDescription) {
    return `${verb} ${tool} commands described as "${promptDescription}"${origin}`;
  }
  let scope: string;
  if (rule.pattern && rule.pattern !== '*') {
    scope = ` matching "${rule.pattern}"`;
  } else if (rule.paramPattern && Object.keys(rule.paramPattern).length > 0) {
    scope = ` with ${Object.entries(rule.paramPattern).map(([k, v]) => `${k}="${v}"`).join(', ')}`;
  } else {
    scope = ' calls';
  }
  return `${verb} ${tool}${scope}${origin}`;
}

/**
 * When a write targets a file inside `<configRoot>/skills/<name>/…` (project or
 * global), return that ONE skill's name plus a NARROW, least-privilege session rule
 * scoped to just that skill dir — instead of the broad `<configRoot>/*` grant that
 * would also hand over settings.json, hooks/, and every OTHER skill. Lets a user
 * iterate on a single skill without granting blanket config-folder access. Mirrors
 * the reference's `getClaudeSkillScope` (utils/permissions/filesystem.ts L101-157), widened
 * to AMI's config folders (`.ami`, `.claude`) and both project + global skill dirs.
 *
 * Returns `null` (caller falls back to the broad config-folder grant) when the path
 * is not inside a skill dir, is a loose file directly under `skills/` (no skill
 * segment), or the skill segment is UNSAFE: empty, `.`, contains `..` (traversal), or
 * carries a glob metacharacter (`* ? [ ]`) that would widen the pattern to ALL skills
 * or enable pattern injection.
 */
export function getSkillScopeRule(
  filePath: string,
  cwd: string = process.cwd(),
): { skillName: string; skillDir: string; ruleContent: string } | null {
  if (!filePath) return null;
  const abs = path.resolve(cwd, expandPath(filePath));
  const absFolded = normalizeCaseForComparison(abs);
  const home = os.homedir();
  const bases: string[] = [];
  for (const name of SKILL_CONFIG_FOLDER_NAMES) {
    bases.push(path.resolve(cwd, name, 'skills'));
    bases.push(path.join(home, name, 'skills'));
  }
  for (const base of bases) {
    const baseFolded = normalizeCaseForComparison(base);
    if (!absFolded.startsWith(baseFolded + path.sep)) continue;
    // Slice the ORIGINAL (un-folded) path so the skill name keeps its real case —
    // downstream path-glob matching is case-sensitive on POSIX.
    const rest = abs.slice(base.length + path.sep.length);
    const sepIdx = rest.indexOf(path.sep);
    // Require a separator: the file must be INSIDE the skill dir, not a loose file
    // directly under skills/ (which gets no skill scope).
    if (sepIdx <= 0) return null;
    const skillName = rest.slice(0, sepIdx);
    // Reject empty/`.`/traversal (`..`) — a traversal segment would escape the skill
    // dir and the broad-grant fallback is safer than a bad narrow rule.
    if (!skillName || skillName === '.' || skillName.includes('..')) return null;
    // Reject glob metacharacters: a skill literally named `*` (valid on POSIX) would
    // produce `…/skills/*/*` and match EVERY skill, defeating the narrowing.
    if (/[*?[\]]/.test(skillName)) return null;
    const skillDir = path.join(base, skillName);
    return { skillName, skillDir, ruleContent: `${skillDir}${path.sep}*` };
  }
  return null;
}

/**
 * Prefix that marks a permission rule's content as a natural-language "prompt
 * rule" — a human-written DESCRIPTION of what to auto-approve (e.g. "running the
 * test suite"), as opposed to a glob prefix (`git:*`). Parity: the reference's
 * `PROMPT_PREFIX` (`utils/permissions/bashClassifier.ts`).
 */
export const PROMPT_RULE_PREFIX = 'prompt:';

/**
 * Encode a free-text description as prompt-rule content (`prompt: <description>`).
 * This is the canonical rule body saved when a user approves a bash command via
 * the "yes, and don't ask again — described as …" classifier path, so the grant
 * is remembered as a reusable natural-language rule rather than a one-off glob.
 * Parity: the reference's `createPromptRuleContent(description)`.
 *
 * DISTINCT from {@link validateAllowedPromptRule}, which guards plan-mode
 * `allowedPrompts` that AMI stores directly AS glob patterns — this codec names
 * the separate `prompt:`-prefixed classifier-rule format and never produces a glob.
 */
export function createPromptRuleContent(description: string): string {
  return `${PROMPT_RULE_PREFIX} ${description.trim()}`;
}

/** Whether a rule's content string is prompt-rule content (carries the
 *  {@link PROMPT_RULE_PREFIX}); tolerant of leading whitespace. */
export function isPromptRuleContent(content: string | undefined): boolean {
  return typeof content === 'string' && content.trimStart().startsWith(PROMPT_RULE_PREFIX);
}

/**
 * Recover the human-readable description from prompt-rule content, or `null` when
 * the content is not a prompt rule or carries no description after the prefix.
 * Round-trips {@link createPromptRuleContent}. Parity: the reference's
 * `extractPromptDescription(ruleContent)`.
 */
export function extractPromptDescription(content: string | undefined): string | null {
  if (typeof content !== 'string') return null;
  const trimmed = content.trimStart();
  if (!trimmed.startsWith(PROMPT_RULE_PREFIX)) return null;
  const description = trimmed.slice(PROMPT_RULE_PREFIX.length).trim();
  return description.length > 0 ? description : null;
}

/**
 * The read-only detail notice shown for a permission rule that originates from
 * managed/enterprise policy ({@link isManagedPermissionSource}) — such a rule
 * cannot be edited or removed by the user. Returns `null` for every
 * user-editable source, so a caller can branch on "is this rule locked". Parity:
 * the reference's PermissionRuleList "Rule details" view for `policySettings` rules.
 */
export function managedRuleReadOnlyNotice(source: PermissionRuleSource | undefined): string | null {
  if (!isManagedPermissionSource(source)) return null;
  return (
    'This rule is configured by managed settings and cannot be modified.\n' +
    'Contact your system administrator for more information.'
  );
}

/**
 * Turn a granted DIRECTORY into a persistable `file_read` allow rule (parity:
 * the reference's `createReadRuleSuggestion`), so "I pointed the agent at this dir" can be
 * saved as a normal, scoped permission rule rather than only a runtime read root.
 * Two algorithmic subtleties are mirrored exactly:
 *  - the filesystem root `/` is rejected as far too broad (returns `undefined`); and
 *  - an ABSOLUTE path gets an extra leading slash to form the root-anchored
 *    `//path/**` pattern, while a relative path becomes `path/**`.
 * The path is POSIX-normalized first so a Windows path produces a forward-slash glob.
 * `destination` defaults to `'session'` (ephemeral) — the caller opts into a
 * persisted tier explicitly.
 */
export function createReadRuleSuggestion(
  dirPath: string,
  destination: PermissionUpdateDestination = 'session',
): PermissionUpdate | undefined {
  // POSIX-normalize for pattern matching (collapse Windows separators).
  const pathForPattern = dirPath.replace(/\\/g, '/');
  // The filesystem root is too broad to be a reasonable permission target.
  if (pathForPattern === '/') return undefined;
  // Absolute paths get an extra leading slash → the `//path/**` root-anchored form.
  const ruleContent = pathForPattern.startsWith('/')
    ? `/${pathForPattern}/**`
    : `${pathForPattern}/**`;
  return {
    type: 'addRules',
    behavior: 'allow',
    destination,
    rules: [{ toolName: 'file_read', ruleContent }],
  };
}

/**
 * Build the model-facing "grant-and-retry" message for a set of just-approved,
 * previously-denied commands, so the agent knows it may re-attempt them. Mirrors
 * the reference's `createPermissionRetryMessage` (utils/messages.ts): the content is
 * `Allowed <cmd>, <cmd>, …`. Returns an empty-but-valid notice when the list is
 * empty so callers never have to special-case it.
 */
export function createPermissionRetryMessage(commands: string[]): string {
  if (commands.length === 0) return 'No denied commands to retry';
  return `Allowed ${commands.join(', ')}`;
}

/**
 * Does a would-be PERSISTENT bash `allow` pattern contain a shell-DYNAMIC construct
 * that must never be baked into a reusable rule? Returns the offending token kind, or
 * `null` when the pattern is a safe static prefix/glob.
 *
 * Rationale (parity: the reference's `staticPrefix.ts` sanitization, L55-140): a persisted
 * allow rule is matched against FUTURE commands, so a dynamic token makes the rule
 * match MORE — or different — commands than the user actually saw and approved:
 *  - a command/arithmetic substitution (`$(...)`, backtick, `${...}`) or a variable
 *    expansion (`$VAR`, `$@`, …) means the real executed command is computed at run
 *    time, so "allow `deploy $(cat target)`" would silently authorize anything the
 *    substitution expands to;
 *  - a backslash path fragment (`C:\…`, `foo\bar`) is an escape/quoting hazard that
 *    the glob matcher does not interpret the way a shell would, so the rule can
 *    over-match.
 * Glob metacharacters (`*`, `?`, `[`) are intentionally TOLERATED — they are the
 * legitimate way an allow pattern scopes arguments (`git *`). Pure + side-effect
 * free. The enforcement point is {@link PermissionManager.addRule}, which refuses to
 * persist such a rule instead of silently granting an over-broad one.
 */
export function dynamicAllowPatternToken(pattern: string | undefined): 'substitution' | 'variable' | 'backslash' | null {
  if (!pattern) return null;
  for (let i = 0; i < pattern.length; i++) {
    const c = pattern[i]!;
    // A backslash is an escape/quoting hazard in a persisted glob (the reference rejects
    // backslash path fragments). A trailing lone backslash counts too.
    if (c === '\\') return 'backslash';
    // Backtick command substitution.
    if (c === '`') return 'substitution';
    if (c === '$') {
      const next = pattern[i + 1];
      // `$(` command / `$((` arithmetic substitution.
      if (next === '(') return 'substitution';
      // `${...}` parameter expansion, or a `$NAME` / special-parameter expansion
      // ($@ $* $# $? $! $$ $- $0-$9). A bare `$` at end-of-string or before a
      // non-expansion char (e.g. a literal `price$`) is not dynamic.
      if (next === '{') return 'variable';
      if (next && /[A-Za-z_@*#?!$0-9-]/.test(next)) return 'variable';
    }
  }
  return null;
}

/** The shape every permission-rule validator (built-in or per-tool custom)
 *  returns — a boolean verdict plus optional actionable guidance. */
export interface PermissionRuleValidationResult {
  valid: boolean;
  error?: string;
  suggestion?: string;
  examples?: string[];
}

/**
 * A per-tool content validator: given the rule's parenthesized CONTENT (never the
 * tool name), return whether it is a well-formed pattern for that specific tool.
 * This is AMI's analogue of the reference's `toolValidationConfig[tool].customValidation`
 * (utils/settings/toolValidationConfig.ts) — a pluggable hook consulted by
 * {@link validatePermissionRule} BEFORE the generic bash/file checks, so a tool
 * whose patterns follow neither bash-prefix nor file-glob syntax (WebFetch domains,
 * WebSearch terms, …) can reject a malformed pattern with tailored guidance.
 */
export type ToolContentValidator = (content: string) => PermissionRuleValidationResult;

/**
 * Registry of per-tool content validators, keyed by AMI CANONICAL tool name (post
 * {@link normalizeToolName}). Previously these checks lived as hardcoded `if
 * (toolName === 'web_search')` / `'web_fetch'` branches inside
 * {@link validatePermissionRule}; consolidating them into one pluggable table is
 * the extensibility surface the reference exposes as `getCustomValidation` and AMI lacked
 * — a new tool's pattern rules are added HERE, in one place, instead of growing the
 * validator with another special case. Consumed via {@link getCustomValidation}.
 */
export const TOOL_CONTENT_VALIDATORS: Readonly<Record<string, ToolContentValidator>> = {
  // WebSearch: patterns are allowed/blocked search TERMS — wildcards are
  // meaningless and silently never match. Reject them with guidance.
  web_search: (content) => {
    if (content.includes('*') || content.includes('?')) {
      return {
        valid: false, error: 'WebSearch patterns cannot contain wildcards',
        suggestion: 'Use a plain search term (wildcards are not supported for WebSearch)',
        examples: ['web_search(weather)', 'web_search(stock prices)'],
      };
    }
    return { valid: true };
  },
  // WebFetch: patterns match a DOMAIN, not a full URL or a URL with a path. AMI
  // uses BARE-domain syntax (`web_fetch(example.com)`) — a deliberate divergence
  // from the reference's required `domain:` prefix — so a domain that merely starts with
  // "http" (e.g. `httpbin.org`) stays valid; only a `scheme://` at the start OR a
  // stray `://` ANYWHERE (a malformed URL whose scheme isn't a clean leading token,
  // e.g. `3http://x`) is rejected, AND a bare domain carrying a `/path` component
  // (`example.com/api/*`) is rejected because matching is domain-only so the path
  // silently never applies. Mirrors the reference's WebFetch validator (toolValidationConfig.ts).
  web_fetch: (content) => {
    if (/^[a-z][a-z0-9+.-]*:\/\//i.test(content) || content.includes('://')) {
      return {
        valid: false, error: 'WebFetch patterns must be a domain, not a URL',
        suggestion: 'Drop the scheme and path — match on the domain only',
        examples: ['web_fetch(example.com)', 'web_fetch(*.github.com)'],
      };
    }
    if (content.includes('/')) {
      return {
        valid: false, error: 'WebFetch patterns match a domain, not a path',
        suggestion: 'Drop the path — a WebFetch rule scopes a host, matching every path on it',
        examples: ['web_fetch(example.com)', 'web_fetch(*.github.com)'],
      };
    }
    return { valid: true };
  },
};

/**
 * Compose the structured result of {@link validatePermissionRule} into a SINGLE
 * human-readable sentence: `error. suggestion. Examples: ex1, ex2`. This is the
 * canonical rejection-text formatter so every surface that shows a dropped/invalid
 * rule (settings loader warnings, the `/permissions` validate command, a config
 * UI) renders it identically — error, then the suggestion (if any), then the
 * examples (if any). Mirrors the reference's `PermissionRuleSchema` superRefine message
 * composition (utils/settings/permissionValidation.ts L244-262), which previously
 * had no AMI equivalent: AMI composed only `error. suggestion` and silently
 * dropped the examples. Returns the bare error for a result with no suggestion or
 * examples; returns an empty string for a `valid` result (nothing to format).
 */
export function formatPermissionRuleError(result: {
  valid: boolean; error?: string; suggestion?: string; examples?: string[];
}): string {
  if (result.valid || !result.error) return '';
  let message = result.error;
  if (result.suggestion) message += `. ${result.suggestion}`;
  if (result.examples && result.examples.length > 0) {
    message += `. Examples: ${result.examples.join(', ')}`;
  }
  return message;
}

/**
 * Upper bound on the length of a model-supplied plan-mode `allowedPrompts` entry.
 * A legitimate scoped action description / command prefix is short; anything far
 * longer is either junk or an attempt to smuggle a huge rule, so it is refused.
 */
export const MAX_ALLOWED_PROMPT_LENGTH = 1000;

/**
 * Validate a single MODEL-supplied plan-mode `allowedPrompts` entry before it is
 * persisted as an `allow` permission rule on plan-mode exit.
 *
 * the reference's ExitPlanMode tool treats `allowedPrompts` as SCOPED action
 * descriptions (`tool: z.enum(['Bash'])`, e.g. "run tests") — never a match-all
 * glob. AMI stores each entry's text directly as a rule pattern
 * (`addRule({ tool, pattern: prompt, action: 'allow' })`), so without this guard
 * a crafted or prompt-injected entry such as `*`, ``, or `**` would become a
 * blanket `bash(*)` allow-EVERYTHING rule — a privilege-escalation vector that
 * silently auto-approves every subsequent command.
 *
 * Returns `{ safe:false, reason }` for a non-string, an empty/whitespace-only
 * value, a pattern that collapses to "match everything" (only glob
 * metacharacters: `*`, `**`, `.*`, `*:*`, a lone `:*`, `?*`, …), or a value
 * longer than {@link MAX_ALLOWED_PROMPT_LENGTH}; otherwise `{ safe:true }`.
 */
export function validateAllowedPromptRule(prompt: unknown): { safe: boolean; reason?: string } {
  if (typeof prompt !== 'string') return { safe: false, reason: 'prompt must be a string' };
  const trimmed = prompt.trim();
  if (!trimmed) return { safe: false, reason: 'prompt is empty' };
  if (trimmed.length > MAX_ALLOWED_PROMPT_LENGTH) {
    return { safe: false, reason: `prompt exceeds ${MAX_ALLOWED_PROMPT_LENGTH} characters` };
  }
  // A pattern made up ONLY of glob/wildcard metacharacters matches every command
  // (the glob matcher treats `*`/`?`/`.*`/`:*` as "anything"), so it is too broad
  // to auto-allow. Whitespace is irrelevant to the match, so collapse it first.
  const stripped = trimmed.replace(/\s+/g, '');
  if (/^[*?.:]+$/.test(stripped)) {
    return { safe: false, reason: 'pattern matches all commands (too broad to auto-allow)' };
  }
  return { safe: true };
}

/**
 * Partition an array of model-supplied plan-mode `allowedPrompts` entries into the
 * ones safe to persist as `allow` rules and the ones refused by
 * {@link validateAllowedPromptRule}, preserving each entry's full shape (so the
 * caller keeps its `tool`). The rejected list carries the refusal reason so the
 * caller can surface WHICH prompts were dropped and WHY (introspection).
 */
export function partitionAllowedPromptRules<T extends { prompt?: unknown }>(
  entries: readonly T[],
): { valid: T[]; rejected: Array<{ entry: T; reason: string }> } {
  const valid: T[] = [];
  const rejected: Array<{ entry: T; reason: string }> = [];
  for (const entry of entries) {
    const result = validateAllowedPromptRule(entry.prompt);
    if (result.safe) valid.push(entry);
    else rejected.push({ entry, reason: result.reason ?? 'invalid prompt' });
  }
  return { valid, rejected };
}

/**
 * Match a single bash sub-command against a rule pattern.
 *
 * For deny/ask rules we ALSO test a de-obfuscated form (r""m, r''m, "rm", r\m →
 * rm) so a command-word split with empty quotes / backslashes / quotes cannot
 * slip a real command past a user deny or ask rule (S3). Allow rules stay
 * STRICT — they only match the literal stripped form — so de-obfuscation can
 * never *broaden* an allow into auto-approving an obfuscated command.
 */
/**
 * Extract the COMMAND PREFIX from a bash rule content written in the reference's `:*`
 * prefix convention (`git commit:*` → `git commit`, `npm:*` → `npm`), or `null`
 * when the content is not a prefix rule (no trailing `:*`, or nothing before it).
 * Mirrors the reference's `permissionRuleExtractPrefix` (shellPermissionHelpers.ts
 * L76-84). AMI's rule validator already BLESSES this form
 * ({@link validatePermissionRule} accepts `bash(git:*)` and even suggests it as
 * the canonical example) and {@link permissionRuleValueFromString} preserves the
 * `:*` content verbatim — but the raw glob matcher treats the `:` literally, so a
 * `git:*` rule never matched `git status`. This extractor lets {@link matchBashSub}
 * honor the convention: a `prefix:*` rule matches a command that IS the prefix or
 * begins with `prefix` followed by its arguments ({@link commandHasRulePrefix}).
 */
export function permissionRuleExtractPrefix(ruleContent: string): string | null {
  if (!ruleContent.endsWith(':*')) return null;
  const prefix = ruleContent.slice(0, -2).trim();
  return prefix.length > 0 ? prefix : null;
}
