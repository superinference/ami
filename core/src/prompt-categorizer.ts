export const PROMPT_INTENTS = ['explain', 'troubleshoot', 'generate', 'refactor', 'review', 'git_ops', 'research', 'other'] as const;
export type Intent = (typeof PROMPT_INTENTS)[number];

export const PROMPT_SCOPES = ['selection', 'current_file', 'few_files', 'codebase'] as const;
export type Scope = (typeof PROMPT_SCOPES)[number];

export interface PromptCategory {
  intent: Intent;
  scope: Scope;
}

const INTENT_PATTERNS: Array<{ pattern: RegExp; intent: Intent }> = [
  { pattern: /\b(explain|what does|how does|why|understand)\b/i, intent: 'explain' },
  { pattern: /\b(fix|bug|error|broken|crash|fail|not working)\b/i, intent: 'troubleshoot' },
  { pattern: /\b(create|generate|write|build|implement|add|new)\b/i, intent: 'generate' },
  { pattern: /\b(refactor|clean|improve|optimize|simplify)\b/i, intent: 'refactor' },
  { pattern: /\b(review|check|audit|evaluate|assess)\b/i, intent: 'review' },
  { pattern: /\b(commit|push|pull|merge|branch|rebase|git)\b/i, intent: 'git_ops' },
  { pattern: /\b(research|analyze|compare|study|investigate|paper)\b/i, intent: 'research' },
];

const SCOPE_PATTERNS: Array<{ pattern: RegExp; scope: Scope }> = [
  { pattern: /\b(this function|this method|this block|selected|the code above)\b/i, scope: 'selection' },
  { pattern: /\b(this file|current file)\b/i, scope: 'current_file' },
  { pattern: /\b(these files|both files|all files in)\b/i, scope: 'few_files' },
  { pattern: /\b(codebase|project|repo|repository|everywhere|all)\b/i, scope: 'codebase' },
];

export function categorizePrompt(userMessage: string): PromptCategory {
  let intent: Intent = 'other';
  for (const { pattern, intent: i } of INTENT_PATTERNS) {
    if (pattern.test(userMessage)) { intent = i; break; }
  }

  let scope: Scope = 'codebase';
  for (const { pattern, scope: s } of SCOPE_PATTERNS) {
    if (pattern.test(userMessage)) { scope = s; break; }
  }

  return { intent, scope };
}

// ---------------------------------------------------------------------------
// Prompt tiers — how much system context a turn warrants.
//
// This is a PURE building block (core). It derives a tier from the raw message
// (NOT just PromptCategory — categorizePrompt defaults scope to 'codebase' and
// intent to 'other', i.e. the heaviest tier, so it cannot gate a cheap path on
// its own). Consumers (common/) decide what each tier includes and apply an
// escalate-only ratchet so a session never de-escalates (preserves prompt cache).
//
// Bias: conservative. Default UP to 'standard'; drop to 'conversational' ONLY
// when the message is clearly chat (no code tokens, no attached files, short,
// explain/other intent). False negatives (over-provisioning) are safe; false
// positives (under-provisioning a coding turn) are not — tool_search self-heals
// the rare miss at the cost of one extra round-trip.
// ---------------------------------------------------------------------------

export const PROMPT_TIERS = ['conversational', 'standard', 'full'] as const;
export type PromptTier = (typeof PROMPT_TIERS)[number];

/** Ascending capability rank — used by the escalate-only ratchet in consumers. */
export const TIER_RANK: Record<PromptTier, number> = {
  conversational: 0,
  standard: 1,
  full: 2,
};

/** Return the higher-capability of two tiers (the ratchet primitive). */
export function maxTier(a: PromptTier, b: PromptTier): PromptTier {
  return TIER_RANK[a] >= TIER_RANK[b] ? a : b;
}

export interface TierSignals {
  /** Model context window in tokens, when known. A small window forces lean. */
  contextWindow?: number;
  /** True when editor/context files are attached — implies real code work. */
  hasContextFiles?: boolean;
}

// Intents that CAN be conversational. Troubleshoot/generate/refactor/review/
// git_ops always imply real work → never conversational.
const CONVERSATIONAL_INTENTS: ReadonlySet<Intent> = new Set<Intent>(['explain', 'other']);

// Intents that imply active code work (edits/commands), used to decide when
// attached context files justify eager (full) provisioning.
const WORK_INTENTS: ReadonlySet<Intent> = new Set<Intent>(['generate', 'refactor', 'review', 'troubleshoot', 'git_ops']);

// Signals that a message is really about code (→ never conversational).
const CODE_TOKEN_PATTERNS: RegExp[] = [
  /```/,                                          // fenced code block
  /`[^`]+`/,                                       // inline code span
  /\.(?:ts|tsx|js|jsx|mjs|cjs|py|go|rs|java|rb|cpp|hpp|cs|kt|swift|php|sh|bash|zsh|sql|html|css|scss|json|yaml|yml|toml|ini|xml|md|lock)\b/i, // dot + known code extension
  /\/[\w.-]+\/[\w.-]+/,                             // path with separators (a/b/c)
  /[A-Za-z_$][\w$]*\([^)]*\)/,                     // function call foo(...)
  /[a-z][a-z0-9]*_[a-z0-9]/i,                      // snake_case identifier
  /[a-z][a-z0-9]*[A-Z][a-z0-9]/,                   // camelCase identifier
  /[\w$]+\.[\w$]+\.[\w$]+/,                         // dotted path a.b.c
  /#!?\[|::|=>|&&|\|\||\$\(|<\/?\w+>/,             // operators / attrs / tags
];

// Explicit breadth markers that justify eager (full) provisioning.
const BREADTH_PATTERN = /\b(?:codebase|whole repo|entire (?:repo|project|codebase)|everywhere|all files|every file|across the (?:repo|codebase|project)|project-wide|migrate|migration)\b/i;

function hasCodeTokens(message: string): boolean {
  return CODE_TOKEN_PATTERNS.some(re => re.test(message));
}

function isShort(message: string): boolean {
  const trimmed = message.trim();
  if (trimmed.length > 280) return false;
  return trimmed.split(/\s+/).filter(Boolean).length <= 30;
}

/**
 * Derive the prompt tier for a turn. Pure and deterministic.
 *
 * Note: 'bare' is a separate hard config flag (bareMode), not a query-derived
 * tier, so it is intentionally not returned here.
 */
export function selectTier(
  userMessage: string,
  category: PromptCategory,
  signals: TierSignals = {},
): PromptTier {
  // A tiny model window can't afford eager everything regardless of query —
  // it caps the upper bound at 'standard' (never 'full'), but the even-leaner
  // 'conversational' tier is still allowed (and preferable) for chat turns.
  const tinyWindow = typeof signals.contextWindow === 'number'
    && signals.contextWindow > 0
    && signals.contextWindow < 32768;

  // Attached files or code tokens → never a chat turn.
  const codey = !!signals.hasContextFiles || hasCodeTokens(userMessage);

  // Breadth/heavy-work signals justify eager (full) provisioning and must take
  // precedence over the conversational short-circuit (e.g. "update every file"
  // has 'other' intent but is clearly work).
  const heavy = BREADTH_PATTERN.test(userMessage)
    || (!!signals.hasContextFiles && WORK_INTENTS.has(category.intent));

  if (!codey
    && !heavy
    && isShort(userMessage)
    && CONVERSATIONAL_INTENTS.has(category.intent)) {
    return 'conversational';
  }

  if (heavy && !tinyWindow) {
    return 'full';
  }

  return 'standard';
}
