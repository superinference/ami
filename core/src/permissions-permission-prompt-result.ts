// Split from permissions.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import type { PermissionContentBlock } from './permission-content-blocks';
import type { AutoModeSafetyContext, AutoModeSafetyVerdict, BashClassification, PermissionMode, PermissionRuleSource } from './permissions-permission-mode-aliases';

/**
 * Optional, injectable safety classifier consulted by {@link PermissionManager.check}
 * on the AUTONOMOUS auto-approve path (`auto-allow` / `bypassPermissions` / inherited
 * plan-bypass / screened `auto`) BEFORE a tool call is silently allowed. It can
 * DOWNGRADE an auto-approval to a prompt (`ask`) or a block (`deny`) with a recorded
 * reason — it can never UPGRADE an `ask`/`deny` to `allow`. Mirrors the reference's
 * transcript/"auto-mode" safety classifier (`classifierApprovals.ts` /
 * `useCanUseTool` yolo classifier): a model- or rule-backed check that gates
 * autonomous approvals. The deterministic engine is the floor; this seam only ever
 * TIGHTENS it. Returning `undefined`, resolving to `allow`, or throwing all FAIL OPEN
 * to the deterministic auto-approval (the static classifier remains authoritative),
 * matching the reference's "classifier error → fall back to the static decision" behavior.
 * The common/engine layer injects a concrete classifier via
 * {@link PermissionManager.setAutoModeSafetyClassifier}; unset → the seam is inert and
 * autonomous behavior is byte-for-byte unchanged.
 */
export type AutoModeSafetyClassifier = (
  ctx: AutoModeSafetyContext,
) => AutoModeSafetyVerdict | undefined | Promise<AutoModeSafetyVerdict | undefined>;

/** Scope for an `allow_session` grant on the agent's own config folder. */
export const SESSION_GRANT_SCOPES = ['config-folder', 'global-config-folder'] as const;

export type SessionGrantScope = (typeof SESSION_GRANT_SCOPES)[number];

export interface PermissionPromptResult {
  /**
   * The user's decision. The two GRANT tiers are deliberately distinct — see
   * {@link permissionGrantTier} for the single canonical classifier the executor
   * and UI share instead of re-deriving the tier inline:
   *   - `allow_once`    — approve THIS call only; no standing rule is created, the
   *                       next matching call prompts again (the reference's user_temporary).
   *   - `allow_session` — a standing allow rule for the REST OF THIS SESSION only:
   *                       kept in memory and NEVER written to disk, even when an
   *                       on-disk persist destination is configured. The "scoped
   *                       session accept" that is explicitly distinct from a
   *                       persisted "allow always" (the reference's `accept-session`).
   *   - `allow_pattern` — a standing allow rule the user means to KEEP: persisted to
   *                       the configured on-disk settings tier when one is set
   *                       (the reference's persisted "don't ask again" / user_permanent).
   *   - `deny` / `cancelled` — refuse / abandon (not a grant).
   */
  action: 'allow_once' | 'allow_pattern' | 'allow_session' | 'deny' | 'cancelled';
  pattern?: string;
  /** Session-grant scope (for `allow_session` self-config edits). */
  scope?: SessionGrantScope;
  /**
   * Free-text the user attached when DENYING ("tell the model what to do
   * differently") — routed back to the model so it can steer the next turn.
   */
  feedback?: string;
  /**
   * Free-text the user attached when APPROVING ("tell the model what to do
   * next") — extra guidance delivered alongside the allowed result.
   */
  acceptFeedback?: string;
  /**
   * Tool input the user EDITED at the prompt (or an IDE "apply changes" flow
   * rewrote) before approving — applied in place of the model's original input
   * when the decision is an allow. Honored only on `allow_once` / `allow_pattern`;
   * ignored on deny/cancelled. The executor validates it against the tool schema
   * (dropping undeclared keys) before substituting it, so the UI cannot smuggle
   * unknown parameters. Mirrors the reference's `onAllow(updatedInput)` /
   * `ideDiffSupport.applyChanges` (the permission dialog returns the user's
   * modified edits instead of a plain allow).
   */
  updatedInput?: Record<string, unknown>;
  /**
   * Rich content blocks a hook or the permission prompt attaches to the decision,
   * flowing back to the model ALONGSIDE the permission result — e.g. a structured
   * text block, an image (a screenshot the user pasted into the deny dialog), or a
   * document. Text blocks are folded into the model-facing feedback; image blocks
   * are delivered as real multimodal content (queued like a tool-result image);
   * document blocks are surfaced as a short naming note. Carried on both approve
   * (`allow_once`/`allow_pattern`) and refuse (`deny`/`cancelled`) decisions so the
   * model never receives ONLY a plain-text line when richer context was provided.
   * Split into delivery channels by `splitPermissionContentBlocks`. Mirrors the reference's
   * `PermissionDecision.contentBlocks` (PermissionContext `buildAllow` /
   * `cancelAndAbort` / `handleUserAllow`), using AMI's own content-block union.
   */
  contentBlocks?: PermissionContentBlock[];
}

/**
 * The persistence TIER a permission decision requests — the single canonical
 * discriminator distinguishing a SESSION-ONLY grant from a PERSISTED-ALWAYS grant,
 * so the tool-executor and the UI never re-derive it with an inline `=== 'allow_…'`
 * check that could drift:
 *   - `'once'`      — approve this call only; no standing rule.
 *   - `'session'`   — a standing rule for the rest of THIS session; in-memory,
 *                     NEVER written to disk even when a persist destination is set.
 *   - `'always'`    — a standing rule the user means to keep; persisted to the
 *                     configured on-disk settings tier when one is set.
 *   - `'deny'` / `'cancelled'` — not a grant.
 * Mirrors the reference's accept-session vs persisted "don't ask again" distinction
 * (FilePermissionDialog `accept-session` vs the persisted rule). Pure.
 *
 * REGRESSION LOCK: if `allow_session` ever maps to `'always'` (or `allow_pattern`
 * to `'session'`) the scoped session accept would silently start PERSISTING grants
 * to disk (over-grant) — the permissions unit test asserts this mapping exactly.
 */
export const PERMISSION_GRANT_TIERS = ['once', 'session', 'always', 'deny', 'cancelled'] as const;

export type PermissionGrantTier = (typeof PERMISSION_GRANT_TIERS)[number];

export function permissionGrantTier(
  action: PermissionPromptResult['action'],
): PermissionGrantTier {
  switch (action) {
    case 'allow_once':
      return 'once';
    case 'allow_session':
      return 'session';
    case 'allow_pattern':
      return 'always';
    case 'deny':
      return 'deny';
    case 'cancelled':
      return 'cancelled';
  }
}

/**
 * True when the decision adds a standing SESSION-ONLY rule that must NEVER be
 * flushed to disk — the discriminator the tool-executor uses to SKIP the on-disk
 * persist step for an `allow_session` grant while still honoring it for a persisted
 * `allow_pattern` grant. Pure.
 */
export function isSessionOnlyGrant(action: PermissionPromptResult['action']): boolean {
  return permissionGrantTier(action) === 'session';
}

export interface PermissionPromptHandler {
  showPrompt(
    toolName: string,
    inputSummary: string,
    classification?: BashClassification,
    /**
     * Optional request abort signal. When it fires while the prompt is pending
     * the handler should resolve `{ action: 'cancelled' }` so a cancelled turn
     * doesn't leave a permission dialog hanging.
     */
    abortSignal?: AbortSignal,
  ): Promise<PermissionPromptResult>;
}

/**
 * Structured reason a permission decision needs confirmation/was denied. The
 * `type` discriminator lets the UI explain WHY approval is required instead of a
 * bare "needs approval". Mirrors the reference's permissionResult.decisionReason union.
 */
export type PermissionDecisionReason =
  | { type: 'hardline'; description: string }
  | {
      type: 'rule';
      action: 'allow' | 'deny' | 'ask';
      ruleValue?: string;
      /** Free-text attribution shown inline ("from <source>") — the rule's own
       *  reason/label, NOT the tier it came from. */
      source?: string;
      /**
       * The originating settings TIER of the matched rule ({@link PermissionRuleSource}).
       * Distinct from the free-text {@link source}: this is the machine-readable
       * provenance used to decide whether a remediation hint applies — a rule from the
       * enterprise-managed `policySettings` tier is admin-owned and cannot be changed by
       * the user, so the "update your rules" hint is suppressed for it (parity with
       * the reference's `decisionReason.rule.source === 'policySettings'` branch in
       * PermissionRuleExplanation.tsx).
       */
      ruleSource?: PermissionRuleSource;
    }
  | { type: 'mode'; mode: PermissionMode }
  | { type: 'safetyCheck'; check: SafetyCheckKind; message: string }
  | { type: 'classifier'; name?: string; reason?: string }
  /**
   * The action targets a path OUTSIDE the agent's workspace (launch cwd + granted
   * read roots), so it needs explicit consent. Mirrors the reference's `workingDir`
   * decisionReason. `path` is the offending target; `reason` is the human sentence.
   */
  | { type: 'workingDir'; path: string; reason: string }
  /**
   * A hook (PreToolUse / PermissionRequest) asked for or denied confirmation.
   * Mirrors the reference's `hook` decisionReason (hookName + optional reason + source tag).
   * `behavior` is the decision the hook actually returned (allow/deny/ask) when it
   * made one, so the message can read "<hook> allowed/denied/asked for confirmation
   * for this action" (parity with the reference's toolHooks `getRuleBehaviorDescription`
   * phrasing) instead of the generic "requires approval".
   */
  | { type: 'hook'; hookName: string; reason?: string; hookSource?: string; behavior?: 'allow' | 'deny' | 'ask' }
  /**
   * The call opted out of the sandbox (`dangerouslyDisableSandbox`), so it must be
   * explicitly authorized to run unsandboxed. Mirrors the reference's `sandboxOverride`
   * decisionReason ("Requires permission to bypass sandbox").
   */
  | { type: 'sandboxOverride' }
  | { type: 'other'; message?: string };

export const SAFETY_CHECK_KINDS = [
  'credential', 'systemDir', 'suspiciousPath', 'agentConfig', 'buildConfig', 'destructive', 'codeExec', 'toctou', 'argLeak',
] as const;

export type SafetyCheckKind = (typeof SAFETY_CHECK_KINDS)[number];

/**
 * Map a permission BEHAVIOR enum (`allow`/`deny`/`ask`) to its grammatical past-tense
 * prose, so a decision can be narrated in a sentence — "<hook> allowed this action",
 * "<rule> denied this action", "<hook> asked for confirmation for this action".
 * Mirrors the reference's `getRuleBehaviorDescription` (utils/permissions/PermissionResult.ts),
 * where `ask` (the default) reads "asked for confirmation for". A single canonical
 * mapping so callers never hand-roll the allow→"allowed" / deny→"denied" conversion.
 *
 * The parameter is intentionally widened to `string` so this is a FAIL-SAFE boundary:
 * any value outside `allow`/`deny` — including a future/unknown behavior a newer
 * settings file might carry — degrades to the most conservative "asked for
 * confirmation for" prose rather than throwing or mislabeling it as an allow (parity
 * with the reference's `getRuleBehaviorDescription` `default` arm).
 */
export function getRuleBehaviorDescription(behavior: string): string {
  switch (behavior) {
    case 'allow':
      return 'allowed';
    case 'deny':
      return 'denied';
    default:
      return 'asked for confirmation for';
  }
}

/**
 * The CATEGORY of a tool for permission-prompt phrasing, mirroring the reference's
 * PermissionRuleExplanation `toolType` (`'tool' | 'command' | 'edit' | 'read'`):
 * it shapes the NOUN in a confirmation/denial message ("…for this command" vs
 * "…for this edit"/"…for this read") so the prompt reads naturally for the tool
 * being gated instead of always saying "command"/"action". Distinct from the
 * tool-search {@link getToolCategory} taxonomy, which classifies tools for
 * discovery and takes a full tool object — this one keys off the tool NAME alone
 * so it is usable anywhere a permission decision is narrated.
 */
export const PERMISSION_PROMPT_TOOL_TYPES = ['command', 'edit', 'read', 'tool'] as const;

export type PermissionPromptToolType = (typeof PERMISSION_PROMPT_TOOL_TYPES)[number];

/** The noun a permission message uses for a {@link PermissionPromptToolType}
 *  (`'tool'` → "tool call", every other category verbatim). */
export function permissionPromptToolNoun(toolType: PermissionPromptToolType): string {
  return toolType === 'tool' ? 'tool call' : toolType;
}

/**
 * Build a human-readable explanation from a structured decision reason. When a
 * {@link PermissionPromptToolType} is supplied, the generic noun is phrased per
 * tool category ("for this edit" / "for this read" / "for this tool call") —
 * parity with the reference's PermissionRuleExplanation, which interpolates the tool
 * category into every message. Omitting it preserves the default "command"
 * (rule) / "action" (mode/hook/other) wording for callers that have no tool
 * context.
 */
export function createPermissionRequestMessage(
  reason: PermissionDecisionReason,
  toolType?: PermissionPromptToolType,
): string {
  const ruleNoun = toolType ? permissionPromptToolNoun(toolType) : 'command';
  const genericNoun = toolType ? permissionPromptToolNoun(toolType) : 'action';
  switch (reason.type) {
    case 'hardline':
      return `Blocked: ${reason.description}`;
    case 'rule': {
      const from = reason.source ? ` from ${reason.source}` : '';
      const which = reason.ruleValue ? ` "${reason.ruleValue}"` : '';
      return `Permission rule${which}${from} requires ${reason.action === 'deny' ? 'denial' : 'approval'} for this ${ruleNoun}`;
    }
    case 'mode':
      return `Current permission mode (${reason.mode}) requires approval for this ${genericNoun}`;
    case 'safetyCheck':
      return reason.message;
    case 'classifier':
      return `The safety classifier${reason.name ? ` (${reason.name})` : ''} flagged this action${reason.reason ? `: ${reason.reason}` : ''}`;
    case 'workingDir':
      return reason.reason;
    case 'hook': {
      const verb = reason.behavior ? getRuleBehaviorDescription(reason.behavior) : 'requires approval for';
      return `Hook "${reason.hookName}" ${verb} this ${genericNoun}${reason.reason ? `: ${reason.reason}` : ''}`;
    }
    case 'sandboxOverride':
      return 'This action requires permission to bypass the sandbox';
    case 'other':
      return reason.message ?? `This ${genericNoun} requires approval`;
  }
}

// ---------------------------------------------------------------------------
// Deterministic permission risk explainer (RiskLevel / PermissionExplanation)
// ---------------------------------------------------------------------------

/** Risk tier of a pending tool call, ordered LOW < MEDIUM < HIGH. */
export const RISK_LEVELS = ['LOW', 'MEDIUM', 'HIGH'] as const;

export type RiskLevel = (typeof RISK_LEVELS)[number];

/** Numeric severity of a {@link RiskLevel} (LOW=1, MEDIUM=2, HIGH=3), for ordering
 *  and analytics bucketing. Parity: the reference's `RISK_LEVEL_NUMERIC` map. */
export function riskLevelRank(level: RiskLevel): number {
  return RISK_LEVELS.indexOf(level) + 1;
}

/**
 * A plain-language risk assessment of a pending tool call, surfaced on the
 * permission prompt so the user can make an informed allow/deny decision and
 * attached to denial records for later review. Parity: the reference's
 * `PermissionExplanation` (utils/permissions/permissionExplainer.ts). the reference
 * generates this with an LLM side-query; AMI computes it DETERMINISTICALLY from the
 * same command-classification primitives the permission engine already uses
 * ({@link PermissionManager.classifyBashCommand}, {@link detectHardlineCommand}),
 * so it needs no extra model round-trip and is stable and unit-testable.
 */
export interface PermissionExplanation {
  /** Overall risk: LOW (safe dev workflow), MEDIUM (recoverable change), HIGH (dangerous/irreversible). */
  riskLevel: RiskLevel;
  /** What the tool call does (1-2 sentences). */
  explanation: string;
  /** Why it is being run, model-voiced (starts with "I"). */
  reasoning: string;
  /** What could go wrong, concisely. */
  risk: string;
}

/** Map a {@link BashClassification} to its {@link RiskLevel}. */
export const BASH_CLASSIFICATION_RISK: Record<BashClassification, RiskLevel> = {
  safe: 'LOW',
  unsafe: 'MEDIUM',
  destructive: 'HIGH',
};

export function extractBashCommandText(input: unknown): string {
  if (typeof input === 'string') return input;
  if (input && typeof input === 'object' && 'command' in input) {
    const c = (input as { command?: unknown }).command;
    if (typeof c === 'string') return c;
  }
  return '';
}

// ---------------------------------------------------------------------------
// 'auto' permission mode — screened autonomy (parity: the reference's AutoMode)
// ---------------------------------------------------------------------------

/**
 * The verdict of the per-call screen the `auto` permission mode applies to EVERY
 * tool call: `allow` runs it without a prompt, `deny` blocks it (the engine
 * reports the block to the model, which may try a different approach — the reference's
 * "block what is judged risky" fallback). This is a middle ground between `ask`
 * (prompt on everything) and `auto-allow` (run everything except hardline): an
 * autonomous run proceeds without prompts yet is bounded by a per-call risk +
 * prompt-injection screen.
 */
export interface AutoModeScreen {
  decision: 'allow' | 'deny';
  /** Deterministic risk level of the call (reuses {@link classifyPermissionRisk}). */
  riskLevel: RiskLevel;
  /** True when a prompt-injection pattern was found in the tool input. */
  injection: boolean;
  /** Human-readable justification for the decision (surfaced in the denial log). */
  reason: string;
}

/**
 * Gather the string-valued fields of a tool input into one blob so a
 * prompt-injection scan can inspect everything the model proposed to execute
 * (the bash `command`, an edit's `content`/`new_string`, a URL, etc.) rather than
 * just one well-known field. Non-string values are ignored.
 */
export function collectToolInputText(input: unknown): string {
  if (typeof input === 'string') return input;
  if (!input || typeof input !== 'object') return '';
  const parts: string[] = [];
  for (const v of Object.values(input as Record<string, unknown>)) {
    if (typeof v === 'string') parts.push(v);
  }
  return parts.join('\n');
}

export interface DenyCheckResult {
  denied: boolean;
  reason?: string;
}

/** A recency-ordered record of a denied action, for user review (`/permissions`). */
export interface RecentDenial {
  /** Human-readable command / summary of the denied action. */
  display: string;
  /** What produced the denial. */
  source: 'rule' | 'hardline' | 'classifier' | 'mode';
  /**
   * When `source === 'classifier'`, WHICH classifier produced it — `'bash'` for the
   * deterministic bash command classifier, `'auto-mode'` for the injected autonomous
   * safety classifier. Lets `/permissions` show HOW a call was gated (parity with
   * the reference's `ClassifierApproval.classifier` discriminant). Absent for non-classifier
   * sources.
   */
  classifier?: 'bash' | 'auto-mode';
  /** Optional explanation. */
  reason?: string;
  /** Deterministic risk tier of the denied action, so a reviewer can triage the
   *  recent-denials list by severity. Computed from {@link classifyPermissionRisk}
   *  when `recordDenial` is given a display string. */
  riskLevel?: RiskLevel;
  timestamp: number;
}
