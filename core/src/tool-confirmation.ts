import * as crypto from 'crypto';
import { wildcardMatch } from './permissions';

/** Recursively sort object keys so JSON.stringify is canonical (for stable hashing). */
export function deepSortKeys(obj: unknown): unknown {
  if (Array.isArray(obj)) return obj.map(deepSortKeys);
  if (obj && typeof obj === 'object') {
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(obj as Record<string, unknown>).sort()) {
      sorted[k] = deepSortKeys((obj as Record<string, unknown>)[k]);
    }
    return sorted;
  }
  return obj;
}

/**
 * @public Scope of a remembered approval/deny decision.
 *
 * `once` is consumed on first match; `session` is cleared by
 * {@link ToolConfirmationService.clearSession}. The remaining tiers persist
 * across session clears and mirror Claude's permission-update destinations so a
 * library/automated caller can distinguish *where* a standing grant lives
 * instead of collapsing every durable grant into one bucket:
 * - `workspace` — the current working directory / repo (AMI's original durable tier);
 * - `user` — user-global settings (`userSettings`);
 * - `project` — shared project settings (`projectSettings`);
 * - `local` — machine-local, un-shared project settings (`localSettings`).
 */
export type ApprovalScope =
  | 'once'
  | 'session'
  | 'workspace'
  | 'user'
  | 'project'
  | 'local';

/** Every known scope, used for revoke-all and iteration. */
const ALL_SCOPES: readonly ApprovalScope[] = [
  'once',
  'session',
  'workspace',
  'user',
  'project',
  'local',
];

/**
 * Scopes cleared by {@link ToolConfirmationService.clearSession} when a
 * session/agent ends. Everything else (`workspace`/`user`/`project`/`local`)
 * is a durable persistence tier that survives the clear.
 */
const EPHEMERAL_SCOPES: ReadonlySet<ApprovalScope> = new Set<ApprovalScope>([
  'once',
  'session',
]);

/** @public Whether a remembered decision allows or denies execution. */
export type ApprovalBehavior = 'allow' | 'deny';

/** @public How a scoped (content-based) approval matches a tool parameter. */
export type ApprovalMatchMode = 'exact' | 'prefix' | 'wildcard';

/**
 * @public Describes a content-based (scoped) match against a single tool
 * parameter, so a remembered approval can be narrowed below tool-wide — e.g.
 * `{ param: 'command', mode: 'prefix', value: 'npm ' }` approves every `npm …`
 * bash command without approving the entire bash tool. Mirrors Claude's
 * exact|prefix|wildcard permission-rule matching.
 */
export interface ApprovalMatcher {
  /** Top-level parameter key whose (string) value is tested. */
  param: string;
  /** Match strategy: exact equality, string prefix, or glob wildcard (`*`). */
  mode: ApprovalMatchMode;
  /** Pattern/prefix/literal to match against the parameter value. */
  value: string;
}

/** @public Represents an approval record for tool execution in automated/library usage. */
export interface ToolApproval {
  scope: ApprovalScope;
  toolName: string;
  /** SHA-256 (truncated) of canonical params for an exact-match record; undefined = tool-wide. */
  paramsHash?: string;
  approvedAt: number;
  /** 'allow' (default) or 'deny'. Deny records take precedence over allows. */
  behavior: ApprovalBehavior;
  /** When set, this is a content-based (scoped) record matched via {@link ApprovalMatcher}. */
  match?: ApprovalMatcher;
}

/**
 * @public A classifier-driven auto-approval, recorded per tool-use (keyed by
 * `toolUseId`) and kept distinct from user-/library-granted {@link ToolApproval}
 * records. Captures whether an auto/yolo mode granted it and which prompt rule
 * matched, for provenance/audit surfacing.
 */
export interface ClassifierApproval {
  /** The tool-use id this decision was recorded for. */
  toolUseId: string;
  /** True when granted by an auto/yolo permission mode (no explicit named rule). */
  auto: boolean;
  /** The matched prompt-rule name, when one was identified. */
  matchedRule?: string;
  /** The raw auto-mode reason string, when granted by auto/yolo mode. */
  reason?: string;
  /** Epoch ms the decision was recorded. */
  approvedAt: number;
}

/**
 * @public Extract the named rule from a classifier reason string of the form
 * `Allowed by prompt rule: "<rule>"`, falling back to the raw reason when it
 * does not match. Mirrors Claude's
 * `reason.match(/^Allowed by prompt rule: "(.+)"$/)?.[1] ?? reason`.
 */
export function extractPromptRule(reason: string): string {
  return reason.match(/^Allowed by prompt rule: "(.+)"$/)?.[1] ?? reason;
}

/** Options for {@link ToolConfirmationService.isApproved}. */
export interface IsApprovedOptions {
  /**
   * Maximum age (ms) a remembered approval/deny may have and still be honored.
   * Records older than this are treated as expired and evicted. When omitted,
   * approvals never expire (back-compatible default).
   */
  maxAgeMs?: number;
}

/** Default upper bound on the number of remembered records before oldest-first eviction. */
export const DEFAULT_MAX_APPROVALS = 1000;

export class ToolConfirmationService {
  private approvals: Map<string, ToolApproval> = new Map();
  private readonly maxApprovals: number;

  /**
   * @param maxApprovals Cap on remembered records; when exceeded, the oldest
   *   (insertion-order) records are evicted so abandoned 'once'/long-lived
   *   session approvals cannot grow the Map without bound. Must be >= 1.
   */
  constructor(maxApprovals: number = DEFAULT_MAX_APPROVALS) {
    this.maxApprovals = maxApprovals > 0 ? Math.floor(maxApprovals) : DEFAULT_MAX_APPROVALS;
  }

  /** Remember an ALLOW decision for an exact param set (or tool-wide when params is null). */
  approve(
    toolName: string,
    params: Record<string, unknown> | null,
    scope: ApprovalScope,
  ): void {
    this.record(toolName, params, scope, 'allow');
  }

  /**
   * Remember a DENY decision. Deny records are checked before allows, so a
   * narrow deny overrides a broader allow (deny-beats-allow precedence).
   */
  deny(
    toolName: string,
    params: Record<string, unknown> | null,
    scope: ApprovalScope,
  ): void {
    this.record(toolName, params, scope, 'deny');
  }

  /**
   * Remember a content-based (scoped) decision that matches a single parameter
   * by prefix/wildcard/exact value, instead of hashing the whole param set.
   * Lets a session/workspace grant cover a command family (e.g. `npm run *`)
   * without approving the entire tool.
   */
  approvePattern(
    toolName: string,
    match: ApprovalMatcher,
    scope: ApprovalScope,
    behavior: ApprovalBehavior = 'allow',
  ): void {
    const key = this.buildPatternKey(toolName, match, scope, behavior);
    this.approvals.set(key, {
      scope,
      toolName,
      approvedAt: Date.now(),
      behavior,
      match: { ...match },
    });
    this.evictIfNeeded();
  }

  private record(
    toolName: string,
    params: Record<string, unknown> | null,
    scope: ApprovalScope,
    behavior: ApprovalBehavior,
  ): void {
    const key = this.buildKey(toolName, params, scope, behavior);
    this.approvals.set(key, {
      scope,
      toolName,
      paramsHash: params ? this.hashParams(params) : undefined,
      approvedAt: Date.now(),
      behavior,
    });
    this.evictIfNeeded();
  }

  /**
   * Resolve whether a tool+params combination is pre-approved.
   *
   * Precedence: a matching DENY (any scope) wins and returns false. Otherwise a
   * matching 'once' ALLOW is consumed and returns true, then any session/
   * workspace ALLOW (exact, tool-wide, or scoped pattern) returns true. Expired
   * records (per {@link IsApprovedOptions.maxAgeMs}) are skipped and evicted.
   */
  isApproved(
    toolName: string,
    params: Record<string, unknown> | null,
    opts: IsApprovedOptions = {},
  ): boolean {
    const now = Date.now();
    const maxAgeMs = opts.maxAgeMs;
    const fresh = (rec: ToolApproval): boolean =>
      maxAgeMs === undefined || now - rec.approvedAt <= maxAgeMs;

    // Pass 1: DENY precedence — a matching deny blocks regardless of any allow.
    for (const [key, rec] of this.approvals) {
      if (rec.behavior !== 'deny') continue;
      if (!this.matches(rec, toolName, params)) continue;
      if (!fresh(rec)) {
        this.approvals.delete(key);
        continue;
      }
      if (rec.scope === 'once') this.approvals.delete(key);
      return false;
    }

    // Pass 2: ALLOW 'once' — consumed on match.
    for (const [key, rec] of this.approvals) {
      if (rec.behavior !== 'allow' || rec.scope !== 'once') continue;
      if (!this.matches(rec, toolName, params)) continue;
      if (!fresh(rec)) {
        this.approvals.delete(key);
        continue;
      }
      this.approvals.delete(key);
      return true;
    }

    // Pass 3: ALLOW session/workspace — persistent for the scope.
    for (const [key, rec] of this.approvals) {
      if (rec.behavior !== 'allow' || rec.scope === 'once') continue;
      if (!this.matches(rec, toolName, params)) continue;
      if (!fresh(rec)) {
        this.approvals.delete(key);
        continue;
      }
      return true;
    }

    return false;
  }

  /**
   * Remove a previously remembered exact/tool-wide record (e.g. a 'once'
   * approval whose tool call was aborted before execution). Returns the number
   * of records removed. Pattern records are not targeted by this call.
   *
   * @param scope When given, only that scope is revoked; otherwise all scopes.
   */
  revoke(
    toolName: string,
    params: Record<string, unknown> | null,
    scope?: ApprovalScope,
  ): number {
    const scopes: readonly ApprovalScope[] = scope ? [scope] : ALL_SCOPES;
    const behaviors: ApprovalBehavior[] = ['allow', 'deny'];
    let removed = 0;
    for (const s of scopes) {
      for (const b of behaviors) {
        const key = this.buildKey(toolName, params, s, b);
        if (this.approvals.delete(key)) removed++;
      }
    }
    return removed;
  }

  /** Does a stored record match the queried tool + params? */
  private matches(
    rec: ToolApproval,
    toolName: string,
    params: Record<string, unknown> | null,
  ): boolean {
    if (rec.toolName !== toolName) return false;
    if (rec.match) {
      if (!params) return false;
      const raw = params[rec.match.param];
      if (typeof raw !== 'string') return false;
      return this.matchValue(rec.match, raw);
    }
    if (rec.paramsHash === undefined) return true; // tool-wide
    if (!params) return false;
    return rec.paramsHash === this.hashParams(params);
  }

  private matchValue(match: ApprovalMatcher, value: string): boolean {
    switch (match.mode) {
      case 'exact':
        return value === match.value;
      case 'prefix':
        return value.startsWith(match.value);
      case 'wildcard':
        return wildcardMatch(match.value, value);
    }
  }

  private buildKey(
    toolName: string,
    params: Record<string, unknown> | null,
    scope: string,
    behavior: ApprovalBehavior,
  ): string {
    const hash = params ? this.hashParams(params) : 'any';
    return `${behavior}:${scope}:${toolName}:${hash}`;
  }

  private buildPatternKey(
    toolName: string,
    match: ApprovalMatcher,
    scope: string,
    behavior: ApprovalBehavior,
  ): string {
    const fieldHash = this.hashParams({ p: match.param, m: match.mode, v: match.value });
    return `${behavior}:${scope}:${toolName}:pat:${fieldHash}`;
  }

  private hashParams(params: Record<string, unknown>): string {
    const sorted = JSON.stringify(deepSortKeys(params));
    return crypto.createHash('sha256').update(sorted).digest('hex').slice(0, 16);
  }

  /** Evict oldest (insertion-order) records until the Map is within its cap. */
  private evictIfNeeded(): void {
    while (this.approvals.size > this.maxApprovals) {
      const oldest = this.approvals.keys().next().value;
      if (oldest === undefined) break;
      this.approvals.delete(oldest);
    }
  }

  /** Number of currently remembered records (after any lazy eviction). */
  size(): number {
    return this.approvals.size;
  }

  clearSession(): void {
    for (const [key, rec] of this.approvals) {
      if (EPHEMERAL_SCOPES.has(rec.scope)) {
        this.approvals.delete(key);
      }
    }
    // Classifier decisions are per-tool-use and never outlive the session.
    this.classifierApprovals.clear();
  }

  // --- Classifier-approval cache (keyed by toolUseId) ---------------------
  //
  // Separate from user-/library-granted approvals above: this records that a
  // tool call was auto-approved by a *classifier* (the bash-safety classifier
  // or an auto/yolo permission mode), keyed per tool-use so the decision's
  // provenance can be surfaced and audited distinctly from a standing user
  // grant. Mirrors Claude's setClassifierApproval / setYoloClassifierApproval
  // (toolPermission interactiveHandler) and its classifierMatchedRule UI field.

  private classifierApprovals: Map<string, ClassifierApproval> = new Map();

  /**
   * Record a classifier auto-approval for a named prompt rule (non-auto-mode).
   * Mirrors Claude's `setClassifierApproval(toolUseID, matchedRule)`.
   */
  setClassifierApproval(toolUseId: string, matchedRule: string): void {
    this.classifierApprovals.set(toolUseId, {
      toolUseId,
      auto: false,
      matchedRule,
      approvedAt: Date.now(),
    });
    this.evictClassifierIfNeeded();
  }

  /**
   * Record a classifier auto-approval granted by an auto/yolo permission mode
   * (no named rule). Mirrors Claude's `setYoloClassifierApproval(toolUseID,
   * reason)`: it still extracts a prompt-rule name from the reason when one is
   * embedded (`Allowed by prompt rule: "…"`), keeping `matchedRule` populated.
   */
  setYoloClassifierApproval(toolUseId: string, reason: string): void {
    this.classifierApprovals.set(toolUseId, {
      toolUseId,
      auto: true,
      reason,
      matchedRule: extractPromptRule(reason),
      approvedAt: Date.now(),
    });
    this.evictClassifierIfNeeded();
  }

  /** Read a recorded classifier approval for a tool-use, if any. */
  getClassifierApproval(toolUseId: string): ClassifierApproval | undefined {
    return this.classifierApprovals.get(toolUseId);
  }

  /**
   * Forget a classifier approval once its tool-use settles (parity with
   * Claude's `clearClassifierChecking(toolUseID)`). Returns whether one was
   * removed.
   */
  clearClassifierApproval(toolUseId: string): boolean {
    return this.classifierApprovals.delete(toolUseId);
  }

  private evictClassifierIfNeeded(): void {
    while (this.classifierApprovals.size > this.maxApprovals) {
      const oldest = this.classifierApprovals.keys().next().value;
      if (oldest === undefined) break;
      this.classifierApprovals.delete(oldest);
    }
  }
}
