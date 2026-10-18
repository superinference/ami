export type HookTier = 'builtin' | 'prepend' | 'user' | 'append' | 'core';

export interface HookRegistration {
  name: string;
  event: string;
  tier: HookTier;
  source: string;
  priority: number;
}

const TIER_PRIORITY: Record<HookTier, number> = {
  builtin: 0,
  prepend: 1,
  user: 2,
  append: 3,
  core: 4,
};

export const PROTECTED_EVENTS = new Set([
  'session.compact',
  'prompt.compose',
  'prompt.section',
  'prompt.context',
  'prompt.attachment',
  'settings.read',
]);

export const PASSTHROUGH_EVENTS = new Set([
  'prompt.submit',
  'turn.start',
  'turn.end',
  'tool.call',
  'command.run',
  'command.register',
]);

export function isProtectedEvent(event: string): boolean {
  return PROTECTED_EVENTS.has(event);
}

export function canHookEvent(event: string, tier: HookTier): boolean {
  if (!isProtectedEvent(event)) return true;
  return tier !== 'user';
}

export function sortHooksByTier(hooks: HookRegistration[]): HookRegistration[] {
  return [...hooks].sort((a, b) => {
    const tierDiff = TIER_PRIORITY[a.tier] - TIER_PRIORITY[b.tier];
    if (tierDiff !== 0) return tierDiff;
    return a.priority - b.priority;
  });
}

export interface HookVerdictCheck {
  event: string;
  hookTier: HookTier;
  verdict: 'allow' | 'deny' | 'ask';
  isStricter: boolean;
}

export function isStricterVerdict(current: 'allow' | 'deny' | 'ask', previous: 'allow' | 'deny' | 'ask'): boolean {
  const rank = { allow: 0, ask: 1, deny: 2 };
  return rank[current] > rank[previous];
}

export class HookChainGuard {
  private _blocked: Array<{ event: string; tier: HookTier; reason: string }> = [];

  checkRegistration(event: string, tier: HookTier): boolean {
    if (canHookEvent(event, tier)) return true;
    this._blocked.push({
      event,
      tier,
      reason: `User-tier hook cannot intercept protected event "${event}"`,
    });
    return false;
  }

  getBlocked(): ReadonlyArray<{ event: string; tier: HookTier; reason: string }> {
    return this._blocked;
  }

  getBlockedCount(): number {
    return this._blocked.length;
  }

  clear(): void {
    this._blocked = [];
  }
}
