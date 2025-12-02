import type { Message, AssistantMessage } from './types';
import { log as coreLog } from './logger';

export type HookFn = (context: HookContext) => Promise<void>;

export interface HookContext {
  messages: Message[];
  lastAssistantMessage?: AssistantMessage;
  toolResults?: Array<{ toolName: string; output: string; isError: boolean }>;
  turnCount: number;
}

// ---------------------------------------------------------------------------
// Extended hook context types
// ---------------------------------------------------------------------------

export interface PreToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
}

export interface PostToolUseContext extends HookContext {
  toolName: string;
  toolInput: Record<string, unknown>;
  toolOutput: string;
  isError: boolean;
}

export interface PreSamplingContext extends HookContext {
  apiMessages: Message[];
}

export type HookDecision =
  | { action: 'allow' }
  | { action: 'deny'; reason: string }
  | { action: 'modify'; updatedInput?: Record<string, unknown>; updatedMessages?: Message[] };

// ---------------------------------------------------------------------------
// Hook function types for new hooks
// ---------------------------------------------------------------------------

export type PreToolUseHookFn = (context: PreToolUseContext) => Promise<HookDecision>;
export type PostToolUseHookFn = (context: PostToolUseContext) => Promise<void>;
export type PreSamplingHookFn = (context: PreSamplingContext) => Promise<HookDecision>;

// ---------------------------------------------------------------------------
// HookManager
// ---------------------------------------------------------------------------

export class HookManager {
  private postSamplingHooks: HookFn[] = [];
  private stopHooks: HookFn[] = [];
  private errorHooks: HookFn[] = [];
  private preToolUseHooks: PreToolUseHookFn[] = [];
  private postToolUseHooks: PostToolUseHookFn[] = [];
  private preSamplingHooks: PreSamplingHookFn[] = [];

  // ---- Existing registration methods (backward-compatible) ----

  onPostSampling(fn: HookFn): void {
    this.postSamplingHooks.push(fn);
  }

  onStop(fn: HookFn): void {
    this.stopHooks.push(fn);
  }

  onError(fn: HookFn): void {
    this.errorHooks.push(fn);
  }

  // ---- New registration methods ----

  onPreToolUse(fn: PreToolUseHookFn): void {
    this.preToolUseHooks.push(fn);
  }

  onPostToolUse(fn: PostToolUseHookFn): void {
    this.postToolUseHooks.push(fn);
  }

  onPreSampling(fn: PreSamplingHookFn): void {
    this.preSamplingHooks.push(fn);
  }

  // ---- Existing execution methods (backward-compatible, now with error logging) ----

  async executePostSampling(ctx: HookContext): Promise<void> {
    for (const hook of this.postSamplingHooks) {
      try {
        await hook(ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
  }

  async executeStop(ctx: HookContext): Promise<void> {
    for (const hook of this.stopHooks) {
      try {
        await hook(ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
  }

  async executeError(ctx: HookContext): Promise<void> {
    for (const hook of this.errorHooks) {
      try {
        await hook(ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
  }

  // ---- New execution methods ----

  /**
   * Execute pre-tool-use hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreToolUse(ctx: PreToolUseContext): Promise<HookDecision> {
    return this.executeDecisionHooks(this.preToolUseHooks, ctx);
  }

  /**
   * Execute post-tool-use hooks. Fire-and-forget, errors are logged.
   */
  async executePostToolUse(ctx: PostToolUseContext): Promise<void> {
    for (const hook of this.postToolUseHooks) {
      try {
        await hook(ctx);
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }
  }

  /**
   * Execute pre-sampling hooks. First deny wins; modify decisions are accumulated
   * (later modify hooks can override earlier ones).
   */
  async executePreSampling(ctx: PreSamplingContext): Promise<HookDecision> {
    return this.executeDecisionHooks(this.preSamplingHooks, ctx);
  }

  /**
   * Shared decision-hook executor. First deny wins; modify decisions are
   * accumulated (later hooks override earlier ones). Errors are logged.
   */
  private async executeDecisionHooks<T extends HookContext>(
    hooks: Array<(ctx: T) => Promise<HookDecision>>,
    ctx: T,
  ): Promise<HookDecision> {
    let accumulated: HookDecision = { action: 'allow' };

    for (const hook of hooks) {
      try {
        const decision = await hook(ctx);
        if (decision.action === 'deny') {
          return decision;
        }
        if (decision.action === 'modify') {
          accumulated = {
            action: 'modify',
            updatedInput: decision.updatedInput ?? (accumulated.action === 'modify' ? accumulated.updatedInput : undefined),
            updatedMessages: decision.updatedMessages ?? (accumulated.action === 'modify' ? accumulated.updatedMessages : undefined),
          };
        }
      } catch (err) {
        coreLog('hooks', 'Hook error', { error: String(err) });
      }
    }

    return accumulated;
  }
}
