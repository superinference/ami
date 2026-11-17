import { BeliefTracker } from './belief';
import { Critic, isCriticDisabledByEnv } from './critic';
import { MemoryGate } from './memory-gate';
import { Retriever } from './retriever';
import type { SuperInferenceConfig, BeliefState, CriticDecision, StopReason } from './types';
import { DEFAULT_CONFIG } from './types';

export class SuperInferenceEngine {
  readonly belief: BeliefTracker;
  readonly critic: Critic;
  readonly memoryGate: MemoryGate;
  readonly retriever: Retriever;
  readonly config: Required<SuperInferenceConfig>;

  constructor(config?: Partial<SuperInferenceConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    // Env kill switch: force the proxy critic when the LLM critic is disabled via
    // CLAUDE_CODE_DISABLE_CRITIC / SUPERINFERENCE_DISABLE_CRITIC, so the engine's
    // existing `useLLMCritic` gate skips the auxiliary model call. Parity: Claude's
    // advisor CLAUDE_CODE_DISABLE_ADVISOR_TOOL override.
    if (isCriticDisabledByEnv()) {
      this.config = { ...this.config, useLLMCritic: false };
    }
    this.belief = new BeliefTracker(this.config);
    // Thread the dedicated reviewer model (parity: Claude's advisor `advisorModel`) into
    // the critic so Critic.evaluate() runs on it instead of always inheriting the main
    // loop model. '' (default) keeps inheriting the caller's model.
    this.critic = new Critic(this.config.criticAlpha, this.config.criticBeta, undefined, this.config.criticModel);
    this.memoryGate = new MemoryGate({
      maxEntries: this.config.memoryMaxEntries,
      maxEmitEntries: this.config.memoryMaxEmitEntries,
      stalenessDays: this.config.memoryStalenessDays,
    });
    this.retriever = new Retriever(this.config.noiseLevel);
  }

  /** Check if we should continue reasoning. */
  shouldContinue(): { continue: boolean; reason: StopReason } {
    const reason = this.belief.shouldStop();
    return { continue: reason.type === 'none', reason };
  }

  /** Get current state for UI/logging. */
  getState(): BeliefState & { ppv: number } {
    const state = this.belief.getState();
    return {
      ...state,
      ppv: this.critic.ppv(state.value),
    };
  }

  /** Reset for new task. */
  reset(): void {
    this.belief.reset();
    this.memoryGate.reset();
  }

  getRetriever(): Retriever { return this.retriever; }
  getMemoryGate(): MemoryGate { return this.memoryGate; }
}

// Re-export everything
export { BeliefTracker } from './belief';
export { Critic } from './critic';
export { MemoryGate, type GatedMemoryEntry, type ConsolidationStatus, type MemoryGateOptions } from './memory-gate';
export { Retriever } from './retriever';
export type { SuperInferenceConfig, BeliefState, CriticDecision, StopReason } from './types';
export { DEFAULT_CONFIG } from './types';
