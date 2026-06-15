import { BeliefTracker } from './belief';
import { Critic } from './critic';
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
    this.belief = new BeliefTracker(this.config);
    this.critic = new Critic(this.config.criticAlpha, this.config.criticBeta);
    this.memoryGate = new MemoryGate();
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
export { MemoryGate, GatedMemoryEntry } from './memory-gate';
export { Retriever } from './retriever';
export type { SuperInferenceConfig, BeliefState, CriticDecision, StopReason } from './types';
export { DEFAULT_CONFIG } from './types';
