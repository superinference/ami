export interface SuperInferenceConfig {
  enabled: boolean;
  initialBelief?: number;        // b_0 (default 0.3)
  confidenceThreshold?: number;  // κ (default 0.9)
  eigThreshold?: number;         // τ (default 0.01)
  criticAlpha?: number;          // α false approval rate (default 0.05)
  criticBeta?: number;           // β false rejection rate (default 0.10)
  lambdaPlus?: number;           // λ+ approval interpolation (default 0.35)
  lambdaMinus?: number;          // λ- rejection decay (default 0.6)
  maxSteps?: number;             // N_max budget (default 25)
  noiseLevel?: number;           // η retrieval noise (default 0.1)
  successScore?: number;         // proxy critic score on tool success (default 0.8)
  errorScore?: number;           // proxy critic score on tool error (default 0.3)
  useLLMCritic?: boolean;        // use real LLM-based critic evaluation (default false)
}

export interface CriticDecision {
  approved: boolean;
  score: number;      // s ∈ [0, 1] — critic's confidence score
  reason?: string;
}

export interface BeliefState {
  value: number;      // b_t ∈ [0.25, 0.95]
  entropy: number;    // H(b_t)
  eig: number;        // EIG_t
  step: number;       // t
}

export interface StopReason {
  type: 'confidence' | 'diminishing_returns' | 'budget' | 'none';
  detail: string;
}

export const DEFAULT_CONFIG: Required<SuperInferenceConfig> = {
  enabled: true,
  initialBelief: 0.3,
  confidenceThreshold: 0.9,
  eigThreshold: 0.01,
  criticAlpha: 0.05,
  criticBeta: 0.10,
  lambdaPlus: 0.35,
  lambdaMinus: 0.6,
  maxSteps: 25,
  noiseLevel: 0.1,
  successScore: 0.8,
  errorScore: 0.3,
  useLLMCritic: true,
};
