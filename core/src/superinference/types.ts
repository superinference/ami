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
  /** When true, the engine may end the session on shouldStop(). Default false. shouldStop() itself does not read this. */
  enforcing?: boolean;
  /** Dedicated model for the LLM critic/reviewer; '' (default) inherits the main-loop
   *  model. Lets the critic run on a cheaper/stronger model than the agent loop
   *  (parity: Claude's advisor `advisorModel`/`baseModel`). */
  criticModel?: string;
  /** Whether the user may override {@link criticModel} (true) or it is pinned by an
   *  experiment/managed policy (false). When false, UI/config surfaces must treat the
   *  critic model as fixed. Default true. Parity: Claude's advisor `canUserConfigure`. */
  canUserConfigureCriticModel?: boolean;
}

export interface CriticDecision {
  approved: boolean;
  score: number;      // s ∈ [0, 1] — critic's confidence score
  reason?: string;
  /** Set when the critic could not render a genuine verdict (network error, timeout,
   *  aborted stream, or unparseable output) — DISTINCT from a substantive rejection, so
   *  the engine can soften the belief floor instead of treating it as a real "no".
   *  Parity: Claude's AdvisorMessage "Advisor unavailable (code)" error branch. */
  unavailable?: boolean;
  /** Machine-readable error code when {@link unavailable} is set. */
  errorCode?: string;
  /** Model that produced this critic/advisor verdict (the resolved critic model, which
   *  may differ from the main-loop model), for review attribution in the transcript.
   *  Parity: Claude's advisor-message model attribution. */
  model?: string;
  /** True when the advisor's reasoning came back encrypted/redacted yet the verdict is
   *  still a COMPLETE review (not an {@link unavailable} failure) — the score/approval
   *  stand; only the rationale was withheld. Parity: Claude's redacted-review completion
   *  state (a redacted advisor output is still a finished review). */
  redacted?: boolean;
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
  useLLMCritic: false,
  enforcing: false,
  criticModel: '',
  canUserConfigureCriticModel: true,
};
