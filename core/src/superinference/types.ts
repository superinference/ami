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
   *  (parity: the reference's advisor `advisorModel`/`baseModel`). */
  criticModel?: string;
  /** Whether the user may override {@link criticModel} (true) or it is pinned by an
   *  experiment/managed policy (false). When false, UI/config surfaces must treat the
   *  critic model as fixed. Default true. Parity: the reference's advisor `canUserConfigure`. */
  canUserConfigureCriticModel?: boolean;
  /** Max critic-approved entries retained in the memory gate's ring (oldest/lowest-scored
   *  evicted past this). Promotes the gate's hardcoded cap to a config knob. Default 100. */
  memoryMaxEntries?: number;
  /** Max entries the gate emits (top-N by critic score) into retriever/persisted context.
   *  Default: inherits {@link memoryMaxEntries}. Parity: the reference's top-N memory selection. */
  memoryMaxEmitEntries?: number;
  /** Age (days) past which a gate entry carries an inline staleness caveat when emitted.
   *  Default 7. Parity: the reference's memoryFreshnessText staleness threshold. */
  memoryStalenessDays?: number;
}

export interface CriticDecision {
  approved: boolean;
  score: number;      // s ∈ [0, 1] — critic's confidence score
  reason?: string;
  /** Set when the critic could not render a genuine verdict (network error, timeout,
   *  aborted stream, or unparseable output) — DISTINCT from a substantive rejection, so
   *  the engine can soften the belief floor instead of treating it as a real "no".
   *  Parity: the reference's AdvisorMessage "Advisor unavailable (code)" error branch. */
  unavailable?: boolean;
  /** Machine-readable error code when {@link unavailable} is set. */
  errorCode?: string;
  /** Model that produced this critic/advisor verdict (the resolved critic model, which
   *  may differ from the main-loop model), for review attribution in the transcript.
   *  Parity: the reference's advisor-message model attribution. */
  model?: string;
  /** True when the advisor's reasoning came back encrypted/redacted yet the verdict is
   *  still a COMPLETE review (not an {@link unavailable} failure) — the score/approval
   *  stand; only the rationale was withheld. Parity: the reference's redacted-review completion
   *  state (a redacted advisor output is still a finished review). */
  redacted?: boolean;
  /** Token usage for THIS auxiliary reviewer call, tagged with the resolved reviewer
   *  {@link model}, so a usage view can attribute reviewer-model spend distinctly from
   *  the main loop. Absent when the provider reported no usage (e.g. an unavailable
   *  review). Parity: the reference's getAdvisorUsage (per-auxiliary-model usage breakdown). */
  usage?: {
    model: string;
    promptTokens: number;
    completionTokens: number;
    totalTokens: number;
  };
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

/** Minimal view of a `superinference` config block carrying only the critic-model knob
 *  and its lock flag — the inputs the reviewer-model resolution reads from a given tier. */
type CriticModelKnob = Pick<SuperInferenceConfig, 'criticModel' | 'canUserConfigureCriticModel'>;

/**
 * Resolve the effective critic/reviewer model from a USER config tier and a
 * MANAGED (operator/experiment policy) tier, honouring {@link SuperInferenceConfig.canUserConfigureCriticModel}.
 *
 * Parity: the reference's advisor `canUserConfigure` lock. When a managed policy pins the
 * reviewer model (`canUserConfigureCriticModel === false`) the user's `criticModel`
 * override is IGNORED and the pinned managed model stands (or `''` = inherit the main-loop
 * model when the policy locked it without naming its own model). Absent a managed opinion,
 * a user config may self-declare the lock. When user-configurable, the user's model wins,
 * falling back to any managed default.
 *
 * Returns the resolved model string, or `undefined` when NEITHER tier configures a critic
 * model nor the lock flag — so the caller can omit the key and keep the DEFAULT_CONFIG
 * ('' = inherit) behaviour unchanged.
 */
export function resolveCriticModelLock(
  userSi: CriticModelKnob | undefined,
  managedSi: CriticModelKnob | undefined,
): string | undefined {
  const managedLock = managedSi?.canUserConfigureCriticModel;
  const userLock = userSi?.canUserConfigureCriticModel;
  const userModel = userSi?.criticModel;
  const managedModel = managedSi?.criticModel;
  if (
    managedLock === undefined && userLock === undefined &&
    userModel === undefined && managedModel === undefined
  ) {
    return undefined;
  }
  // Pinned when the managed policy locks it (managed wins), or — absent a managed opinion —
  // when the user config self-declares the lock.
  const locked = managedLock === false || (managedLock === undefined && userLock === false);
  if (locked) return managedModel ?? '';
  return userModel ?? managedModel ?? '';
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
  memoryMaxEntries: 100,
  memoryMaxEmitEntries: 100,
  memoryStalenessDays: 7,
};
