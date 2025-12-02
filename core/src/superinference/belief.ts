import { SuperInferenceConfig, BeliefState, StopReason, DEFAULT_CONFIG } from './types';

export class BeliefTracker {
  private _bt: number;       // Current belief b_t
  private _step: number = 0;
  private config: Required<SuperInferenceConfig>;

  constructor(config?: Partial<SuperInferenceConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    if (this.config.lambdaPlus <= 0 || this.config.lambdaPlus > 1) {
      throw new RangeError(`lambdaPlus must be in (0, 1], got ${this.config.lambdaPlus}`);
    }
    if (this.config.lambdaMinus <= 0 || this.config.lambdaMinus >= 1) {
      throw new RangeError(`lambdaMinus must be in (0, 1), got ${this.config.lambdaMinus}`);
    }
    this._bt = Math.max(0.25, Math.min(0.95, this.config.initialBelief));
  }

  get belief(): number { return this._bt; }
  get step(): number { return this._step; }

  // Equation 1: Belief update
  // b_{t+1} = b_t + λ_+(s - b_t)  if positive (Critic approves)
  // b_{t+1} = λ_- · b_t           if negative (Critic rejects)
  update(approved: boolean, criticScore?: number): void {
    this._step++;
    if (approved) {
      const s = Math.max(0, Math.min(1, criticScore ?? 1.0));
      this._bt = this._bt + this.config.lambdaPlus * (s - this._bt);
    } else {
      this._bt = this.config.lambdaMinus * this._bt;
    }
    // Clamp to [0.25, 0.95] as per paper
    this._bt = Math.max(0.25, Math.min(0.95, this._bt));
  }

  // Equation 7: Binary entropy
  // H(b_t) = -b_t log₂(b_t) - (1 - b_t) log₂(1 - b_t)
  entropy(b?: number): number {
    const p = b ?? this._bt;
    if (p <= 0 || p >= 1) return 0;
    return -p * Math.log2(p) - (1 - p) * Math.log2(1 - p);
  }

  // Equation 8: Expected Information Gain
  // EIG_t = H(b_t) - [p_+ · H(b_{t+1}^+) + (1 - p_+) · H(b_{t+1}^-)]
  // where p_+ = b_t(1 - β) + (1 - b_t)α
  eig(): number {
    const bt = this._bt;
    const α = this.config.criticAlpha;
    const β = this.config.criticBeta;

    // Probability of approval
    const pPlus = bt * (1 - β) + (1 - bt) * α;

    // Belief after approval (Eq. 1 with s=1)
    const btPlusApproved = Math.max(0.25, Math.min(0.95,
      bt + this.config.lambdaPlus * (1.0 - bt)
    ));

    // Belief after rejection (Eq. 1)
    const btPlusRejected = Math.max(0.25, Math.min(0.95,
      this.config.lambdaMinus * bt
    ));

    // EIG = current entropy - expected future entropy
    const currentEntropy = this.entropy(bt);
    const expectedEntropy = pPlus * this.entropy(btPlusApproved) +
                           (1 - pPlus) * this.entropy(btPlusRejected);

    return Math.max(0, currentEntropy - expectedEntropy);
  }

  // §2.6: Termination criteria
  shouldStop(): StopReason {
    if (this._bt >= this.config.confidenceThreshold) {
      return { type: 'confidence', detail: `Belief ${this._bt.toFixed(3)} ≥ κ=${this.config.confidenceThreshold}` };
    }
    if (this.eig() < this.config.eigThreshold) {
      return { type: 'diminishing_returns', detail: `EIG ${this.eig().toFixed(4)} < τ=${this.config.eigThreshold}` };
    }
    if (this._step >= this.config.maxSteps) {
      return { type: 'budget', detail: `Step ${this._step} ≥ N_max=${this.config.maxSteps}` };
    }
    return { type: 'none', detail: 'Continue reasoning' };
  }

  // Get full state snapshot
  getState(): BeliefState {
    return {
      value: this._bt,
      entropy: this.entropy(),
      eig: this.eig(),
      step: this._step,
    };
  }

  // Reset
  reset(): void {
    this._bt = this.config.initialBelief;
    this._step = 0;
  }
}
