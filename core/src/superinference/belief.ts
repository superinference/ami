import { SuperInferenceConfig, BeliefState, StopReason, DEFAULT_CONFIG } from './types';

export class BeliefTracker {
  private _bt: number;       // Current belief b_t
  private _b0: number;       // Clamped initial belief b_0 (collapse reference)
  private _step: number = 0;
  private config: Required<SuperInferenceConfig>;

  constructor(config?: Partial<SuperInferenceConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    // FUNCTIONAL (hardening): a NaN/±Infinity rate passes the bare `<= 0 || > 1`
    // comparisons (every comparison with NaN is false), silently admitting a
    // non-finite λ that poisons every later update to NaN. Require finiteness.
    if (!Number.isFinite(this.config.lambdaPlus) || this.config.lambdaPlus <= 0 || this.config.lambdaPlus > 1) {
      throw new RangeError(`lambdaPlus must be a finite number in (0, 1], got ${this.config.lambdaPlus}`);
    }
    if (!Number.isFinite(this.config.lambdaMinus) || this.config.lambdaMinus <= 0 || this.config.lambdaMinus >= 1) {
      throw new RangeError(`lambdaMinus must be a finite number in (0, 1), got ${this.config.lambdaMinus}`);
    }
    this._b0 = BeliefTracker.clampBelief(this.config.initialBelief);
    this._bt = this._b0;
  }

  /**
   * NaN/±Infinity-safe clamp of a belief value into the paper's [0.25, 0.95]
   * band. `Math.max(0.25, Math.min(0.95, NaN))` is NaN, so without this guard a
   * single non-finite input (initial belief, a corrupted update) would poison
   * the belief for the rest of the run. A non-finite value collapses to the
   * 0.25 floor — the lowest-confidence, most conservative in-range value.
   * FUNCTIONAL (hardening): previously a non-finite belief propagated as NaN.
   */
  private static clampBelief(x: number): number {
    if (!Number.isFinite(x)) return 0.25;
    return Math.max(0.25, Math.min(0.95, x));
  }

  get belief(): number { return this._bt; }
  get step(): number { return this._step; }
  get lambdaPlus(): number { return this.config.lambdaPlus; }
  get lambdaMinus(): number { return this.config.lambdaMinus; }
  get confidenceThreshold(): number { return this.config.confidenceThreshold; }

  /**
   * Replace λ+ and λ−. The tracker keeps a private copy of config, so writing
   * the engine's si.config does not change the next update or shouldStop().
   */
  applyRates(next: { lambdaPlus: number; lambdaMinus: number }): void {
    // FUNCTIONAL (hardening): reject non-finite rates for the same reason as the
    // constructor — NaN/±Infinity slips past bare range comparisons.
    if (!Number.isFinite(next.lambdaPlus) || next.lambdaPlus <= 0 || next.lambdaPlus > 1) {
      throw new RangeError(`lambdaPlus must be a finite number in (0, 1], got ${next.lambdaPlus}`);
    }
    if (!Number.isFinite(next.lambdaMinus) || next.lambdaMinus <= 0 || next.lambdaMinus >= 1) {
      throw new RangeError(`lambdaMinus must be a finite number in (0, 1), got ${next.lambdaMinus}`);
    }
    this.config = { ...this.config, lambdaPlus: next.lambdaPlus, lambdaMinus: next.lambdaMinus };
  }

  /**
   * Replace κ. Same private-config reason as applyRates. The proxy score 0.8
   * never reaches the paper default 0.9, so a confidence stop needs this.
   */
  applyThreshold(kappa: number): void {
    // FUNCTIONAL (hardening): a non-finite κ passed the bare range check and then
    // disabled the confidence stop forever (b_t >= NaN is always false).
    if (!Number.isFinite(kappa) || kappa <= 0 || kappa > 1) {
      throw new RangeError(`confidenceThreshold must be a finite number in (0, 1], got ${kappa}`);
    }
    this.config = { ...this.config, confidenceThreshold: kappa };
  }

  // Equation 1: Belief update
  // b_{t+1} = b_t + λ_+(s - b_t)  if positive (Critic approves)
  // b_{t+1} = λ_- · b_t           if negative (Critic rejects)
  update(approved: boolean, criticScore?: number): void {
    this._step++;
    if (approved) {
      // FUNCTIONAL (hardening): a non-finite critic score (NaN/±Infinity) would
      // flow through the clamp unchanged and corrupt belief to NaN permanently.
      // Treat it as the default full-confidence score 1.0 (same as an omitted
      // score), so a malformed critic signal can never poison the belief.
      const raw = criticScore ?? 1.0;
      const s = Number.isFinite(raw) ? Math.max(0, Math.min(1, raw)) : 1.0;
      this._bt = this._bt + this.config.lambdaPlus * (s - this._bt);
    } else {
      this._bt = this.config.lambdaMinus * this._bt;
    }
    // Clamp to [0.25, 0.95] as per paper (NaN-safe — see clampBelief)
    this._bt = BeliefTracker.clampBelief(this._bt);
  }

  // Equation 7: Binary entropy
  // H(b_t) = -b_t log₂(b_t) - (1 - b_t) log₂(1 - b_t)
  entropy(b?: number): number {
    const p = b ?? this._bt;
    // FUNCTIONAL (hardening): the bare `p <= 0 || p >= 1` guard does NOT catch
    // NaN (every comparison with NaN is false), so entropy(NaN) returned NaN. A
    // degenerate/invalid belief carries zero binary entropy.
    if (!Number.isFinite(p) || p <= 0 || p >= 1) return 0;
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

    // Belief after approval (Eq. 1 with s=1) — NaN-safe clamp
    const btPlusApproved = BeliefTracker.clampBelief(
      bt + this.config.lambdaPlus * (1.0 - bt)
    );

    // Belief after rejection (Eq. 1) — NaN-safe clamp
    const btPlusRejected = BeliefTracker.clampBelief(
      this.config.lambdaMinus * bt
    );

    // EIG = current entropy - expected future entropy
    const currentEntropy = this.entropy(bt);
    const expectedEntropy = pPlus * this.entropy(btPlusApproved) +
                           (1 - pPlus) * this.entropy(btPlusRejected);

    // FUNCTIONAL (hardening): `Math.max(0, NaN)` is NaN, so a non-finite
    // intermediate (e.g. a non-finite critic α/β feeding p_+) produced a NaN EIG
    // that silently disabled the diminishing-returns stop (`NaN < τ` is false)
    // and poisoned getState(). EIG is always a finite, non-negative bit count.
    const raw = currentEntropy - expectedEntropy;
    return Number.isFinite(raw) ? Math.max(0, raw) : 0;
  }

  // §2.6: Termination criteria
  shouldStop(): StopReason {
    if (this._bt >= this.config.confidenceThreshold) {
      return { type: 'confidence', detail: `Belief ${this._bt.toFixed(3)} ≥ κ=${this.config.confidenceThreshold}` };
    }
    // Diminishing returns means belief has CONVERGED (held or gained ground with
    // little marginal information left), not COLLAPSED. At/near the 0.25 belief
    // floor, eig() is forced to 0 by the clamp on the rejection posterior, so a
    // sub-threshold EIG there signals a struggling agent (collapse), not
    // convergence. Only stop for diminishing returns when belief has not regressed
    // below its starting point; otherwise keep reasoning so the agent can recover.
    if (this._bt >= this._b0 && this.eig() < this.config.eigThreshold) {
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
    this._bt = BeliefTracker.clampBelief(this.config.initialBelief);
    this._step = 0;
  }
}
