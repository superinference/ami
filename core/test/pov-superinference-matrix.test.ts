import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  SuperInferenceEngine,
  BeliefTracker,
  Critic,
  MemoryGate,
  Retriever,
  DEFAULT_CONFIG,
} from '../src/superinference/index';
import type { CriticDecision } from '../src/superinference/types';

// ===========================================================================
// pov-superinference-matrix — behavioral regression matrix over the POMDP
// formalism. We assert the REAL decisions / stop-reasons / belief values the
// code returns for crafted states, computing expected values from the same
// equations the paper (and the code) implement.
// ===========================================================================

// ---------------------------------------------------------------------------
// Fake critic LLM stream — DI seam so Critic.evaluate() never hits a network.
// ---------------------------------------------------------------------------
function fakeStreamOf(text: string) {
  // Matches `typeof streamChatCompletion`: called with (provider, messages,
  // tools, abortSignal), returns an async iterable of chunks.
  return async function* (): AsyncGenerator<{ type: string; text?: string }> {
    // split across chunks to prove accumulation
    const mid = Math.floor(text.length / 2);
    yield { type: 'content_delta', text: text.slice(0, mid) };
    yield { type: 'other_event' }; // non-content chunk must be ignored
    yield { type: 'content_delta', text: text.slice(mid) };
  } as any;
}

async function* throwingStream(): AsyncGenerator<{ type: string; text?: string }> {
  throw new Error('network exploded');
}

const PROVIDER = {} as any;
function makeCritic(text: string, alpha = 0.05, beta = 0.1) {
  return new Critic(alpha, beta, fakeStreamOf(text) as any);
}

// ===========================================================================
// 1. BeliefTracker.update — Equation 1 (deterministic belief dynamics)
// ===========================================================================
describe('BeliefTracker.update — Eq.1 belief dynamics', () => {
  const cases: Array<{
    name: string;
    b0: number;
    steps: Array<{ approved: boolean; score?: number }>;
    expected: number;
  }> = [
    {
      name: 'single approval score=0.8, λ+=0.35 from b0=0.3',
      b0: 0.3,
      steps: [{ approved: true, score: 0.8 }],
      // 0.3 + 0.35*(0.8-0.3) = 0.475
      expected: 0.475,
    },
    {
      name: 'single rejection λ-=0.6 from b0=0.5',
      b0: 0.5,
      steps: [{ approved: false }],
      // 0.6 * 0.5 = 0.30
      expected: 0.3,
    },
    {
      name: 'approval with missing score defaults to 1.0',
      b0: 0.3,
      steps: [{ approved: true }],
      // 0.3 + 0.35*(1-0.3) = 0.545
      expected: 0.545,
    },
    {
      name: 'rejection floors at 0.25',
      b0: 0.3,
      steps: [{ approved: false }, { approved: false }],
      // 0.6*0.3=0.18 -> clamp 0.25 ; 0.6*0.25=0.15 -> clamp 0.25
      expected: 0.25,
    },
    {
      name: 'repeated approvals ceil at 0.95',
      b0: 0.9,
      steps: [
        { approved: true, score: 1 },
        { approved: true, score: 1 },
        { approved: true, score: 1 },
      ],
      expected: 0.95,
    },
    {
      name: 'score clamped below 0 behaves as 0',
      b0: 0.5,
      steps: [{ approved: true, score: -5 }],
      // 0.5 + 0.35*(0-0.5) = 0.325
      expected: 0.325,
    },
    {
      name: 'score clamped above 1 behaves as 1',
      b0: 0.5,
      steps: [{ approved: true, score: 9 }],
      // 0.5 + 0.35*(1-0.5) = 0.675
      expected: 0.675,
    },
  ];

  for (const c of cases) {
    it(c.name, () => {
      const bt = new BeliefTracker({ initialBelief: c.b0 });
      for (const s of c.steps) bt.update(s.approved, s.score);
      assert.ok(
        Math.abs(bt.belief - c.expected) < 1e-9,
        `belief=${bt.belief} expected≈${c.expected}`,
      );
      assert.equal(bt.step, c.steps.length);
      // Invariant: belief always inside [0.25, 0.95]
      assert.ok(bt.belief >= 0.25 && bt.belief <= 0.95);
    });
  }
});

// ===========================================================================
// 2. BeliefTracker.entropy — Eq.7 binary entropy
// ===========================================================================
describe('BeliefTracker.entropy — Eq.7', () => {
  const bt = new BeliefTracker();
  it('H(0.5) == 1.0 (max binary entropy)', () => {
    assert.ok(Math.abs(bt.entropy(0.5) - 1.0) < 1e-12);
  });
  it('H(0.25) == H(0.75) (symmetry)', () => {
    assert.ok(Math.abs(bt.entropy(0.25) - bt.entropy(0.75)) < 1e-12);
  });
  it('H(0) and H(1) and out-of-range are 0', () => {
    assert.equal(bt.entropy(0), 0);
    assert.equal(bt.entropy(1), 0);
    assert.equal(bt.entropy(-0.2), 0);
    assert.equal(bt.entropy(1.5), 0);
  });
  it('entropy() with no arg uses current belief', () => {
    const t = new BeliefTracker({ initialBelief: 0.5 });
    assert.ok(Math.abs(t.entropy() - 1.0) < 1e-12);
  });
});

// ===========================================================================
// 3. BeliefTracker.eig — Eq.8 expected information gain (properties)
// ===========================================================================
describe('BeliefTracker.eig — Eq.8', () => {
  it('eig is non-negative and below current entropy', () => {
    const t = new BeliefTracker({ initialBelief: 0.5 });
    const e = t.eig();
    assert.ok(e >= 0, `eig=${e}`);
    assert.ok(e < t.entropy(0.5));
  });
  it('eig is 0 at the 0.25 belief floor (clamped rejection posterior)', () => {
    const t = new BeliefTracker({ initialBelief: 0.3 });
    t.update(false); // 0.18 -> clamp 0.25
    assert.equal(t.belief, 0.25);
    // At the floor the clamp forces the posterior; eig collapses to ~0.
    assert.ok(t.eig() < 1e-9, `eig=${t.eig()}`);
  });
});

// ===========================================================================
// 4. shouldStop() / engine.shouldContinue() — StopReason matrix (§2.6)
// ===========================================================================
describe('StopReason matrix — §2.6 termination', () => {
  it('none: fresh engine keeps reasoning', () => {
    const eng = new SuperInferenceEngine();
    const r = eng.shouldContinue();
    assert.equal(r.continue, true);
    assert.equal(r.reason.type, 'none');
    assert.match(r.reason.detail, /Continue/);
  });

  it('confidence: belief >= κ stops with type=confidence', () => {
    // κ lowered so the proxy score can reach it.
    const eng = new SuperInferenceEngine({ initialBelief: 0.3, confidenceThreshold: 0.4 });
    eng.belief.update(true, 0.8); // -> 0.475 >= 0.4
    const r = eng.shouldContinue();
    assert.equal(r.continue, false);
    assert.equal(r.reason.type, 'confidence');
    assert.match(r.reason.detail, /κ=0\.4/);
  });

  it('diminishing_returns: belief held and EIG < τ (high τ forces it)', () => {
    // τ=1.0 makes EIG < τ at b0; belief not regressed -> diminishing_returns.
    const eng = new SuperInferenceEngine({ initialBelief: 0.3, eigThreshold: 1.0 });
    const r = eng.shouldContinue();
    assert.equal(r.continue, false);
    assert.equal(r.reason.type, 'diminishing_returns');
    assert.match(r.reason.detail, /τ=1/);
  });

  it('diminishing_returns is SUPPRESSED when belief regressed below b0', () => {
    // Even with τ=1.0, a rejection drops belief below b0 -> recover, not stop.
    const eng = new SuperInferenceEngine({ initialBelief: 0.5, eigThreshold: 1.0, maxSteps: 25 });
    eng.belief.update(false); // 0.3 < b0=0.5
    const r = eng.shouldContinue();
    assert.equal(r.continue, true);
    assert.equal(r.reason.type, 'none');
  });

  it('budget: step >= N_max stops with type=budget (below b0 so no diminishing)', () => {
    const eng = new SuperInferenceEngine({ initialBelief: 0.3, maxSteps: 2, confidenceThreshold: 0.9 });
    eng.belief.update(false); // step1, 0.25
    eng.belief.update(false); // step2, 0.25 (< b0=0.3)
    const r = eng.shouldContinue();
    assert.equal(r.continue, false);
    assert.equal(r.reason.type, 'budget');
    assert.match(r.reason.detail, /N_max=2/);
  });

  it('confidence takes precedence over budget', () => {
    const eng = new SuperInferenceEngine({ initialBelief: 0.3, confidenceThreshold: 0.4, maxSteps: 1 });
    eng.belief.update(true, 0.8); // 0.475 >= 0.4 AND step 1 >= 1
    assert.equal(eng.shouldContinue().reason.type, 'confidence');
  });
});

// ===========================================================================
// 5. applyRates / applyThreshold / constructor validation
// ===========================================================================
describe('BeliefTracker rate + threshold mutation and validation', () => {
  it('constructor rejects out-of-range λ+', () => {
    assert.throws(() => new BeliefTracker({ lambdaPlus: 0 }), RangeError);
    assert.throws(() => new BeliefTracker({ lambdaPlus: 1.5 }), RangeError);
  });
  it('constructor rejects out-of-range λ-', () => {
    assert.throws(() => new BeliefTracker({ lambdaMinus: 0 }), RangeError);
    assert.throws(() => new BeliefTracker({ lambdaMinus: 1 }), RangeError);
  });
  it('applyRates changes the next update', () => {
    const t = new BeliefTracker({ initialBelief: 0.5, lambdaPlus: 0.35 });
    t.applyRates({ lambdaPlus: 1.0, lambdaMinus: 0.5 });
    assert.equal(t.lambdaPlus, 1.0);
    assert.equal(t.lambdaMinus, 0.5);
    t.update(true, 0.9); // λ+=1 -> b jumps straight to s=0.9
    assert.ok(Math.abs(t.belief - 0.9) < 1e-9, `belief=${t.belief}`);
  });
  it('applyRates rejects invalid values', () => {
    const t = new BeliefTracker();
    assert.throws(() => t.applyRates({ lambdaPlus: 2, lambdaMinus: 0.5 }), RangeError);
    assert.throws(() => t.applyRates({ lambdaPlus: 0.5, lambdaMinus: 1 }), RangeError);
  });
  it('applyThreshold lets a confidence stop fire at proxy ceiling', () => {
    const t = new BeliefTracker({ initialBelief: 0.3 });
    t.update(true, 0.8); // 0.475
    assert.equal(t.shouldStop().type, 'none'); // κ=0.9 default not reached
    t.applyThreshold(0.4);
    assert.equal(t.shouldStop().type, 'confidence');
  });
  it('applyThreshold rejects invalid κ', () => {
    const t = new BeliefTracker();
    assert.throws(() => t.applyThreshold(0), RangeError);
    assert.throws(() => t.applyThreshold(1.5), RangeError);
  });
});

// ===========================================================================
// 6. Critic.ppv — Eq.4 positive predictive value
// ===========================================================================
describe('Critic.ppv — Eq.4', () => {
  it("paper example p'=0.7 -> PPV ~0.977", () => {
    const c = new Critic(0.05, 0.1);
    assert.ok(Math.abs(c.ppv(0.7) - 0.9771) < 1e-3, `ppv=${c.ppv(0.7)}`);
  });
  it('ppv(0) == 0 and ppv(1) == 1', () => {
    const c = new Critic(0.05, 0.1);
    assert.equal(c.ppv(0), 0);
    assert.ok(Math.abs(c.ppv(1) - 1) < 1e-12);
  });
  it('denominator-zero guard returns 0 (α=0, prior=0)', () => {
    const c = new Critic(0, 0.1);
    assert.equal(c.ppv(0), 0);
  });
  it('exposes alpha/beta rates', () => {
    const c = new Critic(0.07, 0.2);
    assert.equal(c.alphaRate, 0.07);
    assert.equal(c.betaRate, 0.2);
  });
});

// ===========================================================================
// 7. Critic.extractJSON — balanced-brace parsing
// ===========================================================================
describe('Critic.extractJSON', () => {
  const c = new Critic();
  it('extracts a plain object', () => {
    assert.deepEqual(c.extractJSON('{"approved":true}'), { approved: true });
  });
  it('extracts object embedded in prose', () => {
    assert.deepEqual(c.extractJSON('sure: {"score":0.5} done'), { score: 0.5 });
  });
  it('handles nested braces in a string value', () => {
    const out = c.extractJSON('{"reason":"use {x} here","approved":false}');
    assert.deepEqual(out, { reason: 'use {x} here', approved: false });
  });
  it('handles escaped quotes inside strings', () => {
    const out = c.extractJSON('{"reason":"say \\"hi\\"","approved":true}');
    assert.deepEqual(out, { reason: 'say "hi"', approved: true });
  });
  it('returns null when no opening brace', () => {
    assert.equal(c.extractJSON('no json here'), null);
  });
  it('returns null on unbalanced / unterminated braces', () => {
    assert.equal(c.extractJSON('{"a":1'), null);
  });
  it('returns null on invalid JSON within balanced braces', () => {
    assert.equal(c.extractJSON('{not valid}'), null);
  });
});

// ===========================================================================
// 8. Critic.evaluate — CriticDecision matrix via DI stream seam
// ===========================================================================
describe('Critic.evaluate — CriticDecision matrix', () => {
  const signal = new AbortController().signal;

  const cases: Array<{
    name: string;
    json: string;
    expect: Partial<CriticDecision>;
  }> = [
    {
      name: 'approved with explicit score',
      json: '{"approved": true, "score": 0.9, "reason": "looks good"}',
      expect: { approved: true, score: 0.9, reason: 'looks good' },
    },
    {
      name: 'rejected with explicit score',
      json: '{"approved": false, "score": 0.2, "reason": "incomplete"}',
      expect: { approved: false, score: 0.2, reason: 'incomplete' },
    },
    {
      name: 'approved missing score defaults to 1.0',
      json: '{"approved": true, "reason": "ok"}',
      expect: { approved: true, score: 1.0 },
    },
    {
      name: 'rejected missing score defaults to 0.0',
      json: '{"approved": false, "reason": "no"}',
      expect: { approved: false, score: 0.0 },
    },
    {
      name: 'score above 1 is clamped to 1',
      json: '{"approved": true, "score": 5}',
      expect: { approved: true, score: 1.0 },
    },
    {
      name: 'score below 0 is clamped to 0',
      json: '{"approved": true, "score": -3}',
      expect: { approved: true, score: 0.0 },
    },
    {
      name: 'non-numeric score on approval -> 1.0',
      json: '{"approved": true, "score": "high"}',
      expect: { approved: true, score: 1.0 },
    },
  ];

  for (const tc of cases) {
    it(tc.name, async () => {
      const critic = makeCritic(tc.json);
      const d = await critic.evaluate('q', 'r', PROVIDER, signal);
      for (const [k, v] of Object.entries(tc.expect)) {
        assert.equal((d as any)[k], v, `${tc.name}: ${k}`);
      }
    });
  }

  it('no JSON in response -> fail closed', async () => {
    const critic = makeCritic('I have no opinion.');
    const d = await critic.evaluate('q', 'r', PROVIDER, signal);
    assert.equal(d.approved, false);
    assert.equal(d.score, 0.0);
    assert.match(d.reason!, /no valid JSON/);
  });

  it('stream throwing -> fail closed with error message', async () => {
    const critic = new Critic(0.05, 0.1, throwingStream as any);
    const d = await critic.evaluate('q', 'r', PROVIDER, signal);
    assert.equal(d.approved, false);
    assert.equal(d.score, 0.0);
    assert.match(d.reason!, /network exploded/);
  });
});

// ===========================================================================
// 9. MemoryGate — Eq.5 gating
// ===========================================================================
describe('MemoryGate — Eq.5', () => {
  let gate: MemoryGate;
  beforeEach(() => { gate = new MemoryGate(); });

  it('approved decision is gated in; rejected is not', () => {
    assert.equal(gate.gate('q1', 'r1', { approved: true, score: 0.8 }, 0.5, 1), true);
    assert.equal(gate.gate('q2', 'r2', { approved: false, score: 0.1 }, 0.4, 2), false);
    const entries = gate.getEntries();
    assert.equal(entries.length, 1);
    assert.equal(entries[0].query, 'q1');
    assert.equal(entries[0].criticScore, 0.8);
    assert.equal(entries[0].belief, 0.5);
    assert.equal(entries[0].step, 1);
  });

  it('getEntries returns a copy (mutation isolation)', () => {
    gate.gate('q', 'r', { approved: true, score: 1 }, 0.5, 1);
    const snap = gate.getEntries();
    snap.push({} as any);
    assert.equal(gate.getEntries().length, 1);
  });

  it('getApprovedContext formats Q/A and truncates result to 500 chars', () => {
    const long = 'x'.repeat(600);
    gate.gate('qq', long, { approved: true, score: 1 }, 0.5, 1);
    const ctx = gate.getApprovedContext();
    assert.match(ctx, /^Q: qq\nA: x{500}$/);
  });

  it('reset clears entries', () => {
    gate.gate('q', 'r', { approved: true, score: 1 }, 0.5, 1);
    gate.reset();
    assert.equal(gate.getEntries().length, 0);
    assert.equal(gate.getApprovedContext(), '');
  });
});

// ===========================================================================
// 10. Retriever — Eq.2 noise channel (deterministic at η=0 and η=1)
// ===========================================================================
function mockMemory(opts?: {
  instructions?: string;
  memories?: Array<{ content: string }>;
}) {
  return {
    loadProjectInstructions: () => opts?.instructions ?? '',
    loadMemories: () =>
      (opts?.memories ?? []).map((m) => ({ path: 'p', content: m.content, type: 'memory' as const })),
  } as any;
}

describe('Retriever — Eq.2 noise channel', () => {
  it('clamps noiseLevel into [0,1]', () => {
    assert.equal(new Retriever(-1).noiseLevel, 0);
    assert.equal(new Retriever(5).noiseLevel, 1);
    assert.equal(new Retriever(0.3).noiseLevel, 0.3);
  });

  it('η=0 perfect retrieval: instructions + approved + relevant memories', () => {
    const r = new Retriever(0);
    const gate = new MemoryGate();
    gate.gate('deploy the service', 'approved answer', { approved: true, score: 1 }, 0.5, 1);
    const mem = mockMemory({
      instructions: 'PROJECT RULES',
      memories: [{ content: 'how to deploy the service correctly' }],
    });
    const out = r.retrieve('deploy service', mem, gate);
    assert.match(out, /PROJECT RULES/);
    assert.match(out, /Previously Approved Results/);
    assert.match(out, /approved answer/);
    assert.match(out, /Stored Memories/);
    assert.match(out, /how to deploy/);
  });

  it('η=1 maximally degraded: only instructions survive', () => {
    const r = new Retriever(1);
    const gate = new MemoryGate();
    gate.gate('q', 'approved answer', { approved: true, score: 1 }, 0.5, 1);
    const mem = mockMemory({
      instructions: 'ONLY RULES',
      memories: [{ content: 'some relevant memory text' }],
    });
    const out = r.retrieve('relevant memory', mem, gate);
    assert.equal(out, 'ONLY RULES');
  });

  it('empty memory + empty gate + no instructions -> empty string', () => {
    const r = new Retriever(0);
    const out = r.retrieve('anything', mockMemory(), new MemoryGate());
    assert.equal(out, '');
  });

  it('query relevance filters out non-matching memories (η=0)', () => {
    const r = new Retriever(0);
    const mem = mockMemory({
      memories: [
        { content: 'kubernetes deployment notes' },
        { content: 'completely unrelated cooking recipe' },
      ],
    });
    const out = r.retrieve('kubernetes', mem, new MemoryGate());
    assert.match(out, /kubernetes deployment/);
    assert.doesNotMatch(out, /cooking recipe/);
  });

  it('empty query keeps all memories (relevance=1)', () => {
    const r = new Retriever(0);
    const mem = mockMemory({ memories: [{ content: 'alpha' }, { content: 'beta' }] });
    const out = r.retrieve('', mem, new MemoryGate());
    assert.match(out, /alpha/);
    assert.match(out, /beta/);
  });
});

// ===========================================================================
// 11. SuperInferenceEngine — config merge, getState, reset wiring
// ===========================================================================
describe('SuperInferenceEngine — config + state + reset', () => {
  it('merges partial config over DEFAULT_CONFIG', () => {
    const eng = new SuperInferenceEngine({ initialBelief: 0.6, noiseLevel: 0.42 });
    assert.equal(eng.config.initialBelief, 0.6);
    assert.equal(eng.config.noiseLevel, 0.42);
    assert.equal(eng.config.confidenceThreshold, DEFAULT_CONFIG.confidenceThreshold);
    assert.equal(eng.config.maxSteps, DEFAULT_CONFIG.maxSteps);
    assert.equal(eng.retriever.noiseLevel, 0.42);
    assert.equal(eng.critic.alphaRate, DEFAULT_CONFIG.criticAlpha);
  });

  it('getState returns belief snapshot plus ppv(value)', () => {
    const eng = new SuperInferenceEngine({ initialBelief: 0.3 });
    const s = eng.getState();
    assert.equal(s.value, 0.3);
    assert.equal(s.step, 0);
    assert.ok(Math.abs(s.entropy - eng.belief.entropy(0.3)) < 1e-12);
    // ppv(0.3) with default α/β
    const expectedPpv = eng.critic.ppv(0.3);
    assert.ok(Math.abs(s.ppv - expectedPpv) < 1e-12);
    assert.ok(Math.abs(expectedPpv - 0.8852459) < 1e-5, `ppv=${expectedPpv}`);
  });

  it('reset restores belief to clamped initial and clears gate', () => {
    const eng = new SuperInferenceEngine({ initialBelief: 0.3 });
    eng.belief.update(true, 0.8);
    eng.memoryGate.gate('q', 'r', { approved: true, score: 1 }, 0.5, 1);
    assert.notEqual(eng.getState().value, 0.3);
    assert.equal(eng.memoryGate.getEntries().length, 1);
    eng.reset();
    assert.equal(eng.getState().value, 0.3);
    assert.equal(eng.getState().step, 0);
    assert.equal(eng.memoryGate.getEntries().length, 0);
  });

  it('initialBelief is clamped into [0.25,0.95] at construction and reset', () => {
    const low = new SuperInferenceEngine({ initialBelief: 0.01 });
    assert.equal(low.getState().value, 0.25);
    const high = new SuperInferenceEngine({ initialBelief: 5 });
    assert.equal(high.getState().value, 0.95);
    high.belief.update(false);
    high.reset();
    assert.equal(high.getState().value, 0.95);
  });

  it('getRetriever / getMemoryGate expose the wired instances', () => {
    const eng = new SuperInferenceEngine();
    assert.equal(eng.getRetriever(), eng.retriever);
    assert.equal(eng.getMemoryGate(), eng.memoryGate);
  });
});
