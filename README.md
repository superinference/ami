# SuperInference Core

[![Tests](https://github.com/ccamacho/claw/actions/workflows/ci.yml/badge.svg)](https://github.com/ccamacho/claw/actions/workflows/ci.yml)

The core engine for the [SuperInference](https://www.superinference.org) AI code assistant. This library implements an agentic engine with multi-provider LLM support, 12 built-in tools, and a POMDP-based reasoning framework from the [SuperInference paper](https://www.superinference.org).

## Overview

SuperInference Core provides three layers of AI reasoning:

1. **Agentic Loop** — Multi-turn tool-use conversation loop (read files, edit code, run commands, search the web)
2. **Thinking Models** — Extended reasoning support for Claude, OpenAI o-series, Gemini 2.5, DeepSeek R1
3. **SuperInference Mode** — POMDP-based belief tracking with information-theoretic stopping criteria

## Architecture

```
core/src/
  engine.ts              Agentic conversation loop
  provider.ts            Vercel AI SDK multi-provider streaming
  types.ts               Shared type definitions
  tool-executor.ts       Tool execution with permissions
  model-capabilities.ts  Thinking model detection
  model-registry.ts      Provider auto-detection + model validation
  permissions.ts         Pattern-based permission rules
  system-prompt.ts       Dynamic system prompt builder
  context-manager.ts     LLM-powered context compaction
  memory.ts              CLAUDE.md / SUPERINFERENCE.md instruction loading
  session.ts             Session persistence
  cost-tracker.ts        Per-model token pricing and cost tracking
  error-classifier.ts    9 error categories with recovery strategies
  analytics.ts           JSONL event logging
  checkpoint.ts          File mutation checkpointing with restore
  hooks.ts               Post-sampling, stop, error lifecycle hooks
  profiler.ts            Operation timing profiler
  skills.ts              Skills and agent definitions (YAML frontmatter)
  workspace-indexer.ts   Symbol extraction for TS/Python/Go/Rust/Java

  superinference/        Paper formalisms
    types.ts             Configuration and state interfaces
    belief.ts            Belief tracker (Eq. 1, 7, 8)
    critic.ts            LLM-based critic (Eq. 3, 4)
    memory-gate.ts       Critic-gated memory (Eq. 5)
    retriever.ts         Noisy retrieval channel (Eq. 2)
    index.ts             SuperInferenceEngine composition

  tools/                 12 built-in tools
    bash.ts              Shell command execution
    file-read.ts         Read files with line numbers
    file-write.ts        Write/create files
    file-edit.ts         Search-and-replace editing
    grep.ts              Regex text search
    glob.ts              File pattern matching
    list-dir.ts          Directory listing
    notebook-edit.ts     Jupyter notebook cell editing
    search-symbols.ts    Workspace symbol search
    web-fetch.ts         HTTP content fetching
    web-search.ts        DuckDuckGo search with content extraction
```

## Provider Support

The engine works with any OpenAI-compatible API via the [Vercel AI SDK](https://sdk.vercel.ai/):

| Provider | Models | Thinking Support |
|----------|--------|-----------------|
| **OpenAI** | GPT-4o, GPT-4.1, o1, o3, o4-mini | `reasoningEffort` |
| **Google** | Gemini 2.0 Flash, 2.5 Pro/Flash | `thinkingConfig` |
| **Anthropic** | Claude Sonnet 4, Opus 4 | `experimental_thinking` |
| **DeepSeek** | R1, Reasoner | Inline `<think>` tags |
| **Ollama** | Any local model | - |
| **OpenRouter** | Any model | Via upstream provider |

Provider auto-detection from API key patterns:
- `sk-ant-...` → Anthropic
- `AIza...` → Google
- `sk-or-...` → OpenRouter
- `sk-...` → OpenAI
- Localhost URLs → Ollama

## Thinking Models

The `model-capabilities.ts` module provides automatic detection and configuration:

```typescript
import { isReasoningModel, resolveTemperature, resolveThinkingBudget } from './model-capabilities';

isReasoningModel('claude-opus-4');     // true
isReasoningModel('o3-mini');           // true
isReasoningModel('gemini-2.5-pro');    // true
isReasoningModel('gpt-4o');            // false

// Temperature must be omitted for Claude (requires 1) and o-series (must not be set)
resolveTemperature('claude-opus-4', 0, { enabled: true }); // undefined
resolveTemperature('o3-mini', 0.5, { enabled: true });     // undefined
resolveTemperature('gemini-2.5-pro', 0, { enabled: true }); // 0 (pass-through)
```

Five thinking levels map to provider-specific budgets:

| Level | Budget Tokens | Use Case |
|-------|--------------|----------|
| `off` | 0 | No thinking |
| `low` | 4,096 | Quick tasks |
| `medium` | 10,240 | General reasoning |
| `high` | 16,384 | Complex problems |
| `max` | 32,768 | Deep analysis |

---

## SuperInference Mode — Formal Specification

SuperInference mode implements the POMDP-based reasoning pipeline from the paper. When enabled, the engine tracks a belief state, evaluates tool results through an LLM critic, gates memory writes, and stops reasoning when mathematically justified.

### POMDP Formulation (§2.1)

The agent's reasoning is formalized as a Partially Observable Markov Decision Process:

**M = (S, A, O, T, Z, R, ρ, γ)** where:
- **S** — hidden reasoning state space (task progress, unresolved subgoals)
- **A** — action space (planning queries, retrieval, execution)
- **O** — observation space (critic decisions, execution results)
- **T: S × A → Δ(S)** — transition function
- **Z: S × A → Δ(O)** — observation function (noisy channels)
- **R: S × A → ℝ** — reward function
- **ρ ∈ Δ(S)** — initial state distribution
- **γ ∈ (0,1)** — discount factor

### Belief Update — Equation 1

The agent maintains a scalar belief b_t ∈ [0.25, 0.95]:

```
b_{t+1} = b_t + λ_+(s - b_t)    if Critic approves (positive observation)
b_{t+1} = λ_- · b_t              if Critic rejects (negative observation)
```

Where:
- **s** — critic score ∈ [0, 1]
- **λ_+** — approval interpolation rate (default 0.35)
- **λ_-** — rejection decay factor (default 0.6)

Implementation: `superinference/belief.ts` → `BeliefTracker.update()`

### Noisy Retrieval Channel — Equation 2

```
m̃_t ~ C_η(m_t)
```

Memory retrieval is modeled as a corruption channel parameterized by noise level η. With probability η, each retrieved entry is independently dropped. η=0 yields perfect retrieval; η=1 yields maximally degraded retrieval.

The retriever also filters by query relevance using keyword overlap scoring.

Implementation: `superinference/retriever.ts` → `Retriever.retrieve()` + `applyNoiseChannel()`

### Critic Error Model — Equation 3

The critic can make two types of errors:

```
α = P(approve | incorrect)    — false approval rate (default 0.05)
β = P(reject | correct)       — false rejection rate (default 0.10)
```

When SuperInference mode is active, the engine calls `Critic.evaluate()` which asks the LLM to assess each tool result, returning an approve/reject decision with a confidence score.

Implementation: `superinference/critic.ts` → `Critic.evaluate()`

### Positive Predictive Value — Equation 4

When the critic approves, the probability it's actually correct:

```
PPV = P(correct | approve) = (1-β)·p' / ((1-β)·p' + α·(1-p'))
```

Where p' is the prior probability of correctness (current belief). With default parameters (α=0.05, β=0.10, p'=0.7): PPV ≈ 0.977 — an approval is 97.7% likely to be correct.

Implementation: `superinference/critic.ts` → `Critic.ppv()`

### Critic-Gated Memory — Equation 5

Memory grows only when the critic approves:

```
M_{t+1} = M_t ∪ {(q_t, a_t, metadata)}    if approved
M_{t+1} = M_t                               if rejected
```

Rejected results leave no trace, preventing incorrect intermediate results from contaminating future reasoning.

Implementation: `superinference/memory-gate.ts` → `MemoryGate.gate()`

### Binary Entropy — Equation 7

Since b_t is a Bernoulli parameter over {correct, incorrect}:

```
H(b_t) = -b_t·log₂(b_t) - (1-b_t)·log₂(1-b_t)
```

H(0.5) = 1 bit (maximum uncertainty), H(0.95) ≈ 0.29 bits (high confidence).

Implementation: `superinference/belief.ts` → `BeliefTracker.entropy()`

### Expected Information Gain — Equation 8

```
EIG_t = H(b_t) - [p_+·H(b⁺_{t+1}) + (1-p_+)·H(b⁻_{t+1})]
```

Where:
- p_+ = b_t(1-β) + (1-b_t)α — probability of approval
- b⁺_{t+1} — belief after hypothetical approval (via Eq. 1)
- b⁻_{t+1} — belief after hypothetical rejection (via Eq. 1)

High EIG means the next step is informative. Low EIG means we've learned most of what we can.

Implementation: `superinference/belief.ts` → `BeliefTracker.eig()`

### Stopping Criteria — §2.6

Reasoning stops when any of:

1. **High confidence**: b_t ≥ κ (default κ = 0.9)
2. **Diminishing returns**: EIG_t < τ (default τ = 0.01)
3. **Budget exceeded**: t ≥ N_max (default N_max = 25)

Implementation: `superinference/belief.ts` → `BeliefTracker.shouldStop()`

### Algorithm 1 — PRE Loop with Critic-Gated Memory

```
Input: task x, thresholds τ, κ, budgets N_max
Initialize belief b_0 = 0.3, memory M_0 ← ∅, step t ← 0

WHILE not terminated:
  Compute EIG_t                                    (Eq. 8)
  IF EIG_t < τ OR t ≥ N_max: BREAK
  Planner proposes query q_t
  Retriever returns m̃_t via noisy channel          (Eq. 2)
  Executor produces candidate a_t
  Critic evaluates a_t → {approve, reject}         (Eq. 3)
  IF approved: M_{t+1} ← M_t ∪ {(q_t, a_t)}      (Eq. 5)
  ELSE: M_{t+1} ← M_t
  Update belief b_{t+1}                            (Eq. 1)
  IF b_{t+1} ≥ κ: BREAK
  t ← t + 1

RETURN final answer and memory M_t
```

### Configuration

All parameters are configurable via `SuperInferenceConfig`:

| Parameter | Symbol | Default | Description |
|-----------|--------|---------|-------------|
| `initialBelief` | b₀ | 0.3 | Starting confidence |
| `confidenceThreshold` | κ | 0.9 | Stop when belief reaches this |
| `eigThreshold` | τ | 0.01 | Stop when EIG drops below |
| `criticAlpha` | α | 0.05 | False approval rate |
| `criticBeta` | β | 0.10 | False rejection rate |
| `lambdaPlus` | λ₊ | 0.35 | Approval interpolation rate |
| `lambdaMinus` | λ₋ | 0.6 | Rejection decay factor |
| `maxSteps` | N_max | 25 | Maximum reasoning steps |
| `noiseLevel` | η | 0.1 | Retrieval noise level |
| `successScore` | — | 0.8 | Proxy critic score (success) |
| `errorScore` | — | 0.3 | Proxy critic score (error) |
| `useLLMCritic` | — | true | Use LLM-based critic evaluation |

---

## Permission System

Centralized permission handling with pattern-based rules:

- **PermissionPromptHandler** interface — implemented by consumers
- **PermissionPromptResult** — `allow_once`, `allow_pattern`, or `deny`
- **Pattern matching** — glob patterns (e.g., `git *`, `npm *`, `*`)
- **Bash classification** — commands classified as safe/unsafe/destructive
- **Path safety** — blocks writes to system directories

## Development

```bash
cd core
npm install
npm test                                        # 1,000+ tests
npx tsc --noEmit                                # Type check
npx eslint src/ --ext .ts --max-warnings 0      # Security lint
npm audit --omit=dev --audit-level=high          # CVE scan
```

## Testing

The test suite includes:
- **Unit tests** — every module, every equation, every edge case
- **Integration tests** — engine loop with mock SSE servers
- **SuperInference tests** — all 8 equations verified with paper examples
- **Thinking model tests** — model detection, temperature, budgets
- **Permission tests** — handler flow, rule creation, parity
- **Feature parity tests** — consumers must implement the same interfaces

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

## Links

- Website: [superinference.org](https://www.superinference.org)
- GitHub: [github.com/superinference](https://github.com/superinference)
