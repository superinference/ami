<div align="center">

# SuperInference Core

### Building blocks for sovereign AI agents

[![Tests](https://github.com/superinference/ami/actions/workflows/ci.yml/badge.svg)](https://github.com/superinference/ami/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue?style=flat-square)](https://github.com/superinference/ami/blob/main/LICENSE)

</div>

---

The core library for [SuperInference](https://www.superinference.org) — provider abstraction, tool definitions, permission rules, session management, and the POMDP-based reasoning framework from the [SuperInference paper](https://www.superinference.org/research/).

This is the building-blocks layer. The orchestration glue (engine loop, system prompt, context compaction) lives in the private `common/` layer.

## What's Inside

- **Provider** — Multi-provider streaming, model detection, auto-detection, credential pooling
- **Tools** — File ops, shell, code intelligence, web, git, MCP, task management, scheduling, workflow orchestration
- **SuperInference** — POMDP formalism: belief tracker, LLM critic, critic-gated memory, noisy retrieval channel
- **Permissions** — Pattern-based rules, bash command classification, path safety
- **Session** — Persistence, search, memory, conversation summary
- **Security** — Credential guard, environment protection, input sanitization
- **Observability** — Analytics, cost tracking, profiler, turn metrics

## Provider Support

Any OpenAI-compatible API works via the [Vercel AI SDK](https://sdk.vercel.ai/). Providers are auto-detected from API key prefixes and base URLs. See [docs](https://www.superinference.org/docs/) for the full list.

## SuperInference — POMDP Formalism

Implements the PRE loop (Plan, Retrieve, Execute) with critic-gated memory and information-theoretic stopping criteria from the [SuperInference paper](https://www.superinference.org/research/). When enabled, the engine tracks a belief state, evaluates tool results through an LLM critic, gates memory writes, and stops reasoning when mathematically justified.

See the [research page](https://www.superinference.org/research/) for the full formalism.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

## Links

| | |
|---|---|
| **Website** | [www.superinference.org](https://www.superinference.org) |
| **Docs** | [www.superinference.org/docs/](https://www.superinference.org/docs/) |
| **Research** | [www.superinference.org/research/](https://www.superinference.org/research/) |
| **GitHub** | [github.com/superinference](https://github.com/superinference) |
