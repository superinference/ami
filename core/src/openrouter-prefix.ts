// Extracted leaf: openrouter/ model-id prefix stripping. Dependency-free so
// openrouter-zdr.ts imports it without an openrouter-zdr -> provider cycle.

/**
 * OpenRouter is addressed with an `openrouter/<vendor>/<model>` namespace so a
 * model id self-identifies its provider (and never collides with a native
 * vendor route). OpenRouter's own OpenAI-compatible API expects the bare
 * `<vendor>/<model>` slug, so the `openrouter/` segment is stripped before the
 * id reaches the wire.
 *
 * Exception: OpenRouter's OWN meta-routers live natively under the same prefix
 * as single tokens — `openrouter/auto` (best-model router) and `openrouter/free`
 * (random free-model router) — and MUST be sent verbatim (`openrouter/free`
 * stripped to `free` is rejected with "No endpoints available"). So we strip
 * only when the remainder is itself a `vendor/model` slug (contains a `/`); a
 * single-token remainder is a native OpenRouter id and is left untouched. The
 * `:free` suffix needs no handling — it rides along inside the model segment
 * (`openrouter/nvidia/nemotron:free` → `nvidia/nemotron:free`).
 */
export function stripOpenRouterPrefix(model: string): string {
  if (!model.startsWith('openrouter/')) return model;
  const rest = model.slice('openrouter/'.length);
  return rest.includes('/') ? rest : model;
}
