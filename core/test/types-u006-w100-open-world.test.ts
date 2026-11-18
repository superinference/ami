/**
 * U006 (w100) — uniform, input-aware open-world (SSRF/egress-surface) marker.
 *
 * Parity: Claude's `Tool.isOpenWorld?(input)` — the reference declares the marker
 * per-tool and consults it input-aware (`tool.isOpenWorld?.(input)`, cli/print.ts,
 * MCPToolDetailView). AMI's `ToolDefinition.isOpenWorld` was a plain boolean, so a
 * tool could not declare egress for a SPECIFIC input only. These tests cover the
 * widened `boolean | ((input) => boolean)` field and the single resolver
 * {@link resolveToolOpenWorld} that both the boolean and predicate forms flow through.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { resolveToolOpenWorld } from '../src/types';
import type { ToolDefinition } from '../src/types';

describe('resolveToolOpenWorld (input-aware open-world marker)', () => {
  it('returns false when the flag is absent (closed-world default)', () => {
    assert.equal(resolveToolOpenWorld({}), false);
    assert.equal(resolveToolOpenWorld({ isOpenWorld: undefined }), false);
  });

  it('resolves the plain boolean form', () => {
    assert.equal(resolveToolOpenWorld({ isOpenWorld: true }), true);
    assert.equal(resolveToolOpenWorld({ isOpenWorld: false }), false);
  });

  it('consults an input-aware predicate with the supplied input', () => {
    const tool: Pick<ToolDefinition, 'isOpenWorld'> = {
      isOpenWorld: (input) => typeof input.url === 'string' && /^https?:/i.test(input.url),
    };
    assert.equal(resolveToolOpenWorld(tool, { url: 'https://example.com' }), true);
    assert.equal(resolveToolOpenWorld(tool, { url: 'file:///etc/passwd' }), false);
    // Default input ({}) — the tool-level badge/introspection query — is closed-world
    // here because no url is present. Parity: Claude's `tool.isOpenWorld?.({})`.
    assert.equal(resolveToolOpenWorld(tool), false);
  });

  it('coerces a truthy non-boolean predicate return to a strict boolean', () => {
    // A predicate that returns a truthy non-boolean must NOT leak as open-world
    // unless it is exactly `true` (=== true), matching the reference consult.
    const tool: Pick<ToolDefinition, 'isOpenWorld'> = {
      isOpenWorld: (() => 'yes' as unknown as boolean),
    };
    assert.equal(resolveToolOpenWorld(tool, {}), false);
  });

  it('degrades a THROWING predicate to false (never propagates)', () => {
    const tool: Pick<ToolDefinition, 'isOpenWorld'> = {
      isOpenWorld: () => {
        throw new Error('boom');
      },
    };
    assert.doesNotThrow(() => resolveToolOpenWorld(tool, { url: 'https://x' }));
    assert.equal(resolveToolOpenWorld(tool, { url: 'https://x' }), false);
  });
});
