/**
 * U102 — MCP name normalization parity (hyphen preservation + `__`-delimiter
 * protection), verified at the mcp-auth tool boundary.
 *
 * Gap (confirmed, parity): `normalizeMcpNameSegment` used to replace EVERY
 * character outside `[a-zA-Z0-9_]` with `_`, collapsing a configured hyphen so a
 * `my-server` MCP config surfaced as `mcp__my_server__authenticate` — a name that
 * no longer matched the server the user configured and diverged from the reference's
 * `normalizeNameForMCP` (services/mcp/normalization.ts), which preserves hyphens
 * to stay inside the API pattern `^[a-zA-Z0-9_-]{1,64}$`.
 *
 * Fix: preserve hyphens (`[^a-zA-Z0-9_-]` → `_`) while KEEPING the
 * underscore-collapse + edge-strip step that guarantees no segment can inject a
 * spurious `__` delimiter. Because `createMcpAuthTool` builds its tool name from
 * the same normalizer, the authenticate pseudo-tool's name now matches the
 * server's real (hyphenated) name and the inverse parser round-trips it back to a
 * single server segment.
 *
 * Each assertion discriminates the fix from the pre-fix behavior (RED on revert).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { createMcpAuthTool } from '../src/tools/mcp-auth';
import {
  normalizeMcpNameSegment,
  qualifiedMcpToolName,
  parseMcpToolName,
} from '../src/tools/mcp-tool';

describe('U102 — MCP name normalization: hyphen preservation', () => {
  it('normalizeMcpNameSegment keeps hyphens (RED on revert: would be my_server)', () => {
    assert.equal(normalizeMcpNameSegment('my-server'), 'my-server');
    assert.equal(normalizeMcpNameSegment('context-7'), 'context-7');
  });

  it('still replaces genuinely-illegal chars while preserving hyphens', () => {
    assert.equal(normalizeMcpNameSegment('my-server.v2'), 'my-server_v2');
    assert.equal(normalizeMcpNameSegment('a-b/c d'), 'a-b_c_d');
  });

  it('the authenticate pseudo-tool name preserves the server hyphen', () => {
    const tool = createMcpAuthTool('my-server');
    // RED on revert: pre-fix this was `mcp__my_server__authenticate`.
    assert.equal(tool.name, 'mcp__my-server__authenticate');
  });

  it('qualified tool name preserves hyphens in both server and tool segments', () => {
    assert.equal(
      qualifiedMcpToolName('my-server', 'list-pods'),
      'mcp__my-server__list-pods',
    );
  });
});

describe('U102 — `__`-delimiter protection is retained with hyphens', () => {
  it('collapses underscore runs and strips edges so no spurious `__` is injected', () => {
    // A name mixing hyphens with consecutive illegal chars must not round-trip
    // into an ambiguous multi-segment tool name.
    assert.equal(normalizeMcpNameSegment('a--b'), 'a--b'); // hyphens are not underscores → untouched
    assert.equal(normalizeMcpNameSegment('a..b'), 'a_b');
    assert.equal(normalizeMcpNameSegment('_x-y_'), 'x-y');
  });

  it('a hyphenated server with illegal chars round-trips to exactly one server segment', () => {
    const name = qualifiedMcpToolName('my-server.io!', 'do/thing');
    assert.equal(name, 'mcp__my-server_io__do_thing');
    const parsed = parseMcpToolName(name);
    assert.equal(parsed?.serverName, 'my-server_io');
    assert.equal(parsed?.toolName, 'do_thing');
  });
});
