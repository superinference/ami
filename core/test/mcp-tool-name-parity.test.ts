import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  createMcpTool,
  qualifiedMcpToolName,
  normalizeMcpNameSegment,
  parseMcpToolName,
} from '../src/tools/mcp-tool';

// ---------------------------------------------------------------------------
// MCP tool-name parity: normalization hardening (collapse/strip to protect the
// `__` delimiter) and the centralized inverse parser. Mirrors Claude Code's
// normalizeNameForMCP / mcpInfoFromString.
// ---------------------------------------------------------------------------

describe('normalizeMcpNameSegment — delimiter protection', () => {
  it('replaces illegal characters with a single underscore', () => {
    assert.equal(normalizeMcpNameSegment('my.server'), 'my_server');
  });

  it('collapses consecutive underscores so no segment can inject a `__` delimiter', () => {
    // `a..b` would otherwise normalize to `a__b`, colliding with the delimiter.
    assert.equal(normalizeMcpNameSegment('a..b'), 'a_b');
    assert.equal(normalizeMcpNameSegment('a__b'), 'a_b');
    assert.equal(normalizeMcpNameSegment('x///y!!!z'), 'x_y_z');
  });

  it('strips leading and trailing underscores', () => {
    assert.equal(normalizeMcpNameSegment('_foo_'), 'foo');
    assert.equal(normalizeMcpNameSegment('!bar!'), 'bar');
  });

  it('leaves already-clean names untouched', () => {
    assert.equal(normalizeMcpNameSegment('nmap_scan'), 'nmap_scan');
    assert.equal(normalizeMcpNameSegment('kali'), 'kali');
  });

  it('preserves hyphens (API pattern ^[a-zA-Z0-9_-]{1,64}$) instead of collapsing them to `_`', () => {
    // Parity: hyphens are valid in MCP server/tool names; keep them so the
    // normalized segment stays faithful to the user's configured name.
    assert.equal(normalizeMcpNameSegment('my-server'), 'my-server');
    assert.equal(normalizeMcpNameSegment('context-7'), 'context-7');
    // Only non-[a-zA-Z0-9_-] characters are replaced; hyphens survive alongside.
    assert.equal(normalizeMcpNameSegment('my-server.v2'), 'my-server_v2');
  });
});

describe('qualifiedMcpToolName — hardened normalization', () => {
  it('strips the trailing underscore a special char used to leave (hyphens preserved)', () => {
    // Previously `do/thing!` -> `do_thing_`; the edge underscore is now stripped.
    // The server's hyphen is preserved (only `.` becomes `_`): `my-server.v2` -> `my-server_v2`.
    assert.equal(qualifiedMcpToolName('my-server.v2', 'do/thing!'), 'mcp__my-server_v2__do_thing');
  });

  it('cannot produce a spurious `__` inside a segment (round-trips to one server)', () => {
    const name = qualifiedMcpToolName('a..b', 'c');
    assert.equal(name, 'mcp__a_b__c');
    const parsed = parseMcpToolName(name);
    assert.equal(parsed?.serverName, 'a_b');
    assert.equal(parsed?.toolName, 'c');
  });
});

describe('parseMcpToolName — centralized boundary-safe inverse parser', () => {
  it('splits mcp__server__tool into its parts', () => {
    assert.deepEqual(parseMcpToolName('mcp__slack__send_message'), {
      serverName: 'slack',
      toolName: 'send_message',
    });
  });

  it('preserves double underscores inside the tool name by rejoining', () => {
    assert.deepEqual(parseMcpToolName('mcp__srv__a__b'), {
      serverName: 'srv',
      toolName: 'a__b',
    });
  });

  it('returns toolName undefined for a bare server prefix', () => {
    assert.deepEqual(parseMcpToolName('mcp__srv'), {
      serverName: 'srv',
      toolName: undefined,
    });
  });

  it('returns null for non-MCP or malformed names', () => {
    assert.equal(parseMcpToolName('bash'), null);
    assert.equal(parseMcpToolName('notmcp__srv__tool'), null);
    assert.equal(parseMcpToolName('mcp__'), null); // empty server name
    assert.equal(parseMcpToolName(''), null);
  });
});

describe('createMcpTool → parseMcpToolName round-trip (end-to-end)', () => {
  it('a tool built from special-char server/tool names parses back to the normalized server', () => {
    // A server name with a run of invalid chars would, without delimiter
    // protection, inject a spurious `__` and corrupt the inverse parse.
    const tool = createMcpTool('weather.api  v2', 'get forecast', 'desc', { type: 'object', properties: {} });
    assert.equal(tool.name, 'mcp__weather_api_v2__get_forecast');
    const parsed = parseMcpToolName(tool.name);
    assert.equal(parsed?.serverName, 'weather_api_v2');
    assert.equal(parsed?.toolName, 'get_forecast');
  });
});
