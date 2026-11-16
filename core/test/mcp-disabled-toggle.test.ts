import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { resolveDisabledMcpServers, McpManager } from '../src/mcp/manager';
import { mergeConfigs, KNOWN_CONFIG_KEYS, findUnknownConfigKeys } from '../src/config';

// Parity: Claude Code's disabledMcpServers / enabledMcpServers per-server user
// toggles (disable a configured server without removing it; re-enable by name).
describe('resolveDisabledMcpServers', () => {
  it('returns the disabled names when nothing is re-enabled', () => {
    assert.deepEqual(resolveDisabledMcpServers(['a', 'b']), ['a', 'b']);
  });

  it('drops names that are re-enabled (enable wins per-name)', () => {
    assert.deepEqual(resolveDisabledMcpServers(['a', 'b'], ['b']), ['a']);
  });

  it('matches the enable override case-insensitively', () => {
    assert.deepEqual(resolveDisabledMcpServers(['Foo'], ['foo']), []);
    assert.deepEqual(resolveDisabledMcpServers(['foo'], ['FOO']), []);
  });

  it('trims, drops empty entries, and de-duplicates (case-insensitive)', () => {
    assert.deepEqual(resolveDisabledMcpServers([' a ', '', 'A', 'b']), ['a', 'b']);
  });

  it('handles undefined / empty inputs', () => {
    assert.deepEqual(resolveDisabledMcpServers(), []);
    assert.deepEqual(resolveDisabledMcpServers([]), []);
    assert.deepEqual(resolveDisabledMcpServers(undefined, ['x']), []);
  });

  it('feeds setDeniedServerList so a disabled server is denied at the manager', () => {
    const mgr = new McpManager();
    mgr.setDeniedServerList(resolveDisabledMcpServers(['bad'], []));
    assert.equal(mgr.isServerDenied('bad'), true);
    assert.equal(mgr.isServerDenied('good'), false);
  });
});

describe('disabledMcpServers / enabledMcpServers config plumbing', () => {
  it('are recognized config keys (no unknown-key warning)', () => {
    assert.ok(KNOWN_CONFIG_KEYS.has('disabledMcpServers'));
    assert.ok(KNOWN_CONFIG_KEYS.has('enabledMcpServers'));
    assert.deepEqual(
      findUnknownConfigKeys({ disabledMcpServers: ['x'], enabledMcpServers: ['y'] }),
      [],
    );
  });

  it('accumulate across config layers (deny/allow-style, higher layer augments)', () => {
    const merged = mergeConfigs(
      {},
      {},
      { disabledMcpServers: ['proj'], enabledMcpServers: ['re-proj'] },
      { disabledMcpServers: ['glob'], enabledMcpServers: ['re-glob'] },
    );
    assert.deepEqual([...(merged.disabledMcpServers ?? [])].sort(), ['glob', 'proj']);
    assert.deepEqual([...(merged.enabledMcpServers ?? [])].sort(), ['re-glob', 're-proj']);
  });
});
