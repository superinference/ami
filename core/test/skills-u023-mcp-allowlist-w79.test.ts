import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  mcpToolNamespacePrefix,
  isMcpToolOfServer,
  deriveMcpToolAllowlist,
  parseMcpServerWildcard,
} from '../src/skills';
import { filterToolsForSkill } from '../src/tools/skill';
import { createDefaultTools } from '../src/tools/index';
import type { SkillDefinition } from '../src/skills';
import type { ToolDefinition } from '../src/types';

// ---------------------------------------------------------------------------
// U023 / w79 — MCP-derived tool allowlists (parity: the reference's bundled
// claude-in-chrome skill, whose allowedTools is derived from the MCP server's
// advertised tool set as `mcp__<server>__<tool>`). See core/core/src/skills.ts.
// ---------------------------------------------------------------------------

describe('mcpToolNamespacePrefix', () => {
  it('builds the mcp__<server>__ prefix', () => {
    assert.equal(mcpToolNamespacePrefix('kali'), 'mcp__kali__');
  });
});

describe('isMcpToolOfServer', () => {
  it('matches a qualified tool in the server namespace', () => {
    assert.ok(isMcpToolOfServer('mcp__kali__nmap', 'kali'));
  });
  it('rejects a tool of a different server', () => {
    assert.ok(!isMcpToolOfServer('mcp__chrome__click', 'kali'));
  });
  it('rejects the bare prefix with no tool segment', () => {
    assert.ok(!isMcpToolOfServer('mcp__kali__', 'kali'));
  });
  it('rejects a non-mcp tool', () => {
    assert.ok(!isMcpToolOfServer('bash', 'kali'));
  });
  it('trims the server name before matching', () => {
    assert.ok(isMcpToolOfServer('mcp__kali__nmap', '  kali  '));
  });
  it('rejects a blank server name', () => {
    assert.ok(!isMcpToolOfServer('mcp__kali__nmap', '   '));
  });
  it('rejects a non-string tool name', () => {
    assert.ok(!isMcpToolOfServer(undefined as unknown as string, 'kali'));
  });
});

describe('deriveMcpToolAllowlist', () => {
  it('namespaces bare advertised tool names', () => {
    assert.deepEqual(
      deriveMcpToolAllowlist('claude-in-chrome', ['click', 'type', 'screenshot']),
      ['mcp__claude-in-chrome__click', 'mcp__claude-in-chrome__type', 'mcp__claude-in-chrome__screenshot'],
    );
  });
  it('de-duplicates and drops blank/non-string tool names in input order', () => {
    assert.deepEqual(
      deriveMcpToolAllowlist('kali', ['nmap', '  ', 'nmap', 'nikto', 42 as unknown as string]),
      ['mcp__kali__nmap', 'mcp__kali__nikto'],
    );
  });
  it('returns [] for a blank server name', () => {
    assert.deepEqual(deriveMcpToolAllowlist('   ', ['nmap']), []);
  });
  it('returns [] for a non-string server name', () => {
    assert.deepEqual(deriveMcpToolAllowlist(undefined as unknown as string, ['nmap']), []);
  });
  it('returns [] for no tools', () => {
    assert.deepEqual(deriveMcpToolAllowlist('kali', []), []);
    assert.deepEqual(deriveMcpToolAllowlist('kali', undefined), []);
  });
});

describe('parseMcpServerWildcard', () => {
  it('extracts the server name from a server-wildcard entry', () => {
    assert.equal(parseMcpServerWildcard('mcp__kali__*'), 'kali');
  });
  it('returns null for an exact mcp tool name', () => {
    assert.equal(parseMcpServerWildcard('mcp__kali__nmap'), null);
  });
  it('returns null for a plain tool name', () => {
    assert.equal(parseMcpServerWildcard('bash'), null);
  });
  it('returns null for the global wildcard', () => {
    assert.equal(parseMcpServerWildcard('*'), null);
  });
  it('returns null for a non-string entry', () => {
    assert.equal(parseMcpServerWildcard(undefined as unknown as string), null);
  });
});

describe('filterToolsForSkill — mcp server-wildcard expansion (wiring)', () => {
  const mcpTool = (name: string): ToolDefinition => ({
    name,
    description: name,
    inputSchema: { type: 'object', properties: {} },
    isReadOnly: true,
    isMcp: true,
  }) as ToolDefinition;

  const allTools: ToolDefinition[] = [
    ...createDefaultTools('/tmp').getAll(),
    mcpTool('mcp__kali__nmap'),
    mcpTool('mcp__kali__nikto'),
    mcpTool('mcp__chrome__click'),
  ];

  it('whitelists an allowedTools mcp__<server>__* to that server\'s advertised tools only', () => {
    const skill: Pick<SkillDefinition, 'allowedTools'> = { allowedTools: ['mcp__kali__*'] };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name).sort();
    assert.deepEqual(names, ['mcp__kali__nikto', 'mcp__kali__nmap']);
  });

  it('combines a server-wildcard with a named tool in the allowlist', () => {
    const skill: Pick<SkillDefinition, 'allowedTools'> = { allowedTools: ['mcp__kali__*', 'bash'] };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name).sort();
    assert.deepEqual(names, ['bash', 'mcp__kali__nikto', 'mcp__kali__nmap']);
  });

  it('removes a server\'s tools when the wildcard is disallowed', () => {
    const skill: Pick<SkillDefinition, 'disallowedTools'> = { disallowedTools: ['mcp__kali__*'] };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition);
    const names = result.map(t => t.name);
    assert.ok(!names.includes('mcp__kali__nmap'));
    assert.ok(!names.includes('mcp__kali__nikto'));
    assert.ok(names.includes('mcp__chrome__click'), 'other servers untouched');
    assert.ok(names.includes('bash'), 'native tools untouched');
  });

  it('warns when a server-wildcard matches no advertised MCP tool', () => {
    const warnings: string[] = [];
    const skill: Pick<SkillDefinition, 'allowedTools'> = { allowedTools: ['mcp__ghost__*'] };
    const result = filterToolsForSkill(allTools, skill as SkillDefinition, w => warnings.push(w.message));
    assert.equal(result.length, 0);
    assert.ok(
      warnings.some(w => w.includes('mcp__ghost__*') && w.includes('no advertised MCP tool')),
      `expected an unmatched-wildcard warning, got: ${JSON.stringify(warnings)}`,
    );
  });
});
