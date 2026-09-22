import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { BUILTIN_PERSONAS, matchMcpAutoAllow } from '../src/personas';
import type { PersonaDefinition } from '../src/personas';

describe('BUILTIN_PERSONAS data', () => {
  const findPersona = (name: string): PersonaDefinition =>
    BUILTIN_PERSONAS.find(p => p.name === name)!;

  it('contains all expected personas', () => {
    const names = BUILTIN_PERSONAS.map(p => p.name);
    assert.ok(names.includes('code'));
    assert.ok(names.includes('pentest'));
    assert.ok(names.includes('sre'));
    assert.ok(names.includes('research'));
    assert.ok(BUILTIN_PERSONAS.length >= 4);
  });

  it('does not contain gpu persona (moved to /provision mcp)', () => {
    const names = BUILTIN_PERSONAS.map(p => p.name);
    assert.ok(!names.includes('gpu'));
  });

  it('every persona has required fields', () => {
    for (const p of BUILTIN_PERSONAS) {
      assert.ok(p.name, `persona missing name`);
      assert.ok(p.description, `${p.name} missing description`);
      assert.ok(p.systemPromptOverlay, `${p.name} missing systemPromptOverlay`);
    }
  });

  it('code persona has no mcpServers or autoAllowPatterns', () => {
    const code = findPersona('code');
    assert.equal(code.mcpServers, undefined);
    assert.equal(code.autoAllowPatterns, undefined);
    assert.equal(code.defaultThinkingLevel, 'medium');
  });

  it('pentest persona has kali MCP server with containerConfig', () => {
    const pentest = findPersona('pentest');
    assert.ok(pentest.mcpServers);
    assert.ok(pentest.mcpServers!.kali);
    assert.ok(pentest.mcpServers!.kali.containerConfig);
    assert.equal(pentest.mcpServers!.kali.containerConfig!.image, 'docker.io/cyberillo/kali-mcp-server:latest');
    assert.ok(pentest.mcpAutoAllowPatterns!.includes('mcp__kali__*'));
    assert.equal(pentest.defaultThinkingLevel, 'high');
  });

  it('pentest persona has bash auto-allow patterns for security tools', () => {
    const pentest = findPersona('pentest');
    assert.ok(pentest.autoAllowPatterns);
    assert.ok(pentest.autoAllowPatterns!.some(p => p.includes('nmap')));
    assert.ok(pentest.autoAllowPatterns!.some(p => p.includes('curl')));
    assert.ok(pentest.autoAllowPatterns!.includes('docker pull*'));
    assert.ok(pentest.autoAllowPatterns!.includes('podman pull*'));
  });

  it('sre persona has kubectl auto-allow', () => {
    const sre = findPersona('sre');
    assert.ok(sre.autoAllowPatterns);
    assert.ok(sre.autoAllowPatterns!.some(p => p.includes('kubectl')));
    assert.equal(sre.defaultThinkingLevel, 'medium');
  });

  it('research persona defaults to high thinking', () => {
    const research = findPersona('research');
    assert.equal(research.defaultThinkingLevel, 'high');
    assert.ok(research.autoAllowPatterns!.some(p => p.includes('python')));
  });

  it('pentest has kali auto-allow patterns', () => {
    const pentest = findPersona('pentest');
    assert.ok(matchMcpAutoAllow('mcp__kali__nmap_scan', pentest.mcpAutoAllowPatterns!));
    assert.ok(!matchMcpAutoAllow('mcp__runpod__create_pod', pentest.mcpAutoAllowPatterns!));
  });
});

describe('matchMcpAutoAllow', () => {
  it('matches exact tool name', () => {
    assert.ok(matchMcpAutoAllow('mcp__kali__nmap_scan', ['mcp__kali__nmap_scan']));
  });

  it('matches wildcard prefix', () => {
    assert.ok(matchMcpAutoAllow('mcp__kali__nmap_scan', ['mcp__kali__*']));
    assert.ok(matchMcpAutoAllow('mcp__runpod__create_pod', ['mcp__runpod__*']));
  });

  it('rejects non-matching tool', () => {
    assert.ok(!matchMcpAutoAllow('mcp__other__nmap', ['mcp__kali__*']));
  });

  it('matches global wildcard', () => {
    assert.ok(matchMcpAutoAllow('mcp__anything__any_tool', ['*']));
  });

  it('matches against multiple patterns', () => {
    assert.ok(matchMcpAutoAllow('mcp__other__scan', ['mcp__kali__*', 'mcp__other__*']));
  });

  it('rejects when no patterns match', () => {
    assert.ok(!matchMcpAutoAllow('mcp__third__tool', ['mcp__kali__*', 'mcp__other__scan']));
  });

  it('handles empty patterns array', () => {
    assert.ok(!matchMcpAutoAllow('mcp__kali__nmap', []));
  });
});
