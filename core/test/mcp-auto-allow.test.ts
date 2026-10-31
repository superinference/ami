import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { matchMcpAutoAllow } from '../src/mcp/manager';

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
