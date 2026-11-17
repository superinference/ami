import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  mcpServerName,
  getMcpServerBuckets,
  type CategorizableTool,
} from '../src/tools/index';

describe('mcpServerName — server segment parsing (U011 gap10)', () => {
  it('extracts the server from a canonical mcp__server__tool name', () => {
    assert.equal(mcpServerName('mcp__slack__send_message'), 'slack');
  });

  it('returns the whole remainder when there is a single segment', () => {
    assert.equal(mcpServerName('mcp__github'), 'github');
  });

  it('returns undefined for a non-MCP name', () => {
    assert.equal(mcpServerName('bash'), undefined);
  });

  it('returns undefined for an mcp__ prefix with no server segment', () => {
    assert.equal(mcpServerName('mcp__'), undefined);
  });
});

describe('getMcpServerBuckets — group MCP tools by server (U011 gap10)', () => {
  const tools: CategorizableTool[] = [
    { name: 'mcp__slack__post', isMcp: true },
    { name: 'mcp__github__pr', isMcp: true },
    { name: 'mcp__slack__dm', isMcp: true },
    { name: 'bash' },
    { name: 'file_read', isReadOnly: true },
  ];

  it('groups only MCP tools, ignoring non-MCP tools', () => {
    const buckets = getMcpServerBuckets(tools);
    assert.deepEqual(buckets.map(b => b.server), ['github', 'slack']); // localeCompare sorted
  });

  it('sorts each server\'s tools by name', () => {
    const buckets = getMcpServerBuckets(tools);
    const slack = buckets.find(b => b.server === 'slack');
    assert.ok(slack);
    assert.deepEqual(slack.tools.map(t => t.name), ['mcp__slack__dm', 'mcp__slack__post']);
  });

  it('buckets an MCP tool with no server segment under (unknown)', () => {
    const buckets = getMcpServerBuckets([{ name: 'weird', isMcp: true }]);
    assert.deepEqual(buckets.map(b => b.server), ['(unknown)']);
  });

  it('returns [] when there are no MCP tools', () => {
    assert.deepEqual(getMcpServerBuckets([{ name: 'bash' }]), []);
  });
});
