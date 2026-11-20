// Unit coverage for the U023 w87 additive skills building blocks:
//   - classifyAgentMcpServerSpecs — per-agent MCP server spec classification into
//     REFERENCED (by name, shared, not cleaned up) vs INLINE (single-key object,
//     agent-specific, cleaned up) vs INVALID (parity with the reference's AgentTool
//     `initializeAgentMcpServers` per-spec handling).
//   - the expanded agent source tiers (managed/policy, local, flag) — display label,
//     editability, and recommended memory scope (parity with the reference's AgentsMenu
//     policySettings/localSettings/flagSettings source tiers).

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  classifyAgentMcpServerSpecs,
  type RawAgentMcpServers,
  AGENT_SOURCES,
  getAgentSourceDisplayName,
  AGENT_NONEDITABLE_SOURCES,
  isAgentEditable,
  recommendedAgentMemoryScope,
  agentMemoryScopeOptions,
} from '../src/skills';

describe('classifyAgentMcpServerSpecs (U023 w87)', () => {
  it('classifies the inline-MAP form as inline servers in key order', () => {
    const raw: RawAgentMcpServers = {
      alpha: { command: 'a' } as never,
      beta: { command: 'b' } as never,
    };
    const out = classifyAgentMcpServerSpecs(raw);
    assert.deepEqual(out.referenced, []);
    assert.deepEqual(out.inline.map(i => i.name), ['alpha', 'beta']);
    assert.deepEqual(out.inline[0].config, { command: 'a' });
    assert.deepEqual(out.invalid, []);
  });

  it('classifies a STRING element as a referenced (shared) server name', () => {
    const out = classifyAgentMcpServerSpecs(['parentServer']);
    assert.deepEqual(out.referenced, ['parentServer']);
    assert.deepEqual(out.inline, []);
    assert.deepEqual(out.invalid, []);
  });

  it('classifies a single-key object element as an inline (agent-specific) server', () => {
    const out = classifyAgentMcpServerSpecs([{ fresh: { url: 'http://x' } as never }]);
    assert.deepEqual(out.referenced, []);
    assert.deepEqual(out.inline, [{ name: 'fresh', config: { url: 'http://x' } }]);
    assert.deepEqual(out.invalid, []);
  });

  it('mixes referenced + inline, preserving order (parity: initializeAgentMcpServers loop)', () => {
    const out = classifyAgentMcpServerSpecs([
      'shared1',
      { local1: { command: 'x' } as never },
      'shared2',
    ]);
    assert.deepEqual(out.referenced, ['shared1', 'shared2']);
    assert.deepEqual(out.inline.map(i => i.name), ['local1']);
    assert.deepEqual(out.invalid, []);
  });

  it('de-duplicates referenced names in input order', () => {
    const out = classifyAgentMcpServerSpecs(['a', 'b', 'a']);
    assert.deepEqual(out.referenced, ['a', 'b']);
  });

  it('last-wins on a duplicate inline server name, keeping first position', () => {
    const out = classifyAgentMcpServerSpecs([
      { dup: { command: 'first' } as never },
      { dup: { command: 'second' } as never },
    ]);
    assert.equal(out.inline.length, 1);
    assert.deepEqual(out.inline[0], { name: 'dup', config: { command: 'second' } });
  });

  it('flags a multi-key inline object as invalid (expected exactly one key)', () => {
    const out = classifyAgentMcpServerSpecs([{ a: {} as never, b: {} as never }]);
    assert.equal(out.inline.length, 0);
    assert.equal(out.invalid.length, 1);
    assert.equal(out.invalid[0].index, 0);
    assert.match(out.invalid[0].reason, /exactly one key/);
  });

  it('flags blank reference strings and non-object configs as invalid', () => {
    const out = classifyAgentMcpServerSpecs([
      '   ',
      { bad: 'not-an-object' } as never,
      42 as never,
    ]);
    assert.deepEqual(out.referenced, []);
    assert.deepEqual(out.inline, []);
    assert.deepEqual(out.invalid.map(i => i.index), [0, 1, 2]);
    assert.match(out.invalid[0].reason, /blank/);
    assert.match(out.invalid[1].reason, /non-object config/);
  });

  it('flags an empty object element and a blank inline key as invalid', () => {
    const out = classifyAgentMcpServerSpecs([{}, { '  ': {} as never }]);
    assert.equal(out.invalid.length, 2);
    assert.match(out.invalid[0].reason, /exactly one key \(got 0\)/);
    assert.match(out.invalid[1].reason, /blank inline MCP server name/);
  });

  it('flags a null/array element as invalid', () => {
    const out = classifyAgentMcpServerSpecs([null as never, ['x'] as never]);
    assert.deepEqual(out.invalid.map(i => i.index), [0, 1]);
  });

  it('returns empty classification for undefined / empty inputs', () => {
    for (const raw of [undefined, [], {}] as RawAgentMcpServers[]) {
      const out = classifyAgentMcpServerSpecs(raw);
      assert.deepEqual(out, { referenced: [], inline: [], invalid: [] });
    }
  });
});

describe('expanded agent source tiers (U023 w87)', () => {
  it('declares the managed/local/flag tiers alongside the originals', () => {
    for (const s of ['built-in', 'plugin', 'managed', 'user', 'local', 'project', 'flag'] as const) {
      assert.ok((AGENT_SOURCES as readonly string[]).includes(s), `missing source ${s}`);
    }
  });

  it('maps every new tier to a distinct non-"Unknown" display label', () => {
    assert.equal(getAgentSourceDisplayName('managed'), 'Managed');
    assert.equal(getAgentSourceDisplayName('local'), 'Local');
    assert.equal(getAgentSourceDisplayName('flag'), 'Flag');
    for (const s of AGENT_SOURCES) {
      assert.notEqual(getAgentSourceDisplayName(s), 'Unknown', `no label for ${s}`);
    }
  });

  it('treats managed + flag as read-only; local as editable', () => {
    assert.ok(AGENT_NONEDITABLE_SOURCES.has('managed'));
    assert.ok(AGENT_NONEDITABLE_SOURCES.has('flag'));
    assert.equal(isAgentEditable({ source: 'managed' }), false);
    assert.equal(isAgentEditable({ source: 'flag' }), false);
    assert.equal(isAgentEditable({ source: 'local' }), true);
  });

  it('recommends location-matched memory scope for local (parity: MemoryStep)', () => {
    assert.equal(recommendedAgentMemoryScope('local'), 'local');
    assert.equal(recommendedAgentMemoryScope('user'), 'user');
    assert.equal(recommendedAgentMemoryScope('managed'), 'project');
    assert.equal(recommendedAgentMemoryScope('flag'), 'project');
  });

  it('puts the location-matched local scope first in the wizard options', () => {
    const opts = agentMemoryScopeOptions('local');
    assert.equal(opts[0].value, 'local');
    assert.ok(opts[0].recommended);
    assert.match(opts[0].label, /Recommended/);
  });
});
