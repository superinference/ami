/**
 * U006 (w108): team_create gains a concept searchHint + self-declared shouldDefer
 * (parity: the reference's TeamCreateTool `searchHint: 'create a multi-agent swarm team'`,
 * `shouldDefer: true`). Verifies the fields are set and that tool_search surfaces
 * team_create by concept terms ("swarm", "multi-agent") that are absent from its
 * human name.
 */
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { teamCreateTool } from '../src/tools/team';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolContext } from '../src/types';

const ctx: ToolContext = { cwd: '/tmp', abortSignal: new AbortController().signal };

describe('team_create deferral + search hint', () => {
  beforeEach(() => { setSearchableTools([teamCreateTool]); });

  it('declares a searchHint with swarm/multi-agent synonyms', () => {
    assert.equal(typeof teamCreateTool.searchHint, 'string');
    assert.match(teamCreateTool.searchHint!, /swarm/);
    assert.match(teamCreateTool.searchHint!, /multi-agent/);
    // The synonyms must NOT be in the human name — that is the whole point of a hint.
    assert.ok(!teamCreateTool.name.includes('swarm'));
  });

  it('opts itself into deferral via shouldDefer', () => {
    assert.equal(teamCreateTool.shouldDefer, true);
  });

  it('tool_search surfaces team_create by the concept term "swarm"', async () => {
    const result = await toolSearchTool.execute({ query: 'swarm' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('team_create'), 'team_create found by "swarm"');
  });

  it('tool_search surfaces team_create by "multi-agent"', async () => {
    const result = await toolSearchTool.execute({ query: 'multi-agent' }, ctx);
    assert.ok(!result.isError);
    assert.ok(result.output.includes('team_create'), 'team_create found by "multi-agent"');
  });
});
