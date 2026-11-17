// Unit coverage for getAgentSourceDisplayName (U023 w81) — the ONE canonical
// agent-source → human-readable label mapper (parity with Claude Code's
// getAgentSourceDisplayName, components/agents/utils.ts). Every AGENT_SOURCES
// member plus the aggregate 'all' must map to a stable label, and an
// unknown/absent token must degrade to 'Unknown' rather than a blank heading.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  getAgentSourceDisplayName,
  AGENT_SOURCES,
  type AgentSourceSelector,
} from '../src/skills';

describe('getAgentSourceDisplayName (U023 w81)', () => {
  it('maps each concrete agent source to its label', () => {
    assert.equal(getAgentSourceDisplayName('built-in'), 'Built-in');
    assert.equal(getAgentSourceDisplayName('plugin'), 'Plugin');
    assert.equal(getAgentSourceDisplayName('user'), 'User');
    assert.equal(getAgentSourceDisplayName('project'), 'Project');
  });

  it('maps the aggregate selector', () => {
    assert.equal(getAgentSourceDisplayName('all'), 'All sources');
  });

  it('degrades an unknown/absent source to "Unknown" (never a blank label)', () => {
    assert.equal(getAgentSourceDisplayName(undefined), 'Unknown');
    assert.equal(getAgentSourceDisplayName('bogus' as AgentSourceSelector), 'Unknown');
  });

  it('produces a non-empty label for every declared AGENT_SOURCES member', () => {
    for (const source of AGENT_SOURCES) {
      const label = getAgentSourceDisplayName(source);
      assert.equal(typeof label, 'string');
      assert.ok(label.length > 0, `empty label for source "${source}"`);
      assert.notEqual(label, 'Unknown', `known source "${source}" fell through to Unknown`);
    }
  });

  it('is pure — repeated calls yield the same label', () => {
    assert.equal(getAgentSourceDisplayName('project'), getAgentSourceDisplayName('project'));
  });
});
