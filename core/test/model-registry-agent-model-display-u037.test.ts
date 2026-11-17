/**
 * U037 — getAgentModelDisplay: canonical display string for an agent/subagent
 * model CHOICE (parity: Claude Code's getAgentModelDisplay, utils/model/agent.ts
 * L124-L131). Pairs with getAgentModelOptions (the selectable catalog) so a picker
 * and the rendered value agree on wording.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getAgentModelDisplay } from '../src/model-registry';

describe('getAgentModelDisplay', () => {
  it('treats an absent value as the inherit-by-default case', () => {
    assert.equal(getAgentModelDisplay(undefined), 'Inherit from parent (default)');
    assert.equal(getAgentModelDisplay(null), 'Inherit from parent (default)');
    assert.equal(getAgentModelDisplay(''), 'Inherit from parent (default)');
    assert.equal(getAgentModelDisplay('   '), 'Inherit from parent (default)');
  });

  it('maps the explicit inherit sentinel (any case) to "Inherit from parent"', () => {
    assert.equal(getAgentModelDisplay('inherit'), 'Inherit from parent');
    assert.equal(getAgentModelDisplay('  INHERIT  '), 'Inherit from parent');
  });

  it('resolves a family alias to its friendly display name', () => {
    assert.equal(getAgentModelDisplay('opus'), 'Opus 4');
    assert.equal(getAgentModelDisplay('sonnet'), 'Sonnet 4');
  });

  it('passes an unrecognized custom id through verbatim', () => {
    assert.equal(getAgentModelDisplay('my-org/llm-v9'), 'my-org/llm-v9');
  });
});
