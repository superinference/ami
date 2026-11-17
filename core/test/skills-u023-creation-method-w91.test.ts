// Unit coverage for the U023 w91 additive agent-creation-method building block:
//   - the `generate | manual` method allowlist + case-insensitive parser
//     (parity with Claude Code's new-agent wizard MethodStep methodOptions).
//   - the ordered, recommended-default-first option list with branch metadata
//     (generate → 'generate' step + wasGenerated:true; manual → 'configure' step
//     + wasGenerated:false — mirroring Claude's goNext() vs goToStep(3) branch).

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  AGENT_CREATION_METHODS,
  parseAgentCreationMethod,
  agentCreationMethodOptions,
  type AgentCreationMethod,
} from '../src/skills';

describe('agent creation methods (U023 w91)', () => {
  it('declares exactly generate + manual', () => {
    assert.deepEqual([...AGENT_CREATION_METHODS], ['generate', 'manual']);
  });

  it('parses valid methods case-insensitively with trimming', () => {
    assert.equal(parseAgentCreationMethod('generate'), 'generate');
    assert.equal(parseAgentCreationMethod('MANUAL'), 'manual');
    assert.equal(parseAgentCreationMethod('  Generate  '), 'generate');
  });

  it('returns undefined for unknown / non-string values', () => {
    assert.equal(parseAgentCreationMethod('auto'), undefined);
    assert.equal(parseAgentCreationMethod(''), undefined);
    assert.equal(parseAgentCreationMethod(undefined), undefined);
    assert.equal(parseAgentCreationMethod(42), undefined);
    assert.equal(parseAgentCreationMethod(null), undefined);
  });

  it('lists generate FIRST as the recommended default, then manual', () => {
    const opts = agentCreationMethodOptions();
    assert.equal(opts.length, 2);
    assert.equal(opts[0].value, 'generate');
    assert.equal(opts[0].recommended, true);
    assert.match(opts[0].label, /Recommended/);
    assert.equal(opts[1].value, 'manual');
    assert.equal(opts[1].recommended, false);
    // Exactly one recommended default.
    assert.equal(opts.filter(o => o.recommended).length, 1);
  });

  it('carries the post-selection branch + provenance metadata', () => {
    const byValue = new Map<AgentCreationMethod, ReturnType<typeof agentCreationMethodOptions>[number]>(
      agentCreationMethodOptions().map(o => [o.value, o]),
    );
    const gen = byValue.get('generate');
    const man = byValue.get('manual');
    assert.ok(gen && man);
    // generate → generation step, stamps wasGenerated:true.
    assert.equal(gen.next, 'generate');
    assert.equal(gen.wasGenerated, true);
    // manual → manual-configuration step, wasGenerated:false.
    assert.equal(man.next, 'configure');
    assert.equal(man.wasGenerated, false);
    // Every option carries a non-empty human description.
    for (const o of agentCreationMethodOptions()) {
      assert.ok(o.description.length > 0);
    }
  });

  it('never references an external vendor in option copy (AMI product wording)', () => {
    for (const o of agentCreationMethodOptions()) {
      assert.doesNotMatch(o.label, /claude/i);
      assert.doesNotMatch(o.description, /claude/i);
    }
  });
});
