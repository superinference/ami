// Unit coverage for the U023 w83 additive skills/agent building blocks:
//   - progress affordance kind-awareness (skill 'loading' vs command 'running') —
//     parity with Claude Code's loadPluginCommands `progressMessage: isSkill ?
//     'loading' : 'running'`.
//   - agent source-based editability (AgentsMenu `isEditable`).
//   - composite (name, source) agent identity (AgentsMenu dedup/selection key).
//   - location-aware recommended agent-memory scope + ordered wizard options
//     (MemoryStep recommended-default-first option list).

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  // progress
  DEFAULT_SKILL_PROGRESS_MESSAGE,
  SKILL_LOADING_PROGRESS_MESSAGE,
  defaultProgressMessage,
  skillProgressMessage,
  progressMessageForKind,
  // editability + identity
  AGENT_NONEDITABLE_SOURCES,
  isAgentEditable,
  agentIdentityKey,
  // memory scope
  recommendedAgentMemoryScope,
  agentMemoryScopeOptions,
  AGENT_MEMORY_SCOPES,
  agentMemoryScopeDisplay,
} from '../src/skills';

describe('kind-aware progress affordance (U023 w83)', () => {
  it('exposes the two default affordances', () => {
    assert.equal(DEFAULT_SKILL_PROGRESS_MESSAGE, 'running');
    assert.equal(SKILL_LOADING_PROGRESS_MESSAGE, 'loading');
  });

  it('defaultProgressMessage distinguishes skill-load from command-run', () => {
    assert.equal(defaultProgressMessage(true), 'loading');
    assert.equal(defaultProgressMessage(false), 'running');
  });

  it('progressMessageForKind defaults by kind when none declared', () => {
    assert.equal(progressMessageForKind({}, true), 'loading');
    assert.equal(progressMessageForKind({}, false), 'running');
    assert.equal(progressMessageForKind({ progressMessage: '   ' }, true), 'loading');
  });

  it('progressMessageForKind honors a declared message for BOTH kinds', () => {
    assert.equal(progressMessageForKind({ progressMessage: '  deploying  ' }, true), 'deploying');
    assert.equal(progressMessageForKind({ progressMessage: 'deploying' }, false), 'deploying');
  });

  it('skillProgressMessage keeps its command-default behavior (delegating, no drift)', () => {
    assert.equal(skillProgressMessage({}), 'running');
    assert.equal(skillProgressMessage({ progressMessage: 'building' }), 'building');
    assert.equal(skillProgressMessage({ progressMessage: '' }), 'running');
    // skillProgressMessage === progressMessageForKind(..., false)
    assert.equal(skillProgressMessage({}), progressMessageForKind({}, false));
    assert.equal(
      skillProgressMessage({ progressMessage: 'x' }),
      progressMessageForKind({ progressMessage: 'x' }, false),
    );
  });
});

describe('agent source-based editability (U023 w83)', () => {
  it('built-in, plugin, and flag sources are non-editable', () => {
    assert.ok(AGENT_NONEDITABLE_SOURCES.has('built-in'));
    assert.ok(AGENT_NONEDITABLE_SOURCES.has('plugin'));
    assert.ok(AGENT_NONEDITABLE_SOURCES.has('flag'));
    assert.equal(isAgentEditable({ source: 'built-in' }), false);
    assert.equal(isAgentEditable({ source: 'plugin' }), false);
    assert.equal(isAgentEditable({ source: 'flag' as never }), false);
  });

  it('user/project/local/policy agents are editable', () => {
    assert.equal(isAgentEditable({ source: 'user' }), true);
    assert.equal(isAgentEditable({ source: 'project' }), true);
    assert.equal(isAgentEditable({ source: 'local' as never }), true);
    assert.equal(isAgentEditable({ source: 'policy' as never }), true);
  });

  it('an absent/blank source is treated as non-editable', () => {
    assert.equal(isAgentEditable({ source: undefined }), false);
    assert.equal(isAgentEditable({ source: '' as never }), false);
  });
});

describe('composite agent identity key (U023 w83)', () => {
  it('joins name and source with a NUL separator', () => {
    assert.equal(agentIdentityKey({ name: 'reviewer', source: 'user' }), 'reviewer\u0000user');
  });

  it('keeps same-named agents from different sources distinct', () => {
    const a = agentIdentityKey({ name: 'reviewer', source: 'user' });
    const b = agentIdentityKey({ name: 'reviewer', source: 'project' });
    assert.notEqual(a, b);
  });

  it('defaults an absent source to built-in', () => {
    assert.equal(agentIdentityKey({ name: 'x', source: undefined }), 'x\u0000built-in');
    assert.equal(agentIdentityKey({ name: 'x', source: '' as never }), 'x\u0000built-in');
  });

  it('a NUL-forged name cannot collide with a distinct (name, source) pair', () => {
    // "a" + NUL + "user" (source=user) vs name "a\u0000user" with default source
    const real = agentIdentityKey({ name: 'a', source: 'user' });
    const forged = agentIdentityKey({ name: 'a\u0000user', source: undefined });
    assert.notEqual(real, forged);
  });
});

describe('location-aware recommended memory scope (U023 w83)', () => {
  it('a user agent recommends user memory', () => {
    assert.equal(recommendedAgentMemoryScope('user'), 'user');
  });

  it('every non-user source recommends project memory', () => {
    assert.equal(recommendedAgentMemoryScope('project'), 'project');
    assert.equal(recommendedAgentMemoryScope('plugin'), 'project');
    assert.equal(recommendedAgentMemoryScope('built-in'), 'project');
    assert.equal(recommendedAgentMemoryScope(undefined), 'project');
  });
});

describe('agentMemoryScopeOptions (U023 w83)', () => {
  it('for a user agent, user memory is the recommended first option', () => {
    const opts = agentMemoryScopeOptions('user');
    assert.equal(opts[0].value, 'user');
    assert.equal(opts[0].recommended, true);
    assert.match(opts[0].label, /\(Recommended\)$/);
    assert.ok(opts[0].label.startsWith(agentMemoryScopeDisplay('user')!));
  });

  it('for a project agent, project memory is recommended first, then None', () => {
    const opts = agentMemoryScopeOptions('project');
    assert.equal(opts[0].value, 'project');
    assert.equal(opts[0].recommended, true);
    assert.equal(opts[1].value, 'none');
    assert.equal(opts[1].recommended, false);
    assert.equal(opts[1].label, 'None (no persistent memory)');
  });

  it('includes exactly the recommended + none + every other scope, once each', () => {
    const opts = agentMemoryScopeOptions('user');
    const values = opts.map((o) => o.value);
    // one 'none' + one per scope
    assert.equal(opts.length, AGENT_MEMORY_SCOPES.length + 1);
    assert.equal(values.filter((v) => v === 'none').length, 1);
    for (const scope of AGENT_MEMORY_SCOPES) {
      assert.equal(values.filter((v) => v === scope).length, 1, `scope ${scope} appears once`);
    }
    // exactly one recommended
    assert.equal(opts.filter((o) => o.recommended).length, 1);
  });

  it('non-recommended scopes carry their descriptive label (no Recommended suffix)', () => {
    const opts = agentMemoryScopeOptions('user');
    const local = opts.find((o) => o.value === 'local')!;
    assert.equal(local.label, agentMemoryScopeDisplay('local'));
    assert.doesNotMatch(local.label, /Recommended/);
  });
});
