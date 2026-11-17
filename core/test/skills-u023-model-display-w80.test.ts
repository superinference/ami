// Behavioral coverage for resolveAgentModelDisplay — the resolved display-model
// helper added for Claude parity (agentDisplay.ts resolveAgentModelDisplay): an
// agent's declared model, else the caller-supplied effective subagent default, with
// the `inherit` sentinel surfaced verbatim and a blank result collapsing to
// undefined. Exercises the REAL exported function in core/src/skills.ts.

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { resolveAgentModelDisplay } from '../src/skills';

describe('resolveAgentModelDisplay', () => {
  it('returns the agent\'s own declared model verbatim', () => {
    assert.equal(resolveAgentModelDisplay({ model: 'gpt-4o' }, 'claude-sonnet'), 'gpt-4o');
  });

  it('falls back to the supplied default when the agent declares no model', () => {
    assert.equal(resolveAgentModelDisplay({}, 'claude-sonnet'), 'claude-sonnet');
    assert.equal(resolveAgentModelDisplay({ model: undefined }, 'claude-sonnet'), 'claude-sonnet');
  });

  it('treats a blank/whitespace declared model as absent and uses the default', () => {
    assert.equal(resolveAgentModelDisplay({ model: '   ' }, 'claude-sonnet'), 'claude-sonnet');
  });

  it('trims a declared model', () => {
    assert.equal(resolveAgentModelDisplay({ model: '  gpt-4o  ' }), 'gpt-4o');
  });

  it('surfaces the `inherit` sentinel verbatim (not a concrete name)', () => {
    assert.equal(resolveAgentModelDisplay({ model: 'inherit' }, 'claude-sonnet'), 'inherit');
    // Case/whitespace around the sentinel is trimmed but preserved as-is otherwise.
    assert.equal(resolveAgentModelDisplay({ model: ' inherit ' }), 'inherit');
  });

  it('returns undefined when neither a model nor a usable default is present', () => {
    assert.equal(resolveAgentModelDisplay({}), undefined);
    assert.equal(resolveAgentModelDisplay({ model: '' }, ''), undefined);
    assert.equal(resolveAgentModelDisplay({ model: '   ' }, '   '), undefined);
  });

  it('does not treat a default of "inherit" as a concrete model', () => {
    assert.equal(resolveAgentModelDisplay({}, 'inherit'), 'inherit');
  });
});
