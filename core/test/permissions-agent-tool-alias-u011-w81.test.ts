import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  AGENT_TOOL_NAME,
  LEGACY_AGENT_TOOL_NAME,
  PERMISSION_TOOL_ALIASES,
  normalizeToolName,
  permissionRuleValueFromString,
  PermissionManager,
} from '../src/permissions';

// ===========================================================================
// U011 / w81 — gap: canonical + legacy agent-tool wire-name constants and the
// `Agent` → `task` permission alias (parity: AgentTool/constants.ts
// AGENT_TOOL_NAME='Agent', LEGACY_AGENT_TOOL_NAME='Task'). Claude Code's CURRENT
// subagent-tool wire name is `Agent`; AMI's tool is `task`. A permission rule or
// imported Claude settings file that names `Agent(...)` must gate AMI's `task`
// tool exactly as `task(...)` / the legacy `Task(...)` does.
// ===========================================================================

describe('agent-tool wire-name constants (U011/w81)', () => {
  it('exposes the canonical + legacy Claude wire names', () => {
    assert.equal(AGENT_TOOL_NAME, 'Agent');
    assert.equal(LEGACY_AGENT_TOOL_NAME, 'Task');
  });

  it('wires BOTH names into the alias map, pointing at AMI task', () => {
    assert.equal(PERMISSION_TOOL_ALIASES[AGENT_TOOL_NAME], 'task');
    assert.equal(PERMISSION_TOOL_ALIASES[LEGACY_AGENT_TOOL_NAME], 'task');
  });
});

describe('normalizeToolName — Agent/Task canonicalization (U011/w81)', () => {
  it('canonicalizes the current Claude agent-tool name to AMI task', () => {
    // RED on revert: before the alias, normalizeToolName('Agent') === 'Agent'.
    assert.equal(normalizeToolName('Agent'), 'task');
  });

  it('keeps canonicalizing the legacy Task name (regression lock)', () => {
    assert.equal(normalizeToolName('Task'), 'task');
  });

  it('leaves an unknown tool name untouched', () => {
    assert.equal(normalizeToolName('SomethingElse'), 'SomethingElse');
  });
});

describe('permissionRuleValueFromString — Agent(<type>) parsing (U011/w81)', () => {
  it('parses Agent(<type>) into a task rule with the type as ruleContent', () => {
    assert.deepEqual(
      permissionRuleValueFromString('Agent(researcher)'),
      { toolName: 'task', ruleContent: 'researcher' },
    );
  });

  it('parses a bare Agent into a tool-wide task rule', () => {
    assert.deepEqual(permissionRuleValueFromString('Agent'), { toolName: 'task' });
  });

  it('parses the legacy Task(<type>) identically', () => {
    assert.deepEqual(
      permissionRuleValueFromString('Task(reviewer)'),
      { toolName: 'task', ruleContent: 'reviewer' },
    );
  });
});

describe('PermissionManager end-to-end — Agent deny rule gates task spawns (U011/w81)', () => {
  it('a tool-wide Agent deny rule denies an AMI task tool call', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'Agent', action: 'deny' }]);
    // RED on revert: without the Agent alias, toolNameMatches('Agent','task') is
    // false, so the deny never fires and an ask-mode task call returns 'ask'.
    assert.equal(await pm.check('task', { prompt: 'do work' }), 'deny');
  });

  it('the legacy Task deny rule still gates the task tool (regression lock)', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'Task', action: 'deny' }]);
    assert.equal(await pm.check('task', { prompt: 'do work' }), 'deny');
  });

  it('an unrelated tool is NOT affected by an Agent rule', async () => {
    const pm = new PermissionManager('auto-allow', [{ tool: 'Agent', action: 'deny' }]);
    // A different tool under auto-allow stays allowed — the Agent rule is scoped to task.
    assert.equal(await pm.check('file_read', { file_path: '/tmp/x' }), 'allow');
  });
});
