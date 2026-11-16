import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  capabilityEnabled,
  isTeammatesEnabled,
  isLspEnabled,
  isWorkflowScriptsEnabled,
  AGENT_TEAMS_ENV,
  AGENT_TEAMS_ON_FLAG,
  AGENT_TEAMS_OFF_FLAG,
  LSP_ENV,
  WORKFLOW_SCRIPTS_ENV,
  createDefaultTools,
} from '../src/tools/index';

// Teammate / LSP / workflow subsystem names, for the registration integration checks.
const TEAMMATE_TOOLS = ['task', 'send_message', 'team_create', 'team_delete'];
const LSP_TOOLS = ['diagnostics', 'find_references', 'lsp'];
const WORKFLOW_TOOL = 'workflow';

function toolNames(env: NodeJS.ProcessEnv): Set<string> {
  // createDefaultTools reads process.env; swap it for the duration of the call.
  const saved = process.env;
  process.env = env;
  try {
    return new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
  } finally {
    process.env = saved;
  }
}

describe('tool-gates — capabilityEnabled (default-ON env flag parse)', () => {
  it('treats unset / empty / whitespace as enabled', () => {
    assert.equal(capabilityEnabled(undefined), true);
    assert.equal(capabilityEnabled(''), true);
    assert.equal(capabilityEnabled('   '), true);
  });

  it('treats explicit falsey tokens as disabled (case/space-insensitive)', () => {
    for (const v of ['0', 'false', 'no', 'off', 'OFF', ' Off ', 'FALSE']) {
      assert.equal(capabilityEnabled(v), false, `expected ${JSON.stringify(v)} → disabled`);
    }
  });

  it('treats any other value as enabled', () => {
    for (const v of ['1', 'true', 'yes', 'on', 'enabled']) {
      assert.equal(capabilityEnabled(v), true, `expected ${JSON.stringify(v)} → enabled`);
    }
  });
});

describe('tool-gates — isTeammatesEnabled', () => {
  it('defaults ON when nothing is set', () => {
    assert.equal(isTeammatesEnabled({}, []), true);
  });

  it('is disabled by a falsey env value', () => {
    assert.equal(isTeammatesEnabled({ [AGENT_TEAMS_ENV]: 'off' }, []), false);
    assert.equal(isTeammatesEnabled({ [AGENT_TEAMS_ENV]: '0' }, []), false);
  });

  it('is enabled by a truthy env value', () => {
    assert.equal(isTeammatesEnabled({ [AGENT_TEAMS_ENV]: '1' }, []), true);
  });

  it('--no-agent-teams flag forces off even if env enables it', () => {
    assert.equal(
      isTeammatesEnabled({ [AGENT_TEAMS_ENV]: '1' }, [AGENT_TEAMS_OFF_FLAG]),
      false,
    );
  });

  it('--agent-teams flag forces on even if env disables it', () => {
    assert.equal(
      isTeammatesEnabled({ [AGENT_TEAMS_ENV]: 'off' }, [AGENT_TEAMS_ON_FLAG]),
      true,
    );
  });

  it('off-flag wins over on-flag (deterministic precedence)', () => {
    assert.equal(
      isTeammatesEnabled({}, [AGENT_TEAMS_ON_FLAG, AGENT_TEAMS_OFF_FLAG]),
      false,
    );
  });
});

describe('tool-gates — isLspEnabled / isWorkflowScriptsEnabled', () => {
  it('both default ON', () => {
    assert.equal(isLspEnabled({}), true);
    assert.equal(isWorkflowScriptsEnabled({}), true);
  });

  it('each disabled independently by its own env flag', () => {
    assert.equal(isLspEnabled({ [LSP_ENV]: 'off' }), false);
    assert.equal(isWorkflowScriptsEnabled({ [WORKFLOW_SCRIPTS_ENV]: 'false' }), false);
  });
});

describe('tool-gates — createDefaultTools conditional registration (integration)', () => {
  // Guard against argv-level teammate flags leaking into createDefaultTools
  // (which reads process.argv) from an outer test runner invocation.
  const savedArgv = process.argv;
  afterEach(() => {
    process.argv = savedArgv;
  });

  it('registers every gated subsystem by default', () => {
    process.argv = ['node', 'test'];
    const names = toolNames({});
    for (const t of [...TEAMMATE_TOOLS, ...LSP_TOOLS, WORKFLOW_TOOL]) {
      assert.ok(names.has(t), `expected ${t} registered by default`);
    }
  });

  it('drops the teammate subsystem when AMI_AGENT_TEAMS is off (others stay)', () => {
    process.argv = ['node', 'test'];
    const names = toolNames({ [AGENT_TEAMS_ENV]: 'off' });
    for (const t of TEAMMATE_TOOLS) assert.ok(!names.has(t), `${t} should be dropped`);
    for (const t of LSP_TOOLS) assert.ok(names.has(t), `${t} should remain`);
    assert.ok(names.has(WORKFLOW_TOOL));
    // Ungated essentials unaffected.
    assert.ok(names.has('bash') && names.has('file_edit') && names.has('skill'));
  });

  it('drops only the LSP tools when AMI_LSP is off', () => {
    process.argv = ['node', 'test'];
    const names = toolNames({ [LSP_ENV]: '0' });
    for (const t of LSP_TOOLS) assert.ok(!names.has(t), `${t} should be dropped`);
    for (const t of TEAMMATE_TOOLS) assert.ok(names.has(t), `${t} should remain`);
    assert.ok(names.has(WORKFLOW_TOOL));
  });

  it('drops only the workflow tool when AMI_WORKFLOW_SCRIPTS is off', () => {
    process.argv = ['node', 'test'];
    const names = toolNames({ [WORKFLOW_SCRIPTS_ENV]: 'no' });
    assert.ok(!names.has(WORKFLOW_TOOL), 'workflow should be dropped');
    for (const t of [...TEAMMATE_TOOLS, ...LSP_TOOLS]) {
      assert.ok(names.has(t), `${t} should remain`);
    }
  });
});
