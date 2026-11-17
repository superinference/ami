import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  createDefaultTools,
  isCronEnabled,
  CRON_ENV,
  cronCreateTool,
  cronDeleteTool,
  cronListTool,
  scheduleWakeupTool,
  taskTool,
  sendMessageTool,
  teamCreateTool,
  teamDeleteTool,
  workflowTool,
  lspTool,
  lspDiagnosticsTool,
  lspReferencesTool,
  AGENT_TEAMS_ENV,
  LSP_ENV,
  WORKFLOW_SCRIPTS_ENV,
} from '../src/tools/index';

// U113 — parity: declarative per-tool `isEnabled()` feature-flag gate.
// Every conditionally-registered subsystem tool now carries its own
// `ToolDefinition.isEnabled()` predicate (Claude Code's `Tool.isEnabled()`),
// and `createDefaultTools` honors it uniformly via `filterAvailableTools`
// instead of hand-rolled `if (gate())` blocks — one declarative mechanism.

const CRON_TOOLS = ['cron_create', 'cron_delete', 'cron_list', 'cron_get', 'cron_run', 'schedule_wakeup'];

function toolNames(env: NodeJS.ProcessEnv): Set<string> {
  const saved = process.env;
  process.env = env;
  try {
    return new Set(createDefaultTools('/tmp').getAll().map((t) => t.name));
  } finally {
    process.env = saved;
  }
}

describe('U113 — isCronEnabled gate (default-ON env flag)', () => {
  it('defaults ON when AMI_CRON is unset', () => {
    assert.equal(isCronEnabled({}), true);
  });

  it('is disabled by an explicit falsey AMI_CRON value', () => {
    assert.equal(isCronEnabled({ [CRON_ENV]: 'off' }), false);
    assert.equal(isCronEnabled({ [CRON_ENV]: '0' }), false);
    assert.equal(isCronEnabled({ [CRON_ENV]: 'false' }), false);
  });

  it('stays ON for any truthy value', () => {
    assert.equal(isCronEnabled({ [CRON_ENV]: '1' }), true);
  });

  it('exposes the env var name constant', () => {
    assert.equal(CRON_ENV, 'AMI_CRON');
  });
});

describe('U113 — tools declare a per-tool isEnabled() predicate', () => {
  const saved = process.env;
  afterEach(() => {
    process.env = saved;
  });

  it('cron tools gate on isCronEnabled', () => {
    for (const t of [cronCreateTool, cronDeleteTool, cronListTool, scheduleWakeupTool]) {
      assert.equal(typeof t.isEnabled, 'function', `${t.name} must declare isEnabled`);
    }
    process.env = { [CRON_ENV]: 'off' };
    for (const t of [cronCreateTool, cronDeleteTool, cronListTool, scheduleWakeupTool]) {
      assert.equal(t.isEnabled!(), false, `${t.name} should report disabled`);
    }
    process.env = {};
    for (const t of [cronCreateTool, cronDeleteTool, cronListTool, scheduleWakeupTool]) {
      assert.equal(t.isEnabled!(), true, `${t.name} should report enabled by default`);
    }
  });

  it('teammate tools gate on isTeammatesEnabled', () => {
    for (const t of [taskTool, sendMessageTool, teamCreateTool, teamDeleteTool]) {
      assert.equal(typeof t.isEnabled, 'function', `${t.name} must declare isEnabled`);
    }
    process.env = { [AGENT_TEAMS_ENV]: 'off' };
    const savedArgv = process.argv;
    process.argv = ['node', 'test'];
    try {
      for (const t of [taskTool, sendMessageTool, teamCreateTool, teamDeleteTool]) {
        assert.equal(t.isEnabled!(), false, `${t.name} should report disabled`);
      }
    } finally {
      process.argv = savedArgv;
    }
  });

  it('workflow + lsp tools declare isEnabled gates', () => {
    assert.equal(typeof workflowTool.isEnabled, 'function');
    for (const t of [lspTool, lspDiagnosticsTool, lspReferencesTool]) {
      assert.equal(typeof t.isEnabled, 'function', `${t.name} must declare isEnabled`);
    }
    process.env = { [WORKFLOW_SCRIPTS_ENV]: 'off', [LSP_ENV]: 'off' };
    assert.equal(workflowTool.isEnabled!(), false);
    for (const t of [lspTool, lspDiagnosticsTool, lspReferencesTool]) {
      assert.equal(t.isEnabled!(), false, `${t.name} should report disabled`);
    }
  });
});

describe('U113 — createDefaultTools honors isEnabled (declarative registry filter)', () => {
  const savedArgv = process.argv;
  afterEach(() => {
    process.argv = savedArgv;
  });

  it('registers cron tools by default', () => {
    process.argv = ['node', 'test'];
    const names = toolNames({});
    for (const t of CRON_TOOLS) assert.ok(names.has(t), `expected ${t} registered by default`);
  });

  it('drops the cron subsystem when AMI_CRON is off (others stay)', () => {
    process.argv = ['node', 'test'];
    const names = toolNames({ [CRON_ENV]: 'off' });
    for (const t of CRON_TOOLS) assert.ok(!names.has(t), `${t} should be dropped`);
    // Ungated essentials and other subsystems unaffected.
    assert.ok(names.has('bash') && names.has('file_edit') && names.has('skill'));
    assert.ok(names.has('task'), 'teammate subsystem must remain');
    assert.ok(names.has('workflow'), 'workflow must remain');
  });
});
