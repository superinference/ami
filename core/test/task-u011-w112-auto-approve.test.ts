import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import {
  taskTool,
  resetAgentRegistry,
  parseAutoApproveTools,
  buildAutoApproveRules,
} from '../src/tools/task';
import { PermissionManager, type PermissionRule } from '../src/permissions';
import type { ToolContext, ToolDefinition } from '../src/types';

type Ev = Record<string, unknown>;

/**
 * Run the task tool with a fake engine, capturing the child EngineConfig (so we can
 * assert the auto-approve permission rules + the unchanged tool availability).
 */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<{
  isError?: boolean;
  output: string;
  cfg: { permissionRules?: PermissionRule[]; permissionMode?: string } | null;
  toolNames: string[];
}> {
  let cfg: { permissionRules?: PermissionRule[]; permissionMode?: string } | null = null;
  let toolNames: string[] = [];
  async function* fakeSubmit() {
    for (const e of events) yield e as never;
  }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => {
      cfg = c as typeof cfg;
      toolNames = (c.tools as ToolDefinition[]).map(t => t.name);
      return { submit: fakeSubmit };
    },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg, toolNames };
}

// ---------------------------------------------------------------------------
// U011 gap: per-spawn auto-approve `allowedTools` allowlist (distinct from the
// availability `allowed_tools` scope). Parity: Claude inProcessRunner allowedTools.
// ---------------------------------------------------------------------------
describe('parseAutoApproveTools (shape validation)', () => {
  it('treats an absent value as no auto-approvals', () => {
    assert.deepEqual(parseAutoApproveTools(undefined), { value: undefined });
    assert.deepEqual(parseAutoApproveTools(null), { value: undefined });
  });
  it('accepts an array of strings', () => {
    assert.deepEqual(parseAutoApproveTools(['bash', 'grep']), { value: ['bash', 'grep'] });
  });
  it('rejects a non-array / non-string-element shape with a field-specific message', () => {
    const a = parseAutoApproveTools('bash');
    const b = parseAutoApproveTools([1, 2]);
    assert.ok('error' in a);
    assert.ok('error' in b);
    assert.match((a as { error: string }).error, /auto_approve_tools must be an array of strings/);
  });
});

describe('buildAutoApproveRules', () => {
  it('returns [] for an empty / undefined list (no rules to add)', () => {
    assert.deepEqual(buildAutoApproveRules(undefined), []);
    assert.deepEqual(buildAutoApproveRules([]), []);
  });
  it('maps a bare tool name to a tool-wide allow rule', () => {
    assert.deepEqual(buildAutoApproveRules(['file_read']), [{ tool: 'file_read', action: 'allow' }]);
  });
  it('canonicalizes a cross-framework alias to the AMI tool name', () => {
    assert.deepEqual(buildAutoApproveRules(['Bash', 'Read']), [
      { tool: 'bash', action: 'allow' },
      { tool: 'file_read', action: 'allow' },
    ]);
  });
  it('scopes a patterned spec to its arg glob', () => {
    assert.deepEqual(buildAutoApproveRules(['Bash(git status:*)']), [
      { tool: 'bash', action: 'allow', pattern: 'git status:*' },
    ]);
  });
});

describe('task tool schema advertises auto_approve_tools', () => {
  it('exposes an optional auto_approve_tools array distinct from allowed_tools', () => {
    const props = taskTool.inputSchema.properties as Record<string, { type?: string; items?: { type?: string } }>;
    assert.ok('auto_approve_tools' in props);
    assert.equal(props.auto_approve_tools.type, 'array');
    assert.equal(props.auto_approve_tools.items?.type, 'string');
    assert.ok('allowed_tools' in props, 'availability allowed_tools remains a separate field');
    assert.ok(!taskTool.inputSchema.required?.includes('auto_approve_tools'));
  });
});

describe('auto_approve_tools wiring into the spawned subagent', () => {
  beforeEach(() => resetAgentRegistry());

  it('prepends allow rules to the child permission set without changing availability', async () => {
    const parentRule: PermissionRule = { tool: 'web_fetch', action: 'deny' };
    const { cfg, toolNames, isError } = await runTask(
      { prompt: 'go', mode: 'general', auto_approve_tools: ['bash', 'Read'] },
      { _parentPermissionRules: [parentRule] } as Partial<ToolContext>,
    );
    assert.ok(!isError);
    // Auto-approve rules are prepended, inherited parent rules preserved after them.
    assert.deepEqual(cfg!.permissionRules, [
      { tool: 'bash', action: 'allow' },
      { tool: 'file_read', action: 'allow' },
      parentRule,
    ]);
    // Availability is unchanged — the full general tool set is still present (the
    // auto-approve list does NOT intersect/scope the tools the way allowed_tools does).
    assert.ok(toolNames.includes('bash'));
    assert.ok(toolNames.includes('file_edit'));
  });

  it('leaves permissionRules as the inherited parent rules when the field is absent', async () => {
    const parentRule: PermissionRule = { tool: 'bash', action: 'ask' };
    const { cfg } = await runTask(
      { prompt: 'go', mode: 'general' },
      { _parentPermissionRules: [parentRule] } as Partial<ToolContext>,
    );
    assert.deepEqual(cfg!.permissionRules, [parentRule]);
  });

  it('rejects a malformed auto_approve_tools shape', async () => {
    const { isError, output } = await runTask({ prompt: 'go', auto_approve_tools: 'bash' });
    assert.ok(isError);
    assert.match(output, /auto_approve_tools must be an array of strings/);
  });

  it('rejects an unknown tool name with the available-tools listing', async () => {
    const { isError, output } = await runTask({ prompt: 'go', auto_approve_tools: ['not_a_tool'] });
    assert.ok(isError);
    assert.match(output, /auto_approve_tools names unknown tool\(s\): not_a_tool/);
    assert.match(output, /Available tools:/);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: the generated rules, fed to the REAL permission layer, pre-approve
// the listed tool/pattern while leaving every unlisted tool to prompt per the mode
// — and a deny rule still wins. This proves the whole wired path, not a unit alone.
// ---------------------------------------------------------------------------
describe('auto_approve_tools end-to-end through the permission layer', () => {
  it('flips an otherwise-prompted tool from ask to allow, unlisted tools still ask', async () => {
    const rules = buildAutoApproveRules(['file_edit']);
    const withRule = new PermissionManager('ask', rules);
    const noRule = new PermissionManager('ask', []);
    const edit = { file_path: '/tmp/x.ts', old_string: 'a', new_string: 'b' };
    // Discriminator: the ONLY difference is the auto-approve rule.
    assert.equal(await noRule.check('file_edit', edit), 'ask');
    assert.equal(await withRule.check('file_edit', edit), 'allow');
    // An unlisted mutation tool is unaffected — still prompts per the mode.
    assert.equal(await withRule.check('file_write', { file_path: '/tmp/y.ts', content: 'z' }), 'ask');
  });

  it('honors a patterned auto-approve (matching path allowed, non-matching asks)', async () => {
    // A path-scoped allow on a mutation tool: edits under src/ are pre-approved,
    // edits elsewhere still prompt per the mode.
    const pm = new PermissionManager('ask', buildAutoApproveRules(['Edit(src/**)']));
    assert.equal(await pm.check('file_edit', { file_path: 'src/app.ts', old_string: 'a', new_string: 'b' }), 'allow');
    assert.equal(await pm.check('file_edit', { file_path: 'lib/app.ts', old_string: 'a', new_string: 'b' }), 'ask');
  });

  it('never overrides a matching deny rule (deny wins unconditionally)', async () => {
    const rules = [
      ...buildAutoApproveRules(['bash']),
      { tool: 'bash', action: 'deny' } as PermissionRule,
    ];
    const pm = new PermissionManager('ask', rules);
    assert.equal(await pm.check('bash', { command: 'echo hi' }), 'deny');
  });
});
