import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';

import {
  testingPermissionTool,
  isTestingPermissionEnabled,
  TESTING_PERMISSION_TOOL_NAME,
  TESTING_PERMISSION_ENV,
  createDefaultTools,
} from '../src/tools';
import { PermissionManager } from '../src/permissions';
import type { ToolContext } from '../src/types';

const ctx = (): ToolContext =>
  ({ cwd: os.tmpdir(), abortSignal: new AbortController().signal } as unknown as ToolContext);

const savedEnv = process.env[TESTING_PERMISSION_ENV];
afterEach(() => {
  if (savedEnv === undefined) delete process.env[TESTING_PERMISSION_ENV];
  else process.env[TESTING_PERMISSION_ENV] = savedEnv;
});

describe('U628 testing_permission — env gate', () => {
  it('is OFF by default (invisible in production)', () => {
    assert.equal(isTestingPermissionEnabled({}), false);
  });

  it('accepts "1" and "true" (case-insensitive), rejects other values', () => {
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: '1' }), true);
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: 'true' }), true);
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: 'TRUE' }), true);
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: ' 1 ' }), true);
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: '0' }), false);
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: 'no' }), false);
    assert.equal(isTestingPermissionEnabled({ [TESTING_PERMISSION_ENV]: '' }), false);
  });

  it('reads process.env by default', () => {
    delete process.env[TESTING_PERMISSION_ENV];
    assert.equal(isTestingPermissionEnabled(), false);
    process.env[TESTING_PERMISSION_ENV] = '1';
    assert.equal(isTestingPermissionEnabled(), true);
  });
});

describe('U628 testing_permission — tool definition', () => {
  it('is a side-effect-free no-op whose execute always succeeds', async () => {
    const result = await testingPermissionTool.execute({}, ctx());
    assert.equal(result.isError, undefined);
    assert.ok(result.output.includes(`${TESTING_PERMISSION_TOOL_NAME} executed successfully`));
  });

  it('declares non-read-only (so it routes through the permission-asking path)', () => {
    // INTENTIONAL DIVERGENCE from Claude's isReadOnly()===true: a read-only tool
    // takes AMI's no-prompt fast path and would never reach the prompt.
    assert.equal(testingPermissionTool.isReadOnly, false);
    assert.equal(testingPermissionTool.name, TESTING_PERMISSION_TOOL_NAME);
  });

  it('isEnabled reflects the opt-in env var', () => {
    delete process.env[TESTING_PERMISSION_ENV];
    assert.equal(testingPermissionTool.isEnabled!(), false);
    process.env[TESTING_PERMISSION_ENV] = '1';
    assert.equal(testingPermissionTool.isEnabled!(), true);
  });
});

describe('U628 testing_permission — registry gating', () => {
  it('is dropped from createDefaultTools by default', () => {
    delete process.env[TESTING_PERMISSION_ENV];
    const names = createDefaultTools('/tmp').getAll().map(t => t.name);
    assert.equal(names.includes(TESTING_PERMISSION_TOOL_NAME), false);
  });

  it('is registered + advertised once the opt-in env var is set', () => {
    process.env[TESTING_PERMISSION_ENV] = '1';
    const registry = createDefaultTools('/tmp');
    assert.ok(registry.get(TESTING_PERMISSION_TOOL_NAME), 'tool should be registered');
    assert.ok(
      registry.toOpenAIFormat().some(t => t.function.name === TESTING_PERMISSION_TOOL_NAME),
      'tool should be advertised to the model',
    );
  });
});

describe('U628 testing_permission — centralized permission guard (unconditional ask)', () => {
  for (const mode of ['ask', 'auto-allow', 'acceptEdits', 'bypassPermissions', 'auto', 'plan'] as const) {
    it(`asks unconditionally in ${mode} mode`, async () => {
      const pm = new PermissionManager(mode);
      assert.equal(await pm.check(TESTING_PERMISSION_TOOL_NAME, {}), 'ask');
    });
  }

  it('denies in deny-all mode (no interactive prompt to answer)', async () => {
    const pm = new PermissionManager('deny-all');
    assert.equal(await pm.check(TESTING_PERMISSION_TOOL_NAME, {}), 'deny');
  });

  it('an explicit deny rule still wins over the unconditional ask', async () => {
    const pm = new PermissionManager('ask', [{ tool: TESTING_PERMISSION_TOOL_NAME, action: 'deny' }]);
    assert.equal(await pm.check(TESTING_PERMISSION_TOOL_NAME, {}), 'deny');
  });

  it('a standing allow rule can NOT silently auto-approve it', async () => {
    const pm = new PermissionManager('ask', [{ tool: TESTING_PERMISSION_TOOL_NAME, action: 'allow' }]);
    assert.equal(await pm.check(TESTING_PERMISSION_TOOL_NAME, {}), 'ask');
  });

  it('checkWithReason surfaces the ask decision with a structured reason', async () => {
    const pm = new PermissionManager('ask');
    const verdict = await pm.checkWithReason(TESTING_PERMISSION_TOOL_NAME, {});
    assert.equal(verdict.decision, 'ask');
    assert.ok(verdict.reason, 'expected a structured reason');
    assert.ok(verdict.message, 'expected a prompt message');
  });
});
