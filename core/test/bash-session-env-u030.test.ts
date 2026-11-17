import { describe, it, before, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool } from '../src/tools/bash';
import {
  setSessionEnvVar,
  clearSessionEnvVars,
} from '../src/session-env-vars';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// Deterministic: never wrap these echo probes in a container.
let prevSandbox: string | undefined;
before(() => {
  prevSandbox = process.env.AMI_SANDBOX;
  process.env.AMI_SANDBOX = 'off';
});
after(() => {
  if (prevSandbox === undefined) delete process.env.AMI_SANDBOX;
  else process.env.AMI_SANDBOX = prevSandbox;
});
afterEach(() => {
  clearSessionEnvVars('u030-env-a');
  clearSessionEnvVars('u030-env-b');
});

describe('U030 — session /env vars applied to bash child processes (end-to-end)', () => {
  it('exports a session-scoped var into the spawned command', async () => {
    setSessionEnvVar('U030_SESSION_VAR', 'hello-world', 'u030-env-a');
    const res = await bashTool.execute(
      { command: 'echo "val=[$U030_SESSION_VAR]"' },
      ctx({ sessionId: 'u030-env-a' }),
    );
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /val=\[hello-world\]/);
  });

  it('does NOT leak the var into a different session', async () => {
    setSessionEnvVar('U030_SESSION_VAR', 'secret', 'u030-env-a');
    const res = await bashTool.execute(
      { command: 'echo "val=[$U030_SESSION_VAR]"' },
      ctx({ sessionId: 'u030-env-b' }),
    );
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /val=\[\]/);
  });

  it('does not export anything when the session has no vars (no behavior change)', async () => {
    const res = await bashTool.execute(
      { command: 'echo "val=[$U030_SESSION_VAR]"' },
      ctx({ sessionId: 'u030-env-a' }),
    );
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /val=\[\]/);
  });

  it('keeps the framework skillEnv taking precedence over a user-set var of the same name', async () => {
    setSessionEnvVar('CLAUDE_SKILL_DIR', '/user/attempt', 'u030-env-a');
    const res = await bashTool.execute(
      { command: 'echo "dir=[$CLAUDE_SKILL_DIR]"' },
      ctx({ sessionId: 'u030-env-a', skillEnv: { CLAUDE_SKILL_DIR: '/framework/wins' } }),
    );
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /dir=\[\/framework\/wins\]/);
  });
});
