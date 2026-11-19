import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  userMessageTool,
  isOutputGateEnabled,
  OUTPUT_GATE_ENV,
  OUTPUT_GATE_ENV_COMPAT,
  USER_MESSAGE_MAX_CHARS,
} from '../src/tools/user-message';

const savedGate = process.env[OUTPUT_GATE_ENV];
const savedCompat = process.env[OUTPUT_GATE_ENV_COMPAT];

afterEach(() => {
  if (savedGate === undefined) delete process.env[OUTPUT_GATE_ENV];
  else process.env[OUTPUT_GATE_ENV] = savedGate;
  if (savedCompat === undefined) delete process.env[OUTPUT_GATE_ENV_COMPAT];
  else process.env[OUTPUT_GATE_ENV_COMPAT] = savedCompat;
});

describe('userMessageTool', () => {
  it('has the expected name, schema, and read-only flag', () => {
    assert.equal(userMessageTool.name, 'user_message');
    assert.ok(userMessageTool.isReadOnly);
    assert.ok(userMessageTool.inputSchema.properties.message);
    assert.deepEqual(userMessageTool.inputSchema.required, ['message']);
  });

  it('returns the message verbatim as the user-facing output', async () => {
    const result = await userMessageTool.execute!({ message: 'Hello user' }, {} as never);
    assert.equal(result.output, 'Hello user');
    assert.equal(result.isError, false);
    assert.deepEqual(result.metadata, { userFacing: true });
  });

  it('rejects a missing or empty message', async () => {
    const missing = await userMessageTool.execute!({}, {} as never);
    assert.equal(missing.isError, true);
    const blank = await userMessageTool.execute!({ message: '   ' }, {} as never);
    assert.equal(blank.isError, true);
    const nonString = await userMessageTool.execute!({ message: 42 }, {} as never);
    assert.equal(nonString.isError, true);
  });

  it('truncates an over-length message with an explicit marker', async () => {
    const huge = 'x'.repeat(USER_MESSAGE_MAX_CHARS + 500);
    const result = await userMessageTool.execute!({ message: huge }, {} as never);
    assert.equal(result.isError, false);
    assert.ok(result.output.length < huge.length);
    assert.ok(result.output.includes('[... message truncated ...]'));
  });

  it('is enabled only when the opt-in env flag is set (canonical or compat)', () => {
    delete process.env[OUTPUT_GATE_ENV];
    delete process.env[OUTPUT_GATE_ENV_COMPAT];
    assert.equal(isOutputGateEnabled(), false);
    assert.equal(userMessageTool.isEnabled!(), false);

    process.env[OUTPUT_GATE_ENV] = '1';
    assert.equal(isOutputGateEnabled(), true);
    assert.equal(userMessageTool.isEnabled!(), true);

    delete process.env[OUTPUT_GATE_ENV];
    process.env[OUTPUT_GATE_ENV_COMPAT] = 'true';
    assert.equal(isOutputGateEnabled(), true);
  });

  it('treats a falsey env token as disabled', () => {
    process.env[OUTPUT_GATE_ENV] = '0';
    delete process.env[OUTPUT_GATE_ENV_COMPAT];
    assert.equal(isOutputGateEnabled(), false);
  });
});
