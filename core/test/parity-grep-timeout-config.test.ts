import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks, grepTimeoutMs, isWSL } from '../src/tools/grep';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, ...overrides };
}

const savedTimeoutMs = grepHooks.timeoutMs;
const savedKillGraceMs = grepHooks.killGraceMs;
afterEach(() => {
  mock.restoreAll();
  grepHooks.timeoutMs = savedTimeoutMs;
  grepHooks.killGraceMs = savedKillGraceMs;
  delete process.env.RIPGREP_CONFIG_PATH;
});

describe('grepTimeoutMs / isWSL', () => {
  it('honors an explicit AMI_GREP_TIMEOUT_SECONDS override', () => {
    assert.equal(grepTimeoutMs({ AMI_GREP_TIMEOUT_SECONDS: '7' }), 7000);
  });
  it('bumps the default to 60s under WSL', () => {
    assert.equal(grepTimeoutMs({ WSL_DISTRO_NAME: 'Ubuntu' }), 60_000);
  });
  it('falls back to the 20s default off-WSL with no override', () => {
    assert.equal(grepTimeoutMs({}), 20_000);
  });
  it('ignores a non-positive / non-numeric override', () => {
    assert.equal(grepTimeoutMs({ AMI_GREP_TIMEOUT_SECONDS: 'abc' }), 20_000);
    assert.equal(grepTimeoutMs({ AMI_GREP_TIMEOUT_SECONDS: '0' }), 20_000);
  });
  it('isWSL detects the interop env flag and returns a boolean otherwise', () => {
    assert.equal(isWSL({ WSL_INTEROP: '/run/x' }), true);
    assert.equal(typeof isWSL({}), 'boolean');
  });
});

describe('grep --no-config + RIPGREP_CONFIG_PATH neutralization', () => {
  it('passes --no-config to rg and strips RIPGREP_CONFIG_PATH from the child env', async () => {
    process.env.RIPGREP_CONFIG_PATH = '/evil/.ripgreprc';
    let seenArgs: string[] = [];
    let seenEnv: NodeJS.ProcessEnv | undefined;
    mock.method(grepHooks, 'spawn', (_cmd: string, args: string[], opts: { env?: NodeJS.ProcessEnv }) => {
      seenArgs = args;
      seenEnv = opts.env;
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => true;
      // rg exit 1 = ran, matched nothing (valid empty result).
      setImmediate(() => child.emit('close', 1));
      return child;
    });
    await grepTool.execute!({ pattern: 'needle' }, ctx());
    assert.ok(seenArgs.includes('--no-config'), 'rg must run with --no-config');
    assert.ok(seenEnv && !('RIPGREP_CONFIG_PATH' in seenEnv), 'child env must not carry RIPGREP_CONFIG_PATH');
  });
});

describe('grep returns partial results on timeout', () => {
  it('salvages collected matches (dropping the last incomplete line) with a timeout note', async () => {
    grepHooks.timeoutMs = 20;
    grepHooks.killGraceMs = 10;
    mock.method(grepHooks, 'spawn', () => {
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => true;
      // Emit two complete lines + a trailing incomplete one, then never close so the
      // timeout path fires and salvages the partial output.
      setImmediate(() => child.stdout.emit('data', Buffer.from('a.ts:1:alpha\nb.ts:2:beta\nc.ts:3:incomple')));
      return child;
    });
    const res = await grepTool.execute!({ pattern: 'x', output_mode: 'content' }, ctx());
    assert.notEqual(res.isError, true, 'partial results are not an error');
    assert.match(res.output, /timed out/);
    assert.ok(res.output.includes('alpha'), 'complete lines retained');
    assert.ok(!res.output.includes('incomple'), 'the trailing incomplete line is dropped');
  });

  it('errors out when the timeout produced no usable output', async () => {
    grepHooks.timeoutMs = 20;
    grepHooks.killGraceMs = 10;
    mock.method(grepHooks, 'spawn', () => {
      const child: any = new EventEmitter();
      child.stdout = new EventEmitter();
      child.stderr = new EventEmitter();
      child.kill = () => true;
      return child; // never emits anything, never closes
    });
    const res = await grepTool.execute!({ pattern: 'x' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /timed out/);
  });
});
