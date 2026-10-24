import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';
import { PassThrough } from 'stream';

import { expandPath, isPathSafe, toRelativePath } from '../src/utils/path';
import { execCommand, shellHooks } from '../src/utils/shell';
import {
  detectCpuCount,
  getMaxConcurrent,
  stripMeta,
  parseWorkflowMeta,
  executeWorkflow,
  WorkflowRuntime,
  _wfHooks,
} from '../src/tools/workflow-runtime';

afterEach(() => {
  mock.restoreAll();
  mock.timers.reset();
});

// ===========================================================================
// path.ts — remaining branches
// ===========================================================================

describe('path.ts — branch completion', () => {
  it('expandPath expands a bare "~" (first OR arm)', () => {
    // Exercises `resolved === "~"` which the "~/" and "~\\" tests never hit.
    assert.equal(expandPath('~', '/tmp'), path.normalize(os.homedir()));
  });

  it('isPathSafe accepts a cwd that already ends in a separator', () => {
    const cwd = '/home/user/project' + path.sep;
    assert.equal(isPathSafe('/home/user/project/src/a.ts', cwd), true);
  });

  it('isPathSafe rejects a sibling directory that only shares a prefix', () => {
    // "/home/user2" must NOT be considered inside "/home/user".
    const home = os.homedir();
    if (!'/home/user2'.startsWith(home) && !home.startsWith('/home/user2')) {
      assert.equal(isPathSafe('/home/user2/secret', '/home/user'), false);
    }
    // Deterministic structural variant independent of the real homedir:
    assert.equal(
      toRelativePath('/home/user2/secret', '/home/user'),
      path.normalize('/home/user2/secret'),
    );
  });
});

// ===========================================================================
// shell.ts — deterministic fake-child-process driver
// ===========================================================================

class FakeProc extends EventEmitter {
  stdout = new PassThrough();
  stderr = new PassThrough();
  stdin = new PassThrough();
  pid: number | undefined = 4242;
  killed = false;
  killThrows = false;
  kill(_sig?: NodeJS.Signals | number): boolean {
    this.killed = true;
    if (this.killThrows) throw new Error('proc.kill failed');
    return true;
  }
}

function installFakeSpawn(fake: FakeProc): void {
  mock.method(shellHooks, 'spawn', () => fake as unknown as ReturnType<typeof shellHooks.spawn>);
}

/** Record calls to the process-group kill so we never signal a real pid. */
function spyKill(): Array<[number, NodeJS.Signals]> {
  const calls: Array<[number, NodeJS.Signals]> = [];
  mock.method(shellHooks, 'kill', (pid: number, sig: NodeJS.Signals) => { calls.push([pid, sig]); });
  return calls;
}

const flush = (): Promise<void> => new Promise((r) => setImmediate(r));

describe('shell.ts — streaming via injected child process', () => {
  it('captures stdout + stderr and forwards both to onData, then close', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    const chunks: string[] = [];
    const p = execCommand('whatever', { cwd: '/tmp', stallTimeoutMs: 0, onData: (c) => chunks.push(c) });
    fake.stdout.write('OUT-data');
    fake.stderr.write('ERR-data');
    await flush();
    fake.emit('close', 0);
    const r = await p;
    assert.equal(r.exitCode, 0);
    assert.ok(r.stdout.includes('OUT-data'));
    assert.ok(r.stderr.includes('ERR-data'));
    const all = chunks.join('');
    assert.ok(all.includes('OUT-data') && all.includes('ERR-data'));
  });

  it('ignores EBADF errors on stdout/stderr but still resolves on close', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs: 0 });
    fake.stdout.emit('error', Object.assign(new Error('badf-out'), { code: 'EBADF' }));
    fake.stderr.emit('error', Object.assign(new Error('badf-err'), { code: 'EBADF' }));
    fake.emit('close', 0);
    const r = await p;
    assert.equal(r.exitCode, 0);
    assert.ok(!r.stderr.includes('badf-out'));
    assert.ok(!r.stderr.includes('badf-err'));
  });

  it('a non-EBADF stdout error is recorded and finishes with null', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs: 0 });
    fake.stdout.emit('error', Object.assign(new Error('stdout-broke'), { code: 'EIO' }));
    const r = await p;
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('stdout-broke'));
  });

  it('a non-EBADF stderr error is recorded and finishes with null', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs: 0 });
    fake.stderr.emit('error', Object.assign(new Error('stderr-broke'), { code: 'EPIPE' }));
    const r = await p;
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('stderr-broke'));
  });

  it('a process "error" event is captured in stderr with null exit', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs: 0 });
    fake.emit('error', new Error('spawn-exploded'));
    const r = await p;
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('spawn-exploded'));
  });
});

describe('shell.ts — timeout + killTree (unix / windows / fallbacks)', () => {
  it('timeout kills the unix process group via shellHooks.kill', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    const kills = spyKill();
    const r = await execCommand('x', { cwd: '/tmp', timeout: 10, stallTimeoutMs: 0 });
    assert.equal(r.exitCode, null);
    assert.deepEqual(kills[0], [-4242, 'SIGKILL']);
    assert.ok(fake.stdout.destroyed);
  });

  it('timeout on windows uses taskkill via shellHooks.execSync', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    mock.method(shellHooks, 'platform', () => 'win32' as NodeJS.Platform);
    let cmd = '';
    mock.method(shellHooks, 'execSync', (c: string) => { cmd = c; return Buffer.from(''); });
    const r = await execCommand('x', { cwd: '/tmp', timeout: 10, stallTimeoutMs: 0 });
    assert.equal(r.exitCode, null);
    assert.ok(cmd.includes('taskkill'));
    assert.ok(cmd.includes('4242'));
  });

  it('killTree falls back to proc.kill when the group kill throws', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    mock.method(shellHooks, 'kill', () => { throw new Error('ESRCH'); });
    const r = await execCommand('x', { cwd: '/tmp', timeout: 10, stallTimeoutMs: 0 });
    assert.equal(r.exitCode, null);
    assert.equal(fake.killed, true);
  });

  it('killTree swallows an error even when the fallback proc.kill also throws', async () => {
    const fake = new FakeProc();
    fake.killThrows = true;
    installFakeSpawn(fake);
    mock.method(shellHooks, 'kill', () => { throw new Error('ESRCH'); });
    const r = await execCommand('x', { cwd: '/tmp', timeout: 10, stallTimeoutMs: 0 });
    assert.equal(r.exitCode, null);
    assert.equal(fake.killed, true);
  });

  it('skips a stream that is already destroyed and survives a throwing destroy()', async () => {
    const fake = new FakeProc();
    // stdout already destroyed -> the `!stream.destroyed` arm is exercised.
    fake.stdout.destroy();
    // stderr.destroy() throws -> the try/catch around destroy() is exercised.
    (fake.stderr as unknown as { destroy: () => void }).destroy = () => { throw new Error('nope'); };
    installFakeSpawn(fake);
    spyKill();
    const r = await execCommand('x', { cwd: '/tmp', timeout: 10, stallTimeoutMs: 0 });
    assert.equal(r.exitCode, null);
  });

  it('killTree is a no-op when the child has no pid', async () => {
    const fake = new FakeProc();
    fake.pid = undefined;
    installFakeSpawn(fake);
    const kills = spyKill();
    const r = await execCommand('x', { cwd: '/tmp', timeout: 10, stallTimeoutMs: 0 });
    assert.equal(r.exitCode, null);
    assert.equal(kills.length, 0);
  });
});

describe('shell.ts — abort handling', () => {
  it('short-circuits when the signal is already aborted', async () => {
    const ac = new AbortController();
    ac.abort();
    const r = await execCommand('x', { cwd: '/tmp', abortSignal: ac.signal });
    assert.equal(r.exitCode, null);
    assert.equal(r.stderr, 'Aborted');
  });

  it('aborts a running child and cleans up listeners', async () => {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    spyKill();
    const ac = new AbortController();
    const p = execCommand('x', { cwd: '/tmp', abortSignal: ac.signal, stallTimeoutMs: 0 });
    ac.abort();
    const r = await p;
    assert.equal(r.exitCode, null);
    // A second close after settling must not change the result.
    fake.emit('close', 0);
    assert.equal((await p).exitCode, null);
  });
});

describe('shell.ts — stall detection (mock timers drive looksLikePrompt)', () => {
  async function runStall(output: string, stallTimeoutMs: number, tickMs: number): Promise<{ exitCode: number | null; stderr: string }> {
    const fake = new FakeProc();
    installFakeSpawn(fake);
    spyKill();
    mock.timers.enable({ apis: ['setInterval', 'setTimeout', 'Date'] });
    const p = execCommand('x', { cwd: '/tmp', stallTimeoutMs, timeout: 10_000_000 });
    if (output) { fake.stdout.write(output); await flush(); }
    mock.timers.tick(tickMs);
    // If stall did not kill, deliver a normal close so the promise settles.
    fake.emit('close', 0);
    return p;
  }

  it('kills a child whose last line looks like an interactive prompt', async () => {
    const r = await runStall('Password: ', 1000, 5000);
    assert.equal(r.exitCode, null);
    assert.ok(r.stderr.includes('Stall detected'));
  });

  it('does NOT kill when output does not look like a prompt (some() false)', async () => {
    const r = await runStall('just regular build output line', 1000, 5000);
    assert.equal(r.exitCode, 0);
    assert.ok(!r.stderr.includes('Stall detected'));
  });

  it('does NOT kill when the last line is empty (length 0 guard)', async () => {
    const r = await runStall('', 1000, 5000);
    assert.equal(r.exitCode, 0);
  });

  it('does NOT kill when the last line exceeds 200 chars (length guard)', async () => {
    const r = await runStall('x'.repeat(201) + '?', 1000, 5000);
    assert.equal(r.exitCode, 0);
  });

  it('does NOT kill when elapsed is below the stall threshold', async () => {
    const r = await runStall('Continue? ', 20000, 5000);
    assert.equal(r.exitCode, 0);
  });
});

describe('shell.ts — real subprocess smoke (defaults wired)', () => {
  it('runs through the real default hooks end to end', async () => {
    const r = await execCommand('echo real-run', { cwd: '/tmp', stallTimeoutMs: 0 });
    assert.equal(r.exitCode, 0);
    assert.ok(r.stdout.includes('real-run'));
  });

  it('passes a custom env (exercises the env spread branch)', async () => {
    const r = await execCommand('echo $COV_ENV_PROBE', {
      cwd: '/tmp',
      stallTimeoutMs: 0,
      env: { ...process.env, COV_ENV_PROBE: 'env-visible' },
    });
    assert.ok(r.stdout.includes('env-visible'));
  });
});

// ===========================================================================
// workflow-runtime.ts — detectCpuCount cgroup seams
// ===========================================================================

describe('detectCpuCount — cgroup detection branches', () => {
  function mockReads(map: (p: string) => string): void {
    mock.method(_wfHooks, 'readFileSync', (p: unknown) => {
      const s = String(p);
      const v = map(s);
      if (v === '__throw__') throw new Error('ENOENT');
      return v;
    });
  }

  it('cgroup v2 with a numeric quota returns floor(quota/period)', () => {
    mockReads((p) => (p.includes('cpu.max') ? '200000 100000' : '__throw__'));
    assert.equal(detectCpuCount(), 2);
  });

  it('cgroup v2 "max" means unlimited -> falls back', () => {
    mockReads((p) => (p.includes('cpu.max') ? 'max 100000' : '__throw__'));
    assert.ok(detectCpuCount() >= 1);
  });

  it('cgroup v2 non-positive quota -> falls through to v1', () => {
    mockReads((p) => (p.includes('cpu.max') ? '0 100000' : '__throw__'));
    assert.ok(detectCpuCount() >= 1);
  });

  it('cgroup v1 quota/period returns the computed cpu count', () => {
    mockReads((p) => {
      if (p.includes('cpu.max')) return '__throw__';
      if (p.includes('cfs_quota_us')) return '400000';
      if (p.includes('cfs_period_us')) return '100000';
      return '__throw__';
    });
    assert.equal(detectCpuCount(), 4);
  });

  it('cgroup v2 non-positive period -> falls through (period>0 false arm)', () => {
    mockReads((p) => (p.includes('cpu.max') ? '200000 0' : '__throw__'));
    assert.ok(detectCpuCount() >= 1);
  });

  it('cgroup v1 non-positive period -> osCpus fallback (period>0 false arm)', () => {
    mockReads((p) => {
      if (p.includes('cpu.max')) return '__throw__';
      if (p.includes('cfs_quota_us')) return '400000';
      if (p.includes('cfs_period_us')) return '0';
      return '__throw__';
    });
    assert.ok(detectCpuCount() >= 1);
  });

  it('cgroup v1 non-positive quota -> osCpus fallback', () => {
    mockReads((p) => {
      if (p.includes('cpu.max')) return '__throw__';
      if (p.includes('cfs_quota_us')) return '-1';
      if (p.includes('cfs_period_us')) return '100000';
      return '__throw__';
    });
    assert.ok(detectCpuCount() >= 1);
  });

  it('every read throwing -> osCpus fallback', () => {
    mockReads(() => '__throw__');
    assert.ok(detectCpuCount() >= 1);
  });

  it('falls back to 8 when os.cpus() reports nothing', () => {
    mockReads(() => '__throw__');
    mock.method(_wfHooks, 'cpus', () => undefined as unknown as ReturnType<typeof os.cpus>);
    assert.equal(detectCpuCount(), 8);
  });
});

describe('getMaxConcurrent — env override + default', () => {
  const ORIG = process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
  afterEach(() => {
    if (ORIG === undefined) delete process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS;
    else process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = ORIG;
  });

  it('honours an in-range env value', () => {
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '7';
    assert.equal(getMaxConcurrent(), 7);
  });

  it('ignores an out-of-range env value and uses cpu-derived default', () => {
    process.env.AMI_WORKFLOW_MAX_CONCURRENT_AGENTS = '9999';
    const v = getMaxConcurrent();
    assert.ok(v >= 4 && v <= 16);
  });
});

// ===========================================================================
// workflow-runtime.ts — stripMeta / parseWorkflowMeta edges
// ===========================================================================

describe('stripMeta — brace counting edge cases', () => {
  it('returns the script unchanged when there is no meta block', () => {
    const s = 'const x = 1\nreturn x';
    assert.equal(stripMeta(s), s);
  });

  it('ignores braces that appear inside string and template literals', () => {
    const s = "export const meta = { name: '}{', d: `${1}`, e: \"}\" }\nreturn 1";
    const out = stripMeta(s);
    assert.ok(!out.includes('name:'));
    assert.ok(out.includes('return 1'));
  });

  it('handles escaped quote characters inside a string value', () => {
    const s = "export const meta = { name: 'a\\'b}' }\nreturn 2";
    const out = stripMeta(s);
    assert.ok(!out.includes('name:'));
    assert.ok(out.includes('return 2'));
  });

  it('handles nested object braces (phases with detail objects)', () => {
    const s = "export const meta = { phases: [{ title: 'p', detail: { a: 1 } }] }\nreturn 3";
    const out = stripMeta(s);
    assert.ok(!out.includes('phases'));
    assert.ok(out.includes('return 3'));
  });
});

describe('parseWorkflowMeta — edges', () => {
  it('returns null when there is no meta header', () => {
    assert.equal(parseWorkflowMeta('const x = 1'), null);
  });

  it('returns null for an unbalanced (never-closing) meta brace', () => {
    assert.equal(parseWorkflowMeta("export const meta = { name: 'x'"), null);
  });

  it('returns null when the meta literal is not valid JS', () => {
    assert.equal(parseWorkflowMeta('export const meta = { name: @@@ }'), null);
  });

  it('parses name/description/phases/whenToUse', () => {
    const m = parseWorkflowMeta(
      "export const meta = { name: 'n', description: 'd', phases: [{ title: 't' }], whenToUse: 'when' }",
    );
    assert.deepEqual(m, { name: 'n', description: 'd', phases: [{ title: 't' }], whenToUse: 'when' });
  });

  it('defaults missing fields and drops a non-array phases', () => {
    const m = parseWorkflowMeta('export const meta = { phases: 5 }');
    assert.deepEqual(m, { name: '', description: '', phases: undefined, whenToUse: undefined });
  });
});

// ===========================================================================
// workflow-runtime.ts — runtime behaviour
// ===========================================================================

const CHILD = `
export const meta = { name: 'child', description: 'c' }
log('child-log')
await agent('child-agent')
return { ok: true }
`;

const CHILD_NO_RETURN = `
export const meta = { name: 'child', description: 'c' }
log('noret')
`;

// A child that never calls agent(), so an already-aborted child runtime does
// not short-circuit before line 199 executes.
const CHILD_SAFE = `
export const meta = { name: 'child', description: 'c' }
log('child-log')
return { ok: true }
`;

describe('WorkflowRuntime — workflow() caller', () => {
  it('propagates a pre-existing abort into the child runtime', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => CHILD_SAFE,
    });
    const ctx = runtime.createContext({});
    runtime.abort();
    // Child is created already-aborted (covers the abort-propagation branch) but
    // it never calls agent(), so it still runs to completion.
    const res = (await ctx.workflow('child')) as { ok: boolean };
    assert.ok(res.ok);
    assert.ok(runtime.logs.includes('child-log'));
  });

  it('a later parent abort fires the registered child-abort listener', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => CHILD,
    });
    const ctx = runtime.createContext({});
    await ctx.workflow('child');
    // The listener registered during the call must not throw when fired.
    assert.doesNotThrow(() => runtime.abort());
  });

  it('returns null when the child workflow returns nothing', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => CHILD_NO_RETURN,
    });
    const ctx = runtime.createContext({});
    const res = await ctx.workflow('child');
    assert.equal(res, null);
  });

  it('reports the scriptPath in the not-found error for an object ref', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => null,
    });
    const ctx = runtime.createContext({});
    await assert.rejects(
      () => ctx.workflow({ scriptPath: '/tmp/missing-wf.js' }),
      /Workflow not found: \/tmp\/missing-wf\.js/,
    );
  });

  it('reports the name in the not-found error for a string ref', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => null,
    });
    const ctx = runtime.createContext({});
    await assert.rejects(
      () => ctx.workflow('no-such-workflow'),
      /Workflow not found: no-such-workflow/,
    );
  });
});

describe('WorkflowRuntime — agent guardrails', () => {
  it('rejects agent() after abort', async () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = runtime.createContext({});
    runtime.abort();
    await assert.rejects(() => ctx.agent('p') as Promise<unknown>, /Workflow aborted/);
  });

  it('rejects agent() once the token budget is exhausted', async () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x', budgetTotal: 10 });
    runtime.addTokens(10);
    const ctx = runtime.createContext({});
    assert.equal(ctx.budget.remaining(), 0);
    assert.equal(ctx.budget.spent(), 10);
    await assert.rejects(() => ctx.agent('p') as Promise<unknown>, /Budget exhausted/);
  });

  it('allows agent() while a budget is set but not yet exhausted', async () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'ok', budgetTotal: 100 });
    const ctx = runtime.createContext({});
    const r = await (ctx.agent as (p: string) => Promise<unknown>)('p');
    assert.equal(r, 'ok');
    assert.equal(ctx.budget.remaining(), 100);
  });

  it('enforces the hard agent-count ceiling', async () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x', maxConcurrent: 64 });
    const ctx = runtime.createContext({});
    const agent = ctx.agent as (p: string) => Promise<unknown>;
    for (let i = 0; i < 1000; i++) await agent('p');
    await assert.rejects(() => agent('p'), /Agent limit reached/);
  });

  it('emits agent-error and resolves null when the handler throws an Error', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => { throw new Error('handler-boom'); },
    });
    const errors: Array<{ error: string }> = [];
    runtime.on('agent-error', (e) => errors.push(e));
    const ctx = runtime.createContext({});
    const r = await (ctx.agent as (p: string) => Promise<unknown>)('p');
    assert.equal(r, null);
    assert.equal(errors[0]!.error, 'handler-boom');
  });

  it('stringifies a non-Error thrown value in agent-error', async () => {
    const runtime = new WorkflowRuntime({
      // eslint-disable-next-line @typescript-eslint/no-throw-literal
      agentHandler: async () => { throw 'plain-string-failure'; },
    });
    const errors: Array<{ error: string }> = [];
    runtime.on('agent-error', (e) => errors.push(e));
    const ctx = runtime.createContext({});
    await (ctx.agent as (p: string) => Promise<unknown>)('p');
    assert.equal(errors[0]!.error, 'plain-string-failure');
  });

  it('includes opts.label in the agent-error event when a labelled agent throws', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => { throw new Error('labelled-boom'); },
    });
    const errors: Array<{ label?: string; error: string }> = [];
    runtime.on('agent-error', (e) => errors.push(e));
    const ctx = runtime.createContext({});
    const agent = ctx.agent as (p: string, o?: { phase?: string; label?: string }) => Promise<unknown>;
    await agent('p', { phase: 'ph', label: 'LBL' });
    assert.equal(errors[0]!.label, 'LBL');
    assert.equal(errors[0]!.error, 'labelled-boom');
  });

  it('honours opts.phase/label and counts two agents in the same phase', async () => {
    const started: Array<{ phase?: string; label?: string; agentNumber: number }> = [];
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'ok' });
    runtime.on('agent-start', (e) => started.push(e));
    const ctx = runtime.createContext({});
    const agent = ctx.agent as (p: string, o?: { phase?: string; label?: string }) => Promise<unknown>;
    await agent('one', { phase: 'review', label: 'L1' });
    await agent('two', { phase: 'review', label: 'L2' });
    assert.equal(started.length, 2);
    assert.equal(started[0]!.phase, 'review');
    assert.equal(started[0]!.label, 'L1');
    assert.equal(runtime.agentCount, 2);
  });
});

describe('WorkflowRuntime — parallel/pipeline limits + checkpoints', () => {
  it('parallel() rejects more than the item cap', async () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = runtime.createContext({});
    const thunks = Array.from({ length: 4097 }, () => async () => 1);
    await assert.rejects(() => ctx.parallel(thunks), /parallel\(\) limit exceeded/);
  });

  it('parallel() swallows a throwing thunk to null on the non-staggered path', async () => {
    const prev = process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS;
    process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS = '0';
    try {
      const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
      const ctx = runtime.createContext({});
      const res = await ctx.parallel([
        async () => 'a',
        async () => { throw new Error('boom'); },
      ]);
      assert.deepEqual(res, ['a', null]);
    } finally {
      if (prev === undefined) delete process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS;
      else process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS = prev;
    }
  });

  it('pipeline() rejects more than the item cap', async () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = runtime.createContext({});
    const items = Array.from({ length: 4097 }, (_v, i) => i);
    await assert.rejects(
      () => ctx.pipeline(items, async (p) => p),
      /pipeline\(\) limit exceeded/,
    );
  });

  it('save/load checkpoint round-trips and misses return undefined', () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = runtime.createContext({});
    ctx.saveCheckpoint('k', { v: 1 });
    assert.deepEqual(ctx.loadCheckpoint('k'), { v: 1 });
    assert.equal(ctx.loadCheckpoint('missing'), undefined);
  });

  it('phase() updates currentPhase and emits', () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const seen: string[] = [];
    runtime.on('phase', (t) => seen.push(t));
    const ctx = runtime.createContext({});
    ctx.phase('build');
    assert.equal(runtime.currentPhase, 'build');
    assert.deepEqual(seen, ['build']);
  });

  it('budget.remaining is Infinity when no total is set', () => {
    const runtime = new WorkflowRuntime({ agentHandler: async () => 'x' });
    const ctx = runtime.createContext({});
    assert.equal(ctx.budget.remaining(), Infinity);
    assert.equal(ctx.budget.total, null);
  });
});

describe('executeWorkflow — stagger + abort integration', () => {
  it('runs staggered parallel thunks and tolerates failures', async () => {
    const prev = process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS;
    process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS = '1';
    try {
      const script = `
export const meta = { name: 'p', description: 'd' }
const res = await parallel([
  async () => 'a',
  async () => { throw new Error('nope') },
  async () => 'c',
])
return res
`;
      const { result } = await executeWorkflow(script, async () => 'ok');
      assert.deepEqual(result, ['a', null, 'c']);
    } finally {
      if (prev === undefined) delete process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS;
      else process.env.AMI_WORKFLOW_PREFIX_STAGGER_MS = prev;
    }
  });

  it('coerces a missing top-level return to null', async () => {
    const script = `
export const meta = { name: 'p', description: 'd' }
log('no-return-here')
`;
    const { result, logs } = await executeWorkflow(script, async () => 'ok');
    assert.equal(result, null);
    assert.ok(logs.includes('no-return-here'));
  });

  it('wires an abort signal and the listener fires runtime.abort()', async () => {
    const ac = new AbortController();
    const script = `
export const meta = { name: 'p', description: 'd' }
const r = await agent('x')
return r
`;
    const runner = executeWorkflow(script, async () => 'ok', { abortSignal: ac.signal });
    const { result } = await runner;
    assert.equal(result, 'ok');
    // Firing the signal afterwards invokes the registered abort listener.
    assert.doesNotThrow(() => ac.abort());
  });
});
