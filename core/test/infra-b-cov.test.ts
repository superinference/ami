import { describe, it, before, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as http from 'http';
import * as child_process from 'child_process';

import { generateTitle } from '../src/title-generator';
import { SubagentLimiter } from '../src/subagent-limiter';
import {
  SettingsGuard,
  canOverride,
  getEffectiveTier,
  filterManagedKeys,
  type SettingsSource,
} from '../src/settings-guard';
import {
  ProcessManager,
  isProcessAlive,
  forceKillAfterTimeout,
} from '../src/process-manager';
import { HookManager, EventConcurrencyLimiter } from '../src/hooks';
import type { ProviderConfig } from '../src/types';

// ===========================================================================
// infra-b group coverage: title-generator, settings-guard, subagent-limiter,
// process-manager, hooks. Behaviour-only, ESM-safe (no source-text reads, no
// namespace monkeypatching). Real temp dirs, real child processes, a real mock
// HTTP server, node:test mock.timers, and the title-generator DI seam.
// ===========================================================================

const PROVIDER: ProviderConfig = { model: 'test-model', apiKey: 'k' };

function makeTmpDir(prefix = 'infra-b-'): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

// ---------------------------------------------------------------------------
// title-generator — via the injected streamer seam
// ---------------------------------------------------------------------------

describe('generateTitle (DI seam)', () => {
  it('accumulates content_delta then breaks on a done chunk', async () => {
    const streamer = (async function* () {
      yield { type: 'content_delta', text: 'Hello ' } as any;
      yield { type: 'content_delta', text: 'World' } as any;
      yield { type: 'done' } as any;
      yield { type: 'content_delta', text: 'AFTER-BREAK' } as any; // must be ignored
    });
    const title = await generateTitle('x', PROVIDER, undefined, streamer as any);
    assert.equal(title, 'Hello World');
    assert.ok(!title.includes('AFTER-BREAK'), 'iteration stops at the done chunk');
  });

  it('breaks on an error chunk (error branch of the done||error test)', async () => {
    const streamer = (async function* () {
      yield { type: 'content_delta', text: 'Partial' } as any;
      yield { type: 'error', error: 'boom' } as any;
      yield { type: 'content_delta', text: 'NEVER' } as any;
    });
    const title = await generateTitle('fallback text', PROVIDER, undefined, streamer as any);
    assert.equal(title, 'Partial');
    assert.ok(!title.includes('NEVER'));
  });

  it('falls back to the user message when the stream yields no content', async () => {
    const streamer = (async function* () {
      yield { type: 'done' } as any;
    });
    const title = await generateTitle('the original user message', PROVIDER, undefined, streamer as any);
    assert.equal(title, 'the original user message');
  });

  it('uses the provider model when no compaction model is given', async () => {
    let seenModel: string | undefined;
    const streamer = (provider: ProviderConfig) => {
      seenModel = provider.model;
      return (async function* () { yield { type: 'content_delta', text: 'T' } as any; yield { type: 'done' } as any; })();
    };
    await generateTitle('x', { model: 'primary', apiKey: 'k' }, undefined, streamer as any);
    assert.equal(seenModel, 'primary');
  });

  it('uses the compaction model override when provided', async () => {
    let seenModel: string | undefined;
    const streamer = (provider: ProviderConfig) => {
      seenModel = provider.model;
      return (async function* () { yield { type: 'content_delta', text: 'T' } as any; yield { type: 'done' } as any; })();
    };
    await generateTitle('x', { model: 'primary', apiKey: 'k' }, 'compact-model', streamer as any);
    assert.equal(seenModel, 'compact-model');
  });

  it('strips unclosed <think> tags during cleanup', async () => {
    const streamer = (async function* () {
      yield { type: 'content_delta', text: 'Clean<think>dangling reasoning' } as any;
      yield { type: 'done' } as any;
    });
    const title = await generateTitle('x', PROVIDER, undefined, streamer as any);
    assert.equal(title, 'Clean');
  });

  it('swallows a content_delta chunk with no text (the && chunk.text guard)', async () => {
    const streamer = (async function* () {
      yield { type: 'content_delta' } as any; // no text -> accumulation skipped
      yield { type: 'content_delta', text: 'Kept' } as any;
      yield { type: 'done' } as any;
    });
    const title = await generateTitle('x', PROVIDER, undefined, streamer as any);
    assert.equal(title, 'Kept');
  });

  it('falls back to the user message when the streamer throws (outer catch)', async () => {
    const streamer = (async function* () {
      throw new Error('stream exploded');
    });
    const title = await generateTitle('user message survives', PROVIDER, undefined, streamer as any);
    assert.equal(title, 'user message survives');
  });

  it('aborts via the 30s timeout callback and falls back to the user message', async () => {
    mock.timers.enable({ apis: ['setTimeout'] });
    try {
      const streamer = (_p: ProviderConfig, _m: unknown, _t: unknown, signal: AbortSignal) =>
        (async function* () {
          await new Promise<void>((resolve) => { signal.addEventListener('abort', () => resolve()); });
        })();
      const p = generateTitle('abort me please', PROVIDER, undefined, streamer as any);
      await Promise.resolve();
      mock.timers.tick(30000); // fires setTimeout(() => ac.abort())
      const title = await p;
      assert.equal(title, 'abort me please');
    } finally {
      mock.timers.reset();
    }
  });
});

// ---------------------------------------------------------------------------
// subagent-limiter — queued resolve + queued reject (lifetime exhausted)
// ---------------------------------------------------------------------------

describe('SubagentLimiter queued paths', () => {
  it('resolves a queued waiter on release and rejects when lifetime is exhausted mid-queue', async () => {
    const lim = new SubagentLimiter(1, 0, 2); // maxConcurrent=1, maxLifetime=2
    await lim.acquire();                       // running=1, total=1
    const q1 = lim.acquire();                  // queued (total stays 1)
    const q2 = lim.acquire();                  // queued (total stays 1)
    assert.equal(lim.queuedCount, 2);

    lim.release();                             // resolves q1: total 1<2 -> total=2
    await assert.doesNotReject(q1);
    assert.equal(lim.lifetimeCount, 2);

    lim.release();                             // resolves q2: total 2>=2 -> reject
    await assert.rejects(q2, /Session subagent limit reached \(2\)/);
  });

  it('release with an empty queue decrements the running count (floored at 0)', () => {
    const lim = new SubagentLimiter(5, 0, 100);
    lim.release();
    assert.equal(lim.activeCount, 0);
  });
});

// ---------------------------------------------------------------------------
// settings-guard — exhaustive branch coverage
// ---------------------------------------------------------------------------

describe('settings-guard canOverride', () => {
  it('blocks managed-only keys from non-managed sources, allows from managed', () => {
    assert.equal(canOverride('allowedMcpServers', 'project', 'project'), false);
    assert.equal(canOverride('allowedMcpServers', 'managed', 'global'), true);
  });

  it('blocks tier-locked keys from lower-priority sources, allows equal/higher priority', () => {
    // cli (5) overriding a value set at managed (0): higher priority number -> blocked
    assert.equal(canOverride('permissionMode', 'cli', 'managed'), false);
    // managed (0) overriding cli (5): lower priority number -> allowed
    assert.equal(canOverride('permissionMode', 'managed', 'cli'), true);
    // equal priority -> allowed (not strictly greater)
    assert.equal(canOverride('permissionMode', 'project', 'project'), true);
  });

  it('allows ordinary keys unconditionally', () => {
    assert.equal(canOverride('someRandomKey', 'cli', 'managed'), true);
  });
});

describe('settings-guard getEffectiveTier', () => {
  it('returns null for no sources', () => {
    assert.equal(getEffectiveTier('anyKey', []), null);
  });

  it('managed-only key: returns managed when present, null otherwise', () => {
    const withManaged: SettingsSource[] = [{ tier: 'project' }, { tier: 'managed' }];
    assert.equal(getEffectiveTier('managedMcpServers', withManaged), 'managed');
    const noManaged: SettingsSource[] = [{ tier: 'project' }, { tier: 'local' }];
    assert.equal(getEffectiveTier('managedMcpServers', noManaged), null);
  });

  it('tier-locked key: picks the lowest-priority (highest-precedence) tier regardless of order', () => {
    // second source lower priority -> updates best (the `<` true branch)
    assert.equal(getEffectiveTier('maxTurns', [{ tier: 'cli' }, { tier: 'managed' }]), 'managed');
    // second source higher priority -> keeps best (the `<` false branch)
    assert.equal(getEffectiveTier('maxTurns', [{ tier: 'managed' }, { tier: 'cli' }]), 'managed');
  });

  it('ordinary key: picks the highest-priority (last-wins) tier regardless of order', () => {
    // second source higher priority -> updates best (the `>` true branch)
    assert.equal(getEffectiveTier('foo', [{ tier: 'managed' }, { tier: 'cli' }]), 'cli');
    // second source lower priority -> keeps best (the `>` false branch)
    assert.equal(getEffectiveTier('foo', [{ tier: 'cli' }, { tier: 'managed' }]), 'cli');
  });
});

describe('settings-guard filterManagedKeys', () => {
  it('rejects managed-only keys from a non-managed tier, keeps the rest', () => {
    const { filtered, rejected } = filterManagedKeys(
      { allowedMcpServers: ['a'], theme: 'dark' },
      'project',
    );
    assert.deepEqual(rejected, ['allowedMcpServers']);
    assert.deepEqual(filtered, { theme: 'dark' });
  });

  it('keeps managed-only keys when the source tier is managed', () => {
    const { filtered, rejected } = filterManagedKeys(
      { allowedMcpServers: ['a'], theme: 'dark' },
      'managed',
    );
    assert.deepEqual(rejected, []);
    assert.deepEqual(filtered, { allowedMcpServers: ['a'], theme: 'dark' });
  });
});

describe('SettingsGuard class', () => {
  it('records both managed-only and tier-locked block reasons, and clears them', () => {
    const g = new SettingsGuard();
    assert.equal(g.checkOverride('theme', 'cli', 'cli'), true); // allowed, nothing recorded
    assert.equal(g.getBlockedCount(), 0);

    assert.equal(g.checkOverride('allowedMcpServers', 'project', 'project'), false);
    assert.equal(g.checkOverride('permissionMode', 'cli', 'managed'), false);
    const blocks = g.getBlockedOverrides();
    assert.equal(blocks.length, 2);
    assert.match(blocks[0].reason, /can only be set by managed config/);
    assert.match(blocks[1].reason, /is locked at tier "managed"/);
    assert.equal(g.getBlockedCount(), 2);

    g.clear();
    assert.equal(g.getBlockedCount(), 0);
  });
});

// ---------------------------------------------------------------------------
// process-manager — time/signal paths not reachable from happy-path tests
// ---------------------------------------------------------------------------

function waitForComplete(pm: ProcessManager, taskId: string, timeoutMs = 8000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting ${taskId}`)), timeoutMs);
    pm.on('complete', function handler(ev: any) {
      if (ev.taskId === taskId) { clearTimeout(timer); pm.off('complete', handler); resolve(ev); }
    });
  });
}

describe('process-manager isProcessAlive / forceKillAfterTimeout', () => {
  it('isProcessAlive is true for self, false for a non-existent pid', () => {
    assert.equal(isProcessAlive(process.pid), true);
    assert.equal(isProcessAlive(2147483646), false);
  });

  it('forceKillAfterTimeout returns false immediately for a dead pid', async () => {
    assert.equal(await forceKillAfterTimeout(2147483646, 50), false);
  });

  it('escalates to SIGKILL when the process ignores SIGTERM', async () => {
    const child = child_process.spawn(
      process.execPath,
      ['-e', 'process.on("SIGTERM", () => {}); setInterval(() => {}, 1000);'],
      { stdio: 'ignore' },
    );
    await new Promise(r => setTimeout(r, 150)); // let the SIGTERM handler install
    const result = await forceKillAfterTimeout(child.pid!, 150);
    assert.equal(result, true);
    await new Promise(r => setTimeout(r, 100));
    assert.equal(isProcessAlive(child.pid!), false, 'SIGKILL must have terminated it');
  });

  it('returns false when SIGTERM itself throws (EPERM on a non-owned pid)', async () => {
    const uid = typeof process.getuid === 'function' ? process.getuid() : 1;
    if (uid === 0) return; // root can signal pid 1; skip (noted in report)
    // pid 1 is alive (process.kill(1,0) -> EPERM, non-ESRCH -> true) but we cannot
    // signal it (EPERM) -> SIGTERM throws -> the catch returns false.
    assert.equal(isProcessAlive(1), true);
    assert.equal(await forceKillAfterTimeout(1, 50), false);
  });
});

describe('process-manager ProcessManager signal/timer paths', () => {
  const dirs: string[] = [];
  const pms: ProcessManager[] = [];
  after(async () => {
    for (const pm of pms) { try { pm.cleanup(); } catch { /* ignore */ } }
    await new Promise(r => setTimeout(r, 100));
    for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
  });
  function mk(): ProcessManager {
    const d = makeTmpDir('infra-b-pm-');
    dirs.push(d);
    const pm = new ProcessManager(d);
    pms.push(pm);
    return pm;
  }

  it('emits "error" for non-EBADF stream errors and swallows EBADF', async () => {
    const pm = mk();
    const errors: NodeJS.ErrnoException[] = [];
    pm.on('error', (e: NodeJS.ErrnoException) => errors.push(e));
    const id = pm.spawn('sleep 5', { cwd: (pm as any).tasksDir });
    const proc = (pm as any).processes.get(id).proc;

    const ebadf = Object.assign(new Error('bad fd'), { code: 'EBADF' });
    proc.stdout.emit('error', ebadf); // swallowed — no emit
    assert.equal(errors.length, 0);

    const eio = Object.assign(new Error('io error'), { code: 'EIO' });
    proc.stderr.emit('error', eio); // re-emitted on the manager
    assert.equal(errors.length, 1);
    assert.equal(errors[0].code, 'EIO');

    pm.kill(id);
    // Reap fully before the next test spawns: a late close handler on a killed
    // detached process closes its (possibly reused) fd number, which would
    // otherwise alias the next task's output fd.
    await waitForComplete(pm, id);
  });

  it('kills a background process when its timeout fires', async () => {
    const pm = mk();
    const id = pm.spawn('sleep 30', { cwd: (pm as any).tasksDir, timeoutMs: 300 });
    await waitForComplete(pm, id);
    const task = pm.get(id)!;
    assert.equal(task.status, 'killed');
    const out = pm.getOutput(id, 0)!;
    assert.match(out.output, /exceeded .*time limit/);
  });

  it('kills a stalled process that emits an interactive prompt (stall interval)', async () => {
    mock.timers.enable({ apis: ['setInterval'] });
    let pm: ProcessManager;
    let id: string;
    try {
      pm = mk();
      id = pm.spawn("printf 'Continue? (y/n) '; sleep 30", { cwd: (pm as any).tasksDir });
      // real setTimeout still works (only setInterval is mocked); let output land
      await new Promise(r => setTimeout(r, 400));
      mock.timers.tick(45000); // first tick: output changed since 0 -> records size
      mock.timers.tick(45000); // second tick: size stable + prompt-like -> kill
    } finally {
      mock.timers.reset();
    }
    assert.equal(pm!.get(id!)!.status, 'killed');
    await waitForComplete(pm!, id!);
  });

  it('swallows a write error when the output fd is closed mid-stream (writeChunk + close catch)', async () => {
    const pm = mk();
    const id = pm.spawn("sleep 0.2; echo late-output", { cwd: (pm as any).tasksDir });
    const entry = (pm as any).processes.get(id);
    // Close the backing fd out from under the writer: the delayed stdout chunk
    // makes writeChunk's fs.writeSync throw (caught), and the close handler's
    // fs.closeSync then also throws (caught) — both defensive catches.
    fs.closeSync(entry.outputFd);
    await waitForComplete(pm, id);
    assert.notEqual(pm.get(id)!.status, 'running');
  });

  it('swallows a timeout-write error on a closed fd and a double-close in kill', async () => {
    const pm = mk();
    const id = pm.spawn('sleep 30', { cwd: (pm as any).tasksDir, timeoutMs: 200 });
    const entry = (pm as any).processes.get(id);
    // fd closed: the timeout handler's fs.writeSync throws (caught), then kill()
    // closes the (already closed) entry.outputFd -> its fs.closeSync throws (caught).
    fs.closeSync(entry.outputFd);
    await waitForComplete(pm, id);
    assert.equal(pm.get(id)!.status, 'killed');
  });

  it('falls back to proc.kill when killing the process group throws', () => {
    const pm = mk();
    let fallbackCalled = false;
    (pm as any).processes.set('grp-fail', {
      taskId: 'grp-fail',
      pid: 2147483646,
      command: 'x',
      description: 'x',
      status: 'running',
      exitCode: null,
      outputPath: path.join((pm as any).tasksDir, 'grp-fail.output'),
      startTime: Date.now(),
      proc: { pid: 2147483646, kill: () => { fallbackCalled = true; }, stdout: null, stderr: null, stdin: null },
      outputFd: null,
      bytesWritten: 0,
    });
    const killed = pm.kill('grp-fail');
    assert.equal(killed, true);
    assert.equal(fallbackCalled, true, 'process.kill(-pid) throws ESRCH -> proc.kill fallback runs');
    assert.equal(pm.get('grp-fail')!.status, 'killed');
  });
});

// ---------------------------------------------------------------------------
// hooks — remaining branch/function gaps
// ---------------------------------------------------------------------------

function writeHooks(entries: any[]): string {
  const tmpDir = makeTmpDir('infra-b-hooks-');
  const dir = path.join(tmpDir, '.superinference');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

describe('hooks — EventConcurrencyLimiter', () => {
  it('release without a prior acquire is a safe no-op (the ?? default arm)', () => {
    const lim = new EventConcurrencyLimiter(2);
    assert.equal(lim.getActive('e'), 0);
    assert.doesNotThrow(() => lim.release('e')); // active.get('e') undefined -> ?? 1 path
    assert.equal(lim.getActive('e'), 0);
  });

  it('queues waiters beyond the limit and releases them in order', async () => {
    const lim = new EventConcurrencyLimiter(1);
    await lim.acquire('e');            // active=1
    assert.equal(lim.getActive('e'), 1);
    let resumed = false;
    const waiter = lim.acquire('e').then(() => { resumed = true; }); // queued
    assert.equal(lim.getWaiting('e'), 1);
    lim.release('e');                  // hands off to the queued waiter
    await waiter;
    assert.equal(resumed, true);
    assert.equal(lim.getWaiting('e'), 0);
    lim.release('e');                  // drains back to 0
    assert.equal(lim.getActive('e'), 0);
  });
});

describe('hooks — executeCallbacks', () => {
  it('swallows a throwing callback and returns the first object result', async () => {
    const hm = new HookManager();
    hm.registerCallback('evt', async () => { throw new Error('cb boom'); });
    hm.registerCallback('evt', async () => ({ decision: 'approve' as const }));
    const result = await hm.executeCallbacks('evt', {});
    assert.deepEqual(result, { decision: 'approve' });
  });

  it('returns undefined when there are no callbacks for the event', async () => {
    const hm = new HookManager();
    assert.equal(await hm.executeCallbacks('nope', {}), undefined);
  });

  it('ignores non-object and void callback results, returning undefined', async () => {
    const hm = new HookManager();
    hm.registerCallback('evt', async () => 'a string result' as any); // truthy, not an object
    const unregister = hm.registerCallback('evt', async () => undefined); // void
    const result = await hm.executeCallbacks('evt', {});
    assert.equal(result, undefined);
    // the returned unregister handle removes just that callback
    unregister();
    assert.equal(await hm.executeCallbacks('evt', {}), undefined);
  });
});

describe('hooks — processHookEntry protected-event block', () => {
  it('blocks a user-tier hook registered on a protected event', () => {
    const hm = new HookManager();
    const before = hm.getSummary();
    // _source omitted -> tier defaults to 'user'; 'session.compact' is protected.
    (hm as any).processHookEntry(
      { event: 'session.compact', hook: { type: 'command', command: 'echo blocked' } },
      process.cwd(),
    );
    assert.deepEqual(hm.getSummary(), before, 'no hook registered when blocked');
  });
});

describe('hooks — execute* catch blocks (throwing hooks are logged, not rethrown)', () => {
  it('each execute method swallows a throwing hook', async () => {
    const hm = new HookManager();
    const boom = () => { throw new Error('hook boom'); };

    hm.onPostSampling(boom as any);
    hm.onStop(boom as any);
    hm.onError(boom as any);
    hm.onPostToolUse(boom as any);
    hm.onSessionStart(boom as any);
    hm.onSessionEnd(boom as any);
    hm.onSubagentStart(boom as any);
    hm.onSubagentStop(boom as any);
    hm.onPreCompact(boom as any);
    hm.onPostCompact(boom as any);
    hm.onPermissionRequest(boom as any);
    hm.onPermissionDenied(boom as any);
    hm.onUserPromptSubmit(boom as any);
    hm.onTaskCreated(boom as any);
    hm.onTaskCompleted(boom as any);
    hm.onPostToolUseFailure(boom as any);
    hm.onNotification(boom as any);
    hm.onSetup(boom as any);
    hm.onConfigChange(boom as any);
    hm.onInstructionsLoaded(boom as any);
    hm.onWorktreeCreate(boom as any);
    hm.onWorktreeRemove(boom as any);
    hm.onCwdChanged(boom as any);
    hm.onFileChanged(boom as any);
    hm.onElicitation(boom as any);
    hm.onElicitationResult(boom as any);
    hm.onStopFailure(boom as any);

    const base = { messages: [], turnCount: 1 };
    await assert.doesNotReject(hm.executePostSampling(base as any));
    await assert.doesNotReject(hm.executeStop(base as any)); // also drives executeStopFailure
    await assert.doesNotReject(hm.executeError(base as any));
    await assert.doesNotReject(hm.executePostToolUse({ ...base, toolName: 'bash', toolInput: {}, toolOutput: '', isError: false } as any));
    await assert.doesNotReject(hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any));
    await assert.doesNotReject(hm.executeSessionEnd({ sessionId: 's', cwd: '.' } as any));
    await assert.doesNotReject(hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any));
    await assert.doesNotReject(hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any));
    await assert.doesNotReject(hm.executePreCompact({ messageCount: 1, tokenEstimate: 1 } as any));
    await assert.doesNotReject(hm.executePostCompact({ messageCount: 1, tokenEstimate: 1 } as any));
    await assert.doesNotReject(hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any));
    await assert.doesNotReject(hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as any));
    await assert.doesNotReject(hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 1 } as any));
    await assert.doesNotReject(hm.executeTaskCreated({ taskId: 't', subject: 's' } as any));
    await assert.doesNotReject(hm.executeTaskCompleted({ taskId: 't', subject: 's' } as any));
    await assert.doesNotReject(hm.executePostToolUseFailure({ ...base, toolName: 'bash', toolInput: {}, toolOutput: '', isError: true } as any));
    await assert.doesNotReject(hm.executeNotification({ message: 'm' } as any));
    await assert.doesNotReject(hm.executeSetup({ sessionId: 's', cwd: '.' } as any));
    await assert.doesNotReject(hm.executeConfigChange({ source: 'x' } as any));
    await assert.doesNotReject(hm.executeInstructionsLoaded({ filePath: 'f', loadReason: 'r' } as any));
    await assert.doesNotReject(hm.executeWorktreeCreate({ name: 'w' } as any));
    await assert.doesNotReject(hm.executeWorktreeRemove({ name: 'w' } as any));
    await assert.doesNotReject(hm.executeCwdChanged({ oldCwd: 'a', newCwd: 'b' } as any));
    await assert.doesNotReject(hm.executeFileChanged({ filePath: 'f', event: 'change' } as any));
    await assert.doesNotReject(hm.executeElicitation({ mcpServerName: 'm' } as any));
    await assert.doesNotReject(hm.executeElicitationResult({ mcpServerName: 'm' } as any));
    await assert.doesNotReject(hm.executeStopFailure({ error: 'e', hookIndex: 0 } as any));
  });
});

describe('hooks — command/http closures produce structured results', () => {
  it('postToolUse command hook returning updatedToolOutput JSON is surfaced', async () => {
    const tmpDir = writeHooks([
      { event: 'postToolUse', hook: { type: 'command', command: `echo '{"updatedToolOutput":"rewritten"}'` } },
    ]);
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(tmpDir);
      const res = await hm.executePostToolUse({
        messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, toolOutput: 'orig', isError: false,
      } as any);
      assert.deepEqual(res, { additionalContext: undefined, updatedToolOutput: 'rewritten' });
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('sessionStart http hook returning systemMessage JSON becomes initialUserMessage', async () => {
    const server = http.createServer((_req, res) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ systemMessage: 'from-http' }));
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    const port = (server.address() as any).port;
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(writeHooks([
        { event: 'sessionStart', hook: { type: 'http', url: `http://127.0.0.1:${port}/` } },
      ]));
      const res = await hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any);
      assert.deepEqual(res, { initialUserMessage: 'from-http' });
    } finally {
      server.close();
    }
  });

  it('http hook with onFailure:block denies when the request times out', async () => {
    // Server accepts the socket but never responds -> client timeout fires.
    const server = http.createServer(() => { /* never ends the response */ });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    const port = (server.address() as any).port;
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(writeHooks([
        { event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/`, timeout: 120, onFailure: 'block' } },
      ]));
      const decision = await hm.executePreToolUse({
        messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'ls' },
      } as any);
      assert.equal(decision.action, 'deny');
      assert.match((decision as any).reason, /HTTP hook failed/);
    } finally {
      server.close();
    }
  });
});

describe('hooks — async hooks whose runner rejects hit the fire-and-forget catch', () => {
  const EVENTS = [
    'preToolUse', 'postToolUse', 'preSampling', 'postSampling', 'stop', 'error',
    'sessionStart', 'sessionEnd', 'subagentStart', 'subagentStop', 'preCompact',
    'postCompact', 'permissionRequest', 'permissionDenied', 'userPromptSubmit',
    'taskCreated', 'taskCompleted', 'postToolUseFailure', 'notification', 'setup',
    'configChange', 'instructionsLoaded', 'worktreeCreate', 'worktreeRemove',
    'cwdChanged', 'fileChanged', 'elicitation', 'elicitationResult', 'stopFailure',
  ];

  it('every per-event async branch swallows a rejecting runner', async () => {
    // type:'http' with an invalid URL => new URL() throws => runHookByType rejects
    // => the async `runner().catch(() => {})` handler runs for each event.
    const tmpDir = writeHooks(EVENTS.map(event => ({
      event,
      hook: { type: 'http', url: 'not-a-valid-url', async: true },
    })));
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(tmpDir);

      const base = { messages: [], turnCount: 1 };
      await hm.executePreToolUse({ ...base, toolName: 'bash', toolInput: {} } as any);
      await hm.executePostToolUse({ ...base, toolName: 'bash', toolInput: {}, toolOutput: 'o', isError: false } as any);
      await hm.executePreSampling({ ...base, apiMessages: [] } as any);
      await hm.executePostSampling(base as any);
      await hm.executeStop(base as any);
      await hm.executeError(base as any);
      await hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any);
      await hm.executeSessionEnd({ sessionId: 's', cwd: '.' } as any);
      await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any);
      await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any);
      await hm.executePreCompact({ messageCount: 1, tokenEstimate: 1 } as any);
      await hm.executePostCompact({ messageCount: 1, tokenEstimate: 1 } as any);
      await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
      await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as any);
      await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 1 } as any);
      await hm.executeTaskCreated({ taskId: 't', subject: 's' } as any);
      await hm.executeTaskCompleted({ taskId: 't', subject: 's' } as any);
      await hm.executePostToolUseFailure({ ...base, toolName: 'bash', toolInput: {}, toolOutput: '', isError: true } as any);
      await hm.executeNotification({ message: 'm' } as any);
      await hm.executeSetup({ sessionId: 's', cwd: '.' } as any);
      await hm.executeConfigChange({ source: 'x' } as any);
      await hm.executeInstructionsLoaded({ filePath: 'f', loadReason: 'r' } as any);
      await hm.executeWorktreeCreate({ name: 'w' } as any);
      await hm.executeWorktreeRemove({ name: 'w' } as any);
      await hm.executeCwdChanged({ oldCwd: 'a', newCwd: 'b' } as any);
      await hm.executeFileChanged({ filePath: 'f', event: 'change' } as any);
      await hm.executeElicitation({ mcpServerName: 'm' } as any);
      await hm.executeElicitationResult({ mcpServerName: 'm' } as any);
      await hm.executeStopFailure({ error: 'e', hookIndex: 0 } as any);

      // Let the detached rejections settle through their .catch handlers.
      for (let i = 0; i < 5; i++) await new Promise(r => setImmediate(r));
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('hooks — http hook whose response stream errors', () => {
  it('resolves (no throw) when the response connection is reset mid-body', async () => {
    const server = http.createServer((_req, res) => {
      res.writeHead(200, { 'Content-Length': '1000' });
      res.write('partial-body');
      // Reset the socket before the declared body length arrives -> response 'error'.
      res.socket!.destroy();
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    const port = (server.address() as any).port;
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(writeHooks([
        { event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/` } },
      ]));
      const decision = await hm.executePreToolUse({
        messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'ls' },
      } as any);
      // Non-block hook => onFail() returns '' => allow.
      assert.equal(decision.action, 'allow');
    } finally {
      server.close();
    }
  });
});

describe('hooks — once:true hooks are removed after first run', () => {
  const EVENTS = [
    'preToolUse', 'postToolUse', 'preSampling', 'postSampling', 'stop', 'error',
    'sessionStart', 'sessionEnd', 'subagentStart', 'subagentStop', 'preCompact',
    'postCompact', 'permissionRequest', 'permissionDenied', 'userPromptSubmit',
    'taskCreated', 'taskCompleted', 'postToolUseFailure', 'notification', 'setup',
    'configChange', 'instructionsLoaded', 'worktreeCreate', 'worktreeRemove',
    'cwdChanged', 'fileChanged', 'elicitation', 'elicitationResult', 'stopFailure',
  ];

  it('removes each per-event once hook after the first execute', async () => {
    const tmpDir = writeHooks(EVENTS.map(event => ({
      event, hook: { type: 'command', command: 'echo ok', once: true },
    })));
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      // Provider config set -> buildHookEnv reads model/provider (the non-empty arms).
      hm.setProviderConfig({ model: 'mdl', provider: 'openai' });
      hm.loadFromFile(tmpDir);

      const base = { messages: [], turnCount: 1 };
      await hm.executePreToolUse({ ...base, toolName: 'bash', toolInput: {} } as any);
      await hm.executePostToolUse({ ...base, toolName: 'bash', toolInput: {}, toolOutput: 'o', isError: false } as any);
      await hm.executePreSampling({ ...base, apiMessages: [] } as any);
      await hm.executePostSampling(base as any);
      await hm.executeStop(base as any);
      await hm.executeError(base as any);
      await hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any);
      await hm.executeSessionEnd({ sessionId: 's', cwd: '.' } as any);
      await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any);
      await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any);
      await hm.executePreCompact({ messageCount: 1, tokenEstimate: 1 } as any);
      await hm.executePostCompact({ messageCount: 1, tokenEstimate: 1 } as any);
      await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
      await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as any);
      await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 1 } as any);
      await hm.executeTaskCreated({ taskId: 't', subject: 's' } as any);
      await hm.executeTaskCompleted({ taskId: 't', subject: 's' } as any);
      await hm.executePostToolUseFailure({ ...base, toolName: 'bash', toolInput: {}, toolOutput: '', isError: true } as any);
      await hm.executeNotification({ message: 'm' } as any);
      await hm.executeSetup({ sessionId: 's', cwd: '.' } as any);
      await hm.executeConfigChange({ source: 'x' } as any);
      await hm.executeInstructionsLoaded({ filePath: 'f', loadReason: 'r' } as any);
      await hm.executeWorktreeCreate({ name: 'w' } as any);
      await hm.executeWorktreeRemove({ name: 'w' } as any);
      await hm.executeCwdChanged({ oldCwd: 'a', newCwd: 'b' } as any);
      await hm.executeFileChanged({ filePath: 'f', event: 'change' } as any);
      await hm.executeElicitation({ mcpServerName: 'm' } as any);
      await hm.executeElicitationResult({ mcpServerName: 'm' } as any);
      await hm.executeStopFailure({ error: 'e', hookIndex: 0 } as any);

      // Every once hook removed itself after running.
      const summary = hm.getSummary();
      for (const event of EVENTS) {
        assert.equal(summary[event], 0, `${event} once hook removed`);
      }
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});

describe('hooks — command/http decision return shapes', () => {
  let server: http.Server;
  let port: number;
  before(async () => {
    server = http.createServer((req, res) => {
      const routes: Record<string, unknown> = {
        '/approve': { decision: 'approve' },
        '/deny': { permissionDecision: 'deny', reason: 'blocked by policy' },
        '/decision-deny': { decision: 'deny' },
        '/decision-block': { decision: 'block' },
        '/initial': { initialUserMessage: 'start here' },
        '/ctx': { additionalContext: 'extra context' },
      };
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(routes[req.url ?? ''] ?? {}));
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as any).port;
  });
  after(() => server.close());

  function load(entries: any[]): HookManager {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(writeHooks(entries));
    return hm;
  }

  it('permissionRequest: plain ALLOW output allows', async () => {
    const hm = load([{ event: 'permissionRequest', hook: { type: 'command', command: 'echo ALLOW' } }]);
    const r = await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
    assert.deepEqual(r, { action: 'allow' });
  });

  it('permissionRequest: DENY: prefix denies with reason', async () => {
    const hm = load([{ event: 'permissionRequest', hook: { type: 'command', command: 'echo DENY:too risky' } }]);
    const r = await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
    assert.deepEqual(r, { action: 'deny', reason: 'too risky' });
  });

  it('permissionRequest: JSON decision approve allows (via http raw JSON)', async () => {
    const hm = load([{ event: 'permissionRequest', hook: { type: 'http', url: `http://127.0.0.1:${port}/approve` } }]);
    const r = await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
    assert.deepEqual(r, { action: 'allow' });
  });

  it('permissionRequest: JSON permissionDecision deny denies (via http raw JSON)', async () => {
    const hm = load([{ event: 'permissionRequest', hook: { type: 'http', url: `http://127.0.0.1:${port}/deny` } }]);
    const r = await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
    assert.deepEqual(r, { action: 'deny', reason: 'blocked by policy' });
  });

  it('permissionRequest: JSON decision deny / block both deny', async () => {
    const d1 = await load([{ event: 'permissionRequest', hook: { type: 'http', url: `http://127.0.0.1:${port}/decision-deny` } }])
      .executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
    assert.equal((d1 as any).action, 'deny');
    const d2 = await load([{ event: 'permissionRequest', hook: { type: 'http', url: `http://127.0.0.1:${port}/decision-block` } }])
      .executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
    assert.equal((d2 as any).action, 'deny');
  });

  it('sessionStart: JSON initialUserMessage (http) wins', async () => {
    const hm = load([{ event: 'sessionStart', hook: { type: 'http', url: `http://127.0.0.1:${port}/initial` } }]);
    const r = await hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any);
    assert.deepEqual(r, { initialUserMessage: 'start here' });
  });

  it('sessionStart: command SYSTEM: prefix becomes initialUserMessage', async () => {
    const hm = load([{ event: 'sessionStart', hook: { type: 'command', command: `echo '{"systemMessage":"sys hi"}'` } }]);
    const r = await hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any);
    assert.deepEqual(r, { initialUserMessage: 'sys hi' });
  });

  it('postToolUse: JSON additionalContext (http) is surfaced', async () => {
    const hm = load([{ event: 'postToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/ctx` } }]);
    const r = await hm.executePostToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, toolOutput: 'o', isError: false } as any);
    assert.deepEqual(r, { additionalContext: 'extra context', updatedToolOutput: undefined });
  });

  it('userPromptSubmit: JSON additionalContext (http) is surfaced', async () => {
    const hm = load([{ event: 'userPromptSubmit', hook: { type: 'http', url: `http://127.0.0.1:${port}/ctx` } }]);
    const r = await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 1 } as any);
    assert.deepEqual(r, { additionalContext: 'extra context' });
  });

  it('preToolUse command JSON: block without reason, ask, and updatedInput', async () => {
    const block = load([{ event: 'preToolUse', hook: { type: 'command', command: `echo '{"decision":"block"}'` } }]);
    assert.deepEqual(await block.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any), { action: 'deny', reason: 'Hook blocked' });

    const upd = load([{ event: 'preToolUse', hook: { type: 'command', command: `echo '{"updatedInput":{"command":"safe"}}'` } }]);
    const r = await upd.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'rm' } } as any);
    assert.equal(r.action, 'modify');
    assert.deepEqual((r as any).updatedInput, { command: 'safe' });

    // ask output -> runHookByType returns 'ASK:' which the preToolUse closure treats as allow
    const ask = load([{ event: 'preToolUse', hook: { type: 'command', command: `echo '{"decision":"ask"}'` } }]);
    assert.equal((await ask.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any)).action, 'allow');
  });
});

describe('hooks — permissionDenied retry + denyRulesHold hold-over', () => {
  it('permissionDenied hook returning action:allow signals retry; ctx.retry also retries', async () => {
    const hm = new HookManager();
    hm.onPermissionDenied(async () => ({ action: 'allow' }) as any);
    const r = await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as any);
    assert.deepEqual(r, { retry: true });

    const hm2 = new HookManager();
    const r2 = await hm2.executePermissionDenied({ toolName: 'bash', toolInput: {}, retry: true } as any);
    assert.deepEqual(r2, { retry: true });
  });

  it('fail-closed denial stringifies a non-Error throw', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => { throw 'plain string failure'; }); // non-Error throw
    const decision = await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    assert.equal(decision.action, 'deny');
    assert.match((decision as any).reason, /plain string failure/);
  });

  it('accumulates successive modify decisions, carrying earlier fields forward', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({ action: 'modify', updatedInput: { a: 1 }, additionalContext: 'first' }));
    // second modify omits updatedInput/additionalContext -> earlier values carry over
    hm.onPreToolUse(async () => ({ action: 'modify', updatedMessages: [] }));
    const decision = await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    assert.equal(decision.action, 'modify');
    assert.deepEqual((decision as any).updatedInput, { a: 1 });
    assert.equal((decision as any).additionalContext, 'first');
  });

  it('holds an org-tier verdict over a user-tier loosening when denyRulesHold is on', async () => {
    const hm = new HookManager();
    hm.setDenyRulesHold(true);
    // Two non-user (builtin) hooks: allow then modify (stricter) exercises the
    // verdictRank comparison; a final user allow would loosen it.
    hm.onPreToolUse(async () => ({ action: 'allow' }), 'builtin');
    hm.onPreToolUse(async () => ({ action: 'modify', updatedInput: {} }), 'builtin');
    hm.onPreToolUse(async () => ({ action: 'allow' }), 'user');
    const decision = await hm.executePreToolUse({ messages: [], turnCount: 1, toolName: 'bash', toolInput: {} } as any);
    assert.equal(decision.action, 'deny');
    assert.match((decision as any).reason, /held over/);
  });
});

describe('hooks — asyncRewake on a non-zero (exit 2) command hook', () => {
  it('emits hookRewake with the default message when output is empty', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(writeHooks([
      { event: 'stop', hook: { type: 'command', command: 'exit 2', asyncRewake: true } },
    ]));
    const rewake = new Promise<any>((resolve) => hm.once('hookRewake', resolve));
    await hm.executeStop({ messages: [], turnCount: 1 } as any);
    const ev = await rewake;
    assert.match(ev.message, /exit code 2/);
  });
});

describe('hooks — load every event type and drive each execute method', () => {
  it('each per-event closure runs via loadFromFile + execute', async () => {
    const events = [
      'preToolUse', 'postToolUse', 'preSampling', 'postSampling', 'stop', 'error',
      'sessionStart', 'sessionEnd', 'subagentStart', 'subagentStop', 'preCompact',
      'postCompact', 'permissionRequest', 'permissionDenied', 'userPromptSubmit',
      'taskCreated', 'taskCompleted', 'postToolUseFailure', 'notification', 'setup',
      'configChange', 'instructionsLoaded', 'worktreeCreate', 'worktreeRemove',
      'cwdChanged', 'fileChanged', 'elicitation', 'elicitationResult', 'stopFailure',
    ];
    const tmpDir = writeHooks(events.map(event => ({ event, hook: { type: 'command', command: 'echo ok' } })));
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(tmpDir);
      const summary = hm.getSummary();
      for (const event of events) {
        assert.equal(summary[event], 1, `${event} hook registered`);
      }

      const base = { messages: [], turnCount: 1 };
      await hm.executePreToolUse({ ...base, toolName: 'bash', toolInput: {} } as any);
      await hm.executePostToolUse({ ...base, toolName: 'bash', toolInput: {}, toolOutput: 'o', isError: false } as any);
      await hm.executePreSampling({ ...base, apiMessages: [] } as any);
      await hm.executePostSampling(base as any);
      await hm.executeStop(base as any);
      await hm.executeError(base as any);
      await hm.executeSessionStart({ sessionId: 's', cwd: '.' } as any);
      await hm.executeSessionEnd({ sessionId: 's', cwd: '.' } as any);
      await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any);
      await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'x' } as any);
      await hm.executePreCompact({ messageCount: 1, tokenEstimate: 1 } as any);
      await hm.executePostCompact({ messageCount: 1, tokenEstimate: 1 } as any);
      await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} } as any);
      await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} } as any);
      await hm.executeUserPromptSubmit({ prompt: 'x', turnCount: 1 } as any);
      await hm.executeTaskCreated({ taskId: 't', subject: 's' } as any);
      await hm.executeTaskCompleted({ taskId: 't', subject: 's' } as any);
      await hm.executePostToolUseFailure({ ...base, toolName: 'bash', toolInput: {}, toolOutput: '', isError: true } as any);
      await hm.executeNotification({ message: 'm', title: 't' } as any);
      await hm.executeSetup({ sessionId: 's', cwd: '.' } as any);
      await hm.executeConfigChange({ source: 'x', filePath: 'f' } as any);
      await hm.executeInstructionsLoaded({ filePath: 'f', loadReason: 'r' } as any);
      await hm.executeWorktreeCreate({ name: 'w', worktreePath: 'p' } as any);
      await hm.executeWorktreeRemove({ name: 'w', worktreePath: 'p' } as any);
      await hm.executeCwdChanged({ oldCwd: 'a', newCwd: 'b' } as any);
      await hm.executeFileChanged({ filePath: 'f', event: 'change' } as any);
      await hm.executeElicitation({ mcpServerName: 'm', message: 'x' } as any);
      await hm.executeElicitationResult({ mcpServerName: 'm', message: 'x' } as any);
      await hm.executeStopFailure({ error: 'e', hookIndex: 0 } as any);
    } finally {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
