/**
 * Wave-27 — hooks residual branch arms (excl7: ~95.9% br on hooks.mjs).
 *
 * Targets uncovered lines from cov-core-excl7:
 *   1277 destroy-in-prompt-cancel empty catch
 *   1822 fireAsyncHook early abort (parentSignal aborted)
 *   2012-2013 resolveHookTranscriptPath journalPathFor catch
 *   2214-2216 discoverPluginHookSources categorized catch
 *   2445 getPluginAffectingSettingsSnapshot statSync empty catch
 *   2698-2700 postToolUse continue:false + stopReason string
 *   3435-3436 executeSessionEnd runWithTimeout budget catch
 *   3571-3573 userPromptSubmit preventContinuation + stopReason / throw catch
 *   4439-4441 runHttpHookViaProxy catch (Error vs non-Error)
 *
 * Behavior-preserving DI seams: hookSpawnHooks (existing), hookJournalHooks,
 * hookPluginDiscoverHooks, hookFsHooks. Production defaults unchanged.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';

import {
  HookManager,
  hookSpawnHooks,
  hookJournalHooks,
  hookPluginDiscoverHooks,
  hookFsHooks,
  type SessionContext,
  type PostToolUseHookResult,
  type UserPromptSubmitHookResult,
} from '../src/hooks';
const TEMP = path.join(os.tmpdir(), 'ami-cov-w27-hooks');
const REAL_SPAWN = hookSpawnHooks.spawn;
const REAL_JOURNAL = hookJournalHooks.journalPathFor;
const REAL_DISCOVER = hookPluginDiscoverHooks.discoverPlugins;
const REAL_STAT = hookFsHooks.statSync;
const SESSION_END_ENV = 'CLAUDE_CODE_SESSIONEND_HOOKS_TIMEOUT_MS';

afterEach(() => {
  hookSpawnHooks.spawn = REAL_SPAWN;
  hookJournalHooks.journalPathFor = REAL_JOURNAL;
  hookPluginDiscoverHooks.discoverPlugins = REAL_DISCOVER;
  hookFsHooks.statSync = REAL_STAT;
  delete process.env[SESSION_END_ENV];
  delete process.env.HTTPS_PROXY;
  delete process.env.https_proxy;
  delete process.env.HTTP_PROXY;
  delete process.env.http_proxy;
  delete process.env.NO_PROXY;
});

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function fakeChild(opts: {
  writeThrow?: boolean;
  destroyThrow?: boolean;
  exitCode?: number;
  stdoutData?: string;
  /** Emit a PromptRequest JSON line so requestPrompt is invoked. */
  promptRequestLine?: string;
} = {}): EventEmitter & {
  stdin: PassThrough & { destroy: () => void };
  stdout: PassThrough;
  stderr: PassThrough;
  kill: () => boolean;
  pid: number;
} {
  const ee = new EventEmitter() as EventEmitter & {
    stdin: PassThrough & { destroy: () => void };
    stdout: PassThrough;
    stderr: PassThrough;
    kill: () => boolean;
    pid: number;
  };
  const stdin = new PassThrough() as PassThrough & { destroy: () => void };
  if (opts.writeThrow) {
    stdin.write = (() => {
      throw new Error('stdin-write-fail');
    }) as typeof stdin.write;
  }
  const origDestroy = stdin.destroy.bind(stdin);
  stdin.destroy = ((...args: unknown[]) => {
    if (opts.destroyThrow) throw new Error('stdin-destroy-fail');
    return origDestroy(...(args as []));
  }) as typeof stdin.destroy;
  ee.stdin = stdin;
  ee.stdout = new PassThrough();
  ee.stderr = new PassThrough();
  ee.kill = () => true;
  ee.pid = 2727;
  queueMicrotask(() => {
    if (opts.promptRequestLine) ee.stdout.write(opts.promptRequestLine + '\n');
    if (opts.stdoutData) ee.stdout.write(opts.stdoutData);
    ee.stdout.end();
    ee.stderr.end();
    ee.emit('close', opts.exitCode ?? 0);
  });
  return ee;
}

describe('wave27 — hooks DI residual arms', () => {
  it('prompt-cancel path: requestPrompt reject + stdin.destroy throw (empty catch)', async () => {
    const d = mkTmp('hk27-destroy-');
    const hm = new HookManager({ cwd: d, sessionId: 'w27' });
    // Valid PromptRequest shape (prompt + message + options) so the line scanner fires.
    const promptLine = JSON.stringify({
      prompt: 'p1',
      message: 'Choose',
      options: [{ key: 'a', label: 'A' }],
    });
    hookSpawnHooks.spawn = (() =>
      fakeChild({ destroyThrow: true, promptRequestLine: promptLine })) as typeof hookSpawnHooks.spawn;
    hm.setPromptRequestHandler(async () => {
      throw new Error('user-cancelled');
    });
    hm.registerConfigEntries(
      [{ event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'true', timeout: 2 } }],
      d,
    );
    // Resolver rejects → destroy catch arm; destroy itself throws → empty inner catch.
    await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
    } as never);
  });

  it('fireAsyncHook early abort when parentSignal already aborted', async () => {
    const d = mkTmp('hk27-async-');
    const hm = new HookManager({ cwd: d, sessionId: 'w27' });
    hm.registerConfigEntries(
      [{ event: 'preToolUse', matcher: 'bash', hook: { type: 'command', command: 'true', async: true, timeout: 1 } }],
      d,
    );
    const ac = new AbortController();
    ac.abort();
    // preToolUse threads abortSignal into fireAsyncHook — hits ac.abort() branch.
    await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
      abortSignal: ac.signal,
    } as never);
    await hm.finalizePendingAsyncHooks();
  });

  it('resolveHookTranscriptPath: journalPathFor throw → undefined (no scoped Read)', async () => {
    const d = mkTmp('hk27-journal-');
    const hm = new HookManager({ cwd: d, sessionId: 'w27-sess' });
    hm.setSessionId('w27-sess');
    hookJournalHooks.journalPathFor = () => {
      throw new Error('path-traversal');
    };
    let captured: { appendSystemPrompt?: string; permissionRules?: unknown[] } = {};
    hm.setEngineFactory((cfg: Record<string, unknown>) => {
      captured = cfg as typeof captured;
      return {
        async *submit() {
          yield { type: 'text_delta', text: '{"ok":true}' };
        },
        shutdown() {},
      };
    });
    hm.registerConfigEntries(
      [{ event: 'preToolUse', matcher: 'bash', hook: { type: 'agent', prompt: 'verify', timeout: 2 } }],
      d,
    );
    await hm.executePreToolUse({ toolName: 'bash', toolInput: { command: 'echo' } } as never);
    // Catch arm: no transcript → system prompt still present, no scoped Read rule.
    assert.equal(typeof captured.appendSystemPrompt, 'string');
    assert.ok(!captured.permissionRules || captured.permissionRules.length === 0);
    assert.doesNotMatch(String(captured.appendSystemPrompt), /transcript is available at/i);
  });

  it('discoverPluginHookSources: discoverPlugins throw → categorized catch', () => {
    const d = mkTmp('hk27-discover-');
    const hm = new HookManager({ cwd: d, sessionId: 'w27' });
    hookPluginDiscoverHooks.discoverPlugins = () => {
      throw new Error('plugin-scan-failed');
    };
    // loadFromFile / snapshot both call discoverPluginHookSources.
    hm.loadFromFile(d);
    const snap = hm.getPluginAffectingSettingsSnapshot(d);
    assert.equal(typeof snap, 'string');
    // Non-Error throw exercises String(err) arm inside the catch.
    hookPluginDiscoverHooks.discoverPlugins = () => {
      throw 'bare-string-fail';
    };
    hm.reloadPluginHooks(d);
  });

  it('getPluginAffectingSettingsSnapshot: statSync throw → mtime 0', () => {
    const d = mkTmp('hk27-stat-');
    const plug = path.join(d, '.ami', 'plugins', 'p1');
    fs.mkdirSync(plug, { recursive: true });
    const hooksFile = path.join(plug, 'hooks.json');
    fs.writeFileSync(hooksFile, JSON.stringify({ hooks: [] }));
    // Force a known source path via the discover seam (no real plugin manifest needed).
    hookPluginDiscoverHooks.discoverPlugins = (dir: string) => {
      if (!dir.includes(d)) return [];
      return [{ name: 'p1', root: plug, hooksFile }];
    };
    hookFsHooks.statSync = (() => {
      throw new Error('EACCES');
    }) as typeof hookFsHooks.statSync;
    const hm = new HookManager({ cwd: d, sessionId: 'w27' });
    const snap = hm.getPluginAffectingSettingsSnapshot(d);
    assert.match(snap, /:0/);
    // Real stat arm still works.
    hookFsHooks.statSync = REAL_STAT;
    const snap2 = hm.getPluginAffectingSettingsSnapshot(d);
    assert.ok(typeof snap2 === 'string' && snap2.length > 0);
  });

  it('postToolUse continue:false + string stopReason + object updatedMCPToolOutput', async () => {
    // Command hooks rewrite continue:false → DENY: before tryParseHookJson; a
    // prompt/agent hook returns the raw JSON string, which hits the parse arms.
    const d = mkTmp('hk27-post-');
    const hm = new HookManager({ cwd: d, sessionId: 'w27' });
    const payload = JSON.stringify({
      continue: false,
      stopReason: 'halt-now',
      updatedMCPToolOutput: { k: 1 },
    });
    hm.setEngineFactory(() => ({
      async *submit() {
        yield { type: 'text_delta', text: payload };
      },
      shutdown() {},
    }));
    hm.setWorkspaceTrusted(true);
    hm.registerConfigEntries(
      [{ event: 'postToolUse', matcher: '*', hook: { type: 'prompt', prompt: 'rewrite', timeout: 3 } }],
      d,
    );
    const post = (await hm.executePostToolUse({
      toolName: 'bash',
      toolInput: {},
      toolOutput: 'ok',
    } as never)) as PostToolUseHookResult | undefined;
    assert.equal(post?.preventContinuation, true);
    assert.equal(post?.stopReason, 'halt-now');
    assert.equal(post?.updatedToolOutput, JSON.stringify({ k: 1 }));
  });

  it('executeSessionEnd: hanging hook + never-aborted signal → budget catch', async () => {
    const hm = new HookManager();
    hm.onSessionEnd(() => new Promise<void>(() => { /* never settles */ }));
    process.env[SESSION_END_ENV] = '100';
    // Explicit never-aborted signal disables the synthesized AbortSignal.timeout
    // lifecycle, so raceAbort cannot settle the hang — runWithTimeout must reject.
    const sticky = new AbortController();
    const outcome = await Promise.race([
      hm
        .executeSessionEnd({
          sessionId: 's',
          cwd: process.cwd(),
          reason: 'logout',
          signal: sticky.signal,
        } as SessionContext)
        .then(() => 'returned'),
      new Promise<string>((r) => setTimeout(() => r('hung'), 2_000)),
    ]);
    assert.equal(outcome, 'returned');
  });

  it('userPromptSubmit: preventContinuation+stopReason success arm AND throw catch', async () => {
    const hm = new HookManager();
    hm.onUserPromptSubmit(async () => {
      const r: UserPromptSubmitHookResult = {
        preventContinuation: true,
        stopReason: 'user-halt',
        additionalContext: 'ctx',
      };
      return r;
    });
    hm.onUserPromptSubmit(async () => {
      throw new Error('ups-boom');
    });
    // Also hit the non-Error stringify arm.
    hm.onUserPromptSubmit(async () => {
      throw 'ups-string';
    });
    const result = await hm.executeUserPromptSubmit({
      prompt: 'hi',
      messages: [],
      turnCount: 0,
    } as never);
    assert.equal(result?.preventContinuation, true);
    assert.equal(result?.stopReason, 'user-halt');
  });

  it('runHttpHookViaProxy catch: Error vs non-Error failDetail arms', async () => {
    process.env.HTTPS_PROXY = 'http://127.0.0.1:3128';
    const d = mkTmp('hk27-proxy-');
    fs.mkdirSync(path.join(d, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(d, '.ami', 'hooks.json'),
      JSON.stringify({
        hooks: [
          {
            event: 'preToolUse',
            hook: { type: 'http', url: 'http://webhook.example/h', timeout: 1, onFailure: 'block' },
          },
        ],
      }),
    );
    const hm = new HookManager({ cwd: d, sessionId: 'w27' });
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(d);

    // Error throw → failDetail(err.message)
    hm.setProxiedHookFetch((async () => {
      throw new Error('proxy-transport-down');
    }) as typeof fetch);
    const deny1 = await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
    } as never);
    assert.equal(deny1.action, 'deny');

    // Non-Error throw → failDetail(undefined) → 'transport error'
    hm.setProxiedHookFetch((async () => {
      throw 'not-an-error';
    }) as typeof fetch);
    const deny2 = await hm.executePreToolUse({
      toolName: 'bash',
      toolInput: { command: 'echo' },
    } as never);
    assert.equal(deny2.action, 'deny');
  });
});
