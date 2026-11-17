/**
 * U007 (w72) — hook observability + correctness parity (Claude Code → AMI).
 *
 * Covers these confirmed gaps, all contained in core/core/src/hooks.ts:
 *
 *  - execHttpHook.ts "http hook execution introspection debug logging": log the
 *    outbound request target ("HTTP hook POST to <url>") AND the response
 *    ("response status <code>, body length <n>") — Claude Code's
 *    logForDebugging lines around the axios call.
 *  - execHttpHook.ts "specific http hook failure message / error logging":
 *    a transport error / timeout surfaces its underlying detail in the DENY
 *    reason and the debug log, not a generic "failed".
 *  - execPromptHook.ts "introspection debug trace of the prompt hook lifecycle":
 *    log the processed prompt, the model reply, and the met/not-met decision.
 *  - execPromptHook.ts "user facing output validation messages for prompt hook
 *    replies": a reply that does not conform to the {ok,reason} schema emits a
 *    distinct, NON-blocking diagnostic (AMI keeps its SYSTEM:/raw passthrough).
 *  - execAgentHook.ts "distinct outcomes for cancelled / timeout vs genuine
 *    error vs completion": a run that errors / times out / is cancelled must NOT
 *    return its partial model text as a hook decision.
 *  - updateConfig.ts "three layer settings precedence (user/project/local)" +
 *    addDirPluginSettings.ts "settings.local.json as a per-directory tier":
 *    `.claude/settings.local.json` and `.ami/settings.local.json` are read as
 *    local-after-project hook sources.
 *
 * RED (on revert of each change in hooks.ts):
 *  - partial-text test: the errored prompt hook's "DENY:leaked" partial becomes
 *    the decision (action=deny) instead of being discarded (action=allow).
 *  - settings.local.json test: the local-file hook is never loaded (summary 0).
 *  - the debug-log assertions fail because the lines are not emitted.
 */

import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import type { AddressInfo } from 'node:net';
import { HookManager, type PreToolUseContext } from '../src/hooks';

const tmpDirs: string[] = [];
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ } }
});

function tmpCwd(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-w72-'));
  tmpDirs.push(dir);
  return dir;
}
function writeHookFile(dir: string, rel: string, entries: unknown[]): void {
  const full = path.join(dir, rel);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  fs.writeFileSync(full, JSON.stringify({ hooks: entries }));
}
function preCtx(toolName = 'bash'): PreToolUseContext {
  return { messages: [], turnCount: 0, toolName, toolInput: { command: 'ls' } };
}
/** Run `fn` with coreLog routed to a captured stderr buffer. */
async function withCapturedLog(fn: () => Promise<void>): Promise<string[]> {
  const prev = process.env.AMI_DEBUG_TO_STDERR;
  process.env.AMI_DEBUG_TO_STDERR = '1';
  const lines: string[] = [];
  const spy = mock.method(process.stderr, 'write', (s: string | Uint8Array): boolean => {
    lines.push(String(s));
    return true;
  });
  try {
    await fn();
  } finally {
    spy.mock.restore();
    if (prev === undefined) delete process.env.AMI_DEBUG_TO_STDERR;
    else process.env.AMI_DEBUG_TO_STDERR = prev;
  }
  return lines;
}

// ---------------------------------------------------------------------------
// settings.local.json read set (local-after-project)
// ---------------------------------------------------------------------------
describe('hooks loadFromFile — settings.local.json per-directory local tier', () => {
  it('loads a preToolUse hook declared in .claude/settings.local.json', () => {
    const dir = tmpCwd();
    writeHookFile(dir, '.claude/settings.local.json', [
      { event: 'preToolUse', hook: { type: 'command', command: 'echo local' } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.equal(hm.getSummary().preToolUse, 1, '.claude/settings.local.json hook must be registered');
  });

  it('loads a preToolUse hook declared in .ami/settings.local.json', () => {
    const dir = tmpCwd();
    writeHookFile(dir, '.ami/settings.local.json', [
      { event: 'preToolUse', hook: { type: 'command', command: 'echo ami-local' } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.equal(hm.getSummary().preToolUse, 1, '.ami/settings.local.json hook must be registered');
  });

  it('layers the local tier ON TOP of the project tier (both fire)', () => {
    const dir = tmpCwd();
    writeHookFile(dir, '.ami/hooks.json', [
      { event: 'preToolUse', hook: { type: 'command', command: 'echo project' } },
    ]);
    writeHookFile(dir, '.ami/settings.local.json', [
      { event: 'postToolUse', hook: { type: 'command', command: 'echo local' } },
    ]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.equal(hm.getSummary().preToolUse, 1, 'project hook kept');
    assert.equal(hm.getSummary().postToolUse, 1, 'local-tier hook added on top');
  });
});

// ---------------------------------------------------------------------------
// engine hook: no partial text on error / timeout (distinct cancelled outcomes)
// ---------------------------------------------------------------------------
describe('runEngineHook — a non-completed run never yields a decision', () => {
  function promptHookDir(): string {
    const dir = tmpCwd();
    writeHookFile(dir, '.ami/hooks.json', [
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'verify', timeout: 5 } },
    ]);
    return dir;
  }

  it('discards partial "DENY:" text when the engine emits an error event (allow, not deny)', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(promptHookDir());
    // Partial text that LOOKS like a block, then an error → the run did not
    // complete, so the partial verdict must be discarded.
    hm.setEngineFactory(() => ({
      async *submit(_p: string) {
        yield { type: 'text_delta', text: 'DENY:leaked-partial' };
        yield { type: 'error', error: 'engine blew up' };
      },
      shutdown() {},
    }) as any);
    const decision = await hm.executePreToolUse(preCtx());
    assert.equal(decision.action, 'allow', 'an errored hook run must not block on partial text');
  });

  it('discards partial "DENY:" text when the run times out (allow, not deny)', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    // timeout 0.1s -> 100ms (the clamped minimum) so the deadline fires fast.
    const dir = tmpCwd();
    writeHookFile(dir, '.ami/hooks.json', [
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'verify', timeout: 0.1 } },
    ]);
    hm.loadFromFile(dir);
    hm.setEngineFactory((cfg: any) => ({
      async *submit(_p: string) {
        yield { type: 'text_delta', text: 'DENY:leaked-partial' };
        // Block until the combined timeout aborts the sub-engine, then end.
        await new Promise<void>(res => cfg.abortController.signal.addEventListener('abort', () => res(), { once: true }));
      },
      shutdown() {},
    }) as any);
    const decision = await hm.executePreToolUse(preCtx());
    assert.equal(decision.action, 'allow', 'a timed-out hook run must not block on partial text');
  });

  it('still blocks when the run COMPLETES with {ok:false} (sanity: fix does not over-allow)', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(promptHookDir());
    hm.setEngineFactory(() => ({
      async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":false,"reason":"bad command"}' }; },
      shutdown() {},
    }) as any);
    const decision = await hm.executePreToolUse(preCtx());
    assert.equal(decision.action, 'deny');
    assert.match(decision.action === 'deny' ? decision.reason : '', /bad command/);
  });
});

// ---------------------------------------------------------------------------
// engine hook: introspection debug trace + non-conforming-output diagnostic
// ---------------------------------------------------------------------------
describe('runEngineHook — introspection debug trace', () => {
  function dirWith(prompt = 'is this safe'): string {
    const dir = tmpCwd();
    writeHookFile(dir, '.ami/hooks.json', [
      { event: 'preToolUse', hook: { type: 'prompt', prompt, timeout: 5 } },
    ]);
    return dir;
  }

  it('logs the processed prompt, the model reply, and the met/not-met decision', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dirWith('decide-xyz'));
    hm.setEngineFactory(() => ({
      async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":false,"reason":"nope-trace"}' }; },
      shutdown() {},
    }) as any);
    const logs = (await withCapturedLog(async () => { await hm.executePreToolUse(preCtx()); })).join('\n');
    assert.match(logs, /prompt hook processing prompt:/, 'processed prompt is traced');
    assert.match(logs, /prompt hook model reply:.*nope-trace/, 'model reply is traced');
    assert.match(logs, /prompt hook condition was not met: nope-trace/, 'decision is traced');
  });

  it('emits a distinct non-blocking diagnostic for non-conforming output, keeping SYSTEM passthrough', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dirWith());
    hm.setEngineFactory(() => ({
      async *submit(_p: string) { yield { type: 'text_delta', text: 'SYSTEM:extra ctx' }; },
      shutdown() {},
    }) as any);
    let decision: any;
    const logs = (await withCapturedLog(async () => { decision = await hm.executePreToolUse(preCtx()); })).join('\n');
    // AMI's superset: the output is still applied as non-blocking context.
    assert.equal(decision.action, 'allow');
    assert.equal(decision.additionalContext, 'extra ctx');
    assert.match(logs, /did not match the \{ok,reason\} schema/, 'malformed-output diagnostic emitted');
  });
});

// ---------------------------------------------------------------------------
// http hook: request/response introspection + specific transport-error message
// ---------------------------------------------------------------------------
describe('runHttpHook — observability + specific failure detail', () => {
  function httpHookDir(url: string, onFailure?: 'block'): string {
    const dir = tmpCwd();
    writeHookFile(dir, '.ami/hooks.json', [
      { event: 'preToolUse', hook: { type: 'http', url, ...(onFailure ? { onFailure } : {}) } },
    ]);
    return dir;
  }

  it('logs the POST target and the response status + body length', async () => {
    const server = http.createServer((_req, res) => { res.writeHead(200); res.end('SYSTEM:ok-body'); });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as AddressInfo).port;
    const url = `http://127.0.0.1:${port}/hook`;
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' }));
    hm.loadFromFile(httpHookDir(url));
    const logs = (await withCapturedLog(async () => { await hm.executePreToolUse(preCtx()); })).join('\n');
    server.close();
    assert.match(logs, new RegExp(`HTTP hook POST to ${url.replace(/[/.]/g, m => '\\' + m)}`), 'request target logged');
    assert.match(logs, /HTTP hook response status 200, body length \d+/, 'response status + body length logged');
  });

  it('includes the underlying transport error in the DENY reason and the log', async () => {
    // Bind then immediately close to get a reliably-refused port.
    const probe = http.createServer();
    await new Promise<void>(r => probe.listen(0, '127.0.0.1', r));
    const port = (probe.address() as AddressInfo).port;
    await new Promise<void>(r => probe.close(() => r()));
    const url = `http://127.0.0.1:${port}/hook`;
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' }));
    hm.loadFromFile(httpHookDir(url, 'block'));
    let decision: any;
    const logs = (await withCapturedLog(async () => { decision = await hm.executePreToolUse(preCtx()); })).join('\n');
    assert.equal(decision.action, 'deny');
    assert.match(decision.reason, /HTTP hook failed: /, 'DENY reason carries the specific failure, not a generic message');
    assert.ok(!/fail-closed/.test(decision.reason), 'transport error is not the generic fail-closed message');
    assert.match(logs, /HTTP hook error: /, 'the underlying error is logged');
  });
});
