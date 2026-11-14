/**
 * Parity (pass-hooks) unit — tests for the hook capabilities brought to Claude
 * Code parity in core/core/src/hooks.ts:
 *  - timeout-in-SECONDS contract for every hook type (resolveHookTimeoutMs)
 *  - HTTP-hook URL allowlist (isHttpHookUrlAllowed, three-state)
 *  - HTTP-hook header env-var interpolation allowlist (interpolateHookHeaderEnv)
 *  - hookSpecificOutput envelope flattening (unwrapHookSpecificOutput)
 *  - `if` condition tool-input matching (matchesHookIf, `Bash(git *)`)
 *  - combined abort (hard timeout + parent cancel) (combineHookAbort)
 *  - workspace-trust gate (shouldSkipHookDueToTrust)
 *  - hook-agent disallowed tools (HOOK_AGENT_DISALLOWED_TOOLS)
 * Plus integration through executePreToolUse: HTTP hook SSRF block, URL-allowlist
 * block, non-2xx failure, header interpolation reaching the server, command-hook
 * seconds timeout + abort cancellation, and engine-hook disallowed-tools +
 * hard-timeout wiring.
 */

import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import {
  HookManager,
  resolveHookTimeoutMs,
  combineHookAbort,
  HOOK_AGENT_DISALLOWED_TOOLS,
  isHttpHookUrlAllowed,
  interpolateHookHeaderEnv,
  unwrapHookSpecificOutput,
  matchesHookIf,
  MAX_HOOK_TIMEOUT_MS,
} from '../src/hooks';
import { shouldSkipHookDueToTrust } from '../src/hook-protection';

const tmpDirs: string[] = [];
function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hooks-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch {} }
});

function preCtx(toolName: string, toolInput: Record<string, unknown> = {}, abortSignal?: AbortSignal) {
  return { messages: [], turnCount: 0, toolName, toolInput, abortSignal } as any;
}

// ---------------------------------------------------------------------------
// resolveHookTimeoutMs — seconds contract (gaps 3,4)
// ---------------------------------------------------------------------------
describe('resolveHookTimeoutMs', () => {
  it('interprets the config value as SECONDS', () => {
    assert.equal(resolveHookTimeoutMs(30, 5000), 30_000);
  });
  it('falls back to the per-type default (ms) when unset', () => {
    assert.equal(resolveHookTimeoutMs(undefined, 10_000), 10_000);
  });
  it('clamps to the min floor and the max ceiling', () => {
    assert.equal(resolveHookTimeoutMs(0, 1), 100);                 // 0s -> clamped up to 100ms
    assert.equal(resolveHookTimeoutMs(100_000, 5000), MAX_HOOK_TIMEOUT_MS); // 100000s -> ceiling
  });
});

// ---------------------------------------------------------------------------
// isHttpHookUrlAllowed — three-state allowlist (gaps 22,23,28)
// ---------------------------------------------------------------------------
describe('isHttpHookUrlAllowed', () => {
  it('undefined allowlist => no restriction', () => {
    assert.equal(isHttpHookUrlAllowed('https://evil.example', undefined), true);
  });
  it('empty allowlist => block everything', () => {
    assert.equal(isHttpHookUrlAllowed('https://ok.example', []), false);
  });
  it('non-empty => must match a wildcard pattern', () => {
    assert.equal(isHttpHookUrlAllowed('https://api.corp.com/hook', ['https://api.corp.com/*']), true);
    assert.equal(isHttpHookUrlAllowed('https://evil.example/hook', ['https://api.corp.com/*']), false);
  });
});

// ---------------------------------------------------------------------------
// interpolateHookHeaderEnv (gap 24)
// ---------------------------------------------------------------------------
describe('interpolateHookHeaderEnv', () => {
  const env = { MY_TOKEN: 'secret', OTHER: 'nope' };
  it('interpolates $VAR and ${VAR} when allowlisted', () => {
    assert.equal(interpolateHookHeaderEnv('Bearer $MY_TOKEN', new Set(['MY_TOKEN']), env), 'Bearer secret');
    assert.equal(interpolateHookHeaderEnv('x=${MY_TOKEN}', new Set(['MY_TOKEN']), env), 'x=secret');
  });
  it('resolves a non-allowlisted var to empty string (no exfil)', () => {
    assert.equal(interpolateHookHeaderEnv('Bearer $OTHER', new Set(['MY_TOKEN']), env), 'Bearer ');
  });
  it('resolves a missing env var to empty string', () => {
    assert.equal(interpolateHookHeaderEnv('$MISSING', new Set(['MISSING']), env), '');
  });
});

// ---------------------------------------------------------------------------
// unwrapHookSpecificOutput (gap 12)
// ---------------------------------------------------------------------------
describe('unwrapHookSpecificOutput', () => {
  it('flattens the envelope onto the top level', () => {
    const out = unwrapHookSpecificOutput({ hookSpecificOutput: { hookEventName: 'PreToolUse', permissionDecision: 'deny', permissionDecisionReason: 'nope' } });
    assert.equal(out.permissionDecision, 'deny');
    assert.equal(out.reason, 'nope');
  });
  it('keeps top-level fields winning over the envelope', () => {
    const out = unwrapHookSpecificOutput({ decision: 'approve', hookSpecificOutput: { decision: 'block' } });
    assert.equal(out.decision, 'approve');
  });
  it('passes through a flat object unchanged and tolerates non-objects', () => {
    assert.equal(unwrapHookSpecificOutput({ decision: 'block' }).decision, 'block');
    assert.deepEqual(unwrapHookSpecificOutput(null), {});
    assert.deepEqual(unwrapHookSpecificOutput('str'), {});
  });
});

// ---------------------------------------------------------------------------
// matchesHookIf — Tool(content) syntax (gap 16)
// ---------------------------------------------------------------------------
describe('matchesHookIf', () => {
  it('gates on tool-input content, not just name', () => {
    assert.equal(matchesHookIf('Bash(git *)', 'bash', { command: 'git push' }), true);
    assert.equal(matchesHookIf('Bash(git *)', 'bash', { command: 'rm -rf /' }), false);
  });
  it('translates the Claude tool name in the rule', () => {
    assert.equal(matchesHookIf('Edit(src/*)', 'file_edit', { file_path: 'src/a.ts' }), true);
    assert.equal(matchesHookIf('Edit(src/*)', 'bash', { command: 'x' }), false);
  });
  it('empty inner matches any input for that tool', () => {
    assert.equal(matchesHookIf('Bash()', 'bash', { command: 'anything' }), true);
  });
  it('falls back to a plain tool-name regex when not Tool(content)', () => {
    assert.equal(matchesHookIf('bash', 'bash', {}), true);
    assert.equal(matchesHookIf('bash', 'grep', {}), false);
  });
});

// ---------------------------------------------------------------------------
// combineHookAbort (gaps 18,19,25,30)
// ---------------------------------------------------------------------------
describe('combineHookAbort', () => {
  it('aborts on the hard timeout and reports timedOut()', async () => {
    const a = combineHookAbort(undefined, 20);
    assert.equal(a.aborted(), false);
    await new Promise(r => setTimeout(r, 40));
    assert.equal(a.aborted(), true);
    assert.equal(a.timedOut(), true);
    a.cleanup();
  });
  it('aborts when the parent signal aborts (and not via timeout)', async () => {
    const parent = new AbortController();
    const a = combineHookAbort(parent.signal, 10_000);
    parent.abort();
    assert.equal(a.aborted(), true);
    assert.equal(a.timedOut(), false);
    a.cleanup();
  });
  it('is already aborted if the parent is already aborted', () => {
    const parent = new AbortController();
    parent.abort();
    const a = combineHookAbort(parent.signal, 10_000);
    assert.equal(a.aborted(), true);
    a.cleanup();
  });
  it('cleanup() clears the timer so no late abort fires', async () => {
    const a = combineHookAbort(undefined, 20);
    a.cleanup();
    await new Promise(r => setTimeout(r, 40));
    assert.equal(a.aborted(), false);
  });
});

// ---------------------------------------------------------------------------
// shouldSkipHookDueToTrust (gap 14)
// ---------------------------------------------------------------------------
describe('shouldSkipHookDueToTrust', () => {
  it('skips all hooks when interactive + untrusted', () => {
    assert.equal(shouldSkipHookDueToTrust(false, true), true);
  });
  it('allows hooks when interactive + trusted', () => {
    assert.equal(shouldSkipHookDueToTrust(true, true), false);
  });
  it('non-interactive implies trust (SDK/detached)', () => {
    assert.equal(shouldSkipHookDueToTrust(false, false), false);
  });
});

describe('HOOK_AGENT_DISALLOWED_TOOLS', () => {
  it('excludes subagent + plan-mode tools', () => {
    assert.ok(HOOK_AGENT_DISALLOWED_TOOLS.includes('task'));
    assert.ok(HOOK_AGENT_DISALLOWED_TOOLS.includes('plan_mode'));
  });
});

// ---------------------------------------------------------------------------
// loadFromFile honors the interactive trust gate (gap 14, wired)
// ---------------------------------------------------------------------------
describe('loadFromFile — interactive trust gate', () => {
  it('loads no hooks at all in an untrusted interactive workspace', () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'command', command: 'echo hi' } }]);
    const hm = new HookManager();
    hm.setInteractive(true);
    hm.setWorkspaceTrusted(false);
    hm.loadFromFile(dir);
    assert.equal(hm.getSummary().preToolUse, 0);
  });
  it('loads hooks when the interactive workspace is trusted', () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'command', command: 'echo hi' } }]);
    const hm = new HookManager();
    hm.setInteractive(true);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.equal(hm.getSummary().preToolUse, 1);
  });
});

// ---------------------------------------------------------------------------
// Integration: HTTP hook through executePreToolUse (gaps 22-29)
// ---------------------------------------------------------------------------
describe('HTTP hook integration', () => {
  function makeManager(dir: string): HookManager {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    return hm;
  }

  it('blocks a cloud-metadata / private IP via the real SSRF guard', async () => {
    // 169.254.169.254 is the cloud metadata endpoint — allowLoopback must NOT
    // rescue it. The real validator rejects before any connection.
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'http://169.254.169.254/latest/meta-data/', onFailure: 'block' } }]);
    const hm = makeManager(dir); // real validator
    const t0 = Date.now();
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'deny');
    assert.ok(Date.now() - t0 < 2000, 'must reject without hanging on a connect');
  });

  it('allows a loopback dev-server URL (matching Claude ssrfGuardedLookup)', async () => {
    const server = http.createServer((_req, res) => { res.statusCode = 200; res.end('DENY:local'); });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as any).port;
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/h` } }]);
    const hm = makeManager(dir); // real validator — loopback is allowed
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'deny'); // server body "DENY:local" drives it
    assert.ok(decision.action === 'deny' && decision.reason === 'local');
    server.close();
  });

  it('blocks a URL not on the allowlist before any I/O', async () => {
    let hit = false;
    const server = http.createServer((_req, res) => { hit = true; res.end('ok'); });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as any).port;
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/h`, onFailure: 'block' } }]);
    const hm = makeManager(dir);
    hm.setHttpHookPolicy({ allowedHttpHookUrls: ['https://only.this.example/*'] });
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' })); // would allow if reached
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'deny');
    assert.equal(hit, false);
    server.close();
  });

  it('treats a non-2xx response as a failure and ignores its body', async () => {
    const server = http.createServer((_req, res) => { res.statusCode = 500; res.end('DENY:body-should-be-ignored'); });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as any).port;
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/h`, onFailure: 'block' } }]);
    const hm = makeManager(dir);
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' }));
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'deny');
    assert.ok(decision.action === 'deny' && /fail-closed/.test(decision.reason), 'reason must be the fail-closed message, not the 500 body');
    server.close();
  });

  it('interpolates an allowlisted env var into a header and reaches the server', async () => {
    const received: Record<string, string | undefined> = {};
    const server = http.createServer((req, res) => {
      received.token = req.headers['x-token'] as string | undefined;
      res.statusCode = 200; res.end(`SYSTEM:${req.headers['x-token'] ?? ''}`);
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as any).port;
    process.env.AMI_TEST_HOOK_TOKEN = 'tok-123';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/h`, headers: { 'X-Token': '$AMI_TEST_HOOK_TOKEN' }, allowedEnvVars: ['AMI_TEST_HOOK_TOKEN'] } }]);
    const hm = makeManager(dir);
    hm.setHttpHookPolicy({ httpHookAllowedEnvVars: ['AMI_TEST_HOOK_TOKEN'] });
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' }));
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(received.token, 'tok-123');
    assert.equal(decision.action, 'allow');
    assert.equal(decision.additionalContext, 'tok-123');
    delete process.env.AMI_TEST_HOOK_TOKEN;
    server.close();
  });

  it('does NOT interpolate a var missing from the global allowlist', async () => {
    const received: Record<string, string | undefined> = {};
    const server = http.createServer((req, res) => { received.token = req.headers['x-token'] as string | undefined; res.statusCode = 200; res.end('SYSTEM:ok'); });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as any).port;
    process.env.AMI_TEST_HOOK_TOKEN2 = 'leak';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/h`, headers: { 'X-Token': '$AMI_TEST_HOOK_TOKEN2' }, allowedEnvVars: ['AMI_TEST_HOOK_TOKEN2'] } }]);
    const hm = makeManager(dir);
    hm.setHttpHookPolicy({ httpHookAllowedEnvVars: [] }); // global allowlist empty => blocked
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' }));
    await hm.executePreToolUse(preCtx('bash'));
    assert.equal(received.token, '', 'non-allowlisted env var must not be interpolated');
    delete process.env.AMI_TEST_HOOK_TOKEN2;
    server.close();
  });
});

// ---------------------------------------------------------------------------
// Integration: command hook seconds-timeout + abort cancellation (gaps 3,4,8,15)
// ---------------------------------------------------------------------------
describe('command hook timeout + cancellation', () => {
  it('interprets timeout as seconds (1s allows a 0.3s command)', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'command', command: 'sleep 0.3; echo ok', timeout: 1, onFailure: 'block' } }]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const decision = await hm.executePreToolUse(preCtx('bash'));
    // 0.3s < 1s -> command completes, exit 0, no decision => allow
    assert.equal(decision.action, 'allow');
  });

  it('cancels an in-flight command hook on the parent abort signal (fail-closed deny)', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'command', command: 'sleep 5; echo done', timeout: 30, onFailure: 'block' } }]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const ac = new AbortController();
    const t0 = Date.now();
    setTimeout(() => ac.abort(), 60);
    const decision = await hm.executePreToolUse(preCtx('bash', {}, ac.signal));
    assert.ok(Date.now() - t0 < 2000, 'abort should tear the hook down quickly, not wait 5s');
    assert.equal(decision.action, 'deny'); // cancelled -> runner throws -> fail-closed deny
  });
});

// ---------------------------------------------------------------------------
// Integration: engine-backed hook disallowed-tools + hard timeout (gaps 18,20,21,30)
// ---------------------------------------------------------------------------
describe('engine-backed (prompt) hook wiring', () => {
  it('passes HOOK_AGENT_DISALLOWED_TOOLS and an AbortController to the engine factory', async () => {
    let captured: any = null;
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'prompt', prompt: 'check', timeout: 5 } }]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory((cfg: any) => {
      captured = cfg;
      return { async *submit() { yield { type: 'text_delta', text: 'SYSTEM:done' }; }, shutdown() {} };
    });
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.deepEqual(captured.disallowedTools, ['task', 'plan_mode']);
    assert.ok(captured.abortController instanceof AbortController);
    assert.equal(decision.action, 'allow');
    assert.equal(decision.additionalContext, 'done');
  });

  it('hard-aborts a stalled model call via the timeout (engine honors the signal)', async () => {
    let aborted = false;
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'prompt', prompt: 'hang', timeout: 0.1 } }]);
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    hm.setEngineFactory((cfg: any) => ({
      async *submit() {
        // A realistic engine that yields nothing until its abortController fires.
        for (;;) {
          await new Promise(r => setTimeout(r, 10));
          if (cfg.abortController.signal.aborted) { aborted = true; return; }
        }
      },
      shutdown() {},
    }));
    const t0 = Date.now();
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.ok(aborted, 'engine must observe the hard-timeout abort');
    assert.ok(Date.now() - t0 < 2000, 'hard timeout (0.1s) should fire quickly');
    assert.equal(decision.action, 'allow'); // no output -> allow
  });
});
