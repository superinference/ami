/**
 * U007 (w77) — four hook-layer parity gaps against the reference's hook system:
 *
 *  - PreToolUse `permissionDecision: 'ask'` (toolHooks.ts:L529-554, types/hooks.ts:L64-78):
 *    a PreToolUse hook can neither-allow-nor-deny and instead FORCE an interactive
 *    permission prompt. AMI previously produced the `ASK:` sentinel but the
 *    HookDecision union/handler dropped it → silently downgraded to `allow`. Now
 *    HookDecision has an `ask` arm, the command handler parses `ASK:`, and the
 *    decision combinator holds `ask` against a later `allow` (only `deny` overrides).
 *
 *  - TaskCompleted abort propagation (TaskUpdateTool.ts:L235-245): executeTaskCompleted
 *    honors the forwarded AbortSignal — an already-aborted turn runs no completion
 *    hooks, and a hanging hook is actively cancelled (raceAbort) rather than leaking
 *    past a Ctrl-C.
 *
 *  - hookIndex-derived per-hook env file / CLAUDE_ENV_FILE (types/hooks.ts:L216-218):
 *    an env-ingestible lifecycle hook receives the EXACT deterministic fragment path
 *    to append `export KEY=VAL` to, which later bash commands pick up.
 *
 *  - Visible hook execution errors (toolHooks.ts:L152-185): a hook that throws/times
 *    out on the fail-OPEN path emits a `hookError` event instead of silently
 *    returning '' (Claude Code's hook_error_during_execution attachment).
 */

import { describe, it, beforeEach, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  HookManager,
  type PreToolUseContext,
  type TaskEventContext,
} from '../src/hooks';
import {
  hookEnvFragmentPath,
  getSessionEnvironmentScript,
  invalidateSessionEnvCache,
} from '../src/session-environment';

function preCtx(overrides: Partial<PreToolUseContext> = {}): PreToolUseContext {
  return { messages: [], turnCount: 0, apiMessages: [], toolName: 'bash', toolInput: { command: 'ls' }, ...overrides };
}

function loadHooks(entries: any[]): { hm: HookManager; tmp: string } {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-w77-'));
  const amiDir = path.join(tmp, '.ami');
  fs.mkdirSync(amiDir, { recursive: true });
  fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.setCwd(tmp);
  hm.loadFromFile(tmp);
  return { hm, tmp };
}

// ---------------------------------------------------------------------------
// PreToolUse 'ask' — HookDecision arm + ASK: parse + combinator
// ---------------------------------------------------------------------------

describe('U007 w77 — PreToolUse permissionDecision:ask', () => {
  it('command hook ASK: maps to a HookDecision { action:"ask", reason }', async () => {
    const { hm, tmp } = loadHooks([{
      event: 'preToolUse',
      hook: { type: 'command', command: `printf '{"permissionDecision":"ask","reason":"please confirm"}'` },
    }]);
    try {
      const dec = await hm.executePreToolUse(preCtx());
      assert.equal(dec.action, 'ask');
      assert.equal((dec as { reason?: string }).reason, 'please confirm');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('decision:"ask" (not just permissionDecision) also forces ask', async () => {
    const { hm, tmp } = loadHooks([{
      event: 'preToolUse',
      hook: { type: 'command', command: `printf '{"decision":"ask"}'` },
    }]);
    try {
      const dec = await hm.executePreToolUse(preCtx());
      assert.equal(dec.action, 'ask');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('a later allow hook does NOT loosen a prior ask (ask holds)', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({ action: 'ask', reason: 'confirm-me' }));
    hm.onPreToolUse(async () => ({ action: 'allow' }));
    const dec = await hm.executePreToolUse(preCtx());
    assert.equal(dec.action, 'ask');
    assert.equal((dec as { reason?: string }).reason, 'confirm-me');
  });

  it('a deny hook overrides a prior ask (deny wins)', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({ action: 'ask', reason: 'confirm-me' }));
    hm.onPreToolUse(async () => ({ action: 'deny', reason: 'blocked' }));
    const dec = await hm.executePreToolUse(preCtx());
    assert.equal(dec.action, 'deny');
  });

  it('ask carries updatedInput forward for the prompt to approve', async () => {
    const hm = new HookManager();
    hm.onPreToolUse(async () => ({ action: 'ask', updatedInput: { command: 'ls -la' } }));
    const dec = await hm.executePreToolUse(preCtx());
    assert.equal(dec.action, 'ask');
    assert.deepEqual((dec as { updatedInput?: Record<string, unknown> }).updatedInput, { command: 'ls -la' });
  });
});

// ---------------------------------------------------------------------------
// TaskCompleted abort propagation
// ---------------------------------------------------------------------------

describe('U007 w77 — executeTaskCompleted abort boundary', () => {
  const baseCtx = (signal?: AbortSignal): TaskEventContext => ({ taskId: '1', subject: 'x', signal });

  it('an already-aborted signal runs no completion hooks', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onTaskCompleted(async () => { called = true; });
    const ac = new AbortController();
    ac.abort();
    await hm.executeTaskCompleted(baseCtx(ac.signal));
    assert.equal(called, false, 'completion hook must not run when already aborted');
  });

  it('a hanging completion hook is cancelled when the signal aborts (does not leak)', async () => {
    const hm = new HookManager();
    hm.onTaskCompleted(() => new Promise<void>(() => { /* never resolves */ }));
    const ac = new AbortController();
    const p = hm.executeTaskCompleted(baseCtx(ac.signal));
    // Abort shortly after the hook starts awaiting.
    setTimeout(() => ac.abort(), 20);
    // Must settle promptly rather than hanging forever.
    const settled = await Promise.race([
      p.then(() => 'settled'),
      new Promise<string>((r) => setTimeout(() => r('timeout'), 2000)),
    ]);
    assert.equal(settled, 'settled', 'executeTaskCompleted must resolve once aborted, not hang');
  });

  it('without an abort, completion hooks still run and blocking errors surface', async () => {
    const hm = new HookManager();
    hm.onTaskCompleted(async () => ({ blockingError: 'nope' }));
    const r = await hm.executeTaskCompleted(baseCtx());
    assert.deepEqual(r, { blockingErrors: ['nope'] });
  });
});

// ---------------------------------------------------------------------------
// hookIndex-derived per-hook env file (CLAUDE_ENV_FILE)
// ---------------------------------------------------------------------------

describe('U007 w77 — hookEnvFragmentPath', () => {
  const env = { SUPERINFERENCE_DATA_DIR: '/tmp/ami-envfile-unit' } as NodeJS.ProcessEnv;

  it('returns a deterministic <event>-hook-<index>.sh path for ingestible events', () => {
    const p = hookEnvFragmentPath('sess', 'sessionStart', 0, env);
    assert.ok(p);
    assert.ok(p!.endsWith(path.join('sess', 'sessionstart-hook-0.sh')), p!);
    // The index keys the file so two hooks on the same event do not collide.
    const p1 = hookEnvFragmentPath('sess', 'sessionStart', 1, env);
    assert.ok(p1!.endsWith('sessionstart-hook-1.sh'));
    assert.notEqual(p, p1);
  });

  it('returns null for a non-ingestible event (it would never be re-read)', () => {
    assert.equal(hookEnvFragmentPath('sess', 'preToolUse', 0, env), null);
    assert.equal(hookEnvFragmentPath('sess', 'stop', 0, env), null);
  });

  it('returns null for a negative / non-integer index', () => {
    assert.equal(hookEnvFragmentPath('sess', 'setup', -1, env), null);
    assert.equal(hookEnvFragmentPath('sess', 'setup', 1.5, env), null);
  });
});

describe('U007 w77 — SessionStart hook exports env via CLAUDE_ENV_FILE', () => {
  let prevDataDir: string | undefined;
  let dataDir: string;
  beforeEach(() => {
    prevDataDir = process.env.SUPERINFERENCE_DATA_DIR;
    dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-envfile-'));
    process.env.SUPERINFERENCE_DATA_DIR = dataDir;
    invalidateSessionEnvCache();
  });
  after(() => {
    if (prevDataDir === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
    else process.env.SUPERINFERENCE_DATA_DIR = prevDataDir;
    invalidateSessionEnvCache();
  });

  it('a sessionStart command hook can write to $CLAUDE_ENV_FILE and it is re-ingested', async () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-envfile-cwd-'));
    const amiDir = path.join(tmp, '.ami');
    fs.mkdirSync(amiDir, { recursive: true });
    fs.writeFileSync(path.join(amiDir, 'hooks.json'), JSON.stringify({
      hooks: [{
        event: 'sessionStart',
        // Append an export line to the exact env-file path the hook is handed.
        hook: { type: 'command', command: `printf 'export AMI_W77_ENV=present\\n' >> "$CLAUDE_ENV_FILE"` },
      }],
    }));
    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.setSessionId('envsess');
      hm.setCwd(tmp);
      hm.loadFromFile(tmp);
      await hm.executeSessionStart({ sessionId: 'envsess', cwd: tmp });

      invalidateSessionEnvCache();
      const script = await getSessionEnvironmentScript('envsess');
      assert.ok(script, 'session env script should be assembled from the hook fragment');
      assert.match(script!, /export AMI_W77_ENV=present/);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
      fs.rmSync(dataDir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Visible hook execution errors on the fail-OPEN path
// ---------------------------------------------------------------------------

describe('U007 w77 — hookError event on fail-open hook failure', () => {
  it('a timed-out command hook (onFailure != block) emits a visible hookError', async () => {
    const { hm, tmp } = loadHooks([{
      event: 'preToolUse',
      // Hang far longer than the (clamped-to-100ms) timeout so the runner kills it.
      hook: { type: 'command', command: 'sleep 5', timeout: 0.1 },
    }]);
    try {
      const errors: Array<{ hookType: string; hook: string; error: string }> = [];
      hm.on('hookError', (e: any) => errors.push(e));
      const dec = await hm.executePreToolUse(preCtx());
      // Fail-open: the tool is NOT blocked ...
      assert.equal(dec.action, 'allow');
      // ... but the failure is surfaced, not silent.
      assert.equal(errors.length, 1, 'exactly one hookError emitted');
      assert.equal(errors[0].hookType, 'command');
      assert.match(errors[0].error, /timed out/i);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  it('a blocking (onFailure:block) timed-out hook denies and does NOT emit hookError', async () => {
    const { hm, tmp } = loadHooks([{
      event: 'preToolUse',
      hook: { type: 'command', command: 'sleep 5', timeout: 0.1, onFailure: 'block' },
    }]);
    try {
      const errors: unknown[] = [];
      hm.on('hookError', (e: unknown) => errors.push(e));
      const dec = await hm.executePreToolUse(preCtx());
      assert.equal(dec.action, 'deny');
      assert.equal(errors.length, 0, 'block path DENYs (visible) — no separate hookError');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
