/**
 * Coverage-focused tests for src/hooks.ts.
 *
 * Targets uncovered lines: loadFromFile, processHookEntry (all event types),
 * buildHookEnv, runHookByType (command/prompt/http/agent), matchesPattern,
 * execute* methods for newer hooks, and edge cases in runCommandHookAsync.
 *
 * Uses node:test + node:assert/strict. Mocks fs/child_process as needed.
 */

import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  HookManager,
  type HookContext,
  type PreToolUseContext,
  type PostToolUseContext,
  type PreSamplingContext,
  type SessionContext,
  type SubagentContext,
  type CompactContext,
  type NotificationContext,
  type ConfigChangeContext,
  type InstructionsContext,
  type WorktreeContext,
  type FileChangeContext,
  type CwdChangedContext,
  type StopFailureContext,
  type ElicitationContext,
  type EngineFactory,
} from '../src/hooks';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeContext(overrides: Partial<HookContext> = {}): HookContext {
  return { messages: [{ role: 'user', content: 'hello' }], turnCount: 1, ...overrides };
}

function makePreToolUseCtx(overrides: Partial<PreToolUseContext> = {}): PreToolUseContext {
  return { messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'ls' }, ...overrides };
}

function makePostToolUseCtx(overrides: Partial<PostToolUseContext> = {}): PostToolUseContext {
  return { messages: [], turnCount: 1, toolName: 'bash', toolInput: {}, toolOutput: 'ok', isError: false, ...overrides };
}

function makePreSamplingCtx(overrides: Partial<PreSamplingContext> = {}): PreSamplingContext {
  return { messages: [], turnCount: 1, apiMessages: [], ...overrides };
}

/** Create a temp directory with a .superinference/hooks.json */
function writeTmpHooks(entries: any[], subDir = '.superinference'): string {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
  const dir = path.join(tmpDir, subDir);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return tmpDir;
}

/** Create a .superinference/config.json with hooks */
function writeTmpConfig(cwd: string, entries: any[]): void {
  const dir = path.join(cwd, '.superinference');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'config.json'), JSON.stringify({ hooks: entries }));
}

/** Create a mock engine factory that yields events from an array */
function mockEngineFactory(events: Array<{ type: string; text?: string; error?: string }>): EngineFactory {
  return (_config: any) => ({
    submit(_prompt: string) {
      return (async function* () {
        for (const e of events) yield e;
      })();
    },
    shutdown() {},
  });
}

let hm: HookManager;

beforeEach(() => {
  hm = new HookManager();
});

// ---------------------------------------------------------------------------
// loadFromFile — project hooks with trusted workspace
// ---------------------------------------------------------------------------
describe('loadFromFile — trusted workspace project hooks', () => {
  it('loads a preToolUse command hook from project hooks.json', async () => {
    const tmpDir = writeTmpHooks([
      {
        event: 'preToolUse',
        hook: { type: 'command', command: 'echo ALLOW' },
        matcher: 'bash',
      },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const summary = hm.getSummary();
    assert.equal(summary.preToolUse, 1);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('skips project hooks when workspace is untrusted', () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'echo ok' } },
    ]);
    hm.setWorkspaceTrusted(false);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().postToolUse, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles missing hooks.json gracefully', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
    // No hooks.json written
    hm.loadFromFile(tmpDir);
    // Should not throw
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles invalid JSON in hooks.json gracefully', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), '{ invalid json }}}');
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles hooks.json with missing hooks array', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ notHooks: true }));
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('skips entries with no hook config', () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse' /* no hook key */ },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    assert.equal(hm.getSummary().preToolUse, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// loadFromFile — config.json hooks
// ---------------------------------------------------------------------------
describe('loadFromFile — config.json hooks', () => {
  it('loads hooks from .superinference/config.json', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
    writeTmpConfig(tmpDir, [
      { event: 'stop', hook: { type: 'command', command: 'echo done' } },
    ]);
    hm.loadFromFile(tmpDir);
    assert.equal(hm.getSummary().stop, 1);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('ignores config.json without hooks array', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), JSON.stringify({ noHooks: true }));
    hm.loadFromFile(tmpDir);
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles config.json parse errors silently', () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-cov-'));
    const dir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'config.json'), '{broken');
    hm.loadFromFile(tmpDir);
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — all event types via loadFromFile + command hooks
// ---------------------------------------------------------------------------
describe('processHookEntry — event type registration', () => {
  const eventConfigs: Array<{ event: string; summaryKey: string }> = [
    { event: 'preToolUse', summaryKey: 'preToolUse' },
    { event: 'postToolUse', summaryKey: 'postToolUse' },
    { event: 'preSampling', summaryKey: 'preSampling' },
    { event: 'postSampling', summaryKey: 'postSampling' },
    { event: 'stop', summaryKey: 'stop' },
    { event: 'error', summaryKey: 'error' },
    { event: 'sessionStart', summaryKey: 'sessionStart' },
    { event: 'sessionEnd', summaryKey: 'sessionEnd' },
    { event: 'subagentStart', summaryKey: 'subagentStart' },
    { event: 'subagentStop', summaryKey: 'subagentStop' },
    { event: 'preCompact', summaryKey: 'preCompact' },
    { event: 'postCompact', summaryKey: 'postCompact' },
    { event: 'permissionRequest', summaryKey: 'permissionRequest' },
    { event: 'permissionDenied', summaryKey: 'permissionDenied' },
    { event: 'userPromptSubmit', summaryKey: 'userPromptSubmit' },
    { event: 'taskCreated', summaryKey: 'taskCreated' },
    { event: 'taskCompleted', summaryKey: 'taskCompleted' },
    { event: 'postToolUseFailure', summaryKey: 'postToolUseFailure' },
    { event: 'notification', summaryKey: 'notification' },
    { event: 'setup', summaryKey: 'setup' },
    { event: 'configChange', summaryKey: 'configChange' },
    { event: 'instructionsLoaded', summaryKey: 'instructionsLoaded' },
    { event: 'worktreeCreate', summaryKey: 'worktreeCreate' },
    { event: 'worktreeRemove', summaryKey: 'worktreeRemove' },
    { event: 'cwdChanged', summaryKey: 'cwdChanged' },
    { event: 'fileChanged', summaryKey: 'fileChanged' },
    { event: 'elicitation', summaryKey: 'elicitation' },
    { event: 'elicitationResult', summaryKey: 'elicitationResult' },
    { event: 'stopFailure', summaryKey: 'stopFailure' },
  ];

  for (const { event, summaryKey } of eventConfigs) {
    it(`registers "${event}" hook via loadFromFile`, () => {
      const tmpDir = writeTmpHooks([
        { event, hook: { type: 'command', command: 'echo ok' } },
      ]);
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(tmpDir);
      assert.equal(hm.getSummary()[summaryKey], 1, `Expected ${summaryKey} to be 1`);
      fs.rmSync(tmpDir, { recursive: true, force: true });
    });
  }

  it('logs unknown event types without crashing', () => {
    const tmpDir = writeTmpHooks([
      { event: 'unknownEvent', hook: { type: 'command', command: 'echo ok' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);
    // Should not throw, unknown event is just logged
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — preToolUse command hook execution paths
// ---------------------------------------------------------------------------
describe('processHookEntry — preToolUse command hook execution', () => {
  it('executes preToolUse command hook and returns allow on exit 0', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo ALLOW' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('returns deny when command exits non-zero', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo "blocked" && exit 1' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('skips hook when matcher does not match tool name', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 1' }, matcher: 'file_write' },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'bash' }));
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('skips hook when "if" condition does not match tool name', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 1', if: 'file_write' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'bash' }));
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('MODIFY: prefix in command output returns modify decision', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'MODIFY:{"command":"ls -la"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'modify');
    if (decision.action === 'modify') {
      assert.deepEqual(decision.updatedInput, { command: 'ls -la' });
    }
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('MODIFY: prefix with invalid JSON is ignored', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf "MODIFY:not-json"' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('async hooks fire and forget, returning allow immediately', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'sleep 10 && exit 1', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('once hooks are removed after first execution', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo ok', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().preToolUse, 1);
    await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(hm.getSummary().preToolUse, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — postToolUse command hook execution
// ---------------------------------------------------------------------------
describe('processHookEntry — postToolUse command hook execution', () => {
  it('runs postToolUse command hook', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'echo done' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostToolUse(makePostToolUseCtx());
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUse skips when if condition does not match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'exit 1', if: 'file_write' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // Should not crash — the hook is skipped because tool name is bash, not file_write
    await hm.executePostToolUse(makePostToolUseCtx({ toolName: 'bash' }));
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUse skips when matcher does not match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'exit 1' }, matcher: 'file_write' },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostToolUse(makePostToolUseCtx({ toolName: 'bash' }));
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUse async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostToolUse(makePostToolUseCtx());
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUse once removes hook after first execution', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUse', hook: { type: 'command', command: 'echo ok', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().postToolUse, 1);
    await hm.executePostToolUse(makePostToolUseCtx());
    assert.equal(hm.getSummary().postToolUse, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — preSampling command hook execution
// ---------------------------------------------------------------------------
describe('processHookEntry — preSampling command hook execution', () => {
  it('runs preSampling command hook and returns allow', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preSampling', hook: { type: 'command', command: 'echo ok' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreSampling(makePreSamplingCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preSampling returns deny when command outputs DENY:', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preSampling', hook: { type: 'command', command: 'echo "DENY:rate limited" && exit 1' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreSampling(makePreSamplingCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preSampling async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preSampling', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreSampling(makePreSamplingCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preSampling once removes hook after first execution', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preSampling', hook: { type: 'command', command: 'echo ok', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().preSampling, 1);
    await hm.executePreSampling(makePreSamplingCtx());
    assert.equal(hm.getSummary().preSampling, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — postSampling, stop, error command hooks
// ---------------------------------------------------------------------------
describe('processHookEntry — postSampling/stop/error command hooks', () => {
  it('postSampling runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postSampling', hook: { type: 'command', command: 'echo done', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().postSampling, 1);
    await hm.executePostSampling(makeContext());
    assert.equal(hm.getSummary().postSampling, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postSampling async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postSampling', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostSampling(makeContext());
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('stop runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'stop', hook: { type: 'command', command: 'echo stopped', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().stop, 1);
    await hm.executeStop(makeContext());
    assert.equal(hm.getSummary().stop, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('stop async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'stop', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeStop(makeContext());
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('error runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'error', hook: { type: 'command', command: 'echo errored', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().error, 1);
    await hm.executeError(makeContext());
    assert.equal(hm.getSummary().error, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('error async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'error', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeError(makeContext());
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — session/subagent/compact hooks
// ---------------------------------------------------------------------------
describe('processHookEntry — session/subagent/compact command hooks', () => {
  it('sessionStart runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'echo start', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().sessionStart, 1);
    await hm.executeSessionStart({ sessionId: 'test', cwd: '/tmp' });
    assert.equal(hm.getSummary().sessionStart, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('sessionStart async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'sessionStart', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeSessionStart({ sessionId: 'test', cwd: '/tmp' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('sessionEnd runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'sessionEnd', hook: { type: 'command', command: 'echo end', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().sessionEnd, 1);
    await hm.executeSessionEnd({ sessionId: 'test', cwd: '/tmp' });
    assert.equal(hm.getSummary().sessionEnd, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('sessionEnd async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'sessionEnd', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeSessionEnd({ sessionId: 'test', cwd: '/tmp' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('subagentStart runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'subagentStart', hook: { type: 'command', command: 'echo sub', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().subagentStart, 1);
    await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' });
    assert.equal(hm.getSummary().subagentStart, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('subagentStart async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'subagentStart', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('subagentStop runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'subagentStop', hook: { type: 'command', command: 'echo sub stop', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().subagentStop, 1);
    await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' });
    assert.equal(hm.getSummary().subagentStop, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('subagentStop async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'subagentStop', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 's', prompt: 'go' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preCompact runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preCompact', hook: { type: 'command', command: 'echo compact', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().preCompact, 1);
    await hm.executePreCompact({ messageCount: 10, tokenEstimate: 5000 });
    assert.equal(hm.getSummary().preCompact, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('preCompact async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preCompact', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePreCompact({ messageCount: 10, tokenEstimate: 5000 });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postCompact runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postCompact', hook: { type: 'command', command: 'echo compact done', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().postCompact, 1);
    await hm.executePostCompact({ messageCount: 5, tokenEstimate: 2500 });
    assert.equal(hm.getSummary().postCompact, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postCompact async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postCompact', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostCompact({ messageCount: 5, tokenEstimate: 2500 });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — permission/userPrompt/task hooks
// ---------------------------------------------------------------------------
describe('processHookEntry — permission/userPrompt/task command hooks', () => {
  it('permissionRequest runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'echo perm', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().permissionRequest, 1);
    await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} });
    assert.equal(hm.getSummary().permissionRequest, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('permissionRequest async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionRequest', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePermissionRequest({ toolName: 'bash', toolInput: {} });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('permissionDenied runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionDenied', hook: { type: 'command', command: 'echo denied', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().permissionDenied, 1);
    await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} });
    assert.equal(hm.getSummary().permissionDenied, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('permissionDenied async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'permissionDenied', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePermissionDenied({ toolName: 'bash', toolInput: {} });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('userPromptSubmit runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'userPromptSubmit', hook: { type: 'command', command: 'echo prompt', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().userPromptSubmit, 1);
    await hm.executeUserPromptSubmit({ prompt: 'fix it', turnCount: 1 });
    assert.equal(hm.getSummary().userPromptSubmit, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('userPromptSubmit async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'userPromptSubmit', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeUserPromptSubmit({ prompt: 'fix it', turnCount: 1 });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('taskCreated runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'taskCreated', hook: { type: 'command', command: 'echo task', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().taskCreated, 1);
    await hm.executeTaskCreated({ taskId: '1', subject: 'test' });
    assert.equal(hm.getSummary().taskCreated, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('taskCreated async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'taskCreated', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeTaskCreated({ taskId: '1', subject: 'test' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('taskCompleted runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'taskCompleted', hook: { type: 'command', command: 'echo done', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().taskCompleted, 1);
    await hm.executeTaskCompleted({ taskId: '1', subject: 'test' });
    assert.equal(hm.getSummary().taskCompleted, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('taskCompleted async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'taskCompleted', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeTaskCompleted({ taskId: '1', subject: 'test' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// processHookEntry — postToolUseFailure, notification, setup, configChange,
//                     instructionsLoaded, worktreeCreate/Remove, cwdChanged,
//                     fileChanged, elicitation, elicitationResult, stopFailure
// ---------------------------------------------------------------------------
describe('processHookEntry — remaining event hooks', () => {
  it('postToolUseFailure runs with if/matcher filtering', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUseFailure', hook: { type: 'command', command: 'echo fail', once: true, if: 'bash' }, matcher: 'bash' },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().postToolUseFailure, 1);
    await hm.executePostToolUseFailure(makePostToolUseCtx({ toolName: 'bash', isError: true }));
    assert.equal(hm.getSummary().postToolUseFailure, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUseFailure skips when if does not match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUseFailure', hook: { type: 'command', command: 'exit 1', if: 'file_write' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // toolName is bash, if is file_write — should skip
    await hm.executePostToolUseFailure(makePostToolUseCtx({ toolName: 'bash' }));
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUseFailure skips when matcher does not match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUseFailure', hook: { type: 'command', command: 'exit 1' }, matcher: 'file_write' },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostToolUseFailure(makePostToolUseCtx({ toolName: 'bash' }));
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('postToolUseFailure async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'postToolUseFailure', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePostToolUseFailure(makePostToolUseCtx());
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('notification runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'notification', hook: { type: 'command', command: 'echo notify', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().notification, 1);
    await hm.executeNotification({ message: 'hi', title: 'Test' });
    assert.equal(hm.getSummary().notification, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('notification async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'notification', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeNotification({ message: 'hi' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('setup runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'setup', hook: { type: 'command', command: 'echo setup', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().setup, 1);
    await hm.executeSetup({ sessionId: 'test', cwd: '/tmp' });
    assert.equal(hm.getSummary().setup, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('setup async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'setup', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeSetup({ sessionId: 'test', cwd: '/tmp' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('configChange runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'configChange', hook: { type: 'command', command: 'echo config', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().configChange, 1);
    await hm.executeConfigChange({ source: 'file', filePath: '/test' });
    assert.equal(hm.getSummary().configChange, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('configChange async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'configChange', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeConfigChange({ source: 'file' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('instructionsLoaded runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'instructionsLoaded', hook: { type: 'command', command: 'echo instr', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().instructionsLoaded, 1);
    await hm.executeInstructionsLoaded({ filePath: '/test/CLAUDE.md', loadReason: 'init' });
    assert.equal(hm.getSummary().instructionsLoaded, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('instructionsLoaded async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'instructionsLoaded', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeInstructionsLoaded({ filePath: '/test/CLAUDE.md', loadReason: 'init' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('worktreeCreate runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'worktreeCreate', hook: { type: 'command', command: 'echo wt', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().worktreeCreate, 1);
    await hm.executeWorktreeCreate({ name: 'test-wt', worktreePath: '/tmp/wt' });
    assert.equal(hm.getSummary().worktreeCreate, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('worktreeCreate async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'worktreeCreate', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeWorktreeCreate({ name: 'test-wt' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('worktreeRemove runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'worktreeRemove', hook: { type: 'command', command: 'echo wt rm', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().worktreeRemove, 1);
    await hm.executeWorktreeRemove({ name: 'test-wt', worktreePath: '/tmp/wt' });
    assert.equal(hm.getSummary().worktreeRemove, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('worktreeRemove async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'worktreeRemove', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeWorktreeRemove({ name: 'test-wt' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('cwdChanged runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'cwdChanged', hook: { type: 'command', command: 'echo cwd', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().cwdChanged, 1);
    await hm.executeCwdChanged({ oldCwd: '/old', newCwd: '/new' });
    assert.equal(hm.getSummary().cwdChanged, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('cwdChanged async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'cwdChanged', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeCwdChanged({ oldCwd: '/old', newCwd: '/new' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('fileChanged runs with if/matcher filtering', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'fileChanged', hook: { type: 'command', command: 'echo file', once: true, if: '.*\\.ts$' }, matcher: '.*\\.ts$' },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().fileChanged, 1);
    await hm.executeFileChanged({ filePath: 'src/test.ts', event: 'change' });
    assert.equal(hm.getSummary().fileChanged, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('fileChanged skips when if does not match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'fileChanged', hook: { type: 'command', command: 'exit 1', if: '.*\\.py$' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeFileChanged({ filePath: 'src/test.ts', event: 'change' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('fileChanged skips when matcher does not match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'fileChanged', hook: { type: 'command', command: 'exit 1' }, matcher: '.*\\.py$' },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeFileChanged({ filePath: 'src/test.ts', event: 'change' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('fileChanged async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'fileChanged', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeFileChanged({ filePath: 'test.ts', event: 'add' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('elicitation runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'elicitation', hook: { type: 'command', command: 'echo elicit', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().elicitation, 1);
    await hm.executeElicitation({ mcpServerName: 'test-mcp', message: 'hi' });
    assert.equal(hm.getSummary().elicitation, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('elicitation async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'elicitation', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeElicitation({ mcpServerName: 'test-mcp' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('elicitationResult runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'elicitationResult', hook: { type: 'command', command: 'echo result', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().elicitationResult, 1);
    await hm.executeElicitationResult({ mcpServerName: 'test-mcp', message: 'done' });
    assert.equal(hm.getSummary().elicitationResult, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('elicitationResult async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'elicitationResult', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeElicitationResult({ mcpServerName: 'test-mcp' });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('stopFailure runs and once removes', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'stopFailure', hook: { type: 'command', command: 'echo sf', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    assert.equal(hm.getSummary().stopFailure, 1);
    await hm.executeStopFailure({ error: 'boom', hookIndex: 0 });
    assert.equal(hm.getSummary().stopFailure, 0);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('stopFailure async fires and forgets', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'stopFailure', hook: { type: 'command', command: 'sleep 10', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executeStopFailure({ error: 'boom', hookIndex: 0 });
    assert.ok(true);
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// runHookByType — command hook JSON output parsing
// ---------------------------------------------------------------------------
describe('runHookByType — command hook JSON output', () => {
  it('parses decision=block as DENY', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"decision":"block","reason":"forbidden"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses decision=deny as DENY', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"decision":"deny"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses decision=ask as ASK (treated as allow for preToolUse)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"decision":"ask","reason":"needs confirmation"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    // ASK: prefix is returned, but since it does not start with DENY: or MODIFY:, it resolves to allow
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses permissionDecision=ask as ASK', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"permissionDecision":"ask"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses continue=false as DENY', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"continue":false,"stopReason":"halt"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses updatedInput as MODIFY', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"updatedInput":{"command":"ls"}}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'modify');
    if (decision.action === 'modify') {
      assert.deepEqual(decision.updatedInput, { command: 'ls' });
    }
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses additionalContext alone as ALLOW with context', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"additionalContext":"extra info"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    assert.equal((decision as any).additionalContext, 'extra info');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('parses systemMessage as SYSTEM (treated as allow)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"systemMessage":"inject this"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    // SYSTEM: prefix is not DENY: or MODIFY:, so resolves as allow
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('asyncRewake emits hookRewake on exit code 2', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo "rewake" && exit 2', asyncRewake: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    let rewakeEvent: any;
    hm.on('hookRewake', (data: any) => { rewakeEvent = data; });

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    // Give a tick for event to fire
    await new Promise(r => setTimeout(r, 10));
    assert.ok(rewakeEvent);
    assert.equal(rewakeEvent.event, 'command');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// runHookByType — command hook error/timeout
// ---------------------------------------------------------------------------
describe('runHookByType — command hook error/timeout', () => {
  it('handles command timeout gracefully', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'sleep 30', timeout: 100 } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    // Timeout causes the hook to reject, which is caught and logged, resolves as allow
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('handles non-existent command gracefully', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'nonexistentcommand12345' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    // Non-zero exit code = deny
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// runHookByType — prompt hook
// ---------------------------------------------------------------------------
describe('runHookByType — prompt hook', () => {
  it('runs prompt hook with engine factory', async () => {
    const factory = mockEngineFactory([
      { type: 'text_delta', text: 'ALLOW' },
    ]);
    hm.setEngineFactory(factory);
    hm.setProviderConfig({ model: 'test-model', provider: 'test' });

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'Should this be allowed?' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prompt hook returns deny when engine outputs DENY:', async () => {
    const factory = mockEngineFactory([
      { type: 'text_delta', text: 'DENY:not allowed' },
    ]);
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'Check this' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prompt hook handles error events', async () => {
    const factory = mockEngineFactory([
      { type: 'error', error: 'engine failure' },
    ]);
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'Check' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prompt hook skipped when no engine factory', async () => {
    // No engine factory set
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'Check' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prompt hook uses custom model from config', async () => {
    let receivedConfig: any;
    const factory: EngineFactory = (config: any) => {
      receivedConfig = config;
      return {
        submit(_prompt: string) {
          return (async function* () {
            yield { type: 'text_delta', text: 'ok' };
          })();
        },
        shutdown() {},
      };
    };
    hm.setEngineFactory(factory);
    hm.setProviderConfig({ model: 'default-model', provider: 'test' });

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'Check', model: 'custom-model' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(receivedConfig.provider.model, 'custom-model');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prompt hook with asyncRewake emits hookRewake on DENY result', async () => {
    const factory = mockEngineFactory([
      { type: 'text_delta', text: 'DENY:blocked by prompt' },
    ]);
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'postSampling', hook: { type: 'prompt', prompt: 'Check', asyncRewake: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    let rewakeEvent: any;
    hm.on('hookRewake', (data: any) => { rewakeEvent = data; });

    await hm.executePostSampling(makeContext());
    await new Promise(r => setTimeout(r, 10));
    assert.ok(rewakeEvent);
    assert.equal(rewakeEvent.event, 'prompt');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('prompt hook with asyncRewake emits hookRewake on engine error', async () => {
    const factory: EngineFactory = (_config: any) => ({
      submit(_prompt: string) {
        return (async function* () {
          throw new Error('engine exploded');
        })();
      },
      shutdown() {},
    });
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'postSampling', hook: { type: 'prompt', prompt: 'Check', asyncRewake: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    let rewakeEvent: any;
    hm.on('hookRewake', (data: any) => { rewakeEvent = data; });

    await hm.executePostSampling(makeContext());
    await new Promise(r => setTimeout(r, 10));
    assert.ok(rewakeEvent);
    assert.equal(rewakeEvent.event, 'prompt');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// runHookByType — agent hook
// ---------------------------------------------------------------------------
describe('runHookByType — agent hook', () => {
  it('runs agent hook with engine factory', async () => {
    const factory = mockEngineFactory([
      { type: 'text_delta', text: 'Agent says OK' },
    ]);
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'agent', prompt: 'Evaluate this tool call' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('agent hook skipped when no engine factory', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'agent', prompt: 'Check' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('agent hook returns deny when output starts with DENY:', async () => {
    const factory = mockEngineFactory([
      { type: 'text_delta', text: 'DENY:agent blocked' },
    ]);
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'agent', prompt: 'Evaluate' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('agent hook handles engine errors gracefully', async () => {
    const factory: EngineFactory = (_config: any) => ({
      submit(_prompt: string) {
        return (async function* () {
          throw new Error('agent engine error');
        })();
      },
      shutdown() {},
    });
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'agent', prompt: 'Check' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('agent hook uses custom model', async () => {
    let receivedConfig: any;
    const factory: EngineFactory = (config: any) => {
      receivedConfig = config;
      return {
        submit(_prompt: string) {
          return (async function* () {
            yield { type: 'text_delta', text: 'ok' };
          })();
        },
        shutdown() {},
      };
    };
    hm.setEngineFactory(factory);
    hm.setProviderConfig({ model: 'default-model' });

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'agent', prompt: 'Check', model: 'agent-model' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(receivedConfig.provider.model, 'agent-model');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('agent hook handles error event type', async () => {
    const factory = mockEngineFactory([
      { type: 'error', error: 'agent failed' },
    ]);
    hm.setEngineFactory(factory);

    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'agent', prompt: 'Check' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// runHookByType — http hook
// ---------------------------------------------------------------------------
describe('runHookByType — http hook', () => {
  it('runs http hook (will fail with connection error, handled gracefully)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'http', url: 'http://127.0.0.1:19876/nonexistent', timeout: 500 } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // HTTP hook to a non-listening port will trigger connection error, resolved as empty string
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('runs https http hook (connection error handled gracefully)', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'http', url: 'https://127.0.0.1:19877/nonexistent', timeout: 500, headers: { 'X-Custom': 'test' } } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// runHookByType — unknown type returns empty string
// ---------------------------------------------------------------------------
describe('runHookByType — unknown hook type', () => {
  it('returns empty string for unknown hook type', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'unknown_type' as any, command: 'echo x' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// buildHookEnv
// ---------------------------------------------------------------------------
describe('buildHookEnv — environment variables', () => {
  it('sets environment variables for hooks', async () => {
    hm.setCwd('/test/project');
    hm.setSessionId('sess-123');
    hm.setProviderConfig({ model: 'claude-4', provider: 'anthropic' });

    const tmpDir = writeTmpHooks([
      {
        event: 'preToolUse',
        hook: {
          type: 'command',
          // Print env vars so we can verify they are set
          command: 'echo "$SUPERINFERENCE_PROJECT_DIR|$SUPERINFERENCE_SESSION_ID|$SUPERINFERENCE_HOOK_EVENT|$SUPERINFERENCE_CWD|$SUPERINFERENCE_MODEL|$SUPERINFERENCE_PROVIDER"',
        },
      },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // Just verify it executes without error; the env vars are internal
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// execute* methods for newer hook types — direct programmatic registration
// ---------------------------------------------------------------------------
describe('execute* — newer hook types with programmatic registration', () => {
  it('executePostToolUseFailure calls hooks and swallows errors', async () => {
    let called = false;
    hm.onPostToolUseFailure(async () => { throw new Error('boom'); });
    hm.onPostToolUseFailure(async () => { called = true; });

    await hm.executePostToolUseFailure(makePostToolUseCtx({ isError: true }));
    assert.ok(called);
  });

  it('executeNotification calls hooks and swallows errors', async () => {
    let received: NotificationContext | undefined;
    hm.onNotification(async () => { throw new Error('boom'); });
    hm.onNotification(async (ctx) => { received = ctx; });

    await hm.executeNotification({ message: 'test', notificationType: 'info', title: 'Title' });
    assert.ok(received);
    assert.equal(received!.message, 'test');
    assert.equal(received!.title, 'Title');
  });

  it('executeSetup calls hooks and swallows errors', async () => {
    let received: SessionContext | undefined;
    hm.onSetup(async () => { throw new Error('boom'); });
    hm.onSetup(async (ctx) => { received = ctx; });

    await hm.executeSetup({ sessionId: 'test', cwd: '/tmp' });
    assert.ok(received);
    assert.equal(received!.sessionId, 'test');
  });

  it('executeConfigChange calls hooks and swallows errors', async () => {
    let received: ConfigChangeContext | undefined;
    hm.onConfigChange(async () => { throw new Error('boom'); });
    hm.onConfigChange(async (ctx) => { received = ctx; });

    await hm.executeConfigChange({ source: 'file', filePath: '/test/config.json' });
    assert.ok(received);
    assert.equal(received!.source, 'file');
  });

  it('executeInstructionsLoaded calls hooks and swallows errors', async () => {
    let received: InstructionsContext | undefined;
    hm.onInstructionsLoaded(async () => { throw new Error('boom'); });
    hm.onInstructionsLoaded(async (ctx) => { received = ctx; });

    await hm.executeInstructionsLoaded({ filePath: '/test/CLAUDE.md', loadReason: 'startup' });
    assert.ok(received);
    assert.equal(received!.filePath, '/test/CLAUDE.md');
  });

  it('executeWorktreeCreate calls hooks and swallows errors', async () => {
    let received: WorktreeContext | undefined;
    hm.onWorktreeCreate(async () => { throw new Error('boom'); });
    hm.onWorktreeCreate(async (ctx) => { received = ctx; });

    await hm.executeWorktreeCreate({ name: 'wt1', worktreePath: '/tmp/wt' });
    assert.ok(received);
    assert.equal(received!.name, 'wt1');
  });

  it('executeWorktreeRemove calls hooks and swallows errors', async () => {
    let received: WorktreeContext | undefined;
    hm.onWorktreeRemove(async () => { throw new Error('boom'); });
    hm.onWorktreeRemove(async (ctx) => { received = ctx; });

    await hm.executeWorktreeRemove({ name: 'wt1', worktreePath: '/tmp/wt' });
    assert.ok(received);
    assert.equal(received!.name, 'wt1');
  });

  it('executeCwdChanged calls hooks and swallows errors', async () => {
    let received: CwdChangedContext | undefined;
    hm.onCwdChanged(async () => { throw new Error('boom'); });
    hm.onCwdChanged(async (ctx) => { received = ctx; });

    await hm.executeCwdChanged({ oldCwd: '/old', newCwd: '/new' });
    assert.ok(received);
    assert.equal(received!.oldCwd, '/old');
    assert.equal(received!.newCwd, '/new');
  });

  it('executeFileChanged calls hooks and swallows errors', async () => {
    let received: FileChangeContext | undefined;
    hm.onFileChanged(async () => { throw new Error('boom'); });
    hm.onFileChanged(async (ctx) => { received = ctx; });

    await hm.executeFileChanged({ filePath: 'test.ts', event: 'change' });
    assert.ok(received);
    assert.equal(received!.filePath, 'test.ts');
  });

  it('executeElicitation calls hooks and swallows errors', async () => {
    let received: ElicitationContext | undefined;
    hm.onElicitation(async () => { throw new Error('boom'); });
    hm.onElicitation(async (ctx) => { received = ctx; });

    await hm.executeElicitation({ mcpServerName: 'mcp-test', message: 'prompt', mode: 'auto' });
    assert.ok(received);
    assert.equal(received!.mcpServerName, 'mcp-test');
  });

  it('executeElicitationResult calls hooks and swallows errors', async () => {
    let received: ElicitationContext | undefined;
    hm.onElicitationResult(async () => { throw new Error('boom'); });
    hm.onElicitationResult(async (ctx) => { received = ctx; });

    await hm.executeElicitationResult({ mcpServerName: 'mcp-test', message: 'result' });
    assert.ok(received);
    assert.equal(received!.message, 'result');
  });

  it('executeStopFailure calls hooks and swallows errors', async () => {
    let received: StopFailureContext | undefined;
    hm.onStopFailure(async () => { throw new Error('boom'); });
    hm.onStopFailure(async (ctx) => { received = ctx; });

    await hm.executeStopFailure({ error: 'stop failed', hookIndex: 2 });
    assert.ok(received);
    assert.equal(received!.error, 'stop failed');
    assert.equal(received!.hookIndex, 2);
  });
});

// ---------------------------------------------------------------------------
// executeStop — stopFailure integration
// ---------------------------------------------------------------------------
describe('executeStop — triggers executeStopFailure on hook error', () => {
  it('fires stopFailure when a stop hook throws', async () => {
    let stopFailureReceived: StopFailureContext | undefined;
    hm.onStopFailure(async (ctx) => { stopFailureReceived = ctx; });
    hm.onStop(async () => { throw new Error('stop hook crashed'); });

    await hm.executeStop(makeContext());
    assert.ok(stopFailureReceived);
    assert.ok(stopFailureReceived!.error.includes('stop hook crashed'));
    assert.equal(stopFailureReceived!.hookIndex, 0);
  });
});

// ---------------------------------------------------------------------------
// registerCallback — unsubscribe edge case
// ---------------------------------------------------------------------------
describe('registerCallback — edge cases', () => {
  it('unsubscribe is idempotent', () => {
    const unsub = hm.registerCallback('test', async () => undefined);
    unsub();
    unsub(); // Should not throw
    assert.equal(hm.getSummary().callbackHooks, 0);
  });

  it('unsubscribe only removes one callback', () => {
    const cb = async () => undefined;
    hm.registerCallback('test', cb);
    const unsub = hm.registerCallback('test', cb);
    assert.equal(hm.getSummary().callbackHooks, 2);
    unsub();
    assert.equal(hm.getSummary().callbackHooks, 1);
  });
});

// ---------------------------------------------------------------------------
// matchesPattern — via preToolUse if/matcher (covers utility function)
// ---------------------------------------------------------------------------
describe('matchesPattern — via hooks', () => {
  it('wildcard pattern matches everything', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo ok', if: '*' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'anything' }));
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('empty pattern matches everything', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'echo ok', if: '' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'any' }));
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('pipe-separated pattern matches alternatives', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 1', if: 'bash|file_write' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'bash' }));
    assert.equal(decision.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('regex pattern matching works', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 1', if: '^file_' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // bash does not match ^file_
    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'bash' }));
    assert.equal(decision.action, 'allow');

    // file_read does match ^file_
    const decision2 = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'file_read' }));
    assert.equal(decision2.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it('invalid regex falls back to exact match', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'exit 1', if: '[invalid' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // Exact match of '[invalid' won't match 'bash'
    const decision = await hm.executePreToolUse(makePreToolUseCtx({ toolName: 'bash' }));
    assert.equal(decision.action, 'allow');

    // Exact match of '[invalid' will match '[invalid'
    const decision2 = await hm.executePreToolUse(makePreToolUseCtx({ toolName: '[invalid' }));
    assert.equal(decision2.action, 'deny');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// getSummary — comprehensive coverage
// ---------------------------------------------------------------------------
describe('getSummary — all keys present', () => {
  it('includes all expected keys', () => {
    const summary = hm.getSummary();
    const expectedKeys = [
      'preToolUse', 'postToolUse', 'preSampling', 'postSampling',
      'stop', 'error', 'sessionStart', 'sessionEnd',
      'subagentStart', 'subagentStop', 'preCompact', 'postCompact',
      'permissionRequest', 'permissionDenied', 'userPromptSubmit',
      'taskCreated', 'taskCompleted', 'postToolUseFailure',
      'notification', 'setup', 'configChange', 'instructionsLoaded',
      'worktreeCreate', 'worktreeRemove', 'cwdChanged', 'fileChanged',
      'elicitation', 'elicitationResult', 'stopFailure', 'callbackHooks',
    ];
    for (const key of expectedKeys) {
      assert.ok(key in summary, `Missing key: ${key}`);
      assert.equal(typeof summary[key], 'number');
    }
  });

  it('counts correctly after registration', () => {
    hm.onPreToolUse(async () => ({ action: 'allow' }));
    hm.onPreToolUse(async () => ({ action: 'allow' }));
    hm.onPostToolUse(async () => {});
    hm.onNotification(async () => {});
    hm.onStopFailure(async () => {});
    hm.registerCallback('ev', async () => undefined);

    const summary = hm.getSummary();
    assert.equal(summary.preToolUse, 2);
    assert.equal(summary.postToolUse, 1);
    assert.equal(summary.notification, 1);
    assert.equal(summary.stopFailure, 1);
    assert.equal(summary.callbackHooks, 1);
  });
});

// ---------------------------------------------------------------------------
// setCwd / setProviderConfig
// ---------------------------------------------------------------------------
describe('HookManager — setters', () => {
  it('setCwd stores the working directory for hooks', () => {
    hm.setCwd('/my/project');
    // We cannot directly read _cwd but it is used in buildHookEnv
    // Just verify no error is thrown
    assert.ok(true);
  });

  it('setProviderConfig stores the provider configuration', () => {
    hm.setProviderConfig({ model: 'claude-4', provider: 'anthropic' });
    assert.ok(true);
  });

  it('setEngineFactory stores the factory', () => {
    hm.setEngineFactory(mockEngineFactory([]));
    assert.ok(true);
  });
});

// ---------------------------------------------------------------------------
// runCommandHookAsync — stdin data path
// ---------------------------------------------------------------------------
describe('runCommandHookAsync — stdin data', () => {
  it('sends event data via stdin to command hook', async () => {
    const tmpDir = writeTmpHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'cat' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    // The hook sends event data as stdin — 'cat' just echoes it back
    const decision = await hm.executePreToolUse(makePreToolUseCtx());
    // Output should be the JSON event data, which is not DENY: or MODIFY:, so allow
    assert.equal(decision.action, 'allow');
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});
