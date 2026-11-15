/**
 * Hooks parity — second wiring pass (hooks2 gap file). Each block closes a
 * confirmed Claude Code parity gap that is closeable inside core/core/src/hooks.ts,
 * with the red-green discriminator noted inline:
 *
 *  - Matcher scoping on the session-lifecycle + stopFailure events via their
 *    per-event discriminator field (HOOK_MATCHER_FIELD: sessionStart→source,
 *    sessionEnd→reason, stopFailure→error). Before the fix the `matcher` was
 *    IGNORED for these events, so a scoped hook fired for ALL occurrences.
 *    [gaps 26, 27, 41, 42 — in-file part]
 *  - SessionStart `source` / SessionEnd `reason` discriminator fields are carried
 *    on SessionContext and serialized into the hook stdin payload. [gaps 41, 42]
 *  - Hook-agent disallowed-tool superset: a hook agent cannot spawn subagents,
 *    launch a workflow, enter plan mode, block on AskUserQuestion, or touch the
 *    parent's background tasks (HOOK_AGENT_DISALLOWED_TOOLS). [gaps 13, 14]
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  HOOK_AGENT_DISALLOWED_TOOLS,
  HOOK_MATCHER_FIELD,
  type SessionContext,
  type StopFailureContext,
} from '../src/hooks';

function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hp2-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}

function makeManager(dir: string): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  return hm;
}

// ---------------------------------------------------------------------------
// SessionStart: matcher scopes on `source`
// ---------------------------------------------------------------------------
describe('sessionStart hook matcher scopes on source (gaps 26/27/41/42)', () => {
  it('fires only when the source matches the matcher', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'sessionStart', matcher: 'resume', hook: { type: 'command', command: `echo '{"initialUserMessage":"fired"}'` } },
    ]);
    const hm = makeManager(dir);

    // RED without the fix: sessionStart ignored the matcher → this would ALSO
    // return {initialUserMessage:'fired'} on a non-matching source.
    const startup = await hm.executeSessionStart({ sessionId: 's', cwd: dir, source: 'startup' });
    assert.equal(startup, undefined, 'hook must NOT fire when source != matcher');

    const resume = await hm.executeSessionStart({ sessionId: 's', cwd: dir, source: 'resume' });
    assert.equal(resume?.initialUserMessage, 'fired', 'hook must fire when source == matcher');
  });

  it('a session hook with NO matcher still fires for every source', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'sessionStart', hook: { type: 'command', command: `echo '{"initialUserMessage":"always"}'` } },
    ]);
    const hm = makeManager(dir);
    const r = await hm.executeSessionStart({ sessionId: 's', cwd: dir, source: 'anything' });
    assert.equal(r?.initialUserMessage, 'always');
  });

  it('serializes the source field into the hook stdin payload', async () => {
    const payload = 'start-payload.json';
    const dir = tmpCwdWithHooks([
      { event: 'sessionStart', hook: { type: 'command', command: `cat > ${payload}` } },
    ]);
    const hm = makeManager(dir);
    await hm.executeSessionStart({ sessionId: 's1', cwd: dir, source: 'compact' });
    const written = fs.readFileSync(path.join(dir, payload), 'utf-8');
    const parsed = JSON.parse(written);
    assert.equal(parsed.source, 'compact');
    assert.equal(parsed.hook_event_name, 'SessionStart');
  });
});

// ---------------------------------------------------------------------------
// SessionEnd: matcher scopes on `reason`
// ---------------------------------------------------------------------------
describe('sessionEnd hook matcher scopes on reason (gaps 26/27/41/42)', () => {
  it('fires only when the reason matches the matcher', async () => {
    const marker = 'ended.txt';
    const dir = tmpCwdWithHooks([
      { event: 'sessionEnd', matcher: 'clear', hook: { type: 'command', command: `touch ${marker}` } },
    ]);
    const hm = makeManager(dir);
    const markerPath = path.join(dir, marker);

    // RED without the fix: sessionEnd ignored the matcher → marker created here.
    await hm.executeSessionEnd({ sessionId: 's', cwd: dir, reason: 'logout' } as SessionContext);
    assert.equal(fs.existsSync(markerPath), false, 'hook must NOT fire when reason != matcher');

    await hm.executeSessionEnd({ sessionId: 's', cwd: dir, reason: 'clear' } as SessionContext);
    assert.equal(fs.existsSync(markerPath), true, 'hook must fire when reason == matcher');
  });

  it('serializes the reason field into the hook stdin payload', async () => {
    const payload = 'end-payload.json';
    const dir = tmpCwdWithHooks([
      { event: 'sessionEnd', hook: { type: 'command', command: `cat > ${payload}` } },
    ]);
    const hm = makeManager(dir);
    await hm.executeSessionEnd({ sessionId: 's2', cwd: dir, reason: 'prompt_input_exit' } as SessionContext);
    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal(parsed.reason, 'prompt_input_exit');
    assert.equal(parsed.hook_event_name, 'SessionEnd');
  });
});

// ---------------------------------------------------------------------------
// StopFailure: matcher scopes on `error`
// ---------------------------------------------------------------------------
describe('stopFailure hook matcher scopes on error (gaps 26/27)', () => {
  it('fires only when the error matches the matcher', async () => {
    const marker = 'failed.txt';
    const dir = tmpCwdWithHooks([
      { event: 'stopFailure', matcher: 'rate_limit', hook: { type: 'command', command: `touch ${marker}` } },
    ]);
    const hm = makeManager(dir);
    const markerPath = path.join(dir, marker);

    // RED without the fix: stopFailure ignored the matcher → marker created here.
    await hm.executeStopFailure({ error: 'authentication_failed', hookIndex: 0 } as StopFailureContext);
    assert.equal(fs.existsSync(markerPath), false, 'hook must NOT fire when error != matcher');

    await hm.executeStopFailure({ error: 'rate_limit', hookIndex: 1 } as StopFailureContext);
    assert.equal(fs.existsSync(markerPath), true, 'hook must fire when error == matcher');
  });
});

// ---------------------------------------------------------------------------
// HOOK_MATCHER_FIELD single-source declares the new discriminator fields
// ---------------------------------------------------------------------------
describe('HOOK_MATCHER_FIELD declares session + stopFailure discriminators', () => {
  it('maps sessionStart→source, sessionEnd→reason, stopFailure→error', () => {
    assert.equal(HOOK_MATCHER_FIELD.sessionStart, 'source');
    assert.equal(HOOK_MATCHER_FIELD.sessionEnd, 'reason');
    assert.equal(HOOK_MATCHER_FIELD.stopFailure, 'error');
  });
});

// ---------------------------------------------------------------------------
// Hook-agent disallowed-tool superset (gaps 13, 14)
// ---------------------------------------------------------------------------
describe('HOOK_AGENT_DISALLOWED_TOOLS superset (gaps 13/14)', () => {
  it('blocks subagent spawning, plan mode, workflows, AskUserQuestion, and parent task tools', () => {
    // RED without the fix: the list was only ['task','plan_mode'], so a hook
    // agent could still launch a workflow, block on AskUserQuestion, or reach
    // the parent's background tasks.
    for (const t of ['task', 'plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill']) {
      assert.ok(HOOK_AGENT_DISALLOWED_TOOLS.includes(t), `must block ${t}`);
    }
  });
});
