/**
 * U007 (w78) — hook payload enrichment + bounded command-hook output.
 *
 * Covers three confirmed parity gaps in core/core/src/hooks.ts:
 *  - PermissionRequest / PermissionDenied hooks receive a `tool_use_id`
 *    correlation id (Claude Code's permission-prompt-tool input schema).
 *  - PostToolUseFailure hooks receive `error`, `is_interrupt` and `tool_use_id`
 *    (Claude Code discriminates a user interrupt from an ordinary error and ties
 *    the failure back to the exact call), plus snake-case `is_error`.
 *  - Command-hook stdout/stderr capture is bounded in memory (head-preserving)
 *    so a hook that floods a stream cannot OOM the agent, mirroring Claude Code's
 *    TaskOutput pipe-mode memory boundary — the decision at the head still parses.
 *
 * All three are driven through the REAL HookManager (on-disk .ami/hooks.json
 * command hooks) so the serialized stdin payload / bounding is proven end to end.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  HookManager,
  serializeHookEvent,
  boundedHookAppend,
  hookOutputTruncationNotice,
  MAX_HOOK_COMMAND_OUTPUT_BYTES,
  type HookConfigEntry,
  type PermissionContext,
  type PostToolUseContext,
} from '../src/hooks';

const tmpDirs: string[] = [];
function newCwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'u007-w78-'));
  tmpDirs.push(d);
  return d;
}
after(() => { for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } } });

function makeManager(cwd: string, entries: HookConfigEntry[]): HookManager {
  const dir = path.join(cwd, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  const mgr = new HookManager();
  mgr.setWorkspaceTrusted(true);
  mgr.setCwd(cwd);
  mgr.loadFromFile(cwd);
  return mgr;
}

// A command hook that writes its entire stdin payload to `capFile`.
function captureHook(event: string, capFile: string): HookConfigEntry {
  return { event, hook: { type: 'command', command: `cat > ${JSON.stringify(capFile)}` } };
}

describe('serializeHookEvent — new snake-case parity keys', () => {
  it('maps toolUseId/isError/isInterrupt to snake_case', () => {
    const line = serializeHookEvent({
      event: 'postToolUseFailure',
      toolName: 'bash',
      isError: true,
      isInterrupt: true,
      error: 'boom',
      toolUseId: 'tu_42',
    });
    const obj = JSON.parse(line);
    assert.equal(obj.tool_use_id, 'tu_42');
    assert.equal(obj.is_error, true);
    assert.equal(obj.is_interrupt, true);
    assert.equal(obj.error, 'boom');
  });

  it('omits tool_use_id when the id is absent', () => {
    const obj = JSON.parse(serializeHookEvent({ event: 'permissionRequest', toolName: 'bash', toolUseId: undefined }));
    assert.ok(!('tool_use_id' in obj), 'undefined id must not emit a key');
  });
});

describe('boundedHookAppend — head-preserving in-memory cap', () => {
  it('appends in full below the cap', () => {
    assert.deepEqual(boundedHookAppend('ab', 'cd', 10), { text: 'abcd', truncated: false });
  });
  it('fills exactly to the cap without truncating', () => {
    assert.deepEqual(boundedHookAppend('ab', 'cd', 4), { text: 'abcd', truncated: false });
  });
  it('truncates the overflowing tail and flags truncation', () => {
    const r = boundedHookAppend('abc', 'defgh', 5);
    assert.equal(r.text, 'abcde');
    assert.equal(r.truncated, true);
  });
  it('is a no-op once already full', () => {
    const r = boundedHookAppend('abcde', 'xyz', 5);
    assert.deepEqual(r, { text: 'abcde', truncated: true });
  });
  it('counts UTF-8 bytes, not code units', () => {
    // '€' is 3 UTF-8 bytes; a 3-byte cap admits the one char but not the ASCII 'x'.
    const r = boundedHookAppend('', '€x', 3);
    assert.equal(r.truncated, true);
    assert.equal(r.text, '€');
    assert.equal(Buffer.byteLength(r.text, 'utf-8') <= 3, true);
  });
  it('notice names the stream + the byte ceiling', () => {
    assert.match(hookOutputTruncationNotice('stdout'), /hook stdout truncated/);
    assert.match(hookOutputTruncationNotice('stderr'), new RegExp(String(MAX_HOOK_COMMAND_OUTPUT_BYTES)));
  });
});

describe('PermissionRequest/Denied hook — tool_use_id correlation (end to end)', () => {
  it('a permissionRequest command hook receives tool_use_id on stdin', async () => {
    const cwd = newCwd();
    const cap = path.join(cwd, 'req.json');
    const mgr = makeManager(cwd, [captureHook('permissionRequest', cap)]);
    const ctx: PermissionContext = { toolName: 'bash', toolInput: { command: 'ls' }, toolUseId: 'toolu_req_1' };
    await mgr.executePermissionRequest(ctx);
    const payload = JSON.parse(fs.readFileSync(cap, 'utf-8'));
    assert.equal(payload.tool_use_id, 'toolu_req_1');
    assert.equal(payload.hook_event_name, 'PermissionRequest');
  });

  it('a permissionDenied command hook receives tool_use_id on stdin', async () => {
    const cwd = newCwd();
    const cap = path.join(cwd, 'deny.json');
    const mgr = makeManager(cwd, [captureHook('permissionDenied', cap)]);
    await mgr.executePermissionDenied({ toolName: 'bash', toolInput: { command: 'ls' }, toolUseId: 'toolu_deny_9' });
    const payload = JSON.parse(fs.readFileSync(cap, 'utf-8'));
    assert.equal(payload.tool_use_id, 'toolu_deny_9');
  });
});

describe('PostToolUseFailure hook — error / is_interrupt / tool_use_id (end to end)', () => {
  it('delivers the failure discriminators on stdin', async () => {
    const cwd = newCwd();
    const cap = path.join(cwd, 'fail.json');
    const mgr = makeManager(cwd, [captureHook('postToolUseFailure', cap)]);
    const ctx: PostToolUseContext = {
      toolName: 'bash',
      toolInput: { command: 'nope' },
      toolOutput: 'command not found',
      isError: true,
      durationMs: 12,
      error: 'command not found',
      isInterrupt: true,
      toolUseId: 'toolu_fail_7',
    };
    await mgr.executePostToolUseFailure(ctx);
    const payload = JSON.parse(fs.readFileSync(cap, 'utf-8'));
    assert.equal(payload.tool_use_id, 'toolu_fail_7');
    assert.equal(payload.error, 'command not found');
    assert.equal(payload.is_interrupt, true);
    assert.equal(payload.is_error, true);
  });
});

describe('bounded command-hook output — a flooding hook is capped in memory', () => {
  it('truncates a ~9MB stderr flood with a notice (exit-2 DENY reason)', async () => {
    const cwd = newCwd();
    // A blocking (exit 2) permissionRequest hook that floods stderr past the cap.
    const flood = `node -e "process.stderr.write('y'.repeat(9000000), () => process.exit(2))"`;
    const mgr = makeManager(cwd, [{ event: 'permissionRequest', hook: { type: 'command', command: flood } }]);
    const res = await mgr.executePermissionRequest({ toolName: 'bash', toolInput: { command: 'ls' }, toolUseId: 'toolu_flood' });
    assert.equal(res?.action, 'deny');
    assert.ok(res?.reason, 'a blocking hook yields a reason from stderr');
    // Bounded: the retained reason never exceeds the cap (+ the short notice), and
    // carries the truncation marker. Without the cap it would be the full ~9MB.
    assert.ok(res!.reason!.includes('hook stderr truncated'), 'truncation notice appended');
    assert.ok(Buffer.byteLength(res!.reason!, 'utf-8') <= MAX_HOOK_COMMAND_OUTPUT_BYTES + 128,
      'retained stderr is bounded to the cap');
  });
});
