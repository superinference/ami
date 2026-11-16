/**
 * U007 / w68 — Stop hook stdin payload parity (Claude Code's StopHookInput).
 *
 * Claude Code passes a Stop hook two fields on stdin so the hook can make a
 * decision without reading the transcript file:
 *   - `stop_hook_active`: true when this Stop is a RE-ENTRANT continuation (a
 *     previous Stop hook blocked turn-end and forced another loop) — a hook reads
 *     it to break a block→retry recursion.
 *   - `last_assistant_message`: the final assistant text of the turn.
 *
 * Before the fix the Stop hook received only `{event:'stop'}` (no recursion flag,
 * no last message). RED discriminator: `parsed.stop_hook_active` is `undefined`
 * and `parsed.last_assistant_message` is absent without the serialize change.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type HookContext } from '../src/hooks';

function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007stop-'));
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

describe('Stop hook stdin payload (U007/w68 — stop_hook_active + last_assistant_message)', () => {
  it('serializes stop_hook_active + last_assistant_message into the hook stdin', async () => {
    const payload = 'stop-payload.json';
    const dir = tmpCwdWithHooks([
      { event: 'stop', hook: { type: 'command', command: `cat > ${payload}` } },
    ]);
    const hm = makeManager(dir);

    const ctx: HookContext = {
      messages: [],
      turnCount: 1,
      stopHookActive: true,
      lastAssistantMessage: { role: 'assistant', content: '  all tests pass  ' },
    };
    const result = await hm.executeStop(ctx);
    assert.deepEqual(result, { blockingErrors: [], preventContinuation: false, stopReason: undefined });

    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal(parsed.hook_event_name, 'Stop');
    // RED without the fix: these fields were never serialized.
    assert.equal(parsed.stop_hook_active, true);
    assert.equal(parsed.last_assistant_message, 'all tests pass');
  });

  it('defaults stop_hook_active to false and omits last_assistant_message when absent', async () => {
    const payload = 'stop-payload2.json';
    const dir = tmpCwdWithHooks([
      { event: 'stop', hook: { type: 'command', command: `cat > ${payload}` } },
    ]);
    const hm = makeManager(dir);

    await hm.executeStop({ messages: [], turnCount: 0 });

    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.equal(parsed.stop_hook_active, false);
    assert.ok(!('last_assistant_message' in parsed), 'no last_assistant_message when no final message');
  });

  it('treats a blank/whitespace final message as absent', async () => {
    const payload = 'stop-payload3.json';
    const dir = tmpCwdWithHooks([
      { event: 'stop', hook: { type: 'command', command: `cat > ${payload}` } },
    ]);
    const hm = makeManager(dir);

    await hm.executeStop({
      messages: [],
      turnCount: 0,
      lastAssistantMessage: { role: 'assistant', content: '   ' },
    });

    const parsed = JSON.parse(fs.readFileSync(path.join(dir, payload), 'utf-8'));
    assert.ok(!('last_assistant_message' in parsed), 'whitespace-only message is omitted');
  });
});
