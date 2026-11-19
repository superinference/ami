/**
 * orca::bounded-fail-open-fail-closed-hook-chain — PreToolUse explicit-approve bypass.
 *
 * A PreToolUse hook that EXPLICITLY approves via Claude Code's
 * `permissionDecision: 'allow'` / `decision: 'approve'` now surfaces as
 * `{ action: 'allow', permissionBypass: true }` so the consumer (tool-executor) can
 * bypass a would-be interactive permission prompt. A bare no-objection allow (exit 0,
 * no decision) leaves `permissionBypass` unset and does NOT bypass. The bypass carries
 * across the allow accumulation of a multi-hook chain.
 *
 * RED on revert: removing the preToolUse approve-mapping / the executeDecisionHooks
 * bypass carry in hooks.ts makes `permissionBypass` undefined → these assertions fail.
 *
 * Run from core/core: node --import tsx --test test/pretooluse-permission-bypass-orca.test.ts
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager } from '../src/hooks';
import type { PreToolUseContext } from '../src/hooks';

function preCtx(toolName = 'bash'): PreToolUseContext {
  return { messages: [], turnCount: 0, toolName, toolInput: { command: 'ls' } };
}
function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-ppb-'));
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

describe('PreToolUse explicit-approve → permissionBypass', () => {
  it('permissionDecision:"allow" command hook sets action=allow + permissionBypass', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo '{"permissionDecision":"allow"}'` } },
    ]);
    const d = await makeManager(dir).executePreToolUse(preCtx());
    assert.equal(d.action, 'allow');
    assert.equal((d as { permissionBypass?: boolean }).permissionBypass, true);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('decision:"approve" command hook also sets permissionBypass', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo '{"decision":"approve"}'` } },
    ]);
    const d = await makeManager(dir).executePreToolUse(preCtx());
    assert.equal(d.action, 'allow');
    assert.equal((d as { permissionBypass?: boolean }).permissionBypass, true);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('hookSpecificOutput-nested approve is honored (Claude Code envelope)', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"allow"}}'` } },
    ]);
    const d = await makeManager(dir).executePreToolUse(preCtx());
    assert.equal(d.action, 'allow');
    assert.equal((d as { permissionBypass?: boolean }).permissionBypass, true);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('a bare no-objection hook (exit 0, no decision) does NOT set permissionBypass', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'true' } },
    ]);
    const d = await makeManager(dir).executePreToolUse(preCtx());
    assert.equal(d.action, 'allow');
    assert.equal((d as { permissionBypass?: boolean }).permissionBypass, undefined);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('a deny hook still wins over an approve (approve never overrides deny)', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', hook: { type: 'command', command: `echo '{"permissionDecision":"allow"}'` } },
      { event: 'preToolUse', hook: { type: 'command', command: `echo '{"decision":"deny","reason":"nope"}'` } },
    ]);
    const d = await makeManager(dir).executePreToolUse(preCtx());
    assert.equal(d.action, 'deny');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
