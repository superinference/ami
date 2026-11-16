/**
 * U007 — gap: "User-facing 'HTTP hook blocked' message + affordance for
 * SSRF-blocked hook targets" (ref ssrfGuard.ts:L285-294).
 *
 * AMI already refuses an HTTP hook whose target resolves to a private/link-local/
 * metadata address (the SSRF guard in runHttpHook), but it did so SILENTLY: a
 * non-fail-closed hook returned '' (plain allow, no notice) and a fail-closed
 * hook denied with a generic "HTTP hook failed (fail-closed)" reason that named
 * neither the blocked host nor a stable error code. This locks in Claude Code's
 * ssrfError affordance: a user-facing, host-naming message tagged with the stable
 * ERR_HTTP_HOOK_BLOCKED_ADDRESS code — surfaced as a DENY reason when
 * onFailure:block, else as a non-blocking SYSTEM notice (never silently dropped).
 *
 * RED (revert runHttpHook SSRF branch + helper): the formatter export is gone and
 * the block path returns the generic onFail() — the host/code assertions and the
 * non-fail-closed SYSTEM-notice assertion fail.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  formatHttpHookBlockedMessage,
  ERR_HTTP_HOOK_BLOCKED_ADDRESS,
} from '../src/hooks';

const tmpDirs: string[] = [];
function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-ssrf-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ } }
});
function preCtx(toolName: string) {
  return { messages: [], turnCount: 0, toolName, toolInput: {} } as any;
}
function makeManager(dir: string): HookManager {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  return hm;
}

describe('formatHttpHookBlockedMessage (gap: SSRF user-facing message + stable code)', () => {
  it('names the blocked host, the validator reason, the loopback affordance, and the stable code', () => {
    const msg = formatHttpHookBlockedMessage(
      'http://sneaky.example/hook',
      'Blocked: "sneaky.example" resolves to private IP 10.1.2.3',
    );
    assert.match(msg, /^HTTP hook blocked: sneaky\.example /);
    assert.match(msg, /resolves to private IP 10\.1\.2\.3/);
    assert.match(msg, /Loopback \(127\.0\.0\.1, ::1\) is allowed for local dev\./);
    assert.ok(msg.includes(ERR_HTTP_HOOK_BLOCKED_ADDRESS), 'must carry the stable error code');
  });

  it('falls back to the raw URL as the host when the URL is not parseable', () => {
    const msg = formatHttpHookBlockedMessage('::::not a url', 'Blocked: bad');
    assert.ok(msg.includes('::::not a url'));
    assert.ok(msg.includes(ERR_HTTP_HOOK_BLOCKED_ADDRESS));
  });
});

describe('runHttpHook SSRF block surfacing (integration via executePreToolUse)', () => {
  it('fail-closed (onFailure:block): denies with a host-naming, code-tagged reason', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'http://sneaky.example/h', onFailure: 'block' } }]);
    const hm = makeManager(dir);
    hm.setUrlSafetyValidator(async () => ({ error: 'Blocked: "sneaky.example" resolves to private IP 10.1.2.3' }));
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'deny');
    assert.ok(decision.action === 'deny' && decision.reason.includes('sneaky.example'), 'deny reason must name the host');
    assert.ok(decision.action === 'deny' && decision.reason.includes(ERR_HTTP_HOOK_BLOCKED_ADDRESS), 'deny reason must carry the stable code');
  });

  it('non-fail-closed: surfaces a SYSTEM notice (allow) instead of silently dropping the hook', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: 'http://sneaky.example/h' } }]);
    const hm = makeManager(dir);
    hm.setUrlSafetyValidator(async () => ({ error: 'Blocked: "sneaky.example" resolves to private IP 10.1.2.3' }));
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(decision.action, 'allow');
    assert.ok(decision.action === 'allow' && decision.additionalContext, 'the block must surface a user-facing notice, not be silent');
    assert.ok(
      decision.action === 'allow' && decision.additionalContext!.includes(ERR_HTTP_HOOK_BLOCKED_ADDRESS),
      'the surfaced notice must carry the stable code',
    );
  });
});
