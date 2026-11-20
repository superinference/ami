/**
 * Parity-campaign wiring tests (hooks-core gap file). Each block closes a themed
 * cluster of the reference parity gaps, with red-green discriminators noted inline:
 *
 *  - HTTP header injection hardening (sanitizeHeaderValue; CR/LF/NUL strip after
 *    env interpolation) — SECURITY. [gaps 96,100,104]
 *  - HTTP default timeout = 10 minutes. [gaps 97,99,106]
 *  - Matcher exactness: a simple-token matcher is an EXACT match, never a
 *    substring/regex leak onto a longer tool name — BEHAVIOR. [gap 72]
 *  - $ARGUMENTS / $n / append-fallback substitution of hook input into
 *    prompt/agent hook prompts. [gaps 84,108,121]
 *  - Agent-hook turn cap (MAX_AGENT_TURNS) + thinking-disabled posture passed to
 *    the hook sub-engine; prompt hooks are single-shot (no cap). [gaps 85,88,90,93,119]
 *  - CC wording "<Prompt|Agent> hook condition was not met: …". [gap 117]
 *  - Bounded hook output: UserPromptSubmit context truncated at
 *    MAX_HOOK_OUTPUT_LENGTH with a marker. [gap 139]
 *  - getHookEventMetadata declarative catalog (summary/description/matcherField
 *    derived from the single source HOOK_MATCHER_FIELD). [gaps 122,123]
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as http from 'node:http';
import {
  HookManager,
  sanitizeHeaderValue,
  interpolateHookHeaderEnv,
  substituteHookArguments,
  truncateHookOutput,
  getHookEventMetadata,
  DEFAULT_HTTP_HOOK_TIMEOUT_MS,
  MAX_AGENT_TURNS,
  MAX_HOOK_OUTPUT_LENGTH,
  HOOK_MATCHER_FIELD,
  type PreToolUseContext,
  type UserPromptContext,
} from '../src/hooks';

function preCtx(toolName: string): PreToolUseContext {
  return { messages: [], turnCount: 0, toolName, toolInput: { command: 'ls' } };
}

function tmpCwdWithHooks(entries: unknown[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hpw-'));
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
// HTTP header injection hardening (SECURITY)
// ---------------------------------------------------------------------------
describe('sanitizeHeaderValue / header-injection hardening', () => {
  it('strips CR, LF, and NUL bytes', () => {
    // RED without the fix: these control chars survive → header injection.
    assert.equal(sanitizeHeaderValue('tok\r\nX-Evil: 1'), 'tokX-Evil: 1');
    assert.equal(sanitizeHeaderValue('a\u0000b'), 'ab');
    assert.equal(sanitizeHeaderValue('clean-value'), 'clean-value');
  });

  it('interpolateHookHeaderEnv sanitizes AFTER interpolation (a CRLF env value cannot inject a header)', () => {
    // A malicious env var value carrying CRLF must be neutralised. RED without
    // the fix: interpolateHookHeaderEnv returned the raw value with CRLF intact.
    const env = { EVIL: 'legit\r\nX-Injected: pwned', OK: 'fine' };
    const allowed = new Set(['EVIL', 'OK']);
    assert.equal(interpolateHookHeaderEnv('$EVIL', allowed, env), 'legitX-Injected: pwned');
    assert.equal(interpolateHookHeaderEnv('Bearer ${OK}', allowed, env), 'Bearer fine');
    assert.ok(!interpolateHookHeaderEnv('$EVIL', allowed, env).includes('\n'));
  });

  it('end-to-end: a CRLF-bearing allowlisted env value reaches the server as a sanitized single header', async () => {
    // Node's http layer throws ERR_INVALID_CHAR on a raw CR/LF header value, so
    // WITHOUT sanitization the request errors and the server is never hit.
    let seen: string | undefined;
    const server = http.createServer((req, res) => {
      seen = req.headers['x-token'] as string | undefined;
      res.statusCode = 200; res.end('SYSTEM:ok');
    });
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r));
    const port = (server.address() as { port: number }).port;
    process.env.AMI_HPW_CRLF = 'val\r\nX-Evil: 1';
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/h`, headers: { 'X-Token': '$AMI_HPW_CRLF' }, allowedEnvVars: ['AMI_HPW_CRLF'] } }]);
    const hm = makeManager(dir);
    hm.setHttpHookPolicy({ httpHookAllowedEnvVars: ['AMI_HPW_CRLF'] });
    hm.setUrlSafetyValidator(async () => ({ resolvedIP: '127.0.0.1' }));
    const decision = await hm.executePreToolUse(preCtx('bash'));
    assert.equal(seen, 'valX-Evil: 1', 'header reaches server sanitized (no CRLF)');
    assert.equal(decision.additionalContext, 'ok');
    delete process.env.AMI_HPW_CRLF;
    server.close();
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('HTTP hook default timeout is 10 minutes', () => {
    assert.equal(DEFAULT_HTTP_HOOK_TIMEOUT_MS, 10 * 60 * 1000);
  });
});

// ---------------------------------------------------------------------------
// Matcher exactness (BEHAVIOR)
// ---------------------------------------------------------------------------
describe('matcher exactness (simple token is an exact match, not a substring)', () => {
  it('a simple-token matcher does NOT leak onto a longer tool name', async () => {
    // RED before the fix: matchesPattern treated "file_edit" as a regex, so
    // /file_edit/.test("file_edit_notebook") → true and the hook over-fired.
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', matcher: 'file_edit', hook: { type: 'command', command: 'exit 2' } },
    ]);
    const hm = makeManager(dir);
    const exact = await hm.executePreToolUse(preCtx('file_edit'));
    assert.equal(exact.action, 'deny', 'exact tool name matches');
    const longer = await hm.executePreToolUse(preCtx('file_edit_notebook'));
    assert.equal(longer.action, 'allow', 'a longer tool name must NOT match the simple token');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('a pipe-list matcher matches each token exactly', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', matcher: 'bash|file_read', hook: { type: 'command', command: 'exit 2' } },
    ]);
    const hm = makeManager(dir);
    assert.equal((await hm.executePreToolUse(preCtx('bash'))).action, 'deny');
    assert.equal((await hm.executePreToolUse(preCtx('file_read'))).action, 'deny');
    assert.equal((await hm.executePreToolUse(preCtx('file_reader'))).action, 'allow');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('a regex matcher (with metacharacters) still applies', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', matcher: '^file_', hook: { type: 'command', command: 'exit 2' } },
    ]);
    const hm = makeManager(dir);
    assert.equal((await hm.executePreToolUse(preCtx('file_write'))).action, 'deny');
    assert.equal((await hm.executePreToolUse(preCtx('bash'))).action, 'allow');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// $ARGUMENTS substitution
// ---------------------------------------------------------------------------
describe('substituteHookArguments', () => {
  it('replaces $ARGUMENTS with the full input JSON', () => {
    assert.equal(substituteHookArguments('check: $ARGUMENTS', '{"a":1}'), 'check: {"a":1}');
  });
  it('replaces indexed $n / $ARGUMENTS[n] with whitespace tokens', () => {
    assert.equal(substituteHookArguments('$0 then $1', 'alpha beta'), 'alpha then beta');
    assert.equal(substituteHookArguments('x=$ARGUMENTS[1]', 'alpha beta'), 'x=beta');
    assert.equal(substituteHookArguments('miss=$5', 'alpha'), 'miss=');
  });
  it('appends ARGUMENTS when the prompt has no placeholder', () => {
    assert.equal(substituteHookArguments('no placeholder', 'DATA'), 'no placeholder\n\nARGUMENTS: DATA');
  });
  it('leaves the prompt unchanged when there is neither placeholder nor input', () => {
    assert.equal(substituteHookArguments('bare', ''), 'bare');
  });

  it('a prompt hook referencing $ARGUMENTS gets the event data substituted in place (no duplicate append)', async () => {
    const dir = tmpCwdWithHooks([
      { event: 'preToolUse', hook: { type: 'prompt', prompt: 'Evaluate $ARGUMENTS and respond.', timeout: 5 } },
    ]);
    const hm = makeManager(dir);
    let prompt = '';
    hm.setEngineFactory((_c: unknown) => ({
      async *submit(p: string) { prompt = p; yield { type: 'text_delta', text: '{"ok":true}' }; },
      shutdown() {},
    }));
    await hm.executePreToolUse(preCtx('bash'));
    assert.ok(prompt.includes('Evaluate {'), 'event JSON substituted at the placeholder');
    assert.ok(!prompt.includes('Event data:'), 'no append path when a placeholder is present');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Agent-hook turn cap + thinking-disabled posture + condition wording
// ---------------------------------------------------------------------------
describe('engine-backed hook posture', () => {
  function captureCfg(dir: string, text: string): Promise<{ cfg: any; decision: unknown }> {
    const hm = makeManager(dir);
    let cfg: any = null;
    hm.setEngineFactory((c: any) => { cfg = c; return { async *submit(_p: string) { yield { type: 'text_delta', text }; }, shutdown() {} }; });
    return hm.executePreToolUse(preCtx('bash')).then(decision => ({ cfg, decision }));
  }

  it('agent hook caps turns at MAX_AGENT_TURNS and disables thinking', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'agent', prompt: 'verify', timeout: 5 } }]);
    const { cfg } = await captureCfg(dir, '{"ok":true}');
    assert.equal(cfg.maxTurns, MAX_AGENT_TURNS);
    assert.equal(cfg.maxTurns, 50);
    assert.equal(cfg.disableThinking, true);
    assert.deepEqual(cfg.disallowedTools, ['task', 'plan_mode', 'workflow', 'AskUserQuestion', 'task_output', 'task_kill']);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('prompt hook is single-shot (no turn cap) but still disables thinking', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'prompt', prompt: 'verify', timeout: 5 } }]);
    const { cfg } = await captureCfg(dir, '{"ok":true}');
    assert.equal(cfg.maxTurns, undefined, 'prompt hooks are single-shot, no maxTurns');
    assert.equal(cfg.disableThinking, true);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('uses the reference-parity "Agent hook condition was not met" wording', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'agent', prompt: 'verify', timeout: 5 } }]);
    const { decision } = await captureCfg(dir, '{"ok":false,"reason":"nope"}');
    const d = decision as { action: string; reason?: string };
    assert.equal(d.action, 'deny');
    assert.match(d.reason ?? '', /^Agent hook condition was not met: nope/);
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('uses the reference-parity "Prompt hook condition was not met" wording', async () => {
    const dir = tmpCwdWithHooks([{ event: 'preToolUse', hook: { type: 'prompt', prompt: 'verify', timeout: 5 } }]);
    const { decision } = await captureCfg(dir, '{"ok":false,"reason":"bad"}');
    const d = decision as { action: string; reason?: string };
    assert.equal(d.action, 'deny');
    assert.match(d.reason ?? '', /^Prompt hook condition was not met: bad/);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

// ---------------------------------------------------------------------------
// Bounded hook output
// ---------------------------------------------------------------------------
describe('truncateHookOutput / MAX_HOOK_OUTPUT_LENGTH', () => {
  it('passes short content through unchanged', () => {
    assert.equal(truncateHookOutput('short'), 'short');
    assert.equal(MAX_HOOK_OUTPUT_LENGTH, 10_000);
  });
  it('truncates overflow with the parity marker', () => {
    const big = 'x'.repeat(MAX_HOOK_OUTPUT_LENGTH + 500);
    const out = truncateHookOutput(big);
    assert.ok(out.length < big.length);
    assert.match(out, /\[output truncated - exceeded 10000 characters\]$/);
  });
  it('UserPromptSubmit hook context is bounded at the limit', async () => {
    const hm = new HookManager();
    const huge = 'y'.repeat(MAX_HOOK_OUTPUT_LENGTH + 100);
    hm.onUserPromptSubmit(async () => ({ additionalContext: huge }));
    const ctx: UserPromptContext = { prompt: 'hi', turnCount: 0 };
    const res = await hm.executeUserPromptSubmit(ctx);
    assert.ok(res && res.additionalContext);
    assert.ok(res!.additionalContext!.length <= MAX_HOOK_OUTPUT_LENGTH + 60);
    assert.match(res!.additionalContext!, /output truncated/);
  });
});

// ---------------------------------------------------------------------------
// getHookEventMetadata catalog
// ---------------------------------------------------------------------------
describe('getHookEventMetadata', () => {
  it('returns summary/description + toolName matcher field for tool events', () => {
    const m = getHookEventMetadata('preToolUse');
    assert.ok(m);
    assert.equal(m!.matcherField, 'toolName');
    assert.match(m!.description, /Exit code 2/);
    assert.ok(m!.summary.length > 0);
  });
  it('derives the matcher field from the single-source HOOK_MATCHER_FIELD for non-tool events', () => {
    assert.equal(getHookEventMetadata('notification')!.matcherField, HOOK_MATCHER_FIELD.notification);
    assert.equal(getHookEventMetadata('configChange')!.matcherField, HOOK_MATCHER_FIELD.configChange);
    assert.equal(getHookEventMetadata('preCompact')!.matcherField, 'trigger');
  });
  it('omits the matcher field for events with no matchable field', () => {
    assert.equal(getHookEventMetadata('worktreeCreate')!.matcherField, undefined);
  });
  it('scopes session lifecycle + stopFailure hooks on their discriminator field', () => {
    assert.equal(getHookEventMetadata('sessionStart')!.matcherField, 'source');
    assert.equal(getHookEventMetadata('sessionEnd')!.matcherField, 'reason');
    assert.equal(getHookEventMetadata('stopFailure')!.matcherField, 'error');
  });
  it('returns undefined for an unknown event', () => {
    assert.equal(getHookEventMetadata('bogusEvent'), undefined);
  });
});
