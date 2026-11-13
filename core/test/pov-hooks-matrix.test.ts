// POV: core/core/src/hooks.ts behavioral matrix (round 2).
//
// We exercise OBSERVABLE BEHAVIOR of the hook subsystem by calling the real
// functions / driving a real HookManager with real command hooks (fast, portable
// `bash -c` snippets) and asserting on the RESULTS:
//   - HookManager registering + firing each HOOK_EVENTS kind (loadFromFile from a
//     real .ami/hooks.json in a temp dir, then execute*()).
//   - normalizeMapKeyedHooks translating Claude-Code map-keyed hooks.json into the
//     internal array form (incl. translateHookMatcher tool-name mapping + defensive
//     skips).
//   - sanitizeHookOutput / sanitizeHookEnv behavior.
//   - isProtectedEvent / canHookEvent for protected vs passthrough events.
//   - EventConcurrencyLimiter cap + FIFO.
//
// No reading of .ts source + .includes — expected values are computed by calling
// the code. ESM-safe: fs.mkdtempSync temp dirs cleaned in after(); no network.

import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  HookManager,
  HOOK_EVENTS,
  HOOK_CONFIG_TYPES,
  EventConcurrencyLimiter,
  MAX_CONCURRENT_PER_EVENT,
  sanitizeHookOutput,
  sanitizeHookEnv,
  SENSITIVE_ENV_PATTERNS,
  normalizeMapKeyedHooks,
  translateHookMatcher,
  serializeHookEvent,
  validateHookCommand,
  validateHookTimeout,
  MAX_HOOK_TIMEOUT_MS,
  DEFAULT_HOOK_TIMEOUT_MS,
  type HookConfigEntry,
  type PreToolUseContext,
  type PostToolUseContext,
} from '../src/hooks';
import {
  isProtectedEvent,
  canHookEvent,
  PROTECTED_EVENTS,
  PASSTHROUGH_EVENTS,
} from '../src/hook-protection';

// ---------------------------------------------------------------------------
// helpers: build a trusted HookManager whose hooks come from a real on-disk
// .ami/hooks.json. Workspace must be trusted or project-scoped hooks are skipped.
// ---------------------------------------------------------------------------

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

const tmpDirs: string[] = [];
function newCwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'pov-hooks-'));
  tmpDirs.push(d);
  return d;
}

after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ }
  }
});

// ===========================================================================
// 1. sanitizeHookOutput
// ===========================================================================

describe('sanitizeHookOutput', () => {
  it('escapes opening and closing system-reminder tags (case-insensitive)', () => {
    assert.equal(
      sanitizeHookOutput('a <system-reminder>x</system-reminder> b'),
      'a [tag-escaped]x[tag-escaped] b',
    );
    assert.equal(sanitizeHookOutput('<SYSTEM-REMINDER>'), '[tag-escaped]');
    assert.equal(sanitizeHookOutput('</System-Reminder>'), '[tag-escaped]');
  });

  it('leaves text without tags untouched', () => {
    assert.equal(sanitizeHookOutput('plain text'), 'plain text');
    assert.equal(sanitizeHookOutput(''), '');
    assert.equal(sanitizeHookOutput('<other-tag>'), '<other-tag>');
  });

  it('escapes every occurrence (global)', () => {
    assert.equal(
      sanitizeHookOutput('<system-reminder><system-reminder>'),
      '[tag-escaped][tag-escaped]',
    );
  });
});

// ===========================================================================
// 2. sanitizeHookEnv / SENSITIVE_ENV_PATTERNS
// ===========================================================================

describe('sanitizeHookEnv', () => {
  it('strips API keys, tokens, secrets, passwords, credentials, private keys', () => {
    const clean = sanitizeHookEnv({
      PATH: '/usr/bin',
      ANTHROPIC_API_KEY: 'sk-ant',
      OPENAI_API_KEY: 'sk-oai',
      GITHUB_TOKEN: 'ghp',
      MY_SERVICE_API_KEY: 'secret',
      DB_PASSWORD: 'pw',
      AWS_SECRET_ACCESS_KEY: 'aws',
      SOME_CREDENTIALS: 'creds',
      DEPLOY_PRIVATE_KEY: 'pk',
      NORMAL_VAR: 'ok',
    });
    assert.equal(clean.PATH, '/usr/bin');
    assert.equal(clean.NORMAL_VAR, 'ok');
    for (const k of [
      'ANTHROPIC_API_KEY', 'OPENAI_API_KEY', 'GITHUB_TOKEN', 'MY_SERVICE_API_KEY',
      'DB_PASSWORD', 'AWS_SECRET_ACCESS_KEY', 'SOME_CREDENTIALS', 'DEPLOY_PRIVATE_KEY',
    ]) {
      assert.ok(!(k in clean), `${k} must be stripped`);
    }
  });

  it('drops undefined values', () => {
    const clean = sanitizeHookEnv({ A: undefined, B: 'x' });
    assert.ok(!('A' in clean));
    assert.equal(clean.B, 'x');
  });

  it('SENSITIVE_ENV_PATTERNS is a non-empty readonly list of regexps', () => {
    assert.ok(SENSITIVE_ENV_PATTERNS.length > 0);
    assert.ok(SENSITIVE_ENV_PATTERNS.every(p => p instanceof RegExp));
    // computed: every pattern that matches ANTHROPIC_API_KEY is among the list
    assert.ok(SENSITIVE_ENV_PATTERNS.some(p => p.test('ANTHROPIC_API_KEY')));
  });
});

// ===========================================================================
// 3. serializeHookEvent — camelCase preserved + snake_case / Pascal aliases added
// ===========================================================================

describe('serializeHookEvent', () => {
  it('appends a trailing newline and valid JSON', () => {
    const s = serializeHookEvent({ event: 'stop' });
    assert.ok(s.endsWith('\n'));
    const parsed = JSON.parse(s);
    assert.equal(parsed.event, 'stop');
  });

  it('maps event -> PascalCase hook_event_name for known events', () => {
    const parsed = JSON.parse(serializeHookEvent({ event: 'preToolUse' }));
    assert.equal(parsed.hook_event_name, 'PreToolUse');
    assert.equal(parsed.event, 'preToolUse'); // camelCase kept for back-compat
  });

  it('falls back to the raw event name for unknown events', () => {
    const parsed = JSON.parse(serializeHookEvent({ event: 'customEvent' }));
    assert.equal(parsed.hook_event_name, 'customEvent');
  });

  it('adds snake_case aliases for known camelCase keys without clobbering existing', () => {
    const parsed = JSON.parse(serializeHookEvent({
      event: 'preToolUse', toolName: 'bash', toolInput: { command: 'ls' },
    }));
    assert.equal(parsed.tool_name, 'bash');
    assert.deepEqual(parsed.tool_input, { command: 'ls' });
    assert.equal(parsed.toolName, 'bash'); // original preserved
  });

  it('does not overwrite a snake_case key that already exists', () => {
    const parsed = JSON.parse(serializeHookEvent({
      event: 'postToolUse', toolOutput: 'camel', tool_response: 'preset',
    }));
    assert.equal(parsed.tool_response, 'preset');
  });

  it('omits hook_event_name when event is not a string', () => {
    const parsed = JSON.parse(serializeHookEvent({ foo: 1 }));
    assert.ok(!('hook_event_name' in parsed));
  });
});

// ===========================================================================
// 4. validateHookCommand / validateHookTimeout
// ===========================================================================

describe('validateHookCommand', () => {
  it('rejects empty / whitespace-only commands', () => {
    assert.equal(validateHookCommand('').valid, false);
    assert.equal(validateHookCommand('   ').valid, false);
  });

  it('rejects blocked destructive patterns', () => {
    for (const bad of ['rm -rf /', 'mkfs', 'dd if=', ':(){', 'shutdown', 'reboot', 'halt']) {
      const r = validateHookCommand(`echo x && ${bad} foo`);
      assert.equal(r.valid, false, `${bad} must be rejected`);
      assert.match(r.reason!, /blocked pattern/);
    }
  });

  it('rejects device writes and system chmod 777', () => {
    assert.equal(validateHookCommand('echo x > /dev/sda').valid, false);
    assert.equal(validateHookCommand('chmod 777 /etc').valid, false);
  });

  it('rejects over-long commands', () => {
    const r = validateHookCommand('x'.repeat(5000));
    assert.equal(r.valid, false);
    assert.match(r.reason!, /maximum length/);
  });

  it('accepts a normal command', () => {
    assert.deepEqual(validateHookCommand('echo hello'), { valid: true });
  });
});

describe('validateHookTimeout', () => {
  it('clamps to [100, MAX]', () => {
    assert.equal(validateHookTimeout(0), 100);
    assert.equal(validateHookTimeout(50), 100);
    assert.equal(validateHookTimeout(5000), 5000);
    assert.equal(validateHookTimeout(MAX_HOOK_TIMEOUT_MS + 1), MAX_HOOK_TIMEOUT_MS);
    assert.ok(DEFAULT_HOOK_TIMEOUT_MS > 0 && DEFAULT_HOOK_TIMEOUT_MS <= MAX_HOOK_TIMEOUT_MS);
  });
});

// ===========================================================================
// 5. translateHookMatcher — Claude-Code tool names -> AMI names
// ===========================================================================

describe('translateHookMatcher', () => {
  it('maps canonical Claude-Code tool tokens', () => {
    assert.equal(translateHookMatcher('Bash'), 'bash');
    assert.equal(translateHookMatcher('Edit|Write'), 'file_edit|file_edit');
    assert.equal(translateHookMatcher('Read'), 'file_read');
    assert.equal(translateHookMatcher('MultiEdit'), 'file_edit');
    assert.equal(translateHookMatcher('Glob|Grep|LS'), 'glob|grep|list_dir');
    assert.equal(translateHookMatcher('WebFetch|WebSearch'), 'web_fetch|web_search');
    assert.equal(translateHookMatcher('NotebookEdit|Task|TodoWrite'), 'notebook_edit|task|todo_write');
  });

  it('passes through unknown / already-AMI tokens and trims', () => {
    assert.equal(translateHookMatcher('bash'), 'bash');
    assert.equal(translateHookMatcher('custom_tool'), 'custom_tool');
    assert.equal(translateHookMatcher(' Bash | Write '), 'bash|file_edit');
    assert.equal(translateHookMatcher('*'), '*');
  });
});

// ===========================================================================
// 6. normalizeMapKeyedHooks — Claude-Code map form -> flat entry array
// ===========================================================================

describe('normalizeMapKeyedHooks', () => {
  it('translates a map-keyed PreToolUse block into flat entries', () => {
    const entries = normalizeMapKeyedHooks({
      PreToolUse: [
        { matcher: 'Bash', hooks: [{ type: 'command', command: 'echo a' }] },
        { matcher: 'Edit|Write', hooks: [{ type: 'command', command: 'echo b' }] },
      ],
    });
    assert.equal(entries.length, 2);
    assert.equal(entries[0].event, 'preToolUse'); // lower-camel-cased
    assert.equal(entries[0].matcher, 'bash');      // translated
    assert.equal(entries[1].matcher, 'file_edit|file_edit');
    assert.equal((entries[0].hook as any).command, 'echo a');
  });

  it('lower-camel-cases every event key', () => {
    const entries = normalizeMapKeyedHooks({
      SessionStart: [{ hooks: [{ type: 'command', command: 'x' }] }],
      PostToolUse: [{ hooks: [{ type: 'command', command: 'y' }] }],
    });
    const events = entries.map(e => e.event).sort();
    assert.deepEqual(events, ['postToolUse', 'sessionStart']);
  });

  it('expands multiple hooks within one group into multiple entries', () => {
    const entries = normalizeMapKeyedHooks({
      Stop: [{ hooks: [
        { type: 'command', command: '1' },
        { type: 'command', command: '2' },
      ] }],
    });
    assert.equal(entries.length, 2);
    assert.ok(entries.every(e => e.event === 'stop'));
  });

  it('omits matcher when absent (no matcher key on entry)', () => {
    const entries = normalizeMapKeyedHooks({
      Stop: [{ hooks: [{ type: 'command', command: 'x' }] }],
    });
    assert.ok(!('matcher' in entries[0]));
  });

  it('defensively skips non-object / array / null inputs', () => {
    assert.deepEqual(normalizeMapKeyedHooks(null), []);
    assert.deepEqual(normalizeMapKeyedHooks(undefined), []);
    assert.deepEqual(normalizeMapKeyedHooks('str'), []);
    assert.deepEqual(normalizeMapKeyedHooks(42), []);
    assert.deepEqual(normalizeMapKeyedHooks([1, 2, 3]), []);
  });

  it('skips groups that are not arrays, not objects, or lack a hooks array', () => {
    const entries = normalizeMapKeyedHooks({
      PreToolUse: 'not-an-array',
      PostToolUse: [null, 42, { matcher: 'Bash' /* no hooks */ }, { hooks: 'nope' }],
      Stop: [{ hooks: [null, 7, { type: 'command', command: 'ok' }] }],
    });
    // Only the single valid inner hook under Stop survives.
    assert.equal(entries.length, 1);
    assert.equal(entries[0].event, 'stop');
    assert.equal((entries[0].hook as any).command, 'ok');
  });

  it('ignores a non-string matcher', () => {
    const entries = normalizeMapKeyedHooks({
      PreToolUse: [{ matcher: 123, hooks: [{ type: 'command', command: 'x' }] }],
    });
    assert.ok(!('matcher' in entries[0]));
  });
});

// ===========================================================================
// 7. isProtectedEvent / canHookEvent
// ===========================================================================

describe('isProtectedEvent / canHookEvent', () => {
  it('protected events are recognized', () => {
    for (const e of PROTECTED_EVENTS) assert.equal(isProtectedEvent(e), true);
    assert.equal(isProtectedEvent('session.compact'), true);
  });

  it('passthrough + unknown events are not protected', () => {
    for (const e of PASSTHROUGH_EVENTS) assert.equal(isProtectedEvent(e), false);
    assert.equal(isProtectedEvent('definitely-not-an-event'), false);
  });

  it('user tier is blocked only on protected events', () => {
    assert.equal(canHookEvent('session.compact', 'user'), false);
    assert.equal(canHookEvent('prompt.submit', 'user'), true);
    assert.equal(canHookEvent('tool.call', 'user'), true);
  });

  it('privileged tiers can hook protected events', () => {
    for (const tier of ['builtin', 'prepend', 'append', 'core'] as const) {
      assert.equal(canHookEvent('session.compact', tier), true);
    }
  });

  it('every HOOK_EVENTS kind is passthrough-compatible (user tier may register)', () => {
    for (const e of HOOK_EVENTS) {
      assert.equal(canHookEvent(e, 'user'), true, `${e} should be hookable by user tier`);
    }
  });
});

// ===========================================================================
// 8. EventConcurrencyLimiter cap + FIFO + independence
// ===========================================================================

describe('EventConcurrencyLimiter', () => {
  it('default cap equals MAX_CONCURRENT_PER_EVENT', () => {
    assert.equal(MAX_CONCURRENT_PER_EVENT, 12);
    const l = new EventConcurrencyLimiter();
    // acquire up to the cap without blocking
    return (async () => {
      for (let i = 0; i < MAX_CONCURRENT_PER_EVENT; i++) await l.acquire('x');
      assert.equal(l.getActive('x'), MAX_CONCURRENT_PER_EVENT);
      for (let i = 0; i < MAX_CONCURRENT_PER_EVENT; i++) l.release('x');
      assert.equal(l.getActive('x'), 0);
    })();
  });

  it('blocks beyond the cap, then releases a waiter FIFO', async () => {
    const l = new EventConcurrencyLimiter(1);
    await l.acquire('e');
    const order: number[] = [];
    const p1 = l.acquire('e').then(() => order.push(1));
    const p2 = l.acquire('e').then(() => order.push(2));
    await new Promise(r => setTimeout(r, 10));
    assert.equal(l.getWaiting('e'), 2);
    l.release('e'); // wakes waiter 1
    await p1;
    l.release('e'); // wakes waiter 2
    await p2;
    assert.deepEqual(order, [1, 2]);
    l.release('e');
    assert.equal(l.getActive('e'), 0);
  });

  it('tracks events independently', async () => {
    const l = new EventConcurrencyLimiter(1);
    await l.acquire('a');
    await l.acquire('b');
    assert.equal(l.getActive('a'), 1);
    assert.equal(l.getActive('b'), 1);
    assert.equal(l.getActive('never-touched'), 0);
    l.release('a');
    l.release('b');
  });
});

// ===========================================================================
// 9. HOOK_EVENTS / HOOK_CONFIG_TYPES invariants
// ===========================================================================

describe('HOOK_EVENTS / HOOK_CONFIG_TYPES', () => {
  it('HOOK_EVENTS has unique entries and the core lifecycle kinds', () => {
    const set = new Set(HOOK_EVENTS);
    assert.equal(set.size, HOOK_EVENTS.length, 'no dupes');
    for (const e of ['preToolUse', 'postToolUse', 'sessionStart', 'sessionEnd', 'stop', 'error', 'subagentStart', 'subagentStop']) {
      assert.ok(set.has(e as any), `missing ${e}`);
    }
  });

  it('HOOK_CONFIG_TYPES are the four supported types', () => {
    assert.deepEqual([...HOOK_CONFIG_TYPES].sort(), ['agent', 'command', 'http', 'prompt']);
  });
});

// ===========================================================================
// 10. HookManager.loadFromFile + firing each registered command-hook kind
// ===========================================================================

describe('HookManager — loadFromFile + firing (command hooks)', () => {
  const baseCtx = { messages: [], turnCount: 0 };

  it('getSummary reflects a loaded hook per registered event', () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [
      { event: 'stop', hook: { type: 'command', command: 'true' } },
      { event: 'preToolUse', hook: { type: 'command', command: 'true' } },
      { event: 'postToolUse', hook: { type: 'command', command: 'true' } },
    ]);
    const summary = mgr.getSummary();
    assert.equal(summary.stop, 1);
    assert.equal(summary.preToolUse, 1);
    assert.equal(summary.postToolUse, 1);
  });

  it('project-scoped hooks are skipped when workspace is NOT trusted', () => {
    const cwd = newCwd();
    const dir = path.join(cwd, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({
      hooks: [{ event: 'stop', hook: { type: 'command', command: 'true' } }],
    }));
    const mgr = new HookManager();
    mgr.setWorkspaceTrusted(false);
    mgr.loadFromFile(cwd);
    assert.equal(mgr.getSummary().stop, 0);
  });

  it('preToolUse DENY: stdout(exit 2) -> deny with reason from stderr', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'preToolUse',
      hook: { type: 'command', command: 'echo "nope" >&2; exit 2' },
    }]);
    const ctx: PreToolUseContext = { ...baseCtx, toolName: 'bash', toolInput: { command: 'ls' } };
    const decision = await mgr.executePreToolUse(ctx);
    assert.equal(decision.action, 'deny');
    assert.match((decision as any).reason, /nope/);
  });

  it('preToolUse exit 0 with no decision -> allow', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'preToolUse',
      hook: { type: 'command', command: 'true' },
    }]);
    const decision = await mgr.executePreToolUse({ ...baseCtx, toolName: 'bash', toolInput: {} });
    assert.equal(decision.action, 'allow');
  });

  it('preToolUse MODIFY via stdout JSON updatedInput', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'preToolUse',
      hook: { type: 'command', command: `printf '%s' '{"updatedInput":{"command":"safe"}}'` },
    }]);
    const decision = await mgr.executePreToolUse({ ...baseCtx, toolName: 'bash', toolInput: { command: 'danger' } });
    assert.equal(decision.action, 'modify');
    assert.deepEqual((decision as any).updatedInput, { command: 'safe' });
  });

  it('preToolUse additionalContext-only JSON -> allow + additionalContext', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'preToolUse',
      hook: { type: 'command', command: `printf '%s' '{"additionalContext":"heads up"}'` },
    }]);
    const decision = await mgr.executePreToolUse({ ...baseCtx, toolName: 'bash', toolInput: {} });
    assert.equal(decision.action, 'allow');
    assert.equal((decision as any).additionalContext, 'heads up');
  });

  it('preToolUse matcher gates the hook (non-matching tool -> allow without running)', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'preToolUse',
      matcher: 'file_edit',
      hook: { type: 'command', command: 'exit 2' },
    }]);
    // tool is bash -> matcher excludes it -> allow
    const allowed = await mgr.executePreToolUse({ ...baseCtx, toolName: 'bash', toolInput: {} });
    assert.equal(allowed.action, 'allow');
    // tool is file_edit -> matcher includes it -> deny
    const denied = await mgr.executePreToolUse({ ...baseCtx, toolName: 'file_edit', toolInput: {} });
    assert.equal(denied.action, 'deny');
  });

  it('postToolUse returns updatedToolOutput from raw stdout JSON', async () => {
    // A command hook whose stdout JSON has ONLY updatedToolOutput falls through
    // runHookByType's decision detection and is passed back raw, so the
    // postToolUse handler parses and surfaces it.
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'postToolUse',
      hook: { type: 'command', command: `printf '%s' '{"updatedToolOutput":"redacted"}'` },
    }]);
    const ctx: PostToolUseContext = {
      ...baseCtx, toolName: 'bash', toolInput: {}, toolOutput: 'raw', isError: false,
    };
    const res = await mgr.executePostToolUse(ctx);
    assert.equal(res?.updatedToolOutput, 'redacted');
  });

  it('postToolUse with no decision output returns void', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'postToolUse',
      hook: { type: 'command', command: 'true' },
    }]);
    const res = await mgr.executePostToolUse({
      ...baseCtx, toolName: 'bash', toolInput: {}, toolOutput: 'raw', isError: false,
    });
    assert.equal(res, undefined);
  });

  it('sessionStart initialUserMessage from stdout JSON', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'sessionStart',
      hook: { type: 'command', command: `printf '%s' '{"initialUserMessage":"welcome"}'` },
    }]);
    const res = await mgr.executeSessionStart({ sessionId: 's1', cwd });
    assert.equal(res?.initialUserMessage, 'welcome');
  });

  it('sessionStart SYSTEM: prefix becomes initialUserMessage', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'sessionStart',
      hook: { type: 'command', command: `printf '%s' 'SYSTEM:ctx-line'` },
    }]);
    const res = await mgr.executeSessionStart({ sessionId: 's1', cwd });
    assert.equal(res?.initialUserMessage, 'ctx-line');
  });

  it('userPromptSubmit command hook runs and returns void (additionalContext is routed through MODIFY and not surfaced for this event)', async () => {
    // Observable behavior: runHookByType converts a command hook's additionalContext
    // JSON into a `MODIFY:` string, which userPromptSubmit's handler cannot re-parse,
    // so the handler yields void. We assert the real result rather than a guess.
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'userPromptSubmit',
      hook: { type: 'command', command: `printf '%s' '{"additionalContext":"appended"}'` },
    }]);
    const res = await mgr.executeUserPromptSubmit({ prompt: 'hi', turnCount: 1 });
    assert.equal(res, undefined);
  });

  it('permissionRequest DENY: prefix -> deny', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' 'DENY:blocked by policy'` },
    }]);
    const res = await mgr.executePermissionRequest({ toolName: 'bash', toolInput: {} });
    assert.equal(res?.action, 'deny');
    assert.match(res?.reason ?? '', /blocked by policy/);
  });

  it('permissionRequest ALLOW literal -> allow', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'permissionRequest',
      hook: { type: 'command', command: `printf '%s' 'ALLOW'` },
    }]);
    const res = await mgr.executePermissionRequest({ toolName: 'bash', toolInput: {} });
    assert.equal(res?.action, 'allow');
  });

  it('once:true removes the hook after first fire', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'stop',
      hook: { type: 'command', command: 'true', once: true },
    }]);
    assert.equal(mgr.getSummary().stop, 1);
    await mgr.executeStop(baseCtx);
    assert.equal(mgr.getSummary().stop, 0);
  });

  it('async:true hook returns allow immediately (fire-and-forget)', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [{
      event: 'preToolUse',
      hook: { type: 'command', command: 'exit 2', async: true },
    }]);
    const decision = await mgr.executePreToolUse({ ...baseCtx, toolName: 'bash', toolInput: {} });
    // async hooks do not block: allow despite exit 2
    assert.equal(decision.action, 'allow');
  });

  it('lifecycle events with void return run without error (subagent/compact/task/worktree/etc.)', async () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [
      { event: 'sessionEnd', hook: { type: 'command', command: 'true' } },
      { event: 'subagentStart', hook: { type: 'command', command: 'true' } },
      { event: 'subagentStop', hook: { type: 'command', command: 'true' } },
      { event: 'preCompact', hook: { type: 'command', command: 'true' } },
      { event: 'postCompact', hook: { type: 'command', command: 'true' } },
      { event: 'taskCreated', hook: { type: 'command', command: 'true' } },
      { event: 'taskCompleted', hook: { type: 'command', command: 'true' } },
      { event: 'notification', hook: { type: 'command', command: 'true' } },
      { event: 'setup', hook: { type: 'command', command: 'true' } },
      { event: 'configChange', hook: { type: 'command', command: 'true' } },
      { event: 'instructionsLoaded', hook: { type: 'command', command: 'true' } },
      { event: 'worktreeCreate', hook: { type: 'command', command: 'true' } },
      { event: 'worktreeRemove', hook: { type: 'command', command: 'true' } },
      { event: 'cwdChanged', hook: { type: 'command', command: 'true' } },
      { event: 'fileChanged', hook: { type: 'command', command: 'true' } },
      { event: 'elicitation', hook: { type: 'command', command: 'true' } },
      { event: 'elicitationResult', hook: { type: 'command', command: 'true' } },
      { event: 'stopFailure', hook: { type: 'command', command: 'true' } },
      { event: 'postToolUseFailure', hook: { type: 'command', command: 'true' } },
      { event: 'postSampling', hook: { type: 'command', command: 'true' } },
      { event: 'error', hook: { type: 'command', command: 'true' } },
      { event: 'permissionDenied', hook: { type: 'command', command: 'true' } },
    ]);
    await assert.doesNotReject(Promise.all([
      mgr.executeSessionEnd({ sessionId: 's', cwd }),
      mgr.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 'c', prompt: 'x' }),
      mgr.executeSubagentStop({ parentSessionId: 'p', subagentSessionId: 'c', prompt: 'x' }),
      mgr.executePreCompact({ messageCount: 3, tokenEstimate: 100 }),
      mgr.executePostCompact({ messageCount: 3, tokenEstimate: 100 }),
      mgr.executeTaskCreated({ taskId: 't', subject: 's' }),
      mgr.executeTaskCompleted({ taskId: 't', subject: 's' }),
      mgr.executeNotification({ message: 'm' }),
      mgr.executeSetup({ sessionId: 's', cwd }),
      mgr.executeConfigChange({ source: 'cli' }),
      mgr.executeInstructionsLoaded({ filePath: 'f', loadReason: 'r' }),
      mgr.executeWorktreeCreate({ name: 'w' }),
      mgr.executeWorktreeRemove({ name: 'w' }),
      mgr.executeCwdChanged({ oldCwd: '/a', newCwd: '/b' }),
      mgr.executeFileChanged({ filePath: 'f.ts', event: 'change' }),
      mgr.executeElicitation({ mcpServerName: 'srv' }),
      mgr.executeElicitationResult({ mcpServerName: 'srv' }),
      mgr.executeStopFailure({ error: 'e', hookIndex: 0 }),
      mgr.executePostToolUseFailure({ ...baseCtx, toolName: 'bash', toolInput: {}, toolOutput: '', isError: true }),
      mgr.executePostSampling(baseCtx),
      mgr.executeError(baseCtx),
      mgr.executePermissionDenied({ toolName: 'bash', toolInput: {} }),
    ]));
  });

  it('unknown event kind is logged and not registered', () => {
    const cwd = newCwd();
    const mgr = makeManager(cwd, [
      { event: 'definitelyUnknownEvent', hook: { type: 'command', command: 'true' } } as any,
      { event: 'stop', hook: { type: 'command', command: 'true' } },
    ]);
    // Only the valid 'stop' hook registered; unknown produced no counter.
    const summary = mgr.getSummary();
    assert.equal(summary.stop, 1);
    assert.ok(!('definitelyUnknownEvent' in summary));
  });
});

// ===========================================================================
// 11. Claude-Code map-keyed hooks.json loaded end-to-end via loadFromFile
// ===========================================================================

describe('HookManager — Claude-Code map-keyed hooks.json compatibility', () => {
  it('loads map-keyed .ami/hooks.json and fires a translated-matcher preToolUse deny', async () => {
    const cwd = newCwd();
    const dir = path.join(cwd, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    // Claude-Code format: top-level `hooks` is a MAP keyed by PascalCase events.
    fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({
      hooks: {
        PreToolUse: [
          { matcher: 'Bash', hooks: [{ type: 'command', command: 'echo denied >&2; exit 2' }] },
        ],
      },
    }));
    const mgr = new HookManager();
    mgr.setWorkspaceTrusted(true);
    mgr.loadFromFile(cwd);
    assert.equal(mgr.getSummary().preToolUse, 1);

    // matcher 'Bash' -> translated to 'bash', so a bash tool call is gated+denied
    const denied = await mgr.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: {} });
    assert.equal(denied.action, 'deny');
    // a non-bash tool passes the matcher and is allowed
    const allowed = await mgr.executePreToolUse({ messages: [], turnCount: 0, toolName: 'file_read', toolInput: {} });
    assert.equal(allowed.action, 'allow');
  });

  it('invalid JSON in hooks.json is tolerated (no throw, nothing registered)', () => {
    const cwd = newCwd();
    const dir = path.join(cwd, '.ami');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'hooks.json'), '{ this is not json');
    const mgr = new HookManager();
    mgr.setWorkspaceTrusted(true);
    assert.doesNotThrow(() => mgr.loadFromFile(cwd));
    assert.equal(mgr.getSummary().preToolUse, 0);
  });

  it('missing hooks.json is a no-op', () => {
    const cwd = newCwd();
    const mgr = new HookManager();
    mgr.setWorkspaceTrusted(true);
    assert.doesNotThrow(() => mgr.loadFromFile(cwd));
    assert.equal(mgr.getSummary().stop, 0);
  });
});

// ===========================================================================
// 12. registerCallback / executeCallbacks
// ===========================================================================

describe('HookManager — callback hooks', () => {
  it('fires registered callbacks and returns first object result', async () => {
    const mgr = new HookManager();
    const seen: string[] = [];
    mgr.registerCallback('custom', async () => { seen.push('a'); });
    mgr.registerCallback('custom', async () => { seen.push('b'); return { additionalContext: 'ctx' }; });
    mgr.registerCallback('custom', async () => { seen.push('c'); return { additionalContext: 'later' }; });
    const res = await mgr.executeCallbacks('custom', { x: 1 });
    assert.deepEqual(res, { additionalContext: 'ctx' });
    // 'c' is not reached because 'b' already returned an object
    assert.deepEqual(seen, ['a', 'b']);
  });

  it('unregister via returned disposer removes the callback', async () => {
    const mgr = new HookManager();
    let fired = 0;
    const off = mgr.registerCallback('e', async () => { fired++; });
    off();
    await mgr.executeCallbacks('e', {});
    assert.equal(fired, 0);
  });

  it('executeCallbacks for an unregistered event returns undefined', async () => {
    const mgr = new HookManager();
    assert.equal(await mgr.executeCallbacks('none', {}), undefined);
  });

  it('a throwing callback is swallowed and later callbacks still run', async () => {
    const mgr = new HookManager();
    let reached = false;
    mgr.registerCallback('e', async () => { throw new Error('boom'); });
    mgr.registerCallback('e', async () => { reached = true; });
    await mgr.executeCallbacks('e', {});
    assert.equal(reached, true);
  });

  it('getSummary counts callback hooks', () => {
    const mgr = new HookManager();
    mgr.registerCallback('a', async () => {});
    mgr.registerCallback('a', async () => {});
    mgr.registerCallback('b', async () => {});
    assert.equal(mgr.getSummary().callbackHooks, 3);
  });
});

// ===========================================================================
// 13. Accessor state round-trips
// ===========================================================================

describe('HookManager — state accessors', () => {
  it('effort level and workspace trust round-trip', () => {
    const mgr = new HookManager();
    assert.equal(mgr.getEffortLevel(), 'high'); // default
    mgr.setEffortLevel('low');
    assert.equal(mgr.getEffortLevel(), 'low');
    assert.equal(mgr.isWorkspaceTrusted(), false);
    mgr.setWorkspaceTrusted(true);
    assert.equal(mgr.isWorkspaceTrusted(), true);
  });
});
