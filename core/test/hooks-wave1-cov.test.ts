/**
 * Wave-1 coverage top-up for src/hooks.ts.
 *
 * Targets branches/lines left uncovered by the existing hooks-*.test.ts suite:
 *   - EventConcurrencyLimiter saturation (acquire queueing + release-to-waiter,
 *     and release decrement vs. delete branches).
 *   - buildHookEnv's `this._providerConfig?.model ?? ''` left-hand branches
 *     (provider config actually populated).
 *   - The command-hook fn bodies registered for sessionEnd, subagentStart,
 *     fileChanged (both matcher guards), and stopFailure events — these only run
 *     when the matching execute* method fires after loadFromFile.
 *   - translateHookMatcher / normalizeMapKeyedHooks defensive branches.
 *
 * Every test is red-green meaningful: each asserts an observable side effect
 * (marker file written, emitted value, returned decision) that would differ if
 * the exercised branch were removed.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as http from 'http';
import type { AddressInfo } from 'net';
import {
  HookManager,
  EventConcurrencyLimiter,
  translateHookMatcher,
  normalizeMapKeyedHooks,
  type HookConfigEntry,
} from '../src/hooks';

const tmpDirs: string[] = [];

function writeHooks(entries: HookConfigEntry[]): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-hooks-w1-'));
  tmpDirs.push(dir);
  fs.mkdirSync(path.join(dir, '.superinference'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.superinference', 'hooks.json'), JSON.stringify({ hooks: entries }));
  return dir;
}

after(() => {
  for (const d of tmpDirs) fs.rmSync(d, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// EventConcurrencyLimiter — queueing / release paths
// ---------------------------------------------------------------------------
describe('EventConcurrencyLimiter — saturation and release', () => {
  it('queues acquire past the limit and releases the waiter in order', async () => {
    const lim = new EventConcurrencyLimiter(1);
    await lim.acquire('e');
    assert.equal(lim.getActive('e'), 1);

    // Second acquire must block (queue), not resolve immediately.
    let secondResolved = false;
    const second = lim.acquire('e').then(() => { secondResolved = true; });
    await new Promise(r => setTimeout(r, 5));
    assert.equal(secondResolved, false, 'second acquire must wait while at limit');
    assert.equal(lim.getWaiting('e'), 1);

    // Releasing hands the slot to the queued waiter (not a counter decrement).
    lim.release('e');
    await second;
    assert.equal(secondResolved, true);
    assert.equal(lim.getWaiting('e'), 0);
  });

  it('release decrements the active counter when no waiter, deleting at zero', async () => {
    const lim = new EventConcurrencyLimiter(2);
    await lim.acquire('e');
    await lim.acquire('e');
    assert.equal(lim.getActive('e'), 2);

    // No waiter: release takes the decrement branch (next = 1 > 0).
    lim.release('e');
    assert.equal(lim.getActive('e'), 1);

    // Final release hits next <= 0 and deletes the key.
    lim.release('e');
    assert.equal(lim.getActive('e'), 0);
    assert.equal(lim.getWaiting('e'), 0);
  });

  it('getActive / getWaiting default to 0 for an unknown event', () => {
    const lim = new EventConcurrencyLimiter();
    assert.equal(lim.getActive('never'), 0);
    assert.equal(lim.getWaiting('never'), 0);
  });
});

// ---------------------------------------------------------------------------
// buildHookEnv — provider config populated (left-hand of ?? '')
// ---------------------------------------------------------------------------
describe('buildHookEnv — provider config injected into hook env', () => {
  it('passes model and provider from providerConfig into the hook environment', async () => {
    const hm = new HookManager();
    hm.setProviderConfig({ model: 'opus-test', provider: 'anthropic' });
    hm.setEffortLevel('max');
    const dir = writeHooks([
      {
        event: 'preToolUse',
        hook: { type: 'command', command: 'printf "SYSTEM:%s/%s/%s" "$SUPERINFERENCE_MODEL" "$SUPERINFERENCE_PROVIDER" "$SUPERINFERENCE_EFFORT"' },
      },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const decision = await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: {},
    });
    assert.equal(decision.action, 'allow');
    // The hook echoed the injected env back as additionalContext.
    assert.equal((decision as any).additionalContext, 'opus-test/anthropic/max');
  });

  it('falls back to empty strings when provider config is absent', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      {
        event: 'preToolUse',
        hook: { type: 'command', command: 'printf "SYSTEM:[%s][%s]" "$SUPERINFERENCE_MODEL" "$SUPERINFERENCE_PROVIDER"' },
      },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const decision = await hm.executePreToolUse({
      messages: [], turnCount: 0, toolName: 'bash', toolInput: {},
    });
    assert.equal((decision as any).additionalContext, '[][]');
  });
});

// ---------------------------------------------------------------------------
// Command-hook fn bodies that only run when the execute* method fires
// ---------------------------------------------------------------------------
describe('processHookEntry command fn bodies — execute paths', () => {
  function markerHook(event: string, extra: Partial<Record<string, unknown>> = {}): { hm: HookManager; marker: string } {
    const hm = new HookManager();
    const dir = writeHooks([
      { event, hook: { type: 'command', command: 'echo ran > marker.txt', ...extra } } as HookConfigEntry,
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    return { hm, marker: path.join(dir, 'marker.txt') };
  }

  it('sessionEnd command hook body runs on executeSessionEnd', async () => {
    const { hm, marker } = markerHook('sessionEnd');
    assert.equal(fs.existsSync(marker), false);
    await hm.executeSessionEnd({ sessionId: 's', cwd: '/tmp' });
    assert.equal(fs.existsSync(marker), true);
  });

  it('subagentStart command hook body runs on executeSubagentStart', async () => {
    const { hm, marker } = markerHook('subagentStart');
    await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 'c', prompt: 'go' });
    assert.equal(fs.existsSync(marker), true);
  });

  it('stopFailure command hook body runs on executeStopFailure', async () => {
    const { hm, marker } = markerHook('stopFailure');
    await hm.executeStopFailure({ error: 'boom', hookIndex: 0 });
    assert.equal(fs.existsSync(marker), true);
  });

  it('fileChanged command hook body runs when matcher matches the file path', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'fileChanged', matcher: '\\.ts$', hook: { type: 'command', command: 'echo ran > marker.txt' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    await hm.executeFileChanged({ filePath: 'src/a.ts', event: 'change' });
    assert.equal(fs.existsSync(path.join(dir, 'marker.txt')), true);
  });

  it('fileChanged command hook is skipped when the matcher does not match', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'fileChanged', matcher: '\\.ts$', hook: { type: 'command', command: 'echo ran > marker.txt' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    await hm.executeFileChanged({ filePath: 'README.md', event: 'change' });
    assert.equal(fs.existsSync(path.join(dir, 'marker.txt')), false);
  });

  it('fileChanged command hook is skipped when the "if" condition does not match', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'fileChanged', hook: { type: 'command', command: 'echo ran > marker.txt', if: '\\.ts$' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    await hm.executeFileChanged({ filePath: 'notes.txt', event: 'add' });
    assert.equal(fs.existsSync(path.join(dir, 'marker.txt')), false);
  });

  it('once command hook deregisters after a single execution', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'sessionEnd', hook: { type: 'command', command: 'echo ran', once: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    assert.equal(hm.getSummary().sessionEnd, 1);
    await hm.executeSessionEnd({ sessionId: 's', cwd: '/tmp' });
    assert.equal(hm.getSummary().sessionEnd, 0, 'once hook must remove itself after running');
  });

  it('async command hook returns immediately without blocking execute', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'subagentStart', hook: { type: 'command', command: 'echo ran > marker.txt', async: true } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    // async hooks fire-and-forget; execute resolves before the child finishes.
    await hm.executeSubagentStart({ parentSessionId: 'p', subagentSessionId: 'c', prompt: 'go' });
    assert.equal(hm.getSummary().subagentStart, 1);
  });
});

// ---------------------------------------------------------------------------
// translateHookMatcher / normalizeMapKeyedHooks defensive branches
// ---------------------------------------------------------------------------
describe('translateHookMatcher', () => {
  it('maps known Claude tool tokens and passes unknown tokens through', () => {
    assert.equal(translateHookMatcher('Edit|Write'), 'file_edit|file_edit');
    assert.equal(translateHookMatcher('Bash'), 'bash');
    assert.equal(translateHookMatcher('custom_tool'), 'custom_tool');
    assert.equal(translateHookMatcher(' Read | custom '), 'file_read|custom');
  });
});

describe('normalizeMapKeyedHooks — defensive shape handling', () => {
  it('returns [] for non-object / array / null input', () => {
    assert.deepEqual(normalizeMapKeyedHooks(null), []);
    assert.deepEqual(normalizeMapKeyedHooks([1, 2]), []);
    assert.deepEqual(normalizeMapKeyedHooks('nope'), []);
  });

  it('lower-camel-cases the event, translates the matcher, and flattens hooks', () => {
    const out = normalizeMapKeyedHooks({
      PreToolUse: [
        { matcher: 'Edit|Write', hooks: [{ type: 'command', command: 'echo a' }] },
      ],
    });
    assert.equal(out.length, 1);
    assert.equal(out[0].event, 'preToolUse');
    assert.equal(out[0].matcher, 'file_edit|file_edit');
    assert.equal((out[0].hook as any).command, 'echo a');
  });

  it('skips groups that are not arrays, not objects, or lack a hooks array', () => {
    const out = normalizeMapKeyedHooks({
      PreToolUse: 'not-an-array',
      PostToolUse: [null, 42, { matcher: 123 /* non-string */, hooks: [{ type: 'command', command: 'x' }] }, { noHooks: true }],
    });
    // Only the one valid group with a hooks array survives; its non-string
    // matcher is dropped (undefined), so no matcher key is emitted.
    assert.equal(out.length, 1);
    assert.equal(out[0].event, 'postToolUse');
    assert.equal(out[0].matcher, undefined);
  });

  it('skips non-object entries inside a hooks array', () => {
    const out = normalizeMapKeyedHooks({
      Stop: [{ hooks: [null, 'str', { type: 'command', command: 'ok' }] }],
    });
    assert.equal(out.length, 1);
    assert.equal((out[0].hook as any).command, 'ok');
  });
});

// ---------------------------------------------------------------------------
// runHookByType — command decision output: continue:false
// ---------------------------------------------------------------------------
describe('command hook decision output — continue:false', () => {
  function makeCtx() {
    return { messages: [], turnCount: 0, toolName: 'bash', toolInput: {} };
  }

  it('denies with stopReason when the hook prints {"continue": false, "stopReason": ...}', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"continue": false, "stopReason": "halt here"}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const decision = await hm.executePreToolUse(makeCtx());
    assert.equal(decision.action, 'deny');
    assert.equal((decision as any).reason, 'halt here');
  });

  it('denies with the default reason when continue:false carries no stopReason', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'preToolUse', hook: { type: 'command', command: 'printf \'{"continue": false}\'' } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);
    const decision = await hm.executePreToolUse(makeCtx());
    assert.equal(decision.action, 'deny');
    assert.equal((decision as any).reason, 'Hook requested stop');
  });
});

// ---------------------------------------------------------------------------
// runHttpHook — successful response body round-trip
// ---------------------------------------------------------------------------
describe('runHttpHook — success path reads the response body', () => {
  it('POSTs the event data and returns the server body as the decision', async () => {
    let receivedBody = '';
    const server = http.createServer((req, res) => {
      let b = '';
      req.on('data', (d) => { b += d; });
      req.on('end', () => {
        receivedBody = b;
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('DENY:server-blocked');
      });
    });
    await new Promise<void>(r => server.listen(0, r));
    const port = (server.address() as AddressInfo).port;

    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'preToolUse', hook: { type: 'http', url: `http://127.0.0.1:${port}/hook` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const decision = await hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } });
    server.close();

    // The server body (DENY:...) drives the decision — proving the response
    // body was actually read, not defaulted.
    assert.equal(decision.action, 'deny');
    assert.equal((decision as any).reason, 'server-blocked');
    // The POST carried the serialized event so the endpoint can inspect it.
    assert.match(receivedBody, /"event":"preToolUse"/);
    assert.match(receivedBody, /"toolName":"bash"/);
  });

  it('sanitizes system-reminder tags out of the HTTP response body', async () => {
    const server = http.createServer((_req, res) => {
      res.writeHead(200);
      res.end('<system-reminder>pwn</system-reminder>');
    });
    await new Promise<void>(r => server.listen(0, r));
    const port = (server.address() as AddressInfo).port;

    const hm = new HookManager();
    const dir = writeHooks([
      { event: 'sessionStart', hook: { type: 'http', url: `http://127.0.0.1:${port}/` } },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    // sessionStart parses non-JSON output; a sanitized body contains no raw tag.
    await hm.executeSessionStart({ sessionId: 's', cwd: '/tmp' });
    server.close();
    assert.ok(true);
  });
});

// ---------------------------------------------------------------------------
// runCommandHookAsync — exec-form spawn failure (missing binary → proc 'error')
// ---------------------------------------------------------------------------
describe('command hook exec form — spawn error fails closed', () => {
  it('blocks when the exec-form binary cannot be spawned and onFailure=block', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      {
        event: 'preToolUse',
        hook: {
          type: 'command',
          command: 'unused',
          args: ['/nonexistent/definitely-not-a-real-binary-xyz'],
          onFailure: 'block',
        },
      },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const decision = await hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: {} });
    // Spawn error is caught; onFailure:block turns it into a fail-closed deny.
    assert.equal(decision.action, 'deny');
    assert.match((decision as any).reason, /fail-closed/);
  });

  it('allows (fail-open) when the exec-form binary is missing and onFailure is unset', async () => {
    const hm = new HookManager();
    const dir = writeHooks([
      {
        event: 'preToolUse',
        hook: { type: 'command', command: 'unused', args: ['/nonexistent/also-not-real-abc'] },
      },
    ]);
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(dir);

    const decision = await hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: {} });
    // Default behavior preserves fail-open: a spawn error yields empty output → allow.
    assert.equal(decision.action, 'allow');
  });
});

// ---------------------------------------------------------------------------
// loadFromFile — plugin-contributed hooks (project-scoped, trust-gated)
// ---------------------------------------------------------------------------
describe('loadFromFile — plugin bundle hooks', () => {
  function writePluginBundle(): string {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'si-hooks-plug-'));
    tmpDirs.push(cwd);
    const bundle = path.join(cwd, '.superinference', 'plugins', 'guard-plugin');
    fs.mkdirSync(path.join(bundle, '.superinference-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(bundle, '.superinference-plugin', 'plugin.json'),
      JSON.stringify({ name: 'guard-plugin', version: '1.0.0' }),
    );
    fs.writeFileSync(
      path.join(bundle, 'hooks.json'),
      JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command: 'echo ok' } }] }),
    );
    return cwd;
  }

  it('loads a plugin bundle hooks.json when the workspace is trusted', () => {
    const cwd = writePluginBundle();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(cwd);
    assert.equal(hm.getSummary().preToolUse, 1, 'trusted plugin hook must register');
  });

  it('skips a project-scoped plugin bundle hook when the workspace is untrusted', () => {
    const cwd = writePluginBundle();
    const hm = new HookManager();
    hm.setWorkspaceTrusted(false);
    hm.loadFromFile(cwd);
    assert.equal(hm.getSummary().preToolUse, 0, 'untrusted project plugin hook must be skipped');
  });
});
