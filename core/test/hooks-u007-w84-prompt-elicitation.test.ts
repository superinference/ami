/**
 * Claude -> AMI parity (U007, wave 84): hook-driven prompt-elicitation protocol.
 *
 * Reference:
 *  - references/claude/claude-code-main/src/types/hooks.ts:L26-L47
 *      (promptRequestSchema: {prompt,message,options:[{key,label,description?}]}
 *       + PromptResponse {prompt_response, selected})
 *  - references/claude/claude-code-main/src/components/hooks/PromptDialog.tsx
 *      (hook presents selectable options; chosen key returned to the hook)
 *  - references/claude/claude-code-main/src/utils/hooks.ts:L1060-L1109
 *      (stdout line-scan for PromptRequest JSON -> answer on stdin, request lines
 *       stripped from captured stdout by content match; stdin kept open for the
 *       response round-trip)
 *
 * A command hook can pause mid-run and ask the user to pick one of a labelled
 * list of options by emitting a PromptRequest JSON line on stdout. The engine
 * resolves it to a key, writes a PromptResponse line back to the hook's stdin,
 * and strips the request line from the hook's captured stdout.
 */

import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  validatePromptRequest,
  parsePromptRequestLine,
  isValidPromptSelection,
  buildPromptResponse,
  serializePromptResponse,
  createPromptLineScanner,
} from '../src/index';
import type { PromptRequest, HookConfigEntry } from '../src/hooks';

const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-u007-w84-'));
  tmpDirs.push(d);
  return d;
}
function writeHooks(entries: HookConfigEntry[]): string {
  const cwd = mkTmp();
  const dir = path.join(cwd, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'hooks.json'), JSON.stringify({ hooks: entries }));
  return cwd;
}
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

const REQ: PromptRequest = {
  prompt: 'p1',
  message: 'Pick one',
  options: [
    { key: 'go', label: 'Proceed', description: 'run the thing' },
    { key: 'stop', label: 'Abort' },
  ],
};

// ---------------------------------------------------------------------------
// validatePromptRequest
// ---------------------------------------------------------------------------
describe('validatePromptRequest', () => {
  it('accepts a well-formed request (description optional per option)', () => {
    const out = validatePromptRequest({
      prompt: 'p1', message: 'Pick', options: [{ key: 'a', label: 'A', description: 'd' }, { key: 'b', label: 'B' }],
    });
    assert.deepEqual(out, {
      prompt: 'p1', message: 'Pick', options: [{ key: 'a', label: 'A', description: 'd' }, { key: 'b', label: 'B' }],
    });
  });

  it('accepts an empty options array (Claude parity: z.array allows empty)', () => {
    assert.deepEqual(validatePromptRequest({ prompt: 'p', message: '', options: [] }), { prompt: 'p', message: '', options: [] });
  });

  it('rejects a missing / empty prompt id (discriminator)', () => {
    assert.equal(validatePromptRequest({ message: 'm', options: [] }), undefined);
    assert.equal(validatePromptRequest({ prompt: '', message: 'm', options: [] }), undefined);
  });

  it('rejects when message is not a string', () => {
    assert.equal(validatePromptRequest({ prompt: 'p', message: 5, options: [] }), undefined);
  });

  it('rejects when options is not an array', () => {
    assert.equal(validatePromptRequest({ prompt: 'p', message: 'm', options: {} }), undefined);
  });

  it('rejects an option missing key or label, or with a non-string description', () => {
    assert.equal(validatePromptRequest({ prompt: 'p', message: 'm', options: [{ label: 'A' }] }), undefined);
    assert.equal(validatePromptRequest({ prompt: 'p', message: 'm', options: [{ key: 'a' }] }), undefined);
    assert.equal(validatePromptRequest({ prompt: 'p', message: 'm', options: [{ key: 'a', label: 'A', description: 7 }] }), undefined);
    assert.equal(validatePromptRequest({ prompt: 'p', message: 'm', options: ['x'] }), undefined);
  });

  it('does NOT mistake an async-output line or non-objects for a request', () => {
    assert.equal(validatePromptRequest({ async: true, asyncTimeout: 5 }), undefined);
    assert.equal(validatePromptRequest(null), undefined);
    assert.equal(validatePromptRequest([1, 2]), undefined);
    assert.equal(validatePromptRequest('str'), undefined);
  });
});

// ---------------------------------------------------------------------------
// parsePromptRequestLine
// ---------------------------------------------------------------------------
describe('parsePromptRequestLine', () => {
  it('parses a valid request line, tolerating surrounding whitespace', () => {
    const line = '   ' + JSON.stringify(REQ) + '  ';
    assert.deepEqual(parsePromptRequestLine(line), REQ);
  });
  it('returns undefined for blank, non-JSON, and non-request JSON', () => {
    assert.equal(parsePromptRequestLine(''), undefined);
    assert.equal(parsePromptRequestLine('   '), undefined);
    assert.equal(parsePromptRequestLine('just a log line'), undefined);
    assert.equal(parsePromptRequestLine('{"decision":"approve"}'), undefined);
  });
});

// ---------------------------------------------------------------------------
// selection + response helpers
// ---------------------------------------------------------------------------
describe('prompt response helpers', () => {
  it('isValidPromptSelection only accepts an offered key', () => {
    assert.equal(isValidPromptSelection(REQ, 'go'), true);
    assert.equal(isValidPromptSelection(REQ, 'stop'), true);
    assert.equal(isValidPromptSelection(REQ, 'nope'), false);
  });
  it('buildPromptResponse echoes the request id and the chosen key', () => {
    assert.deepEqual(buildPromptResponse(REQ, 'go'), { prompt_response: 'p1', selected: 'go' });
  });
  it('serializePromptResponse appends the trailing newline a line-reader needs', () => {
    const s = serializePromptResponse(buildPromptResponse(REQ, 'go'));
    assert.equal(s, '{"prompt_response":"p1","selected":"go"}\n');
    assert.ok(s.endsWith('\n'));
  });
});

// ---------------------------------------------------------------------------
// createPromptLineScanner (streaming line-scan + content-match strip)
// ---------------------------------------------------------------------------
describe('createPromptLineScanner', () => {
  it('detects requests across chunk boundaries and preserves order', () => {
    const seen: string[] = [];
    const s = createPromptLineScanner((req) => { seen.push(req.prompt); });
    const r1 = JSON.stringify({ ...REQ, prompt: 'r1' });
    const r2 = JSON.stringify({ ...REQ, prompt: 'r2' });
    // Split the first request across two feeds; a normal line in between.
    s.feed(r1.slice(0, 10));
    s.feed(r1.slice(10) + '\nhello world\n' + r2 + '\n');
    assert.deepEqual(seen, ['r1', 'r2']);
    assert.equal(s.processedLines.size, 2);
    assert.ok(s.processedLines.has(r1));
    assert.ok(s.processedLines.has(r2));
  });

  it('does not process an incomplete trailing line until it is completed', () => {
    const seen: string[] = [];
    const s = createPromptLineScanner((req) => { seen.push(req.prompt); });
    s.feed(JSON.stringify(REQ)); // no trailing newline yet
    assert.deepEqual(seen, []);
    s.feed('\n');
    assert.deepEqual(seen, ['p1']);
  });

  it('stripProcessed removes only handled request lines, keeping ordinary output', () => {
    const s = createPromptLineScanner(() => { /* noop */ });
    const reqLine = JSON.stringify(REQ);
    const full = `before\n${reqLine}\n{"decision":"approve"}\n`;
    s.feed(full);
    const stripped = s.stripProcessed(full);
    assert.ok(!stripped.includes(reqLine), 'request line removed');
    assert.ok(stripped.includes('before'));
    assert.ok(stripped.includes('{"decision":"approve"}'));
  });

  it('stripProcessed is a no-op when nothing matched', () => {
    const s = createPromptLineScanner(() => { /* noop */ });
    s.feed('plain output\nmore\n');
    assert.equal(s.stripProcessed('plain output\nmore\n'), 'plain output\nmore\n');
  });
});

// ---------------------------------------------------------------------------
// HookManager wiring + introspection
// ---------------------------------------------------------------------------
describe('HookManager prompt-elicitation wiring', () => {
  it('reports resolver presence via getSummary + hasPromptRequestHandler', () => {
    const hm = new HookManager();
    assert.equal(hm.hasPromptRequestHandler(), false);
    assert.equal(hm.getSummary().promptRequestHandler, 0);
    hm.setPromptRequestHandler(async () => 'go');
    assert.equal(hm.hasPromptRequestHandler(), true);
    assert.equal(hm.getSummary().promptRequestHandler, 1);
    hm.setPromptRequestHandler(undefined);
    assert.equal(hm.hasPromptRequestHandler(), false);
  });
});

// ---------------------------------------------------------------------------
// End-to-end: a real command hook asks, the resolver answers, the chosen key
// travels back to the hook over stdin and drives its decision. Also proves the
// request line is stripped (otherwise the two-line stdout would fail JSON.parse
// and never yield a `block` decision).
// ---------------------------------------------------------------------------
describe('command hook prompt-elicitation round-trip (e2e)', () => {
  const HOOK = [
    'read -r _event',
    `echo '{"prompt":"p1","message":"Proceed?","options":[{"key":"go","label":"Go"},{"key":"stop","label":"Stop"}]}'`,
    'read -r resp',
    `case "$resp" in`,
    `  *'"selected":"go"'*) echo '{"decision":"approve"}';;`,
    `  *'"selected":"stop"'*) echo '{"decision":"block","reason":"user chose stop"}';;`,
    `  *) echo '{"decision":"block","reason":"no response"}';;`,
    'esac',
  ].join('\n');

  function makeCtx() {
    return { messages: [], turnCount: 1, toolName: 'bash', toolInput: { command: 'ls' } };
  }

  it('selected key "go" reaches the hook -> approve (allow)', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setPromptRequestHandler(async (req) => { assert.equal(req.options.length, 2); return 'go'; });
    const cwd = writeHooks([{ event: 'preToolUse', hook: { type: 'command', command: HOOK } }]);
    hm.loadFromFile(cwd);
    const decision = await hm.executePreToolUse(makeCtx());
    assert.equal(decision.action, 'allow');
  });

  it('selected key "stop" reaches the hook -> block (deny) — discriminates the round-trip', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setPromptRequestHandler(async () => 'stop');
    const cwd = writeHooks([{ event: 'preToolUse', hook: { type: 'command', command: HOOK } }]);
    hm.loadFromFile(cwd);
    const decision = await hm.executePreToolUse(makeCtx());
    assert.equal(decision.action, 'deny');
    assert.match((decision as { reason: string }).reason, /stop/);
  });

  it('a rejecting resolver (user cancelled) closes stdin so the hook does not hang', async () => {
    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setPromptRequestHandler(async () => { throw new Error('user cancelled'); });
    const cwd = writeHooks([{ event: 'preToolUse', hook: { type: 'command', command: HOOK } }]);
    hm.loadFromFile(cwd);
    // The hook's `read -r resp` hits EOF; it emits the `no response` block path.
    const decision = await hm.executePreToolUse(makeCtx());
    assert.equal(decision.action, 'deny');
  });
});
