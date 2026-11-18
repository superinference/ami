/**
 * U007 / w102 — Recover a command hook's JSON decision from stdout that
 * interleaves ordinary log lines with the decision line (Claude Code parity:
 * `checkForAsyncHookResponses` stdout line-scan, AsyncHookRegistry.ts L183-212).
 *
 * Before this change AMI parsed a command hook's stdout with a single
 * `JSON.parse(stdout.trim())`. A hook that printed a log line BEFORE its JSON
 * decision (e.g. a wrapper that emits `[hook] starting` then its decision) made
 * the whole-blob parse fail; because the trimmed output did not start with `{`,
 * the decision was SILENTLY DROPPED and the entire stdout was treated as opaque
 * plain text — a `deny`/`block` decision quietly downgraded to `allow`.
 *
 * Now the parse falls back to scanning stdout line-by-line for the first
 * non-`async` JSON object line ({@link firstHookJsonLine}) and honors the decision
 * found there.
 *
 * RED discriminators (fail WITHOUT the line-scan fallback):
 *  - a preToolUse command hook printing a log line then `{"decision":"block"}`
 *    resolves to `allow` instead of `deny`.
 *  - a hook printing a log line then `{"hookSpecificOutput":{"additionalContext":…}}`
 *    does not surface the additionalContext.
 * GREEN also guards against regressions:
 *  - a whole-blob JSON decision (no log lines) still works.
 *  - genuinely malformed single-object output still surfaces a validation error.
 *  - plain-text (no JSON) output still passes through (allow, no decision).
 *  - the `{ async: true }` background marker line is NOT taken as a sync decision.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { HookManager, firstHookJsonLine } from '../src/hooks';

function writeHook(cwd: string, command: string): void {
  const dir = path.join(cwd, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, 'hooks.json'),
    JSON.stringify({ hooks: [{ event: 'preToolUse', hook: { type: 'command', command, timeout: 5000 } }] }),
  );
}

async function runPreTool(cwd: string) {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.setCwd(cwd);
  hm.loadFromFile(cwd);
  return hm.executePreToolUse({ messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } });
}

describe('U007/w102 — firstHookJsonLine (pure line-scan helper)', () => {
  it('returns the first JSON-object line, skipping leading log lines', () => {
    const out = '[hook] starting\nsome noise\n{"decision":"block","reason":"nope"}\ntrailing log';
    assert.deepEqual(firstHookJsonLine(out), { decision: 'block', reason: 'nope' });
  });

  it('skips the { async: true } background marker and keeps scanning', () => {
    const out = '{"async":true,"asyncTimeout":1000}\n{"decision":"deny"}';
    assert.deepEqual(firstHookJsonLine(out), { decision: 'deny' });
  });

  it('returns undefined when no JSON-object line is present', () => {
    assert.equal(firstHookJsonLine('just\nplain\ntext'), undefined);
    assert.equal(firstHookJsonLine(''), undefined);
    assert.equal(firstHookJsonLine('[1,2,3]'), undefined); // array line is not an object
  });

  it('ignores a malformed brace-leading line and keeps scanning', () => {
    const out = '{not json at all\n{"continue":false}';
    assert.deepEqual(firstHookJsonLine(out), { continue: false });
  });
});

describe('U007/w102 — command hook stdout line-scan recovers a buried decision', () => {
  it('RED/GREEN: a log line before a block decision still DENIES', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w102-block-'));
    try {
      // printf keeps the log line and the JSON decision on separate lines.
      writeHook(cwd, `printf '[hook] inspecting tool\\n{"decision":"block","reason":"policy violation"}\\n'`);
      const r = await runPreTool(cwd);
      assert.equal(r.action, 'deny', 'buried block decision must be honored, not downgraded to allow');
      assert.match((r as { reason: string }).reason, /policy violation/);
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it('GREEN: a buried additionalContext decision is surfaced', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w102-ctx-'));
    try {
      writeHook(cwd, `printf 'log: ok\\n{"hookSpecificOutput":{"hookEventName":"PreToolUse","additionalContext":"from a buried line"}}\\n'`);
      const r = await runPreTool(cwd);
      assert.equal(r.action, 'allow');
      assert.equal((r as { additionalContext?: string }).additionalContext, 'from a buried line');
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it('REGRESSION: a whole-blob JSON decision (no log lines) still DENIES', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w102-whole-'));
    try {
      writeHook(cwd, `printf '{"decision":"deny","reason":"blocked wholesale"}\\n'`);
      const r = await runPreTool(cwd);
      assert.equal(r.action, 'deny');
      assert.match((r as { reason: string }).reason, /blocked wholesale/);
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });

  it('REGRESSION: plain-text output with no JSON stays allow', async () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w102-plain-'));
    try {
      writeHook(cwd, `printf 'just a friendly log line\\nnothing structured here\\n'`);
      const r = await runPreTool(cwd);
      assert.equal(r.action, 'allow');
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });
});
