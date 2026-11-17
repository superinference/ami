/**
 * U007-w92 — agent-hook verification system prompt + transcript path parity.
 *
 *  Claude Code's execAgentHook (utils/hooks/execAgentHook.ts:L107-L153) runs a
 *  stop-condition verification agent with a DEDICATED system prompt that (a) frames
 *  the verification task, (b) tells the agent WHERE the conversation transcript is
 *  and that it may read it, (c) nudges it to "use as few steps as possible - be
 *  efficient and direct", and (d) states the structured {ok, reason} output
 *  contract. It also injects a narrowly-scoped session Read rule for exactly the
 *  transcript file so the agent can read the transcript without a permission prompt.
 *
 *  AMI previously ran agent hooks with NO system prompt and NO transcript path —
 *  the verification agent could not read the prior conversation to decide. This
 *  suite locks in:
 *   G12. runAgentHook passes an `appendSystemPrompt` describing the task, transcript
 *        location, efficiency nudge, and the {ok, reason} contract.
 *   G13. runAgentHook resolves the session transcript path, embeds it in the system
 *        prompt, AND adds a scoped `file_read` allow rule for exactly that file.
 *   Regression: PROMPT hooks (single-shot, no transcript) get NEITHER the agent
 *        system prompt NOR the transcript Read rule.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  HookManager,
  buildAgentHookSystemPrompt,
  type PreToolUseContext,
} from '../src/hooks';
import { journalPathFor } from '../src/reference-journal';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

function captureEngine(onCfg: (cfg: any) => void) {
  return (cfg: any) => {
    onCfg(cfg);
    return { async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":true}' }; }, shutdown() {} };
  };
}

function tmpCwdWithHook(hook: Record<string, unknown>): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w92-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({ hooks: [{ event: 'preToolUse', hook }] }));
  return dir;
}

async function runPreToolUseHook(dir: string, sessionId?: string): Promise<any> {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.setCwd(dir);
  hm.loadFromFile(dir);
  if (sessionId) hm.setSessionId(sessionId);
  hm.setProviderConfig({ provider: 'anthropic', model: 'claude-opus-5', apiKey: 'sk-test' });
  let cfg: any = null;
  hm.setEngineFactory(captureEngine(c => { cfg = c; }));
  await hm.executePreToolUse(preCtx);
  return cfg;
}

describe('U007-w92 G12: agent-hook verification system prompt', () => {
  it('buildAgentHookSystemPrompt frames the task, nudges efficiency, and states the {ok,reason} contract', () => {
    const sp = buildAgentHookSystemPrompt();
    assert.match(sp, /verify/i);
    assert.match(sp, /use as few steps as possible/i);
    assert.match(sp, /be efficient and direct/i);
    assert.match(sp, /"ok": true/);
    assert.match(sp, /"ok": false/);
    assert.match(sp, /"reason"/);
  });

  it('an agent hook passes the verification system prompt via appendSystemPrompt', async () => {
    const dir = tmpCwdWithHook({ type: 'agent', prompt: 'verify the plan', timeout: 5 });
    const cfg = await runPreToolUseHook(dir);
    assert.equal(typeof cfg.appendSystemPrompt, 'string');
    assert.match(cfg.appendSystemPrompt, /verify/i);
    assert.match(cfg.appendSystemPrompt, /be efficient and direct/i);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('U007-w92 G13: transcript path + scoped Read rule for the verification agent', () => {
  it('includes the transcript path in the system prompt when a session is active', () => {
    const sp = buildAgentHookSystemPrompt('/home/u/.ami/projects/slug/sess-1.jsonl');
    assert.match(sp, /\/home\/u\/\.ami\/projects\/slug\/sess-1\.jsonl/);
    assert.match(sp, /transcript/i);
  });

  it('omits the transcript sentence when no transcript path is known', () => {
    const sp = buildAgentHookSystemPrompt(undefined);
    assert.doesNotMatch(sp, /transcript is available at/i);
  });

  it('an agent hook embeds the resolved transcript path AND a scoped file_read allow rule', async () => {
    const dir = tmpCwdWithHook({ type: 'agent', prompt: 'verify', timeout: 5 });
    const cfg = await runPreToolUseHook(dir, 'sess-w92');
    const expected = journalPathFor(dir, 'sess-w92');
    // system prompt carries the exact transcript file location
    assert.ok(cfg.appendSystemPrompt.includes(expected), cfg.appendSystemPrompt);
    // scoped auto-allow rule so the agent can read exactly that file without a prompt
    assert.ok(Array.isArray(cfg.permissionRules), 'permissionRules must be present');
    const rule = cfg.permissionRules.find((r: any) => r.tool === 'file_read' && r.pattern === expected);
    assert.ok(rule, `expected a file_read allow rule for ${expected}, got ${JSON.stringify(cfg.permissionRules)}`);
    assert.equal(rule.action, 'allow');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('an agent hook with no active session still gets the system prompt but no transcript Read rule', async () => {
    const dir = tmpCwdWithHook({ type: 'agent', prompt: 'verify', timeout: 5 });
    const cfg = await runPreToolUseHook(dir); // no sessionId
    assert.equal(typeof cfg.appendSystemPrompt, 'string');
    assert.ok(!cfg.permissionRules || cfg.permissionRules.length === 0, 'no transcript rule without a session');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe('U007-w92 regression: prompt hooks do NOT get the agent system prompt / transcript rule', () => {
  it('a prompt hook receives neither appendSystemPrompt nor a transcript Read rule', async () => {
    const dir = tmpCwdWithHook({ type: 'prompt', prompt: 'is it ok?', timeout: 5 });
    const cfg = await runPreToolUseHook(dir, 'sess-w92');
    assert.equal(cfg.appendSystemPrompt, undefined);
    assert.ok(!cfg.permissionRules || cfg.permissionRules.length === 0);
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
