/**
 * U051-w67 — a prompt hook is a cheap, single-shot verification verdict, so it
 * defaults to the operator-configured small/fast model (getSmallFastModel:
 * ANTHROPIC_SMALL_FAST_MODEL / AMI_SMALL_FAST_MODEL) instead of burning the main
 * model. Parity: Claude's execPromptHook `model: hook.model ?? getSmallFastModel()`.
 * With no env set the session model is kept (current behavior preserved); an
 * explicit `hook.model` always wins.
 */
import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type PreToolUseContext } from '../src/hooks';

const preCtx: PreToolUseContext = { messages: [], turnCount: 0, toolName: 'bash', toolInput: { command: 'ls' } };

function tmpCwdWithPromptHook(hook: Record<string, unknown> = { type: 'prompt', prompt: 'verify', timeout: 5 }): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-fast-'));
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'hooks.json'), JSON.stringify({
    hooks: [{ event: 'preToolUse', hook }],
  }));
  return dir;
}

function captureEngine(onCfg: (cfg: any) => void) {
  return (cfg: any) => {
    onCfg(cfg);
    return { async *submit(_p: string) { yield { type: 'text_delta', text: '{"ok":true}' }; }, shutdown() {} };
  };
}

async function runHook(dir: string): Promise<any> {
  const hm = new HookManager();
  hm.setWorkspaceTrusted(true);
  hm.loadFromFile(dir);
  hm.setProviderConfig({ provider: 'anthropic', model: 'claude-opus-5', apiKey: 'sk-test' });
  let cfg: any = null;
  hm.setEngineFactory(captureEngine(c => { cfg = c; }));
  await hm.executePreToolUse(preCtx);
  return cfg;
}

describe('prompt-hook small/fast model default (U051-w67)', () => {
  const saved = { a: process.env.ANTHROPIC_SMALL_FAST_MODEL, b: process.env.AMI_SMALL_FAST_MODEL };
  afterEach(() => {
    if (saved.a === undefined) delete process.env.ANTHROPIC_SMALL_FAST_MODEL; else process.env.ANTHROPIC_SMALL_FAST_MODEL = saved.a;
    if (saved.b === undefined) delete process.env.AMI_SMALL_FAST_MODEL; else process.env.AMI_SMALL_FAST_MODEL = saved.b;
  });

  it('uses the small/fast model when ANTHROPIC_SMALL_FAST_MODEL is set', async () => {
    delete process.env.AMI_SMALL_FAST_MODEL;
    process.env.ANTHROPIC_SMALL_FAST_MODEL = 'claude-haiku-4';
    const dir = tmpCwdWithPromptHook();
    const cfg = await runHook(dir);
    assert.equal(cfg.provider.model, 'claude-haiku-4');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('keeps the session model when no small/fast env is set (no regression)', async () => {
    delete process.env.ANTHROPIC_SMALL_FAST_MODEL;
    delete process.env.AMI_SMALL_FAST_MODEL;
    const dir = tmpCwdWithPromptHook();
    const cfg = await runHook(dir);
    assert.equal(cfg.provider.model, 'claude-opus-5');
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('an explicit hook.model overrides the small/fast default', async () => {
    process.env.ANTHROPIC_SMALL_FAST_MODEL = 'claude-haiku-4';
    const dir = tmpCwdWithPromptHook({ type: 'prompt', prompt: 'verify', timeout: 5, model: 'claude-sonnet-4-6' });
    const cfg = await runHook(dir);
    assert.equal(cfg.provider.model, 'claude-sonnet-4-6');
    fs.rmSync(dir, { recursive: true, force: true });
  });
});
