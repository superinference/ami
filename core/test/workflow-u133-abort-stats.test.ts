import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { workflowTool } from '../src/tools/workflow';
import type { ToolContext } from '../src/types';

// U133 (continuation) — additive Workflow tool parity:
//  - per-agent cancellation: each agent gets its own AbortController wired to the
//    process-manager abortFn and forwarded the workflow-level abort signal;
//  - completion output reports aggregate tokens, tool uses, and wall-clock duration;
//  - live progress: partial agent output is flushed to disk and the live token
//    count is mirrored onto the process entry while the agent is still running;
//  - self-describing, high-entropy agent ids (generateTaskId: randomBytes(8) with
//    an embedded sanitized label prefix).
//
// Each test discriminates the new behavior from the pre-fix behavior (RED note
// inline) so it is a genuine red-green reproducer.

const dirs: string[] = [];
afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
function mkcwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-u133x-'));
  dirs.push(d);
  return d;
}

type Registered = { id: string; opts: { command: string; description: string; outputPath: string; abortFn: () => void } };
type Update = { id: string; upd: Record<string, unknown> };

/** Minimal fake ProcessManager capturing registerAgent / updateEntry. */
function fakePm() {
  const registered: Registered[] = [];
  const updates: Update[] = [];
  return {
    registered,
    updates,
    flowManager: undefined as unknown,
    registerAgent(id: string, opts: Registered['opts']) { registered.push({ id, opts }); },
    updateEntry(id: string, upd: Record<string, unknown>) { updates.push({ id, upd }); },
  };
}

/** Context with a capturing engine factory + fake pm. The supplied submit
 *  generator drives the per-agent event stream. */
function ctxWith(
  cwd: string,
  submitGen: (config: any) => AsyncIterable<any>,
  opts?: { pm?: ReturnType<typeof fakePm>; abortSignal?: AbortSignal },
): { ctx: ToolContext; configs: any[] } {
  const configs: any[] = [];
  const ctx: any = {
    cwd,
    abortSignal: opts?.abortSignal ?? new AbortController().signal,
    processManager: opts?.pm,
    _providerConfig: { provider: 'test', model: 'base-model', baseUrl: 'u', apiKey: 'k' },
    _engineFactory: (config: any) => {
      configs.push(config);
      return { submit: () => submitGen(config), shutdown() {} };
    },
  };
  return { ctx, configs };
}

async function* oneText(text: string) { yield { type: 'text_delta', text }; }

describe('U133x workflow — per-agent cancellation wiring', () => {
  it('passes a per-agent AbortController to the engine factory and wires abortFn to it', async () => {
    const cwd = mkcwd();
    const pm = fakePm();
    const { ctx, configs } = ctxWith(cwd, () => oneText('ok'), { pm });
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do')";
    const r = await workflowTool.execute!({ script, run_in_background: false }, ctx);
    assert.equal(r.isError, false);
    // RED (unwired): config had no abortController and abortFn was `() => {}`.
    const cfg = configs[0];
    assert.ok(cfg.abortController instanceof AbortController, 'per-agent abortController passed to engine');
    const reg = pm.registered[0];
    assert.equal(cfg.abortController.signal.aborted, false);
    reg.opts.abortFn();
    assert.equal(cfg.abortController.signal.aborted, true, 'abortFn aborts the per-agent controller');
  });

  it('forwards the workflow-level abort into the per-agent controller mid-run', async () => {
    const cwd = mkcwd();
    const outer = new AbortController();
    const { ctx } = ctxWith(cwd, async function* (config: any) {
      outer.abort(); // workflow cancelled while the agent is streaming
      yield { type: 'text_delta', text: config.abortController.signal.aborted ? 'ABORTED' : 'LIVE' };
    }, { abortSignal: outer.signal });
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do')";
    const r = await workflowTool.execute!({ script, run_in_background: false }, ctx);
    // RED (no forwarding): aborting the outer signal never reaches the agent.
    assert.match(r.output, /ABORTED/);
  });
});

describe('U133x workflow — completion usage stats', () => {
  it('reports aggregate tokens, tool uses, and duration on the completion output', async () => {
    const cwd = mkcwd();
    const { ctx } = ctxWith(cwd, async function* () {
      yield { type: 'tool_use_start', toolName: 'bash', toolCallId: '1', input: {} };
      yield { type: 'tool_use_start', toolName: 'grep', toolCallId: '2', input: {} };
      yield { type: 'usage_update', stats: { totalTokens: 123 } };
      yield { type: 'text_delta', text: 'hi' };
    });
    const script = "export const meta = { name: 's', description: 'd' }\nreturn await agent('do')";
    const r = await workflowTool.execute!({ script, run_in_background: false }, ctx);
    assert.equal(r.isError, false);
    // RED (no stats line): completion output never mentioned tokens/tool uses/duration.
    assert.match(r.output, /Tokens: 123/);
    assert.match(r.output, /Tool uses: 2/);
    assert.match(r.output, /Duration: \d+\.\d+s/);
  });

  it('aggregates tokens and tool uses across multiple agents', async () => {
    const cwd = mkcwd();
    const { ctx } = ctxWith(cwd, async function* () {
      yield { type: 'tool_use_start', toolName: 'bash', toolCallId: '1', input: {} };
      yield { type: 'usage_update', stats: { totalTokens: 10 } };
    });
    const script = "export const meta = { name: 's', description: 'd' }\nawait agent('a')\nawait agent('b')\nreturn 1";
    const r = await workflowTool.execute!({ script, run_in_background: false }, ctx);
    assert.match(r.output, /Tokens: 20/);
    assert.match(r.output, /Tool uses: 2/);
  });
});

describe('U133x workflow — live progress', () => {
  it('flushes partial output to disk and mirrors a live token count before the agent finishes', async () => {
    const cwd = mkcwd();
    const pm = fakePm();
    let fileAtTokenUpdate: string | null = null;
    // When the live token count is written, the first text chunk must already be
    // on disk — proving the flush happened mid-run, not only at completion.
    const origUpdate = pm.updateEntry.bind(pm);
    pm.updateEntry = (id: string, upd: Record<string, unknown>) => {
      if (upd.tokenCount !== undefined) {
        const op = pm.registered[0]?.opts.outputPath;
        fileAtTokenUpdate = op && fs.existsSync(op) ? fs.readFileSync(op, 'utf-8') : null;
      }
      origUpdate(id, upd);
    };
    const { ctx } = ctxWith(cwd, async function* () {
      yield { type: 'text_delta', text: 'partial-live' }; // flushed immediately
      yield { type: 'usage_update', stats: { totalTokens: 7 } }; // triggers tokenCount update
    }, { pm });
    const script = "export const meta = { name: 'p', description: 'd' }\nreturn await agent('do')";
    const r = await workflowTool.execute!({ script, run_in_background: false }, ctx);
    assert.equal(r.isError, false);
    // RED (no live flush): file was empty until finish → fileAtTokenUpdate !== 'partial-live'.
    // RED (no live token mirror): updateEntry never got a tokenCount → fileAtTokenUpdate stays null.
    assert.equal(fileAtTokenUpdate, 'partial-live');
    assert.ok(pm.updates.some(u => u.upd.tokenCount === 7), 'live token count mirrored onto process entry');
  });
});

describe('U133x workflow — self-describing high-entropy agent ids', () => {
  it('derives a labelled, 16-hex-char agent id (randomBytes(8))', async () => {
    const cwd = mkcwd();
    const pm = fakePm();
    const { ctx } = ctxWith(cwd, () => oneText('ok'), { pm });
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { label: 'Build Site' })";
    await workflowTool.execute!({ script, run_in_background: false }, ctx);
    const id = pm.registered[0].id;
    // RED (old): id was `agent-<8 hex>` with NO embedded label and only 2^32 entropy.
    assert.match(id, /^agent-Build_Site-[0-9a-f]{16}$/);
  });

  it('falls back to a plain high-entropy id when no label slug survives sanitization', async () => {
    const cwd = mkcwd();
    const pm = fakePm();
    const { ctx } = ctxWith(cwd, () => oneText('ok'), { pm });
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { label: '!!!' })";
    await workflowTool.execute!({ script, run_in_background: false }, ctx);
    const id = pm.registered[0].id;
    assert.match(id, /^agent-[0-9a-f]{16}$/);
  });
});
