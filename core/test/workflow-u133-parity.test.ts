import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  workflowTool,
  writeTaskOutputNoFollow,
} from '../src/tools/workflow';
import type { ToolContext } from '../src/types';

// U133 — Workflow tool parity (additive): per-agent model override, per-agent
// permission-mode knob, symlink-safe (O_NOFOLLOW) output writes, structured
// error on unreadable workflow script (TOCTOU), and a resume/continuation
// affordance hint in both the success and failure output.
//
// Each test discriminates the new behavior from the pre-fix behavior (RED notes
// inline) so it is a genuine red-green reproducer.

const dirs: string[] = [];
afterEach(() => { for (const d of dirs.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });
function mkcwd(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'si-wf-u133-'));
  dirs.push(d);
  return d;
}

/** Context with a capturing engine factory so we can assert the per-agent
 *  EngineConfig the workflow handler builds. */
function capturingCtx(cwd: string): { ctx: ToolContext; configs: any[] } {
  const configs: any[] = [];
  const ctx: any = {
    cwd,
    abortSignal: new AbortController().signal,
    _providerConfig: { provider: 'test', model: 'base-model', baseUrl: 'u', apiKey: 'k' },
    _engineFactory: (config: any) => {
      configs.push(config);
      return {
        async *submit() { yield { type: 'text_delta', text: 'ok' } as any; },
        shutdown() {},
      };
    },
  };
  return { ctx, configs };
}

describe('U133 workflow — per-agent model override knob', () => {
  it('applies opts.model to the sub-engine provider config', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', { model: 'opus-x' })";
    const r = await workflowTool.execute!({ script }, ctx);
    // RED (unwired): handler passes context._providerConfig verbatim, so
    // provider.model stays 'base-model' regardless of opts.model.
    assert.equal(r.isError, false);
    assert.equal(configs.length, 1);
    assert.equal(configs[0].provider.model, 'opus-x');
  });

  it('leaves provider config unchanged when no model override is given', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd);
    const script = "export const meta = { name: 'm', description: 'd' }\nreturn await agent('do', {})";
    await workflowTool.execute!({ script }, ctx);
    assert.equal(configs[0].provider.model, 'base-model');
    // Must not mutate the shared provider config object.
    assert.equal((ctx as any)._providerConfig.model, 'base-model');
  });
});

describe('U133 workflow — per-agent permission-mode knob', () => {
  it('honors a valid opts.permissionMode on the sub-engine', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd);
    const script = "export const meta = { name: 'p', description: 'd' }\nreturn await agent('do', { permissionMode: 'plan' })";
    await workflowTool.execute!({ script }, ctx);
    // RED (hardcoded): handler always set permissionMode: 'auto-allow'.
    assert.equal(configs[0].permissionMode, 'plan');
  });

  it('defaults to auto-allow when no permission mode is given', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd);
    const script = "export const meta = { name: 'p', description: 'd' }\nreturn await agent('do', {})";
    await workflowTool.execute!({ script }, ctx);
    assert.equal(configs[0].permissionMode, 'auto-allow');
  });

  it('falls back to auto-allow for an unrecognized permission mode', async () => {
    const cwd = mkcwd();
    const { ctx, configs } = capturingCtx(cwd);
    const script = "export const meta = { name: 'p', description: 'd' }\nreturn await agent('do', { permissionMode: 'bogus' })";
    await workflowTool.execute!({ script }, ctx);
    assert.equal(configs[0].permissionMode, 'auto-allow');
  });
});

describe('U133 workflow — symlink-safe (O_NOFOLLOW) output writes', () => {
  it('writeTaskOutputNoFollow writes a normal file', () => {
    const cwd = mkcwd();
    const p = path.join(cwd, 'out.txt');
    writeTaskOutputNoFollow(p, 'hello');
    assert.equal(fs.readFileSync(p, 'utf-8'), 'hello');
  });

  it('refuses to follow a symlink and never clobbers the symlink target', () => {
    const cwd = mkcwd();
    const target = path.join(cwd, 'secret.txt');
    fs.writeFileSync(target, 'ORIGINAL');
    const link = path.join(cwd, 'agent.out');
    fs.symlinkSync(target, link);
    // RED (plain writeFileSync): follows the symlink and overwrites 'secret.txt'.
    assert.throws(() => writeTaskOutputNoFollow(link, 'ATTACK'), /ELOOP|EEXIST/);
    assert.equal(fs.readFileSync(target, 'utf-8'), 'ORIGINAL');
  });
});

describe('U133 workflow — structured error on unreadable script (TOCTOU)', () => {
  it('returns a clean error instead of throwing when scriptPath is a directory', async () => {
    const cwd = mkcwd();
    const dir = path.join(cwd, 'adir');
    fs.mkdirSync(dir);
    const ctx: any = { cwd, abortSignal: new AbortController().signal };
    // RED (un-wrapped readFileSync): existsSync(dir) is true, then
    // readFileSync(dir) throws EISDIR and escapes execute() as a rejection.
    const r = await workflowTool.execute!({ scriptPath: dir }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /cannot read workflow script/);
  });

  it('returns a clean error when a saved-name script is unreadable', async () => {
    const cwd = mkcwd();
    const wfDir = path.join(cwd, '.ami', 'workflows');
    fs.mkdirSync(wfDir, { recursive: true });
    // Make the resolved path a directory so readFileSync throws EISDIR.
    fs.mkdirSync(path.join(wfDir, 'broken.js'));
    const ctx: any = { cwd, abortSignal: new AbortController().signal };
    const r = await workflowTool.execute!({ name: 'broken' }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /cannot read workflow script/);
  });
});

describe('U133 workflow — resume/continuation affordance hint', () => {
  it('appends a resumeKey hint to the completion output', async () => {
    const cwd = mkcwd();
    const ctx: any = { cwd, abortSignal: new AbortController().signal };
    const script = "export const meta = { name: 'h', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, resumeKey: 'myk' }, ctx);
    assert.equal(r.isError, false);
    // RED (no affordance): output never mentions how to resume.
    assert.match(r.output, /resumeKey 'myk'/);
  });

  it('appends a resumeFromRunId hint to the completion output', async () => {
    const cwd = mkcwd();
    const ctx: any = { cwd, abortSignal: new AbortController().signal };
    const script = "export const meta = { name: 'h', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script, runId: 'runZ' }, ctx);
    assert.equal(r.isError, false);
    assert.match(r.output, /resumeFromRunId 'runZ'/);
  });

  it('includes the resume hint on the failure path too', async () => {
    const cwd = mkcwd();
    const ctx: any = { cwd, abortSignal: new AbortController().signal };
    // require() trips validateWorkflowScript inside executeWorkflow -> catch.
    const script = "export const meta = { name: 'h', description: 'd' }\nconst x = require('fs')\nreturn 1";
    const r = await workflowTool.execute!({ script, resumeKey: 'failk' }, ctx);
    assert.equal(r.isError, true);
    assert.match(r.output, /Error executing workflow/);
    assert.match(r.output, /resumeKey 'failk'/);
  });

  it('emits no resume hint when neither resumeKey nor runId is supplied', async () => {
    const cwd = mkcwd();
    const ctx: any = { cwd, abortSignal: new AbortController().signal };
    const script = "export const meta = { name: 'h', description: 'd' }\nreturn 1";
    const r = await workflowTool.execute!({ script }, ctx);
    assert.equal(r.isError, false);
    assert.doesNotMatch(r.output, /resumeKey|resumeFromRunId/);
  });
});
