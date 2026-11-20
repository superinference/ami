import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  filterToolsForAgent,
  wrapExploreReadOnlyBash,
  EXPLORE_READ_ONLY_TOOLS,
  EXPLORE_BASH_WRITE_REFUSAL,
} from '../src/tools/task';
import { bashTool } from '../src/tools/bash';
import { createDefaultTools } from '../src/tools/index';
import type { ToolContext, ToolDefinition, ToolResult } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

/** A minimal fake bash tool that records whether its execute was reached. */
function fakeBash(): { tool: ToolDefinition; calls: Array<Record<string, unknown>> } {
  const calls: Array<Record<string, unknown>> = [];
  const tool: ToolDefinition = {
    name: 'bash',
    description: 'fake bash',
    inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] },
    isReadOnly: false,
    isReadOnlyForInput: () => false,
    async execute(input): Promise<ToolResult> {
      calls.push(input);
      return { output: 'RAN', isError: false };
    },
  };
  return { tool, calls };
}

describe('U011 w78 — explore mode retains read-only bash (parity: the reference Explore)', () => {
  it('EXPLORE_READ_ONLY_TOOLS includes bash', () => {
    assert.ok(EXPLORE_READ_ONLY_TOOLS.has('bash'));
  });

  it('filterToolsForAgent(explore) keeps a read-only-guarded bash', () => {
    const all = createDefaultTools(process.cwd()).getAll();
    const explore = filterToolsForAgent(all, 'explore');
    const bash = explore.find(t => t.name === 'bash');
    assert.ok(bash, 'explore mode must retain the bash tool');
    // It is the GUARDED variant: pinned read-only and with the per-input override dropped.
    assert.equal(bash!.isReadOnly, true);
    assert.equal(bash!.isReadOnlyForInput, undefined);
    // Still no edit/write tools in explore.
    assert.ok(!explore.some(t => t.name === 'file_edit'));
    assert.ok(!explore.some(t => t.name === 'file_write'));
  });

  it('filterToolsForAgent(general) keeps the UNGUARDED bash (not pinned read-only)', () => {
    const all = createDefaultTools(process.cwd()).getAll();
    const general = filterToolsForAgent(all, 'general', 0, 5);
    const bash = general.find(t => t.name === 'bash');
    assert.ok(bash);
    // General bash keeps its true (writable) classification — only explore pins it.
    assert.equal(bash!.isReadOnly, false);
    // And it is the SAME object reference as the registered tool (not wrapped).
    assert.equal(bash, all.find(t => t.name === 'bash'));
  });

  it('wrapExploreReadOnlyBash delegates a read-only command to the underlying tool', async () => {
    const { tool, calls } = fakeBash();
    const guarded = wrapExploreReadOnlyBash(tool);
    const res = await guarded.execute({ command: 'git status' }, ctx());
    assert.equal(res.output, 'RAN');
    assert.ok(!res.isError);
    assert.equal(calls.length, 1, 'underlying bash must be reached for a read-only command');
  });

  it('wrapExploreReadOnlyBash REFUSES a mutating command without reaching the tool', async () => {
    const { tool, calls } = fakeBash();
    const guarded = wrapExploreReadOnlyBash(tool);
    for (const cmd of ['rm -rf foo', 'touch x', 'mkdir d', 'git commit -m x', 'echo hi > out.txt']) {
      const res = await guarded.execute({ command: cmd }, ctx());
      assert.equal(res.isError, true, `${cmd} must be refused`);
      assert.equal(res.output, EXPLORE_BASH_WRITE_REFUSAL);
    }
    assert.equal(calls.length, 0, 'a refused command must NEVER reach the underlying bash tool');
  });

  it('wrapExploreReadOnlyBash refuses a missing / non-string command', async () => {
    const { tool, calls } = fakeBash();
    const guarded = wrapExploreReadOnlyBash(tool);
    for (const input of [{}, { command: 42 }, { command: '' }, { command: '   ' }] as Array<Record<string, unknown>>) {
      const res = await guarded.execute(input, ctx());
      assert.equal(res.isError, true);
      assert.equal(res.output, EXPLORE_BASH_WRITE_REFUSAL);
    }
    assert.equal(calls.length, 0);
  });

  it('does not mutate the original tool object', () => {
    const { tool } = fakeBash();
    const guarded = wrapExploreReadOnlyBash(tool);
    assert.notEqual(guarded, tool);
    assert.equal(tool.isReadOnly, false, 'original must be untouched');
    assert.equal(typeof tool.isReadOnlyForInput, 'function');
  });
});

describe('U011 w78 — explore read-only bash end-to-end (real bash tool)', () => {
  let dir: string;
  const prevSandbox = process.env.AMI_SANDBOX;

  before(() => {
    // Non-container bash for a deterministic on-disk side-effect assertion.
    process.env.AMI_SANDBOX = 'off';
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-explore-bash-'));
  });

  after(() => {
    if (prevSandbox === undefined) delete process.env.AMI_SANDBOX;
    else process.env.AMI_SANDBOX = prevSandbox;
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('runs a real read-only command through the guarded bash', async () => {
    const guarded = wrapExploreReadOnlyBash(bashTool);
    const res = await guarded.execute({ command: 'echo explore-ok' }, ctx({ cwd: dir }));
    assert.ok(res.output.includes('explore-ok'));
    assert.ok(!res.isError);
  });

  it('refuses a real write and leaves NO file on disk (structural read-only guarantee)', async () => {
    const guarded = wrapExploreReadOnlyBash(bashTool);
    const target = path.join(dir, 'should-not-exist.txt');
    const res = await guarded.execute({ command: `touch ${target}` }, ctx({ cwd: dir }));
    assert.equal(res.isError, true);
    assert.equal(res.output, EXPLORE_BASH_WRITE_REFUSAL);
    assert.equal(fs.existsSync(target), false, 'the refused write must not have created the file');
  });
});
