import { describe, it, beforeEach, before, after, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { taskTool, resetAgentRegistry } from '../src/tools/task';
import * as logger from '../src/logger';
import type { ToolContext, ToolDefinition, EngineConfig } from '../src/types';

type Ev = Record<string, unknown>;

interface Harness {
  isError?: boolean;
  output: string;
  cfg: EngineConfig | null;
}

/** Run the task tool against a fake engine. */
async function runTask(
  input: Record<string, unknown>,
  ctx: Partial<ToolContext> = {},
  events: Ev[] = [{ type: 'text_delta', text: 'ok' }],
): Promise<Harness> {
  let cfg: EngineConfig | null = null;
  async function* fakeSubmit(_p?: string) { for (const e of events) yield e as never; }
  const result = await taskTool.execute(input, {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    _providerConfig: { baseUrl: 'http://x', apiKey: 'k', model: 'claude-opus-4-8' },
    _engineFactory: (c) => { cfg = c; void (c.tools as ToolDefinition[]); return { submit: fakeSubmit, shutdown: () => {} }; },
    ...ctx,
  } as unknown as ToolContext);
  return { isError: result.isError, output: result.output, cfg };
}

let dataDir: string;
before(() => {
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w81-'));
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
});
after(() => {
  delete process.env.SUPERINFERENCE_DATA_DIR;
  try { fs.rmSync(dataDir, { recursive: true, force: true }); } catch { /* best-effort */ }
});

// ===========================================================================
// Gap: reuse/continuation-before-spawn heuristic in the task tool description
// (parity: claudeCodeGuideAgent whenToUse — "Before spawning a new agent, check
// if there is already a running or recently completed agent you can continue").
// ===========================================================================
describe('task tool description — reuse-before-spawn guidance (U011/w81)', () => {
  it('steers the model to continue an existing same-kind agent before spawning a duplicate', () => {
    const d = taskTool.description;
    // RED on revert: the old description never mentions continuing an existing agent.
    assert.match(d, /before spawning a\s+new agent/i);
    assert.match(d, /running or recently-completed/i);
    assert.match(d, /instead of spawning a duplicate/i);
    // It points at the real continuation mechanisms AMI already exposes.
    assert.match(d, /resume/i);
    assert.match(d, /send_message/i);
  });
});

// ===========================================================================
// Gap: debug/introspection hint when a resumed agent's recorded worktree/cwd was
// removed and the spawn falls back to the parent cwd (parity: resumeAgent.ts
// L80-92 — besides the user-visible header note, log a debug hint).
// ===========================================================================
describe('resume worktree-gone fallback — debug-log introspection hint (U011/w81)', () => {
  let prevLevel: string | undefined;
  beforeEach(() => resetAgentRegistry());
  before(() => {
    prevLevel = process.env.AMI_DEBUG_LOG_LEVEL;
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
    delete process.env.AMI_DISABLE_DEBUG_LOG;
    delete process.env.AMI_DEBUG_COMPONENTS;
  });
  after(() => {
    if (prevLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL;
    else process.env.AMI_DEBUG_LOG_LEVEL = prevLevel;
  });

  it('emits BOTH a header note AND a debug-log hint naming the missing + fallback cwd', async () => {
    // A parent cwd (its subdir is a valid, in-boundary spawn cwd) and a child dir
    // the agent runs in — recorded in the agent registry on spawn.
    const parent = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u011-w81-parent-'));
    const child = path.join(parent, 'wt');
    fs.mkdirSync(child);

    // 1) Spawn a named agent in the child dir so its cwd is recorded for resume.
    const spawn = await runTask(
      { prompt: 'start', mode: 'explore', name: 'builder', cwd: child },
      { cwd: parent, allowedReadRoots: [parent] },
    );
    assert.ok(!spawn.isError, `spawn should succeed: ${spawn.output}`);

    // 2) The worktree/cwd is cleaned up out from under the agent.
    fs.rmSync(child, { recursive: true, force: true });

    // 3) Capture every debug line the resume emits.
    const captured: string[] = [];
    const appendMock = mock.method(logger.fsHooks, 'appendFileSync', (_f: unknown, line: unknown) => {
      captured.push(String(line));
    });
    // mkdir of the (tmp) log dir is a harmless no-op under the mock.
    const mkdirMock = mock.method(logger.fsHooks, 'mkdirSync', () => undefined);

    let resume: Harness;
    try {
      resume = await runTask(
        { prompt: 'again', resume: 'builder' },
        { cwd: parent, allowedReadRoots: [parent] },
      );
    } finally {
      appendMock.mock.restore();
      mkdirMock.mock.restore();
    }

    assert.ok(!resume.isError, `resume should succeed: ${resume.output}`);
    // User-visible header note (pre-existing behavior) still present.
    assert.match(resume.output, /no longer exists/i);

    // Debug-log hint (the new introspection surface) names the missing + fallback cwd.
    const hint = captured.find(l => /resume:.*worktree\/cwd missing/i.test(l));
    assert.ok(hint, `expected a resume-fallback debug line, got: ${captured.join('\n')}`);
    assert.ok(hint!.includes(child), 'debug hint names the missing recorded cwd');
    assert.ok(hint!.includes(parent), 'debug hint names the fallback (parent) cwd');

    fs.rmSync(parent, { recursive: true, force: true });
  });
});
