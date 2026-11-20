import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { taskTool } from '../src/tools/task';
import { ProcessManager } from '../src/process-manager';
import type { ToolContext } from '../src/types';

/* ---------------------------------------------------------------- *
 * U237 / w88 — GAP 3 (E2E): enableSummarization gate wired into the
 * background-agent drain. Parity: the reference gates live progress-summary
 * generation on enableSummarization. AMI keeps it ON by default (superset) and
 * AMI_SUBAGENT_SUMMARIES=off disables ONLY the free-text progressSummary while
 * the tool-use count is still tracked.
 *
 * Red-green: reverting the `...(summariesEnabled ? { progressSummary } : {})`
 * gate in runBackgroundAgentDrain back to an unconditional progressSummary makes
 * the "off" case below fail (progressSummary would still be set).
 * ---------------------------------------------------------------- */
function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, ...overrides } as ToolContext;
}

async function* fakeSubmit(_prompt: string) {
  yield { type: 'tool_use_start' as const, toolName: 'grep', input: { pattern: 'foo' } };
  yield { type: 'tool_use_start' as const, toolName: 'glob', input: { pattern: '*.ts' } };
  yield { type: 'text_delta' as const, text: 'done' };
}

async function runDrain(tmpDir: string, pm: ProcessManager): Promise<any> {
  const result = await taskTool.execute(
    { prompt: 'read a bunch', run_in_background: true },
    ctx({
      cwd: tmpDir,
      _providerConfig: { baseUrl: 'http://localhost', apiKey: 'k', model: 'm' },
      _engineFactory: () => ({ submit: fakeSubmit }),
      processManager: pm,
    }),
  );
  const taskId = result.output.match(/agent-[a-f0-9]+/)?.[0];
  assert.ok(taskId);
  await new Promise((r) => setTimeout(r, 150));
  return pm.list().find((e: any) => e.taskId === taskId);
}

describe('taskTool background drain — enableSummarization gate (U237)', () => {
  const prev = process.env.AMI_SUBAGENT_SUMMARIES;
  afterEach(() => {
    if (prev === undefined) delete process.env.AMI_SUBAGENT_SUMMARIES;
    else process.env.AMI_SUBAGENT_SUMMARIES = prev;
  });

  it('keeps the live progress summary ON by default (AMI superset)', async () => {
    delete process.env.AMI_SUBAGENT_SUMMARIES;
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u237-on-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const entry = await runDrain(tmpDir, pm);
      assert.ok(entry);
      assert.equal(entry.progressSummary, 'Reading/searching (2 ops)');
      assert.equal(entry.toolCount, 2);
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });

  it('suppresses the progress summary when AMI_SUBAGENT_SUMMARIES=off (count still tracked)', async () => {
    process.env.AMI_SUBAGENT_SUMMARIES = 'off';
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u237-off-'));
    const pm = new ProcessManager(tmpDir);
    try {
      const entry = await runDrain(tmpDir, pm);
      assert.ok(entry);
      assert.equal(entry.progressSummary, undefined);
      // The tool-use COUNT is always surfaced so task_list stays accurate.
      assert.equal(entry.toolCount, 2);
    } finally {
      pm.cleanup();
      fs.rmSync(tmpDir, { recursive: true, force: true });
    }
  });
});
