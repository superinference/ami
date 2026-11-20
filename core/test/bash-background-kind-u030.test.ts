/**
 * U030 — model-facing `background_kind` ('bash' | 'monitor') on the bash tool.
 *
 * A background shell command may be spawned as a 'monitor' (a long-lived stream
 * expected to idle between bursts). The schema advertises the knob, and the tool
 * threads an explicit 'monitor' into ProcessManager.spawn so the stall watchdog is
 * disabled and completion is worded as a stream ending. Parity: the reference's
 * BashTaskKind threaded through the spawn input (LocalShellTask).
 *
 * RED-GREEN: remove the `background_kind` → spawn `kind` threading in bash.ts and
 * the "passes monitor" test fails (captured opts carry no kind).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { bashTool, buildBashInputSchema } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function baseCtx(overrides: Partial<ToolContext> = {}): ToolContext {
  return { cwd: process.cwd(), abortSignal: new AbortController().signal, ...overrides } as ToolContext;
}

/** Capturing fake ProcessManager — records the opts passed to spawn(). */
function capturingPm(): { pm: unknown; lastOpts: () => Record<string, unknown> | undefined } {
  let opts: Record<string, unknown> | undefined;
  const pm = {
    spawn: (_cmd: string, o: Record<string, unknown>) => { opts = o; return 'task-x'; },
    get: () => ({ pid: 1, outputPath: '/tmp/task-x.output' }),
  };
  return { pm, lastOpts: () => opts };
}

describe('buildBashInputSchema — background_kind', () => {
  it('advertises background_kind with the bash|monitor enum by default', () => {
    const schema = buildBashInputSchema(false, false);
    assert.ok('background_kind' in schema.properties);
    assert.deepEqual(schema.properties.background_kind.enum, ['bash', 'monitor']);
  });

  it('omits background_kind when background tasks are disabled by policy', () => {
    const schema = buildBashInputSchema(true, false);
    assert.ok(!('background_kind' in schema.properties));
    // run_in_background is dropped alongside it; command stays required/present.
    assert.ok(!('run_in_background' in schema.properties));
    assert.ok('command' in schema.properties);
  });
});

describe('bashTool.execute — background_kind → spawn kind threading', () => {
  it('passes kind:"monitor" to spawn for a monitor background run', async () => {
    const { pm, lastOpts } = capturingPm();
    const res = await bashTool.execute(
      { command: 'tail -f app.log', run_in_background: true, background_kind: 'monitor' },
      baseCtx({ processManager: pm } as Partial<ToolContext>),
    );
    assert.equal(res.isError, false);
    assert.equal(lastOpts()?.kind, 'monitor', 'the monitor kind is threaded into spawn');
  });

  it('passes NO kind for a default background run (spawn then defaults to bash)', async () => {
    const { pm, lastOpts } = capturingPm();
    await bashTool.execute(
      { command: 'npm run build', run_in_background: true },
      baseCtx({ processManager: pm } as Partial<ToolContext>),
    );
    assert.equal('kind' in (lastOpts() ?? {}), false, 'no kind key when none was requested');
  });

  it('treats background_kind:"bash" as the default (no explicit kind threaded)', async () => {
    const { pm, lastOpts } = capturingPm();
    await bashTool.execute(
      { command: 'npm test', run_in_background: true, background_kind: 'bash' },
      baseCtx({ processManager: pm } as Partial<ToolContext>),
    );
    assert.equal('kind' in (lastOpts() ?? {}), false, 'explicit "bash" is the default, not an override');
  });
});
