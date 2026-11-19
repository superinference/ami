/**
 * bash tool × headless terminal emulator — END-TO-END integration + RED-GREEN
 * reproducer.
 *
 * Drives the REAL bashTool.execute with a command that emits a `\r`-driven
 * progress bar and asserts the model-visible output is the FINAL rendered line,
 * not the raw "ANSI soup" of superimposed redraws (Orca "headless terminal
 * emulator for the bash tool" parity).
 *
 * REGRESSION LOCK / RED-GREEN: the discriminating assertions below PASS only with
 * the headless fold wired into presentCommandOutput and FAIL if that wiring is
 * reverted:
 *   - with the fold: stdout === 'Progress: 100%' and NO '10%'/'50%' frames.
 *   - without the fold: stdout contains the raw '10%\rProgress: 50%\r…' soup, so
 *     both `assert.equal(..., 'Progress: 100%')` and the `doesNotMatch` fail.
 * The `AMI_BASH_HEADLESS_RENDER=off` case proves the kill switch restores the raw
 * bytes, so the two cases cannot both pass with one implementation.
 *
 * CORE ISOLATION: imports only from ../src/tools/bash. No common/cli/vscode.
 */

import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';

import { bashTool } from '../src/tools/bash';
import { BASH_HEADLESS_RENDER_ENV } from '../src/tools/bash/headless-output';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// Commands whose stdout overwrites the same line three times via `\r`, ending
// with a newline — the canonical progress-bar shape. Distinct labels per test so
// the read-only bash result cache cannot serve one test's output to another.
const progressCmd = (label: string): string =>
  `printf '${label}: 10%%\\r${label}: 50%%\\r${label}: 100%%\\n'`;

const savedSandbox = process.env.AMI_SANDBOX;
const savedMode = process.env[BASH_HEADLESS_RENDER_ENV];

describe('bashTool × headless render — end-to-end', () => {
  before(() => {
    // Deterministic, no container wrapping for this non-container exec test.
    process.env.AMI_SANDBOX = 'off';
  });
  after(() => {
    if (savedSandbox === undefined) delete process.env.AMI_SANDBOX;
    else process.env.AMI_SANDBOX = savedSandbox;
    if (savedMode === undefined) delete process.env[BASH_HEADLESS_RENDER_ENV];
    else process.env[BASH_HEADLESS_RENDER_ENV] = savedMode;
  });

  it('RED-GREEN: collapses a \\r progress bar to the final line (default auto)', async () => {
    delete process.env[BASH_HEADLESS_RENDER_ENV]; // default = auto
    const res = await bashTool.execute({ command: progressCmd('Download') }, ctx());
    assert.equal(res.isError, false, res.output);
    // Discriminating assertion: the final rendered line, no superimposed frames.
    assert.match(res.output, /Download: 100%/);
    assert.doesNotMatch(
      res.output,
      /Download: 10%|Download: 50%/,
      'headless fold must erase the earlier redraws — raw ANSI soup regression',
    );
    // Side-band metadata flags that the screen was flattened.
    const meta = res.metadata as { headlessRender?: { used?: boolean } } | undefined;
    assert.equal(meta?.headlessRender?.used, true, 'headlessRender metadata must be present when folded');
  });

  it('RED-GREEN: AMI_BASH_HEADLESS_RENDER=off restores the raw bytes', async () => {
    process.env[BASH_HEADLESS_RENDER_ENV] = 'off';
    const res = await bashTool.execute({ command: progressCmd('Loading') }, ctx());
    assert.equal(res.isError, false, res.output);
    // With the kill switch the earlier frames survive (raw bytes), proving the
    // two modes genuinely diverge.
    assert.match(res.output, /Loading: 10%/);
    const meta = res.metadata as { headlessRender?: unknown } | undefined;
    assert.equal(meta?.headlessRender, undefined, 'no headlessRender metadata when disabled');
  });

  it('surfaces OSC 7 cwd + OSC title side-band metadata when control-heavy', async () => {
    delete process.env[BASH_HEADLESS_RENDER_ENV];
    // OSC 7 cwd + OSC 0 title followed by a \r overwrite so the heuristic fires.
    const res = await bashTool.execute(
      {
        command:
          "printf '\\033]7;file://host/tmp/wdir\\007\\033]0;job-title\\007step 1\\rdone\\n'",
      },
      ctx(),
    );
    assert.equal(res.isError, false, res.output);
    const meta = res.metadata as {
      headlessRender?: { used?: boolean; cwd?: string; title?: string };
    } | undefined;
    assert.equal(meta?.headlessRender?.used, true);
    assert.equal(meta?.headlessRender?.cwd, '/tmp/wdir');
    assert.equal(meta?.headlessRender?.title, 'job-title');
  });

  it('leaves ordinary multi-line output untouched (no false positives)', async () => {
    delete process.env[BASH_HEADLESS_RENDER_ENV];
    const res = await bashTool.execute(
      { command: "printf 'alpha\\nbeta\\ngamma\\n'" },
      ctx(),
    );
    assert.equal(res.isError, false, res.output);
    assert.match(res.output, /alpha\nbeta\ngamma/);
    const meta = res.metadata as { headlessRender?: unknown } | undefined;
    assert.equal(meta?.headlessRender, undefined, 'plain output must not trigger the fold');
  });
});
