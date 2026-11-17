import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  bashTool,
  buildBashDescription,
  buildBashInputSchema,
  resolveBashMaxOutput,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Unit U030 (wave 70) — additive model-facing guidance on the bash tool, matching
// Claude Code's BashTool/PowerShellTool prompt sections:
//  - working-directory stability: avoid `cd`, use absolute paths (prompt.ts:L334)
//  - parallel-tool-call composition guidance (prompt.ts:L297-L302)
//  - anti-retry-loop / anti-poll guidance (prompt.ts:L310-L328)
//  - output-truncation threshold surfaced to the model (PowerShell prompt L121-125)
//  - dangerouslyDisableSandbox guardrails: $TMPDIR + do-not-reach-credential-paths
//    (prompt.ts:L228-L257)
// These are advertised through the SAME live getters the engine uses, so TUI and
// detached modes see identical guidance (single-source).
// ---------------------------------------------------------------------------

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  } as ToolContext;
}

describe('U030-w70 working-directory stability guidance', () => {
  it('tells the model to avoid cd and prefer absolute paths', () => {
    const d = buildBashDescription(false);
    assert.match(d, /avoid `cd`/);
    assert.match(d, /only `cd` when the user explicitly asks/);
  });
  it('is present regardless of the background/git toggles', () => {
    for (const bg of [false, true]) {
      for (const git of [false, true]) {
        assert.match(buildBashDescription(bg, git), /avoid `cd`/);
      }
    }
  });
});

describe('U030-w70 parallel-tool-call composition guidance', () => {
  it('steers independent commands to parallel tool calls, not && chaining', () => {
    const d = buildBashDescription(false);
    assert.match(d, /parallel bash tool calls in a single message/);
    assert.match(d, /chain only commands that genuinely\s+depend on each other/);
  });
});

describe('U030-w70 anti-retry-loop / anti-poll guidance', () => {
  it('forbids retrying a failing command in a sleep loop', () => {
    const d = buildBashDescription(false);
    // The pre-existing "Do not sleep between commands." sentence stays verbatim.
    assert.match(d, /Do not sleep between commands\./);
    assert.match(d, /Do not retry a failing command in a sleep loop/);
  });
  it('adds the do-not-poll background note only when background tasks are enabled', () => {
    assert.match(buildBashDescription(false), /do not poll it/);
    // When background is disabled the whole run_in_background section (and its
    // do-not-poll note) is dropped — but the anti-retry-loop line stays.
    const disabled = buildBashDescription(true);
    assert.ok(!/run_in_background/.test(disabled));
    assert.ok(!/do not poll it/.test(disabled));
    assert.match(disabled, /Do not retry a failing command in a sleep loop/);
  });
});

describe('U030-w70 output-truncation threshold surfaced to the model', () => {
  const SAVE = process.env.BASH_MAX_OUTPUT_LENGTH;
  afterEach(() => {
    if (SAVE === undefined) delete process.env.BASH_MAX_OUTPUT_LENGTH;
    else process.env.BASH_MAX_OUTPUT_LENGTH = SAVE;
  });

  it('states the default truncation cap when the knob is unset', () => {
    delete process.env.BASH_MAX_OUTPUT_LENGTH;
    const d = buildBashDescription(false);
    assert.match(d, new RegExp(`Output longer than ${resolveBashMaxOutput()} characters \\(BASH_MAX_OUTPUT_LENGTH\\) is truncated`));
    assert.match(d, /inspect it with file_read/);
  });

  it('reflects a custom BASH_MAX_OUTPUT_LENGTH at advertise time', () => {
    process.env.BASH_MAX_OUTPUT_LENGTH = '12345';
    // The live getter (what the engine advertises) must pick up the new cap.
    assert.match(bashTool.description, /Output longer than 12345 characters/);
  });
});

describe('U030-w70 dangerouslyDisableSandbox guardrails', () => {
  it('adds $TMPDIR + do-not-reach-credential-paths guidance to the schema', () => {
    const schema = buildBashInputSchema(false);
    const desc = (schema.properties.dangerouslyDisableSandbox as { description: string }).description;
    assert.match(desc, /\$TMPDIR \(sandbox-writable\)/);
    assert.match(desc, /never weaken the sandbox to reach/);
    assert.match(desc, /~\/\.ssh/);
  });
  it('drops the whole param (and its guidance) when the escape hatch is disabled', () => {
    const schema = buildBashInputSchema(false, /* sandboxEscapeDisabled */ true);
    assert.ok(!('dangerouslyDisableSandbox' in schema.properties));
  });
});

describe('U030-w70 live advertise path carries the new guidance (E2E)', () => {
  it('the real bashTool getters (the engine advertise path) expose every addition', () => {
    const d = bashTool.description;
    assert.match(d, /avoid `cd`/);
    assert.match(d, /parallel bash tool calls in a single message/);
    assert.match(d, /Do not retry a failing command in a sleep loop/);
    assert.match(d, /is truncated \(head \+ tail\)/);
    const schema = bashTool.inputSchema;
    const dds = schema.properties.dangerouslyDisableSandbox as { description: string } | undefined;
    assert.ok(dds, 'dangerouslyDisableSandbox advertised by default');
    assert.match(dds!.description, /\$TMPDIR/);
  });

  it('the promised truncation actually happens on a real execute', async () => {
    const prev = process.env.BASH_MAX_OUTPUT_LENGTH;
    process.env.BASH_MAX_OUTPUT_LENGTH = '2000';
    try {
      const cap = resolveBashMaxOutput();
      // Produce far more than the cap so head+tail truncation must engage. awk is
      // used (no `$()`/`<`, which the security layer blocks) to print 20000 chars.
      const res = await bashTool.execute(
        { command: `awk 'BEGIN{for(i=0;i<20000;i++)printf "x"}'` },
        ctx(),
      );
      assert.equal(res.isError, false);
      assert.match(res.output, /truncated/);
      // The returned blob is bounded by the cap (plus the truncation banner), not
      // the full 20000 chars the command printed — proving the advertised limit.
      assert.ok(res.output.length < 20000, `expected truncation under full output, got ${res.output.length}`);
      assert.ok(res.output.length <= cap + 500, `expected within cap+banner, got ${res.output.length}`);
    } finally {
      if (prev === undefined) delete process.env.BASH_MAX_OUTPUT_LENGTH;
      else process.env.BASH_MAX_OUTPUT_LENGTH = prev;
    }
  });
});
