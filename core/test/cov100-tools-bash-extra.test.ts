/**
 * cov100 — bash.ts formatTimeoutBackgroundOutput
 *
 * Covers the auto-background timeout formatter (src/tools/bash.ts, the
 * `formatTimeoutBackgroundOutput` function) and BOTH arms of its single
 * branch at the "nothing captured" guard:
 *
 *   1. stdout AND stderr are blank (after trim) -> only the still-running
 *      notice is returned (no "[output so far]" section).
 *   2. some partial output was captured -> the notice is followed by an
 *      "[output so far]" block containing the formatted stdout/stderr.
 *
 * The guard deliberately decides "nothing captured" from the RAW stream
 * lengths, not by string-comparing the formatted result to its
 * "(no output)" sentinel — so a command whose real partial output is
 * literally "(no output)" must still be surfaced. That edge is asserted too.
 *
 * CORE ISOLATION: imports only from ../src/tools/bash. No common/cli/vscode.
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { formatTimeoutBackgroundOutput } from '../src/tools/bash';

describe('formatTimeoutBackgroundOutput — notice content', () => {
  it('embeds the timeout, the task id, and the task_output hint in the notice', () => {
    const out = formatTimeoutBackgroundOutput('', '', 4200, 'task-abc');
    assert.match(out, /Command timed out after 4200ms\./);
    assert.match(out, /Auto-moved to background as task task-abc\./);
    assert.match(out, /still running in the background/);
    assert.match(out, /use task_output to check progress/);
  });
});

describe('formatTimeoutBackgroundOutput — "nothing captured" arm', () => {
  it('returns ONLY the notice when both streams are empty (no output-so-far block)', () => {
    const out = formatTimeoutBackgroundOutput('', '', 1000, 't1');
    assert.doesNotMatch(out, /\[output so far\]/);
    // The notice is the entire payload — nothing appended after it.
    assert.ok(out.endsWith('use task_output to check progress.'),
      `expected notice-only, got: ${out}`);
  });

  it('treats whitespace-only streams as "nothing captured" (trim-based guard)', () => {
    const out = formatTimeoutBackgroundOutput('   \n\t ', '  \n ', 1000, 't2');
    assert.doesNotMatch(out, /\[output so far\]/);
  });
});

describe('formatTimeoutBackgroundOutput — "partial output" arm', () => {
  it('appends an [output so far] block with the captured stdout', () => {
    const out = formatTimeoutBackgroundOutput('partial-stdout-line', '', 1500, 't3');
    assert.match(out, /Auto-moved to background as task t3\./);
    assert.match(out, /\n\n\[output so far\]\n/);
    assert.match(out, /partial-stdout-line/);
  });

  it('includes captured stderr under a [stderr] label alongside stdout', () => {
    const out = formatTimeoutBackgroundOutput('out-part', 'err-part', 2000, 't4');
    assert.match(out, /\[output so far\]/);
    assert.match(out, /out-part/);
    assert.match(out, /\[stderr\]/);
    assert.match(out, /err-part/);
  });

  it('surfaces output even when only stderr was captured', () => {
    const out = formatTimeoutBackgroundOutput('', 'only-stderr', 2500, 't5');
    assert.match(out, /\[output so far\]/);
    assert.match(out, /only-stderr/);
  });

  it('surfaces partial output that is LITERALLY the "(no output)" sentinel string', () => {
    // Raw stream is non-empty (it is the 11-char string "(no output)"), so the
    // guard must NOT mistake it for "nothing captured" — the block is emitted.
    const out = formatTimeoutBackgroundOutput('(no output)', '', 3000, 't6');
    assert.match(out, /\[output so far\]/, 'a real "(no output)" payload must still be surfaced');
    assert.match(out, /\(no output\)/);
  });
});
