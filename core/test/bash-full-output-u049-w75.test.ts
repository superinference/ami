/**
 * U049 (w75) — E2E: a large bash command's COMPLETE stdout is spilled to disk,
 * so the middle that AMI's in-memory head+tail window drops is still retrievable
 * via file_read. Parity with Claude Code's persist-complete-output model
 * (BashTool.tsx:L725-753).
 *
 * RED (before the fix): bash spilled `presented.stdout`, the already-truncated
 * ~1MB head+tail buffer, so the spill file never contained the dropped middle.
 * GREEN (with the fix): bash reads the shell layer's persisted full-stdout file
 * and spills THAT, so the spill file contains the middle marker.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';
import { projectStateDir } from '../src/reference-journal';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

describe('bashTool – full-output persistence (U049 w75)', () => {
  it('spills the COMPLETE stdout so a marker in the dropped middle survives', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-fullout-'));
    try {
      // Produce >1MB of stdout with a unique marker at ~offset 700000, which lies
      // inside the dropped middle region [HEAD=670000, total-TAIL=330000) for a
      // ~1.2MB total — i.e. a byte the head+tail window irrecoverably discards.
      const marker = 'MIDDLE_MARKER_c0ffee_U049';
      const command =
        `python3 -c "import sys; sys.stdout.write('A'*700000 + '${marker}' + 'B'*500000)"`;
      const result = await bashTool.execute({ command }, ctx({ cwd: dir }));

      // The inline preview is a bounded head+tail + a persisted banner; the marker
      // from the middle is NOT in the inline output.
      assert.match(result.output, /persisted/, 'large output was spilled to disk');
      assert.equal(result.output.includes(marker), false, 'the dropped middle is absent from the inline preview');

      // The spilled file on disk must contain the COMPLETE output, including the
      // middle marker — this is the byte the pre-fix head+tail spill would lose.
      const spillDir = projectStateDir(dir, 'tool-results');
      const spills = fs.readdirSync(spillDir).filter(f => f.startsWith('bash-'));
      assert.ok(spills.length > 0, 'a bash spill file was written');
      const found = spills.some(f => fs.readFileSync(path.join(spillDir, f), 'utf8').includes(marker));
      assert.ok(found, 'the complete output (with the dropped-middle marker) is retrievable from disk');
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});
