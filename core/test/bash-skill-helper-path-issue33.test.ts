/**
 * ui#33 — imported-skill helper scripts must be invocable by BARE name from ANY cwd.
 *
 * An imported Claude-Code skill frequently ships a helper under `<skillDir>/scripts/`.
 * The agent runs the skill from a scratch/project cwd (NOT the skill dir), so invoking the
 * helper would fail with "command not found" — the impeccable skill flaked exactly this way.
 * Fix (issue #33's suggested approach): when a skill is active (context.skillEnv carries
 * CLAUDE_SKILL_DIR), the bash subprocess env PREPENDS `<skillDir>/scripts` (and the skill
 * dir) to PATH, so a helper resolves by bare name regardless of cwd.
 *
 * RED-GREEN: without the PATH prepend in scrubEnv(), the bare-name invocation below prints
 * "command not found" and res.isError is true. With it, the helper runs and prints its
 * marker. The "no skill active → helper NOT found" case proves the prepend is SCOPED to an
 * active skill (no blanket PATH pollution).
 *
 * Run: cd core/core && node --import tsx --test test/bash-skill-helper-path-issue33.test.ts
 */
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

let skillDir: string;
let scratchCwd: string;
const MARKER = 'IMPECCABLE_HELPER_RAN_OK';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: scratchCwd, abortSignal: new AbortController().signal, ...overrides };
}

let prevSandbox: string | undefined;
before(() => {
  prevSandbox = process.env.AMI_SANDBOX;
  process.env.AMI_SANDBOX = 'off'; // deterministic: no container wrapping for these probes
  // A skill dir that ships a helper under scripts/, like an imported Claude-Code skill.
  skillDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ui33-skill-'));
  fs.mkdirSync(path.join(skillDir, 'scripts'), { recursive: true });
  const helper = path.join(skillDir, 'scripts', 'myskillhelper');
  fs.writeFileSync(helper, `#!/bin/sh\necho ${MARKER} "$@"\n`, { mode: 0o755 });
  fs.chmodSync(helper, 0o755);
  // The agent's cwd is a DIFFERENT, empty scratch dir (not the skill dir).
  scratchCwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ui33-scratch-'));
});
after(() => {
  if (prevSandbox === undefined) delete process.env.AMI_SANDBOX;
  else process.env.AMI_SANDBOX = prevSandbox;
  for (const d of [skillDir, scratchCwd]) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

describe('ui#33 — skill helper resolvable by bare name from any cwd', () => {
  it('runs a bare-name helper shipped under <skillDir>/scripts when the skill is active', async () => {
    if (process.platform === 'win32') return; // POSIX helper fixture
    const res = await bashTool.execute(
      { command: 'myskillhelper restyle' },
      ctx({ skillEnv: { CLAUDE_SKILL_DIR: skillDir, SKILL_DIR: skillDir } }),
    );
    assert.equal(res.isError, false, `expected the skill helper to resolve from PATH, got: ${res.output}`);
    assert.match(res.output, new RegExp(`${MARKER} restyle`));
  });

  it('does NOT resolve the helper when no skill is active (prepend is scoped)', async () => {
    if (process.platform === 'win32') return;
    const res = await bashTool.execute({ command: 'myskillhelper' }, ctx());
    // No CLAUDE_SKILL_DIR → the skill scripts dir is not on PATH → not found.
    assert.equal(res.isError, true, `expected not-found without an active skill, got: ${res.output}`);
    assert.doesNotMatch(res.output, new RegExp(MARKER));
  });
});
