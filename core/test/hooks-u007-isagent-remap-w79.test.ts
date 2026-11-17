/**
 * U007 (w79) — registerConfigEntries `isAgent` Stop→SubagentStop remap + source
 * attribution logging (parity with Claude Code's registerFrontmatterHooks).
 *
 * Covered confirmed gaps (core/core/src/hooks.ts):
 *  - "Session/agent-scoped hook registration + `isAgent` Stop→SubagentStop remap
 *     parameter" (registerFrontmatterHooks.ts:L13-L45): a spawned subagent fires
 *     SubagentStop, NOT Stop, so an agent's frontmatter `stop` hook must be
 *     registered as `subagentStop` or it would never run.
 *  - "Agent Stop→SubagentStop hook-event remapping boundary".
 *  - "Convert agent-scoped Stop hooks to SubagentStop (special-case branch)".
 *  - "Debug message explaining the Stop → SubagentStop hook conversion for agents".
 *  - "Introspection/debug log attributing registered frontmatter hooks to their
 *     source" + "registered-count summary".
 *
 * RED on revert (restoring the unconditional registration in registerConfigEntries):
 *  - the isAgent case leaves the hook on `stop` (subagentStop stays 0), failing the
 *    first assertion;
 *  - the "Converting Stop hook to SubagentStop" + "Registered N frontmatter hook(s)"
 *    debug lines are never emitted, failing the log assertions.
 */

import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { HookManager, type HookConfigEntry } from '../src/hooks';

const tmpDirs: string[] = [];
after(() => {
  for (const d of tmpDirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ } }
});
function tmpCwd(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u007-isagent-'));
  tmpDirs.push(dir);
  return dir;
}
function stopEntry(command = 'echo done'): HookConfigEntry {
  return { event: 'stop', hook: { type: 'command', command } };
}
/** Run `fn` with coreLog routed to a captured stderr buffer. */
function withCapturedLog(fn: () => void): string[] {
  const prev = process.env.AMI_DEBUG_TO_STDERR;
  process.env.AMI_DEBUG_TO_STDERR = '1';
  const lines: string[] = [];
  const spy = mock.method(process.stderr, 'write', (s: string | Uint8Array): boolean => {
    lines.push(String(s));
    return true;
  });
  try {
    fn();
  } finally {
    spy.mock.restore();
    if (prev === undefined) delete process.env.AMI_DEBUG_TO_STDERR;
    else process.env.AMI_DEBUG_TO_STDERR = prev;
  }
  return lines;
}

describe('registerConfigEntries — isAgent Stop→SubagentStop remap', () => {
  it('remaps an agent `stop` hook to `subagentStop` when isAgent:true', () => {
    const hm = new HookManager();
    hm.registerConfigEntries([stopEntry()], tmpCwd(), { isAgent: true });
    const s = hm.getSummary();
    assert.equal(s.subagentStop, 1, 'stop hook registered as subagentStop for an agent');
    assert.equal(s.stop, 0, 'no stop hook left on the Stop event');
  });

  it('keeps a skill `stop` hook on `stop` when isAgent is falsy', () => {
    const hm = new HookManager();
    hm.registerConfigEntries([stopEntry()], tmpCwd(), { isAgent: false });
    const s = hm.getSummary();
    assert.equal(s.stop, 1, 'skill stop hook stays on Stop');
    assert.equal(s.subagentStop, 0, 'no remap for a skill');
  });

  it('defaults to no remap when no options object is passed', () => {
    const hm = new HookManager();
    hm.registerConfigEntries([stopEntry()], tmpCwd());
    const s = hm.getSummary();
    assert.equal(s.stop, 1);
    assert.equal(s.subagentStop, 0);
  });

  it('only remaps `stop` — other agent events are untouched', () => {
    const hm = new HookManager();
    hm.registerConfigEntries(
      [{ event: 'preToolUse', hook: { type: 'command', command: 'echo pre' } }],
      tmpCwd(),
      { isAgent: true },
    );
    const s = hm.getSummary();
    assert.equal(s.preToolUse, 1, 'non-stop agent hook registered to its own event');
    assert.equal(s.subagentStop, 0, 'preToolUse is not remapped');
  });

  it('the introspection descriptor reflects the remapped event', () => {
    const hm = new HookManager();
    hm.registerConfigEntries([stopEntry('echo x')], tmpCwd(), { isAgent: true });
    const descs = hm.describeHooks();
    assert.equal(descs.length, 1);
    assert.equal(descs[0].event, 'subagentStop', 'descriptor shows the remapped event');
  });
});

describe('registerConfigEntries — source attribution + conversion logging', () => {
  it('logs the Stop→SubagentStop conversion naming the source', () => {
    const hm = new HookManager();
    const lines = withCapturedLog(() => {
      hm.registerConfigEntries([stopEntry()], tmpCwd(), { isAgent: true, sourceName: "agent 'reviewer'" });
    });
    const joined = lines.join('');
    assert.match(joined, /Converting Stop hook to SubagentStop from agent 'reviewer' \(subagents trigger SubagentStop\)/);
  });

  it('logs the registered-count summary with the source', () => {
    const hm = new HookManager();
    const lines = withCapturedLog(() => {
      hm.registerConfigEntries(
        [stopEntry('echo a'), { event: 'preToolUse', hook: { type: 'command', command: 'echo b' } }],
        tmpCwd(),
        { isAgent: false, sourceName: "skill 'deploy'" },
      );
    });
    assert.match(lines.join(''), /Registered 2 frontmatter hook\(s\) from skill 'deploy'/);
  });

  it('skips the count line entirely when every entry is malformed', () => {
    const hm = new HookManager();
    const lines = withCapturedLog(() => {
      hm.registerConfigEntries(
        [{ event: 'stop' } as unknown as HookConfigEntry],
        tmpCwd(),
        { sourceName: "skill 'x'" },
      );
    });
    assert.ok(!lines.join('').includes('Registered'), 'no count line for zero valid entries');
    assert.equal(hm.getSummary().stop, 0);
  });
});
