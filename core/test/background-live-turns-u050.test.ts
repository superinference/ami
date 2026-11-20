import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  BackgroundLiveTurns,
  DEFAULT_LIVE_TURN_RETENTION,
  resolveLiveTurnRetention,
  isFileMutatingTool,
  FILE_MUTATING_TOOLS,
} from '../src/tools/background-live-turns';

/**
 * U050 — background subagent live-turn retention + phase tracking (PARITY with
 * Claude Code's DreamTask live-display state). Covers the bounded ring buffer
 * (MAX_TURNS=30 default), the 'starting'|'updating' sub-phase flip on the first
 * file-mutating tool_use, the env-waterfall retention resolver, and the
 * file-mutating-tool predicate.
 */
describe('U050 background-live-turns — phase', () => {
  it('starts in the "starting" phase', () => {
    const t = new BackgroundLiveTurns();
    assert.equal(t.phase, 'starting');
  });

  it('flips to "updating" on the first file-mutating tool_use', () => {
    const t = new BackgroundLiveTurns();
    t.recordToolUse('file_read');
    assert.equal(t.phase, 'starting', 'a read tool does not flip the phase');
    t.recordToolUse('file_edit');
    assert.equal(t.phase, 'updating');
  });

  it('stays "updating" once flipped (irreversible)', () => {
    const t = new BackgroundLiveTurns();
    t.recordToolUse('file_write');
    assert.equal(t.phase, 'updating');
    t.recordToolUse('file_read');
    assert.equal(t.phase, 'updating');
  });

  it('recognises Claude Code tool names (Edit/Write/MultiEdit/NotebookEdit)', () => {
    const t = new BackgroundLiveTurns();
    t.recordToolUse('Edit');
    assert.equal(t.phase, 'updating');
  });
});

describe('U050 background-live-turns — turn ring buffer', () => {
  it('collapses tool uses to a count per turn', () => {
    const t = new BackgroundLiveTurns();
    t.recordToolUse('bash');
    t.recordToolUse('bash');
    t.commitTurn('did two things');
    assert.deepEqual([...t.turns], [{ text: 'did two things', toolUseCount: 2 }]);
    assert.deepEqual(t.lastTurn(), { text: 'did two things', toolUseCount: 2 });
  });

  it('resets the per-turn tool counter after each commit', () => {
    const t = new BackgroundLiveTurns();
    t.recordToolUse('bash');
    t.commitTurn('a');
    t.recordToolUse('bash');
    t.recordToolUse('bash');
    t.recordToolUse('bash');
    t.commitTurn('b');
    assert.equal(t.turns[0].toolUseCount, 1);
    assert.equal(t.turns[1].toolUseCount, 3);
  });

  it('skips a pure no-op turn (empty text and no tool uses)', () => {
    const t = new BackgroundLiveTurns();
    t.commitTurn('');
    assert.equal(t.turns.length, 0, 'no-op turn is not retained');
    // A turn with text but no tools IS kept.
    t.commitTurn('hello');
    assert.equal(t.turns.length, 1);
    // A turn with no text but tool uses IS kept.
    t.recordToolUse('bash');
    t.commitTurn('');
    assert.equal(t.turns.length, 2);
    assert.deepEqual(t.turns[1], { text: '', toolUseCount: 1 });
  });

  it('coerces a non-string turn text to an empty string', () => {
    const t = new BackgroundLiveTurns();
    t.recordToolUse('bash');
    // Simulate an event whose content was null.
    t.commitTurn(null as unknown as string);
    assert.deepEqual(t.lastTurn(), { text: '', toolUseCount: 1 });
  });

  it('retains only the N most recent turns (bounded ring buffer)', () => {
    const t = new BackgroundLiveTurns(3);
    for (let i = 1; i <= 10; i++) t.commitTurn(`turn ${i}`);
    assert.equal(t.turns.length, 3);
    assert.deepEqual(
      t.turns.map((x) => x.text),
      ['turn 8', 'turn 9', 'turn 10'],
    );
  });

  it('defaults retention to MAX_TURNS=30 (parity)', () => {
    assert.equal(DEFAULT_LIVE_TURN_RETENTION, 30);
    const t = new BackgroundLiveTurns();
    assert.equal(t.retention, 30);
    for (let i = 0; i < 40; i++) t.commitTurn(`t${i}`);
    assert.equal(t.turns.length, 30);
  });

  it('clamps an invalid directly-constructed retention to the default', () => {
    assert.equal(new BackgroundLiveTurns(0).retention, 30);
    assert.equal(new BackgroundLiveTurns(-5).retention, 30);
    assert.equal(new BackgroundLiveTurns(2.5).retention, 30);
  });

  it('works correctly for a retention of 1', () => {
    const t = new BackgroundLiveTurns(1);
    t.commitTurn('a');
    t.commitTurn('b');
    assert.equal(t.turns.length, 1);
    assert.equal(t.turns[0].text, 'b');
  });

  it('lastTurn() is undefined before any commit', () => {
    assert.equal(new BackgroundLiveTurns().lastTurn(), undefined);
  });

  it('describe() reports phase and buffer usage', () => {
    const t = new BackgroundLiveTurns(5);
    assert.equal(t.describe(), 'phase: starting, 0/5 turns retained');
    t.recordToolUse('file_edit');
    t.commitTurn('x');
    assert.equal(t.describe(), 'phase: updating, 1/5 turns retained');
  });
});

describe('U050 background-live-turns — resolveLiveTurnRetention', () => {
  const saved = { ami: process.env.AMI_LIVE_TURN_RETENTION, cc: process.env.CLAUDE_CODE_LIVE_TURN_RETENTION };
  afterEach(() => {
    if (saved.ami === undefined) delete process.env.AMI_LIVE_TURN_RETENTION;
    else process.env.AMI_LIVE_TURN_RETENTION = saved.ami;
    if (saved.cc === undefined) delete process.env.CLAUDE_CODE_LIVE_TURN_RETENTION;
    else process.env.CLAUDE_CODE_LIVE_TURN_RETENTION = saved.cc;
  });

  it('an explicit argument wins over everything', () => {
    process.env.AMI_LIVE_TURN_RETENTION = '7';
    assert.equal(resolveLiveTurnRetention(12), 12);
  });

  it('falls back to AMI_LIVE_TURN_RETENTION', () => {
    delete process.env.AMI_LIVE_TURN_RETENTION;
    delete process.env.CLAUDE_CODE_LIVE_TURN_RETENTION;
    process.env.AMI_LIVE_TURN_RETENTION = '15';
    assert.equal(resolveLiveTurnRetention(), 15);
  });

  it('falls back to CLAUDE_CODE_LIVE_TURN_RETENTION when AMI is unset', () => {
    delete process.env.AMI_LIVE_TURN_RETENTION;
    process.env.CLAUDE_CODE_LIVE_TURN_RETENTION = '9';
    assert.equal(resolveLiveTurnRetention(), 9);
  });

  it('ignores a non-positive / non-integer / unparseable value (falls through)', () => {
    delete process.env.CLAUDE_CODE_LIVE_TURN_RETENTION;
    process.env.AMI_LIVE_TURN_RETENTION = '0';
    assert.equal(resolveLiveTurnRetention(), DEFAULT_LIVE_TURN_RETENTION);
    process.env.AMI_LIVE_TURN_RETENTION = '-3';
    assert.equal(resolveLiveTurnRetention(), DEFAULT_LIVE_TURN_RETENTION);
    process.env.AMI_LIVE_TURN_RETENTION = '2.5';
    assert.equal(resolveLiveTurnRetention(), DEFAULT_LIVE_TURN_RETENTION);
    process.env.AMI_LIVE_TURN_RETENTION = 'abc';
    assert.equal(resolveLiveTurnRetention(), DEFAULT_LIVE_TURN_RETENTION);
    process.env.AMI_LIVE_TURN_RETENTION = '';
    assert.equal(resolveLiveTurnRetention(), DEFAULT_LIVE_TURN_RETENTION);
  });

  it('ignores an invalid explicit argument (falls through to env/default)', () => {
    delete process.env.AMI_LIVE_TURN_RETENTION;
    delete process.env.CLAUDE_CODE_LIVE_TURN_RETENTION;
    assert.equal(resolveLiveTurnRetention(0), DEFAULT_LIVE_TURN_RETENTION);
    assert.equal(resolveLiveTurnRetention(-1), DEFAULT_LIVE_TURN_RETENTION);
  });

  it('defaults when no source is set', () => {
    delete process.env.AMI_LIVE_TURN_RETENTION;
    delete process.env.CLAUDE_CODE_LIVE_TURN_RETENTION;
    assert.equal(resolveLiveTurnRetention(), DEFAULT_LIVE_TURN_RETENTION);
  });
});

describe('U050 background-live-turns — isFileMutatingTool', () => {
  it('matches AMI canonical mutating tools', () => {
    for (const name of ['file_edit', 'file_write', 'notebook_edit', 'multi_edit']) {
      assert.equal(isFileMutatingTool(name), true, name);
    }
  });

  it('matches the reference mutating tool names', () => {
    for (const name of ['Edit', 'Write', 'MultiEdit', 'NotebookEdit']) {
      assert.equal(isFileMutatingTool(name), true, name);
    }
  });

  it('rejects read/search/other tools', () => {
    for (const name of ['file_read', 'bash', 'grep', 'web_fetch', '']) {
      assert.equal(isFileMutatingTool(name), false, name);
    }
  });

  it('FILE_MUTATING_TOOLS is the shared canonical set', () => {
    assert.ok(FILE_MUTATING_TOOLS.has('file_edit'));
    assert.ok(FILE_MUTATING_TOOLS.has('multi_edit'));
    assert.equal(FILE_MUTATING_TOOLS.has('file_read'), false);
  });
});
