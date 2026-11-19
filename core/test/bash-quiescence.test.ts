import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  classifyBashQuiescence,
  detectBashBlockedPrompt,
  lastNonEmptyLine,
  BashQuiescenceTracker,
} from '../src/tools/bash/quiescence';

describe('lastNonEmptyLine', () => {
  it('returns the last line with visible content', () => {
    assert.equal(lastNonEmptyLine('a\nb\n   \n'), 'b');
  });
  it('returns empty when all lines are blank', () => {
    assert.equal(lastNonEmptyLine('\n  \n\t\n'), '');
  });
});

describe('detectBashBlockedPrompt', () => {
  const cases: Array<[string, string | null]> = [
    ['[sudo] password for me:', 'sudo-password'],
    ['Password for admin@host:', 'sudo-password'],
    ['Enter passphrase for key /home/me/.ssh/id_rsa:', 'passphrase'],
    ['Repeat passphrase:', 'passphrase'],
    ['Password:', 'password'],
    ['Enter password', 'password'],
    ['Continue? (y/n)', 'yes-no'],
    ['Proceed [y/N]', 'yes-no'],
    ['Delete it [y/n, default n]', 'yes-no'],
    ['Overwrite existing file?', 'overwrite'],
    ["File 'x' already exists. Replace it?", 'overwrite'],
    ['Are you sure?', 'confirm'],
    ['Do you want to continue?', 'confirm'],
    ['Press any key to continue', 'press-key'],
    ['Press Enter to proceed', 'press-key'],
    ['Which branch would you like to use?', 'generic-prompt'],
    ['Select an option:', 'generic-prompt'],
    ['Building project...', null],
    ['$ ', null],
    ['done.', null],
    ['', null],
  ];
  for (const [line, expected] of cases) {
    it(`classifies ${JSON.stringify(line)} as ${expected}`, () => {
      assert.equal(detectBashBlockedPrompt(line), expected);
    });
  }

  it('scans only the last non-empty line of a multi-line tail', () => {
    assert.equal(detectBashBlockedPrompt('some logs\nmore logs\nPassword:'), 'password');
    assert.equal(detectBashBlockedPrompt('Password:\nnow idle output'), null);
  });

  it('does not treat a shell prompt ending in $ or # as a question', () => {
    assert.equal(detectBashBlockedPrompt('user@host:~/dir$'), null);
  });
});

describe('classifyBashQuiescence', () => {
  it('reports streaming when output arrived within the quiet window', () => {
    const v = classifyBashQuiescence({
      renderedTail: 'working...',
      msSinceLastOutput: 100,
      quietThresholdMs: 500,
    });
    assert.deepEqual(v, { state: 'streaming' });
  });

  it('reports quiescent when quiet with no prompt', () => {
    const v = classifyBashQuiescence({
      renderedTail: 'still computing',
      msSinceLastOutput: 900,
      quietThresholdMs: 500,
    });
    assert.deepEqual(v, { state: 'quiescent' });
  });

  it('reports blocked-on-input when the quiet tail is a prompt', () => {
    const v = classifyBashQuiescence({
      renderedTail: 'Password:',
      msSinceLastOutput: 900,
      quietThresholdMs: 500,
    });
    assert.deepEqual(v, { state: 'blocked-on-input', reason: 'password' });
  });

  it('surfaces a blocked prompt even before the quiet window elapses', () => {
    // The just-arrived byte IS the prompt being drawn, so do not wait it out.
    const v = classifyBashQuiescence({
      renderedTail: 'Continue? (y/n)',
      msSinceLastOutput: 10,
      quietThresholdMs: 500,
    });
    assert.deepEqual(v, { state: 'blocked-on-input', reason: 'yes-no' });
  });

  it('a finished command is always quiescent, never blocked', () => {
    const v = classifyBashQuiescence({
      renderedTail: 'Password:',
      msSinceLastOutput: 10,
      quietThresholdMs: 500,
      finished: true,
    });
    assert.deepEqual(v, { state: 'quiescent' });
  });
});

describe('BashQuiescenceTracker', () => {
  it('has no output clock until the first output', () => {
    const t = new BashQuiescenceTracker(500);
    assert.equal(t.hasOutputClock, false);
    // With no clock, an empty tail settles to quiescent (not busy).
    assert.deepEqual(t.evaluate('', 1000), { state: 'quiescent' });
  });

  it('reports streaming right after output, quiescent once quiet', () => {
    const t = new BashQuiescenceTracker(500);
    t.onOutput(1000);
    assert.equal(t.hasOutputClock, true);
    assert.deepEqual(t.evaluate('log line', 1100), { state: 'streaming' });
    assert.deepEqual(t.evaluate('log line', 1700), { state: 'quiescent' });
  });

  it('detects a blocked prompt via the tracker', () => {
    const t = new BashQuiescenceTracker(500);
    t.onOutput(1000);
    assert.deepEqual(t.evaluate('[sudo] password for me:', 1700), {
      state: 'blocked-on-input',
      reason: 'sudo-password',
    });
  });

  it('honors the finished flag through the tracker', () => {
    const t = new BashQuiescenceTracker(500);
    t.onOutput(1000);
    assert.deepEqual(t.evaluate('Password:', 1100, true), { state: 'quiescent' });
  });

  it('uses Date.now() by default for onOutput and evaluate', () => {
    const t = new BashQuiescenceTracker(10_000);
    t.onOutput();
    // Default-now evaluate, immediately after output => streaming.
    assert.deepEqual(t.evaluate('x'), { state: 'streaming' });
  });
});
