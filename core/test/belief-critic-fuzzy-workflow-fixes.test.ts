import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { BeliefTracker } from '../src/superinference/belief';
import { Critic } from '../src/superinference/critic';
import { fuzzyFindAndReplace } from '../src/tools/fuzzy-match';
import { parseWorkflowMeta } from '../src/tools/workflow-runtime';

// ---------------------------------------------------------------------------
// belief.ts — reset() must clamp like the constructor
// ---------------------------------------------------------------------------

describe('BeliefTracker.reset() — clamping', () => {
  it('clamps initialBelief=0.0 to 0.25 after reset', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.0 });
    assert.equal(tracker.belief, 0.25);
    tracker.update(true, 0.8);
    tracker.reset();
    assert.equal(tracker.belief, 0.25);
  });

  it('clamps initialBelief=1.0 to 0.95 after reset', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 1.0 });
    assert.equal(tracker.belief, 0.95);
    tracker.update(false);
    tracker.reset();
    assert.equal(tracker.belief, 0.95);
  });

  it('reset resets step to 0', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(true);
    tracker.update(true);
    assert.equal(tracker.step, 2);
    tracker.reset();
    assert.equal(tracker.step, 0);
  });

  it('entropy/eig/shouldStop work correctly after reset from out-of-range initial', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.0 });
    tracker.reset();
    assert.ok(Number.isFinite(tracker.entropy()));
    assert.ok(Number.isFinite(tracker.eig()));
    assert.ok(tracker.entropy() > 0);
    const stop = tracker.shouldStop();
    assert.ok(stop.type === 'none' || stop.type === 'diminishing_returns');
  });

  it('preserves belief=0.5 through reset (no clamping needed)', () => {
    const tracker = new BeliefTracker({ enabled: true, initialBelief: 0.5 });
    tracker.update(true);
    tracker.reset();
    assert.equal(tracker.belief, 0.5);
  });
});

// ---------------------------------------------------------------------------
// critic.ts — extractJSON must handle braces inside string values
// ---------------------------------------------------------------------------

describe('Critic.extractJSON — string-aware brace tracking', () => {
  it('handles braces inside reason string', () => {
    const critic = new Critic();
    const input = '{"approved": true, "score": 0.9, "reason": "the {issue} was fixed"}';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, true);
    assert.equal(result!.score, 0.9);
    assert.equal(result!.reason, 'the {issue} was fixed');
  });

  it('handles nested braces in reason string', () => {
    const critic = new Critic();
    const input = '{"approved": false, "score": 0.1, "reason": "missing {a: {b: c}} format"}';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, false);
    assert.equal(result!.reason, 'missing {a: {b: c}} format');
  });

  it('handles escaped quotes inside strings', () => {
    const critic = new Critic();
    const input = '{"approved": true, "score": 0.5, "reason": "said \\"hello\\""}';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, true);
    assert.equal(result!.reason, 'said "hello"');
  });

  it('handles closing brace inside string without premature termination', () => {
    const critic = new Critic();
    const input = 'Here is: {"approved": true, "reason": "fix the } issue"} done';
    const result = critic.extractJSON(input);
    assert.ok(result !== null);
    assert.equal(result!.approved, true);
    assert.equal(result!.reason, 'fix the } issue');
  });

  it('still returns null for unbalanced braces', () => {
    const critic = new Critic();
    assert.equal(critic.extractJSON('{{{'), null);
  });

  it('still returns null for no braces', () => {
    const critic = new Critic();
    assert.equal(critic.extractJSON('no json here'), null);
  });

  it('handles empty object', () => {
    const critic = new Critic();
    const result = critic.extractJSON('prefix {} suffix');
    assert.deepEqual(result, {});
  });
});

// ---------------------------------------------------------------------------
// fuzzy-match.ts — em-dash position mapping
// ---------------------------------------------------------------------------

describe('fuzzyFindAndReplace — em-dash normalization', () => {
  it('correctly replaces em-dash content via escape_normalized strategy', () => {
    const content = 'value = a — b;';
    const oldStr = 'value = a -- b;';
    const newStr = 'value = x -- y;';
    const result = fuzzyFindAndReplace(content, oldStr, newStr);
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'escape_normalized');
    assert.ok(result.newContent !== null);
    assert.ok(!result.newContent!.includes('value = a'));
  });

  it('correct position after em-dash: subsequent text is not corrupted', () => {
    const content = 'first — second — third';
    const oldStr = 'first -- second -- third';
    const newStr = 'REPLACED';
    const result = fuzzyFindAndReplace(content, oldStr, newStr);
    assert.equal(result.error, null);
    assert.equal(result.newContent, 'REPLACED');
  });

  it('en-dash maps correctly (1:1 no expansion)', () => {
    const content = 'range: 1–5';
    const oldStr = 'range: 1-5';
    const newStr = 'range: 2-6';
    const result = fuzzyFindAndReplace(content, oldStr, newStr);
    assert.equal(result.error, null);
    assert.equal(result.strategy, 'escape_normalized');
  });

  it('mixed em-dash and smart quotes', () => {
    const content = '“Hello” — world';
    const oldStr = '"Hello" -- world';
    const newStr = '"Goodbye" -- universe';
    const result = fuzzyFindAndReplace(content, oldStr, newStr);
    assert.equal(result.error, null);
    assert.ok(result.newContent !== null);
  });
});

// ---------------------------------------------------------------------------
// workflow-runtime.ts — parseWorkflowMeta must handle colons in strings
// ---------------------------------------------------------------------------

describe('parseWorkflowMeta — robust parsing', () => {
  it('handles colons in description', () => {
    const script = `export const meta = {
  name: 'review',
  description: 'Review code: find bugs and issues',
}
const x = 1;`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.equal(meta.name, 'review');
    assert.equal(meta.description, 'Review code: find bugs and issues');
  });

  it('handles apostrophes in description', () => {
    const script = `export const meta = {
  name: 'check',
  description: "don't break things",
}
const x = 1;`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.equal(meta.name, 'check');
    assert.equal(meta.description, "don't break things");
  });

  it('handles phases with detail containing colons', () => {
    const script = `export const meta = {
  name: 'pipeline',
  description: 'test',
  phases: [{ title: 'Scan', detail: 'scan for: issues' }],
}
const x = 1;`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.ok(Array.isArray(meta.phases));
    assert.equal(meta.phases![0].title, 'Scan');
    assert.equal(meta.phases![0].detail, 'scan for: issues');
  });

  it('still returns null for missing meta', () => {
    assert.equal(parseWorkflowMeta('const x = 1;'), null);
  });

  it('still returns null for completely invalid meta', () => {
    assert.equal(parseWorkflowMeta('export const meta = { @#$% }'), null);
  });

  it('parses simple valid meta', () => {
    const script = `export const meta = {
  name: 'simple',
  description: 'a simple workflow',
}
phase('Run')`;
    const meta = parseWorkflowMeta(script);
    assert.ok(meta);
    assert.equal(meta.name, 'simple');
  });
});

// ---------------------------------------------------------------------------
// bash.ts — exit code no longer duplicated
// ---------------------------------------------------------------------------

describe('bash exit code formatting', () => {
  it('non-zero exit code appears exactly once in output', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const context = {
      cwd: process.cwd(),
      sessionId: 'test',
      abortSignal: new AbortController().signal,
      config: {} as any,
    };
    const result = await bashTool.execute({ command: 'exit 42' }, context);
    const matches = result.output.match(/exit.?code/gi) || [];
    assert.equal(matches.length, 1,
      `exit code should appear exactly once, got ${matches.length}: ${result.output}`);
  });

  it('exit code 0 still appears in output', async () => {
    const { bashTool } = await import('../src/tools/bash');
    const context = {
      cwd: process.cwd(),
      sessionId: 'test',
      abortSignal: new AbortController().signal,
      config: {} as any,
    };
    const result = await bashTool.execute({ command: 'true' }, context);
    assert.ok(result.output.includes('Exit code: 0'));
  });
});
