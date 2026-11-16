/**
 * U075 — size-1 last-command parse cache for parseBashAST (parity: Claude Code's
 * memoized parse). The same command is parsed more than once per security
 * evaluation, so a one-entry memo keyed on (input + budget) elides the duplicate
 * work. Behavior-preserving: a cache hit returns the identical AST reference; a
 * different input or budget misses; PARSE_ABORTED is never cached.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { parseBashAST, PARSE_ABORTED } from '../src/tools/bash-parser';

describe('parseBashAST — size-1 parse cache', () => {
  it('returns the SAME AST reference for two consecutive identical calls (cache hit)', () => {
    const a = parseBashAST('grep foo bar.txt | wc -l');
    const b = parseBashAST('grep foo bar.txt | wc -l');
    assert.ok(a && a !== PARSE_ABORTED);
    assert.strictEqual(a, b); // reference identity proves memoization
  });

  it('misses (new object) for a different input', () => {
    const a = parseBashAST('echo one');
    const b = parseBashAST('echo two');
    assert.notStrictEqual(a, b);
    // original is re-parsed to a fresh object once the single slot is evicted
    const a2 = parseBashAST('echo one');
    assert.notStrictEqual(a, a2);
  });

  it('keys on the budget — same input under a different budget misses', () => {
    const a = parseBashAST('ls -la', { timeoutMs: 1000 });
    const b = parseBashAST('ls -la', { timeoutMs: 2000 });
    assert.notStrictEqual(a, b);
  });

  it('caches a deterministic null result (empty/over-length input)', () => {
    const a = parseBashAST('   ');
    const b = parseBashAST('   ');
    assert.equal(a, null);
    assert.equal(b, null);
  });

  it('does NOT cache PARSE_ABORTED (re-evaluated each call)', () => {
    // A node budget of 0 forces an abort on any real command.
    const a = parseBashAST('a && b && c', { maxNodes: 0 });
    assert.equal(a, PARSE_ABORTED);
    // A later successful parse of the same input (default budget) is not shadowed
    // by a cached abort — distinct key AND abort was never stored.
    const b = parseBashAST('a && b && c');
    assert.ok(b && b !== PARSE_ABORTED);
  });
});
