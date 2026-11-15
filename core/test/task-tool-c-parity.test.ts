import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  resolveSubagentModel,
  parsePositiveIntEnv,
} from '../src/tools/task';

// Red-green reproducers for two fully-in-file task.ts parity gaps:
//  - F1899: case-insensitive model/alias + inherit-sentinel normalization.
//  - F1771: positive-integer env parsing (reject zero/negative/NaN → default).

describe('F1899 — case-insensitive model/alias normalization in resolveSubagentModel', () => {
  it('matches a family alias regardless of case (anti-downgrade keeps parent pin)', () => {
    // RED before fix: 'Opus' misses the (lowercase-keyed) MODEL_ALIASES table and
    // the hasOwnProperty check, so it falls through and is returned verbatim 'Opus'.
    assert.equal(resolveSubagentModel('Opus', 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentModel('SONNET', 'claude-sonnet-4-6'), 'claude-sonnet-4-6');
  });

  it('resolves an upper/mixed-case alias to its default when the parent is a different family', () => {
    // RED before fix: 'OPUS' !== any MODEL_ALIASES key → returned verbatim 'OPUS'.
    assert.equal(resolveSubagentModel('OPUS', 'claude-sonnet-4-6'), 'claude-opus-4');
    assert.equal(resolveSubagentModel('Haiku', 'gpt-4o'), 'claude-haiku-4-5');
  });

  it('recognizes the inherit sentinel in any case and with surrounding whitespace', () => {
    // RED before fix: strict `requested === 'inherit'` fails for 'INHERIT'/' inherit '.
    assert.equal(resolveSubagentModel('INHERIT', 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentModel('  inherit  ', 'gpt-4o'), 'gpt-4o');
  });

  it('still returns a full/unknown model name VERBATIM (never lower-cased)', () => {
    // Guards against the normalization leaking into the passthrough branch.
    assert.equal(resolveSubagentModel('claude-opus-4-8', 'claude-opus-4-8'), 'claude-opus-4-8');
    assert.equal(resolveSubagentModel('GPT-4o', 'claude-opus-4-8'), 'GPT-4o');
    assert.equal(resolveSubagentModel(undefined, 'claude-opus-4-8'), undefined);
  });
});

describe('F1771 — parsePositiveIntEnv rejects non-positive env values', () => {
  it('rejects a NEGATIVE value and uses the fallback (the core footgun)', () => {
    // RED before fix: inline `parseInt('-5') || DEFAULT` === -5 (a negative stall
    // timeout makes setTimeout fire immediately → instant false "stall" kills).
    assert.equal(parsePositiveIntEnv('-5', 1000), 1000);
    assert.equal(parsePositiveIntEnv('-99999', 42), 42);
  });

  it('rejects zero, NaN, empty, and undefined → fallback', () => {
    assert.equal(parsePositiveIntEnv('0', 1000), 1000);
    assert.equal(parsePositiveIntEnv('abc', 1000), 1000);
    assert.equal(parsePositiveIntEnv('', 1000), 1000);
    assert.equal(parsePositiveIntEnv(undefined, 1000), 1000);
  });

  it('accepts a positive value (including trailing-text numerics parseInt tolerates)', () => {
    assert.equal(parsePositiveIntEnv('200', 1000), 200);
    assert.equal(parsePositiveIntEnv('40ms', 1000), 40);
  });
});
