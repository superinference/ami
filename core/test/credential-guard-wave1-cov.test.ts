import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
  REDACT_PLACEHOLDER,
  CredentialGuard,
} from '../src/credential-guard';

// ---------------------------------------------------------------------------
// Fixtures (fake but pattern-matching) — distinct from the base test file.
// ---------------------------------------------------------------------------
const OPENAI_KEY = 'sk-' + 'A'.repeat(40);
const OPENAI_KEY_2 = 'sk-' + 'Z'.repeat(40);
const ANTHROPIC_KEY = 'sk-ant-' + '1'.repeat(30);
const AWS_KEY = 'AKIA' + 'B'.repeat(16); // AKIA + 16 upper/digits
const JWT = 'eyJ' + 'h'.repeat(25) + '.' + 'p'.repeat(25) + '.' + 's'.repeat(25);

// ===========================================================================
// Boundary: the `text.length < 10` guard — exercise exactly-10 and exactly-9.
// ===========================================================================
describe('credential-guard length boundary branches', () => {
  it('containsCredential: a 10-char non-credential string passes the length guard but finds nothing', () => {
    const s = '0123456789'; // length === 10 -> not short, but no credential
    assert.equal(s.length, 10);
    assert.equal(containsCredential(s), false);
  });

  it('containsCredential: a 9-char string is rejected by the length guard', () => {
    const s = '012345678'; // length === 9 -> short-circuits to false
    assert.equal(s.length, 9);
    assert.equal(containsCredential(s), false);
  });

  it('redactCredentials: a 10-char clean string is scanned (length guard passes) and returned unchanged', () => {
    const s = 'abcdefghij';
    assert.equal(s.length, 10);
    assert.equal(redactCredentials(s), s);
  });

  it('redactCredentials: a 9-char string short-circuits and is returned verbatim', () => {
    const s = 'abcdefghi';
    assert.equal(s.length, 9);
    assert.equal(redactCredentials(s), s);
  });

  it('findCredentials: a 10-char clean string passes the guard and yields no creds', () => {
    const s = 'xxxxxxxxxx';
    assert.equal(s.length, 10);
    assert.deepEqual(findCredentials(s), []);
  });

  it('findCredentials: a 9-char string short-circuits to empty', () => {
    const s = 'xxxxxxxxx';
    assert.equal(s.length, 9);
    assert.deepEqual(findCredentials(s), []);
  });
});

// ===========================================================================
// findCredentials: while-loop repeat (same pattern matched twice) + sort
// comparator exercised in BOTH directions (positive and negative returns).
// ===========================================================================
describe('findCredentials loop + sort branches', () => {
  it('matches the SAME pattern multiple times (while loop iterates >1)', () => {
    const text = `a=${OPENAI_KEY} b=${OPENAI_KEY_2}`;
    const creds = findCredentials(text);
    const openai = creds.filter(c => c.type === 'openai_key');
    assert.equal(openai.length, 2, 'both openai keys found via repeated while-loop exec');
    // ascending start order proves the sort kept them ordered
    assert.ok(openai[0].start < openai[1].start);
  });

  it('sorts out-of-order matches ascending (comparator returns both signs)', () => {
    // AWS key appears BEFORE the anthropic key in the string, but typeMap
    // pushes anthropic-family matches first, so the sort must reorder them.
    const text = `${AWS_KEY} middle ${ANTHROPIC_KEY}`;
    const creds = findCredentials(text);
    assert.ok(creds.length >= 2);
    // result must be ascending by start regardless of typeMap push order
    for (let i = 1; i < creds.length; i++) {
      assert.ok(creds[i - 1].start <= creds[i].start, 'strictly ascending start offsets');
    }
    assert.equal(creds[0].type, 'aws_access_key', 'earliest offset comes first');
  });

  it('reports accurate length for each match', () => {
    const creds = findCredentials(`tok=${AWS_KEY}`);
    const aws = creds.find(c => c.type === 'aws_access_key');
    assert.ok(aws);
    assert.equal(aws!.length, AWS_KEY.length);
  });
});

// ===========================================================================
// .some short-circuit: first-pattern hit vs last-pattern-only hit.
// ===========================================================================
describe('containsCredential .some iteration branches', () => {
  it('returns true on the FIRST pattern (anthropic) without scanning the rest', () => {
    assert.equal(containsCredential(`k=${ANTHROPIC_KEY}`), true);
  });

  it('returns true only after scanning through to the LAST pattern (jwt)', () => {
    // JWT matches only the final pattern in the list, forcing .some to run
    // every earlier predicate and short-circuit on the last.
    assert.equal(containsCredential(`authorization: Bearer ${JWT}`), true);
  });
});

// ===========================================================================
// Security: red-green bypass variants — each would FAIL if the guard pattern
// were loosened/deleted, so they pin real behavior.
// ===========================================================================
describe('credential-guard bypass variants (red-green meaningful)', () => {
  it('does NOT flag an sk- token that is one char too short', () => {
    const tooShort = 'sk-' + 'a'.repeat(19); // needs {20,}
    assert.equal(containsCredential(tooShort), false);
    assert.deepEqual(findCredentials(tooShort), []);
  });

  it('does NOT flag a lowercase "akia..." (AWS pattern requires uppercase)', () => {
    const lower = 'akia' + 'b'.repeat(16); // lowercase akia + lowercase tail
    assert.equal(containsCredential(lower), false);
  });

  it('still detects a credential embedded in JSON with quotes and escapes', () => {
    const blob = `{"api_key":"${OPENAI_KEY}","note":"x"}`;
    assert.equal(containsCredential(blob), true);
    const red = redactCredentials(blob);
    assert.ok(!red.includes(OPENAI_KEY));
    assert.ok(red.includes(REDACT_PLACEHOLDER));
    // surrounding JSON structure preserved
    assert.ok(red.startsWith('{"api_key":"'));
  });

  it('still detects a credential split only by surrounding newlines/whitespace', () => {
    const blob = `line1\n   export TOKEN=${AWS_KEY}  \nline3`;
    assert.equal(containsCredential(blob), true);
  });

  it('redactCredentials removes ALL occurrences of the same key (global flag)', () => {
    const blob = `first ${OPENAI_KEY} then again ${OPENAI_KEY} end`;
    const red = redactCredentials(blob);
    assert.ok(!red.includes(OPENAI_KEY));
    // two placeholders expected
    const count = red.split(REDACT_PLACEHOLDER).length - 1;
    assert.equal(count, 2);
  });

  it('redactCredentials leaves genuinely clean long text untouched', () => {
    const clean = 'this is a perfectly ordinary sentence with no secrets whatsoever';
    assert.equal(redactCredentials(clean), clean);
  });
});

// ===========================================================================
// CredentialGuard: branch coverage for scan accumulation across multiple
// creds in one call + capacity boundary (crossing the _maxDetections edge).
// ===========================================================================
describe('CredentialGuard scan branches', () => {
  it('scan with an empty-cred clean input returns false and records nothing', () => {
    const guard = new CredentialGuard();
    assert.equal(guard.scan('nothing to see here at all', 'bash'), false);
    assert.equal(guard.getDetectionCount(), 0);
  });

  it('scan records one detection per credential found in a single call', () => {
    const guard = new CredentialGuard();
    const text = `a=${OPENAI_KEY} b=${AWS_KEY}`;
    assert.equal(guard.scan(text, 'tool_output'), true);
    assert.ok(guard.getDetectionCount() >= 2);
    const types = guard.getDetections().map(d => d.type);
    assert.ok(types.includes('openai_key'));
    assert.ok(types.includes('aws_access_key'));
    assert.ok(guard.getDetections().every(d => d.source === 'tool_output'));
  });

  it('stops recording exactly at _maxDetections (capacity boundary)', () => {
    const guard = new CredentialGuard();
    // each scan of a single-key text adds 1 detection
    for (let i = 0; i < 999; i++) guard.scan(`k=${OPENAI_KEY}`, 's');
    assert.equal(guard.getDetectionCount(), 999);
    // this scan has TWO creds: crosses from 999 -> 1000, the second is dropped
    assert.equal(guard.scan(`a=${OPENAI_KEY} b=${AWS_KEY}`, 's'), true);
    assert.equal(guard.getDetectionCount(), 1000);
    // further scans add nothing (capacity branch false)
    assert.equal(guard.scan(`k=${OPENAI_KEY}`, 's'), true);
    assert.equal(guard.getDetectionCount(), 1000);
  });

  it('redact() delegates to redactCredentials', () => {
    const guard = new CredentialGuard();
    assert.equal(guard.redact('hi'), 'hi'); // short-circuit path
    const red = guard.redact(`key=${OPENAI_KEY}`);
    assert.ok(red.includes(REDACT_PLACEHOLDER));
  });

  it('getDetections returns a read-only live view and clear empties it', () => {
    const guard = new CredentialGuard();
    guard.scan(`k=${OPENAI_KEY}`, 'bash');
    assert.equal(guard.getDetections().length, 1);
    guard.clear();
    assert.equal(guard.getDetections().length, 0);
    assert.equal(guard.getDetectionCount(), 0);
  });
});
