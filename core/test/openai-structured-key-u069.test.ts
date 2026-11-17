import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { scanForSecrets, redactSecrets } from '../src/tools/tool-utils';
import {
  containsCredential,
  redactCredentials,
  findCredentials,
  getCredentialLabel,
} from '../src/credential-guard';

// ---------------------------------------------------------------------------
// U069 — Structurally-anchored OpenAI standard (user) API key parity.
//
// Claude Code's secretScanner `openai-api-key` rule anchors on the mandatory
// `T3BlbkFJ` infix. Besides the scoped `sk-(proj|svcacct|admin)-…T3BlbkFJ…`
// form (already covered), it carries the standard user-key form
// `sk-<20>T3BlbkFJ<20>`. AMI previously had NO structural rule for that form —
// a modern user key was caught only by the broad generic `sk-…` heuristic,
// losing the near-zero-false-positive structural identification. These tests
// assert the dedicated structural rule now fires (tool-utils + credential-guard)
// while AMI's generic superset net is retained.
//
// Fixtures are synthetic token SHAPES (no real secrets): 20 alnum + the public
// `T3BlbkFJ` marker + 20 alnum.
// ---------------------------------------------------------------------------

const USER_KEY = 'sk-' + 'A'.repeat(20) + 'T3BlbkFJ' + 'B'.repeat(20);
// A plain high-entropy `sk-` token WITHOUT the structural `T3BlbkFJ` infix.
const PLAIN_KEY = 'sk-' + 'C'.repeat(40);

describe('U069 tool-utils :: OpenAI user key structural rule', () => {
  it('scanForSecrets fires openai-user-key on the T3BlbkFJ user-key form (revert: only generic openai-api-key)', () => {
    const found = scanForSecrets(`OPENAI_API_KEY=${USER_KEY}`);
    assert.ok(
      found.some(m => m.ruleId === 'openai-user-key'),
      `expected openai-user-key; got ${found.map(m => m.ruleId).join(',')}`,
    );
    const m = found.find(x => x.ruleId === 'openai-user-key');
    assert.equal(m!.label, 'OpenAI User Key');
  });

  it('the structural rule requires the T3BlbkFJ anchor (a plain sk- token does NOT fire openai-user-key)', () => {
    const found = scanForSecrets(`OPENAI_API_KEY=${PLAIN_KEY}`);
    assert.ok(!found.some(m => m.ruleId === 'openai-user-key'), 'anchorless token must not match the structural rule');
    // AMI's broad generic net still catches the plain token (superset behavior).
    assert.ok(found.some(m => m.ruleId === 'openai-api-key'), 'generic net must still fire for a plain sk- token');
  });

  it('redactSecrets scrubs the structural user key fully (shared rule source)', () => {
    const out = redactSecrets(`OPENAI_API_KEY=${USER_KEY}`);
    assert.ok(!out.includes(USER_KEY), 'user key must not survive redaction');
    assert.ok(out.includes('[REDACTED-SECRET]'));
  });
});

describe('U069 credential-guard :: OpenAI user key structural rule', () => {
  it('findCredentials types the user-key form as openai_user_key (revert: only openai_key)', () => {
    const creds = findCredentials(`value: ${USER_KEY}`);
    const match = creds.find(c => c.type === 'openai_user_key');
    assert.ok(match, `expected openai_user_key; got ${creds.map(c => c.type).join(',')}`);
    assert.equal(match!.label, 'OpenAI User Key');
  });

  it('getCredentialLabel renders the brand-correct label', () => {
    assert.equal(getCredentialLabel('openai_user_key'), 'OpenAI User Key');
  });

  it('containsCredential + redactCredentials cover the user-key form', () => {
    const text = `token=${USER_KEY}`;
    assert.equal(containsCredential(text), true);
    const red = redactCredentials(text);
    assert.ok(!red.includes(USER_KEY), 'user key token must be redacted');
    assert.ok(red.includes('[REDACTED]'));
  });

  it('a plain anchorless sk- token is NOT typed openai_user_key (structural precision)', () => {
    const creds = findCredentials(`value: ${PLAIN_KEY}`);
    assert.ok(!creds.some(c => c.type === 'openai_user_key'), 'anchorless token must not get the structural type');
    assert.ok(creds.some(c => c.type === 'openai_key'), 'generic net must still type it openai_key');
  });
});
