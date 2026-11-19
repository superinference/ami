import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  unwrapWrapperChain,
  tokenizeArgvQuoteAware,
} from '../src/tools/command-prefix';
import { suggestAllowPrefix } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 — word-integrity / over-approval boundary check
// (parity: references/claude/claude-code-main/src/utils/powershell/staticPrefix.ts
//  L89-140 — a persisted rule must never match more than the approved invocation;
//  a quoted argv element must never be split into multiple prefix words)
// ---------------------------------------------------------------------------

test('tokenizeArgvQuoteAware keeps a quoted span as ONE token', () => {
  assert.deepEqual(tokenizeArgvQuoteAware("sudo -u 'admin user' git push"), [
    'sudo', '-u', "'admin user'", 'git', 'push',
  ]);
  assert.deepEqual(tokenizeArgvQuoteAware('git commit -m "fix the bug"'), [
    'git', 'commit', '-m', '"fix the bug"',
  ]);
  // Collapses runs of whitespace, preserves quote chars.
  assert.deepEqual(tokenizeArgvQuoteAware("  git   'push origin' "), [
    'git', "'push origin'",
  ]);
});

test('tokenizeArgvQuoteAware fails closed on an unterminated quote', () => {
  assert.equal(tokenizeArgvQuoteAware("git 'push origin"), null);
});

test('tokenizeArgvQuoteAware honors backslash escapes inside double quotes', () => {
  assert.deepEqual(tokenizeArgvQuoteAware('a "x\\" y" b'), ['a', '"x\\" y"', 'b']);
});

// --- the core gap: a quoted wrapper operand must NOT leak into the inner command.
test('unwrapWrapperChain does not split a quoted wrapper operand (word integrity)', () => {
  // RED before the fix: naive split produced inner "user' git push" and a bogus
  // composed rule. The quoted -u value is one operand; the inner is exactly `git push`.
  assert.deepEqual(unwrapWrapperChain("sudo -u 'admin user' git push"), {
    wrappers: ['sudo'],
    inner: 'git push',
  });
  assert.deepEqual(unwrapWrapperChain('sudo -u "admin user" git status'), {
    wrappers: ['sudo'],
    inner: 'git status',
  });
});

test('unwrapWrapperChain fails closed on an unterminated quote', () => {
  assert.equal(unwrapWrapperChain("sudo -u 'admin user git push"), null);
});

test('suggestAllowPrefix never derives a rule broader than the approved invocation', () => {
  // RED before the fix: suggested the bogus over-broad `sudo user' git`.
  // GREEN: the quoted operand is scoped away, leaving the real `sudo git push`.
  assert.equal(suggestAllowPrefix("sudo -u 'admin user' git push"), 'sudo git push');
  // A genuinely quoted subcommand element must NOT be promoted into extra prefix
  // words (`git push origin` would match `git push origin --force`): the quote
  // survives tokenization so the shape check rejects it → falls back to null.
  assert.equal(suggestAllowPrefix("sudo git 'push origin'"), null);
});
