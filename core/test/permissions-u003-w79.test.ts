import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  getDestructiveCommandWarning,
  deriveWebFetchDomainRuleContent,
  buildPermissionSuggestions,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 (w79): two additive permission-parity capabilities, both pure-core and
// wired through existing consumers (no new field, no cross-layer churn):
//
//  1. getDestructiveCommandWarning — a bash DestructivePattern { pattern, warning }
//     registry returning a specific, human-readable, most-specific-first warning
//     ("Note: may recursively force-remove files", …). Parity with Claude's
//     utils/.../destructiveCommandWarning.ts (bash variant). Purely informational;
//     surfaced through the existing `destructive` safetyCheck decision reason so a
//     permission prompt explains WHY a command is destructive instead of a generic
//     line.
//
//  2. deriveWebFetchDomainRuleContent + its wiring into buildPermissionSuggestions —
//     a domain-scoped "always allow" rule candidate for web_fetch (the URL's
//     hostname). Parity with Claude's WebFetchPermissionRequest
//     ("Yes, and don't ask again for <hostname>"). AMI's canonical web_fetch rule
//     content is the BARE DOMAIN (not Claude's `domain:` prefix) so it matches AMI's
//     existing matcher + sandbox-domain extraction — an intentional superset
//     divergence kept consistent with validatePermissionRule.
// ---------------------------------------------------------------------------

// --- Feature 1: getDestructiveCommandWarning -------------------------------

test('getDestructiveCommandWarning: null for a benign command', () => {
  assert.equal(getDestructiveCommandWarning('ls -la'), null);
  assert.equal(getDestructiveCommandWarning('git status'), null);
  assert.equal(getDestructiveCommandWarning(''), null);
});

test('getDestructiveCommandWarning: recursive+force rm is the most specific match', () => {
  assert.equal(getDestructiveCommandWarning('rm -rf build'), 'Note: may recursively force-remove files');
  // flag order independent
  assert.equal(getDestructiveCommandWarning('rm -fr build'), 'Note: may recursively force-remove files');
});

test('getDestructiveCommandWarning: plain recursive vs plain force rm', () => {
  assert.equal(getDestructiveCommandWarning('rm -r build'), 'Note: may recursively remove files');
  assert.equal(getDestructiveCommandWarning('rm -f file.txt'), 'Note: may force-remove files');
});

test('getDestructiveCommandWarning: git data-loss operations', () => {
  assert.equal(getDestructiveCommandWarning('git reset --hard HEAD~1'), 'Note: may discard uncommitted changes');
  assert.equal(getDestructiveCommandWarning('git push --force origin main'), 'Note: may overwrite remote history');
  assert.equal(getDestructiveCommandWarning('git push -f'), 'Note: may overwrite remote history');
  assert.equal(getDestructiveCommandWarning('git clean -fd'), 'Note: may permanently delete untracked files');
  assert.equal(getDestructiveCommandWarning('git checkout -- .'), 'Note: may discard all working tree changes');
  assert.equal(getDestructiveCommandWarning('git restore .'), 'Note: may discard all working tree changes');
  assert.equal(getDestructiveCommandWarning('git stash drop'), 'Note: may permanently remove stashed changes');
  assert.equal(getDestructiveCommandWarning('git branch -D feature'), 'Note: may force-delete a branch');
});

test('getDestructiveCommandWarning: git safety-bypass operations', () => {
  assert.equal(getDestructiveCommandWarning('git commit --no-verify -m x'), 'Note: may skip safety hooks');
  assert.equal(getDestructiveCommandWarning('git commit --amend -m x'), 'Note: may rewrite the last commit');
});

test('getDestructiveCommandWarning: a dry-run git clean is NOT flagged', () => {
  assert.equal(getDestructiveCommandWarning('git clean -n'), null);
  assert.equal(getDestructiveCommandWarning('git clean --dry-run'), null);
});

test('getDestructiveCommandWarning: database + infrastructure patterns', () => {
  assert.equal(getDestructiveCommandWarning('psql -c "DROP TABLE users"'), 'Note: may drop or truncate database objects');
  assert.equal(getDestructiveCommandWarning('psql -c "TRUNCATE TABLE users"'), 'Note: may drop or truncate database objects');
  assert.equal(getDestructiveCommandWarning('psql -c "DELETE FROM users;"'), 'Note: may delete all rows from a database table');
  assert.equal(getDestructiveCommandWarning('kubectl delete pod mypod'), 'Note: may delete Kubernetes resources');
  assert.equal(getDestructiveCommandWarning('terraform destroy'), 'Note: may destroy Terraform infrastructure');
});

test('getDestructiveCommandWarning: `git rm --force` (word boundary) is not an rm -force false positive', () => {
  // The rm patterns are statement-anchored; a git subcommand does not trigger them.
  assert.equal(getDestructiveCommandWarning('git rm --cached file'), null);
});

test('getDestructiveCommandWarning is pure — repeated calls are identical', () => {
  assert.equal(getDestructiveCommandWarning('rm -rf x'), getDestructiveCommandWarning('rm -rf x'));
});

// --- Feature 1 wiring: the specific warning surfaces in the decision reason -

test('checkWithReason surfaces the specific destructive warning for a destructive bash command', async () => {
  const pm = new PermissionManager('ask');
  const r = await pm.checkWithReason('bash', { command: 'rm -rf build' });
  assert.equal(r.decision, 'ask');
  assert.ok(r.reason && r.reason.type === 'safetyCheck' && r.reason.check === 'destructive');
  // The message carries the specific, human-readable warning (not just a generic line).
  assert.match(r.message ?? '', /may recursively force-remove files/);
});

test('checkWithReason falls back to a generic destructive message when no specific pattern matches', async () => {
  const pm = new PermissionManager('ask');
  // `shred` is in the destructive command set but has no DestructivePattern warning.
  const r = await pm.checkWithReason('bash', { command: 'shred secret.txt' });
  assert.ok(r.reason && r.reason.type === 'safetyCheck' && r.reason.check === 'destructive');
  assert.match(r.message ?? '', /destructive/i);
});

// --- Feature 2: deriveWebFetchDomainRuleContent ----------------------------

test('deriveWebFetchDomainRuleContent: extracts the lowercased hostname from a URL', () => {
  assert.equal(deriveWebFetchDomainRuleContent({ url: 'https://example.com/path?q=1' }), 'example.com');
  assert.equal(deriveWebFetchDomainRuleContent({ url: 'https://API.GitHub.com/repos' }), 'api.github.com');
  assert.equal(deriveWebFetchDomainRuleContent({ url: 'http://sub.domain.co.uk:8080/x' }), 'sub.domain.co.uk');
});

test('deriveWebFetchDomainRuleContent: null for missing/invalid/non-string url', () => {
  assert.equal(deriveWebFetchDomainRuleContent({}), null);
  assert.equal(deriveWebFetchDomainRuleContent({ url: 123 }), null);
  assert.equal(deriveWebFetchDomainRuleContent({ url: 'not a url' }), null);
  assert.equal(deriveWebFetchDomainRuleContent({ url: '' }), null);
});

// --- Feature 2 wiring: buildPermissionSuggestions offers a domain allow rule -

test('buildPermissionSuggestions offers a bare-domain web_fetch allow rule', () => {
  const s = buildPermissionSuggestions('web_fetch', { url: 'https://docs.example.com/guide' });
  assert.deepEqual(s, [{
    type: 'addRules',
    behavior: 'allow',
    destination: 'session',
    rules: [{ toolName: 'web_fetch', ruleContent: 'docs.example.com' }],
  }]);
});

test('buildPermissionSuggestions honors the WebFetch tool alias', () => {
  const s = buildPermissionSuggestions('WebFetch', { url: 'https://example.org/a' });
  assert.equal(s.length, 1);
  assert.equal(s[0].type, 'addRules');
});

test('buildPermissionSuggestions: no web_fetch suggestion when the URL is unusable', () => {
  assert.deepEqual(buildPermissionSuggestions('web_fetch', { url: 'garbage' }), []);
  assert.deepEqual(buildPermissionSuggestions('web_fetch', {}), []);
});

test('the suggested web_fetch domain rule is applicable + matches the hostname via the manager', () => {
  const pm = new PermissionManager('ask');
  const [suggestion] = buildPermissionSuggestions('web_fetch', { url: 'https://trusted.example/data' });
  const { applied } = pm.applyUpdate(suggestion);
  assert.equal(applied, 1);
  // The bare-domain rule content is AMI's canonical form, so the granted domain
  // feeds the same web_fetch matcher used at runtime.
  const rule = pm.getRules().find(r => r.tool === 'web_fetch');
  assert.ok(rule);
  assert.equal(rule?.pattern, 'trusted.example');
});
