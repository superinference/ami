import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  filterInvalidPermissionRules,
  validatePermissionRule,
  wildcardMatch,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// Wildcard escape semantics (literal-asterisk / literal-backslash)
// ---------------------------------------------------------------------------

test('wildcardMatch: unescaped * is the wildcard', () => {
  assert.equal(wildcardMatch('git *', 'git log'), true);
  assert.equal(wildcardMatch('a*c', 'abc'), true);
  assert.equal(wildcardMatch('a*c', 'axyzc'), true);
});

test('wildcardMatch: \\* matches a LITERAL asterisk, not a wildcard', () => {
  // `a\*c` must match only the literal string "a*c", not "abc".
  assert.equal(wildcardMatch('a\\*c', 'a*c'), true);
  assert.equal(wildcardMatch('a\\*c', 'abc'), false);
  // A command that literally contains an asterisk is matchable.
  assert.equal(wildcardMatch('echo \\*', 'echo *'), true);
  assert.equal(wildcardMatch('echo \\*', 'echo hello'), false);
});

test('wildcardMatch: \\\\ matches a literal backslash', () => {
  assert.equal(wildcardMatch('a\\\\b', 'a\\b'), true);
  assert.equal(wildcardMatch('a\\\\b', 'ab'), false);
});

test('wildcardMatch: other regex metachars stay escaped (not special)', () => {
  assert.equal(wildcardMatch('a.c', 'a.c'), true);
  assert.equal(wildcardMatch('a.c', 'axc'), false);
  assert.equal(wildcardMatch('(x)', '(x)'), true);
});

test('wildcardMatch: a lone backslash (not before * or \\) stays literal', () => {
  // `a\b` (one backslash + b) must match the literal string "a\b".
  assert.equal(wildcardMatch('a\\b', 'a\\b'), true);
  assert.equal(wildcardMatch('a\\b', 'ab'), false);
});

// ---------------------------------------------------------------------------
// filterInvalidPermissionRules — fault-tolerant string-array filter
// ---------------------------------------------------------------------------

test('filterInvalidPermissionRules keeps valid rules, drops bad with warnings', () => {
  const { kept, warnings } = filterInvalidPermissionRules(
    ['Bash(ls)', 'WebFetch', 'Bash(', 'Bash()'],
    'permissions.allow',
  );
  assert.deepEqual(kept, ['Bash(ls)', 'WebFetch']);
  assert.equal(warnings.length, 2);
  assert.match(warnings[0]!, /Invalid permission rule "Bash\(" was skipped: Mismatched parentheses/);
  assert.match(warnings[1]!, /Invalid permission rule "Bash\(\)" was skipped: Empty parentheses/);
});

test('filterInvalidPermissionRules reports a non-string entry removed', () => {
  const { kept, warnings } = filterInvalidPermissionRules(['Bash(ls)', 123, null], 'permissions.deny');
  assert.deepEqual(kept, ['Bash(ls)']);
  assert.equal(warnings.length, 2);
  assert.ok(warnings.every(w => /Non-string value in permissions\.deny array was removed/.test(w)));
});

test('filterInvalidPermissionRules tolerates a non-array input (fail-open)', () => {
  assert.deepEqual(filterInvalidPermissionRules(undefined), { kept: [], warnings: [] });
  assert.deepEqual(filterInvalidPermissionRules('nope' as unknown), { kept: [], warnings: [] });
});

test('filterInvalidPermissionRules handles an error with no suggestion (empty rule)', () => {
  const { kept, warnings } = filterInvalidPermissionRules(['']);
  assert.deepEqual(kept, []);
  assert.equal(warnings.length, 1);
  // 'Permission rule cannot be empty' carries no suggestion → no trailing period-suffix.
  assert.equal(warnings[0], 'Invalid permission rule "" was skipped: Permission rule cannot be empty');
});

// ---------------------------------------------------------------------------
// Per-tool content validation (WebFetch / WebSearch)
// ---------------------------------------------------------------------------

test('validatePermissionRule rejects WebSearch wildcards and WebFetch URLs', () => {
  const ws = validatePermissionRule('WebSearch(breaking *)');
  assert.equal(ws.valid, false);
  assert.equal(ws.error, 'WebSearch patterns cannot contain wildcards');
  assert.equal(validatePermissionRule('WebSearch(weather)').valid, true);

  const wf = validatePermissionRule('WebFetch(https://example.com/x)');
  assert.equal(wf.valid, false);
  assert.equal(wf.error, 'WebFetch patterns must be a domain, not a URL');
  assert.equal(validatePermissionRule('WebFetch(example.com)').valid, true);

  // '?' wildcard is also rejected for WebSearch (second operand of the guard).
  assert.equal(validatePermissionRule('WebSearch(a?b)').valid, false);
  // A WebFetch wildcard DOMAIN (not a URL) is fine.
  assert.equal(validatePermissionRule('WebFetch(*.github.com)').valid, true);
});

// ---------------------------------------------------------------------------
// loadRules wiring — validatePermissionRule drops a malformed rule with a
// warning instead of silently loading a dead rule. RED-GREEN: reverting the
// validatePermissionRule call in _coerceRule makes the malformed rule LOAD
// (rules.length === 1, warnings empty) and fails the two assertions below.
// ---------------------------------------------------------------------------

function withTempPermissions(fileData: unknown, fn: (cwd: string) => void): void {
  const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-'));
  try {
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'permissions.json'), JSON.stringify(fileData), 'utf-8');
    fn(cwd);
  } finally {
    fs.rmSync(cwd, { recursive: true, force: true });
  }
}

test('loadRules drops a malformed bash :* rule with an actionable warning (RED-GREEN)', () => {
  withTempPermissions(
    { rules: [{ tool: 'bash', pattern: 'npm:* run', action: 'allow' }] },
    (cwd) => {
      const pm = new PermissionManager();
      const warnings = pm.loadRules(cwd);
      // GREEN: the malformed rule is NOT loaded …
      assert.equal(pm.getRules().length, 0);
      // … and the user is told exactly why.
      assert.equal(warnings.length, 1);
      assert.match(warnings[0]!, /Invalid permission rule "bash\(npm:\* run\)" was skipped: The :\* pattern must be at the end/);
    },
  );
});

test('loadRules accepts a well-formed rule (no warning)', () => {
  // `ls:*` is a well-formed `:*`-at-end rule that is NOT a dangerous code-exec
  // prefix, so it loads cleanly. (A dangerous interpreter grant such as
  // `npm run:*` from the committable project tier is now stripped — see
  // permissions-u003-w95.test.ts; a user wanting it must grant it in a personal
  // settings tier, not a project-committed file.)
  withTempPermissions(
    { rules: [{ tool: 'bash', pattern: 'ls:*', action: 'allow' }] },
    (cwd) => {
      const pm = new PermissionManager();
      const warnings = pm.loadRules(cwd);
      assert.equal(pm.getRules().length, 1);
      assert.deepEqual(warnings, []);
      assert.equal(pm.getRules()[0]!.pattern, 'ls:*');
    },
  );
});

test('loadRules drops an empty-tool rule with a warning', () => {
  withTempPermissions(
    { rules: [{ tool: '', action: 'deny' }] },
    (cwd) => {
      const pm = new PermissionManager();
      const warnings = pm.loadRules(cwd);
      assert.equal(pm.getRules().length, 0);
      assert.equal(warnings.length, 1);
      assert.match(warnings[0]!, /was skipped/);
    },
  );
});
