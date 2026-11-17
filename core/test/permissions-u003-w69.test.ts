import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  stripOutputRedirections,
  suggestAllowPrefix,
  buildPermissionSuggestions,
  getCompoundCommandPrefixes,
  type PermissionUpdate,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003-w69 — three additive permission-layer parities, all in
// core/core/src/permissions.ts:
//   (A) stripOutputRedirections / commandTransform: a redirect target filename
//       must never leak into a derived "allow always" rule prefix.
//       (ref: components/permissions/shellPermissionHelpers.tsx:L62-L84 —
//        generateShellSuggestionsLabel's commandTransform)
//   (B) buildPermissionSuggestions + checkWithReason.suggestions: engine-computed
//       PermissionUpdate[] "always allow X" candidates attached to an ask/deny
//       decision. (ref: components/permissions/hooks.ts:L36-L53;
//        PermissionDecisionDebugInfo.tsx:L186-L209; useCanUseTool.tsx:L102-164)
//   (C) checkWithReason.interrupt: a hardline (blocked-command) deny is
//       turn-aborting, not just tool-blocking.
//       (ref: entrypoints/sdk/coreSchemas.ts:L315-335 — deny-branch `interrupt`)
// ---------------------------------------------------------------------------

// ---- (A) strip output redirections -----------------------------------------

test('stripOutputRedirections: removes >, >>, 2>&1, &>, < targets', () => {
  assert.equal(stripOutputRedirections('npm test > out.txt'), 'npm test');
  assert.equal(stripOutputRedirections('make >>build.log'), 'make');
  assert.equal(stripOutputRedirections('cmd 2>&1'), 'cmd');
  assert.equal(stripOutputRedirections('cmd 2> err.log'), 'cmd');
  assert.equal(stripOutputRedirections('run &> both.txt'), 'run');
  assert.equal(stripOutputRedirections('cat < input.txt'), 'cat');
  assert.equal(stripOutputRedirections('tee <<<"heredoc"'), 'tee');
});

test('stripOutputRedirections: preserves control operators and quoted >', () => {
  // A background/control `&` (not `&>`) and `&&` chaining are untouched.
  assert.equal(stripOutputRedirections('a && b'), 'a && b');
  assert.equal(stripOutputRedirections('sleep 1 &'), 'sleep 1 &');
  // A `>` inside quotes is literal data, not a redirection.
  assert.equal(stripOutputRedirections('grep ">" file'), 'grep ">" file');
  assert.equal(stripOutputRedirections("echo 'a > b'"), "echo 'a > b'");
  // Digits that are plain arguments are not treated as a file descriptor.
  assert.equal(stripOutputRedirections('head -n 20 file'), 'head -n 20 file');
});

test('suggestAllowPrefix: redirection target never leaks into the prefix', () => {
  // RED before the commandTransform: `npm test > out.txt` tokenizes with the
  // redirect, so a naive prefix could include the filename; it must match the
  // clean command's prefix exactly.
  assert.equal(suggestAllowPrefix('npm test > out.txt'), suggestAllowPrefix('npm test'));
  assert.equal(suggestAllowPrefix('npm test > out.txt'), 'npm test');
  // Single-word root with a redirect still resolves to the bare root.
  assert.equal(suggestAllowPrefix('make > build.log'), suggestAllowPrefix('make'));
  // The derived prefix never equals the redirect target.
  const p = suggestAllowPrefix('cargo build 2> errors.txt');
  assert.ok(p === null || !p.includes('errors.txt'));
});

// ---- (B) permission-rule suggestions ---------------------------------------

test('buildPermissionSuggestions: bash derives a scoped allow rule per subcommand', () => {
  const sugg = buildPermissionSuggestions('bash', { command: 'git status' });
  assert.equal(sugg.length, 1);
  const u = sugg[0]!;
  assert.equal(u.type, 'addRules');
  assert.ok(u.type === 'addRules');
  assert.equal(u.behavior, 'allow');
  assert.equal(u.destination, 'session');
  assert.deepEqual(u.rules, [{ toolName: 'bash', ruleContent: 'git status*' }]);
});

test('buildPermissionSuggestions: bash reuses the canonical compound-prefix helper', () => {
  const command = 'cd src && git fetch && git worktree add wt';
  const sugg = buildPermissionSuggestions('bash', { command });
  assert.equal(sugg.length, 1);
  const u = sugg[0]!;
  assert.ok(u.type === 'addRules');
  // Same prefixes getCompoundCommandPrefixes derives, suffixed with `*` — no
  // second heuristic.
  const expected = getCompoundCommandPrefixes(command).map(p => `${p}*`);
  assert.deepEqual(u.rules.map(r => r.ruleContent), expected);
});

test('buildPermissionSuggestions: no safe suggestion → empty array', () => {
  assert.deepEqual(buildPermissionSuggestions('bash', {}), []);
  // A bare shell/interpreter with no scoped prefix yields nothing.
  assert.deepEqual(buildPermissionSuggestions('bash', { command: 'bash -c "echo hi"' }), []);
  // A bare env-var assignment (no command) yields nothing.
  assert.deepEqual(buildPermissionSuggestions('bash', { command: 'FOO=bar' }), []);
});

test('buildPermissionSuggestions: read-shaped file tool widens the read boundary', () => {
  const sugg = buildPermissionSuggestions('file_read', { file_path: '/repo/src/a.ts' });
  // A read now offers BOTH the runtime read-boundary widening AND a persistable,
  // path-scoped file_read Read rule (createReadRuleSuggestion) — additive (w87/U003).
  assert.equal(sugg.length, 2);
  const u = sugg[0]!;
  assert.ok(u.type === 'addDirectories');
  assert.deepEqual(u.directories, ['/repo/src']);
  assert.equal(u.destination, 'session');
  const rule = sugg[1]!;
  assert.ok(rule.type === 'addRules');
  assert.equal(rule.behavior, 'allow');
  assert.deepEqual(rule.rules, [{ toolName: 'file_read', ruleContent: '//repo/src/**' }]);
  // Claude-Code tool-name alias (`Read`) canonicalizes to the same suggestions.
  const aliased = buildPermissionSuggestions('Read', { path: '/repo/src/a.ts' });
  assert.deepEqual(aliased, sugg);
});

test('buildPermissionSuggestions: mutating file tool suggests a path allow rule', () => {
  const sugg = buildPermissionSuggestions('file_edit', { file_path: '/repo/x.ts' });
  assert.equal(sugg.length, 1);
  const u = sugg[0]!;
  assert.ok(u.type === 'addRules');
  assert.deepEqual(u.rules, [{ toolName: 'file_edit', ruleContent: '/repo/x.ts' }]);
});

test('checkWithReason: attaches suggestions to an ask decision through the Engine path', async () => {
  // `ask` mode → a non-denied bash call requires approval and must carry the
  // pre-baked "always allow git status*" suggestion the prompt can offer.
  const pm = new PermissionManager('ask');
  const res = await pm.checkWithReason('bash', { command: 'git status' });
  assert.equal(res.decision, 'ask');
  assert.ok(res.suggestions, 'ask decision should carry suggestions');
  assert.equal(res.suggestions!.length, 1);
  const u = res.suggestions![0]! as Extract<PermissionUpdate, { type: 'addRules' }>;
  assert.deepEqual(u.rules, [{ toolName: 'bash', ruleContent: 'git status*' }]);
});

test('checkWithReason: allow decision carries no suggestions/interrupt', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git status', action: 'allow' }]);
  const res = await pm.checkWithReason('bash', { command: 'git status' });
  assert.equal(res.decision, 'allow');
  assert.equal(res.suggestions, undefined);
  assert.equal(res.interrupt, undefined);
});

// ---- (C) hardline deny is turn-aborting ------------------------------------

test('checkWithReason: a hardline (blocked) deny is turn-interrupting', async () => {
  const pm = new PermissionManager('ask');
  // `rm -rf /` is a hardline-blocked command → deny with interrupt.
  const res = await pm.checkWithReason('bash', { command: 'rm -rf /' });
  assert.equal(res.decision, 'deny');
  assert.equal(res.reason?.type, 'hardline');
  assert.equal(res.interrupt, true);
});

test('checkWithReason: a rule-based deny is NOT turn-interrupting', async () => {
  const pm = new PermissionManager('ask', [{ tool: 'file_edit', action: 'deny' }]);
  const res = await pm.checkWithReason('file_edit', { file_path: '/repo/x.ts' });
  assert.equal(res.decision, 'deny');
  assert.equal(res.reason?.type, 'rule');
  assert.equal(res.interrupt, undefined);
});
