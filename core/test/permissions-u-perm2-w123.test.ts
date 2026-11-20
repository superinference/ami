import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  getLegacyToolNames,
  normalizeToolName,
  findOverlyBroadBashPermissions,
  formatPermissionRuleError,
  validatePermissionRule,
  type PermissionRule,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// getLegacyToolNames — reverse of normalizeToolName (parity: the reference's
// permissionRuleParser.getLegacyToolNames, L35-41). Enumerate every legacy /
// cross-framework alias that maps to a canonical AMI tool, for display.
// ---------------------------------------------------------------------------

test('getLegacyToolNames returns every alias mapping to a canonical tool, sorted', () => {
  assert.deepEqual(getLegacyToolNames('bash'), ['Bash']);
  assert.deepEqual(getLegacyToolNames('file_edit'), ['Edit', 'MultiEdit', 'Write']);
  assert.deepEqual(getLegacyToolNames('task'), ['Agent', 'Task']);
  assert.deepEqual(getLegacyToolNames('task_output'), ['AgentOutputTool', 'BashOutputTool']);
});

test('getLegacyToolNames returns [] for a tool with no aliases', () => {
  assert.deepEqual(getLegacyToolNames('not_a_real_tool'), []);
});

test('getLegacyToolNames is the inverse of normalizeToolName for each alias', () => {
  // Every returned legacy name must normalize back to the canonical tool.
  for (const canonical of ['bash', 'file_edit', 'task', 'web_fetch']) {
    for (const legacy of getLegacyToolNames(canonical)) {
      assert.equal(normalizeToolName(legacy), canonical, `${legacy} -> ${canonical}`);
    }
  }
});

// ---------------------------------------------------------------------------
// findOverlyBroadBashPermissions — structured YOLO-equivalent detection over
// loaded rules AND raw CLI --allow specs (parity: the reference's
// permissionSetup.findOverlyBroadBashPermissions, L376-412).
// ---------------------------------------------------------------------------

test('findOverlyBroadBashPermissions flags pattern-less and all-tools bash allows', () => {
  const rules: PermissionRule[] = [
    { tool: 'bash', action: 'allow', source: 'projectSettings' }, // broad
    { tool: 'bash', pattern: 'git *', action: 'allow' },          // scoped — not broad
    { tool: '*', action: 'allow', source: 'userSettings' },       // all-tools — broad
    { tool: 'bash', action: 'deny' },                             // deny — never a hazard
  ];
  const found = findOverlyBroadBashPermissions(rules);
  assert.equal(found.length, 2);
  assert.equal(found[0].ruleDisplay, 'bash(*)');
  assert.equal(found[0].source, 'projectSettings');
  assert.equal(found[1].source, 'userSettings');
});

test('findOverlyBroadBashPermissions scans raw CLI --allow specs with a cliArg source', () => {
  const found = findOverlyBroadBashPermissions([], ['bash', 'bash(git status)', 'Bash(*)']);
  // 'bash' and 'Bash(*)' are broad; 'bash(git status)' is scoped.
  assert.equal(found.length, 2);
  for (const d of found) {
    assert.equal(d.source, 'cliArg');
    assert.equal(d.sourceDisplay, '--allow');
    assert.equal(d.ruleDisplay, 'bash(*)');
  }
});

test('findOverlyBroadBashPermissions returns [] when no broad grants exist', () => {
  assert.deepEqual(findOverlyBroadBashPermissions([{ tool: 'bash', pattern: 'npm *', action: 'allow' }], ['bash(ls)']), []);
});

// ---------------------------------------------------------------------------
// formatPermissionRuleError — composed `error. suggestion. Examples: a, b`
// (parity: the reference's PermissionRuleSchema superRefine, permissionValidation.ts
// L244-262). Previously AMI composed only `error. suggestion` and dropped examples.
// ---------------------------------------------------------------------------

test('formatPermissionRuleError composes error + suggestion + examples', () => {
  const msg = formatPermissionRuleError({
    valid: false,
    error: 'Empty parentheses',
    suggestion: 'Either specify a pattern or use just the tool',
    examples: ['bash', 'bash(git status)'],
  });
  assert.equal(msg, 'Empty parentheses. Either specify a pattern or use just the tool. Examples: bash, bash(git status)');
});

test('formatPermissionRuleError degrades gracefully and ignores valid results', () => {
  assert.equal(formatPermissionRuleError({ valid: false, error: 'Bad rule' }), 'Bad rule');
  assert.equal(formatPermissionRuleError({ valid: false, error: 'Bad', suggestion: 'Fix it' }), 'Bad. Fix it');
  assert.equal(formatPermissionRuleError({ valid: true }), '');
  assert.equal(formatPermissionRuleError({ valid: false }), '');
});

test('validatePermissionRule examples flow through formatPermissionRuleError', () => {
  const result = validatePermissionRule('bash()');
  assert.equal(result.valid, false);
  const composed = formatPermissionRuleError(result);
  // The examples that validate produced must survive into the composed string.
  assert.ok(composed.includes('Examples:'), composed);
  assert.ok(composed.startsWith(result.error!), composed);
});

// ---------------------------------------------------------------------------
// WebFetch domain-pattern validation — reject any URL-form (`scheme://` at the
// start OR a stray `://` anywhere), keep bare domains (incl. `http`-prefixed).
// Parity: the reference's WebFetch validator (toolValidationConfig.ts L55-66); AMI keeps
// its bare-domain syntax (no required `domain:` prefix).
// ---------------------------------------------------------------------------

test('validatePermissionRule rejects WebFetch URL-form patterns', () => {
  assert.equal(validatePermissionRule('web_fetch(https://example.com)').valid, false);
  // Stray `://` not at a clean leading scheme token — now caught (was missed before).
  assert.equal(validatePermissionRule('web_fetch(3http://evil)').valid, false);
  assert.equal(validatePermissionRule('web_fetch(foo bar://x)').valid, false);
});

test('validatePermissionRule keeps bare WebFetch domains (incl. http-prefixed) and wildcards', () => {
  assert.equal(validatePermissionRule('web_fetch(example.com)').valid, true);
  assert.equal(validatePermissionRule('web_fetch(*.github.com)').valid, true);
  // A domain that merely starts with "http" (AMI bare-domain divergence from Claude).
  assert.equal(validatePermissionRule('web_fetch(httpbin.org)').valid, true);
});

// ---------------------------------------------------------------------------
// denialLimitWarning — human-readable circuit-breaker message (parity: the reference's
// permissions.ts L999-L1058). Total-limit message takes precedence; null while open.
// ---------------------------------------------------------------------------

test('denialLimitWarning is null until the circuit breaker trips', () => {
  const pm = new PermissionManager('ask');
  assert.equal(pm.denialLimitWarning(), null);
  pm.recordDenial();
  pm.recordDenial();
  assert.equal(pm.shouldAbort(), false);
  assert.equal(pm.denialLimitWarning(), null);
});

test('denialLimitWarning reports the consecutive run when the consecutive limit is hit', () => {
  const pm = new PermissionManager('ask');
  pm.recordDenial();
  pm.recordDenial();
  pm.recordDenial(); // MAX_CONSECUTIVE = 3
  assert.equal(pm.shouldAbort(), true);
  assert.equal(pm.denialLimitWarning(), '3 consecutive actions were blocked. Please review the transcript before continuing.');
});

test('denialLimitWarning reports the total when the total limit is hit (precedence over consecutive)', () => {
  const pm = new PermissionManager('ask');
  for (let i = 0; i < 19; i++) pm.recordDenial();
  pm.recordSuccess(); // reset consecutive, keep total = 19
  pm.recordDenial();  // total = 20 >= MAX_TOTAL, consecutive = 1
  assert.equal(pm.shouldAbort(), true);
  assert.equal(pm.denialLimitWarning(), '20 actions were blocked this session. Please review the transcript before continuing.');
});

test('denialLimitWarning clears after the circuit breaker is reset', () => {
  const pm = new PermissionManager('ask');
  pm.recordDenial();
  pm.recordDenial();
  pm.recordDenial();
  assert.notEqual(pm.denialLimitWarning(), null);
  pm.resetCircuitBreaker();
  assert.equal(pm.denialLimitWarning(), null);
});
