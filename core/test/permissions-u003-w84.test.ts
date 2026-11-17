/**
 * U003 / wave w84 — permissions.ts parity hardening (three genuinely-missing
 * capabilities ported from Claude Code's permission layer).
 *
 *  1. Preserve unrecognized keys on the permissions.json round-trip
 *     (permissionsLoader.ts L193-L216/L277-L295: Claude spreads `...settingsData`
 *     so unrelated keys survive a rule edit). AMI's loadRules read only
 *     {mode, rules} and saveRules wrote only {mode, rules}, so any other key a
 *     user (or a future AMI version) placed in `.ami/permissions.json` was
 *     silently destroyed on the next save. Now loadRules captures the extra
 *     top-level keys and saveRules re-emits them.
 *
 *  2. Dedup-on-add with legacy-name normalization
 *     (permissionsLoader.ts L244-L285: Claude builds a normalized Set of existing
 *     rules and skips duplicates; the roundtrip parse→serialize normalization at
 *     L184-191/L263-269 makes a legacy alias — `Bash` — match its canonical form
 *     `bash`). AMI's addRule did a bare `unshift` with no duplicate check, so
 *     repeated "always allow" grants accumulated duplicate rules. Now addRule
 *     skips a rule already present under a normalized identity key.
 *
 *  3. Case-insensitive wildcard matching option
 *     (shellRuleMatching.ts L90-L94/L150: matchWildcardPattern accepts a
 *     `caseInsensitive` param adding the regex `i` flag). AMI's wildcardMatch had
 *     no case knob. Now wildcardMatch(pattern, value, caseInsensitive) is exposed,
 *     and a FILE-PATH deny rule is matched case-insensitively on a
 *     case-insensitive filesystem (macOS/Windows), where `/x/.SSH/k` and
 *     `/x/.ssh/k` name the SAME file — so the deny can't be bypassed by case.
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { PermissionManager, wildcardMatch } from '../src/permissions';
import { _pathCompareHooks } from '../src/tools/tool-utils';

function tmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-perm-w84-'));
}

// ---------------------------------------------------------------------------
// 1. Preserve unrecognized keys on round-trip
// ---------------------------------------------------------------------------

test('loadRules + saveRules preserves unrecognized top-level keys', () => {
  const dir = tmp();
  try {
    const amiDir = path.join(dir, '.ami');
    fs.mkdirSync(amiDir, { recursive: true });
    const file = path.join(amiDir, 'permissions.json');
    fs.writeFileSync(file, JSON.stringify({
      mode: 'ask',
      rules: [{ tool: 'bash', pattern: 'git *', action: 'allow' }],
      hooks: { preToolUse: ['echo hi'] },
      customFutureKey: { nested: true, n: 42 },
    }));

    const pm = new PermissionManager();
    pm.loadRules(dir);
    const save = pm.saveRules(dir);
    assert.equal(save.ok, true);

    const roundtripped = JSON.parse(fs.readFileSync(file, 'utf-8'));
    // The modeled keys are still there...
    assert.equal(roundtripped.mode, 'ask');
    assert.equal(Array.isArray(roundtripped.rules), true);
    // ...and the unrecognized keys survived the save (previously destroyed).
    assert.deepEqual(roundtripped.hooks, { preToolUse: ['echo hi'] });
    assert.deepEqual(roundtripped.customFutureKey, { nested: true, n: 42 });
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('saveRules without a prior load writes only {mode, rules} (no spurious keys)', () => {
  const dir = tmp();
  try {
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'bash', pattern: 'ls *', action: 'allow' });
    assert.equal(pm.saveRules(dir).ok, true);
    const data = JSON.parse(fs.readFileSync(path.join(dir, '.ami', 'permissions.json'), 'utf-8'));
    assert.deepEqual(Object.keys(data).sort(), ['mode', 'rules']);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

// ---------------------------------------------------------------------------
// 2. Dedup-on-add (with legacy/alias tool-name normalization)
// ---------------------------------------------------------------------------

test('addRule skips an exact duplicate rule', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'bash', pattern: 'curl *', action: 'allow' });
  pm.addRule({ tool: 'bash', pattern: 'curl *', action: 'allow' });
  assert.equal(pm.getRules().length, 1);
});

test('addRule dedup normalizes a legacy/alias tool name (Bash == bash)', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'Bash', pattern: 'curl *', action: 'allow' });
  pm.addRule({ tool: 'bash', pattern: 'curl *', action: 'allow' });
  assert.equal(pm.getRules().length, 1);
});

test('addRule keeps rules that differ by action, pattern, or source', () => {
  const pm = new PermissionManager('ask');
  pm.addRule({ tool: 'grep', action: 'deny' });
  pm.addRule({ tool: 'grep', action: 'allow' });          // different action
  pm.addRule({ tool: 'grep', pattern: 'x*', action: 'deny' }); // different pattern
  pm.addRule({ tool: 'grep', action: 'deny', source: 'session' }); // different source
  assert.equal(pm.getRules().length, 4);
});

// ---------------------------------------------------------------------------
// 3. Case-insensitive wildcard matching option + file-path deny wiring
// ---------------------------------------------------------------------------

test('wildcardMatch honors the caseInsensitive option', () => {
  assert.equal(wildcardMatch('*SECRET*', 'my-secret-file'), false);
  assert.equal(wildcardMatch('*SECRET*', 'my-secret-file', true), true);
  // exact (literal) fast-path is also case-folded when requested
  assert.equal(wildcardMatch('README', 'readme', true), true);
  assert.equal(wildcardMatch('README', 'readme', false), false);
});

test('file-path deny rule matches case-insensitively on a case-insensitive FS', async () => {
  const saved = _pathCompareHooks.platform;
  try {
    const pm = new PermissionManager('auto-allow');
    // A NON-sensitive path so the only variable is the deny rule's letter-case
    // (a credential path like .ssh would be gated by the sensitive-path guard
    // regardless, masking what this test exercises).
    pm.addRule({ tool: 'file_edit', pattern: '*/VENDORED/*', action: 'deny' });
    const input = { file_path: '/home/u/vendored/data.txt' };

    // Case-SENSITIVE FS (Linux): the differently-cased deny does NOT match, so
    // auto-allow lets it through.
    _pathCompareHooks.platform = 'linux';
    assert.equal(await pm.check('file_edit', input), 'allow');

    // Case-INSENSITIVE FS (macOS): the deny matches despite the case difference.
    _pathCompareHooks.platform = 'darwin';
    assert.equal(await pm.check('file_edit', input), 'deny');
  } finally {
    _pathCompareHooks.platform = saved;
  }
});

test('bash deny rules are unaffected by the case-insensitive path wiring', async () => {
  const saved = _pathCompareHooks.platform;
  try {
    _pathCompareHooks.platform = 'darwin';
    const pm = new PermissionManager('auto-allow');
    pm.addRule({ tool: 'bash', pattern: 'rm *', action: 'deny' });
    // bash still matches via matchBashPattern (not the path case-fold path)
    assert.equal(await pm.check('bash', { command: 'rm -rf /tmp/x' }), 'deny');
    // a case variant of the bash command is NOT auto-folded (commands are case-sensitive)
    assert.equal(await pm.check('bash', { command: 'RM -rf /tmp/x' }), 'allow');
  } finally {
    _pathCompareHooks.platform = saved;
  }
});
