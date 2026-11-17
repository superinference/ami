import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  PermissionManager,
  createReadRuleSuggestion,
  buildPermissionSuggestions,
  isDefaultMode,
  isExternalPermissionMode,
  toExternalPermissionMode,
  EXTERNAL_PERMISSION_MODES,
  getRuleBehaviorDescription,
  PERMISSION_MODES,
  type PermissionUpdate,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003/w87 — createReadRuleSuggestion (gaps 107 / 276)
// ---------------------------------------------------------------------------
describe('createReadRuleSuggestion', () => {
  it('rejects the filesystem root as too broad', () => {
    assert.equal(createReadRuleSuggestion('/'), undefined);
  });

  it('builds a root-anchored //abs/** rule for an absolute path', () => {
    const update = createReadRuleSuggestion('/home/user/project');
    assert.ok(update);
    assert.equal(update!.type, 'addRules');
    assert.equal(update!.behavior, 'allow');
    assert.equal(update!.destination, 'session');
    assert.deepEqual(update!.rules, [{ toolName: 'file_read', ruleContent: '//home/user/project/**' }]);
  });

  it('builds a relative path/** rule for a relative path', () => {
    const update = createReadRuleSuggestion('src/lib');
    assert.ok(update);
    assert.deepEqual(update!.rules, [{ toolName: 'file_read', ruleContent: 'src/lib/**' }]);
  });

  it('POSIX-normalizes Windows separators', () => {
    const update = createReadRuleSuggestion('C:\\Users\\me\\proj');
    assert.ok(update);
    assert.equal(update!.rules[0]!.ruleContent, 'C:/Users/me/proj/**');
  });

  it('honors an explicit (non-default) destination', () => {
    const update = createReadRuleSuggestion('/data', 'projectSettings');
    assert.equal(update!.destination, 'projectSettings');
  });
});

describe('buildPermissionSuggestions wires in a persistable Read rule for reads', () => {
  it('offers BOTH addDirectories and a file_read Read rule on a read op', () => {
    const updates = buildPermissionSuggestions('file_read', { file_path: '/repo/src/a.ts' });
    const dirUpdate = updates.find((u) => u.type === 'addDirectories');
    const ruleUpdate = updates.find((u) => u.type === 'addRules') as Extract<PermissionUpdate, { type: 'addRules' }> | undefined;
    assert.ok(dirUpdate, 'expected an addDirectories suggestion');
    assert.ok(ruleUpdate, 'expected a persistable Read rule suggestion');
    assert.equal(ruleUpdate!.rules[0]!.toolName, 'file_read');
    assert.equal(ruleUpdate!.rules[0]!.ruleContent, '//repo/src/**');
  });
});

// ---------------------------------------------------------------------------
// isDefaultMode (gap 262)
// ---------------------------------------------------------------------------
describe('isDefaultMode', () => {
  it('treats undefined and the neutral mode (ask) as default', () => {
    assert.equal(isDefaultMode(undefined), true);
    assert.equal(isDefaultMode('ask'), true);
  });
  it('is false for every non-default mode', () => {
    for (const m of PERMISSION_MODES) {
      if (m === 'ask') continue;
      assert.equal(isDefaultMode(m), false, `${m} should not be default`);
    }
  });
});

// ---------------------------------------------------------------------------
// External mode mapping (gap 261)
// ---------------------------------------------------------------------------
describe('toExternalPermissionMode / isExternalPermissionMode', () => {
  it('collapses the neutral mode and undefined to default', () => {
    assert.equal(toExternalPermissionMode('ask'), 'default');
    assert.equal(toExternalPermissionMode(undefined), 'default');
  });
  it('passes shared modes through unchanged', () => {
    assert.equal(toExternalPermissionMode('acceptEdits'), 'acceptEdits');
    assert.equal(toExternalPermissionMode('bypassPermissions'), 'bypassPermissions');
    assert.equal(toExternalPermissionMode('plan'), 'plan');
  });
  it('maps internal-only blanket modes to their closest external equivalent', () => {
    assert.equal(toExternalPermissionMode('auto-allow'), 'bypassPermissions');
    assert.equal(toExternalPermissionMode('deny-all'), 'default');
  });
  it('isExternalPermissionMode recognizes exactly the external set', () => {
    for (const m of EXTERNAL_PERMISSION_MODES) assert.equal(isExternalPermissionMode(m), true);
    assert.equal(isExternalPermissionMode('ask'), false);
    assert.equal(isExternalPermissionMode('auto-allow'), false);
    assert.equal(isExternalPermissionMode('deny-all'), false);
  });
  it('every internal mode maps to a valid external mode', () => {
    for (const m of PERMISSION_MODES) {
      assert.ok((EXTERNAL_PERMISSION_MODES as readonly string[]).includes(toExternalPermissionMode(m)));
    }
  });
});

// ---------------------------------------------------------------------------
// getRuleBehaviorDescription fail-safe default (gap 263)
// ---------------------------------------------------------------------------
describe('getRuleBehaviorDescription fail-safe', () => {
  it('maps known behaviors', () => {
    assert.equal(getRuleBehaviorDescription('allow'), 'allowed');
    assert.equal(getRuleBehaviorDescription('deny'), 'denied');
    assert.equal(getRuleBehaviorDescription('ask'), 'asked for confirmation for');
  });
  it('degrades an unknown/future behavior to the safest prose', () => {
    assert.equal(getRuleBehaviorDescription('passthrough'), 'asked for confirmation for');
    assert.equal(getRuleBehaviorDescription(''), 'asked for confirmation for');
  });
});

// ---------------------------------------------------------------------------
// Auto-allow --help commands (gaps 236 / 237) — red-green: revert the check()
// block and these flip from 'allow' back to 'ask'.
// ---------------------------------------------------------------------------
describe('check() auto-allows plain --help commands', () => {
  it('allows a bare --help in ask mode without a prompt', async () => {
    const pm = new PermissionManager('ask');
    assert.equal(await pm.check('bash', { command: 'git commit --help' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'python --help' }), 'allow');
  });
  it('allows --help in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'ls --help' }), 'allow');
  });
  it('an explicit deny rule still wins over the --help fast-path', async () => {
    const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git *', action: 'deny' }]);
    assert.equal(await pm.check('bash', { command: 'git commit --help' }), 'deny');
  });
  it('deny-all mode still denies --help', async () => {
    const pm = new PermissionManager('deny-all');
    assert.equal(await pm.check('bash', { command: 'ls --help' }), 'deny');
  });
  it('a non-simple --help form (extra flags) is NOT fast-path allowed', async () => {
    const pm = new PermissionManager('ask');
    // '-la' is a disqualifying extra flag → not isHelpCommand → ordinary ask.
    assert.equal(await pm.check('bash', { command: 'ls -la --help' }), 'ask');
  });
});

// ---------------------------------------------------------------------------
// Plan-mode bypass inheritance (gap 268) — red-green: revert the plan branch and
// the write flips from 'allow' back to 'deny'.
// ---------------------------------------------------------------------------
describe('check() plan mode inherits bypass when available', () => {
  it('constructor derives bypass availability from an initial bypassPermissions mode', () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(pm.isBypassModeAvailable(), true);
    const plain = new PermissionManager('ask');
    assert.equal(plain.isBypassModeAvailable(), false);
  });

  it('plan mode stays read-only when bypass is NOT available', async () => {
    const pm = new PermissionManager('plan');
    assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'deny');
    assert.equal(await pm.check('file_read', { file_path: 'x.ts' }), 'allow');
  });

  it('plan mode allows a write when bypass is available (inherited)', async () => {
    const pm = new PermissionManager('bypassPermissions');
    pm.setMode('plan');
    assert.equal(pm.getMode(), 'plan');
    assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'allow');
  });

  it('setBypassModeAvailable gates the inheritance explicitly', async () => {
    const pm = new PermissionManager('plan');
    assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'deny');
    pm.setBypassModeAvailable(true);
    assert.equal(await pm.check('file_edit', { file_path: 'x.ts' }), 'allow');
  });

  it('an explicit deny rule still wins over inherited bypass in plan mode', async () => {
    const pm = new PermissionManager('plan', [{ tool: 'bash', action: 'deny' }]);
    pm.setBypassModeAvailable(true);
    assert.equal(await pm.check('bash', { command: 'echo hi' }), 'deny');
  });
});
