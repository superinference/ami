import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'node:os';
import {
  isHelpCommand,
  suggestAllowPrefix,
  isDangerousRemovalPath,
  commandHasDangerousRemoval,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003-w71 — two the reference-parity primitives for core/core/src/permissions.ts:
//
//  1. isHelpCommand: a plain `<cmd> --help` is read-only; the derived allow rule
//     should be the FULL command, never a broad `cmd:*` prefix (which would grant
//     arbitrary execution through the interpreter). Mirrors the reference's
//     utils/bash/commands.ts isHelpCommand bypassing prefix extraction.
//
//  2. isDangerousRemovalPath / commandHasDangerousRemoval: a generalized
//     catastrophic-removal guard (root, home, EVERY direct child of root, drive
//     roots, `/*` wildcard) that forces an explicit prompt and can never be
//     auto-allowed — even under a wildcard allow rule or a blanket auto-allow
//     mode. Mirrors the reference's tools/BashTool/pathValidation.ts
//     isDangerousRemovalPath + checkDangerousRemovalPaths.
// ---------------------------------------------------------------------------

describe('isHelpCommand', () => {
  it('accepts simple --help invocations', () => {
    assert.ok(isHelpCommand('python --help'));
    assert.ok(isHelpCommand('git commit --help'));
    assert.ok(isHelpCommand('  npm --help  '));
  });

  it('rejects commands not ending in --help', () => {
    assert.ok(!isHelpCommand('python --help --version'));
    assert.ok(!isHelpCommand('git log --oneline'));
    assert.ok(!isHelpCommand('python'));
  });

  it('rejects other flags besides --help', () => {
    assert.ok(!isHelpCommand('python -c --help'));
    assert.ok(!isHelpCommand('rm -rf --help'));
  });

  it('rejects quotes and non-alphanumeric tokens (injection vectors)', () => {
    assert.ok(!isHelpCommand("python '; rm -rf / ' --help"));
    assert.ok(!isHelpCommand('./configure --help'));
    assert.ok(!isHelpCommand('python $(evil) --help'));
    assert.ok(!isHelpCommand('cat file.txt --help'));
  });

  it('rejects unsafe help backends (man -l / git help --web) spelled with --help', () => {
    // These never *end* with a bare --help, but guard against regressions.
    assert.ok(!isHelpCommand('man -l page --help'));
  });
});

describe('suggestAllowPrefix with help commands', () => {
  it('offers the FULL command for a help command, not a broad prefix', () => {
    // Regression guard: before this fix suggestAllowPrefix collapsed this to the
    // over-broad `python`, so an "allow always" rule granted arbitrary python.
    assert.equal(suggestAllowPrefix('python --help'), 'python --help');
    assert.equal(suggestAllowPrefix('git commit --help'), 'git commit --help');
    assert.notEqual(suggestAllowPrefix('python --help'), 'python');
  });

  it('is unchanged for ordinary commands', () => {
    assert.equal(suggestAllowPrefix('git log --oneline'), 'git log');
    assert.equal(suggestAllowPrefix('npm install foo'), 'npm install');
  });
});

describe('isDangerousRemovalPath', () => {
  it('flags the root, home, drive roots and wildcards', () => {
    assert.ok(isDangerousRemovalPath('/'));
    assert.ok(isDangerousRemovalPath('*'));
    assert.ok(isDangerousRemovalPath('/some/dir/*'));
    assert.ok(isDangerousRemovalPath('C:\\'));
    assert.ok(isDangerousRemovalPath('C:/'));
    assert.ok(isDangerousRemovalPath(os.homedir()));
  });

  it('flags EVERY direct child of root and of a drive root', () => {
    assert.ok(isDangerousRemovalPath('/srv'));
    assert.ok(isDangerousRemovalPath('/root'));
    assert.ok(isDangerousRemovalPath('/opt'));
    assert.ok(isDangerousRemovalPath('/etc/'));
    assert.ok(isDangerousRemovalPath('C:\\Windows'));
    assert.ok(isDangerousRemovalPath('C:/Users'));
  });

  it('does NOT flag grandchildren or ordinary project paths', () => {
    assert.ok(!isDangerousRemovalPath('/usr/local'));
    assert.ok(!isDangerousRemovalPath('/home/user/project/build'));
    assert.ok(!isDangerousRemovalPath('C:/Users/me/project'));
    assert.ok(!isDangerousRemovalPath(os.homedir() + '/.cache/foo'));
  });
});

describe('commandHasDangerousRemoval', () => {
  const cwd = '/home/user/project';
  it('detects catastrophic rm/rmdir targets', () => {
    assert.ok(commandHasDangerousRemoval('rm -rf /srv', cwd));
    assert.ok(commandHasDangerousRemoval('rm -rf ~', cwd));
    assert.ok(commandHasDangerousRemoval('rmdir /root', cwd));
    assert.ok(commandHasDangerousRemoval('rm -rf *', cwd));
    assert.ok(commandHasDangerousRemoval('rm -rf "/opt"', cwd));
  });

  it('detects it inside a compound command', () => {
    assert.ok(commandHasDangerousRemoval('cd /tmp && rm -rf /srv', cwd));
    assert.ok(!commandHasDangerousRemoval('cd /srv && ls', cwd));
  });

  it('does NOT flag safe relative or deep removals', () => {
    assert.ok(!commandHasDangerousRemoval('rm -rf ./build', cwd));
    assert.ok(!commandHasDangerousRemoval('rm -rf node_modules', cwd));
    assert.ok(!commandHasDangerousRemoval('rm -rf ~/.cache/foo', cwd));
    assert.ok(!commandHasDangerousRemoval('rm -rf /usr/local/share', cwd));
    assert.ok(!commandHasDangerousRemoval('git status', cwd));
  });
});

describe('PermissionManager.check — dangerous removal is never auto-allowed', () => {
  it('forces a prompt under auto-allow for a direct-child-of-root removal', async () => {
    const pm = new PermissionManager('auto-allow');
    // Regression discriminator: /srv is NOT in the enumerated system-dir list,
    // so without the generalized guard this would silently auto-allow.
    assert.equal(await pm.check('bash', { command: 'rm -rf /srv' }), 'ask');
  });

  it('allows an ordinary relative removal under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', { command: 'rm -rf ./build' }), 'allow');
  });

  it('forces a prompt under a wildcard allow rule in acceptEdits mode', async () => {
    // acceptEdits does NOT apply the ask-only isDestructiveBash downgrade, so a
    // wildcard allow rule would otherwise auto-approve this catastrophic removal —
    // a true discriminator for the dangerous-removal guard.
    const pm = new PermissionManager('acceptEdits', [
      { tool: 'bash', pattern: 'rm -rf *', action: 'allow' },
    ]);
    assert.equal(await pm.check('bash', { command: 'rm -rf ~' }), 'ask');
  });

  it('still lets an explicit deny win over the removal guard', async () => {
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'rm -rf *', action: 'deny' },
    ]);
    assert.equal(await pm.check('bash', { command: 'rm -rf /srv' }), 'deny');
  });
});
