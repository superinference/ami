import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isAcceptEditsPowerShellWrite,
  POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS,
} from '../src/tools/powershell-security.ts';
import {
  PermissionManager,
  getAutoAllowedCommands,
  getAutoAllowedPowerShellCmdlets,
  getDefaultPermissionPatterns,
} from '../src/permissions.ts';

// ===========================================================================
// UNIT U015 (w101) — acceptEdits mode auto-allows the PowerShell edit-adjacent
// write cmdlets, the PowerShell counterpart of the bash mkdir/touch/mv/cp/sed set.
//
// This is the SAME capability U003 implements via the canonical
// isAcceptEditsPowerShellWrite gate + POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS set
// (set-content / add-content / out-file / tee-object / new-item). The two units
// discovered the same gap; the canonical single-command gate is the one wired
// into the permission engine and barrel, so this test documents THAT contract:
//   - one write cmdlet per invocation (no `;`/`|`/chain — fails closed);
//   - New-Item is allowed EXCEPT when it creates a filesystem link;
//   - deletes (remove-item) and content-wipes (clear-content) stay gated — AMI's
//     destructive-command safety invariant (an intentional superset divergence).
// ===========================================================================

describe('isAcceptEditsPowerShellWrite — pure classifier (w101)', () => {
  it('allows single edit-adjacent write cmdlets, aliases, and non-link New-Item', () => {
    for (const cmd of [
      'Set-Content file.txt x',
      "set-content file.txt 'hello world'",
      'Add-Content log.txt x',
      'ac log.txt x', // ac → add-content
      'Microsoft.PowerShell.Management\\Set-Content a b', // module qualifier
      'New-Item -ItemType File x', // non-link New-Item is edit-adjacent
      'Out-File f', // redirect-to-file write
    ]) {
      assert.equal(isAcceptEditsPowerShellWrite(cmd), true, cmd);
    }
  });

  it('does NOT allow deletes, content-wipes, link creation, Tier-3 moves, or the ambiguous sc alias', () => {
    for (const cmd of [
      'Remove-Item f', // delete — gated
      'ri f', // remove-item alias — gated
      'Clear-Content f', // content-wipe — gated (AMI divergence)
      'clc f', // clear-content alias — gated
      'New-Item -ItemType SymbolicLink l', // link creation — redirection vector, gated
      'Copy-Item a b', // Tier-3 move/copy
      'Move-Item a b', // Tier-3 move/copy
      'sc file.txt x', // ambiguous sc → sc.exe, NOT set-content — gated
    ]) {
      assert.equal(isAcceptEditsPowerShellWrite(cmd), false, cmd);
    }
  });

  it('fails closed on substitution / call-operator / chained-or-piped / empty', () => {
    for (const cmd of [
      'Set-Content $(whoami) x', // command substitution
      'Set-Content `whoami` x', // backtick
      'Set-Content a b & Add-Content c d', // call/background operator (also &&)
      'Set-Content a x && Add-Content b y', // chain operator contains &
      'Get-Content a | Set-Content b', // piped — single-command gate rejects
      'Set-Content a x; Add-Content b y', // chained — single-command gate rejects
      '   ', // empty
    ]) {
      assert.equal(isAcceptEditsPowerShellWrite(cmd), false, cmd);
    }
  });

  it('POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS includes the write cmdlets and excludes remove-item/clear-content', () => {
    assert.equal(POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS.has('set-content'), true);
    assert.equal(POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS.has('add-content'), true);
    assert.equal(POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS.has('remove-item'), false);
    assert.equal(POWERSHELL_ACCEPT_EDITS_WRITE_CMDLETS.has('clear-content'), false);
  });
});

describe('PermissionManager acceptEdits — end-to-end PowerShell write wiring (w101)', () => {
  it('auto-allows single PowerShell edit-adjacent writes (and aliases) in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits', []);
    for (const command of [
      'Set-Content file.txt x',
      "set-content file.txt 'hi'",
      'Add-Content f x',
      'ac f x',
      'New-Item -ItemType File x',
      'Out-File f',
    ]) {
      assert.equal(await pm.check('bash', { command }), 'allow', command);
    }
  });

  it('keeps deletes, content-wipes, link creation, chains, and sensitive targets gated in acceptEdits', async () => {
    const pm = new PermissionManager('acceptEdits', []);
    for (const command of [
      'Remove-Item f', // delete
      'Clear-Content f', // content-wipe
      'New-Item -ItemType SymbolicLink l', // link creation
      'Copy-Item a b', // Tier-3
      'sc file.txt x', // ambiguous
      'Set-Content a x; Add-Content b y', // chained — single-command gate rejects
      'Set-Content ~/.ssh/authorized_keys x', // credential path
      'Set-Content $(whoami) x', // substitution
    ]) {
      assert.equal(await pm.check('bash', { command }), 'ask', command);
    }
  });

  it('does NOT auto-allow PowerShell writes outside acceptEdits (ask mode stays gated)', async () => {
    const pm = new PermissionManager('ask', []);
    assert.equal(await pm.check('bash', { command: 'Set-Content file.txt x' }), 'ask');
  });

  it('surfaces the PowerShell write cmdlets via the dedicated introspection accessor', () => {
    const psAllowed = getAutoAllowedPowerShellCmdlets('acceptEdits');
    assert.deepEqual([...psAllowed], ['add-content', 'new-item', 'out-file', 'set-content', 'tee-object']);
    // Excluded destructive cmdlets never surface.
    assert.ok(!psAllowed.includes('remove-item'), 'remove-item not listed');
    assert.ok(!psAllowed.includes('clear-content'), 'clear-content not listed');
    // Non-acceptEdits modes expose nothing.
    assert.deepEqual(getAutoAllowedPowerShellCmdlets('ask'), []);
    // The bash accessor keeps its own contract (unchanged — no PS pollution).
    assert.deepEqual([...getAutoAllowedCommands('acceptEdits')], ['cp', 'mkdir', 'mv', 'sed', 'touch']);
  });

  it('exposes both auto-allowed sets through getDefaultPermissionPatterns', () => {
    const defaults = getDefaultPermissionPatterns();
    assert.deepEqual(defaults.acceptEditsAutoAllowedPowerShellCmdlets, ['add-content', 'new-item', 'out-file', 'set-content', 'tee-object']);
    assert.deepEqual(defaults.acceptEditsAutoAllowedCommands, ['cp', 'mkdir', 'mv', 'sed', 'touch']);
  });
});
