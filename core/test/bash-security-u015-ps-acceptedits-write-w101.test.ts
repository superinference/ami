import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  isAcceptEditsPowerShellWrite,
  ACCEPT_EDITS_PS_WRITE_CMDLETS,
} from '../src/tools/powershell-security.ts';
import {
  PermissionManager,
  getAutoAllowedCommands,
  getAutoAllowedPowerShellCmdlets,
  getDefaultPermissionPatterns,
} from '../src/permissions.ts';

// ===========================================================================
// UNIT U015 (w101) — acceptEdits mode auto-allows the PowerShell edit-adjacent
// write cmdlets (Set-Content / Add-Content and aliases), the PowerShell
// counterpart of the bash mkdir/touch/mv/cp/sed set.
//
// Parity with Claude Code's PowerShellTool/modeValidation.ts
// ACCEPT_EDITS_ALLOWED_CMDLETS + resolveToCanonical() alias resolution. Before
// this fix, acceptEdits auto-allowed `mkdir`/`sed -i`/etc. but a PowerShell
// `Set-Content file.txt 'x'` (an edit, run through the same bash tool on Windows)
// fell through to an explicit prompt.
//
// AMI superset divergence (documented in ACCEPT_EDITS_PS_WRITE_CMDLETS): Claude's
// `remove-item` (delete) and `clear-content` (content-wipe) are DELIBERATELY NOT
// auto-allowed — AMI's destructive-command safety invariant keeps every deletion /
// data-loss op gated, exactly as the bash set excludes rm/rmdir.
// ===========================================================================

describe('isAcceptEditsPowerShellWrite — pure classifier (w101)', () => {
  it('allows the edit-adjacent write cmdlets and their canonical aliases', () => {
    for (const cmd of [
      'Set-Content file.txt x',
      "set-content file.txt 'hello world'",
      'Add-Content log.txt x',
      'ac log.txt x', // ac → add-content
      'Microsoft.PowerShell.Management\\Set-Content a b', // module qualifier
      'Set-Content a x; Add-Content b y', // every segment is allowed
    ]) {
      assert.equal(isAcceptEditsPowerShellWrite(cmd), true, cmd);
    }
  });

  it('does NOT allow deletes, content-wipes, Tier-3 cmdlets, or the ambiguous sc alias', () => {
    for (const cmd of [
      'Remove-Item f', // delete — gated
      'ri f', // remove-item alias — gated
      'Clear-Content f', // content-wipe — gated (AMI divergence)
      'clc f', // clear-content alias — gated
      'New-Item -ItemType File x', // Tier-3
      'Copy-Item a b', // Tier-3
      'Move-Item a b', // Tier-3
      'sc file.txt x', // ambiguous sc → sc.exe, NOT set-content — gated
    ]) {
      assert.equal(isAcceptEditsPowerShellWrite(cmd), false, cmd);
    }
  });

  it('fails closed on substitution / call-operator / mixed pipelines / empty', () => {
    for (const cmd of [
      'Set-Content $(whoami) x', // command substitution
      'Set-Content `whoami` x', // backtick
      'Set-Content a b & Add-Content c d', // call/background operator (also &&)
      'Set-Content a x && Add-Content b y', // chain operator contains &
      'Get-Content a | Set-Content b', // a non-write segment in the pipeline
      'Set-Content a x; rm b', // a bash command mixed in
      '   ', // empty
    ]) {
      assert.equal(isAcceptEditsPowerShellWrite(cmd), false, cmd);
    }
  });

  it('ACCEPT_EDITS_PS_WRITE_CMDLETS excludes remove-item and clear-content', () => {
    assert.equal(ACCEPT_EDITS_PS_WRITE_CMDLETS.has('set-content'), true);
    assert.equal(ACCEPT_EDITS_PS_WRITE_CMDLETS.has('add-content'), true);
    assert.equal(ACCEPT_EDITS_PS_WRITE_CMDLETS.has('remove-item'), false);
    assert.equal(ACCEPT_EDITS_PS_WRITE_CMDLETS.has('clear-content'), false);
  });
});

describe('PermissionManager acceptEdits — end-to-end PowerShell write wiring (w101)', () => {
  it('auto-allows Set-Content / Add-Content (and aliases) in acceptEdits mode', async () => {
    const pm = new PermissionManager('acceptEdits', []);
    for (const command of [
      'Set-Content file.txt x',
      "set-content file.txt 'hi'",
      'Add-Content f x',
      'ac f x',
      'Set-Content a x; Add-Content b y',
    ]) {
      assert.equal(await pm.check('bash', { command }), 'allow', command);
    }
  });

  it('keeps deletes, content-wipes, Tier-3, and sensitive targets gated in acceptEdits', async () => {
    const pm = new PermissionManager('acceptEdits', []);
    for (const command of [
      'Remove-Item f', // delete
      'Clear-Content f', // content-wipe
      'New-Item -ItemType File x', // Tier-3
      'Copy-Item a b', // Tier-3
      'sc file.txt x', // ambiguous
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
    assert.deepEqual([...psAllowed], ['add-content', 'set-content']);
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
    assert.deepEqual(defaults.acceptEditsAutoAllowedPowerShellCmdlets, ['add-content', 'set-content']);
    assert.deepEqual(defaults.acceptEditsAutoAllowedCommands, ['cp', 'mkdir', 'mv', 'sed', 'touch']);
  });
});
