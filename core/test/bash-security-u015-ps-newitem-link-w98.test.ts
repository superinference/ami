import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectSymlinkCreateCompound } from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 (w98) — extend the compound-command link-create TOCTOU guard to the
// PowerShell `New-Item -ItemType SymbolicLink|Junction|HardLink` link creator.
//
// Parity with Claude Code's isSymlinkCreatingCommand (PowerShellTool/
// modeValidation.ts L82-117), which recognises link creation across
// SymbolicLink / Junction / HardLink. AMI's system prompt tells the model to use
// PowerShell on Windows, and such a command runs through the same bash tool, so a
// `New-Item -ItemType SymbolicLink -Path link -Target /etc/passwd; Get-Content
// link` compound is the exact read-through-a-just-created-link exfil the bash
// `ln`/`cp -s` guard already escalates — but New-Item was slipping past.
//
// The `-ItemType` parameter is matched across its PowerShell abbreviations
// (`-it`…`-itemtype`) and the `-Type` alias (`-ty`…`-type`), tolerating a
// Unicode-dash or forward-slash parameter prefix, a backtick escape, and a
// colon-/equals-/space-bound value in optional quotes.
// ===========================================================================

describe('detectSymlinkCreateCompound — PowerShell New-Item link parity (w98)', () => {
  it('escalates New-Item link creation chained with a later statement', () => {
    for (const cmd of [
      'New-Item -ItemType SymbolicLink -Path x -Target /etc/passwd; Get-Content x',
      'ni -ItemType Junction -Path x -Target C:/secrets && Get-Content x',
      'New-Item -ItemType HardLink -Path x -Target /etc/shadow; type x',
      'New-Item -it SymbolicLink -Path x -Target /etc; cat x/passwd',   // -it abbreviation
      'New-Item -Type SymbolicLink -Path x -Target /etc; cat x/passwd', // -Type alias
      'New-Item -ty HardLink -Path x -Target /etc/shadow && cat x',     // -ty abbreviation
      'New-Item -ItemType:SymbolicLink -Path x -Target /etc; cat x',    // colon-bound value
      'New-Item -ItemType "SymbolicLink" -Path x -Target /etc; cat x',  // quoted value
    ]) {
      const r = detectSymlinkCreateCompound(cmd);
      assert.equal(r.escalate, true, `expected escalate for: ${cmd}`);
      assert.match(r.reason!, /link/);
    }
  });

  it('does not escalate a non-link New-Item (File/Directory) in a compound', () => {
    for (const cmd of [
      'New-Item -ItemType File -Path x; Get-Content x',
      'New-Item -ItemType Directory -Path x && cd x',
      'ni -Path x; Get-Content x',          // no -ItemType at all → plain item
    ]) {
      assert.equal(detectSymlinkCreateCompound(cmd).escalate, false, `unexpected escalate for: ${cmd}`);
    }
  });

  it('does not escalate a standalone New-Item link (no later statement)', () => {
    assert.equal(detectSymlinkCreateCompound('New-Item -ItemType SymbolicLink -Path x -Target /etc').escalate, false);
  });

  it('does not treat New-Item used as an argument/string as a link creator', () => {
    assert.equal(detectSymlinkCreateCompound('echo New-Item -ItemType SymbolicLink && cat file').escalate, false);
    assert.equal(detectSymlinkCreateCompound('Write-Output "New-Item -ItemType SymbolicLink"; Get-Content y').escalate, false);
  });

  it('still escalates the pre-existing bash link creators (no regression)', () => {
    assert.equal(detectSymlinkCreateCompound('ln -s /etc x && cat x/shadow').escalate, true);
    assert.equal(detectSymlinkCreateCompound('cp -s /etc x && cat x/shadow').escalate, true);
  });
});
