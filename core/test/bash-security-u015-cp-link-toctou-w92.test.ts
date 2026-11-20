import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { detectSymlinkCreateCompound } from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 (w92) — broaden the compound-command link-create TOCTOU guard to
// every link-creating coreutil, not just `ln`.
//
// Parity with the reference's isSymlinkCreatingCommand, which recognises link
// creation across SymbolicLink / Junction / HardLink (PowerShellTool/
// modeValidation.ts L82-117). On bash the same read-through-a-just-created-link
// exfil (`ln -s /etc x && cat x/shadow`) is equally reachable via:
//   • `cp -s` / `cp --symbolic-link`  (GNU cp creates a symlink)
//   • `cp -l` / `cp --link`           (GNU cp creates a hard link)
//   • `link`                          (coreutil hard-link creator)
// detectSymlinkCreateCompound previously only matched the base command `ln`, so
// these equivalent link creators slipped past the escalation.
// ===========================================================================

describe('detectSymlinkCreateCompound — cp/link coreutil parity (w92)', () => {
  it('escalates cp symbolic/hard link creation chained with a later statement', () => {
    for (const cmd of [
      'cp -s /etc x && cat x/shadow',
      'cp --symbolic-link /etc x && cat x/shadow',
      'cp -l /etc/shadow x && cat x',
      'cp --link /etc/shadow x && cat x',
      'cp -sf /etc x; cat x/shadow',      // bundled short flags
      'link /etc/shadow x && cat x',       // coreutil hard-link
    ]) {
      const r = detectSymlinkCreateCompound(cmd);
      assert.equal(r.escalate, true, `expected escalate for: ${cmd}`);
      assert.match(r.reason!, /link/);
    }
  });

  it('does not escalate an ordinary (non-link) cp in a compound', () => {
    for (const cmd of [
      'cp a b && cat a',
      'cp -r a b && cat a',
      'cp -a src dst && echo done',       // archive, not a link flag
      'cp -L src dst && cat dst',          // -L dereferences, does not create a link
    ]) {
      assert.equal(detectSymlinkCreateCompound(cmd).escalate, false, `unexpected escalate for: ${cmd}`);
    }
  });

  it('does not escalate a standalone cp/link creation (no later statement)', () => {
    assert.equal(detectSymlinkCreateCompound('cp -s /etc x').escalate, false);
    assert.equal(detectSymlinkCreateCompound('link /etc/shadow x').escalate, false);
  });

  it('does not treat cp/link used as an argument as a link creator', () => {
    assert.equal(detectSymlinkCreateCompound('echo cp -s && cat file').escalate, false);
    assert.equal(detectSymlinkCreateCompound('echo link && cat file').escalate, false);
  });
});
