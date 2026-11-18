import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  stripPathObscuring,
  detectPathExpansionHazard,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U013 (w108) — obscured-path deny-only guess.
//
// An obscured path (backtick / $(…) / ${VAR} / $VAR / %VAR% / leading '=') is
// classified as an expansion hazard and routed to a prompt. Before prompting,
// classifyReadAccess now strips the obscuring construct to a best-effort literal
// and, if THAT guess is a forbidden credential/system path, DENIES outright — so
// an obscured forbidden path can no longer downgrade a hard deny into an "ask"
// (which an auto-allow session would then silently grant). Parity with Claude's
// checkDenyRuleForGuessedPath (PowerShellTool/pathValidation.ts L984-L1008).
// ---------------------------------------------------------------------------

describe('stripPathObscuring', () => {
  it('removes backtick command substitution', () => {
    assert.equal(stripPathObscuring('/x/`whoami`/y'), '/x//y');
  });
  it('removes $(…) substitution', () => {
    assert.equal(stripPathObscuring('/x/$(echo etc)/passwd'), '/x//passwd');
  });
  it('removes ${VAR} and bare $VAR references', () => {
    assert.equal(stripPathObscuring('${HOME}/.ssh/id_rsa'), '/.ssh/id_rsa');
    assert.equal(stripPathObscuring('$HOME/.aws/credentials'), '/.aws/credentials');
  });
  it('removes %VAR% (Windows) references', () => {
    assert.equal(stripPathObscuring('%USERPROFILE%/.ssh/id_rsa'), '/.ssh/id_rsa');
  });
  it('removes a single leading = (zsh equals-expansion)', () => {
    assert.equal(stripPathObscuring('=rg'), 'rg');
  });
  it('is a no-op for an inert literal path', () => {
    assert.equal(stripPathObscuring('/repo/src/app.ts'), '/repo/src/app.ts');
  });
});

describe('classifyReadAccess — obscured forbidden path denies before prompting', () => {
  let CWD = '';
  before(() => {
    CWD = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'obscured-deny-')));
  });
  after(() => { try { fs.rmSync(CWD, { recursive: true, force: true }); } catch { /* ignore */ } });

  const pm = () => new PermissionManager('ask');

  it('denies an obscured system-dir path whose obscuring SPLITS the forbidden segment', () => {
    // The obscuring construct is embedded INSIDE the forbidden directory name
    // ("/et`echo`c/shadow", "/et${X}c/shadow"), so the LITERAL resolved path does
    // NOT match the /etc system-dir / credential guard — without the deny guess
    // classifyReadAccess would return 'prompt', and an auto-allow session would
    // then silently read it (the checkPathPermission auto-grant branch). Only the
    // stripped guess ("/etc/shadow") reveals the forbidden target, forcing 'deny'.
    // These cases are the red-green discriminators for this fix.
    assert.ok(detectPathExpansionHazard('/et`echo`c/shadow'));
    assert.ok(detectPathExpansionHazard('/et${X}c/shadow'));
    assert.equal(stripPathObscuring('/et`echo`c/shadow'), '/etc/shadow');
    assert.equal(stripPathObscuring('/et${X}c/shadow'), '/etc/shadow');

    assert.equal(pm().classifyReadAccess('/et`echo`c/shadow', CWD), 'deny');
    assert.equal(pm().classifyReadAccess('/et${X}c/shadow', CWD), 'deny');
  });

  it('still PROMPTS for an obscured but non-forbidden path (guess is harmless)', () => {
    // A $VAR-bearing in-workspace-looking path is still TOCTOU-unsafe → prompt,
    // not deny (the guess is harmless so the hard-deny guard must not fire).
    assert.equal(pm().classifyReadAccess('$PWD/src/app.ts', CWD), 'prompt');
    assert.equal(pm().classifyReadAccess('${X}notes.txt', CWD), 'prompt');
  });

  it('still PROMPTS for a tilde-variant (stripPathObscuring does not touch ~, so no deny guess runs)', () => {
    assert.ok(detectPathExpansionHazard('~user/notes.txt'));
    // guess == original → deny-guess block is skipped → falls through to prompt.
    assert.equal(pm().classifyReadAccess('~user/notes.txt', CWD), 'prompt');
  });
});
