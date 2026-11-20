import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  validateFlagArgument,
  type FlagArgType,
} from '../src/permissions.ts';

// ===========================================================================
// Gap: FlagArgType per-flag argument-type system (none/number/string/char/{}/EOF)
//   claudeRef references/claude/.../BashTool/readOnlyValidation.ts:L17-L50
//
// AMI's SAFE_COMMAND_FLAGS previously modeled flags as plain allowed/blocked
// Sets with NO argument-type metadata, so validateCommandFlags never reasoned
// about which token a flag consumes nor validated the value. A `-d`/`--max-depth`
// (numeric) flag could be handed a non-numeric value, an arg-taking flag could
// be missing its argument, and bundled arg-taking short flags could smuggle a
// value token past the validator (the parser-differential class Claude closes).
// ===========================================================================
describe('U003/w49: FlagArgType per-flag argument-type validation', () => {
  it('validateFlagArgument enforces each arg type', () => {
    assert.equal(validateFlagArgument('3', 'number'), true);
    assert.equal(validateFlagArgument('abc', 'number'), false);
    assert.equal(validateFlagArgument('', 'number'), false);
    assert.equal(validateFlagArgument('anything', 'string'), true);
    assert.equal(validateFlagArgument('', 'string'), true);
    assert.equal(validateFlagArgument('x', 'char'), true);
    assert.equal(validateFlagArgument('xy', 'char'), false);
    assert.equal(validateFlagArgument('{}', '{}'), true);
    assert.equal(validateFlagArgument('x', '{}'), false);
    assert.equal(validateFlagArgument('EOF', 'EOF'), true);
    assert.equal(validateFlagArgument('eof', 'EOF'), false);
    // 'none' must never have been called with a value — always false.
    assert.equal(validateFlagArgument('x', 'none'), false);
    // unknown type → false (defensive default)
    assert.equal(validateFlagArgument('x', 'bogus' as unknown as FlagArgType), false);
  });

  it('accepts a numeric flag given a valid numeric value', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'fd -d 3 foo' }), 'allow');
    assert.equal(await pm.check('bash', { command: 'fd --max-depth=2 foo' }), 'allow');
  });

  it('rejects a numeric flag given a non-numeric value (separate + inline)', async () => {
    const pm = new PermissionManager('acceptEdits');
    // Was 'allow' before the arg-type model (validator ignored the value).
    assert.equal(await pm.check('bash', { command: 'fd -d abc foo' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'fd --max-depth=abc foo' }), 'ask');
  });

  it('rejects an arg-taking flag that is missing its argument', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'fd -d' }), 'ask');
    // next token is itself a flag → the arg is missing
    assert.equal(await pm.check('bash', { command: 'fd -d -H foo' }), 'ask');
  });

  it('accepts a string flag with a valid value but rejects a dash-led value', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'fd -e js foo' }), 'allow');
    // type-confusion: a 'string' value that looks like a flag is rejected
    assert.equal(await pm.check('bash', { command: 'fd -e --exec foo' }), 'ask');
  });

  it('rejects a bundled short cluster that contains an arg-taking flag', async () => {
    const pm = new PermissionManager('acceptEdits');
    // -He bundles -H (none) + -e (string, arg-taking) → GNU getopt would consume
    // the next token as -e's value; the validator cannot safely model this → ask.
    assert.equal(await pm.check('bash', { command: 'fd -He foo' }), 'ask');
    // an all-'none' bundle is still fine
    assert.equal(await pm.check('bash', { command: 'fd -HI foo' }), 'allow');
  });

  it('still blocks the exec flags on fd regardless of arg-type parsing', async () => {
    const pm = new PermissionManager('acceptEdits');
    assert.equal(await pm.check('bash', { command: 'fd -x rm foo' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'fdfind --exec rm foo' }), 'ask');
  });
});

// ===========================================================================
// Gap: Sensitive-path safety check still fires in autonomous (yolo/auto) mode
//   claudeRef references/claude/.../types/permissions.ts:L312-L320
//
// The `auto-allow` (yolo) branch of check() previously returned 'allow' after
// nothing but a deny-rule scan, so a bash credential read/exfil that did not
// match a hardline regex (e.g. `cat ~/.ssh/id_rsa`, `cp x ~/.aws/credentials`)
// was silently auto-allowed — an asymmetric defense-in-depth hole vs file_read.
// the reference keeps the sensitive-path safetyCheck in every mode; AMI now mirrors
// the bypassPermissions safety-path handling in auto-allow too.
// ===========================================================================
describe('U003/w49: sensitive-path safety still fires in auto-allow (yolo)', () => {
  it('prompts for a credential-path bash read under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', { command: 'cat ~/.ssh/id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'cp x ~/.aws/credentials' }), 'ask');
  });

  it('prompts for a system-directory write under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', { command: 'tee /etc/hosts' }), 'ask');
  });

  it('prompts for an edit to a sensitive config path under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('file_edit', { file_path: '.git/config' }), 'ask');
    assert.equal(await pm.check('file_write', { file_path: 'project/.ssh/known_hosts' }), 'ask');
  });

  it('still auto-allows ordinary commands and tools under auto-allow', async () => {
    const pm = new PermissionManager('auto-allow');
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
    assert.equal(await pm.check('file_read', { file_path: 'src/index.ts' }), 'allow');
    assert.equal(await pm.check('file_edit', { file_path: 'src/app.ts' }), 'allow');
  });

  it('an explicit deny rule still wins over the sensitive-path prompt', async () => {
    const pm = new PermissionManager('auto-allow', [
      { tool: 'bash', action: 'deny', pattern: 'cat*' },
    ]);
    assert.equal(await pm.check('bash', { command: 'cat ~/.ssh/id_rsa' }), 'deny');
  });

  it('bypassPermissions keeps prompting for the same sensitive paths (parity)', async () => {
    const pm = new PermissionManager('bypassPermissions');
    assert.equal(await pm.check('bash', { command: 'cat ~/.ssh/id_rsa' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'tee /etc/hosts' }), 'ask');
    assert.equal(await pm.check('file_edit', { file_path: '.git/config' }), 'ask');
    assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
  });
});
