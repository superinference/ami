/**
 * U015 / w95 — two parity additions to the bash safety layer:
 *
 *  1. Glob-path permission reasoning (parity: Claude Code
 *     PowerShellTool/pathValidation.ts getGlobBaseDirectory + "check deny rules on
 *     the base directory", finding #15). A glob metacharacter on a DIRECTORY
 *     component between a sensitive root and a sensitive leaf — `/etc/<glob>/shadow`,
 *     `~/.aws/<glob>/credentials` — previously broke the contiguous protected-path
 *     match, so `cat /etc/<glob>/shadow` was auto-classified 'safe'. `protectedPathComparands`
 *     is now glob-aware (glob base directory + glob-segment-dropped form), so the
 *     EXISTING credential-read gate (`commandTargetsCredentialPath` →
 *     `PermissionManager.classifyBashCommand`) catches it. `getGlobBaseDirectory`
 *     is exported as the reusable parity primitive.
 *
 *  2. Approval affordance on gated sed blocks (parity: Claude Code
 *     BashTool/sedValidation.ts checkSedConstraints, which frames a dangerous sed
 *     as 'requires approval'). AMI's SED_DANGEROUS messages are more specific than
 *     Claude's but previously surfaced as a bare `Error: …` with no hint that the
 *     restriction is a gate with a path forward. Every sed block now carries a
 *     single-source affordance clause ({@link sedBlock}).
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  getGlobBaseDirectory,
  isCredentialPath,
  commandTargetsCredentialPath,
  validateBashSecurity,
} from '../src/tools/bash-security';
import { PermissionManager } from '../src/permissions';

describe('getGlobBaseDirectory — parity primitive', () => {
  it('returns the directory up to and including the separator before the first glob char', () => {
    assert.equal(getGlobBaseDirectory('/etc/*/shadow'), '/etc/');
    assert.equal(getGlobBaseDirectory('/a/b/c-?.txt'), '/a/b/');
    assert.equal(getGlobBaseDirectory('~/.aws/*/credentials'), '~/.aws/');
    assert.equal(getGlobBaseDirectory('/var/log/[abc]/x'), '/var/log/');
  });

  it('returns "." when a glob char precedes any separator', () => {
    assert.equal(getGlobBaseDirectory('*.log'), '.');
    assert.equal(getGlobBaseDirectory('a?b/c'), '.');
  });

  it('returns the path unchanged when it has no glob metacharacter', () => {
    assert.equal(getGlobBaseDirectory('/home/u/project/foo.txt'), '/home/u/project/foo.txt');
    assert.equal(getGlobBaseDirectory('relative/path.ts'), 'relative/path.ts');
  });

  it('excludes braces (literal brace expansion, not a filesystem glob)', () => {
    assert.equal(getGlobBaseDirectory('/etc/{a,b}/shadow'), '/etc/{a,b}/shadow');
  });
});

describe('isCredentialPath — glob-aware comparands', () => {
  it('matches a credential leaf hidden behind a glob directory component', () => {
    // RED without the fix: the `*/` dir component breaks the `/etc/shadow` match.
    assert.equal(isCredentialPath('/etc/*/shadow'), true);
    assert.equal(isCredentialPath('~/.aws/*/credentials'), true);
    assert.equal(isCredentialPath('/home/u/*/.ssh/id_rsa'), true);
  });

  it('still matches the plain (glob-free) credential path', () => {
    assert.equal(isCredentialPath('/etc/shadow'), true);
    assert.equal(isCredentialPath('~/.aws/credentials'), true);
  });

  it('does not false-positive on a benign glob path (base is cwd / non-sensitive)', () => {
    assert.equal(isCredentialPath('*.log'), false);
    assert.equal(isCredentialPath('src/*.ts'), false);
    assert.equal(isCredentialPath('/home/u/p/*.txt'), false);
  });
});

describe('commandTargetsCredentialPath — glob-dir credential reads', () => {
  it('flags a read whose glob-dir path resolves to a credential leaf', () => {
    assert.equal(commandTargetsCredentialPath('cat /etc/*/shadow'), true);
    assert.equal(commandTargetsCredentialPath('grep x /etc/*/passwd'), true);
    assert.equal(commandTargetsCredentialPath('cat ~/.aws/*/credentials'), true);
  });

  it('leaves a benign glob read unflagged', () => {
    assert.equal(commandTargetsCredentialPath('cat *.log'), false);
    assert.equal(commandTargetsCredentialPath('cat src/*.ts'), false);
  });
});

describe('glob-path credential read — wired end-to-end through the classifier', () => {
  it('routes a glob-dir credential read to a prompt (red-green discriminator)', () => {
    const pm = new PermissionManager();
    // RED on revert: without the glob-aware comparands these classify 'safe'.
    assert.equal(pm.classifyBashCommand('cat /etc/*/shadow'), 'unsafe');
    assert.equal(pm.classifyBashCommand('cat ~/.aws/*/credentials'), 'unsafe');
  });

  it('preserves the plain credential-read invariant and benign reads', () => {
    const pm = new PermissionManager();
    assert.equal(pm.classifyBashCommand('cat /etc/shadow'), 'unsafe');
    assert.equal(pm.classifyBashCommand('cat *.log'), 'safe');
    assert.equal(pm.classifyBashCommand('cat README.md'), 'safe');
  });
});

describe('sed block approval affordance', () => {
  const AFFORDANCE = 'gated for safety';
  const EDIT_TOOL = 'file-editing tool';

  it('appends the affordance clause to a sed write-command block', () => {
    const r = validateBashSecurity("sed -e 'w /tmp/evil' file", { cwd: '/home/u/p' });
    assert.equal(r.safe, false);
    assert.ok(r.message.includes(AFFORDANCE), r.message);
    assert.ok(r.message.includes(EDIT_TOOL), r.message);
    // AMI's specific message is preserved IN ADDITION to the affordance.
    assert.ok(/writes to an arbitrary file/.test(r.message), r.message);
  });

  it('appends the affordance clause across the different sed block kinds', () => {
    const cases = [
      "sed '1e cat /etc/passwd' f",          // execute command
      "sed -i 's/a/b/' /etc/shadow",         // in-place on sensitive path
      "sed 's/a/b/ file",                    // unbalanced quoting (fail-closed)
    ];
    for (const c of cases) {
      const r = validateBashSecurity(c, { cwd: '/home/u/p' });
      assert.equal(r.safe, false, c);
      assert.ok(r.message.includes(AFFORDANCE), `${c} -> ${r.message}`);
    }
  });

  it('does not alter a safe sed command (no block, no affordance text)', () => {
    const r = validateBashSecurity("sed 's/a/b/' file", { cwd: '/home/u/p' });
    assert.equal(r.safe, true);
    assert.ok(!r.message.includes(AFFORDANCE));
  });
});
