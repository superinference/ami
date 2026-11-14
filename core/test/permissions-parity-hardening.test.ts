import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  isGitInvocationReadOnly,
  stripEnvVarPrefix,
  commandHasCdWriteDesync,
  countDirectoryChanges,
  wildcardMatch,
  isBuildConfigFile,
} from '../src/permissions';

// Parity hardening batch — fully-core permission refinements that bring the bash
// classifier and the permission check() flow in line with Claude Code's
// read-only validator and bypass-immunity ordering.

// --- Gap: blanket $-substitution rejection on ALL auto-allowed safe prefixes ---
// Claude rejects any `$VAR`/`$(cmd)` on an auto-allowed external/compound command
// (git/gh/docker AND npm/go/cargo/make): the validator sees the literal text but
// the shell expands it at runtime — a parser differential that can smuggle a
// non-read subcommand or leak a secret via an error message.
test('shell substitution on a non-git safe prefix is never auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('npm list $(evil)'), 'unsafe');
  assert.equal(pm.classifyBashCommand('npm ls ${SECRET}'), 'unsafe');
  assert.equal(pm.classifyBashCommand('go test $PKG'), 'unsafe');
  assert.equal(pm.classifyBashCommand('cargo build $X'), 'unsafe');
  assert.equal(pm.classifyBashCommand('make test $(cat /etc/passwd)'), 'unsafe');
  // substitution-free safe prefixes stay safe
  assert.equal(pm.classifyBashCommand('npm list'), 'safe');
  assert.equal(pm.classifyBashCommand('go test ./...'), 'safe');
  assert.equal(pm.classifyBashCommand('git status'), 'safe');
});

// --- Gap: git branch/tag ref CREATION via a post-`--` positional ---
// `git branch -- -l` is NOT a list: after the bare `--` end-of-options marker,
// `-l` is a positional REF NAME (ref creation = a write). Claude's getopt-accurate
// validator treats everything after `--` as a positional.
test('git branch/tag creation via post-double-dash positional is a write', () => {
  // `-l` after `--` is a ref name, not the --list flag
  assert.equal(isGitInvocationReadOnly(['git', 'branch', '--', '-l']), false);
  assert.equal(isGitInvocationReadOnly(['git', 'branch', '--', 'feature']), false);
  assert.equal(isGitInvocationReadOnly(['git', 'tag', '--', 'v1.0.0']), false);
  // a bare `git branch --` with no trailing positional still just lists
  assert.equal(isGitInvocationReadOnly(['git', 'branch', '--']), true);
  // `--list` before a `--` with no positional stays read-only
  assert.equal(isGitInvocationReadOnly(['git', 'branch', '--list']), true);
});

test('post-double-dash ref creation is not auto-classified safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('git branch -- -l'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git tag -- v2'), 'unsafe');
  // genuine list forms remain safe
  assert.equal(pm.classifyBashCommand('git branch -l'), 'safe');
  assert.equal(pm.classifyBashCommand('git tag -l "v*"'), 'safe');
});

// --- Gap: explicit content-specific `ask` rules are bypass-immune ---
// A user-configured `{tool, action:'ask'}` content rule must still prompt in
// bypassPermissions mode, before the blanket auto-allow — Claude evaluates
// ask/safety-check rules BEFORE the bypass allow.
test('content-specific ask rule still prompts under bypassPermissions', async () => {
  const pm = new PermissionManager();
  pm.addRule({ tool: 'bash', pattern: '*secret*', action: 'ask' });
  pm.setMode('bypassPermissions');
  assert.equal(await pm.check('bash', { command: 'cat secret.txt' }), 'ask');
  // a non-matching command is still blanket-allowed
  assert.equal(await pm.check('bash', { command: 'ls -la' }), 'allow');
});

// --- Gap: getopt-accurate xargs target validation (bundled arg-taking flags) ---
// xargs' arg-taking flags consume a value (inline `-n1`, bundled `-rI {}`, or the
// next token `-I {}`). A naive scanner reads a flag's VALUE as the target command
// and can wave an unsafe real target through (`xargs -I echo ./evil.sh`). The
// getopt-accurate parser finds the true target; xargs is validated even behind a
// pipe so a safe leading command can't short-circuit the check.
test('xargs target is validated getopt-accurately, even behind a pipe', () => {
  const pm = new PermissionManager();
  // legitimate read-only targets stay safe through flag variations
  assert.equal(pm.classifyBashCommand('echo file | xargs cat'), 'safe');
  assert.equal(pm.classifyBashCommand('xargs -I {} grep foo {}'), 'safe');
  assert.equal(pm.classifyBashCommand('xargs -n1 cat'), 'safe');
  assert.equal(pm.classifyBashCommand('xargs -P4 grep foo'), 'safe');
  // the real target is an arg-taking flag's value → parser differential closed
  assert.equal(pm.classifyBashCommand('ls | xargs -I echo ./evil.sh'), 'unsafe');
  assert.equal(pm.classifyBashCommand('ls | xargs -rI {} ./evil.sh'), 'unsafe');
  // a safe leading command no longer short-circuits an unsafe xargs target
  assert.equal(pm.classifyBashCommand('ls | xargs chmod +x f'), 'unsafe');
});

// --- Gap: dangerous git/tool config files require approval ---
test('gitmodules/ripgreprc/mailmap are build-config guarded', async () => {
  assert.equal(isBuildConfigFile('.gitmodules'), true);
  assert.equal(isBuildConfigFile('.ripgreprc'), true);
  assert.equal(isBuildConfigFile('/repo/.mailmap'), true);
  // wired into acceptEdits: editing one prompts even when edits are auto-accepted
  const pm = new PermissionManager('acceptEdits');
  assert.equal(await pm.check('file_edit', { file_path: '/repo/.ripgreprc' }), 'ask');
  assert.equal(await pm.check('file_edit', { file_path: '/repo/src/app.ts' }), 'allow');
});

// --- Gap: sort -o/--output writes a file (strict allowlist flag validation) ---
test('sort output flags write a file and are not auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('sort -o out.txt f'), 'unsafe');
  assert.equal(pm.classifyBashCommand('sort --output=out.txt f'), 'unsafe');
  // read-only sort stays safe
  assert.equal(pm.classifyBashCommand('sort f'), 'safe');
  assert.equal(pm.classifyBashCommand('sort -r -u f'), 'safe');
});

// --- Gap: wrapper stripping consumes the timeout/nice numeric operand ---
test('deny matching sees through a wrapper numeric operand', () => {
  // the wrapper + its numeric operand are stripped for deny/ask matching
  assert.equal(stripEnvVarPrefix('timeout 5 git status'), 'git status');
  assert.equal(stripEnvVarPrefix('timeout 1m git status'), 'git status');
  assert.equal(stripEnvVarPrefix('nice -n 10 git status'), 'git status');
  assert.equal(stripEnvVarPrefix('nice 10 git status'), 'git status');
  // a deny rule therefore still fires through `timeout N …`
  const pm = new PermissionManager('ask', [{ tool: 'bash', pattern: 'rm *', action: 'deny' }]);
  return pm.check('bash', { command: 'timeout 5 rm -rf /tmp/x' }).then((v) => {
    assert.equal(v, 'deny');
  });
});

// --- Gap: dotAll wildcard matching across embedded newlines ---
test('wildcards match across embedded newlines (dotAll)', async () => {
  assert.equal(wildcardMatch('git *', 'git log\nfoo'), true);
  assert.equal(wildcardMatch('*rm -rf*', 'echo hi\nrm -rf /tmp/x'), true);
  // a deny rule cannot be bypassed by splitting the command across lines
  const pm = new PermissionManager('ask', [{ tool: 'web_fetch', pattern: '*evil*', action: 'deny' }]);
  assert.equal(await pm.check('web_fetch', { url: 'https://good\nevil.com' }), 'deny');
});

// --- Gap: single-word read-only utility positional/capability guards ---
test('hostname/tput/lsof positional and option guards', () => {
  const pm = new PermissionManager();
  // hostname: a positional sets the system hostname
  assert.equal(pm.classifyBashCommand('hostname newname'), 'unsafe');
  assert.equal(pm.classifyBashCommand('hostname'), 'safe');
  assert.equal(pm.classifyBashCommand('hostname -f'), 'safe');
  // tput: dangerous capabilities and the -S batch flag prompt; read caps stay safe
  assert.equal(pm.classifyBashCommand('tput reset'), 'unsafe');
  assert.equal(pm.classifyBashCommand('tput init'), 'unsafe');
  assert.equal(pm.classifyBashCommand('tput -S'), 'unsafe');
  assert.equal(pm.classifyBashCommand('tput cols'), 'safe');
  assert.equal(pm.classifyBashCommand('tput lines'), 'safe');
  // lsof: `+` options (e.g. +m mount-supplement file) write; `-` read flags are safe
  assert.equal(pm.classifyBashCommand('lsof +m /x'), 'unsafe');
  assert.equal(pm.classifyBashCommand('lsof -i'), 'safe');
});

// --- Gap: more than one directory change requires approval ---
test('multiple directory changes require approval', () => {
  const pm = new PermissionManager();
  assert.equal(countDirectoryChanges('cd a && cd b && ls'), 2);
  assert.equal(countDirectoryChanges('cd a && ls'), 1);
  assert.equal(countDirectoryChanges('ls -la'), 0);
  assert.equal(pm.classifyBashCommand('cd a && cd b && ls'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pushd a; cd b; cat f'), 'unsafe');
  // a single cd + read stays safe
  assert.equal(pm.classifyBashCommand('cd a && ls'), 'safe');
});

// --- Gap: privilege/shell wrappers are not stripped for allow matching ---
test('wrappers stripped for deny but kept for allow', () => {
  // deny still catches a wrapped command
  assert.equal(stripEnvVarPrefix('sudo python x.py'), 'python x.py');
  assert.equal(stripEnvVarPrefix('env python x.py'), 'python x.py');
  // allow keeps the wrapper so the inner command is not silently auto-approved
  assert.equal(stripEnvVarPrefix('sudo git status', true), 'sudo git status');
  assert.equal(stripEnvVarPrefix('env git status', true), 'env git status');
});

// --- Gap: cd + file-writing redirection path-desync guard ---
// A compound that changes directory then writes via `>`/`>>` resolves the target
// against the NEW cwd at runtime — a path-desync the validator can't see. Claude
// requires manual approval; AMI must not auto-classify it 'safe'.
test('commandHasCdWriteDesync detects cd + redirect, ignores fd-dups and reads', () => {
  assert.equal(commandHasCdWriteDesync('cd /tmp && echo x > f'), true);
  assert.equal(commandHasCdWriteDesync('cd /tmp && echo x >> f'), true);
  assert.equal(commandHasCdWriteDesync('pushd /tmp; echo x > f'), true);
  assert.equal(commandHasCdWriteDesync('Set-Location C:/t; echo x > f'), true);
  // fd-dup (2>&1) is not a file write
  assert.equal(commandHasCdWriteDesync('cd /tmp && ls 2>&1'), false);
  // redirect WITHOUT a cd stays untouched (writes in the known cwd)
  assert.equal(commandHasCdWriteDesync('echo x > f'), false);
  // read after cd is fine
  assert.equal(commandHasCdWriteDesync('cd /tmp && cat a'), false);
  // a `>` inside quotes is not a redirect
  assert.equal(commandHasCdWriteDesync('cd /tmp && echo "a > b"'), false);
});

test('cd + redirect is never auto-classified safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('cd /outside && echo payload > config'), 'unsafe');
  assert.equal(pm.classifyBashCommand('cd /tmp && ls >> log.txt'), 'unsafe');
  // standalone redirect and read-after-cd stay safe (behavior unchanged)
  assert.equal(pm.classifyBashCommand('echo hi > y'), 'safe');
  assert.equal(pm.classifyBashCommand('cd /tmp && ls'), 'safe');
});

// --- Gap: allow-rule env stripping is restricted to a vetted safe allowlist ---
// Allow rules strip only SAFE_ENV_VARS; an escalation prefix (PATH/LD_PRELOAD/
// GIT_DIR/DOCKER_HOST/…) stays and breaks the match → prompt. Deny/ask rules strip
// ALL env vars so a denied command cannot hide behind any prefix.
test('stripEnvVarPrefix strips only safe vars in safe-only mode', () => {
  // safe-only (allow): safe vars dropped, unsafe vars + exact tail preserved
  assert.equal(stripEnvVarPrefix('NODE_ENV=prod git status', true), 'git status');
  assert.equal(stripEnvVarPrefix('LANG=C TERM=xterm ls -la', true), 'ls -la');
  assert.equal(stripEnvVarPrefix('PATH=/evil git status', true), 'PATH=/evil git status');
  assert.equal(stripEnvVarPrefix('NODE_ENV=x PATH=/evil git status', true), 'PATH=/evil git status');
  assert.equal(stripEnvVarPrefix('git status', true), 'git status');
  // strip-all (deny/ask): every assignment removed regardless of safety
  assert.equal(stripEnvVarPrefix('PATH=/evil git status'), 'git status');
  assert.equal(stripEnvVarPrefix('A=1 B=2 git status'), 'git status');
});

test('allow rule only matches behind a safe env prefix, deny strips all', async () => {
  const allow = new PermissionManager('ask', [{ tool: 'bash', pattern: 'git *', action: 'allow' }]);
  assert.equal(await allow.check('bash', { command: 'LANG=C git status' }), 'allow');
  assert.equal(await allow.check('bash', { command: 'LD_PRELOAD=/x.so git status' }), 'ask');
  assert.equal(await allow.check('bash', { command: 'DOCKER_HOST=tcp://x git status' }), 'ask');

  const deny = new PermissionManager('ask', [{ tool: 'bash', pattern: 'python *', action: 'deny' }]);
  // deny still fires through ANY env prefix, safe or not
  assert.equal(await deny.check('bash', { command: 'PATH=/evil python hack.py' }), 'deny');
});

test('tool-wide ask rule is bypass-immune but deny still wins', async () => {
  const pm = new PermissionManager();
  pm.addRule({ tool: 'web_fetch', action: 'ask' });
  pm.setMode('bypassPermissions');
  assert.equal(await pm.check('web_fetch', { url: 'https://example.com' }), 'ask');

  const pm2 = new PermissionManager();
  pm2.addRule({ tool: 'bash', pattern: '*secret*', action: 'deny' });
  pm2.addRule({ tool: 'bash', pattern: '*secret*', action: 'ask' });
  pm2.setMode('bypassPermissions');
  // deny is checked first and unconditionally wins over the ask
  assert.equal(await pm2.check('bash', { command: 'cat secret.txt' }), 'deny');
});
