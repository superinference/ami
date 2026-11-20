import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// Auto-allow hardening — classifyBashCommand must NOT auto-classify as 'safe'
// commands whose flags turn an otherwise read-only utility into an
// arbitrary-command-execution / file-writing vector, and must ESCALATE a
// handful of irreversible git/kubectl forms from 'unsafe' to 'destructive'.
//
// Every assertion below is red-green validated: on reverting the matching
// permissions.ts change the "safe"/"unsafe"→expected line flips back and the
// test fails. The negative-control lines (read-only invocations) lock in that
// the hardening does not over-block.
// Mirrors the reference's per-flag safeFlags restrictions (jq --library-path, man
// pager/html exec, file --compile/--magic-file) and its force-push / branch
// force-delete / kubectl-delete destructive escalation.
// ---------------------------------------------------------------------------

const pm = new PermissionManager();

test('jq/yq --library-path (long form of -L) is not auto-safe', () => {
  // RED without the SAFE_COMMAND_FLAGS.jq + isJqDangerous --library-path entry:
  // classifyBashCommand returns 'safe'.
  assert.equal(pm.classifyBashCommand('jq --library-path /tmp .'), 'unsafe');
  assert.equal(pm.classifyBashCommand('yq --library-path /tmp .'), 'unsafe');
  // negative control: a plain filter stays safe
  assert.equal(pm.classifyBashCommand('jq -r .name data.json'), 'safe');
});

test('man pager/html flags (arbitrary command execution) are not auto-safe', () => {
  // `man -P <cmd>` / `--pager=<cmd>` runs an arbitrary pager; `-H` launches a browser.
  assert.equal(pm.classifyBashCommand('man -P sh ls'), 'unsafe');
  assert.equal(pm.classifyBashCommand('man --pager=sh ls'), 'unsafe');
  assert.equal(pm.classifyBashCommand('man -H ls'), 'unsafe');
  // negative control: a normal man lookup stays safe
  assert.equal(pm.classifyBashCommand('man ls'), 'safe');
});

test('file --compile/--magic-file (disk write / attacker magic DB) is not auto-safe', () => {
  assert.equal(pm.classifyBashCommand('file -C -m /tmp/evil foo'), 'unsafe');
  assert.equal(pm.classifyBashCommand('file --compile'), 'unsafe');
  assert.equal(pm.classifyBashCommand('file --magic-file /tmp/x foo'), 'unsafe');
  // negative control: plain identification stays safe
  assert.equal(pm.classifyBashCommand('file foo.txt'), 'safe');
});

test('git force-push is escalated to destructive (overwrites remote history)', () => {
  // RED without the force-push branch: these classify as merely 'unsafe'.
  assert.equal(pm.classifyBashCommand('git push --force'), 'destructive');
  assert.equal(pm.classifyBashCommand('git push -f origin main'), 'destructive');
  assert.equal(pm.classifyBashCommand('git push --force-with-lease'), 'destructive');
  // negative control: a normal push is unsafe (prompts) but not destructive
  assert.equal(pm.classifyBashCommand('git push origin main'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git push --ff-only origin main'), 'unsafe');
});

test('git branch force-delete (-D / --delete --force) is destructive', () => {
  assert.equal(pm.classifyBashCommand('git branch -D feature'), 'destructive');
  assert.equal(pm.classifyBashCommand('git branch --delete --force feature'), 'destructive');
  // negative control: a safe (-d) delete refuses unmerged work and stays unsafe
  assert.equal(pm.classifyBashCommand('git branch -d feature'), 'unsafe');
});

test('kubectl delete is destructive (removes live cluster resources)', () => {
  assert.equal(pm.classifyBashCommand('kubectl delete pod mypod'), 'destructive');
  assert.equal(pm.classifyBashCommand('kubectl --context prod delete deploy web'), 'destructive');
  // negative control: a read command is unsafe (unknown) but not destructive
  assert.equal(pm.classifyBashCommand('kubectl get pods'), 'unsafe');
});

// ---------------------------------------------------------------------------
// End-to-end through PermissionManager.check(): a wildcard Bash allow rule in
// `ask` mode must NOT silently auto-approve a now-destructive force push — the
// destructive classification downgrades the matching allow to 'ask'. This proves
// the escalation is wired into the real decision path, not just the classifier.
// ---------------------------------------------------------------------------
test('wildcard Bash allow does not auto-approve a destructive force-push (ask mode)', async () => {
  const m = new PermissionManager();
  m.setMode('ask');
  m.addRule({ tool: 'bash', pattern: '*', action: 'allow' });
  // A benign push matches the wildcard allow → allowed.
  assert.equal(await m.check('bash', { command: 'git status' }), 'allow');
  // A force push is destructive → the allow is downgraded to ask.
  assert.equal(await m.check('bash', { command: 'git push --force origin main' }), 'ask');
});
