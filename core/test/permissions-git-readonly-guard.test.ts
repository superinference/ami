import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  PermissionManager,
  isGitInvocationReadOnly,
  isGitReadOnlyCommandSafe,
} from '../src/permissions';
import { isReadOnlyBashCommand } from '../src/tools/bash-cache';

// --- classification: dangerous git forms must NOT auto-classify as 'safe' -----
// These forms match a git SAFE_PREFIX (git branch/tag/remote/log/...) but smuggle
// a WRITE (ref creation, remote mutation, --output file write) or a sandbox escape
// (cd into an attacker dir, then git runs planted .git/hooks). Claude Code's
// read-only validator (readOnlyCommandValidation.ts) rejects every one of these.

test('git branch <name> (ref creation) is not auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('git branch newbranch'), 'unsafe');
  // list forms stay safe
  assert.equal(pm.classifyBashCommand('git branch'), 'safe');
  assert.equal(pm.classifyBashCommand('git branch -l'), 'safe');
  assert.equal(pm.classifyBashCommand('git branch --list "feature/*"'), 'safe');
});

test('git branch delete/move/force is not auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('git branch -d old'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git branch -D old'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git branch -m a b'), 'unsafe');
});

test('git tag <name> (ref creation) is not auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('git tag v1.0.0'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git tag -a v1 -m x'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git tag -d v1'), 'unsafe');
  // list forms stay safe
  assert.equal(pm.classifyBashCommand('git tag'), 'safe');
  assert.equal(pm.classifyBashCommand('git tag -l "v*"'), 'safe');
});

test('git remote mutation is not auto-safe; list/show stay safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('git remote add origin https://x'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git remote remove origin'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git remote set-url origin https://evil'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git remote rename a b'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git remote'), 'safe');
  assert.equal(pm.classifyBashCommand('git remote -v'), 'safe');
  assert.equal(pm.classifyBashCommand('git remote show origin'), 'safe');
  assert.equal(pm.classifyBashCommand('git remote get-url origin'), 'safe');
});

test('git read command with --output/-o file write is not auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('git log --output=/tmp/evil.txt'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git diff --output /tmp/evil.txt'), 'unsafe');
  assert.equal(pm.classifyBashCommand('git show -o /tmp/evil.txt'), 'unsafe');
  // benign read stays safe
  assert.equal(pm.classifyBashCommand('git log --oneline -20'), 'safe');
});

test('cd + git compound (sandbox escape via planted hooks) is not auto-safe', () => {
  const pm = new PermissionManager();
  assert.equal(pm.classifyBashCommand('cd /tmp/attacker && git status'), 'unsafe');
  assert.equal(pm.classifyBashCommand('pushd /tmp/x && git log'), 'unsafe');
  // git piped into a read-only filter stays safe (no cwd change)
  assert.equal(pm.classifyBashCommand('git status | head'), 'safe');
});

// --- the shared write-guard helper ------------------------------------------

test('isGitInvocationReadOnly tokens-level guard', () => {
  assert.equal(isGitInvocationReadOnly(['git', 'status']), true);
  assert.equal(isGitInvocationReadOnly(['git', 'branch']), true);
  assert.equal(isGitInvocationReadOnly(['git', 'branch', 'newbranch']), false);
  assert.equal(isGitInvocationReadOnly(['git', 'tag', 'v1']), false);
  assert.equal(isGitInvocationReadOnly(['git', 'remote', 'add', 'o', 'u']), false);
  assert.equal(isGitInvocationReadOnly(['git', 'log', '--output=x']), false);
  // git config read vs write
  assert.equal(isGitInvocationReadOnly(['git', 'config', '--get', 'user.name']), true);
  assert.equal(isGitInvocationReadOnly(['git', 'config', 'user.name']), true);
  assert.equal(isGitInvocationReadOnly(['git', 'config', 'user.name', 'Evil']), false);
  assert.equal(isGitInvocationReadOnly(['git', 'config', '--unset', 'user.name']), false);
  // non-git is not this guard's concern
  assert.equal(isGitInvocationReadOnly(['ls', '-la']), true);
});

test('isGitReadOnlyCommandSafe segment-level guard', () => {
  assert.equal(isGitReadOnlyCommandSafe('git status'), true);
  assert.equal(isGitReadOnlyCommandSafe('git branch newbranch'), false);
  assert.equal(isGitReadOnlyCommandSafe('cd /x && git status'), false);
});

// --- bash-cache consolidation: the SAME guard fixes cacheability -------------

test('bash-cache treats git ref/config writes as not read-only', () => {
  // read-only git stays cacheable
  assert.equal(isReadOnlyBashCommand('git status'), true);
  assert.equal(isReadOnlyBashCommand('git log --oneline'), true);
  // writes must NOT be classified read-only (would cache a mutating command)
  assert.equal(isReadOnlyBashCommand('git branch newbranch'), false);
  assert.equal(isReadOnlyBashCommand('git tag v1'), false);
  assert.equal(isReadOnlyBashCommand('git remote add o u'), false);
  assert.equal(isReadOnlyBashCommand('git config user.name Evil'), false);
});
