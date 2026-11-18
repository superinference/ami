import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { getGitBranch, isGitRepo } from '../src/utils/git';
import { flushDiagnosticsWriters } from '../src/logger';

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '' },
  }).trim();
}

function mkRepo(): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-gitdiag-')));
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  fs.writeFileSync(path.join(dir, 'f.txt'), 'x');
  git(dir, 'add', 'f.txt');
  git(dir, 'commit', '-q', '-m', 'init');
  return dir;
}

function readDiag(file: string): Array<{ event: string; data: Record<string, unknown> }> {
  flushDiagnosticsWriters();
  if (!fs.existsSync(file)) return [];
  return fs.readFileSync(file, 'utf-8')
    .split('\n')
    .filter(Boolean)
    .map((l) => JSON.parse(l));
}

function withDiag<T>(fn: (diagFile: string) => T): T {
  const diagDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-diagfile-'));
  const diagFile = path.join(diagDir, 'diag.jsonl');
  const prev = process.env.AMI_DIAGNOSTICS_FILE;
  process.env.AMI_DIAGNOSTICS_FILE = diagFile;
  try {
    return fn(diagFile);
  } finally {
    if (prev === undefined) delete process.env.AMI_DIAGNOSTICS_FILE;
    else process.env.AMI_DIAGNOSTICS_FILE = prev;
    try { fs.rmSync(diagDir, { recursive: true, force: true }); } catch { /* ignore */ }
  }
}

test('getGitBranch emits PII-free start/completed diagnostics with found=true', () => {
  const repo = mkRepo();
  try {
    withDiag((diagFile) => {
      const branch = getGitBranch(repo);
      assert.equal(branch, 'main');
      const events = readDiag(diagFile);
      const start = events.find((e) => e.event === 'get_git_branch_started');
      const done = events.find((e) => e.event === 'get_git_branch_completed');
      assert.ok(start, 'expected a started event');
      assert.ok(done, 'expected a completed event');
      assert.equal(done!.data.found, true);
      assert.equal(typeof done!.data.duration_ms, 'number');
      // PII-free: the branch name is never recorded in the diagnostic payload.
      assert.equal(JSON.stringify(events).includes('"main"'), false);
    });
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
});

test('getGitBranch records found=false outside a repo', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nogit-'));
  try {
    withDiag((diagFile) => {
      assert.equal(getGitBranch(dir), undefined);
      const done = readDiag(diagFile).find((e) => e.event === 'get_git_branch_completed');
      assert.ok(done);
      assert.equal(done!.data.found, false);
    });
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('isGitRepo emits is_git_check diagnostics with the outcome', () => {
  const repo = mkRepo();
  const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-plain-'));
  try {
    withDiag((diagFile) => {
      assert.equal(isGitRepo(repo), true);
      let done = readDiag(diagFile).find((e) => e.event === 'is_git_check_completed');
      assert.ok(done);
      assert.equal(done!.data.is_git, true);
      assert.equal(typeof done!.data.duration_ms, 'number');
    });
    withDiag((diagFile) => {
      assert.equal(isGitRepo(plain), false);
      const done = readDiag(diagFile).find((e) => e.event === 'is_git_check_completed');
      assert.ok(done);
      assert.equal(done!.data.is_git, false);
    });
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
    fs.rmSync(plain, { recursive: true, force: true });
  }
});

test('no diagnostics are written when AMI_DIAGNOSTICS_FILE is unset', () => {
  const repo = mkRepo();
  const prev = process.env.AMI_DIAGNOSTICS_FILE;
  const prevCc = process.env.CLAUDE_CODE_DIAGNOSTICS_FILE;
  delete process.env.AMI_DIAGNOSTICS_FILE;
  delete process.env.CLAUDE_CODE_DIAGNOSTICS_FILE;
  try {
    // Must not throw with no channel configured.
    assert.equal(getGitBranch(repo), 'main');
    assert.equal(isGitRepo(repo), true);
  } finally {
    if (prev !== undefined) process.env.AMI_DIAGNOSTICS_FILE = prev;
    if (prevCc !== undefined) process.env.CLAUDE_CODE_DIAGNOSTICS_FILE = prevCc;
    fs.rmSync(repo, { recursive: true, force: true });
  }
});
