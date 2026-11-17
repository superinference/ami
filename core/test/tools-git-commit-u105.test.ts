import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  gitCommitTool,
  buildCommitEnv,
  resolveClientSurface,
  resolveCommitAttribution,
  CO_AUTHOR_TRAILER,
} from '../src/tools/git-commit';
import { detectGitTransientState } from '../src/utils/git';
import type { ToolContext } from '../src/types';

function ctx(cwd: string, overrides?: Partial<ToolContext>): ToolContext {
  return { cwd, abortSignal: new AbortController().signal, ...overrides };
}

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '', GIT_CONFIG_NOSYSTEM: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}

function gitAllowFail(cwd: string, ...args: string[]): void {
  try {
    git(cwd, ...args);
  } catch {
    /* expected to fail (e.g. a conflicting merge) */
  }
}

const dirs: string[] = [];
function mkRepo(prefix: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(dir);
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  git(dir, 'config', 'commit.gpgsign', 'false');
  return dir;
}
after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// detectGitTransientState (core/src/utils/git.ts) — unit
// ---------------------------------------------------------------------------

describe('detectGitTransientState', () => {
  it('returns null for a clean repo', () => {
    const dir = mkRepo('ami-u105-clean-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
    git(dir, 'add', 'a.txt');
    git(dir, 'commit', '-q', '-m', 'init');
    assert.equal(detectGitTransientState(dir), null);
  });

  it('returns null when cwd is not a git working tree', () => {
    const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u105-nogit-')));
    dirs.push(dir);
    assert.equal(detectGitTransientState(dir), null);
  });

  for (const [marker, label] of [
    ['MERGE_HEAD', 'merge'],
    ['CHERRY_PICK_HEAD', 'cherry-pick'],
    ['REVERT_HEAD', 'revert'],
    ['BISECT_LOG', 'bisect'],
  ] as const) {
    it(`detects an in-progress ${label} via the ${marker} marker`, () => {
      const dir = mkRepo(`ami-u105-${label}-`);
      fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
      git(dir, 'add', 'a.txt');
      git(dir, 'commit', '-q', '-m', 'init');
      fs.writeFileSync(path.join(dir, '.git', marker), 'deadbeef\n');
      assert.equal(detectGitTransientState(dir), label);
    });
  }

  it('detects an in-progress rebase via the rebase-merge directory marker', () => {
    const dir = mkRepo('ami-u105-rebase-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
    git(dir, 'add', 'a.txt');
    git(dir, 'commit', '-q', '-m', 'init');
    fs.mkdirSync(path.join(dir, '.git', 'rebase-merge'));
    assert.equal(detectGitTransientState(dir), 'rebase');
  });

  it('detects a genuine, git-created merge conflict state end-to-end', () => {
    const dir = mkRepo('ami-u105-realmerge-');
    const f = path.join(dir, 'conflict.txt');
    fs.writeFileSync(f, 'base\n');
    git(dir, 'add', 'conflict.txt');
    git(dir, 'commit', '-q', '-m', 'base');
    git(dir, 'checkout', '-q', '-b', 'feature');
    fs.writeFileSync(f, 'feature-change\n');
    git(dir, 'add', 'conflict.txt');
    git(dir, 'commit', '-q', '-m', 'feature');
    git(dir, 'checkout', '-q', 'main');
    fs.writeFileSync(f, 'main-change\n');
    git(dir, 'add', 'conflict.txt');
    git(dir, 'commit', '-q', '-m', 'main');
    gitAllowFail(dir, 'merge', 'feature'); // conflicts → leaves MERGE_HEAD
    assert.equal(detectGitTransientState(dir), 'merge');
  });
});

// ---------------------------------------------------------------------------
// git_commit — transient-state guard (integration through the tool)
// ---------------------------------------------------------------------------

describe('git_commit — transient git-state guard', () => {
  it('refuses to create a new commit while a merge is in progress', async () => {
    const dir = mkRepo('ami-u105-guard-');
    const f = path.join(dir, 'conflict.txt');
    fs.writeFileSync(f, 'base\n');
    git(dir, 'add', 'conflict.txt');
    git(dir, 'commit', '-q', '-m', 'base');
    git(dir, 'checkout', '-q', '-b', 'feature');
    fs.writeFileSync(f, 'feature-change\n');
    git(dir, 'add', 'conflict.txt');
    git(dir, 'commit', '-q', '-m', 'feature');
    git(dir, 'checkout', '-q', 'main');
    fs.writeFileSync(f, 'main-change\n');
    git(dir, 'add', 'conflict.txt');
    git(dir, 'commit', '-q', '-m', 'main');
    gitAllowFail(dir, 'merge', 'feature');

    // Stage a resolution so the only thing blocking is the transient guard.
    fs.writeFileSync(f, 'resolved\n');
    const result = await gitCommitTool.execute(
      { message: 'resolve conflict', files: ['conflict.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /merge is in progress/);
    assert.match(result.output, /refusing to create a new commit/);
  });

  it('allows a normal commit when no transient operation is active', async () => {
    const dir = mkRepo('ami-u105-normal-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
    const result = await gitCommitTool.execute(
      { message: 'add a', files: ['a.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, result.output);
    assert.match(git(dir, 'log', '--oneline', '-1'), /add a/);
  });
});

// ---------------------------------------------------------------------------
// resolveClientSurface + buildCommitEnv wiring
// ---------------------------------------------------------------------------

describe('resolveClientSurface', () => {
  it('defaults to "cli" when no entrypoint env is set', () => {
    assert.equal(resolveClientSurface({}), 'cli');
  });
  it('prefers AMI_ENTRYPOINT', () => {
    assert.equal(resolveClientSurface({ AMI_ENTRYPOINT: 'vscode' }), 'vscode');
  });
  it('honors CLAUDE_CODE_ENTRYPOINT for cross-tool compatibility', () => {
    assert.equal(resolveClientSurface({ CLAUDE_CODE_ENTRYPOINT: 'sdk' }), 'sdk');
  });
  it('AMI_ENTRYPOINT wins over CLAUDE_CODE_ENTRYPOINT', () => {
    assert.equal(
      resolveClientSurface({ AMI_ENTRYPOINT: 'cli', CLAUDE_CODE_ENTRYPOINT: 'sdk' }),
      'cli',
    );
  });
  it('falls back to "cli" for a blank value', () => {
    assert.equal(resolveClientSurface({ AMI_ENTRYPOINT: '   ' }), 'cli');
  });
  it('is stamped into the commit env as AI_SURFACE', () => {
    const prev = process.env.AMI_ENTRYPOINT;
    process.env.AMI_ENTRYPOINT = 'test-surface';
    try {
      const env = buildCommitEnv();
      assert.equal(env.AI_SURFACE, 'test-surface');
      assert.equal(env.AI_AGENT, 'superinference');
    } finally {
      if (prev === undefined) delete process.env.AMI_ENTRYPOINT;
      else process.env.AMI_ENTRYPOINT = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// resolveCommitAttribution — config precedence over the deprecated env opt-out
// ---------------------------------------------------------------------------

describe('resolveCommitAttribution — precedence', () => {
  function withEnv(val: string | undefined, fn: () => void): void {
    const prev = process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    if (val === undefined) delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    else process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = val;
    try {
      fn();
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
      else process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = prev;
    }
  }

  function writeConfig(dir: string, commit: string): void {
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    fs.writeFileSync(
      path.join(dir, '.ami', 'config.json'),
      JSON.stringify({ attribution: { commit } }),
    );
  }

  it('returns the default AMI trailer with no config and no env opt-out', () => {
    const dir = mkRepo('ami-u105-attr-default-');
    withEnv(undefined, () => {
      assert.equal(resolveCommitAttribution(dir), CO_AUTHOR_TRAILER);
    });
  });

  it('suppresses the trailer via the deprecated env opt-out when no config override', () => {
    const dir = mkRepo('ami-u105-attr-envoff-');
    withEnv('1', () => {
      assert.equal(resolveCommitAttribution(dir), null);
    });
  });

  it('config attribution.commit takes PRECEDENCE over the env opt-out', () => {
    const dir = mkRepo('ami-u105-attr-prec-');
    writeConfig(dir, 'Signed-off-by: Policy <policy@example.com>');
    withEnv('1', () => {
      // Even with the binary opt-out set, an explicit config override wins.
      assert.equal(
        resolveCommitAttribution(dir),
        'Signed-off-by: Policy <policy@example.com>',
      );
    });
  });

  it('an empty config attribution.commit suppresses the trailer regardless of env', () => {
    const dir = mkRepo('ami-u105-attr-empty-');
    writeConfig(dir, '');
    withEnv(undefined, () => {
      assert.equal(resolveCommitAttribution(dir), null);
    });
  });
});
