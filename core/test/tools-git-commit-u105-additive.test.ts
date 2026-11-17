import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  gitCommitTool,
  buildCommitEnv,
  resolveCommitModel,
  sanitizeModelName,
  resolveCommitAttribution,
  shouldPreserveCommitHooks,
  formatCommitSummary,
  _gitCommitFs,
  CO_AUTHOR_TRAILER,
} from '../src/tools/git-commit';
import { classifyRepoRemote } from '../src/utils/git';
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
function writeConfig(dir: string, attribution: Record<string, unknown>): void {
  fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
  fs.writeFileSync(path.join(dir, '.ami', 'config.json'), JSON.stringify({ attribution }));
}
after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

// A synthetic GitHub PAT, assembled at runtime so the literal never lands in the
// committed source (defang against push-protection secret scanning). Matches the
// `github-pat` rule: ghp_ + 36 alphanumerics.
const FAKE_PAT = 'ghp' + '_' + 'A'.repeat(36);

// ---------------------------------------------------------------------------
// Gap 1/3 — secret-CONTENT guard over the staged diff (not just paths/message)
// ---------------------------------------------------------------------------

describe('git_commit — staged-content secret guard', () => {
  it('refuses to commit when an ADDED line contains a credential', async () => {
    const dir = mkRepo('ami-u105-content-secret-');
    fs.writeFileSync(path.join(dir, 'config.ts'), `export const token = "${FAKE_PAT}";\n`);
    const result = await gitCommitTool.execute(
      { message: 'add config', files: ['config.ts'] },
      ctx(dir),
    );
    assert.equal(result.isError, true, result.output);
    assert.match(result.output, /potential secrets detected in the staged changes/);
    // The commit must NOT have been created.
    assert.throws(() => git(dir, 'rev-parse', 'HEAD'));
  });

  it('does NOT flag a diff that only REMOVES a credential', async () => {
    const dir = mkRepo('ami-u105-content-remove-');
    // Commit a file that already contains the secret (bypassing the tool).
    fs.writeFileSync(path.join(dir, 'old.txt'), `leftover = ${FAKE_PAT}\n`);
    git(dir, 'add', 'old.txt');
    git(dir, 'commit', '-q', '-m', 'seed');
    // Now remove the secret line and commit via the tool — removal is a `-` line,
    // which must not trip the added-lines scan.
    fs.writeFileSync(path.join(dir, 'old.txt'), 'cleaned\n');
    const result = await gitCommitTool.execute(
      { message: 'remove leaked token', files: ['old.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, result.output);
  });

  it('commits a clean staged diff without complaint', async () => {
    const dir = mkRepo('ami-u105-content-clean-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'just some text\n');
    const result = await gitCommitTool.execute(
      { message: 'add a', files: ['a.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, result.output);
  });
});

// ---------------------------------------------------------------------------
// Gap 2 / Gap 8 — commit-message quality guidance + secret-file wording
// ---------------------------------------------------------------------------

describe('git_commit — description guidance', () => {
  it('description steers the model on verb semantics and the "why"', () => {
    assert.match(gitCommitTool.description, /add.*new feature/i);
    assert.match(gitCommitTool.description, /update.*enhancement/i);
    assert.match(gitCommitTool.description, /fix.*bug fix/i);
    assert.match(gitCommitTool.description, /why/i);
    assert.match(gitCommitTool.description, /recent commits|repository's existing style/i);
  });

  it('message param echoes the style/verb/why guidance', () => {
    const props = gitCommitTool.inputSchema.properties as Record<string, { description: string }>;
    assert.match(props.message.description, /why/i);
    assert.match(props.message.description, /add = new feature/i);
  });

  it('files param warns against staging secret-bearing files', () => {
    const props = gitCommitTool.inputSchema.properties as Record<string, { description: string }>;
    assert.match(props.files.description, /\.env/);
    assert.match(props.files.description, /credentials\.json/);
    assert.match(props.files.description, /secrets/i);
  });
});

// ---------------------------------------------------------------------------
// Gap 6 — cancellable / non-blocking staging via the async execFileAsync seam
// ---------------------------------------------------------------------------

describe('git_commit — cancellable staging', () => {
  it('forwards context.abortSignal into the staging child', async () => {
    const dir = mkRepo('ami-u105-stage-signal-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
    const controller = new AbortController();
    let seenSignal: unknown;
    const spy = mock.method(_gitCommitFs, 'execFileAsync', async (
      _file: string,
      _args: readonly string[],
      options: { signal?: AbortSignal },
    ) => {
      seenSignal = options.signal;
      // Actually stage so the rest of the flow proceeds normally.
      execFileSync('git', ['add', '--', 'a.txt'], { cwd: dir });
      return { stdout: '', stderr: '' };
    });
    try {
      const result = await gitCommitTool.execute(
        { message: 'add a', files: ['a.txt'] },
        ctx(dir, { abortSignal: controller.signal }),
      );
      assert.equal(result.isError, false, result.output);
      assert.equal(spy.mock.callCount(), 1);
      assert.equal(seenSignal, controller.signal, 'staging must forward the abort signal');
    } finally {
      mock.restoreAll();
    }
  });

  it('aborts staging when the signal is already triggered', async () => {
    const dir = mkRepo('ami-u105-stage-aborted-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
    const controller = new AbortController();
    controller.abort();
    const result = await gitCommitTool.execute(
      { message: 'add a', files: ['a.txt'] },
      ctx(dir, { abortSignal: controller.signal }),
    );
    assert.equal(result.isError, true);
    assert.match(result.output, /Error staging files/);
  });
});

// ---------------------------------------------------------------------------
// Gap 7 — opt-in to PRESERVE commit hooks even in detached mode
// ---------------------------------------------------------------------------

describe('shouldPreserveCommitHooks', () => {
  it('is false by default (detached still bypasses hooks)', () => {
    assert.equal(shouldPreserveCommitHooks({}), false);
  });
  it('is true when AMI_DETACHED_PRESERVE_HOOKS=1', () => {
    assert.equal(shouldPreserveCommitHooks({ AMI_DETACHED_PRESERVE_HOOKS: '1' }), true);
  });
});

describe('git_commit — detached preserve-hooks opt-in', () => {
  function repoWithFailingHook(prefix: string): string {
    const dir = mkRepo(prefix);
    const hookPath = path.join(dir, '.git', 'hooks', 'pre-commit');
    fs.writeFileSync(hookPath, '#!/bin/sh\nexit 1\n', { mode: 0o755 });
    fs.writeFileSync(path.join(dir, 'f.txt'), 'x\n');
    return dir;
  }

  it('AMI_DETACHED_PRESERVE_HOOKS=1 keeps the failing hook active in detached mode', async () => {
    const dir = repoWithFailingHook('ami-u105-preserve-');
    const prev = process.env.AMI_DETACHED_PRESERVE_HOOKS;
    process.env.AMI_DETACHED_PRESERVE_HOOKS = '1';
    try {
      const result = await gitCommitTool.execute(
        { message: 'feat: preserve hooks', files: ['f.txt'] },
        ctx(dir, { detachedMode: true }),
      );
      assert.equal(result.isError, true, 'the preserved pre-commit hook must block the commit');
    } finally {
      if (prev === undefined) delete process.env.AMI_DETACHED_PRESERVE_HOOKS;
      else process.env.AMI_DETACHED_PRESERVE_HOOKS = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// Gap 9 — structured commit-result summary line
// ---------------------------------------------------------------------------

describe('formatCommitSummary', () => {
  it('normalizes "<sha> <subject>" to "Committed <sha>: <subject>"', () => {
    assert.equal(formatCommitSummary('a1b2c3d feat: add thing'), 'Committed a1b2c3d: feat: add thing');
  });
  it('handles a sha with no subject', () => {
    assert.equal(formatCommitSummary('a1b2c3d'), 'Committed a1b2c3d');
  });
  it('returns empty string for empty input', () => {
    assert.equal(formatCommitSummary('   '), '');
  });
});

describe('git_commit — output carries the normalized summary line', () => {
  it('prepends "Committed <shortsha>: <subject>" to the tool output', async () => {
    const dir = mkRepo('ami-u105-summary-');
    fs.writeFileSync(path.join(dir, 'a.txt'), 'x\n');
    const result = await gitCommitTool.execute(
      { message: 'feat: summary line', files: ['a.txt'] },
      ctx(dir),
    );
    assert.equal(result.isError, false, result.output);
    const short = git(dir, 'rev-parse', '--short', 'HEAD');
    assert.match(result.output, new RegExp(`Committed ${short}: feat: summary line`));
    // The short sha is still present (existing contract preserved).
    assert.ok(result.output.includes(short));
  });
});

// ---------------------------------------------------------------------------
// Gap 12 — model-name stamp in the commit env (sanitized)
// ---------------------------------------------------------------------------

describe('sanitizeModelName', () => {
  it('strips provider prefix, vendor family, and date suffix', () => {
    assert.equal(sanitizeModelName('anthropic/claude-opus-4-8'), 'opus-4-8');
    assert.equal(sanitizeModelName('claude-opus-4-8-20260115'), 'opus-4-8');
    assert.equal(sanitizeModelName('us.anthropic.claude-sonnet-4-5'), 'sonnet-4-5');
    assert.equal(sanitizeModelName('gpt-4o@2026-01-01'), '4o');
  });
  it('returns undefined for blank/empty input', () => {
    assert.equal(sanitizeModelName(undefined), undefined);
    assert.equal(sanitizeModelName('   '), undefined);
  });
});

describe('resolveCommitModel + buildCommitEnv', () => {
  it('prefers AMI_COMMIT_MODEL and sanitizes it', () => {
    assert.equal(resolveCommitModel({ AMI_COMMIT_MODEL: 'claude-opus-4-8-20260115' }), 'opus-4-8');
  });
  it('falls back to AMI_MODEL then ANTHROPIC_MODEL', () => {
    assert.equal(resolveCommitModel({ AMI_MODEL: 'claude-haiku-4-5' }), 'haiku-4-5');
    assert.equal(resolveCommitModel({ ANTHROPIC_MODEL: 'claude-sonnet-4-5' }), 'sonnet-4-5');
  });
  it('is undefined when no model env is set', () => {
    assert.equal(resolveCommitModel({}), undefined);
  });
  it('stamps AI_MODEL into the commit env when set', () => {
    const prev = process.env.AMI_COMMIT_MODEL;
    process.env.AMI_COMMIT_MODEL = 'claude-opus-4-8';
    try {
      assert.equal(buildCommitEnv().AI_MODEL, 'opus-4-8');
    } finally {
      if (prev === undefined) delete process.env.AMI_COMMIT_MODEL;
      else process.env.AMI_COMMIT_MODEL = prev;
    }
  });
  it('omits AI_MODEL when no model env is present', () => {
    const saved = {
      a: process.env.AMI_COMMIT_MODEL, b: process.env.AMI_MODEL, c: process.env.ANTHROPIC_MODEL,
    };
    delete process.env.AMI_COMMIT_MODEL;
    delete process.env.AMI_MODEL;
    delete process.env.ANTHROPIC_MODEL;
    try {
      assert.equal('AI_MODEL' in buildCommitEnv(), false);
    } finally {
      if (saved.a !== undefined) process.env.AMI_COMMIT_MODEL = saved.a;
      if (saved.b !== undefined) process.env.AMI_MODEL = saved.b;
      if (saved.c !== undefined) process.env.ANTHROPIC_MODEL = saved.c;
    }
  });
});

// ---------------------------------------------------------------------------
// Gap 13/14/15 — repo-class-aware attribution (classifyRepoRemote + auto gate)
// ---------------------------------------------------------------------------

describe('classifyRepoRemote', () => {
  it('returns "none" when there is no origin remote', () => {
    const dir = mkRepo('ami-u105-class-none-');
    assert.equal(classifyRepoRemote(dir, ['github.com/me/']), 'none');
  });
  it('returns "owned" when the remote matches an owned pattern', () => {
    const dir = mkRepo('ami-u105-class-owned-');
    git(dir, 'remote', 'add', 'origin', 'git@github.com:myorg/app.git');
    assert.equal(classifyRepoRemote(dir, ['github.com/myorg/']), 'owned');
  });
  it('returns "external" when the remote matches no owned pattern', () => {
    const dir = mkRepo('ami-u105-class-ext-');
    git(dir, 'remote', 'add', 'origin', 'https://github.com/upstream/project.git');
    assert.equal(classifyRepoRemote(dir, ['github.com/myorg/']), 'external');
  });
  it('ignores blank owned patterns (no accidental match)', () => {
    const dir = mkRepo('ami-u105-class-blank-');
    git(dir, 'remote', 'add', 'origin', 'https://github.com/upstream/project.git');
    assert.equal(classifyRepoRemote(dir, ['', '   ']), 'external');
  });
});

describe('resolveCommitAttribution — opt-in repo-class gate', () => {
  function withoutEnvOptOut(fn: () => void): void {
    const prev = process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    try { fn(); } finally {
      if (prev !== undefined) process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = prev;
    }
  }

  it('auto=true suppresses the trailer on an external remote (fail-safe-ON)', () => {
    const dir = mkRepo('ami-u105-auto-ext-');
    git(dir, 'remote', 'add', 'origin', 'https://github.com/upstream/project.git');
    writeConfig(dir, { auto: true, ownRemotes: ['github.com/myorg/'] });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), null));
  });

  it('auto=true suppresses the trailer when there is no remote', () => {
    const dir = mkRepo('ami-u105-auto-none-');
    writeConfig(dir, { auto: true, ownRemotes: ['github.com/myorg/'] });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), null));
  });

  it('auto=true keeps the trailer on a confirmed owned remote', () => {
    const dir = mkRepo('ami-u105-auto-owned-');
    git(dir, 'remote', 'add', 'origin', 'git@github.com:myorg/app.git');
    writeConfig(dir, { auto: true, ownRemotes: ['github.com/myorg/'] });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), CO_AUTHOR_TRAILER));
  });

  it('an explicit commit override wins over the auto gate', () => {
    const dir = mkRepo('ami-u105-auto-override-');
    git(dir, 'remote', 'add', 'origin', 'https://github.com/upstream/project.git');
    writeConfig(dir, { auto: true, ownRemotes: ['github.com/myorg/'], commit: 'Signed-off-by: P <p@e.com>' });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), 'Signed-off-by: P <p@e.com>'));
  });

  it('auto defaults off: the trailer is kept on an external remote when auto is unset', () => {
    const dir = mkRepo('ami-u105-auto-off-');
    git(dir, 'remote', 'add', 'origin', 'https://github.com/upstream/project.git');
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), CO_AUTHOR_TRAILER));
  });
});
