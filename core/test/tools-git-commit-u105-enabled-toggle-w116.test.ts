/**
 * U105 (w116) — settings-driven MASTER attribution disable toggle
 * (`attribution.enabled: false`). Parity with the reference's deprecated
 * `includeCoAuthoredBy: false`, which zeroed both the commit and PR attribution
 * texts via settings alone (no env var). This covers the COMMIT half in
 * {@link resolveCommitAttribution} plus the end-to-end git_commit path.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  gitCommitTool,
  resolveCommitAttribution,
  CO_AUTHOR_TRAILER,
} from '../src/tools/git-commit';
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

/** Run `fn` with the deprecated env opt-out removed so it cannot skew a test. */
function withoutEnvOptOut(fn: () => void): void {
  const prev = process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
  delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
  try { fn(); } finally {
    if (prev !== undefined) process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = prev;
  }
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('resolveCommitAttribution — settings master disable toggle (attribution.enabled)', () => {
  it('attribution.enabled === false suppresses the trailer (settings-only, no env var)', () => {
    const dir = mkRepo('ami-u105-enabled-false-');
    writeConfig(dir, { enabled: false });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), null));
  });

  it('attribution.enabled === true leaves the default trailer intact (not a force-on no-op)', () => {
    const dir = mkRepo('ami-u105-enabled-true-');
    writeConfig(dir, { enabled: true });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), CO_AUTHOR_TRAILER));
  });

  it('an explicit commit override wins over enabled:false (most-specific setting)', () => {
    const dir = mkRepo('ami-u105-enabled-override-');
    writeConfig(dir, { enabled: false, commit: 'Signed-off-by: P <p@e.com>' });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), 'Signed-off-by: P <p@e.com>'));
  });

  it('enabled:false wins over the auto repo-class gate on an owned remote', () => {
    const dir = mkRepo('ami-u105-enabled-auto-');
    git(dir, 'remote', 'add', 'origin', 'git@github.com:myorg/app.git');
    writeConfig(dir, { enabled: false, auto: true, ownRemotes: ['github.com/myorg/'] });
    withoutEnvOptOut(() => assert.equal(resolveCommitAttribution(dir), null));
  });

  it('end-to-end: git_commit omits the trailer when attribution.enabled is false', async () => {
    const dir = mkRepo('ami-u105-enabled-e2e-');
    writeConfig(dir, { enabled: false });
    fs.writeFileSync(path.join(dir, 'a.txt'), 'hello\n');
    const result = await withEnvOptOutRemoved(() =>
      gitCommitTool.execute({ message: 'add a', files: ['a.txt'] }, ctx(dir)),
    );
    assert.equal(result.isError, false, result.output);
    const body = git(dir, 'log', '-1', '--pretty=%B');
    assert.ok(!/Co-Authored-By:\s*AMI/i.test(body), `trailer must be absent:\n${body}`);
  });

  it('end-to-end: git_commit keeps the trailer when attribution.enabled is absent (default)', async () => {
    const dir = mkRepo('ami-u105-enabled-default-e2e-');
    fs.writeFileSync(path.join(dir, 'b.txt'), 'hello\n');
    const result = await withEnvOptOutRemoved(() =>
      gitCommitTool.execute({ message: 'add b', files: ['b.txt'] }, ctx(dir)),
    );
    assert.equal(result.isError, false, result.output);
    const body = git(dir, 'log', '-1', '--pretty=%B');
    assert.ok(/Co-Authored-By:\s*AMI <ami@superinference\.org>/i.test(body), `trailer must be present:\n${body}`);
  });
});

/** Async variant of {@link withoutEnvOptOut} for the end-to-end commit paths. */
async function withEnvOptOutRemoved<T>(fn: () => Promise<T>): Promise<T> {
  const prev = process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
  delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
  try { return await fn(); } finally {
    if (prev !== undefined) process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = prev;
  }
}
