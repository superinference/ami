import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as childProcess from 'child_process';
import {
  bashTool,
  detectForcePushAdvisory,
  detectCompoundCdGit,
  detectForcePushToProtected,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Unit: detectForcePushAdvisory (parity: destructiveCommandWarning.ts —
// warn on ANY force push, independent of branch name)
// ---------------------------------------------------------------------------

describe('detectForcePushAdvisory', () => {
  it('warns on force push to a non-protected feature branch', () => {
    const r = detectForcePushAdvisory('git push --force origin feature/my-work');
    assert.ok(r);
    assert.match(r!, /overwrite remote history/);
  });

  it('warns on force push with no explicit branch arg', () => {
    const r = detectForcePushAdvisory('git push --force');
    assert.ok(r);
  });

  it('warns on --force-with-lease', () => {
    const r = detectForcePushAdvisory('git push --force-with-lease origin dev-branch');
    assert.ok(r);
  });

  it('warns on short -f flag', () => {
    const r = detectForcePushAdvisory('git push -f origin wip');
    assert.ok(r);
  });

  it('is independent of the protected-branch block (fires for feature branch where the hard block does not)', () => {
    const cmd = 'git push --force origin feature/x';
    assert.equal(detectForcePushToProtected(cmd), null); // not hard-blocked
    assert.ok(detectForcePushAdvisory(cmd)); // but still advised
  });

  it('does not warn on a regular (non-force) push', () => {
    assert.equal(detectForcePushAdvisory('git push origin feature/x'), null);
  });

  it('does not warn when "git push --force" appears inside echo', () => {
    assert.equal(detectForcePushAdvisory('echo git push --force'), null);
  });

  it('does not warn on an unrelated command', () => {
    assert.equal(detectForcePushAdvisory('ls -la'), null);
  });
});

// ---------------------------------------------------------------------------
// Unit: detectCompoundCdGit (parity: bashCommandHelpers.ts — bare repository
// attack guard for compound cd+git commands)
// ---------------------------------------------------------------------------

describe('detectCompoundCdGit', () => {
  it('warns when cd into a dir is followed by git', () => {
    const r = detectCompoundCdGit('cd some-repo && git status');
    assert.ok(r);
    assert.match(r!, /bare repository|arbitrary code|untrusted/i);
  });

  it('warns across a pipe/semicolon compound', () => {
    assert.ok(detectCompoundCdGit('cd sub; git log'));
  });

  it('warns when git has a leading path', () => {
    assert.ok(detectCompoundCdGit('cd repo && /usr/bin/git fetch'));
  });

  it('warns for sudo git after cd', () => {
    assert.ok(detectCompoundCdGit('cd repo && sudo git status'));
  });

  it('does not warn for a single git command (no cd)', () => {
    assert.equal(detectCompoundCdGit('git status'), null);
  });

  it('does not warn for cd without git', () => {
    assert.equal(detectCompoundCdGit('cd repo && ls'), null);
  });

  it('does not warn for a bare cd (home) before git', () => {
    assert.equal(detectCompoundCdGit('cd && git status'), null);
  });

  it('does not warn for cd - before git', () => {
    assert.equal(detectCompoundCdGit('cd - && git status'), null);
  });

  it('does not treat "git" substring words as a git invocation', () => {
    assert.equal(detectCompoundCdGit('cd repo && gitk-like status'), null);
  });
});

// ---------------------------------------------------------------------------
// E2E: advisories are appended to the real bashTool output
// ---------------------------------------------------------------------------

describe('bashTool – destructive advisories (e2e)', () => {
  let tmp: string;
  before(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-adv-'));
    // A real git repo so `cd sub && git ...` executes a git subprocess.
    const sub = path.join(tmp, 'sub');
    fs.mkdirSync(sub);
    childProcess.execFileSync('git', ['init', '-q'], { cwd: sub });
  });
  after(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  it('appends a force-push warning to the output of a real force push', async () => {
    // No remote configured → the push fails fast locally, but the command still
    // runs and the warning is appended regardless of exit code.
    const r = await bashTool.execute(
      { command: 'git push --force origin feature/does-not-exist' },
      ctx({ cwd: tmp }),
    );
    assert.match(r.output, /\[Warning:[^\]]*overwrite remote history/);
  });

  it('appends a bare-repository warning for a compound cd+git command', async () => {
    const r = await bashTool.execute(
      { command: 'cd sub && git status' },
      ctx({ cwd: tmp }),
    );
    assert.match(r.output, /\[Warning:[^\]]*bare repository|\[Warning:[^\]]*arbitrary code/);
  });

  it('does not append a warning for a plain command', async () => {
    const r = await bashTool.execute({ command: 'echo hello' }, ctx({ cwd: tmp }));
    assert.ok(!/\[Warning:/.test(r.output));
  });
});
