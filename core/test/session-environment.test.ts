import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  _sessionEnvHooks,
  sessionEnvDirPath,
  clearCwdEnvFiles,
  invalidateSessionEnvCache,
  getSessionEnvironmentScript,
} from '../src/session-environment';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

/**
 * Persisted session-environment script (Claude Code `sessionEnvironment.ts`
 * parity): CLAUDE_ENV_FILE/AMI_ENV_FILE handoff, per-hook env fragments with
 * deterministic ordering, tri-state cache + invalidation, cwd-fragment clearing,
 * and the Windows-unsupported branch.
 */
describe('session-environment', () => {
  let tmp: string;
  let env: NodeJS.ProcessEnv;
  const SESSION = 'sess-ABC';

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-session-env-'));
    // SUPERINFERENCE_DATA_DIR makes resolveConfigDir point at the temp dir.
    env = { SUPERINFERENCE_DATA_DIR: tmp };
    invalidateSessionEnvCache();
    _sessionEnvHooks.platform = 'linux';
  });

  afterEach(() => {
    invalidateSessionEnvCache();
    _sessionEnvHooks.platform = process.platform;
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  function dirFor(session = SESSION): string {
    return sessionEnvDirPath(session, env);
  }
  function writeFrag(name: string, content: string, session = SESSION): void {
    const dir = dirFor(session);
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, name), content);
  }

  describe('sessionEnvDirPath', () => {
    it('builds <configDir>/session-env/<id> and sanitizes the id', () => {
      const p = sessionEnvDirPath('a/b..c id', env);
      assert.ok(p.includes(path.join('session-env')));
      const seg = path.basename(p);
      assert.ok(!seg.includes('/'));
      assert.ok(!seg.includes('..'));
      assert.match(seg, /^[A-Za-z0-9._-]+$/);
    });

    it('falls back to "default" for an empty/whitespace id', () => {
      assert.equal(path.basename(sessionEnvDirPath('   ', env)), 'default');
      assert.equal(path.basename(sessionEnvDirPath(undefined, env)), 'default');
    });

    it('caps an over-long id to 200 chars', () => {
      const seg = path.basename(sessionEnvDirPath('x'.repeat(500), env));
      assert.equal(seg.length, 200);
    });
  });

  describe('getSessionEnvironmentScript — Windows', () => {
    it('returns null and does not read disk on Windows', async () => {
      _sessionEnvHooks.platform = 'win32';
      writeFrag('sessionstart-hook-0.sh', 'export A=1');
      assert.equal(await getSessionEnvironmentScript(SESSION, env), null);
    });
  });

  describe('getSessionEnvironmentScript — CLAUDE_ENV_FILE / AMI_ENV_FILE', () => {
    it('loads the parent env file first, trimmed', async () => {
      const f = path.join(tmp, 'parent.sh');
      fs.writeFileSync(f, '  source /opt/venv/bin/activate\n');
      const script = await getSessionEnvironmentScript(SESSION, { ...env, CLAUDE_ENV_FILE: f });
      assert.equal(script, 'source /opt/venv/bin/activate');
    });

    it('AMI_ENV_FILE takes precedence over CLAUDE_ENV_FILE', async () => {
      const a = path.join(tmp, 'ami.sh');
      const c = path.join(tmp, 'claude.sh');
      fs.writeFileSync(a, 'export FROM=ami');
      fs.writeFileSync(c, 'export FROM=claude');
      const script = await getSessionEnvironmentScript(SESSION, { ...env, AMI_ENV_FILE: a, CLAUDE_ENV_FILE: c });
      assert.equal(script, 'export FROM=ami');
    });

    it('ignores an empty env file', async () => {
      const f = path.join(tmp, 'empty.sh');
      fs.writeFileSync(f, '   \n');
      assert.equal(await getSessionEnvironmentScript(SESSION, { ...env, CLAUDE_ENV_FILE: f }), null);
    });

    it('is ENOENT-tolerant for a missing env file', async () => {
      const script = await getSessionEnvironmentScript(SESSION, { ...env, CLAUDE_ENV_FILE: path.join(tmp, 'nope.sh') });
      assert.equal(script, null);
    });

    it('does not crash when the env file is unreadable (non-ENOENT)', async () => {
      // Point at a directory: reading a dir as a file yields EISDIR, not ENOENT.
      const d = path.join(tmp, 'adir');
      fs.mkdirSync(d);
      const script = await getSessionEnvironmentScript(SESSION, { ...env, CLAUDE_ENV_FILE: d });
      assert.equal(script, null);
    });

    it('ignores a blank/whitespace env-file path', async () => {
      writeFrag('setup-hook-0.sh', 'export X=1');
      const script = await getSessionEnvironmentScript(SESSION, { ...env, CLAUDE_ENV_FILE: '   ' });
      assert.equal(script, 'export X=1');
    });
  });

  describe('getSessionEnvironmentScript — hook fragments', () => {
    it('returns null when the session-env dir does not exist (ENOENT-tolerant)', async () => {
      assert.equal(await getSessionEnvironmentScript('never-used', env), null);
    });

    it('concatenates fragments in deterministic priority+index order', async () => {
      // Written out of order; must come back setup < sessionstart < cwdchanged < filechanged, then by index.
      writeFrag('filechanged-hook-0.sh', 'export D=4');
      writeFrag('cwdchanged-hook-1.sh', 'export C2=3b');
      writeFrag('cwdchanged-hook-0.sh', 'export C1=3a');
      writeFrag('sessionstart-hook-0.sh', 'export B=2');
      writeFrag('setup-hook-0.sh', 'export A=1');
      invalidateSessionEnvCache();
      const script = await getSessionEnvironmentScript(SESSION, env);
      assert.equal(
        script,
        ['export A=1', 'export B=2', 'export C1=3a', 'export C2=3b', 'export D=4'].join('\n'),
      );
    });

    it('skips empty fragments and files that do not match the naming convention', async () => {
      writeFrag('setup-hook-0.sh', '   ');
      writeFrag('sessionstart-hook-0.sh', 'export KEEP=1');
      writeFrag('random.sh', 'export IGNORED=1');
      writeFrag('notes.txt', 'export ALSO_IGNORED=1');
      invalidateSessionEnvCache();
      assert.equal(await getSessionEnvironmentScript(SESSION, env), 'export KEEP=1');
    });

    it('merges the parent env file (first) with hook fragments', async () => {
      const f = path.join(tmp, 'parent.sh');
      fs.writeFileSync(f, 'export PARENT=1');
      writeFrag('setup-hook-0.sh', 'export HOOK=2');
      invalidateSessionEnvCache();
      const script = await getSessionEnvironmentScript(SESSION, { ...env, CLAUDE_ENV_FILE: f });
      assert.equal(script, 'export PARENT=1\nexport HOOK=2');
    });
  });

  describe('tri-state cache + invalidation', () => {
    it('caches a loaded script (second read ignores new disk state until invalidated)', async () => {
      writeFrag('setup-hook-0.sh', 'export A=1');
      const first = await getSessionEnvironmentScript(SESSION, env);
      assert.equal(first, 'export A=1');
      // Mutate disk; without invalidation the cached value is returned.
      writeFrag('sessionstart-hook-0.sh', 'export B=2');
      assert.equal(await getSessionEnvironmentScript(SESSION, env), 'export A=1');
      invalidateSessionEnvCache();
      assert.equal(await getSessionEnvironmentScript(SESSION, env), 'export A=1\nexport B=2');
    });

    it('caches the empty (null) result so disk is not re-scanned', async () => {
      assert.equal(await getSessionEnvironmentScript(SESSION, env), null);
      writeFrag('setup-hook-0.sh', 'export LATE=1');
      // null is cached — still null until invalidated.
      assert.equal(await getSessionEnvironmentScript(SESSION, env), null);
      invalidateSessionEnvCache();
      assert.equal(await getSessionEnvironmentScript(SESSION, env), 'export LATE=1');
    });

    it('recomputes when the session id changes (cache keyed by session)', async () => {
      writeFrag('setup-hook-0.sh', 'export A=1', 'one');
      writeFrag('setup-hook-0.sh', 'export B=2', 'two');
      assert.equal(await getSessionEnvironmentScript('one', env), 'export A=1');
      assert.equal(await getSessionEnvironmentScript('two', env), 'export B=2');
    });
  });

  describe('clearCwdEnvFiles', () => {
    it('truncates cwd/file-changed fragments, preserves setup/sessionstart', async () => {
      writeFrag('setup-hook-0.sh', 'export KEEP1=1');
      writeFrag('sessionstart-hook-0.sh', 'export KEEP2=2');
      writeFrag('cwdchanged-hook-0.sh', 'export DROP1=3');
      writeFrag('filechanged-hook-0.sh', 'export DROP2=4');
      await clearCwdEnvFiles(SESSION, env);
      const dir = dirFor();
      assert.equal(fs.readFileSync(path.join(dir, 'setup-hook-0.sh'), 'utf8'), 'export KEEP1=1');
      assert.equal(fs.readFileSync(path.join(dir, 'sessionstart-hook-0.sh'), 'utf8'), 'export KEEP2=2');
      assert.equal(fs.readFileSync(path.join(dir, 'cwdchanged-hook-0.sh'), 'utf8'), '');
      assert.equal(fs.readFileSync(path.join(dir, 'filechanged-hook-0.sh'), 'utf8'), '');
    });

    it('is a no-op (no throw) when the session-env dir is absent', async () => {
      await assert.doesNotReject(() => clearCwdEnvFiles('missing', env));
    });

    it('invalidation after clear yields only the preserved fragments', async () => {
      writeFrag('setup-hook-0.sh', 'export KEEP=1');
      writeFrag('cwdchanged-hook-0.sh', 'export OLD=2');
      await getSessionEnvironmentScript(SESSION, env); // prime cache
      await clearCwdEnvFiles(SESSION, env);
      invalidateSessionEnvCache();
      assert.equal(await getSessionEnvironmentScript(SESSION, env), 'export KEEP=1');
    });
  });
});

/**
 * End-to-end through the real `bash` tool: an exported var from the parent
 * CLAUDE_ENV_FILE (and from a per-hook fragment) must be visible to a command
 * even though each bash invocation spawns a fresh, scrubbed environment —
 * proving the session-env script is prepended to the executed command.
 *
 * RED/GREEN: reverting the bash.ts prepend makes `$MYVAR`/`$HOOKVAR` empty, so
 * `val=` / `hook=` fail these assertions. They are discriminating.
 */
describe('session-environment — end-to-end through bash tool', () => {
  let tmp: string;
  const saved: Record<string, string | undefined> = {};

  function setEnv(k: string, v: string | undefined): void {
    if (!(k in saved)) saved[k] = process.env[k];
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }

  beforeEach(() => {
    tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-session-env-e2e-'));
    setEnv('SUPERINFERENCE_DATA_DIR', tmp);
    setEnv('AMI_SANDBOX', 'off'); // non-container determinism (AGENTS.md)
    invalidateSessionEnvCache();
    _sessionEnvHooks.platform = process.platform;
  });

  afterEach(() => {
    invalidateSessionEnvCache();
    for (const [k, v] of Object.entries(saved)) setEnv(k, v);
    for (const k of Object.keys(saved)) delete saved[k];
    fs.rmSync(tmp, { recursive: true, force: true });
  });

  function ctx(sessionId: string): ToolContext {
    return { cwd: process.cwd(), abortSignal: new AbortController().signal, sessionId };
  }

  it('prepends CLAUDE_ENV_FILE exports so a var survives into a bash command', async () => {
    const envFile = path.join(tmp, 'activate.sh');
    fs.writeFileSync(envFile, 'export MYVAR=hello-from-parent\n');
    setEnv('CLAUDE_ENV_FILE', envFile);
    invalidateSessionEnvCache();

    const r1 = await bashTool.execute({ command: 'echo "val=$MYVAR"' }, ctx('e2e-1'));
    assert.ok(!r1.isError);
    assert.match(r1.output, /val=hello-from-parent/);

    // A SECOND, separate invocation still sees it (persisted across stateless calls).
    const r2 = await bashTool.execute({ command: 'echo "again=$MYVAR"' }, ctx('e2e-1'));
    assert.match(r2.output, /again=hello-from-parent/);
  });

  it('folds a per-hook env fragment into subsequent bash commands', async () => {
    const dir = sessionEnvDirPath('e2e-2');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(path.join(dir, 'sessionstart-hook-0.sh'), 'export HOOKVAR=from-hook\n');
    invalidateSessionEnvCache();

    const r = await bashTool.execute({ command: 'echo "hook=$HOOKVAR"' }, ctx('e2e-2'));
    assert.match(r.output, /hook=from-hook/);
  });

  it('leaves the environment untouched when no session env is configured', async () => {
    setEnv('CLAUDE_ENV_FILE', undefined);
    invalidateSessionEnvCache();
    const r = await bashTool.execute({ command: 'echo "plain=$MYVAR"' }, ctx('e2e-3'));
    assert.match(r.output, /plain=/);
    assert.ok(!r.output.includes('hello-from-parent'));
  });
});
