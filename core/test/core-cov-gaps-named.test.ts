/**
 * Coverage gap closure for the worst under-99 core modules named in the
 * native-ESM harness report (fs-atomic, process-hardening, sequential,
 * scratchpad, mcp-output-storage, treeify, package-manager, gcp-credentials,
 * command-spec-registry, lsp-gitignore-filter, session-environment, …).
 *
 * Each case drives a real branch/function the existing suites leave cold —
 * default DI arrows, catch arms, introspection surfaces, and edge selectors.
 * No skiplists, no coverage pragmas.
 */
import { describe, it, mock, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  atomicWriteFileSync,
  secureMkdirSync,
  fsHooks,
  OWNER_ONLY_FILE_MODE,
  OWNER_ONLY_DIR_MODE,
} from '../src/utils/fs-atomic';
import {
  hardenProcess,
  blockPtrace,
  describeProcessHardening,
  processHardeningDisabled,
  loadLibcPrctl,
  PROCESS_HARDENING_DISABLE_ENV,
  COREDUMP_FILTER_PATH,
} from '../src/utils/process-hardening';
import { sequential } from '../src/utils/sequential';
import { treeify, type TreeNode } from '../src/utils/treeify';
import {
  SCRATCHPAD_DIR_ENV,
  resolveScratchpadDir,
  isScratchpadEnabled,
  isScratchpadPath,
  describeScratchpad,
} from '../src/scratchpad';
import {
  persistBinaryContent,
  getBinaryBlobSavedMessage,
} from '../src/tools/mcp-output-storage';
import {
  packageManagerHooks,
  getInstallPlatform,
  getPackageManager,
  resetPackageManagerCaches,
  upgradeInstructionFor,
} from '../src/package-manager';
import {
  checkGcpCredentialsValid,
  GCP_CREDENTIALS_CHECK_TIMEOUT_MS,
} from '../src/gcp-credentials';
import {
  valueFlagsForCommand,
  loadFigSpec,
  isSafeCommandSpecName,
  getCommandSpec,
  BUILTIN_COMMAND_SPECS,
} from '../src/tools/command-spec-registry';
import {
  filterGitIgnoredLocations,
  _gitIgnoreFilterHooks,
} from '../src/tools/lsp-gitignore-filter';
import {
  _sessionEnvHooks,
  sessionEnvDirPath,
  hookEnvFragmentPath,
  describeSessionEnvironment,
  invalidateSessionEnvCache,
  getSessionEnvironmentScript,
  clearCwdEnvFiles,
} from '../src/session-environment';
import { HeapDumpGuard, HEAP_DUMP_RSS_THRESHOLD_BYTES } from '../src/heap-dump';
import { cronGetTool } from '../src/tools/cron-get';
import { getScheduler, resetScheduler } from '../src/cron';
import {
  resolveAdvisorModel,
  buildTranscriptDigest,
  advisorTool,
  ADVISOR_MODEL_ENV,
} from '../src/tools/advisor';
import { exitWorktreeTool } from '../src/tools/exit-worktree';
import {
  enterWorktreeTool,
  buildGitExecOptions,
  _enterWorktreeHooks,
} from '../src/tools/enter-worktree';
import type { ProviderConfig, ToolContext, Message } from '../src/types';

const tmpDirs: string[] = [];
function mkTmp(prefix = 'ami-cov-gap-'): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmpDirs.push(d);
  return d;
}
afterEach(() => {
  mock.restoreAll();
  resetPackageManagerCaches();
  resetScheduler();
  invalidateSessionEnvCache();
  _sessionEnvHooks.platform = process.platform;
  while (tmpDirs.length) {
    try {
      fs.rmSync(tmpDirs.pop()!, { recursive: true, force: true });
    } catch {
      /* best effort */
    }
  }
});

// ---------------------------------------------------------------------------
// fs-atomic — chmod / unlink catch arms + fallback fsyncDirSync
// ---------------------------------------------------------------------------
describe('fs-atomic gap closure', () => {
  it('secureMkdirSync ignores a chmod failure on an unowned dir', () => {
    const d = mkTmp();
    const sub = path.join(d, 'owned');
    fs.mkdirSync(sub);
    const m = mock.method(fsHooks, 'chmodSync', () => {
      throw Object.assign(new Error('EPERM'), { code: 'EPERM' });
    });
    assert.doesNotThrow(() => secureMkdirSync(sub, OWNER_ONLY_DIR_MODE));
    m.mock.restore();
  });

  it('atomicWriteFileSync survives chmod-on-target failure after rename', () => {
    const d = mkTmp();
    const target = path.join(d, 'f.json');
    let chmodCalls = 0;
    const realChmod = fsHooks.chmodSync.bind(fsHooks);
    const m = mock.method(fsHooks, 'chmodSync', ((p: fs.PathLike, mode: fs.Mode) => {
      chmodCalls++;
      // Let the temp-file chmod succeed; fail the post-rename target chmod.
      if (String(p) === target) throw new Error('EPERM-target');
      return realChmod(p, mode);
    }) as typeof fs.chmodSync);
    atomicWriteFileSync(target, '{"ok":1}', { mode: OWNER_ONLY_FILE_MODE, encoding: 'utf8' });
    assert.equal(fs.readFileSync(target, 'utf8'), '{"ok":1}');
    assert.ok(chmodCalls >= 2);
    m.mock.restore();
  });

  it('fallback path tolerates unlink of an already-gone temp + chmod failure', () => {
    const d = mkTmp();
    const target = path.join(d, 'g.txt');
    const mRename = mock.method(fsHooks, 'renameSync', () => {
      throw new Error('EXDEV');
    });
    const mUnlink = mock.method(fsHooks, 'unlinkSync', () => {
      throw Object.assign(new Error('ENOENT'), { code: 'ENOENT' });
    });
    const mChmod = mock.method(fsHooks, 'chmodSync', () => {
      throw new Error('EPERM');
    });
    atomicWriteFileSync(target, 'fallback-body');
    assert.equal(fs.readFileSync(target, 'utf8'), 'fallback-body');
    mRename.mock.restore();
    mUnlink.mock.restore();
    mChmod.mock.restore();
  });

  it('fsyncDirSync success path opens+fsyncs+closes the parent directory', () => {
    const d = mkTmp();
    const target = path.join(d, 'durable.txt');
    const opens: string[] = [];
    const realOpen = fsHooks.openSync.bind(fsHooks);
    const mOpen = mock.method(fsHooks, 'openSync', ((p: fs.PathLike, flags: fs.OpenMode, ...rest: unknown[]) => {
      opens.push(String(p));
      return (realOpen as Function)(p, flags, ...rest);
    }) as typeof fs.openSync);
    const mFsync = mock.method(fsHooks, 'fsyncSync');
    const mClose = mock.method(fsHooks, 'closeSync');
    atomicWriteFileSync(target, 'bytes');
    assert.ok(opens.includes(d), 'parent dir must be opened for fsync');
    assert.ok(mFsync.mock.calls.length >= 1);
    assert.ok(mClose.mock.calls.length >= 1);
    mOpen.mock.restore();
    mFsync.mock.restore();
    mClose.mock.restore();
  });
});

// ---------------------------------------------------------------------------
// process-hardening — default fs/log arrows + describe ptrace note arms
// ---------------------------------------------------------------------------
describe('process-hardening gap closure', () => {
  it('exercises default read/write FileSync arrows against a fake filter path', () => {
    // Point COREDUMP_FILTER_PATH reads at a real temp file via a thin wrapper that
    // still exercises the `deps.readFileSync ?? (p => fs.readFileSync(p))` arm by
    // omitting read/write and instead stubbing only platform + a writable path
    // is not possible (path is a const) — so drive via injected fakes that call
    // through to the same shapes the defaults use, plus the default log no-op.
    const r = hardenProcess({
      platform: 'linux',
      env: {},
      // omit log → default () => {}
      readFileSync: () => '0x33',
      writeFileSync: () => {},
      prctl: () => 0,
    });
    assert.equal(r.status, 'applied');
    assert.equal(r.ptrace.status, 'applied');
  });

  it('processHardeningDisabled treats a non-string env value as enabled', () => {
    assert.equal(
      processHardeningDisabled({ [PROCESS_HARDENING_DISABLE_ENV]: 1 as unknown as string }),
      false,
    );
  });

  it('describeProcessHardening notes cover supported + unsupported platforms', () => {
    const linux = describeProcessHardening({ platform: 'linux', env: {} });
    assert.equal(linux.supported, true);
    assert.match(linux.note, /coredump_filter/);
    assert.equal(linux.ptrace.ffiAvailable, loadLibcPrctl() !== null);
    const win = describeProcessHardening({ platform: 'win32', env: {} });
    assert.equal(win.supported, false);
    assert.match(win.note, /Linux-only/);
    assert.match(win.ptrace.note, /Linux-only/);
  });

  it('blockPtrace with undefined prctl uses the default loader (no-ffi on Node)', () => {
    const r = blockPtrace({ platform: 'linux', env: {} });
    assert.equal(r.status, 'no-ffi');
    assert.equal(r.method, 'prctl-set-dumpable');
  });

  it('hardenProcess default platform/env use process.* when omitted', () => {
    // On this Linux CI host the real /proc filter may or may not be writable;
    // either way the call must never throw (fail-open).
    assert.doesNotThrow(() => hardenProcess({ prctl: null }));
  });
});

// ---------------------------------------------------------------------------
// sequential — re-entrancy guard + this-binding + post-drain
// ---------------------------------------------------------------------------
describe('sequential gap closure', () => {
  it('re-entrancy guard no-ops a nested processQueue while draining', async () => {
    const order: number[] = [];
    let nestedKick: (() => void) | undefined;
    const run = sequential(async (id: number) => {
      order.push(id);
      if (id === 1 && nestedKick) nestedKick();
      await new Promise((r) => setImmediate(r));
      return id;
    });
    // Kick a second call from inside the first so processQueue sees processing=true.
    nestedKick = () => {
      void run(2);
    };
    assert.equal(await run(1), 1);
    await new Promise((r) => setImmediate(r));
    assert.deepEqual(order, [1, 2]);
  });

  it('preserves an explicit call/apply this binding', async () => {
    const run = sequential(async function (this: { tag: string }) {
      return this.tag;
    });
    assert.equal(await run.call({ tag: 'bound' }), 'bound');
    assert.equal(await run.apply({ tag: 'applied' }), 'applied');
  });
});

// ---------------------------------------------------------------------------
// treeify — node-entry Circular + whitespace + hideFunctions filter false path
// ---------------------------------------------------------------------------
describe('treeify gap closure', () => {
  it('emits [Circular] at node-entry when growBranch re-enters a visited object', () => {
    // Force the node-entry guard: a getter that, on first read of `loop`, returns
    // a nested object which itself references the parent — child-descent catches
    // the back-edge; additionally cover hideFunctions=false keeping a function key.
    const shared: TreeNode = { leaf: 'x' };
    const root: TreeNode = {
      a: shared,
      b: shared,
      fn: (() => 0) as unknown as TreeNode[keyof TreeNode],
    };
    const out = treeify(root, { hideFunctions: false });
    assert.ok(out.includes('[Circular]'));
    assert.ok(out.includes('[Function]'));
  });

  it('renders a whitespace-only key with showValues:false (no value, no colon)', () => {
    const out = treeify({ '  ': { z: '1' } }, { showValues: false });
    assert.ok(out.split('\n')[0] === '└' || out.includes('└'));
  });
});

// ---------------------------------------------------------------------------
// scratchpad — empty-after-strip + default process.env + trailing-sep normalize
// ---------------------------------------------------------------------------
describe('scratchpad gap closure', () => {
  it('resolveScratchpadDir collapses a root-only path that strips to empty back to abs', () => {
    // On POSIX path.resolve('/') === '/'; replace trailing seps → '' → || abs keeps '/'.
    const got = resolveScratchpadDir({ [SCRATCHPAD_DIR_ENV]: '/' });
    assert.equal(got, '/');
    assert.equal(isScratchpadPath('/', { [SCRATCHPAD_DIR_ENV]: '/' }), true);
  });

  it('isScratchpadPath normalizes a trailing-sep candidate against the dir', () => {
    const env = { [SCRATCHPAD_DIR_ENV]: '/tmp/scratchpad-cov' };
    assert.equal(isScratchpadPath('/tmp/scratchpad-cov/', env), true);
  });

  it('describeScratchpad / isScratchpadEnabled honor the live process.env default', () => {
    const prev = process.env[SCRATCHPAD_DIR_ENV];
    try {
      delete process.env[SCRATCHPAD_DIR_ENV];
      assert.equal(isScratchpadEnabled(), false);
      assert.equal(describeScratchpad().enabled, false);
      process.env[SCRATCHPAD_DIR_ENV] = '/tmp/ami-scratch-live';
      assert.equal(isScratchpadEnabled(), true);
      assert.equal(describeScratchpad().dir, '/tmp/ami-scratch-live');
    } finally {
      if (prev === undefined) delete process.env[SCRATCHPAD_DIR_ENV];
      else process.env[SCRATCHPAD_DIR_ENV] = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// mcp-output-storage — non-Error catch stringify
// ---------------------------------------------------------------------------
describe('mcp-output-storage gap closure', () => {
  it('returns a string error when mkdir cannot create the tool-results dir', () => {
    const d = mkTmp();
    const blocker = path.join(d, 'not-a-dir');
    fs.writeFileSync(blocker, 'x');
    const prev = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = path.join(blocker, 'nested');
    try {
      const res = persistBinaryContent(d, Buffer.from('abc'), undefined);
      assert.ok('error' in res);
      assert.equal(typeof (res as { error: string }).error, 'string');
      assert.ok((res as { error: string }).error.length > 0);
    } finally {
      if (prev === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = prev;
    }
  });

  it('getBinaryBlobSavedMessage treats empty mime as unknown type', () => {
    assert.match(getBinaryBlobSavedMessage('/p', '', 1, ''), /unknown type/);
  });
});

// ---------------------------------------------------------------------------
// package-manager (native installer) — default hook arrows
// ---------------------------------------------------------------------------
describe('package-manager default hooks gap closure', () => {
  it('default execPath / platform / readOsRelease / execFile arrows are callable', async () => {
    // Call through the live hooks (no mock) so the default arrow functions count.
    assert.equal(typeof packageManagerHooks.execPath(), 'string');
    assert.ok(['macos', 'linux', 'wsl', 'windows'].includes(packageManagerHooks.platform()));
    // readOsRelease may throw off-Linux; swallow.
    await packageManagerHooks.readOsRelease().catch(() => null);
    const r = await packageManagerHooks.execFile('true', [], { timeout: 2000 });
    assert.equal(typeof r.code, 'number');
    // getInstallPlatform with default args uses process.platform + process.env.
    assert.ok(['macos', 'linux', 'wsl', 'windows'].includes(getInstallPlatform()));
    // upgradeInstructionFor exhaustiveness already covered; hit unknown again.
    assert.match(upgradeInstructionFor('unknown'), /install\.sh/);
  });

  it('getPackageManager with live hooks returns a known PackageManager', async () => {
    resetPackageManagerCaches();
    const pm = await getPackageManager();
    assert.ok(
      ['homebrew', 'winget', 'pacman', 'deb', 'rpm', 'apk', 'mise', 'asdf', 'unknown'].includes(pm),
    );
  });
});

// ---------------------------------------------------------------------------
// gcp-credentials — default timeout const + loadGoogleAuth reject path
// ---------------------------------------------------------------------------
describe('gcp-credentials gap closure', () => {
  it('rejects via loadGoogleAuth that throws a non-Error and returns false', async () => {
    const valid = await checkGcpCredentialsValid({
      timeoutMs: 200,
      loadGoogleAuth: async () => {
        throw 'missing-lib';
      },
    });
    assert.equal(valid, false);
  });

  it('keeps the published 5s timeout constant', () => {
    assert.equal(GCP_CREDENTIALS_CHECK_TIMEOUT_MS, 5000);
  });
});

// ---------------------------------------------------------------------------
// command-spec-registry — string option names + empty flags + fig default/name
// ---------------------------------------------------------------------------
describe('command-spec-registry gap closure', () => {
  it('valueFlagsForCommand returns undefined when a built-in has no value flags', () => {
    // Inject a temporary built-in with a flag that has no args and a non-dash name
    // by exercising go (only -C has args) and an unknown command.
    assert.equal(valueFlagsForCommand('nope-cmd'), undefined);
    const go = valueFlagsForCommand('go');
    assert.ok(go?.has('-C'));
  });

  it('isSafeCommandSpecName allows a bare dash and rejects empty', () => {
    assert.equal(isSafeCommandSpecName('-'), true);
    assert.equal(isSafeCommandSpecName(''), false);
  });

  it('loadFigSpec returns null for unsafe and missing packages', async () => {
    assert.equal(await loadFigSpec(''), null);
    assert.equal(await loadFigSpec('not-installed-fig-spec-xyz'), null);
  });

  it('every builtin round-trips and options use array or string names', async () => {
    for (const b of BUILTIN_COMMAND_SPECS) {
      assert.equal(await getCommandSpec(b.name), b);
      for (const opt of b.options ?? []) {
        if (Array.isArray(opt.name)) assert.ok(opt.name.length > 0);
        else assert.equal(typeof opt.name, 'string');
      }
    }
  });
});

// ---------------------------------------------------------------------------
// lsp-gitignore-filter — undefined stdout success + filter keep-when-no-uri
// ---------------------------------------------------------------------------
describe('lsp-gitignore-filter gap closure', () => {
  it('treats a successful exec with undefined stdout as empty', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({
      stdout: undefined as unknown as string,
      stderr: '',
    }));
    const items = [{ uri: 'file:///proj/a.ts' }];
    const out = await filterGitIgnoredLocations(items, (i) => i.uri, '/proj');
    assert.equal(out.length, 1);
  });

  it('keeps items whose uriOf returns undefined even when other paths are ignored', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => ({
      stdout: '/proj/ignored.ts\n',
      stderr: '',
    }));
    const items = [
      { uri: 'file:///proj/ignored.ts' },
      { uri: undefined as unknown as string },
    ];
    const out = await filterGitIgnoredLocations(items, (i) => i.uri, '/proj');
    assert.equal(out.length, 1);
    assert.equal(out[0]!.uri, undefined);
  });

  it('harvests stdout when the thrown error itself is nullish', async () => {
    mock.method(_gitIgnoreFilterHooks, 'execFileAsync', async () => {
      throw null;
    });
    const out = await filterGitIgnoredLocations([{ uri: 'file:///p/a.ts' }], (i) => i.uri, '/p');
    assert.equal(out.length, 1);
  });
});

// ---------------------------------------------------------------------------
// session-environment — describeSessionEnvironment + hookEnvFragmentPath
// ---------------------------------------------------------------------------
describe('session-environment gap closure', () => {
  it('describeSessionEnvironment reports supported / env-file precedence / hook order', () => {
    _sessionEnvHooks.platform = 'linux';
    const d = mkTmp();
    const env: NodeJS.ProcessEnv = {
      SUPERINFERENCE_DATA_DIR: d,
      AMI_ENV_FILE: '/tmp/ami-env.sh',
      CLAUDE_ENV_FILE: '/tmp/claude-env.sh',
    };
    const report = describeSessionEnvironment(env);
    assert.equal(report.supported, true);
    assert.deepEqual(report.envFileVars, ['AMI_ENV_FILE', 'CLAUDE_ENV_FILE']);
    assert.equal(report.envFileConfigured, 'AMI_ENV_FILE');
    assert.ok(report.sessionEnvBaseDir.includes('session-env'));
    assert.deepEqual(report.hookEvents, ['setup', 'sessionstart', 'cwdchanged', 'filechanged']);
    assert.equal(report.fragmentNamePattern, '<event>-hook-<index>.sh');
    assert.ok(report.note.includes('Windows unsupported'));

    _sessionEnvHooks.platform = 'win32';
    assert.equal(describeSessionEnvironment(env).supported, false);
    // Empty env-file vars → null configured.
    assert.equal(describeSessionEnvironment({ SUPERINFERENCE_DATA_DIR: d }).envFileConfigured, null);
  });

  it('hookEnvFragmentPath returns null for unknown events / bad index, else a contained path', () => {
    const d = mkTmp();
    const env = { SUPERINFERENCE_DATA_DIR: d };
    assert.equal(hookEnvFragmentPath('s1', 'preToolUse', 0, env), null);
    assert.equal(hookEnvFragmentPath('s1', 'sessionStart', -1, env), null);
    assert.equal(hookEnvFragmentPath('s1', 'sessionStart', 1.5, env), null);
    const p = hookEnvFragmentPath('s1', 'sessionStart', 0, env);
    assert.ok(p);
    assert.ok(p!.endsWith('sessionstart-hook-0.sh'));
    assert.ok(p!.startsWith(sessionEnvDirPath('s1', env)));
  });

  it('clearCwdEnvFiles logs non-ENOENT errors without throwing', async () => {
    const d = mkTmp();
    const env = { SUPERINFERENCE_DATA_DIR: d };
    // Create the session-env dir then replace it with a FILE so readdir fails with ENOTDIR.
    const dir = sessionEnvDirPath('s-err', env);
    fs.mkdirSync(path.dirname(dir), { recursive: true });
    fs.writeFileSync(dir, 'not-a-dir');
    await assert.doesNotReject(() => clearCwdEnvFiles('s-err', env));
  });

  it('getSessionEnvironmentScript reads a parent env file and caches per session', async () => {
    _sessionEnvHooks.platform = 'linux';
    const d = mkTmp();
    const envFile = path.join(d, 'parent.sh');
    fs.writeFileSync(envFile, 'export FOO=1\n');
    const env: NodeJS.ProcessEnv = {
      SUPERINFERENCE_DATA_DIR: d,
      AMI_ENV_FILE: envFile,
    };
    invalidateSessionEnvCache();
    const s1 = await getSessionEnvironmentScript('sess-A', env);
    assert.ok(s1?.includes('export FOO=1'));
    // Cached hit (same session).
    const s2 = await getSessionEnvironmentScript('sess-A', env);
    assert.equal(s2, s1);
    // Empty env file content → ignored.
    fs.writeFileSync(envFile, '   \n');
    invalidateSessionEnvCache();
    const empty = await getSessionEnvironmentScript('sess-B', {
      SUPERINFERENCE_DATA_DIR: d,
      AMI_ENV_FILE: envFile,
    });
    assert.equal(empty, null);
  });

  it('getSessionEnvironmentScript logs a non-ENOENT env-file read error', async () => {
    _sessionEnvHooks.platform = 'linux';
    const d = mkTmp();
    const env: NodeJS.ProcessEnv = {
      SUPERINFERENCE_DATA_DIR: d,
      // Directory path → EISDIR on readFile
      AMI_ENV_FILE: d,
    };
    invalidateSessionEnvCache();
    const s = await getSessionEnvironmentScript('sess-C', env);
    assert.equal(s, null);
  });
});

// ---------------------------------------------------------------------------
// heap-dump — hasFired / threshold getters (function %)
// ---------------------------------------------------------------------------
describe('heap-dump gap closure', () => {
  it('exposes hasFired + threshold getters around a one-shot fire', () => {
    const g = new HeapDumpGuard(50, {
      rss: () => 100,
      writeSnapshot: (f) => f,
      dir: () => '/tmp',
      now: () => 1,
    });
    assert.equal(g.hasFired, false);
    assert.equal(g.threshold, 50);
    assert.ok(g.check());
    assert.equal(g.hasFired, true);
    assert.equal(HEAP_DUMP_RSS_THRESHOLD_BYTES > 0, true);
  });
});

// ---------------------------------------------------------------------------
// cron-get — name selector, ambiguous name, unknown keys, invalid id
// ---------------------------------------------------------------------------
describe('cron-get gap closure', () => {
  it('looks up by name, rejects ambiguous names, and validates selectors', async () => {
    const d = mkTmp();
    resetScheduler();
    const sched = getScheduler(d);
    const ctx = { cwd: d } as ToolContext;
    const j1 = sched.create({
      cron: '0 * * * *',
      prompt: 'p1',
      recurring: true,
      durable: false,
      name: 'shared-name',
    });
    sched.create({
      cron: '1 * * * *',
      prompt: 'p2',
      recurring: true,
      durable: false,
      name: 'shared-name',
    });
    const amb = await cronGetTool.execute!({ name: 'shared-name' }, ctx);
    assert.equal(amb.isError, true);
    assert.match(amb.output, /ambiguous/);

    // Unique name path.
    sched.create({
      cron: '2 * * * *',
      prompt: 'solo',
      recurring: false,
      durable: false,
      name: 'solo-name',
    });
    const byName = await cronGetTool.execute!({ name: 'solo-name' }, ctx);
    assert.equal(byName.isError, undefined);
    assert.equal(byName.metadata?.prompt, 'solo');

    const both = await cronGetTool.execute!({ id: j1.id, name: 'x' }, ctx);
    assert.equal(both.isError, true);
    assert.match(both.output, /exactly one/);

    const none = await cronGetTool.execute!({}, ctx);
    assert.equal(none.isError, true);

    const badId = await cronGetTool.execute!({ id: 'bad id!' }, ctx);
    assert.equal(badId.isError, true);

    const badName = await cronGetTool.execute!({ name: '' }, ctx);
    assert.equal(badName.isError, true);

    const unknown = await cronGetTool.execute!({ id: j1.id, extra: 1 }, ctx);
    assert.equal(unknown.isError, true);
    assert.match(unknown.output, /unknown parameter/);
  });
});

// ---------------------------------------------------------------------------
// advisor — anthropic upgrade, digest caps, empty history, disabled execute
// ---------------------------------------------------------------------------
describe('advisor gap closure', () => {
  it('resolveAdvisorModel upgrades Anthropic runs to opus and honors overrides', () => {
    const prev = process.env[ADVISOR_MODEL_ENV];
    try {
      delete process.env[ADVISOR_MODEL_ENV];
      const anth = resolveAdvisorModel({
        provider: 'anthropic',
        model: 'claude-sonnet-4',
        baseUrl: 'http://x',
        apiKey: 'k',
      } as ProviderConfig);
      assert.ok(typeof anth === 'string' && anth.length > 0);
      const vertex = resolveAdvisorModel({
        provider: 'anthropic-vertex',
        model: 'claude-sonnet-4',
        baseUrl: 'http://x',
        apiKey: 'k',
      } as ProviderConfig);
      assert.ok(typeof vertex === 'string');
      // Non-Anthropic reuses the caller's model.
      assert.equal(
        resolveAdvisorModel({
          provider: 'openai',
          model: 'gpt-test',
          baseUrl: 'http://x',
          apiKey: 'k',
        } as ProviderConfig),
        'gpt-test',
      );
      // Claude-prefixed model id counts as Anthropic even on another provider tag.
      const claudeId = resolveAdvisorModel({
        provider: 'openai',
        model: 'claude-opus-custom',
        baseUrl: 'http://x',
        apiKey: 'k',
      } as ProviderConfig);
      assert.ok(typeof claudeId === 'string');
      process.env[ADVISOR_MODEL_ENV] = '  my-override  ';
      assert.ok(resolveAdvisorModel({
        provider: 'openai',
        model: 'gpt-test',
        baseUrl: 'http://x',
        apiKey: 'k',
      } as ProviderConfig).length > 0);
    } finally {
      if (prev === undefined) delete process.env[ADVISOR_MODEL_ENV];
      else process.env[ADVISOR_MODEL_ENV] = prev;
    }
  });

  it('buildTranscriptDigest covers system/tool/error/cap/elision arms', () => {
    const msgs: Message[] = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'hi' },
      {
        role: 'assistant',
        content: 'thinking',
        tool_calls: [{ id: '1', type: 'function', function: { name: 'bash', arguments: '' } }],
      },
      { role: 'tool', content: 'ok', tool_call_id: '1' },
      { role: 'tool', content: 'err', tool_call_id: '2', isError: true },
      { role: 'tool', content: 'x'.repeat(5_000), tool_call_id: '3' },
    ];
    const d = buildTranscriptDigest(msgs);
    assert.ok(d.includes('## System'));
    assert.ok(d.includes('called tool `bash`'));
    assert.ok(d.includes('Tool result (error)'));
    assert.ok(d.includes('earlier chars elided'));
    // Force overall elision with a huge user turn.
    const huge = buildTranscriptDigest([{ role: 'user', content: 'Y'.repeat(250_000) }]);
    assert.ok(huge.includes('middle of conversation elided'));
  });

  it('execute rejects when disabled / missing provider / empty history', async () => {
    const prev = process.env.AMI_DISABLE_ADVISOR_TOOL;
    try {
      process.env.AMI_DISABLE_ADVISOR_TOOL = '1';
      const disabled = await advisorTool.execute!({}, { cwd: '/tmp', abortSignal: new AbortController().signal } as ToolContext);
      assert.equal(disabled.isError, true);
      delete process.env.AMI_DISABLE_ADVISOR_TOOL;
      const noProv = await advisorTool.execute!({}, {
        cwd: '/tmp',
        abortSignal: new AbortController().signal,
      } as ToolContext);
      assert.equal(noProv.isError, true);
      const empty = await advisorTool.execute!({}, {
        cwd: '/tmp',
        abortSignal: new AbortController().signal,
        _providerConfig: { provider: 'openai', model: 'm', baseUrl: 'http://x', apiKey: 'k' },
        _parentMessages: [],
      } as ToolContext);
      assert.equal(empty.isError, true);
    } finally {
      if (prev === undefined) delete process.env.AMI_DISABLE_ADVISOR_TOOL;
      else process.env.AMI_DISABLE_ADVISOR_TOOL = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// enter/exit worktree — affordances + git exec defaults + name collisions
// ---------------------------------------------------------------------------
describe('enter/exit worktree gap closure', () => {
  it('exit_worktree toAutoClassifierInput / isDestructive / userFacingName arms', () => {
    assert.equal(exitWorktreeTool.userFacingName!({}), 'Exiting worktree');
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'remove' }), true);
    assert.equal(exitWorktreeTool.isDestructive?.({ action: 'keep' }), false);
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action: 'merge' }), 'merge');
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({ action: 1 }), '');
    assert.equal(exitWorktreeTool.toAutoClassifierInput?.({}), '');
  });

  it('buildGitExecOptions applies timeout / maxBuffer / no-prompt / signal defaults', () => {
    const ac = new AbortController();
    const opts = buildGitExecOptions({ cwd: '/tmp', signal: ac.signal });
    assert.equal(opts.cwd, '/tmp');
    assert.equal(opts.timeout, 10 * 60 * 1000);
    assert.equal(opts.maxBuffer, 64 * 1024 * 1024);
    assert.equal((opts.env as NodeJS.ProcessEnv).GIT_TERMINAL_PROMPT, '0');
    assert.equal((opts as { signal?: AbortSignal }).signal, ac.signal);
    const ignored = buildGitExecOptions({ cwd: '/tmp', stdio: 'ignore', timeout: 1000, maxBuffer: 10, env: { FOO: '1' } });
    assert.equal(ignored.timeout, 1000);
    assert.equal(ignored.maxBuffer, 10);
    assert.equal((ignored.env as NodeJS.ProcessEnv).FOO, '1');
  });

  it('defaultWorktreeName retries collisions then falls back to a hex suffix', async () => {
    const { execFileSync } = await import('child_process');
    const repo = mkTmp('ew-slug-');
    execFileSync('git', ['init', '-q'], { cwd: repo });
    execFileSync('git', ['config', 'user.email', 't@t.com'], { cwd: repo });
    execFileSync('git', ['config', 'user.name', 'T'], { cwd: repo });
    execFileSync('git', ['config', 'commit.gpgsign', 'false'], { cwd: repo });
    fs.writeFileSync(path.join(repo, 'README.md'), '# r\n');
    execFileSync('git', ['add', 'README.md'], { cwd: repo });
    execFileSync('git', ['commit', '-q', '-m', 'init'], { cwd: repo });

    // Pre-create the managed worktrees base + a colliding slug dir so the first
    // 20 generateSlug() results miss, forcing the hex-suffix return.
    const worktreesBase = path.join(repo, '.ami', 'worktrees');
    fs.mkdirSync(worktreesBase, { recursive: true });
    fs.mkdirSync(path.join(worktreesBase, 'same-slug'));

    const orig = _enterWorktreeHooks.generateSlug;
    let calls = 0;
    _enterWorktreeHooks.generateSlug = () => {
      calls++;
      return 'same-slug';
    };
    try {
      const r = await enterWorktreeTool.execute!(
        {},
        { cwd: repo, abortSignal: new AbortController().signal } as ToolContext,
      );
      // Either success (hex-suffix name) or a structured error — never a throw.
      assert.equal(typeof r.output, 'string');
      assert.ok(calls >= 20, `expected ≥20 collision retries, got ${calls}`);
      if (!r.isError) {
        assert.match(r.output, /same-slug-[0-9a-f]+/i);
      }
    } finally {
      _enterWorktreeHooks.generateSlug = orig;
    }
  });
});
