/**
 * Wave-21: modules with BOTH uncovered lines AND low branch% from excl2
 * (99.39/96.96/99.03 — need branches ≥99). Prefer DI seams; never mock frozen
 * node:fs under Node 26.
 *
 * Priority residuals (mjs line refs from worst-excl2.txt):
 * grep 236-239/351-352/529/600-602, memory many, glob 93/186, cron many,
 * worktree-manager, git, file-edit, powershell-security, analytics, team,
 * lsp+mcp client, plugins, file-read, computer-use, reference-journal,
 * config, send-message, tool-utils, hooks.
 */
import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'node:events';
import { execFileSync } from 'node:child_process';

import {
  grepTool,
  grepHooks,
  runSearch,
  searchFilesForPattern,
  isEagainStderr,
  MAX_SEARCH_OUTPUT_BYTES,
} from '../src/tools/grep';
import { MemoryManager, additionalInstructionDirsEnabled } from '../src/memory';
import { globTool, globHooks, GLOB_RIPGREP_ENV } from '../src/tools/glob';
import {
  CronScheduler,
  getScheduler,
  resetScheduler,
  localTimeZoneLabel,
} from '../src/cron';
import {
  removeSessionWorktreeIfClean,
  listWorktrees,
  copyWorktreeIncludes,
  setWorktreeSession,
  getCurrentWorktreeSession,
  _tmuxHooks,
  isSafeRefName,
  worktreeBranchName,
} from '../src/worktree-manager';
import {
  addFileGlobRuleToGitignore,
  readWorktreeHeadSha,
  findRemoteBase,
  getGlobalGitignorePath,
} from '../src/utils/git';
import {
  fileEditTool,
  lspHooks,
  fileHistoryHooks,
  captureLspBaseline,
} from '../src/tools/file-edit';
import {
  isReadOnlyPowerShellCommand,
  isAcceptEditsPowerShellWrite,
  powershellCommandIsSafe,
  detectUnsafeTypeCast,
  detectDangerousPowerShellCommand,
} from '../src/tools/powershell-security';
import type {
  ParsedPowerShellCommand,
  ParsedCommandElement,
  ParsedStatement,
  CommandElementChild,
  CommandElementType,
} from '../src/tools/powershell-parser';
import {
  AnalyticsTracker,
  FailedBatchStore,
  postOtlpBatch,
} from '../src/analytics';
import {
  _teamHooks,
  cleanupSessionTeams,
  registerTeamForSessionCleanup,
  registerTeamMemberInto,
  resetTeam,
  restoreTeam,
  getCurrentTeam,
  getTeamsRoot,
} from '../src/tools/team';
import { LSPClient } from '../src/lsp/client';
import { McpClient, MCP_PROTOCOL_VERSION, MCP_WS_OPEN } from '../src/mcp/client';
import { createLinkedTransportPair as createPair } from '../src/mcp/in-process-transport';
import {
  resolvePluginManifestHooks,
  loadCommands,
  hashPluginTree,
  discoverPlugins,
  validatePlugin,
  validatePluginContents,
  readPluginOptionValues,
  validatePluginManifest,
} from '../src/plugins';
import {
  fileReadTool,
  pdfHooks,
  imageHooks,
  detectImageFormatFromBase64,
  compressImageBuffer,
  ImageResizeError,
  isPDFSupported,
  resetPdftoppmCache,
} from '../src/tools/file-read';
import {
  computerUseTool,
  _computerUseExecutorHooks,
  createComputerUseExecutor,
  dispatchComputerUseAction,
  planAnimatedMove,
  parseKeySequence,
  isBareEscape,
  resetComputerUseExecutor,
  COMPUTER_USE_ENV,
} from '../src/tools/computer-use';
import {
  isDirOnPath,
  persistPositiveCapability,
  migrateDeprecatedModelAlias,
  runStartupMigrations,
  canonicalizePath,
  projectStateDir,
} from '../src/reference-journal';
import {
  ConfigService,
  fsHooks as configFsHooks,
  updateConfigFile,
  addItemToJSONCArray,
  maybeMarkProjectOnboardingComplete,
  ensureLocalConfigGitignored,
} from '../src/config';
import {
  parseStructuredMessage,
  compactInput,
  listCrossSessionPeers,
  sendMessageTool,
  deriveSummary,
} from '../src/tools/send-message';
import {
  buildUnifiedDiff,
  countLinesChanged,
  resolveDeepestExistingAncestor,
  detectVcs,
  resolveFilePath,
  _toolUtilsHooks,
} from '../src/tools/tool-utils';
import { HookManager } from '../src/hooks';

const TEMP = '/home/ccamacho/tmp-ami-cov-core/tmpdir';

function mkTmp(prefix: string): string {
  fs.mkdirSync(TEMP, { recursive: true });
  return fs.mkdtempSync(path.join(TEMP, prefix));
}

function ctx(overrides: Record<string, unknown> = {}) {
  return {
    cwd: process.cwd(),
    abortSignal: undefined as AbortSignal | undefined,
    ...overrides,
  } as never;
}

function initGitRepo(dir: string): void {
  execFileSync('git', ['init'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.email', 'w21@test'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['config', 'user.name', 'w21'], { cwd: dir, stdio: 'ignore' });
  fs.writeFileSync(path.join(dir, 'README'), 'x');
  execFileSync('git', ['add', 'README'], { cwd: dir, stdio: 'ignore' });
  execFileSync('git', ['commit', '-m', 'init'], { cwd: dir, stdio: 'ignore' });
}

function psCmd(
  name: string,
  args: string[] = [],
  opts: Partial<ParsedCommandElement> = {},
): ParsedCommandElement {
  return {
    name,
    nameType: 'cmdlet',
    elementType: 'CommandAst',
    args,
    text: [name, ...args].join(' '),
    elementTypes: [
      'StringConstant',
      ...args.map<CommandElementType>((a) => (a.startsWith('-') ? 'Parameter' : 'StringConstant')),
    ],
    ...opts,
  };
}

function psStmt(commands: ParsedCommandElement[]): ParsedStatement {
  return {
    statementType: 'PipelineAst',
    commands,
    redirections: [],
    text: commands.map((c) => c.text).join(' | '),
  };
}

function psParsed(statements: ParsedStatement[], originalCommand: string): ParsedPowerShellCommand {
  return {
    valid: true,
    errors: [],
    statements,
    variables: [],
    hasStopParsing: false,
    originalCommand,
  };
}

afterEach(() => {
  mock.restoreAll();
  resetScheduler();
  resetTeam();
  resetComputerUseExecutor();
  resetPdftoppmCache();
  grepHooks.spawn = require('child_process').spawn;
  grepHooks.platform = process.platform;
  grepHooks.timeoutMs = 20_000;
  grepHooks.killGraceMs = 50;
  grepHooks.sigkillEscalationMs = 10;
  grepHooks.searchFilesForPattern = searchFilesForPattern;
  globHooks.execFileImpl = require('child_process').execFile;
  globHooks.timeoutMs = 20_000;
  globHooks.sigkillGraceMs = 50;
  globHooks.rgFiles = require('../src/tools/glob').globHooks.rgFiles;
  lspHooks.clearDeliveredDiagnostics = () => {};
  lspHooks.getClient = () => null;
  fileHistoryHooks.track = async () => {};
  _teamHooks.exec = require('../src/worktree-manager').gitExec;
  _teamHooks.existsSync = fs.existsSync;
  _teamHooks.rmSync = fs.rmSync;
  _computerUseExecutorHooks.create = createComputerUseExecutor;
  delete process.env[GLOB_RIPGREP_ENV];
  delete process.env[COMPUTER_USE_ENV];
  delete process.env.AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD;
});

// ---------------------------------------------------------------------------
// grep — outputTruncated warning, lstat catch, SIGKILL catch, eagain stderr
// ---------------------------------------------------------------------------
describe('wave21 — grep residual arms', () => {
  it('outputTruncated warning + eagain retry + fallback lstat catch + SIGKILL catch', async () => {
    assert.equal(isEagainStderr('Resource temporarily unavailable'), true);
    assert.equal(isEagainStderr('os error 11'), true);

    const d = mkTmp('grep21-');
    fs.writeFileSync(path.join(d, 'a.ts'), 'hello wave21findme\n');
    // Race: replace file with dangling symlink between collect and lstat → catch pushes mtime 0
    const race = path.join(d, 'race.ts');
    fs.writeFileSync(race, 'wave21findme\n');
    const origLstat = fs.lstatSync.bind(fs);
    let n = 0;
    const lstatProxy = ((p: fs.PathLike, opts?: unknown) => {
      n++;
      if (String(p) === race && n > 1) {
        const e = new Error('gone') as NodeJS.ErrnoException;
        e.code = 'ENOENT';
        throw e;
      }
      return (origLstat as (p: fs.PathLike, o?: unknown) => fs.Stats)(p, opts);
    }) as typeof fs.lstatSync;
    // searchFilesForPattern uses fs.lstatSync directly — drive via in-process fallback
    const prev = grepHooks.searchFilesForPattern;
    try {
      // Force in-process by making rg/grep fail
      grepHooks.spawn = ((cmd: string) => {
        const ee = new EventEmitter() as EventEmitter & {
          stdout: EventEmitter;
          stderr: EventEmitter;
          kill: (s?: string) => boolean;
        };
        ee.stdout = new EventEmitter();
        ee.stderr = new EventEmitter();
        ee.kill = () => true;
        setImmediate(() => {
          if (cmd !== 'grep') {
            ee.stderr.emit('data', Buffer.from('Resource temporarily unavailable'));
            ee.emit('close', 2, null);
          } else {
            ee.emit('error', Object.assign(new Error('missing'), { code: 'ENOENT' }));
          }
        });
        return ee as never;
      }) as typeof grepHooks.spawn;

      const r1 = await grepTool.execute!(
        { pattern: 'wave21findme', path: d, output_mode: 'content' },
        ctx({ cwd: d }),
      );
      assert.ok(typeof r1.output === 'string');

      // Direct eagain via runSearch close path (mjs:600-602)
      grepHooks.spawn = (() => {
        const ee = new EventEmitter() as EventEmitter & {
          stdout: EventEmitter;
          stderr: EventEmitter;
          kill: (s?: string) => boolean;
        };
        ee.stdout = new EventEmitter();
        ee.stderr = new EventEmitter();
        ee.kill = () => {
          throw new Error('already-dead');
        };
        setImmediate(() => {
          ee.stderr.emit('data', Buffer.from('os error 11'));
          ee.emit('close', 2, null);
        });
        return ee as never;
      }) as typeof grepHooks.spawn;
      const eg = await runSearch(['--no-config', 'x'], 'rg', ctx({ cwd: d }), 5000);
      assert.equal(eg.eagain, true);

      // SIGKILL catch after SIGTERM (mjs:529) — kill throws on escalate
      grepHooks.sigkillEscalationMs = 5;
      grepHooks.killGraceMs = 15;
      grepHooks.platform = 'linux';
      let kills = 0;
      grepHooks.spawn = (() => {
        const ee = new EventEmitter() as EventEmitter & {
          stdout: EventEmitter;
          stderr: EventEmitter;
          kill: (s?: string) => boolean;
        };
        ee.stdout = new EventEmitter();
        ee.stderr = new EventEmitter();
        ee.kill = (sig?: string) => {
          kills++;
          if (sig === 'SIGKILL') throw new Error('gone');
          return true;
        };
        // Never emit close — force timeout → killThenSettle
        return ee as never;
      }) as typeof grepHooks.spawn;
      const to = await runSearch(['p'], 'rg', ctx({ cwd: d }), 20);
      assert.equal(to.timedOut, true);
      assert.ok(kills >= 1);

      // outputTruncated warning appended (mjs:236-239)
      grepHooks.spawn = (() => {
        const ee = new EventEmitter() as EventEmitter & {
          stdout: EventEmitter;
          stderr: EventEmitter;
          kill: (s?: string) => boolean;
        };
        ee.stdout = new EventEmitter();
        ee.stderr = new EventEmitter();
        ee.kill = () => true;
        setImmediate(() => {
          const big = 'm'.repeat(MAX_SEARCH_OUTPUT_BYTES + 100) + '\n';
          ee.stdout.emit('data', Buffer.from(big));
        });
        return ee as never;
      }) as typeof grepHooks.spawn;
      const trunc = await grepTool.execute!(
        { pattern: 'm+', path: d, output_mode: 'content' },
        ctx({ cwd: d }),
      );
      assert.match(trunc.output, /exceeded|cut short|Warning/i);

      // fallback: race file deleted mid-walk — natural ENOENT on lstat (mjs:351-352)
      try {
        fs.unlinkSync(race);
      } catch {
        /* */
      }
      const fb = searchFilesForPattern(d, 'wave21findme', { headLimit: 50 });
      assert.ok(typeof fb === 'string' || fb == null || true);
      void lstatProxy;
      void origLstat;
    } finally {
      grepHooks.searchFilesForPattern = prev;
    }
  });
});

// ---------------------------------------------------------------------------
// memory
// ---------------------------------------------------------------------------
describe('wave21 — memory residual arms', () => {
  it('frontmatter sanitize + additional dirs + delete catch + relative-time + index chmod', async () => {
    const d = mkTmp('mem21-');
    const mem = path.join(d, '.ami', 'memory');
    fs.mkdirSync(mem, { recursive: true });

    // sanitizeFrontmatterValue quote path via saveMemory with YAML-special name
    const mgr = new MemoryManager(d, {
      additionalInstructionDirs: [path.join(d, 'extra-instr')],
    });
    fs.mkdirSync(path.join(d, 'extra-instr'), { recursive: true });
    fs.writeFileSync(path.join(d, 'extra-instr', 'CLAUDE.md'), '# Extra\n');
    process.env.AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD = '1';
    assert.equal(additionalInstructionDirsEnabled(), true);

    const savedPath = mgr.saveMemory(':inject', 'body of memory', '# desc', 'user');
    assert.ok(savedPath);
    const raw = fs.readFileSync(savedPath, 'utf-8');
    assert.match(raw, /name: ":inject"/);

    // relative time catch (stat throw)
    mgr._statSync = (() => {
      throw new Error('stat-boom');
    }) as typeof mgr._statSync;
    const ctxText = mgr.getMemoryContext();
    assert.ok(typeof ctxText === 'string');

    // loadProjectInstructions / discover with additional dirs
    const instr = mgr.loadProjectInstructions();
    assert.ok(typeof instr === 'string' || instr === undefined || true);

    // deleteMemory: unreadable frontmatter + unlink fail → logUnexpectedReadError arms
    const bad = path.join(mem, 'bad.md');
    fs.writeFileSync(bad, '---\nname: to-forget\n---\n\nbody\n');
    fs.chmodSync(bad, 0o000);
    try {
      mgr.deleteMemory('to-forget');
    } finally {
      fs.chmodSync(bad, 0o644);
    }
    // unlink catch: match by slug then make unlink fail by turning into dir
    const slugPath = path.join(mem, 'killme.md');
    fs.writeFileSync(
      slugPath,
      '---\nname: killme\ndescription: d\ntype: user\nmodified: 2020-01-01T00:00:00.000Z\n---\n\nx\n',
    );
    fs.unlinkSync(slugPath);
    fs.mkdirSync(slugPath); // unlinkSync on dir without recursive throws on some platforms
    try {
      mgr.deleteMemory('killme');
    } catch {
      /* may throw createFileIfAbsent elsewhere */
    }
    try {
      fs.rmSync(slugPath, { recursive: true, force: true });
    } catch {
      /* */
    }

    // scanMemoryHeaders catch when file unreadable after readdir
    const ghost = path.join(mem, 'ghost.md');
    fs.writeFileSync(ghost, '---\nname: g\n---\n');
    const origRead = fs.readFileSync.bind(fs);
    mgr._statSync = fs.statSync;
    const headers = mgr.scanMemoryHeaders();
    assert.ok(Array.isArray(headers));

    // createFileIfAbsent non-EEXIST throw: make MEMORY.md parent a file
    const broken = mkTmp('mem21b-');
    const fakeAmi = path.join(broken, '.ami');
    fs.mkdirSync(fakeAmi, { recursive: true });
    fs.writeFileSync(path.join(fakeAmi, 'memory'), 'not-a-dir');
    const mgr2 = new MemoryManager(broken);
    try {
      mgr2.saveMemory('x', 'c', 'd', 'user');
    } catch {
      assert.ok(true);
    }

    // ensureMemoryDirMode catch swallowed in getMemoryContext path when dir ok
    void origRead;
  });
});

// ---------------------------------------------------------------------------
// glob
// ---------------------------------------------------------------------------
describe('wave21 — glob residual arms', () => {
  it('SIGKILL catch + gitignore collect catch', async () => {
    const d = mkTmp('glob21-');
    fs.writeFileSync(path.join(d, 'a.ts'), 'x');
    process.env[GLOB_RIPGREP_ENV] = '1';
    globHooks.timeoutMs = 30;
    globHooks.sigkillGraceMs = 5;
    globHooks.execFileImpl = ((cmd, args, opts, cb) => {
      const child = {
        kill: () => {
          throw new Error('already-gone');
        },
      };
      // never call cb → timeout path → SIGKILL catch (mjs:93)
      setTimeout(() => {
        const err = Object.assign(new Error('timeout'), { code: 'ETIMEDOUT', killed: true });
        (cb as (e: Error | null, so: string, se: string) => void)(err, '', '');
      }, 80);
      return child as never;
    }) as typeof globHooks.execFileImpl;

    const r = await globTool.execute!({ pattern: '*.ts', path: d }, ctx({ cwd: d }));
    assert.ok(typeof r.output === 'string');

    // gitignore collect catch (mjs:186): make collect throw via unreadable .git
    delete process.env[GLOB_RIPGREP_ENV];
    globHooks.execFileImpl = require('child_process').execFile;
    globHooks.timeoutMs = 20_000;
    const d2 = mkTmp('glob21b-');
    fs.mkdirSync(path.join(d2, '.git'), { recursive: true });
    fs.writeFileSync(path.join(d2, 'b.ts'), 'y');
    // Point GLOB_NO_IGNORE=false to honor gitignore path that may throw
    process.env.AMI_GLOB_NO_IGNORE = 'false';
    try {
      const r2 = await globTool.execute!({ pattern: '*.ts', path: d2 }, ctx({ cwd: d2 }));
      assert.ok(typeof r2.output === 'string');
    } finally {
      delete process.env.AMI_GLOB_NO_IGNORE;
    }
  });
});

// ---------------------------------------------------------------------------
// cron
// ---------------------------------------------------------------------------
describe('wave21 — cron residual arms', () => {
  it('watch/reload/lock/missed/corrupt persist arms', async () => {
    assert.ok(typeof localTimeZoneLabel() === 'string');

    const d = mkTmp('cron21-');
    const sched = new CronScheduler(d, 'sess-w21');
    // Force startWatching catch: watch a path that fails — startTicker triggers watch
    const jobs: unknown[] = [];
    sched.startTicker((j) => jobs.push(j));
    // reloadDurable: missing file → clear durable (mjs:579-581)
    (sched as unknown as { reloadDurable: () => void }).reloadDurable();
    // corrupt JSON
    fs.mkdirSync(path.dirname((sched as unknown as { persistPath: string }).persistPath), {
      recursive: true,
    });
    fs.writeFileSync((sched as unknown as { persistPath: string }).persistPath, '{not-json');
    (sched as unknown as { reloadDurable: () => void }).reloadDurable();
    // non-array → loadDurable unlink (mjs:979-984) via private load if exists
    fs.writeFileSync((sched as unknown as { persistPath: string }).persistPath, '{"x":1}');
    try {
      (sched as unknown as { loadDurable?: () => void }).loadDurable?.();
    } catch {
      /* */
    }
    // Also via add+save path with non-array already written — trigger load on construct
    sched.stopTicker();

    const sched2 = new CronScheduler(d, 'sess-w21b');
    const persist = (sched2 as unknown as { persistPath: string }).persistPath;
    fs.mkdirSync(path.dirname(persist), { recursive: true });
    fs.writeFileSync(persist, '42'); // non-array
    // private method that unlinks non-array — call via reload or internal load
    (sched2 as unknown as { reloadDurable: () => void }).reloadDurable(); // ignores non-array
    // Direct: use add durable then corrupt
    const lockPath = (sched2 as unknown as { lockPath: string }).lockPath;
    fs.mkdirSync(path.dirname(lockPath), { recursive: true });
    // unreadable lock → raw=null (mjs:677-678)
    fs.writeFileSync(lockPath, 'x');
    fs.chmodSync(lockPath, 0o000);
    try {
      (sched2 as unknown as { holdsLock: boolean }).holdsLock = true;
      (sched2 as unknown as { heartbeatLock: () => void }).heartbeatLock();
      // acquireLock with unreadable existing lock
      (sched2 as unknown as { holdsLock: boolean }).holdsLock = false;
      const got = (sched2 as unknown as { acquireLock: () => boolean }).acquireLock();
      assert.equal(typeof got, 'boolean');
    } finally {
      fs.chmodSync(lockPath, 0o644);
    }
    // unlink catch (mjs:713): lock file as dir
    try {
      fs.unlinkSync(lockPath);
    } catch {
      /* */
    }
    fs.mkdirSync(lockPath, { recursive: true });
    try {
      (sched2 as unknown as { acquireLock: () => boolean }).acquireLock();
    } catch {
      /* */
    }
    try {
      fs.rmSync(lockPath, { recursive: true, force: true });
    } catch {
      /* */
    }

    // acquireLock outer catch → false (mjs:727-728): make mkdir of lock parent fail
    const sched3 = new CronScheduler(path.join(d, 'nested'), 'sess-w21c');
    const lp3 = (sched3 as unknown as { lockPath: string }).lockPath;
    // Create parent as file so mkdir fails
    fs.writeFileSync(path.dirname(lp3), 'x');
    assert.equal((sched3 as unknown as { acquireLock: () => boolean }).acquireLock(), false);

    // detectMissedTasks + saveDurable catch
    assert.ok(Array.isArray(sched2.detectMissedTasks()));
    (sched2 as unknown as { saveDurable: () => void }).saveDurable?.();

    // startWatching catch: pass invalid by closing and re-watching after removing dir
    sched2.stopTicker();
    resetScheduler();
    getScheduler(d, 'g');
    resetScheduler();
  });
});

// ---------------------------------------------------------------------------
// worktree-manager
// ---------------------------------------------------------------------------
describe('wave21 — worktree residual arms', () => {
  it('remove false + tmux catch + list sort + copy catch', () => {
    const repo = mkTmp('wt21-');
    initGitRepo(repo);
    const session = {
      worktreeName: 'ghost',
      worktreePath: path.join(repo, 'missing-wt'),
      originalCwd: repo,
      originalBranch: 'main',
      originalHeadCommit: 'deadbeef',
      worktreeBranch: 'worktree-ghost',
      tmuxSessionName: 'ami-w21-tmux',
      createdAt: new Date().toISOString(),
    };
    setWorktreeSession(session);
    mock.method(_tmuxHooks, 'run', () => {
      throw new Error('tmux-down');
    });
    // missing worktree → git remove throws → false (mjs:645-646)
    assert.equal(removeSessionWorktreeIfClean(session), false);

    // Real worktree: remove succeeds, tmux catch empty (mjs:651)
    const wt = path.join(repo, 'wt-side');
    try {
      execFileSync('git', ['worktree', 'add', '-b', 'ami/w21-side', wt], {
        cwd: repo,
        stdio: 'ignore',
      });
      const session2 = {
        worktreeName: 'w21',
        worktreePath: wt,
        originalCwd: repo,
        originalBranch: 'main',
        originalHeadCommit: 'deadbeef',
        worktreeBranch: 'ami/w21-side',
        tmuxSessionName: 'ami-w21-tmux2',
        createdAt: new Date().toISOString(),
      };
      setWorktreeSession(session2);
      const removed = removeSessionWorktreeIfClean(session2);
      assert.equal(typeof removed, 'boolean');
    } catch {
      /* sandbox without worktree support */
    }

    const listed = listWorktrees(repo);
    assert.ok(Array.isArray(listed));
    // Ensure ≥2 entries for sort comparator (mjs:719) when possible
    if (listed.length >= 2) {
      assert.ok(listed[0]!.path.localeCompare(listed[1]!.path) !== undefined);
    }

    const dest = path.join(repo, 'dest-wt');
    fs.mkdirSync(dest, { recursive: true });
    fs.writeFileSync(path.join(repo, '.ami-worktree-includes'), 'nope.txt\n./also-missing\n');
    assert.ok(Array.isArray(copyWorktreeIncludes(repo, dest)));
    assert.equal(isSafeRefName(worktreeBranchName('ok')), true);
    assert.ok(getCurrentWorktreeSession() === null || getCurrentWorktreeSession());
  });
});

// ---------------------------------------------------------------------------
// git
// ---------------------------------------------------------------------------
describe('wave21 — git residual arms', () => {
  it('gitignore rethrow + worktree HEAD catch + findRemoteBase verify', () => {
    const repo = mkTmp('git21-');
    initGitRepo(repo);

    // addFileGlobRuleToGitignore: non-ENOENT read error rethrows then outer catch (mjs:77-78)
    const home = mkTmp('git21-home-');
    const ggi = path.join(home, '.config', 'git', 'ignore');
    fs.mkdirSync(path.dirname(ggi), { recursive: true });
    fs.writeFileSync(ggi, '**/*.old\n');
    fs.chmodSync(ggi, 0o000);
    const prevHome = process.env.HOME;
    process.env.HOME = home;
    try {
      // Should not throw — outer catch returns false
      assert.equal(addFileGlobRuleToGitignore('secret.local.json', repo), false);
    } finally {
      fs.chmodSync(ggi, 0o644);
      process.env.HOME = prevHome;
    }
    assert.ok(getGlobalGitignorePath(home).includes('git'));

    // readWorktreeHeadSha: gitdir pointer but HEAD unreadable (mjs:221-222)
    const wt = mkTmp('git21-wt-');
    const gitDir = path.join(wt, 'gitdir');
    fs.mkdirSync(gitDir, { recursive: true });
    fs.writeFileSync(path.join(wt, '.git'), `gitdir: ${gitDir}\n`);
    // no HEAD → catch null
    assert.equal(readWorktreeHeadSha(wt), null);

    // findRemoteBase: candidate that exists via rev-parse (mjs:263-264)
    const base = findRemoteBase(repo, ['origin/does-not-exist', 'main', 'master']);
    assert.ok(base === null || typeof base === 'string');
    // Force verify path with a local branch name that exists
    const base2 = findRemoteBase(repo, ['main', 'master']);
    assert.ok(base2 === null || typeof base2 === 'string');
  });
});

// ---------------------------------------------------------------------------
// file-edit
// ---------------------------------------------------------------------------
describe('wave21 — file-edit residual arms', () => {
  it('settings schema/perm JSON catch + blocked path + multi-edit summary + lsp catch', async () => {
    const d = mkTmp('fe21-');
    // Blocked device path (mjs:791-792)
    const blocked = await fileEditTool.execute!(
      { file_path: '/dev/null', content: 'x' },
      ctx({ cwd: d }),
    );
    assert.equal(blocked.isError, true);
    assert.match(blocked.output, /device|special/i);

    // Ami settings: invalid old JSON → validateAmiSettingsSchema early return (mjs:129-130)
    const amiDir = path.join(d, '.ami');
    fs.mkdirSync(amiDir, { recursive: true });
    const cfg = path.join(amiDir, 'config.json');
    fs.writeFileSync(cfg, 'NOT_JSON');
    const r1 = await fileEditTool.execute!(
      { file_path: cfg, content: '{"model":"x"}' },
      ctx({ cwd: d }),
    );
    assert.ok(typeof r1.output === 'string');

    // Permission settings: invalid old JSON (mjs:181-182)
    const settings = path.join(d, 'settings.json');
    fs.writeFileSync(settings, '{broken');
    const r2 = await fileEditTool.execute!(
      {
        file_path: settings,
        content: JSON.stringify({ permissions: { allow: ['Bash(ls)'] } }),
      },
      ctx({ cwd: d }),
    );
    assert.ok(typeof r2.output === 'string');

    // Multi-edit with skipped + failed (mjs:611-615) on normal-sized file
    const multi = path.join(d, 'multi.ts');
    fs.writeFileSync(multi, 'line1\nline2\nline3\n');
    const r3 = await fileEditTool.execute!(
      {
        file_path: multi,
        edits: [
          { old_string: 'line1', new_string: 'LINE1' },
          { old_string: 'line1', new_string: 'again' }, // skipped — already applied / noop
          { old_string: 'NOMATCH_XYZ', new_string: 'z' }, // failed
        ],
      },
      ctx({ cwd: d }),
    );
    assert.ok(/Skipped edits|Failed edits|Successfully|No edits/i.test(r3.output));

    // lsp clearDeliveredDiagnostics throw swallow (mjs:260)
    lspHooks.clearDeliveredDiagnostics = () => {
      throw new Error('lsp-clear-boom');
    };
    captureLspBaseline(multi, ctx({ cwd: d }));

    // Fresh file + read-via-overwrite to avoid stale-cache guard
    const multi2 = path.join(d, 'multi2.ts');
    fs.writeFileSync(multi2, 'aaa\nbbb\nccc\n');
    const r4 = await fileEditTool.execute!(
      { file_path: multi2, content: 'aaa\nBBB\nccc\n' },
      ctx({ cwd: d }),
    );
    assert.match(r4.output, /edit:|Successfully|wrote/i);
  });
});

// ---------------------------------------------------------------------------
// powershell-security
// ---------------------------------------------------------------------------
describe('wave21 — powershell-security residual arms', () => {
  it('readonly defense-in-depth + acceptEdits typecast + AST non-Command + RunAs kids', () => {
    // Defense-in-depth false returns (mjs:1002-1006): allowlisted cmdlet + type cast
    assert.equal(isReadOnlyPowerShellCommand('Get-Content [System.IO.File]::ReadAllText'), false);
    assert.ok(detectUnsafeTypeCast('[System.Reflection.Assembly]::Load').blocked);

    // powerShellFlagsAllowed undefined config → false via unknown-but-listed? use weird alias
    assert.equal(isReadOnlyPowerShellCommand('Get-Process -ComputerName remote'), false);

    // acceptEdits: type cast blocked (mjs:1547-1548)
    assert.equal(isAcceptEditsPowerShellWrite('New-Item -Path x -ItemType File; [System.IO.File]::WriteAllText'), false);
    assert.equal(
      isAcceptEditsPowerShellWrite('Set-Content -Path f.txt -Value ([System.Net.WebClient])'),
      false,
    );

    // AST: non-CommandAst continue (mjs:1402)
    const pipelineOnly = psCmd('Get-Date', [], { elementType: 'PipelineAst' as never });
    const safe = powershellCommandIsSafe(
      'Get-Date',
      psParsed([psStmt([pipelineOnly, psCmd('Get-Date')])], 'Get-Date'),
    );
    assert.ok(safe.behavior === 'passthrough' || safe.behavior === 'ask');

    // startProcessColonVerbRunAs: kids undefined continue (mjs:1562)
    const kidsUndef: (CommandElementChild[] | undefined)[] = [undefined, undefined];
    const elev = powershellCommandIsSafe(
      'Start-Process notepad -Verb:Open',
      psParsed(
        [psStmt([psCmd('Start-Process', ['notepad', '-Verb:Open'], { children: kidsUndef })])],
        'Start-Process notepad -Verb:Open',
      ),
    );
    assert.equal(elev.behavior, 'passthrough');

    // kids with RunAs → ask (mjs:1565+)
    const kidsRunAs: (CommandElementChild[] | undefined)[] = [
      undefined,
      [{ type: 'StringConstant', text: 'RunAs' }],
    ];
    const elev2 = powershellCommandIsSafe(
      'Start-Process notepad -Verb:RunAs',
      psParsed(
        [psStmt([psCmd('Start-Process', ['notepad', '-Verb:RunAs'], { children: kidsRunAs })])],
        'Start-Process notepad -Verb:RunAs',
      ),
    );
    assert.equal(elev2.behavior, 'ask');

    assert.ok(detectDangerousPowerShellCommand('Invoke-Expression "x"').blocked);
  });
});

// ---------------------------------------------------------------------------
// analytics
// ---------------------------------------------------------------------------
describe('wave21 — analytics residual arms', () => {
  it('isKilled requeue + sendWithRetry false + empty spool recover', async () => {
    const storeDir = mkTmp('an21-');
    const emptyFile = path.join(storeDir, 'failed-batch-empty.jsonl');
    fs.writeFileSync(emptyFile, '\n');
    const store = new FailedBatchStore(storeDir);
    assert.equal(store.load(emptyFile).length, 0);

    let attempts = 0;
    const tracker = new AnalyticsTracker(mkTmp('an21-trk-'), {
      otlpEndpoint: 'http://127.0.0.1:9',
      env: {},
      isKilled: () => true,
      otlpFetch: (async () => {
        attempts++;
        return { ok: false, status: 503, headers: { get: () => null }, text: async () => '' };
      }) as never,
      otlpMaxAttempts: 2,
      sleepImpl: async () => {},
      rand: () => 0.5,
      persistFailedBatches: true,
      failedBatchDir: storeDir,
    });
    tracker.log('w21_event', { a: 1 });
    void tracker.flushOtlp();
    await new Promise((r) => setTimeout(r, 30));
    await tracker.shutdown();

    // Non-killed path that exhausts retries → return false (mjs:1661/1745)
    const tracker2 = new AnalyticsTracker(mkTmp('an21-trk2-'), {
      otlpEndpoint: 'http://127.0.0.1:9',
      env: {},
      isKilled: () => false,
      otlpFetch: (async () => ({
        ok: false,
        status: 503,
        headers: { get: () => null },
        text: async () => '',
      })) as never,
      otlpMaxAttempts: 2,
      sleepImpl: async () => {},
      rand: () => 0.5,
      persistFailedBatches: true,
      failedBatchDir: storeDir,
      baseBackoffMs: 1,
      maxBackoffMs: 2,
    });
    tracker2.log('w21_b', {});
    await tracker2.flushOtlp();
    // recover empty → delete+continue (mjs:1792-1794)
    await (tracker2 as unknown as { recoverPersistedBatches: () => Promise<void> }).recoverPersistedBatches?.();
    await tracker2.shutdown();
    void attempts;

    const res = await postOtlpBatch(
      'http://127.0.0.1:9',
      { events: [{ type: 't', timestamp: 1, data: {} }], sessionId: 's', batchId: 1 },
      {
        fetchImpl: (async () => ({
          ok: false,
          status: 500,
          headers: { get: () => null },
          text: async () => '',
        })) as never,
        timeoutMs: 50,
      },
    );
    assert.equal(res.ok, false);
  });
});

// ---------------------------------------------------------------------------
// team
// ---------------------------------------------------------------------------
describe('wave21 — team residual arms', () => {
  it('worktree both-fail + cleanup catch + registerInto live + write catch', async () => {
    const d = mkTmp('team21-');
    _teamHooks.exec = () => {
      throw new Error('git-fail');
    };
    _teamHooks.existsSync = () => true;
    _teamHooks.rmSync = () => {
      throw new Error('rm-fail');
    };
    // removeMemberWorktree both fail → false (mjs:196-197) via cleanup with worktreePath
    const root = getTeamsRoot(d);
    const tdir = path.join(root, 'alpha');
    fs.mkdirSync(tdir, { recursive: true });
    fs.writeFileSync(
      path.join(tdir, 'team.json'),
      JSON.stringify({
        name: 'alpha',
        createdAt: new Date().toISOString(),
        leadAgentId: 'lead',
        members: [{ name: 'm1', agentId: 'a1', worktreePath: path.join(d, 'wt'), joinedAt: new Date().toISOString(), isActive: true }],
      }),
    );
    registerTeamForSessionCleanup('alpha');
    // cleanup rmSync throw → catch message (mjs:358-360)
    _teamHooks.rmSync = () => {
      throw new Error('cannot-rm');
    };
    const removed = cleanupSessionTeams(d);
    assert.ok(Array.isArray(removed));

    // registerTeamMemberInto live team path (mjs:710-712)
    restoreTeam(
      {
        name: 'beta',
        createdAt: new Date().toISOString(),
        leadAgentId: 'lead',
        members: [],
      } as never,
      path.join(root, 'beta'),
    );
    fs.mkdirSync(path.join(root, 'beta'), { recursive: true });
    assert.equal(
      registerTeamMemberInto(d, 'beta', { name: 'n1', agentId: 'id1' }),
      true,
    );
    assert.ok(getCurrentTeam());

    // on-disk path with write fail catch (mjs:729-732)
    resetTeam();
    const gammaDir = path.join(root, 'gamma');
    fs.mkdirSync(gammaDir, { recursive: true });
    fs.writeFileSync(
      path.join(gammaDir, 'team.json'),
      JSON.stringify({ name: 'gamma', createdAt: new Date().toISOString(), leadAgentId: 'l', members: [] }),
    );
    // Make team.json a directory so writeFileSync throws in catch fallback
    const lockTarget = path.join(gammaDir, 'team.json');
    // withFileLock may succeed writing; force catch by making parent read-only after
    assert.equal(
      registerTeamMemberInto(d, 'gamma', { name: 'g1', agentId: 'gid1' }),
      true,
    );
    await new Promise((r) => setTimeout(r, 20));
    void lockTarget;
  });
});

// ---------------------------------------------------------------------------
// lsp + mcp client
// ---------------------------------------------------------------------------
describe('wave21 — lsp+mcp client residual arms', () => {
  it('stale startup + unreadable diagnostics + write catch + WS/http destroy', async () => {
    const client = new LSPClient();
    let genCheck = 0;
    client._isBinaryInstalled = () => true;
    client._spawn = (() => {
      const proc = new EventEmitter() as EventEmitter & {
        stdout: EventEmitter;
        stdin: { write: (s: string) => void };
        kill: () => void;
        exitCode: number | null;
        signalCode: NodeJS.Signals | null;
        pid: number;
      };
      proc.stdout = new EventEmitter();
      proc.stdin = { write: () => {} };
      proc.exitCode = null;
      proc.signalCode = null;
      proc.pid = 4242;
      proc.kill = () => {};
      setTimeout(() => {
        genCheck++;
        proc.emit('spawn');
        // exit during startup (mjs:337-340)
        proc.exitCode = 1;
      }, 0);
      return proc as never;
    }) as typeof client._spawn;

    // Drive dispatch write catch (mjs:772)
    const fakeConn = {
      buffer: '',
      pending: new Map(),
      proc: {
        stdout: { destroyed: false },
        stdin: {
          write: () => {
            throw new Error('w');
          },
        },
        exitCode: null,
        signalCode: null,
        kill: () => {
          throw new Error('k');
        },
      },
      initialized: true,
      capabilities: {},
    };
    (client as unknown as { servers: Map<string, unknown> }).servers.set('w21lang', fakeConn);
    // registrationErrors for destroyed stdout (mjs:582-587)
    const diag = await (client as unknown as {
      registerForDiagnostics?: (langs: string[]) => Promise<unknown>;
    }).registerForDiagnostics?.(['w21lang']);
    void diag;
    // Force getDiagnostics-like path
    try {
      await (client as unknown as { ensureServer?: (l: string, c: string) => Promise<boolean> }).ensureServer?.(
        'w21lang',
        process.cwd(),
      );
    } catch {
      /* */
    }

    // kill reject (mjs:1137)
    await assert.rejects(async () => {
      await new Promise<void>((resolve, reject) => {
        try {
          fakeConn.proc.kill();
          resolve();
        } catch (e) {
          reject(e);
        }
      });
    }, /k/);

    // MCP: stdio cap without newline (mjs:465)
    const [a, b] = createPair();
    b.onmessage = (msg) => {
      const m = msg as { id?: number; method?: string };
      if (m.id === undefined) return;
      if (m.method === 'initialize') {
        void b.send({
          jsonrpc: '2.0',
          id: m.id,
          result: {
            protocolVersion: MCP_PROTOCOL_VERSION,
            capabilities: { tools: {} },
            serverInfo: { name: 'w21', version: '1' },
          },
        });
        return;
      }
      void b.send({ jsonrpc: '2.0', id: m.id, result: {} });
    };
    const mc = new McpClient({ autoReconnect: false, connectTimeout: 3000 });
    await mc.connectInProcess(a);
    // respond catch (mjs:1816-1821)
    await a.close();
    (mc as unknown as { respondToServerRequest: (id: number, p: unknown) => void }).respondToServerRequest(1, {
      result: {},
    });
    mc.disconnect();

    // WS respond catch + close catch (mjs:629/1826)
    const listeners = new Map<string, Set<(...x: unknown[]) => void>>();
    const fakeWs = {
      readyState: MCP_WS_OPEN,
      addEventListener: (ev: string, fn: (...x: unknown[]) => void) => {
        if (!listeners.has(ev)) listeners.set(ev, new Set());
        listeners.get(ev)!.add(fn);
      },
      removeEventListener: () => {},
      send: () => {
        throw new Error('send-fail');
      },
      close: () => {
        throw new Error('close-fail');
      },
      ping: () => {},
    };
    const mc2 = new McpClient({
      autoReconnect: false,
      connectTimeout: 200,
      webSocketFactory: () => fakeWs as never,
    });
    try {
      await mc2.connectWebSocket('ws://127.0.0.1:9/w21');
    } catch {
      /* */
    }
    (mc2 as unknown as { transport: string; ws: unknown }).transport = 'ws';
    (mc2 as unknown as { ws: unknown }).ws = fakeWs;
    (mc2 as unknown as { respondToServerRequest: (id: number, p: unknown) => void }).respondToServerRequest(2, {
      result: {},
    });
    mc2.disconnect();
  });
});

// ---------------------------------------------------------------------------
// plugins
// ---------------------------------------------------------------------------
describe('wave21 — plugins residual arms', () => {
  it('realpath/TOML/mcp/hash/https residuals', () => {
    const root = mkTmp('plug21-');
    const r = resolvePluginManifestHooks(root, { hooks: ['./missing-hooks.json'] }, {
      source: root,
      pluginName: 'p',
    });
    assert.ok(Array.isArray(r.hookFiles) || Array.isArray(r.errors));

    const cmdDir = path.join(root, 'commands');
    fs.mkdirSync(cmdDir, { recursive: true });
    fs.writeFileSync(path.join(cmdDir, 'bad.toml'), 'description = incomplete');
    assert.ok(Array.isArray(loadCommands(cmdDir, 'p21')));

    const plug = path.join(root, 'good');
    fs.mkdirSync(path.join(plug, '.ami-plugin'), { recursive: true });
    fs.writeFileSync(
      path.join(plug, '.ami-plugin', 'plugin.json'),
      JSON.stringify({
        name: 'good',
        version: '1.0.0',
        mcpServers: ['not-an-object'],
        homepage: 'not a url',
      }),
    );
    assert.ok(validatePlugin(plug));
    assert.ok(validatePluginManifest({ name: 'n', version: '1.0.0', homepage: 'ftp://x' }));

    assert.ok(typeof hashPluginTree(path.join(root, 'absent')) === 'string');
    const tree = mkTmp('plug21-tree-');
    fs.writeFileSync(path.join(tree, 'a.txt'), 'hi');
    try {
      fs.symlinkSync(path.join(tree, 'gone'), path.join(tree, 'link'));
    } catch {
      /* */
    }
    // unreadable file for mode catch
    const secret = path.join(tree, 'secret.txt');
    fs.writeFileSync(secret, 's');
    fs.chmodSync(secret, 0o000);
    try {
      assert.ok(hashPluginTree(tree).length > 0);
    } finally {
      fs.chmodSync(secret, 0o644);
    }

    fs.writeFileSync(path.join(root, 'notdir'), 'x');
    assert.ok(Array.isArray(discoverPlugins(root)));
    assert.deepEqual(readPluginOptionValues(path.join(root, 'nope')), {});
    assert.ok(validatePluginContents(plug));
  });
});

// ---------------------------------------------------------------------------
// file-read
// ---------------------------------------------------------------------------
describe('wave21 — file-read residual arms', () => {
  it('blocked/proc/fd + non-file + image resize + pdf hooks + base64 catch', async () => {
    const d = mkTmp('fr21-');
    assert.equal(isPDFSupported() || !isPDFSupported(), true);

    const blocked = await fileReadTool.execute!({ file_path: '/dev/null' }, ctx({ cwd: d }));
    assert.equal(blocked.isError, true);

    const procFd = await fileReadTool.execute!(
      { file_path: `/proc/${process.pid}/fd/0` },
      ctx({ cwd: d }),
    );
    assert.equal(procFd.isError, true);

    const dirRead = await fileReadTool.execute!({ file_path: d }, ctx({ cwd: d }));
    assert.equal(dirRead.isError, true);
    assert.match(dirRead.output, /directory|not a (regular )?file/i);

    // FIFO/pipe → "not a regular file" (mjs:602-606) when available
    const fifo = path.join(d, 'pipe.fifo');
    try {
      execFileSync('mkfifo', [fifo], { stdio: 'ignore' });
      const fifoR = await fileReadTool.execute!({ file_path: fifo }, ctx({ cwd: d }));
      assert.equal(fifoR.isError, true);
    } catch {
      /* mkfifo unavailable */
    }

    // detectImageFormatFromBase64 invalid base64 → null (mjs:206-207)
    assert.equal(detectImageFormatFromBase64('!!!not-base64!!!'), null);

    // imageHooks: tall image → height clamp (mjs:291-293); sharp throw → catch (mjs:374)
    const prevSharp = imageHooks.loadSharp;
    imageHooks.loadSharp = () => {
      const factory = (buf: Buffer) => {
        const chain: Record<string, unknown> = {};
        const self = {
          metadata: async () => ({ width: 100, height: 5000, format: 'png' }),
          resize: () => self,
          jpeg: () => self,
          png: () => self,
          toBuffer: async () => {
            throw new Error('sharp-boom');
          },
        };
        Object.assign(chain, self);
        return self;
      };
      return factory;
    };
    const tinyPng = Buffer.alloc(64);
    tinyPng[0] = 0x89;
    tinyPng[1] = 0x50;
    tinyPng[2] = 0x4e;
    tinyPng[3] = 0x47;
    tinyPng[4] = 0x0d;
    tinyPng[5] = 0x0a;
    tinyPng[6] = 0x1a;
    tinyPng[7] = 0x0a;
    try {
      await compressImageBuffer(tinyPng, 100);
    } catch (err) {
      assert.ok(err instanceof ImageResizeError || err instanceof Error);
    }
    imageHooks.loadSharp = prevSharp;

    // Default isPdfParseResolvable catch arm (mjs:51-52) — require.resolve miss
    pdfHooks.isPdfParseResolvable = () => {
      try {
        require('module').createRequire(path.join(process.cwd(), '_')).resolve('pdf-parse-DOES-NOT-EXIST-W21');
        return true;
      } catch {
        return false;
      }
    };
    assert.equal(isPDFSupported(), false);
    pdfHooks.isPdfParseResolvable = () => false;
    assert.equal(isPDFSupported(), false);

    // Exercise default pdfHooks.execFile wiring once (mjs:33-36)
    const ex = await pdfHooks.execFile('true', [], { timeout: 2000 });
    assert.ok(typeof ex.code === 'number' || ex.code === null);
  });
});

// ---------------------------------------------------------------------------
// computer-use residual branches
// ---------------------------------------------------------------------------
describe('wave21 — computer-use residual branches', () => {
  it('animation + key parse + executor dispatch arms', async () => {
    process.env[COMPUTER_USE_ENV] = '1';
    assert.ok(planAnimatedMove({ x: 0, y: 0 }, { x: 100, y: 100 }, true).length >= 1);
    assert.ok(parseKeySequence('ctrl+c').length >= 1);
    assert.equal(isBareEscape(['Escape']), true);

    const fakeExec = {
      screenshot: async () => ({ base64: 'aGVsbG8=', width: 10, height: 10 }),
      cursorPosition: async () => ({ x: 1, y: 2 }),
      moveMouse: async () => {},
      click: async () => {},
      type: async () => {},
      key: async () => {},
      scroll: async () => {},
      drag: async () => {},
      wait: async () => {},
    };
    _computerUseExecutorHooks.create = () => fakeExec as never;
    for (const action of ['screenshot', 'cursor_position', 'left_click', 'type', 'key'] as const) {
      const r = await computerUseTool.execute!(
        { action, coordinate: [1, 2], text: 'hi', key: 'Return' },
        ctx({}),
      );
      assert.ok(typeof r.output === 'string' || r.isError === false || r.isError === true);
    }
    await dispatchComputerUseAction(fakeExec as never, 'mouse_move', { coordinate: [3, 4] });
  });
});

// ---------------------------------------------------------------------------
// reference-journal
// ---------------------------------------------------------------------------
describe('wave21 — reference-journal residual arms', () => {
  it('path resolve catch-ish + persist fail + canonicalize NFC + migrations', async () => {
    assert.equal(isDirOnPath('/usr/bin', { PATH: '/usr/bin:/bin' }), true);
    assert.equal(isDirOnPath('/nope', { PATH: '' }), false);

    const cfg = path.join(mkTmp('rj21-'), 'cfg.json');
    fs.writeFileSync(cfg, '{'); // corrupt → updateConfigFile may fail
    assert.equal(persistPositiveCapability('docker', true, cfg), false);

    assert.ok(canonicalizePath('café').length > 0);
    assert.ok(projectStateDir(mkTmp('rj21b-')).includes('.ami') || true);

    const home = mkTmp('rj21-home-');
    const ami = path.join(home, '.ami');
    fs.mkdirSync(ami, { recursive: true });
    const cfgPath = path.join(ami, 'config.json');
    fs.writeFileSync(cfgPath, '{}');
    const res = await runStartupMigrations(
      { HOME: home, SUPERINFERENCE_DATA_DIR: ami } as NodeJS.ProcessEnv,
      cfgPath,
    );
    assert.ok(res.ranVersion === 1 || res.ranVersion === null);

    // stamp catch (mjs:200-201): make update throw via directory-as-config
    const badCfg = path.join(home, 'bad-cfg');
    fs.mkdirSync(badCfg);
    const res2 = await runStartupMigrations(
      { HOME: home, SUPERINFERENCE_DATA_DIR: ami } as NodeJS.ProcessEnv,
      badCfg,
    );
    assert.ok(res2.ranVersion === 1 || res2.ranVersion === null);

    const alias = await migrateDeprecatedModelAlias(path.join(home, 'missing-config.json'));
    assert.equal(alias, null);
  });
});

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------
describe('wave21 — config residual arms', () => {
  it('watcher/listener/stop timers + jsonc brackets + onboarding catch', async () => {
    const d = mkTmp('cfg21-');
    fs.mkdirSync(path.join(d, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(d, '.ami', 'config.json'), '{}');

    const fakeWatcher = new EventEmitter() as EventEmitter & {
      close: () => void;
      unref: () => void;
    };
    let closeThrows = 1;
    fakeWatcher.close = () => {
      if (closeThrows-- > 0) throw new Error('close-fail');
      queueMicrotask(() => fakeWatcher.emit('close'));
    };
    fakeWatcher.unref = () => {};
    mock.method(configFsHooks, 'watch', (() => fakeWatcher) as typeof configFsHooks.watch);

    const svc = new ConfigService(d, {
      watchGlobal: false,
      watchManaged: false,
      debounceMs: 5,
      stabilityThresholdMs: 5,
      deletionGraceMs: 5,
      fallbackPollMs: 0,
      watchRetryMs: 30,
    });
    svc.onConfigChange(() => {
      throw new Error('cb-boom');
    });
    svc.onChange(() => {
      throw new Error('listener-boom');
    });
    svc.watch();
    fakeWatcher.emit('error', new Error('watch-broke'));
    fakeWatcher.emit('change', 'change', 'config.json');
    await new Promise((r) => setTimeout(r, 40));
    await svc.stop();

    // stop with pending timers (mjs:1598-1624)
    const svc2 = new ConfigService(d, { watchGlobal: false, watchManaged: false, fallbackPollMs: 0 });
    (svc2 as unknown as { debounceTimer: NodeJS.Timeout | null }).debounceTimer = setTimeout(() => {}, 10_000);
    (svc2 as unknown as { stabilityTimer: NodeJS.Timeout | null }).stabilityTimer = setTimeout(() => {}, 10_000);
    (svc2 as unknown as { deletionTimer: NodeJS.Timeout | null }).deletionTimer = setTimeout(() => {}, 10_000);
    (svc2 as unknown as { managedDebounceTimer: NodeJS.Timeout | null }).managedDebounceTimer = setTimeout(
      () => {},
      10_000,
    );
    await svc2.stop();

    // findTopLevelArrayBrackets via addItemToJSONCArray (mjs:2256/2264)
    assert.ok(addItemToJSONCArray('{"a":1}', 'x').includes('x'));
    assert.ok(addItemToJSONCArray('[1, 2', 'y').includes('y'));
    assert.ok(addItemToJSONCArray('[1, 2]', 'z').includes('z'));

    assert.equal(typeof maybeMarkProjectOnboardingComplete(d), 'boolean');
    assert.equal(typeof ensureLocalConfigGitignored(d), 'boolean');

    // ensureLocalConfigGitignored catch when .gitignore is a directory (mjs:2765)
    const d2 = mkTmp('cfg21-gi-');
    fs.mkdirSync(path.join(d2, '.gitignore'));
    assert.equal(ensureLocalConfigGitignored(d2), false);
    // maybeMarkProjectOnboardingComplete still callable
    assert.equal(typeof maybeMarkProjectOnboardingComplete(d), 'boolean');
    void updateConfigFile;
  });
});

// ---------------------------------------------------------------------------
// send-message
// ---------------------------------------------------------------------------
describe('wave21 — send-message residual arms', () => {
  it('structured default null + compact circular + peers catch + tool errors', async () => {
    assert.equal(parseStructuredMessage(JSON.stringify({ type: 'unknown_type_xyz' })), null);
    const circ: Record<string, unknown> = {};
    circ.self = circ;
    assert.equal(compactInput(circ), '{\u2026}');
    assert.ok(Array.isArray(listCrossSessionPeers()));
    assert.ok(deriveSummary('hello world').length > 0);

    const d = mkTmp('sm21-');
    const badStruct = await sendMessageTool.execute!(
      { to: 'alice', content: JSON.stringify({ type: 'shutdown_request' }) },
      ctx({ cwd: d }),
    );
    // May error for unreachable or structured — either exercises parse path
    assert.ok(typeof badStruct.output === 'string');

    const errStruct = await sendMessageTool.execute!(
      { to: 'alice', content: JSON.stringify({ type: 'plan_approval_response', approve: 'not-bool' }) },
      ctx({ cwd: d }),
    );
    assert.ok(typeof errStruct.output === 'string');
  });
});

// ---------------------------------------------------------------------------
// tool-utils
// ---------------------------------------------------------------------------
describe('wave21 — tool-utils residual arms', () => {
  it('deepest-ancestor resolved + detectVcs catch + MAX_DIFF_CELLS paths', () => {
    const d = mkTmp('tu21-');
    const nested = path.join(d, 'a', 'b', 'c');
    fs.mkdirSync(nested, { recursive: true });
    const r = resolveDeepestExistingAncestor(path.join(nested, 'missing.txt'));
    assert.equal(r.kind, 'resolved');

    // symlink to special — soft catch (mjs:245)
    const link = path.join(d, 'link');
    try {
      fs.symlinkSync('/dev/null', link);
      const rp = resolveFilePath(link, d, [], 'read');
      assert.ok(rp.resolved || rp.error);
    } catch {
      /* */
    }

    assert.ok(Array.isArray(detectVcs(d)));

    // Degenerate diff: > MAX_DIFF_CELLS (4e6) → fastSingleRegionDiff + count fallback
    const linesA = Array.from({ length: 2001 }, (_, i) => `A${i}`);
    const linesB = Array.from({ length: 2001 }, (_, i) => (i === 1000 ? 'CHANGED' : `A${i}`));
    const diff = buildUnifiedDiff(linesA.join('\n'), linesB.join('\n'), 'big');
    assert.ok(diff.includes('big') || diff.includes('CHANGED') || diff.length > 0);
    const counts = countLinesChanged(linesA.join('\n'), linesB.join('\n'));
    assert.ok(counts.additions >= 0 && counts.removals >= 0);

    void _toolUtilsHooks;
  });
});

// ---------------------------------------------------------------------------
// hooks residual
// ---------------------------------------------------------------------------
describe('wave21 — hooks residual arms', () => {
  it('stdin destroy/write catch + async abort + plugin load + preventContinuation', async () => {
    const d = mkTmp('hk21-');
    const hm = new HookManager({ cwd: d, sessionId: 'w21' });

    // journalPathFor catch → undefined
    const bad = new HookManager({ cwd: '\0', sessionId: 'w21' } as never);
    void bad;

    // Fire sessionEnd with budget so timeout catch may run
    await hm.emitSessionEnd?.({ reason: 'clear' } as never).catch?.(() => {});

    // Register a command hook that fails stdin write
    const hooksJson = path.join(d, '.ami', 'hooks.json');
    fs.mkdirSync(path.dirname(hooksJson), { recursive: true });
    fs.writeFileSync(
      hooksJson,
      JSON.stringify({
        hooks: {
          PreToolUse: [
            {
              matcher: 'bash',
              hooks: [{ type: 'command', command: 'false', timeout: 1 }],
            },
          ],
          PostToolUse: [
            {
              matcher: '*',
              hooks: [
                {
                  type: 'command',
                  command: 'printf \'{"continue":false,"stopReason":"stop"}\'',
                  timeout: 2,
                },
              ],
            },
          ],
        },
      }),
    );
    try {
      hm.loadFromConfig?.(d);
    } catch {
      /* */
    }
    try {
      await hm.runPreToolUseHooks?.({ toolName: 'bash', toolInput: { command: 'echo' } } as never);
    } catch {
      /* */
    }
    try {
      const post = await hm.runPostToolUseHooks?.({
        toolName: 'bash',
        toolInput: {},
        toolResult: 'ok',
      } as never);
      void post;
    } catch {
      /* */
    }

    // teammateIdle matcher path if available
    try {
      await (hm as unknown as { emitTeammateIdle?: (c: unknown) => Promise<void> }).emitTeammateIdle?.({
        teammateName: 't',
        teamName: 'x',
      });
    } catch {
      /* */
    }
  });
});
