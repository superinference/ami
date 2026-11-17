import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { MemoryManager, isMemorySourceEnabled, type MemorySource } from '../src/memory';
import * as logger from '../src/logger';

// ---------------------------------------------------------------------------
// U016 (w76): per-tier instruction-source enable toggles (parity: Claude Code's
// per-source isSettingSourceEnabled) + errno-aware instruction-file read error
// handling (ENOENT/ENOTDIR silent, EACCES/other surfaced).
// ---------------------------------------------------------------------------

const DISABLE_ENVS: Record<MemorySource, string> = {
  user: 'AMI_DISABLE_USER_MEMORY',
  project: 'AMI_DISABLE_PROJECT_MEMORY',
  local: 'AMI_DISABLE_LOCAL_MEMORY',
};

let cwdDir: string;
let homeDir: string;
const savedEnv: Record<string, string | undefined> = {};
const savedHome = process.env.HOME;
const savedMemDir = process.env.AMI_MEMORY_DIR;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

beforeEach(() => {
  cwdDir = fs.realpathSync(mkTmp('si-srctoggle-cwd-'));
  homeDir = fs.realpathSync(mkTmp('si-srctoggle-home-'));
  // Make cwd a git root so the parent-walk stops here and never discovers the
  // real repo's instruction files above the temp dir.
  fs.mkdirSync(path.join(cwdDir, '.git'), { recursive: true });
  // Redirect HOME so loadUserGlobalInstructions reads only test fixtures.
  savedEnv.HOME = process.env.HOME;
  process.env.HOME = homeDir;
  // Isolate the auto-memory store.
  process.env.AMI_MEMORY_DIR = mkTmp('si-srctoggle-store-');
  for (const k of Object.values(DISABLE_ENVS)) {
    savedEnv[k] = process.env[k];
    delete process.env[k];
  }
});

afterEach(() => {
  mock.restoreAll();
  process.env.HOME = savedHome;
  process.env.AMI_MEMORY_DIR = savedMemDir;
  for (const k of Object.values(DISABLE_ENVS)) {
    if (savedEnv[k] === undefined) delete process.env[k];
    else process.env[k] = savedEnv[k];
  }
  for (const d of [cwdDir, homeDir]) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

function writeInstruction(dir: string, name: string, body: string): void {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, name), body, 'utf-8');
}

describe('isMemorySourceEnabled (per-tier gate)', () => {
  const sources: MemorySource[] = ['user', 'project', 'local'];

  for (const source of sources) {
    const envVar = DISABLE_ENVS[source];

    it(`${source}: truthy ${envVar} forces OFF`, () => {
      assert.equal(isMemorySourceEnabled(source, { [envVar]: '1' }), false);
      assert.equal(isMemorySourceEnabled(source, { [envVar]: 'true' }), false);
    });

    it(`${source}: explicitly-falsy ${envVar} forces ON (overrides settings opt-out)`, () => {
      assert.equal(isMemorySourceEnabled(source, { [envVar]: '0' }, false), true);
      assert.equal(isMemorySourceEnabled(source, { [envVar]: 'off' }, false), true);
    });

    it(`${source}: unset env defers to settingsEnabled`, () => {
      assert.equal(isMemorySourceEnabled(source, {}, false), false);
      assert.equal(isMemorySourceEnabled(source, {}, true), true);
    });

    it(`${source}: unset env + no setting defaults ON`, () => {
      assert.equal(isMemorySourceEnabled(source, {}), true);
      assert.equal(isMemorySourceEnabled(source, { [envVar]: '   ' }), true);
    });
  }

  it('each source reads its OWN env var, not another tier\'s', () => {
    assert.equal(isMemorySourceEnabled('project', { AMI_DISABLE_USER_MEMORY: '1' }), true);
    assert.equal(isMemorySourceEnabled('user', { AMI_DISABLE_PROJECT_MEMORY: '1' }), true);
    assert.equal(isMemorySourceEnabled('local', { AMI_DISABLE_PROJECT_MEMORY: '1' }), true);
  });
});

describe('loadProjectInstructions honors per-tier toggles', () => {
  beforeEach(() => {
    writeInstruction(cwdDir, 'CLAUDE.md', '# Project rules\nalpha-project-marker\n');
    writeInstruction(cwdDir, 'CLAUDE.local.md', '# Local overrides\nbeta-local-marker\n');
  });

  it('loads both project + local by default', () => {
    const out = new MemoryManager(cwdDir).loadProjectInstructions();
    assert.match(out, /alpha-project-marker/);
    assert.match(out, /beta-local-marker/);
  });

  it('AMI_DISABLE_PROJECT_MEMORY drops the project file, keeps local', () => {
    process.env.AMI_DISABLE_PROJECT_MEMORY = '1';
    const out = new MemoryManager(cwdDir).loadProjectInstructions();
    assert.doesNotMatch(out, /alpha-project-marker/);
    assert.match(out, /beta-local-marker/);
  });

  it('AMI_DISABLE_LOCAL_MEMORY drops local variants, keeps project', () => {
    process.env.AMI_DISABLE_LOCAL_MEMORY = '1';
    const out = new MemoryManager(cwdDir).loadProjectInstructions();
    assert.match(out, /alpha-project-marker/);
    assert.doesNotMatch(out, /beta-local-marker/);
  });

  it('disabling both yields empty project instructions', () => {
    process.env.AMI_DISABLE_PROJECT_MEMORY = '1';
    process.env.AMI_DISABLE_LOCAL_MEMORY = '1';
    assert.equal(new MemoryManager(cwdDir).loadProjectInstructions(), '');
  });
});

describe('loadUserGlobalInstructions honors the user toggle', () => {
  beforeEach(() => {
    writeInstruction(path.join(homeDir, '.ami'), 'CLAUDE.md', 'gamma-user-marker\n');
  });

  it('loads user-global by default', () => {
    assert.match(new MemoryManager(cwdDir).loadUserGlobalInstructions(), /gamma-user-marker/);
  });

  it('AMI_DISABLE_USER_MEMORY drops user-global entirely', () => {
    process.env.AMI_DISABLE_USER_MEMORY = '1';
    assert.equal(new MemoryManager(cwdDir).loadUserGlobalInstructions(), '');
  });
});

describe('collectInstructionSources honors per-tier toggles', () => {
  beforeEach(() => {
    writeInstruction(cwdDir, 'CLAUDE.md', 'proj-src-marker\n');
    writeInstruction(cwdDir, 'CLAUDE.local.md', 'local-src-marker\n');
    writeInstruction(path.join(homeDir, '.ami'), 'CLAUDE.md', 'user-src-marker\n');
  });

  it('includes project/local/user levels by default', async () => {
    const levels = (await new MemoryManager(cwdDir).collectInstructionSources()).map(s => s.level);
    assert.ok(levels.includes('project'));
    assert.ok(levels.includes('local'));
    assert.ok(levels.includes('user'));
  });

  it('drops the gated tiers when each toggle is set', async () => {
    process.env.AMI_DISABLE_PROJECT_MEMORY = '1';
    process.env.AMI_DISABLE_LOCAL_MEMORY = '1';
    process.env.AMI_DISABLE_USER_MEMORY = '1';
    const levels = (await new MemoryManager(cwdDir).collectInstructionSources()).map(s => s.level);
    assert.ok(!levels.includes('project'));
    assert.ok(!levels.includes('local'));
    assert.ok(!levels.includes('user'));
  });
});

describe('getMemoryContext reflects the project toggle', () => {
  it('omits the Project Instructions section when project is disabled', () => {
    writeInstruction(cwdDir, 'CLAUDE.md', 'ctx-project-marker\n');
    const withProj = new MemoryManager(cwdDir).getMemoryContext();
    assert.match(withProj, /ctx-project-marker/);
    process.env.AMI_DISABLE_PROJECT_MEMORY = '1';
    const noProj = new MemoryManager(cwdDir).getMemoryContext();
    assert.doesNotMatch(noProj, /ctx-project-marker/);
  });
});

describe('errno-aware instruction-file read error handling', () => {
  // Capture emitted log lines via the logger's mockable fsHooks sink (the logger
  // namespace itself is a frozen ESM import and must not be monkeypatched).
  let logLines: string[];
  const savedLogLevel = process.env.AMI_DEBUG_LOG_LEVEL;
  const savedDisableLog = process.env.AMI_DISABLE_DEBUG_LOG;
  const savedToStderr = process.env.AMI_DEBUG_TO_STDERR;

  beforeEach(() => {
    logLines = [];
    process.env.AMI_DEBUG_LOG_LEVEL = 'debug'; // ensure warn passes the verbosity gate
    delete process.env.AMI_DISABLE_DEBUG_LOG;
    delete process.env.AMI_DEBUG_TO_STDERR;
    mock.method(logger.fsHooks, 'appendFileSync', (_f: unknown, line: unknown) => {
      logLines.push(String(line));
    });
  });

  afterEach(() => {
    if (savedLogLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL;
    else process.env.AMI_DEBUG_LOG_LEVEL = savedLogLevel;
    if (savedDisableLog === undefined) delete process.env.AMI_DISABLE_DEBUG_LOG;
    else process.env.AMI_DISABLE_DEBUG_LOG = savedDisableLog;
    if (savedToStderr === undefined) delete process.env.AMI_DEBUG_TO_STDERR;
    else process.env.AMI_DEBUG_TO_STDERR = savedToStderr;
  });

  const runUnreadable = process.getuid?.() !== 0; // root bypasses mode bits
  it('surfaces a non-ENOENT read failure via the memory logger, not a silent skip',
    { skip: runUnreadable ? false : 'needs non-root to enforce 0o000 (EACCES)' }, () => {
    const claudeMd = path.join(cwdDir, 'CLAUDE.md');
    writeInstruction(cwdDir, 'CLAUDE.md', 'should-not-load\n');
    // statSync still reports isFile() (stat needs dir-exec, not file-read), so the
    // file IS discovered, then readFileSync fails with a genuine EACCES.
    fs.chmodSync(claudeMd, 0o000);
    try {
      // Must not throw, and must not include the unreadable file's content.
      const out = new MemoryManager(cwdDir).loadProjectInstructions();
      assert.doesNotMatch(out, /should-not-load/);

      const warn = logLines.find(l => /\[memory\]/.test(l) && /read failed/.test(l));
      assert.ok(warn, 'expected a memory read-failure diagnostic to be logged');
      assert.match(warn!, /EACCES/);
      assert.match(warn!, /\[WARN\]/);
    } finally {
      fs.chmodSync(claudeMd, 0o600); // allow afterEach cleanup
    }
  });

  it('stays silent for an ordinary absent file (ENOENT)', () => {
    // No instruction files present at all.
    const out = new MemoryManager(cwdDir).loadProjectInstructions();
    assert.equal(out, '');
    const readFail = logLines.find(l => /\[memory\]/.test(l) && /read failed/.test(l));
    assert.equal(readFail, undefined, 'absent files must not log a read failure');
  });
});
