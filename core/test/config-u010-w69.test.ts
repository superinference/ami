import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as childProcess from 'child_process';
import {
  getManagedConfigPath,
  getManagedDropInDir,
  mergeConfigs,
  type ProjectConfig,
} from '../src/config';
import { resolveCommitAttribution } from '../src/tools/git-commit';
import { gitCommitTool } from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';

/** Run `fn` with `process.platform` temporarily forced to `platform`. */
function withPlatform(platform: NodeJS.Platform, fn: () => void): void {
  const orig = Object.getOwnPropertyDescriptor(process, 'platform')!;
  Object.defineProperty(process, 'platform', { value: platform, configurable: true });
  try {
    fn();
  } finally {
    Object.defineProperty(process, 'platform', orig);
  }
}

// ---------------------------------------------------------------------------
// U010 — platform-specific managed-settings path (getManagedConfigPath)
// ---------------------------------------------------------------------------

describe('getManagedConfigPath — platform-specific default', () => {
  let prevEnv: string | undefined;
  let prevProgramData: string | undefined;
  before(() => {
    prevEnv = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    prevProgramData = process.env.PROGRAMDATA;
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
  });
  after(() => {
    if (prevEnv === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevEnv;
    if (prevProgramData === undefined) delete process.env.PROGRAMDATA;
    else process.env.PROGRAMDATA = prevProgramData;
  });

  it('uses /etc/superinference on Linux and other platforms', () => {
    withPlatform('linux', () => {
      assert.equal(getManagedConfigPath(), '/etc/superinference/config.json');
    });
    withPlatform('freebsd', () => {
      assert.equal(getManagedConfigPath(), '/etc/superinference/config.json');
    });
  });

  it('uses the macOS Application Support path on darwin', () => {
    withPlatform('darwin', () => {
      assert.equal(
        getManagedConfigPath(),
        '/Library/Application Support/SuperInference/config.json',
      );
    });
  });

  it('uses %PROGRAMDATA%\\SuperInference on Windows', () => {
    process.env.PROGRAMDATA = 'D:\\PData';
    withPlatform('win32', () => {
      assert.equal(
        getManagedConfigPath(),
        path.join('D:\\PData', 'SuperInference', 'config.json'),
      );
    });
  });

  it('falls back to C:\\ProgramData on Windows when PROGRAMDATA is unset', () => {
    delete process.env.PROGRAMDATA;
    withPlatform('win32', () => {
      assert.equal(
        getManagedConfigPath(),
        path.join('C:\\ProgramData', 'SuperInference', 'config.json'),
      );
    });
  });

  it('SUPERINFERENCE_MANAGED_CONFIG override wins on every platform', () => {
    process.env.SUPERINFERENCE_MANAGED_CONFIG = '/custom/managed.json';
    try {
      for (const p of ['linux', 'darwin', 'win32'] as NodeJS.Platform[]) {
        withPlatform(p, () => {
          assert.equal(getManagedConfigPath(), '/custom/managed.json');
        });
      }
    } finally {
      delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    }
  });

  it('drop-in dir tracks the platform-specific managed base', () => {
    delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    withPlatform('darwin', () => {
      assert.equal(
        getManagedDropInDir(),
        path.join('/Library/Application Support/SuperInference', 'config.d'),
      );
    });
  });
});

// ---------------------------------------------------------------------------
// U010 — managed tier authoritative for security-relevant scalar settings
// ---------------------------------------------------------------------------

describe('mergeConfigs — managed tier authoritative for TIER_LOCKED scalars', () => {
  it('managed permissionMode wins over cli/env/project/local/global', () => {
    const merged = mergeConfigs(
      { permissionMode: 'auto-allow' }, // cli (would normally win)
      { permissionMode: 'auto-allow' }, // env
      { permissionMode: 'ask' },        // project
      { permissionMode: 'ask' },        // global
      { permissionMode: 'ask' },        // local
      { permissionMode: 'deny-all' },   // managed — authoritative
    );
    assert.equal(merged.permissionMode, 'deny-all');
  });

  it('managed maxBudgetUsd caps a higher cli value', () => {
    const merged = mergeConfigs(
      { maxBudgetUsd: 1000 },
      {},
      { maxBudgetUsd: 500 },
      null,
      null,
      { maxBudgetUsd: 5 },
    );
    assert.equal(merged.maxBudgetUsd, 5);
  });

  it('managed maxTurns overrides a lower-tier value', () => {
    const merged = mergeConfigs(
      { maxTurns: 999 },
      {},
      { maxTurns: 50 },
      null,
      null,
      { maxTurns: 10 },
    );
    assert.equal(merged.maxTurns, 10);
  });

  it('managed safeMode:true cannot be relaxed by a lower tier', () => {
    const merged = mergeConfigs(
      { safeMode: false }, // cli tries to disable
      {},
      { safeMode: false },
      null,
      null,
      { safeMode: true }, // managed enforces
    );
    assert.equal(merged.safeMode, true);
  });

  it('does not override when the managed layer omits the key (cli still wins)', () => {
    const merged = mergeConfigs(
      { permissionMode: 'auto-allow', maxBudgetUsd: 2 },
      {},
      { permissionMode: 'ask', maxBudgetUsd: 10 },
      null,
      null,
      { enforceAvailableModels: ['x'] }, // managed sets an unrelated key
    );
    assert.equal(merged.permissionMode, 'auto-allow', 'cli still wins without a managed value');
    assert.equal(merged.maxBudgetUsd, 2);
  });

  it('no managed layer leaves normal last-layer-wins precedence intact', () => {
    const merged = mergeConfigs(
      { permissionMode: 'auto-allow' },
      {},
      { permissionMode: 'ask' },
      null,
    );
    assert.equal(merged.permissionMode, 'auto-allow');
  });
});

// ---------------------------------------------------------------------------
// U010 — attribution.commit / attribution.pr config knob
// ---------------------------------------------------------------------------

describe('mergeConfigs — attribution merge', () => {
  it('merges attribution across layers (higher layer wins per sub-key)', () => {
    const merged = mergeConfigs(
      {},
      {},
      { attribution: { commit: 'Project trailer', pr: 'Project PR' } },
      { attribution: { commit: 'Global trailer' } },
    );
    // project (higher) overrides global commit; project pr retained.
    assert.equal(merged.attribution?.commit, 'Project trailer');
    assert.equal(merged.attribution?.pr, 'Project PR');
  });

  it('is absent when no layer sets it', () => {
    const merged = mergeConfigs({}, {}, null, null);
    assert.equal(merged.attribution, undefined);
  });
});

describe('resolveCommitAttribution', () => {
  let tmpDir: string;
  let prevDisable: string | undefined;
  let prevManaged: string | undefined;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-attr-'));
    fs.mkdirSync(path.join(tmpDir, '.ami'), { recursive: true });
    prevDisable = process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    prevManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    // Point the managed path at a non-existent file so the host's /etc policy
    // (if any) never leaks into these assertions.
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(tmpDir, 'no-managed.json');
    delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
  });
  after(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
    if (prevDisable === undefined) delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    else process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = prevDisable;
    if (prevManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevManaged;
  });

  function writeProjectConfig(cfg: ProjectConfig): void {
    fs.writeFileSync(path.join(tmpDir, '.ami', 'config.json'), JSON.stringify(cfg));
  }

  it('returns the default AMI trailer when no attribution is configured', () => {
    writeProjectConfig({});
    assert.equal(
      resolveCommitAttribution(tmpDir),
      'Co-Authored-By: AMI <ami@superinference.org>',
    );
  });

  it('returns a custom trailer from attribution.commit', () => {
    writeProjectConfig({ attribution: { commit: 'Signed-off-by: Jane <jane@acme.test>' } });
    assert.equal(
      resolveCommitAttribution(tmpDir),
      'Signed-off-by: Jane <jane@acme.test>',
    );
  });

  it('suppresses the trailer when attribution.commit is an empty string', () => {
    writeProjectConfig({ attribution: { commit: '' } });
    assert.equal(resolveCommitAttribution(tmpDir), null);
  });

  it('suppresses the trailer when SUPERINFERENCE_DISABLE_CO_AUTHOR=1', () => {
    // No config attribution.commit here: an explicit config attribution takes
    // PRECEDENCE over this deprecated binary env opt-out (parity with Claude Code,
    // whose `attribution` setting overrides the legacy toggle). The env opt-out is
    // exercised on its own so it genuinely suppresses the trailer.
    process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = '1';
    try {
      assert.equal(resolveCommitAttribution(tmpDir), null);
    } finally {
      delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    }
  });

  it('config attribution.commit takes precedence over the deprecated env opt-out', () => {
    writeProjectConfig({ attribution: { commit: 'Co-Authored-By: Custom <x@y.z>' } });
    process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR = '1';
    try {
      assert.equal(resolveCommitAttribution(tmpDir), 'Co-Authored-By: Custom <x@y.z>');
    } finally {
      delete process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR;
    }
  });
});

// ---------------------------------------------------------------------------
// E2E — a real git_commit through the tool honors attribution.commit
// ---------------------------------------------------------------------------

describe('git_commit E2E — attribution.commit lands in the real commit', () => {
  let repo: string;
  let prevManaged: string | undefined;

  function git(args: string[]): string {
    return childProcess.execFileSync('git', args, { cwd: repo, encoding: 'utf-8' }).trim();
  }

  before(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-attr-e2e-'));
    prevManaged = process.env.SUPERINFERENCE_MANAGED_CONFIG;
    process.env.SUPERINFERENCE_MANAGED_CONFIG = path.join(repo, 'no-managed.json');
    git(['init', '-q']);
    git(['config', 'user.email', 'test@test.test']);
    git(['config', 'user.name', 'Test']);
    git(['config', 'commit.gpgsign', 'false']);
    fs.mkdirSync(path.join(repo, '.ami'), { recursive: true });
  });
  after(() => {
    fs.rmSync(repo, { recursive: true, force: true });
    if (prevManaged === undefined) delete process.env.SUPERINFERENCE_MANAGED_CONFIG;
    else process.env.SUPERINFERENCE_MANAGED_CONFIG = prevManaged;
  });

  function ctx(): ToolContext {
    return { cwd: repo, abortSignal: new AbortController().signal };
  }

  it('uses a custom attribution.commit trailer instead of the default AMI trailer', async () => {
    fs.writeFileSync(
      path.join(repo, '.ami', 'config.json'),
      JSON.stringify({ attribution: { commit: 'Signed-off-by: Policy Bot <bot@acme.test>' } }),
    );
    fs.writeFileSync(path.join(repo, 'a.txt'), 'hello');
    const res = await gitCommitTool.execute(
      { message: 'feat: add a.txt', files: ['a.txt'] },
      ctx(),
    );
    assert.equal(res.isError, false, res.output);
    const body = git(['log', '-1', '--pretty=%B']);
    assert.ok(
      body.includes('Signed-off-by: Policy Bot <bot@acme.test>'),
      `expected custom trailer, got:\n${body}`,
    );
    assert.ok(
      !body.includes('Co-Authored-By: AMI'),
      `default AMI trailer must be replaced, got:\n${body}`,
    );
  });

  it('suppresses the trailer entirely when attribution.commit is empty', async () => {
    fs.writeFileSync(
      path.join(repo, '.ami', 'config.json'),
      JSON.stringify({ attribution: { commit: '' } }),
    );
    fs.writeFileSync(path.join(repo, 'b.txt'), 'world');
    const res = await gitCommitTool.execute(
      { message: 'feat: add b.txt', files: ['b.txt'] },
      ctx(),
    );
    assert.equal(res.isError, false, res.output);
    const body = git(['log', '-1', '--pretty=%B']);
    assert.ok(!body.includes('Co-Authored-By'), `expected no trailer, got:\n${body}`);
    assert.ok(!body.includes('Signed-off-by'), `expected no trailer, got:\n${body}`);
  });
});
