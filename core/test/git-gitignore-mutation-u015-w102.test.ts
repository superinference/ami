/**
 * U015 (w102) — getGlobalGitignorePath + addFileGlobRuleToGitignore.
 *
 * Parity with the reference's `utils/git/gitignore.ts` (L43-99): a SYNC global-ignore
 * mutator that keeps a locally-generated, never-shared file out of version control.
 * Branches mirrored from the reference:
 *   1. no-op outside a git repo;
 *   2. directory-pattern (trailing `/`) probe via a synthetic child path;
 *   3. two-layer dedup — git check-ignore (local OR global) THEN the file's own text;
 *   4. ENOENT → create `~/.config/git/` recursively and write fresh;
 *   5. every failure swallowed (returns false, never throws).
 *
 * Git isolation: HOME / XDG_CONFIG_HOME / GIT_CONFIG_GLOBAL / GIT_CONFIG_SYSTEM are
 * all pointed at temp locations so the test NEVER reads or writes the developer's
 * real global gitignore, and `git check-ignore` resolves the same XDG path the code
 * writes to (default `core.excludesFile` = `$XDG_CONFIG_HOME/git/ignore`).
 */
import { describe, it, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as childProcess from 'child_process';
import { getGlobalGitignorePath, addFileGlobRuleToGitignore } from '../src/utils/git';

function git(args: string, cwd: string): void {
  childProcess.execSync(`git ${args}`, { cwd, encoding: 'utf-8', stdio: ['pipe', 'pipe', 'pipe'] });
}

const dirs: string[] = [];
function mkTemp(prefix: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(dir);
  return dir;
}
function initRepo(): string {
  const dir = mkTemp('u015-gi-repo-');
  git('init', dir);
  return dir;
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('U015 getGlobalGitignorePath — canonical resolver', () => {
  it('uses $XDG_CONFIG_HOME/git/ignore when XDG_CONFIG_HOME is set', () => {
    assert.equal(getGlobalGitignorePath('/home/u', '/xdg'), path.join('/xdg', 'git', 'ignore'));
  });
  it('falls back to ~/.config/git/ignore when XDG is unset', () => {
    assert.equal(getGlobalGitignorePath('/home/u', undefined), path.join('/home/u', '.config', 'git', 'ignore'));
  });
  it('treats a whitespace-only XDG value as unset', () => {
    assert.equal(getGlobalGitignorePath('/home/u', '   '), path.join('/home/u', '.config', 'git', 'ignore'));
  });
});

describe('U015 addFileGlobRuleToGitignore — global gitignore mutation', () => {
  let saved: Record<string, string | undefined>;
  let xdg: string;

  beforeEach(() => {
    saved = {
      HOME: process.env.HOME,
      XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME,
      GIT_CONFIG_GLOBAL: process.env.GIT_CONFIG_GLOBAL,
      GIT_CONFIG_SYSTEM: process.env.GIT_CONFIG_SYSTEM,
    };
    const home = mkTemp('u015-gi-home-');
    xdg = mkTemp('u015-gi-xdg-');
    process.env.HOME = home;
    process.env.XDG_CONFIG_HOME = xdg;
    // Empty global/system config → no inherited core.excludesFile, so git's default
    // excludesFile resolves to $XDG_CONFIG_HOME/git/ignore — the file the code writes.
    const emptyGlobal = path.join(mkTemp('u015-gi-cfg-'), 'config');
    fs.writeFileSync(emptyGlobal, '');
    process.env.GIT_CONFIG_GLOBAL = emptyGlobal;
    process.env.GIT_CONFIG_SYSTEM = '/dev/null';
  });

  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
  });

  it('is a no-op outside a git repository (returns false, writes nothing)', () => {
    const plain = mkTemp('u015-gi-plain-');
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', plain), false);
    assert.equal(fs.existsSync(getGlobalGitignorePath()), false);
  });

  it('creates the global gitignore (ENOENT branch) and ignores the file', () => {
    const repo = initRepo();
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), true);
    const gi = getGlobalGitignorePath();
    assert.equal(fs.readFileSync(gi, 'utf-8'), '**/permissions.local.json\n');
    // git now resolves the file as ignored via the global excludes.
    assert.equal(
      childProcess.execSync('git check-ignore --quiet -- permissions.local.json; echo $?', { cwd: repo, encoding: 'utf-8', shell: '/bin/bash' }).trim(),
      '0',
    );
  });

  it('is idempotent — a second call short-circuits at git-level dedup (returns false)', () => {
    const repo = initRepo();
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), true);
    const before = fs.readFileSync(getGlobalGitignorePath(), 'utf-8');
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), false);
    assert.equal(fs.readFileSync(getGlobalGitignorePath(), 'utf-8'), before, 'file unchanged on dedup');
  });

  it('appends to an existing global gitignore that lacks the entry', () => {
    const repo = initRepo();
    const gi = getGlobalGitignorePath();
    fs.mkdirSync(path.dirname(gi), { recursive: true });
    fs.writeFileSync(gi, '*.log\n');
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), true);
    const content = fs.readFileSync(gi, 'utf-8');
    assert.match(content, /\*\.log/);
    assert.match(content, /\*\*\/permissions\.local\.json/);
  });

  it('special-cases a directory pattern (trailing /) with a synthetic child probe', () => {
    const repo = initRepo();
    assert.equal(addFileGlobRuleToGitignore('cache/', repo), true);
    assert.equal(fs.readFileSync(getGlobalGitignorePath(), 'utf-8'), '**/cache/\n');
  });

  it('dedups on the file text when git cannot see the entry (content.includes branch)', () => {
    const repo = initRepo();
    // Point git at a SEPARATE empty excludesFile so check-ignore does NOT match,
    // while the code still reads/writes the XDG path that already holds the entry.
    const other = path.join(mkTemp('u015-gi-other-'), 'excludes');
    fs.writeFileSync(other, '');
    const cfg = path.join(mkTemp('u015-gi-cfg2-'), 'config');
    fs.writeFileSync(cfg, `[core]\n\texcludesFile = ${other}\n`);
    process.env.GIT_CONFIG_GLOBAL = cfg;
    const gi = getGlobalGitignorePath();
    fs.mkdirSync(path.dirname(gi), { recursive: true });
    fs.writeFileSync(gi, '**/permissions.local.json\n');
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), false);
  });

  it('swallows a write failure and returns false (outer catch)', () => {
    const repo = initRepo();
    // Make the `git` config dir a FILE so mkdirSync(dirname(global ignore)) throws.
    const giDir = path.dirname(getGlobalGitignorePath());
    fs.mkdirSync(path.dirname(giDir), { recursive: true });
    fs.writeFileSync(giDir, 'not-a-directory');
    assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), false);
  });

  it('returns false when git cannot be spawned (empty PATH)', () => {
    const repo = initRepo();
    const savedPath = process.env.PATH;
    process.env.PATH = '';
    try {
      assert.equal(addFileGlobRuleToGitignore('permissions.local.json', repo), false);
    } finally {
      process.env.PATH = savedPath;
    }
  });
});
