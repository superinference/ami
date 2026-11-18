/**
 * U015 (w102) — integration: persisting a rule to the project-LOCAL destination
 * auto-adds the local permissions file to the global gitignore.
 *
 * AMI's save-destination metadata advertises the project-local tier as "gitignored
 * — not shared with collaborators" (common/src/permission-save-destinations.ts), but
 * nothing used to enforce it. `PermissionManager.saveRulesToDestination` now calls
 * `addFileGlobRuleToGitignore` on a `localSettings` write — exactly as Claude Code
 * does for its `localSettings` write (utils/settings/settings.ts L508-514) — so the
 * promise is actually kept.
 *
 * RED/GREEN: reverting the `if (destination === 'localSettings')` hook in
 * permissions.ts makes "adds the local permissions file to the global gitignore"
 * fail (the global ignore file is never created).
 */
import { describe, it, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as childProcess from 'child_process';
import { PermissionManager } from '../src/permissions';
import { getGlobalGitignorePath } from '../src/utils/git';

const dirs: string[] = [];
function mkTemp(prefix: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(dir);
  return dir;
}
function initRepo(): string {
  const dir = mkTemp('u015-plg-repo-');
  childProcess.execSync('git init', { cwd: dir, stdio: ['pipe', 'pipe', 'pipe'] });
  return dir;
}

after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

describe('U015 local destination auto-gitignores the local permissions file', () => {
  let saved: Record<string, string | undefined>;

  beforeEach(() => {
    saved = {
      HOME: process.env.HOME,
      XDG_CONFIG_HOME: process.env.XDG_CONFIG_HOME,
      GIT_CONFIG_GLOBAL: process.env.GIT_CONFIG_GLOBAL,
      GIT_CONFIG_SYSTEM: process.env.GIT_CONFIG_SYSTEM,
    };
    process.env.HOME = mkTemp('u015-plg-home-');
    process.env.XDG_CONFIG_HOME = mkTemp('u015-plg-xdg-');
    const emptyGlobal = path.join(mkTemp('u015-plg-cfg-'), 'config');
    fs.writeFileSync(emptyGlobal, '');
    process.env.GIT_CONFIG_GLOBAL = emptyGlobal;
    process.env.GIT_CONFIG_SYSTEM = '/dev/null';
  });

  afterEach(() => {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    }
  });

  it('adds **/permissions.local.json to the global gitignore on a localSettings save', () => {
    const repo = initRepo();
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls', action: 'allow', source: 'localSettings' },
    ]);
    const r = pm.saveRulesToDestination(repo, 'localSettings');
    assert.equal(r.ok, true);
    // the local permissions file was written ...
    assert.ok(fs.existsSync(path.join(repo, '.ami', 'permissions.local.json')));
    // ... and the global gitignore now ignores it.
    const gi = getGlobalGitignorePath();
    assert.ok(fs.existsSync(gi), 'global gitignore created');
    assert.match(fs.readFileSync(gi, 'utf-8'), /\*\*\/permissions\.local\.json/);
  });

  it('does NOT touch the global gitignore for a non-local (project) destination', () => {
    const repo = initRepo();
    const pm = new PermissionManager('ask', [
      { tool: 'web_fetch', action: 'deny', source: 'projectSettings' },
    ]);
    const r = pm.saveRulesToDestination(repo, 'projectSettings');
    assert.equal(r.ok, true);
    assert.ok(fs.existsSync(path.join(repo, '.ami', 'permissions.json')));
    assert.equal(fs.existsSync(getGlobalGitignorePath()), false, 'global gitignore untouched');
  });

  it('a localSettings save still succeeds outside a git repo (gitignore side-effect is best-effort)', () => {
    const plain = mkTemp('u015-plg-plain-');
    const pm = new PermissionManager('ask', [
      { tool: 'bash', pattern: 'ls', action: 'allow', source: 'localSettings' },
    ]);
    const r = pm.saveRulesToDestination(plain, 'localSettings');
    assert.equal(r.ok, true);
    assert.ok(fs.existsSync(path.join(plain, '.ami', 'permissions.local.json')));
    assert.equal(fs.existsSync(getGlobalGitignorePath()), false, 'no repo → nothing ignored');
  });
});
