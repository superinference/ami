// U010 / w117 — collectActiveConfigSources: the live config SOURCE STACK.
//
// Parity: Claude Code's Settings status `buildSettingSourcesProperties()` active
// setting-sources list. Distinct from resolveSettingsFilePaths (lists the four POSSIBLE
// tier paths regardless of existence) and collectConfigDiagnostics (only tiers with
// ERRORS): this enumerates the file-based tiers in precedence order (highest first) with
// whether each is ACTIVELY contributing (file exists + parsed to a config object).

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  collectActiveConfigSources,
  resetConfigCache,
  getLocalConfigPath,
  getProjectConfigPath,
  getGlobalConfigPath,
  getManagedConfigPath,
  type ConfigSource,
} from '../src/config';

const made: string[] = [];
const savedEnv: Record<string, string | undefined> = {};
function setEnv(k: string, v: string | undefined): void {
  if (!(k in savedEnv)) savedEnv[k] = process.env[k];
  if (v === undefined) delete process.env[k]; else process.env[k] = v;
}
function tmp(prefix: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  made.push(d);
  return d;
}
function sourcesOf(cwd: string): ConfigSource[] {
  return collectActiveConfigSources(cwd).map(s => s.source);
}

afterEach(() => {
  for (const [k, v] of Object.entries(savedEnv)) { if (v === undefined) delete process.env[k]; else process.env[k] = v; }
  for (const k of Object.keys(savedEnv)) delete savedEnv[k];
  for (const d of made.splice(0)) fs.rmSync(d, { recursive: true, force: true });
  resetConfigCache();
});

describe('collectActiveConfigSources (U010/w117)', () => {
  it('lists the file tiers in precedence order (highest first): local > project > global > managed', () => {
    const home = tmp('u010-w117-home-');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    // No managed override env → managed path still enumerated, inactive.
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', path.join(tmp('u010-w117-managed-'), 'config.json'));
    const cwd = tmp('u010-w117-proj-');
    resetConfigCache();
    assert.deepEqual(sourcesOf(cwd), ['local', 'project', 'global', 'managed']);
  });

  it('reports each tier path even when absent, all inactive for an empty workspace', () => {
    const home = tmp('u010-w117-home2-');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', path.join(tmp('u010-w117-m2-'), 'config.json'));
    const cwd = tmp('u010-w117-empty-');
    resetConfigCache();
    const sources = collectActiveConfigSources(cwd);
    for (const s of sources) assert.equal(s.active, false, `${s.source} inactive when no file`);
    const bySource = new Map(sources.map(s => [s.source, s]));
    assert.equal(bySource.get('local')!.path, getLocalConfigPath(cwd));
    assert.equal(bySource.get('project')!.path, getProjectConfigPath(cwd));
    assert.equal(bySource.get('global')!.path, getGlobalConfigPath());
    assert.equal(bySource.get('managed')!.path, getManagedConfigPath());
    // Every row carries a human-readable provenance name.
    for (const s of sources) assert.ok(s.sourceName && s.sourceName.length > 0);
  });

  it('marks a tier active only when its file exists and parses to a config object', () => {
    const home = tmp('u010-w117-home3-');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', path.join(tmp('u010-w117-m3-'), 'config.json'));
    const cwd = tmp('u010-w117-proj3-');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(getProjectConfigPath(cwd), JSON.stringify({ model: 'x' }));
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(getGlobalConfigPath()!, JSON.stringify({ thinkingLevel: 'high' }));
    resetConfigCache();

    const bySource = new Map(collectActiveConfigSources(cwd).map(s => [s.source, s]));
    assert.equal(bySource.get('project')!.active, true, 'project active (file present)');
    assert.equal(bySource.get('global')!.active, true, 'global active (file present)');
    assert.equal(bySource.get('local')!.active, false, 'local inactive (no file)');
    assert.equal(bySource.get('managed')!.active, false, 'managed inactive (no file)');
  });

  it('treats an unparseable project file as inactive (does not throw)', () => {
    const home = tmp('u010-w117-home4-');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', path.join(tmp('u010-w117-m4-'), 'config.json'));
    const cwd = tmp('u010-w117-proj4-');
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.writeFileSync(getProjectConfigPath(cwd), '{ this is : not json');
    resetConfigCache();
    const bySource = new Map(collectActiveConfigSources(cwd).map(s => [s.source, s]));
    assert.equal(bySource.get('project')!.active, false, 'malformed project config is inactive');
  });

  it('reports the managed tier active when a valid managed policy file is present', () => {
    const home = tmp('u010-w117-home5-');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    const managedDir = tmp('u010-w117-m5-');
    const managedPath = path.join(managedDir, 'config.json');
    fs.writeFileSync(managedPath, JSON.stringify({ model: 'org-default' }));
    // A relocatable (env-overridden) managed source must clear the admin-grade trust
    // bar (not group/world-writable, owned by self) before its policy is honored.
    fs.chmodSync(managedPath, 0o600);
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', managedPath);
    const cwd = tmp('u010-w117-proj5-');
    resetConfigCache();
    const bySource = new Map(collectActiveConfigSources(cwd).map(s => [s.source, s]));
    assert.equal(bySource.get('managed')!.path, managedPath);
    assert.equal(bySource.get('managed')!.active, true, 'valid managed policy is active');
  });

  it('fails open: an untrusted/broken managed policy is reported inactive, not thrown', () => {
    const home = tmp('u010-w117-home6-');
    setEnv('HOME', home);
    setEnv('USERPROFILE', home);
    const managedDir = tmp('u010-w117-m6-');
    const managedPath = path.join(managedDir, 'config.json');
    // A non-object payload makes loadManagedConfig throw ManagedConfigError (fail-closed).
    fs.writeFileSync(managedPath, '"not an object"');
    setEnv('SUPERINFERENCE_MANAGED_CONFIG', managedPath);
    const cwd = tmp('u010-w117-proj6-');
    resetConfigCache();
    let sources: ReturnType<typeof collectActiveConfigSources> = [];
    assert.doesNotThrow(() => { sources = collectActiveConfigSources(cwd); });
    const managed = sources.find(s => s.source === 'managed')!;
    assert.equal(managed.active, false, 'a broken managed policy is reported inactive');
  });
});
