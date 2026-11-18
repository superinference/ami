/**
 * U010 / w115 — collectConfigDiagnostics multi-source dedup.
 *
 * Parity with Claude Code's loadSettingsFromDisk seenFiles/seenErrors: when two
 * config tiers resolve to the SAME physical file (most commonly running AMI from
 * the home directory, where `<cwd>/.ami/config.json` IS `<home>/.ami/config.json`),
 * the file must be loaded/diagnosed only ONCE and the identical diagnostic must not
 * be reported twice. Distinct files across tiers must still each be reported.
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { collectConfigDiagnostics } from '../src/config';

describe('collectConfigDiagnostics — multi-source dedup (U010 w115)', () => {
  let prevHome: string | undefined;
  let prevUserProfile: string | undefined;

  beforeEach(() => {
    prevHome = process.env.HOME;
    prevUserProfile = process.env.USERPROFILE;
  });
  afterEach(() => {
    if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
    if (prevUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = prevUserProfile;
  });

  it('reports a corrupt file only ONCE when project and global resolve to the same path', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cfg-dedup-'));
    // cwd === home: the project path and the global path are the SAME file.
    process.env.HOME = dir;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
    const cfgPath = path.join(dir, '.ami', 'config.json');
    fs.writeFileSync(cfgPath, '{ not valid json ');

    const diags = collectConfigDiagnostics(dir);
    const forFile = diags.filter(d => path.resolve(d.path) === path.resolve(cfgPath));
    assert.equal(forFile.length, 1, `same physical file must be diagnosed once, got ${forFile.length}`);
    assert.equal(forFile[0].kind, 'parse-error');

    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('still reports DISTINCT corrupt files across project and global tiers', () => {
    const cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cfg-cwd-'));
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cfg-home-'));
    process.env.HOME = home;
    delete process.env.USERPROFILE;
    fs.mkdirSync(path.join(cwd, '.ami'), { recursive: true });
    fs.mkdirSync(path.join(home, '.ami'), { recursive: true });
    fs.writeFileSync(path.join(cwd, '.ami', 'config.json'), '{ broken project ');
    fs.writeFileSync(path.join(home, '.ami', 'config.json'), '{ broken global ');

    const diags = collectConfigDiagnostics(cwd);
    assert.ok(diags.some(d => d.source === 'project'), 'project diagnostic present');
    assert.ok(diags.some(d => d.source === 'global'), 'global diagnostic present');

    fs.rmSync(cwd, { recursive: true, force: true });
    fs.rmSync(home, { recursive: true, force: true });
  });
});
