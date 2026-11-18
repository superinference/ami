/**
 * U058 — integration: a POSITIVE Docker availability probe write-throughs the
 * asymmetric capability cache into the shared global config, while a NEGATIVE probe
 * never does (so a machine without a reachable daemon can't poison a shared
 * `config.json`). Exercises the real getDockerAvailability → persistPositiveCapability
 * wiring with a mocked `docker version` shell seam (no real container).
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-docker-capability.test.ts
 */
import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { getDockerAvailability, _resetDockerAvailableCache } from '../src/tools/docker-sandbox';
import { shellHooks } from '../src/utils/shell';
import { readCachedCapability } from '../src/reference-journal';

let home: string;
let savedHome: string | undefined;

beforeEach(() => {
  _resetDockerAvailableCache();
  home = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-docker-cap-'));
  savedHome = process.env.HOME;
  process.env.HOME = home;
});

afterEach(() => {
  mock.restoreAll();
  _resetDockerAvailableCache();
  if (savedHome === undefined) delete process.env.HOME;
  else process.env.HOME = savedHome;
  fs.rmSync(home, { recursive: true, force: true });
});

describe('Docker availability write-through to the asymmetric capability cache', () => {
  it('persists dockerAvailable=true on a successful probe', () => {
    mock.method(shellHooks, 'execSync', () => Buffer.from('29.9.3'));
    assert.equal(getDockerAvailability().available, true);
    assert.equal(readCachedCapability('dockerAvailable', path.join(home, '.ami', 'config.json')), true);
  });

  it('does NOT persist anything on a failed probe (no poisoning false)', () => {
    mock.method(shellHooks, 'execSync', () => { throw new Error('daemon unreachable'); });
    assert.equal(getDockerAvailability().available, false);
    assert.equal(readCachedCapability('dockerAvailable', path.join(home, '.ami', 'config.json')), undefined);
  });
});
