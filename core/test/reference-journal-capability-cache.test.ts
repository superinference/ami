/**
 * U058 — reference-journal asymmetric positive-only capability cache:
 *  - persistPositiveCapability: write ONLY a positive machine-local capability
 *    detection into the shared global config; a negative is a deliberate no-op so a
 *    machine that lacks the capability can never poison a shared `config.json`
 *    (parity: the reference's isChromeExtensionInstalled_CACHED_MAY_BE_STALE, which
 *    persists only a positive `cachedChromeExtensionInstalled`).
 *  - readCachedCapability: pure read of a previously-cached detection, so a
 *    startup-critical path can consult a prior positive without re-probing.
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-capability-cache.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { persistPositiveCapability, readCachedCapability } from '../src/reference-journal';

describe('asymmetric positive-only capability cache', () => {
  let dir: string;
  let cfg: string;

  beforeEach(() => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-capcache-'));
    cfg = path.join(dir, 'config.json');
  });
  afterEach(() => {
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it('persists a POSITIVE detection and reads it back', () => {
    assert.equal(readCachedCapability('dockerAvailable', cfg), undefined);
    assert.equal(persistPositiveCapability('dockerAvailable', true, cfg), true);
    assert.equal(readCachedCapability('dockerAvailable', cfg), true);
    const raw = JSON.parse(fs.readFileSync(cfg, 'utf8'));
    assert.deepEqual(raw.cachedCapabilities, { dockerAvailable: true });
  });

  it('NEVER persists a negative detection (no file write, stays uncached)', () => {
    assert.equal(persistPositiveCapability('dockerAvailable', false, cfg), false);
    assert.equal(fs.existsSync(cfg), false);
    assert.equal(readCachedCapability('dockerAvailable', cfg), undefined);
  });

  it('a negative detection never flips an already-cached positive to false', () => {
    persistPositiveCapability('dockerAvailable', true, cfg);
    assert.equal(persistPositiveCapability('dockerAvailable', false, cfg), false);
    assert.equal(readCachedCapability('dockerAvailable', cfg), true);
  });

  it('is idempotent — a second positive write is a no-op (no second write)', () => {
    assert.equal(persistPositiveCapability('dockerAvailable', true, cfg), true);
    assert.equal(persistPositiveCapability('dockerAvailable', true, cfg), false);
    assert.equal(readCachedCapability('dockerAvailable', cfg), true);
  });

  it('preserves unrelated config keys and other cached capabilities', () => {
    fs.writeFileSync(cfg, JSON.stringify({ model: 'claude-opus-4-8', cachedCapabilities: { browser: true } }));
    assert.equal(persistPositiveCapability('dockerAvailable', true, cfg), true);
    const raw = JSON.parse(fs.readFileSync(cfg, 'utf8'));
    assert.equal(raw.model, 'claude-opus-4-8');
    assert.deepEqual(raw.cachedCapabilities, { browser: true, dockerAvailable: true });
  });

  it('readCachedCapability returns undefined for an unknown key / missing file', () => {
    assert.equal(readCachedCapability('dockerAvailable', path.join(dir, 'nope.json')), undefined);
    persistPositiveCapability('dockerAvailable', true, cfg);
    assert.equal(readCachedCapability('somethingElse', cfg), undefined);
  });

  it('a null config path is a safe no-op (no throw)', () => {
    assert.equal(persistPositiveCapability('dockerAvailable', true, null), false);
    assert.equal(readCachedCapability('dockerAvailable', null), undefined);
  });
});
