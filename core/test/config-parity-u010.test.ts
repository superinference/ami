/**
 * U010 — core/core/src/config.ts parity gaps (w67).
 *
 * Covers:
 *  - JSONC trailing-comma tolerance (stripJsoncTrailingCommas / parseJsonc / loader).
 *  - Content-keyed parse memoization (parseJsonc returns an equal-but-distinct tree).
 *  - Path-safety guard (UNC + special-file) on config I/O (loadConfigFileChecked).
 *  - Deep-merge of nested config objects across layers (mergeConfigs).
 *  - Human-readable config-source provenance names (collectConfigDiagnostics).
 *  - cleanupPeriodDays transcript-retention knob (ProjectConfig + merge + validation).
 */

import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  stripJsoncTrailingCommas,
  parseJsonc,
  isUncConfigPath,
  isSpecialFileStats,
  getConfigSourceName,
  CONFIG_SOURCE_NAMES,
  loadConfigFileChecked,
  collectConfigDiagnostics,
  validateConfigJson,
  validateProjectConfigValues,
  mergeConfigs,
  fsHooks,
  type ProjectConfig,
} from '../src/config';

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cfg-u010-'));
}

// ---------------------------------------------------------------------------
// #2 — JSONC trailing-comma tolerance
// ---------------------------------------------------------------------------
describe('stripJsoncTrailingCommas (#2 trailing-comma tolerance)', () => {
  it('drops a trailing comma before a closing brace', () => {
    assert.equal(stripJsoncTrailingCommas('{"a": 1,}'), '{"a": 1}');
  });

  it('drops a trailing comma before a closing bracket (with whitespace)', () => {
    assert.equal(stripJsoncTrailingCommas('["x",\n  ]'), '["x"\n  ]');
  });

  it('leaves a non-trailing comma between items untouched', () => {
    assert.equal(stripJsoncTrailingCommas('{"a": 1, "b": 2}'), '{"a": 1, "b": 2}');
  });

  it('never touches a comma inside a string literal', () => {
    const input = '{"note": "a, b, ]", "x": 1}';
    assert.equal(stripJsoncTrailingCommas(input), input);
  });

  it('preserves an escaped quote inside a string so string-tracking stays correct', () => {
    const input = '{"q": "he said \\"hi,\\"", "y": 2}';
    assert.equal(stripJsoncTrailingCommas(input), input);
  });

  it('parseJsonc parses an object with a trailing comma (previously corrupt)', () => {
    assert.throws(() => JSON.parse('{"a": 1,}'));
    assert.deepEqual(parseJsonc('{"a": 1,}'), { a: 1 });
  });

  it('parseJsonc combines BOM + comments + trailing comma', () => {
    const raw = '﻿{\n  "a": 1, // one\n  "b": [2,],\n}';
    assert.deepEqual(parseJsonc(raw), { a: 1, b: [2] });
  });

  it('loadConfigFileChecked loads a config file that has a trailing comma', () => {
    const dir = mkTmp();
    try {
      const p = path.join(dir, 'config.json');
      fs.writeFileSync(p, '{\n  "model": "m",\n  "maxTurns": 5,\n}');
      const { config, diagnostic } = loadConfigFileChecked(p);
      assert.equal(diagnostic, undefined);
      assert.deepEqual(config, { model: 'm', maxTurns: 5 });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// #9 — content-keyed parse memoization
// ---------------------------------------------------------------------------
describe('parseJsonc memoization (#9)', () => {
  it('returns an equal but DISTINCT tree each call (cache cannot be mutated by a caller)', () => {
    const raw = '{"nested": {"k": 1}, "arr": [1,2]}';
    const a = parseJsonc(raw) as { nested: { k: number }; arr: number[] };
    const b = parseJsonc(raw) as { nested: { k: number }; arr: number[] };
    assert.deepEqual(a, b);
    assert.notEqual(a, b, 'distinct top-level object');
    assert.notEqual(a.nested, b.nested, 'distinct nested object (deep clone)');
    // Mutating one result must not affect the next parse of identical content.
    a.nested.k = 999;
    const c = parseJsonc(raw) as { nested: { k: number } };
    assert.equal(c.nested.k, 1);
  });

  it('a large payload above the cache byte-ceiling is still parsed correctly (uncached path)', () => {
    const big = `{"pad": "${'x'.repeat(120_000)}", "n": 7}`;
    const parsed = parseJsonc(big) as { n: number };
    assert.equal(parsed.n, 7);
  });
});

// ---------------------------------------------------------------------------
// #1 — path-safety guard (UNC + special files)
// ---------------------------------------------------------------------------
describe('path-safety guard on config I/O (#1)', () => {
  it('isUncConfigPath flags // and \\\\ prefixes only', () => {
    assert.equal(isUncConfigPath('//server/share/config.json'), true);
    assert.equal(isUncConfigPath('\\\\server\\share'), true);
    assert.equal(isUncConfigPath('/home/u/.ami/config.json'), false);
    assert.equal(isUncConfigPath('relative/config.json'), false);
  });

  it('loadConfigFileChecked treats a UNC path as absent WITHOUT any fs access', () => {
    const existsSpy = mock.method(fsHooks, 'existsSync');
    const statSpy = mock.method(fsHooks, 'statSync');
    try {
      const res = loadConfigFileChecked('//evil-host/share/config.json');
      assert.deepEqual(res, { config: null });
      assert.equal(existsSpy.mock.callCount(), 0, 'no existsSync for a UNC path');
      assert.equal(statSpy.mock.callCount(), 0, 'no statSync for a UNC path');
    } finally {
      existsSpy.mock.restore();
      statSpy.mock.restore();
    }
  });

  it('isSpecialFileStats flags FIFO/socket/char/block devices, not regular/dir', () => {
    const mk = (over: Partial<Record<string, () => boolean>>): fs.Stats => ({
      isFIFO: () => false, isSocket: () => false, isCharacterDevice: () => false,
      isBlockDevice: () => false, ...over,
    } as unknown as fs.Stats);
    assert.equal(isSpecialFileStats(mk({ isFIFO: () => true })), true);
    assert.equal(isSpecialFileStats(mk({ isSocket: () => true })), true);
    assert.equal(isSpecialFileStats(mk({ isCharacterDevice: () => true })), true);
    assert.equal(isSpecialFileStats(mk({ isBlockDevice: () => true })), true);
    assert.equal(isSpecialFileStats(mk({})), false);
  });

  it('loadConfigFileChecked skips a special file (FIFO) without reading it (no hang)', () => {
    const fifoStats = {
      isFIFO: () => true, isSocket: () => false,
      isCharacterDevice: () => false, isBlockDevice: () => false,
    } as unknown as fs.Stats;
    const statSpy = mock.method(fsHooks, 'statSync', () => fifoStats);
    const readSpy = mock.method(fsHooks, 'readFileSync', () => { throw new Error('must not read a FIFO'); });
    try {
      const res = loadConfigFileChecked('/tmp/some-fifo-config.json');
      assert.deepEqual(res, { config: null });
      assert.equal(readSpy.mock.callCount(), 0, 'readFileSync is never called on a special file');
    } finally {
      statSpy.mock.restore();
      readSpy.mock.restore();
    }
  });

  it('a normal regular file (statSync reports not-special) still loads', () => {
    const dir = mkTmp();
    try {
      const p = path.join(dir, 'config.json');
      fs.writeFileSync(p, '{"model": "ok"}');
      const { config } = loadConfigFileChecked(p);
      assert.deepEqual(config, { model: 'ok' });
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// #3 — deep-merge nested config objects across layers
// ---------------------------------------------------------------------------
describe('mergeConfigs deep-merges nested objects (#3)', () => {
  it('merges superinference flags from two layers instead of clobbering', () => {
    const global: ProjectConfig = { superinference: { enforcing: true } };
    const project: ProjectConfig = { superinference: { useLLMCritic: true } };
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual(merged.superinference, { enforcing: true, useLLMCritic: true });
  });

  it('a higher layer OVERRIDES a shared nested scalar but keeps the other layer keys', () => {
    const global: ProjectConfig = { superinference: { enforcing: false, useLLMCritic: true } };
    const project: ProjectConfig = { superinference: { enforcing: true } };
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual(merged.superinference, { enforcing: true, useLLMCritic: true });
  });

  it('deep-merges tipsOverride, accumulating string arrays (concat + dedupe)', () => {
    const global: ProjectConfig = { tipsOverride: { tips: ['a', 'b'], excludeDefault: true } };
    const project: ProjectConfig = { tipsOverride: { tips: ['b', 'c'] } };
    const merged = mergeConfigs({}, {}, project, global);
    assert.deepEqual(merged.tipsOverride, { tips: ['a', 'b', 'c'], excludeDefault: true });
  });

  it('does not alias the lower layer (merging does not mutate inputs)', () => {
    const global: ProjectConfig = { superinference: { enforcing: true } };
    const project: ProjectConfig = { superinference: { useLLMCritic: true } };
    mergeConfigs({}, {}, project, global);
    assert.deepEqual(global.superinference, { enforcing: true });
    assert.deepEqual(project.superinference, { useLLMCritic: true });
  });
});

// ---------------------------------------------------------------------------
// #6 — human-readable provenance names
// ---------------------------------------------------------------------------
describe('config source provenance names (#6)', () => {
  it('maps every source to a human-readable name', () => {
    assert.equal(getConfigSourceName('project'), 'project');
    assert.equal(getConfigSourceName('local'), 'project, gitignored');
    assert.equal(getConfigSourceName('global'), 'user');
    assert.equal(getConfigSourceName('managed'), 'managed');
    assert.equal(getConfigSourceName('env'), 'environment variable');
    assert.equal(getConfigSourceName('cli'), 'command-line flag');
    // The map is exhaustive over the type.
    assert.equal(Object.keys(CONFIG_SOURCE_NAMES).length, 6);
  });

  it('collectConfigDiagnostics tags a corrupt project file with its source + sourceName', () => {
    const dir = mkTmp();
    const prevHome = process.env.HOME;
    try {
      // Point HOME at an empty dir so only the project file is diagnosed.
      process.env.HOME = path.join(dir, 'home-empty');
      fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(dir, '.ami', 'config.json'), '{ not json ');
      const diags = collectConfigDiagnostics(dir);
      const projectDiag = diags.find(d => d.source === 'project');
      assert.ok(projectDiag, 'project diagnostic present');
      assert.equal(projectDiag!.sourceName, 'project');
      assert.equal(projectDiag!.kind, 'parse-error');
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('tags a corrupt local file as "project, gitignored"', () => {
    const dir = mkTmp();
    const prevHome = process.env.HOME;
    try {
      process.env.HOME = path.join(dir, 'home-empty');
      fs.mkdirSync(path.join(dir, '.ami'), { recursive: true });
      fs.writeFileSync(path.join(dir, '.ami', 'config.local.json'), '[1,2,3]');
      const diags = collectConfigDiagnostics(dir);
      const localDiag = diags.find(d => d.source === 'local');
      assert.ok(localDiag);
      assert.equal(localDiag!.sourceName, 'project, gitignored');
      assert.equal(localDiag!.kind, 'not-object');
    } finally {
      if (prevHome === undefined) delete process.env.HOME; else process.env.HOME = prevHome;
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// #5 — cleanupPeriodDays knob (config level)
// ---------------------------------------------------------------------------
describe('cleanupPeriodDays config knob (#5, config level)', () => {
  it('is a recognized key (no unknown-key diagnostic)', () => {
    const dir = mkTmp();
    try {
      const p = path.join(dir, 'config.json');
      fs.writeFileSync(p, JSON.stringify({ cleanupPeriodDays: 30 }));
      const { config, diagnostic } = loadConfigFileChecked(p);
      assert.equal(diagnostic, undefined);
      assert.equal(config!.cleanupPeriodDays, 30);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('a non-number value is a validation issue', () => {
    const issues = validateProjectConfigValues({ cleanupPeriodDays: '30' as unknown as number });
    const issue = issues.find(i => i.field === 'cleanupPeriodDays');
    assert.ok(issue, 'cleanupPeriodDays type issue surfaced');
    assert.match(issue!.message, /must be a number/);
  });

  it('participates in the merge waterfall (higher layer wins)', () => {
    const global: ProjectConfig = { cleanupPeriodDays: 30 };
    const project: ProjectConfig = { cleanupPeriodDays: 7 };
    assert.equal(mergeConfigs({}, {}, project, global).cleanupPeriodDays, 7);
    assert.equal(mergeConfigs({}, {}, null, global).cleanupPeriodDays, 30);
  });
});

// ---------------------------------------------------------------------------
// validateConfigJson now tolerant of JSONC
// ---------------------------------------------------------------------------
describe('validateConfigJson JSONC tolerance', () => {
  let tmp: string;
  beforeEach(() => { tmp = mkTmp(); });
  afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  it('accepts a payload with a trailing comma + comment', () => {
    assert.deepEqual(validateConfigJson('{ "a": 1, // c\n }'), { valid: true });
  });

  it('still rejects a genuinely broken payload with a recovery hint', () => {
    const res = validateConfigJson('{ "a": }');
    assert.equal(res.valid, false);
    assert.match(res.error!, /Check for missing commas/);
  });
});
