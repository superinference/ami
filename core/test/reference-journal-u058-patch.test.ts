/**
 * U058 — reference-journal parity additions (structured-patch journal schema + BOM-tolerant
 * config reads):
 *  - buildUserEntry carries a tool_result's structuredPatch AND the "edit location not shown"
 *    truncation signal through toolUseResult (the field set the engine now populates from a
 *    file-mutating tool result — previously declared but never written).
 *  - resolveReadDirs / registerDataDir strip a leading UTF-8 BOM before JSON.parse so a
 *    BOM-prefixed dashboard/shared config is read (and merged) instead of being silently
 *    discarded as corrupt.
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-u058-patch.test.ts
 */
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { buildUserEntry, resolveReadDirs, registerDataDir } from '../src/reference-journal';
import type { DiffHunk } from '../src/types';

const tmp: string[] = [];
afterEach(() => { for (const d of tmp.splice(0)) fs.rmSync(d, { recursive: true, force: true }); });

function mkdir(prefix: string): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tmp.push(d);
  return d;
}

const HUNK: DiffHunk = { oldStart: 1, oldLines: 1, newStart: 1, newLines: 1, lines: ['-a', '+b'] };

describe('buildUserEntry — structuredPatch + truncated journal schema', () => {
  it('carries a tool_result structuredPatch verbatim onto the journal line', () => {
    const entry = buildUserEntry({
      uuid: 'u1', parentUuid: null, timestamp: 't', sessionId: 's', cwd: '/c',
      gitBranch: 'main', isSidechain: false,
      content: [{ type: 'tool_result', tool_use_id: 'x', is_error: false }],
      toolUseResult: { structuredPatch: [HUNK], is_error: false },
    });
    assert.deepEqual(entry.toolUseResult?.structuredPatch, [HUNK]);
    assert.equal(entry.toolUseResult?.truncated, undefined);
  });

  it('records the truncated "location not shown" signal (size-capped edit)', () => {
    const entry = buildUserEntry({
      uuid: 'u2', parentUuid: null, timestamp: 't', sessionId: 's', cwd: '/c',
      gitBranch: 'main', isSidechain: false,
      content: [{ type: 'tool_result', tool_use_id: 'y', is_error: false }],
      toolUseResult: { structuredPatch: [], truncated: true, is_error: false },
    });
    assert.deepEqual(entry.toolUseResult?.structuredPatch, []);
    assert.equal(entry.toolUseResult?.truncated, true);
  });

  it('omits toolUseResult entirely for a plain user prompt', () => {
    const entry = buildUserEntry({
      uuid: 'u3', parentUuid: null, timestamp: 't', sessionId: 's', cwd: '/c',
      gitBranch: 'main', isSidechain: false, content: 'hello',
    });
    assert.equal(entry.toolUseResult, undefined);
  });
});

describe('BOM-tolerant config reads', () => {
  it('resolveReadDirs reads a BOM-prefixed dashboard config (claudeConfigDirs honored)', () => {
    const cfgDir = mkdir('rj-bom-read-');
    const cfg = path.join(cfgDir, 'dash.json');
    // Leading U+FEFF BOM + JSON — a raw JSON.parse would throw on the BOM.
    fs.writeFileSync(cfg, '\ufeff' + JSON.stringify({ claudeConfigDirs: ['/extra/dash/dir'] }));
    const env = { HOME: '/home/u058-bom', SUPERINFERENCE_DASHBOARD_CONFIG: cfg } as NodeJS.ProcessEnv;
    const dirs = resolveReadDirs(env);
    assert.ok(dirs.includes('/extra/dash/dir'), `BOM config must be parsed: ${JSON.stringify(dirs)}`);
  });

  it('registerDataDir merges into a BOM-prefixed config without clobbering other keys', () => {
    const cfgDir = mkdir('rj-bom-reg-');
    const cfg = path.join(cfgDir, 'cfg.json');
    fs.writeFileSync(cfg, '\ufeff' + JSON.stringify({ other: 'keep', claudeConfigDirs: ['/a'] }));
    const wrote = registerDataDir('/new/dir', cfg);
    assert.equal(wrote, true, 'a new dir is appended');
    const after = JSON.parse(fs.readFileSync(cfg, 'utf8')) as Record<string, unknown>;
    assert.equal(after.other, 'keep', 'pre-existing keys survive (BOM did not blank the config)');
    assert.deepEqual(after.claudeConfigDirs, ['/a', '/new/dir']);
  });

  it('registerDataDir is idempotent on a BOM-prefixed config already listing the dir', () => {
    const cfgDir = mkdir('rj-bom-idem-');
    const cfg = path.join(cfgDir, 'cfg.json');
    fs.writeFileSync(cfg, '\ufeff' + JSON.stringify({ claudeConfigDirs: ['/dup'] }));
    assert.equal(registerDataDir('/dup', cfg), false, 'no write when already present');
  });
});
