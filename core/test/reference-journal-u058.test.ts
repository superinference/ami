/**
 * U058 — reference-journal parity additions:
 *  - resolveCacheDir: a transient-cache config knob honoring XDG_CACHE_HOME,
 *    distinct from the durable data dir (parity: the reference's cachePaths cache base).
 *  - subagentJournalPathFor: optional run-grouping subdir so related subagent
 *    transcripts nest under a run folder (parity: the reference's agentId→subdir map).
 *  - getMcpServerLogPath wiring: transient MCP logs follow the cache dir.
 * Run: cd core/core && node --import tsx --test test/reference-journal-u058.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'path';
import { resolveCacheDir, resolveConfigDir, subagentJournalPathFor } from '../src/reference-journal';
import { getMcpServerLogPath } from '../src/mcp/log-sink';

const HOME = '/home/u058';

describe('resolveCacheDir (XDG_CACHE_HOME transient-cache knob)', () => {
  it('honors XDG_CACHE_HOME → <XDG_CACHE_HOME>/ami when no storage override is set', () => {
    const env = { HOME, XDG_CACHE_HOME: '/xdg/cache-a' } as NodeJS.ProcessEnv;
    assert.equal(resolveCacheDir(env), path.join('/xdg/cache-a', 'ami'));
  });

  it('is DISTINCT from the data dir when XDG_CACHE_HOME is set', () => {
    const env = { HOME, XDG_CACHE_HOME: '/xdg/cache-b' } as NodeJS.ProcessEnv;
    assert.notEqual(resolveCacheDir(env), resolveConfigDir(env));
    assert.equal(resolveConfigDir(env), path.join(HOME, '.ami'));
  });

  it('falls back to the data dir when XDG_CACHE_HOME is unset/blank (unchanged default)', () => {
    const env = { HOME: '/home/u058-c', XDG_CACHE_HOME: '   ' } as NodeJS.ProcessEnv;
    assert.equal(resolveCacheDir(env), resolveConfigDir(env));
    assert.equal(resolveCacheDir(env), path.join('/home/u058-c', '.ami'));
  });

  it('an explicit storage override wins and keeps cache co-located with data', () => {
    const env = { HOME, SUPERINFERENCE_DATA_DIR: '/data/dir', XDG_CACHE_HOME: '/xdg/cache-d' } as NodeJS.ProcessEnv;
    assert.equal(resolveCacheDir(env), '/data/dir');
    assert.equal(resolveCacheDir(env), resolveConfigDir(env));
  });

  it('rejects a dangerous XDG_CACHE_HOME and falls back to the data dir', () => {
    const env = { HOME: '/home/u058-e', XDG_CACHE_HOME: '/' } as NodeJS.ProcessEnv; // near-root → dangerous
    assert.equal(resolveCacheDir(env), resolveConfigDir(env));
  });

  it('expands a leading ~ in XDG_CACHE_HOME against HOME', () => {
    const env = { HOME: '/home/u058-f', XDG_CACHE_HOME: '~/cache' } as NodeJS.ProcessEnv;
    assert.equal(resolveCacheDir(env), path.join('/home/u058-f', 'cache', 'ami'));
  });
});

describe('subagentJournalPathFor run-grouping subdir', () => {
  const env = { SUPERINFERENCE_DATA_DIR: '/data' } as NodeJS.ProcessEnv;
  const base = path.join('/data', 'projects');

  it('keeps the flat layout with no subdir (backward compatible)', () => {
    const p = subagentJournalPathFor('/repo', 'ag1', env);
    assert.ok(p.startsWith(base));
    assert.ok(p.endsWith(path.join('subagents', 'agent-ag1.jsonl')));
  });

  it('nests the agent transcript under a run subdir when given', () => {
    const p = subagentJournalPathFor('/repo', 'ag1', env, 'workflows-run42');
    assert.ok(p.endsWith(path.join('subagents', 'workflows-run42', 'agent-ag1.jsonl')));
  });

  it('a blank subdir is ignored (flat layout)', () => {
    const p = subagentJournalPathFor('/repo', 'ag1', env, '   ');
    assert.ok(p.endsWith(path.join('subagents', 'agent-ag1.jsonl')));
  });

  it('sanitizes a traversal-bearing run id so it cannot escape the subagents root', () => {
    const p = subagentJournalPathFor('/repo', 'ag1', env, '../../etc');
    assert.ok(!p.includes('..'), `subdir must be neutralized: ${p}`);
    assert.ok(p.includes(path.join('subagents', '______etc')) || p.includes('subagents'),
      `must stay under subagents: ${p}`);
  });
});

describe('getMcpServerLogPath follows the transient cache dir', () => {
  it('places MCP logs under <XDG_CACHE_HOME>/ami when set (distinct from data dir)', () => {
    const env = { HOME, XDG_CACHE_HOME: '/xdg/cache-mcp' } as NodeJS.ProcessEnv;
    // getMcpServerLogPath reads process.env by default; prove the wiring uses the
    // cache dir by pinning process.env for this single synchronous call.
    const saved = { ...process.env };
    try {
      process.env.HOME = env.HOME;
      process.env.XDG_CACHE_HOME = env.XDG_CACHE_HOME;
      delete process.env.CLAUDE_CONFIG_DIR;
      delete process.env.SUPERINFERENCE_DATA_DIR;
      const p = getMcpServerLogPath('srv', { now: new Date('2026-01-02T00:00:00Z') });
      assert.ok(p.startsWith(path.join('/xdg/cache-mcp', 'ami', 'mcp-logs-srv')), `got ${p}`);
      assert.ok(!p.startsWith(resolveConfigDir(env)), 'MCP logs must not sit under the data dir when XDG cache is set');
    } finally {
      for (const k of Object.keys(process.env)) delete process.env[k];
      Object.assign(process.env, saved);
    }
  });

  it('an explicit configDir override still wins', () => {
    const p = getMcpServerLogPath('srv', { configDir: '/cfg', now: new Date('2026-01-02T00:00:00Z') });
    assert.equal(p, path.join('/cfg', 'mcp-logs-srv', '2026-01-02.jsonl'));
  });
});
