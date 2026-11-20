/**
 * U016 (wave 68) — memory.ts parity gaps:
 *  - isMemoryPath()        — traversal-safe auto-memory membership predicate
 *                            (parity: memdir/paths.ts isAutoMemPath, AgentTool
 *                            isAgentMemoryPath)
 *  - isMemoryFilePath()    — recognize a .md path inside the memory tree
 *                            (parity: the reference's markdown-memory module isMemoryFilePath)
 *  - isTeamMemFile()       — shared team-memory path predicate
 *                            (parity: utils/teamMemoryOps.ts / teamMemPaths)
 *  - memoryScopeForPath()  — personal|team|null, TEAM-FIRST ordering
 *                            (parity: utils/memoryFileDetection.ts)
 *  - getMemoryScopeDisplay() — human-readable scope + resolved path
 *                            (parity: AgentTool/agentMemory.ts)
 *  - shared-memory secret-ingest guard — scan team files for credentials, skip
 *    the file + emit `memorySecretWarning` (parity: services/teamMemorySync/
 *    index.ts + teamMemSecretGuard.ts)
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-w68.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { MemoryManager } from '../src/memory';

// The auto-memory store is redirected to an isolated temp dir via AMI_MEMORY_DIR
// so getMemoryDir()/loadTeamMemories() read only test fixtures.
let cwdDir: string;
let memDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedDisableEnv = process.env.AMI_DISABLE_AUTO_MEMORY;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function writeTeamFile(name: string, body: string): string {
  const teamDir = path.join(memDir, 'team');
  fs.mkdirSync(teamDir, { recursive: true });
  const p = path.join(teamDir, `${name}.md`);
  fs.writeFileSync(
    p,
    `---\nname: ${name}\ndescription: ${name} desc\ntype: project\n---\n\n${body}\n`,
    'utf-8',
  );
  return p;
}

beforeEach(() => {
  cwdDir = mkTmp('si-mem-w68-cwd-');
  memDir = mkTmp('si-mem-w68-store-');
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_DISABLE_AUTO_MEMORY;
});

afterEach(() => {
  try { fs.rmSync(cwdDir, { recursive: true, force: true }); } catch { /* ignore */ }
  try { fs.rmSync(memDir, { recursive: true, force: true }); } catch { /* ignore */ }
  if (savedMemEnv === undefined) delete process.env.AMI_MEMORY_DIR;
  else process.env.AMI_MEMORY_DIR = savedMemEnv;
  if (savedDisableEnv === undefined) delete process.env.AMI_DISABLE_AUTO_MEMORY;
  else process.env.AMI_DISABLE_AUTO_MEMORY = savedDisableEnv;
});

// ── isMemoryPath ────────────────────────────────────────────────────────────
describe('U016-w68 — isMemoryPath', () => {
  it('recognizes a file at the private memory root', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.isMemoryPath(path.join(memDir, 'user_role.md')), true);
  });
  it('recognizes a file in the nested team sub-dir', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.isMemoryPath(path.join(memDir, 'team', 'conventions.md')), true);
  });
  it('rejects a path outside the memory tree', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.isMemoryPath(path.join(os.tmpdir(), 'elsewhere.md')), false);
  });
  it('is traversal-safe: a ..-escaping path is not a member', () => {
    const mgr = new MemoryManager(cwdDir);
    const escaping = path.join(memDir, '..', '..', 'etc', 'passwd');
    assert.equal(mgr.isMemoryPath(escaping), false);
  });
});

// ── isMemoryFilePath ────────────────────────────────────────────────────────
describe('U016-w68 — isMemoryFilePath', () => {
  it('accepts a .md inside the tree, rejects a non-.md inside it', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.isMemoryFilePath(path.join(memDir, 'a.md')), true);
    assert.equal(mgr.isMemoryFilePath(path.join(memDir, 'a.txt')), false);
  });
  it('rejects a .md OUTSIDE the tree', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.isMemoryFilePath(path.join(os.tmpdir(), 'x.md')), false);
  });
});

// ── isTeamMemFile ───────────────────────────────────────────────────────────
describe('U016-w68 — isTeamMemFile', () => {
  it('is true only inside the team sub-dir', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.isTeamMemFile(path.join(memDir, 'team', 'c.md')), true);
    // a file at the private root is NOT a team file even though it is a member
    assert.equal(mgr.isTeamMemFile(path.join(memDir, 'c.md')), false);
    assert.equal(mgr.isTeamMemFile(path.join(os.tmpdir(), 'c.md')), false);
  });
});

// ── memoryScopeForPath (team-first ordering) ────────────────────────────────
describe('U016-w68 — memoryScopeForPath', () => {
  it('returns team for a team path (checked before personal)', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.memoryScopeForPath(path.join(memDir, 'team', 'c.md')), 'team');
  });
  it('returns personal for a private-root path', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.memoryScopeForPath(path.join(memDir, 'c.md')), 'personal');
  });
  it('returns null outside every store', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.equal(mgr.memoryScopeForPath(path.join(os.tmpdir(), 'c.md')), null);
  });
});

// ── getMemoryScopeDisplay ───────────────────────────────────────────────────
describe('U016-w68 — getMemoryScopeDisplay', () => {
  it('describes the default private store with no argument', () => {
    const mgr = new MemoryManager(cwdDir);
    const d = mgr.getMemoryScopeDisplay();
    assert.match(d, /^Private \(/);
    assert.ok(d.includes(memDir) || d.includes('~'));
  });
  it('labels team / personal / external paths', () => {
    const mgr = new MemoryManager(cwdDir);
    assert.match(mgr.getMemoryScopeDisplay(path.join(memDir, 'team', 'c.md')), /^Team \(/);
    assert.match(mgr.getMemoryScopeDisplay(path.join(memDir, 'c.md')), /^Private \(/);
    assert.match(mgr.getMemoryScopeDisplay(path.join(os.tmpdir(), 'c.md')), /^External \(/);
  });
});

// ── shared-memory secret-ingest guard (integration) ─────────────────────────
describe('U016-w68 — team-memory secret-ingest guard', () => {
  it('skips a team file containing a secret and loads a clean one', () => {
    writeTeamFile('clean', 'Use conventional commits in this repo.');
    writeTeamFile('leaky', 'token: ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');

    const mgr = new MemoryManager(cwdDir);
    const events: Array<{ event: string; data: any }> = [];
    mgr.setHookCallback((event, data) => events.push({ event, data }));

    const loaded = mgr.loadTeamMemories();
    // clean file loaded, leaky file dropped
    assert.ok(loaded.some(m => m.includes('conventional commits')), 'clean team memory must load');
    assert.ok(
      !loaded.some(m => m.includes('ghp_')),
      'a team file with a secret must NOT be loaded into context',
    );

    // warning emitted with the credential label (never the secret value)
    const warn = events.find(e => e.event === 'memorySecretWarning');
    assert.ok(warn, 'a memorySecretWarning event must fire');
    assert.equal(warn!.data.scope, 'team');
    assert.deepEqual(warn!.data.credentials, ['GitHub PAT']);
    assert.ok(String(warn!.data.location).startsWith('Team ('), 'warning carries a scope display');
    assert.ok(
      !JSON.stringify(warn!.data).includes('ghp_aaaa'),
      'the warning payload must never echo the secret value',
    );
  });

  it('getMemoryContext never surfaces a secret-bearing team file body', () => {
    // An instruction file guarantees the memory system renders (empty-memory
    // affordance needs preceding instruction content).
    fs.writeFileSync(path.join(cwdDir, 'AGENTS.md'), '# Rules\nBe precise.', 'utf-8');
    writeTeamFile('leaky', 'secret-marker-xyz\nkey: ghp_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');

    const ctx = new MemoryManager(cwdDir).getMemoryContext();
    assert.ok(!ctx.includes('secret-marker-xyz'), 'secret-bearing team body must be excluded from the prompt');
    assert.ok(!ctx.includes('ghp_'), 'no credential may leak into the system prompt');
  });

  it('a clean team store loads with no warning (guard does not false-positive)', () => {
    writeTeamFile('ok', 'Prefer small PRs.');
    const mgr = new MemoryManager(cwdDir);
    const events: string[] = [];
    mgr.setHookCallback((event) => events.push(event));
    const loaded = mgr.loadTeamMemories();
    assert.deepEqual(loaded, ['Prefer small PRs.']);
    assert.ok(!events.includes('memorySecretWarning'));
  });
});
