import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  MEMORY_SOURCE_LEVELS,
  parseFrontmatter,
} from '../src/memory';

// ---------------------------------------------------------------------------
// U016 — Unified memory-source taxonomy + malformed-frontmatter surfacing.
//
// Gaps implemented here (parity with the reference utils/memory/types.ts
// MEMORY_TYPE_VALUES = User/Project/Local/Managed/AutoMem/(TeamMem) and
// utils/yaml.ts error surfacing):
//   - Distinct 'local' InstructionSource level for *.local.md override files
//     (was collapsed into 'user').
//   - Distinct 'automem' level for auto-captured stored memories (was 'user').
//   - First-class 'team' level for the shared team-memory store, feature-gated
//     by AMI_ENABLE_TEAM_MEMORY (parity: feature('TEAMMEM')).
//   - parseFrontmatter surfaces a named warning for a malformed (no-colon) line
//     instead of silently dropping it.
//
// The auto-memory store is redirected to an isolated temp dir via AMI_MEMORY_DIR
// so nothing touches the real ~/.ami store.
// ---------------------------------------------------------------------------

let cwdDir: string;
let memDir: string;
const savedMemEnv = process.env.AMI_MEMORY_DIR;
const savedTeamEnv = process.env.AMI_ENABLE_TEAM_MEMORY;

function mkTmp(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

beforeEach(() => {
  cwdDir = mkTmp('si-u016-cwd-');
  memDir = mkTmp('si-u016-store-');
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.AMI_ENABLE_TEAM_MEMORY;
  // A git root so the upward scan stops at cwd.
  fs.mkdirSync(path.join(cwdDir, '.git'), { recursive: true });
});

afterEach(() => {
  try { fs.rmSync(cwdDir, { recursive: true, force: true }); } catch { /* ignore */ }
  try { fs.rmSync(memDir, { recursive: true, force: true }); } catch { /* ignore */ }
  const restore = (k: string, v: string | undefined) => {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  };
  restore('AMI_MEMORY_DIR', savedMemEnv);
  restore('AMI_ENABLE_TEAM_MEMORY', savedTeamEnv);
});

function writeStoredMemory(name: string, body: string): void {
  fs.mkdirSync(memDir, { recursive: true });
  fs.writeFileSync(
    path.join(memDir, `${name}.md`),
    `---\nname: ${name}\ndescription: desc ${name}\ntype: user\n---\n\n${body}\n`,
  );
}

describe('MEMORY_SOURCE_LEVELS — unified taxonomy', () => {
  it('exposes the six first-class source levels', () => {
    assert.deepEqual(
      [...MEMORY_SOURCE_LEVELS].sort(),
      ['automem', 'local', 'managed', 'project', 'team', 'user'].sort(),
    );
  });
});

describe('collectInstructionSources — distinct local/automem scopes', () => {
  it("tags *.local.md as 'local', NOT 'user'", async () => {
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.md'), 'checked-in project rule');
    fs.writeFileSync(path.join(cwdDir, 'CLAUDE.local.md'), 'gitignored override rule');

    const sources = await new MemoryManager(cwdDir).collectInstructionSources();
    const local = sources.find(s => s.content.includes('gitignored override rule'));
    assert.ok(local, 'local override must be collected');
    assert.equal(local.level, 'local');
    assert.ok(sources.some(s => s.level === 'project' && s.content.includes('checked-in project rule')));
    // The local override is no longer collapsed into the 'user' tier.
    assert.ok(!sources.some(s => s.level === 'user' && s.content.includes('gitignored override rule')));
  });

  it("tags auto-captured stored memories as 'automem', NOT 'user'", async () => {
    writeStoredMemory('pref', 'A captured preference');

    const sources = await new MemoryManager(cwdDir).collectInstructionSources();
    const auto = sources.find(s => s.content.includes('A captured preference'));
    assert.ok(auto, 'stored memory must be collected');
    assert.equal(auto.level, 'automem');
  });
});

describe('collectInstructionSources — first-class team scope (feature-gated)', () => {
  function writeTeamMemory(body: string): void {
    const teamDir = path.join(memDir, 'team');
    fs.mkdirSync(teamDir, { recursive: true });
    fs.writeFileSync(path.join(teamDir, 'shared.md'), `---\nname: shared\n---\n\n${body}\n`);
  }

  it('omits the team store when team memory is disabled (default)', async () => {
    writeTeamMemory('a shared team note');
    const sources = await new MemoryManager(cwdDir).collectInstructionSources();
    assert.ok(!sources.some(s => s.level === 'team'));
    assert.ok(!sources.some(s => s.content.includes('a shared team note')));
  });

  it("includes the team store tagged 'team' when AMI_ENABLE_TEAM_MEMORY is set", async () => {
    process.env.AMI_ENABLE_TEAM_MEMORY = '1';
    writeTeamMemory('a shared team note');
    const sources = await new MemoryManager(cwdDir).collectInstructionSources();
    const team = sources.find(s => s.content.includes('a shared team note'));
    assert.ok(team, 'team memory must be collected when enabled');
    assert.equal(team.level, 'team');
  });
});

describe('parseFrontmatter — malformed-line surfacing', () => {
  let savedWrite: typeof process.stderr.write;
  let savedToStderr: string | undefined;
  let savedLevel: string | undefined;
  let captured: string;

  beforeEach(() => {
    captured = '';
    savedToStderr = process.env.AMI_DEBUG_TO_STDERR;
    savedLevel = process.env.AMI_DEBUG_LOG_LEVEL;
    process.env.AMI_DEBUG_TO_STDERR = '1';
    process.env.AMI_DEBUG_LOG_LEVEL = 'warn';
    savedWrite = process.stderr.write.bind(process.stderr);
    (process.stderr as NodeJS.WriteStream).write = ((chunk: string | Uint8Array): boolean => {
      captured += typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf-8');
      return true;
    }) as typeof process.stderr.write;
  });

  afterEach(() => {
    process.stderr.write = savedWrite;
    const restore = (k: string, v: string | undefined) => {
      if (v === undefined) delete process.env[k]; else process.env[k] = v;
    };
    restore('AMI_DEBUG_TO_STDERR', savedToStderr);
    restore('AMI_DEBUG_LOG_LEVEL', savedLevel);
  });

  it('warns on a malformed (no-colon) frontmatter line instead of dropping it silently', () => {
    const raw = '---\nname: ok\nthis line has no colon\n---\nbody';
    const { frontmatter, body } = parseFrontmatter(raw, '/tmp/memory.md');
    // Behaviour-preserving: the valid key still parses, the body is intact.
    assert.equal(frontmatter.name, 'ok');
    assert.equal(body, 'body');
    assert.match(captured, /malformed line \(no key\)/);
    assert.match(captured, /this line has no colon/);
  });

  it('does NOT warn on blank, comment, or list-item lines', () => {
    const raw = '---\nname: ok\n\n# a comment\n- a stray item\n---\nbody';
    parseFrontmatter(raw, '/tmp/memory.md');
    assert.doesNotMatch(captured, /malformed line/);
  });
});
