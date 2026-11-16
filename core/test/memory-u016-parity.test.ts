import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  MemoryManager,
  MAX_MEMORY_CHARACTER_COUNT,
  resolveMemoryDirOverride,
} from '../src/memory';
import { projectStateDir, canonicalGitRoot } from '../src/reference-journal';

let tmpDir: string;
let memOverride: string;
const savedEnv = process.env.AMI_MEMORY_DIR;

function createTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-u016-'));
}
function cleanup(dir: string): void {
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch {}
}

function writeMemory(dir: string, rel: string, name: string, body: string): void {
  const fp = path.join(dir, rel);
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `---\nname: ${name}\ntype: project\n---\n\n${body}\n`, 'utf-8');
}

describe('U016 — AMI_MEMORY_DIR full-path override (getMemoryDir)', () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
    memOverride = createTmpDir();
  });
  afterEach(() => {
    cleanup(tmpDir);
    cleanup(memOverride);
    if (savedEnv === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = savedEnv;
  });

  it('redirects the memory dir to the validated override path', () => {
    delete process.env.AMI_MEMORY_DIR;
    const mgr = new MemoryManager(tmpDir);
    const defaultDir = mgr.getMemoryDir();

    process.env.AMI_MEMORY_DIR = memOverride;
    const overridden = mgr.getMemoryDir();
    assert.equal(overridden, memOverride.normalize('NFC'));
    assert.notEqual(overridden, defaultDir);

    // Round-trips through the overridden location, not the default one.
    mgr.saveMemory('over-mem', 'override body', 'desc', 'project');
    assert.ok(fs.existsSync(path.join(memOverride, 'over-mem.md')));
    assert.equal(mgr.loadMemories().length, 1);
  });

  it('falls back to the computed default when override is unset', () => {
    delete process.env.AMI_MEMORY_DIR;
    const mgr = new MemoryManager(tmpDir);
    assert.equal(mgr.getMemoryDir(), projectStateDir(canonicalGitRoot(tmpDir), 'memory'));
  });

  it('rejects a dangerous (relative / near-root) override and falls back', () => {
    const mgr = new MemoryManager(tmpDir);
    const def = projectStateDir(canonicalGitRoot(tmpDir), 'memory');

    process.env.AMI_MEMORY_DIR = '../evil';
    assert.equal(mgr.getMemoryDir(), def);
    assert.equal(resolveMemoryDirOverride({ AMI_MEMORY_DIR: '../evil' } as NodeJS.ProcessEnv), null);

    process.env.AMI_MEMORY_DIR = '/';
    assert.equal(mgr.getMemoryDir(), def);
    assert.equal(resolveMemoryDirOverride({ AMI_MEMORY_DIR: '/' } as NodeJS.ProcessEnv), null);
  });

  it('tilde-expands a ~/ override to an absolute home-rooted path', () => {
    const resolved = resolveMemoryDirOverride({
      ...process.env,
      HOME: os.homedir(),
      AMI_MEMORY_DIR: '~/.ami-u016-expand-check',
    } as NodeJS.ProcessEnv);
    assert.ok(resolved !== null);
    assert.ok(path.isAbsolute(resolved!));
    assert.ok(!resolved!.includes('~'));
    assert.ok(resolved!.endsWith(`${path.sep}.ami-u016-expand-check`));
  });

  it('returns null for an unset / empty override', () => {
    assert.equal(resolveMemoryDirOverride({} as NodeJS.ProcessEnv), null);
    assert.equal(resolveMemoryDirOverride({ AMI_MEMORY_DIR: '   ' } as NodeJS.ProcessEnv), null);
  });
});

describe('U016 — recursive memory-directory scan (loadMemories)', () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
    memOverride = createTmpDir();
    process.env.AMI_MEMORY_DIR = memOverride;
  });
  afterEach(() => {
    cleanup(tmpDir);
    cleanup(memOverride);
    if (savedEnv === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = savedEnv;
  });

  it('discovers memories nested in topic subdirectories', () => {
    writeMemory(memOverride, 'flat.md', 'flat', 'flat body');
    writeMemory(memOverride, 'topic/nested.md', 'nested', 'nested body');
    writeMemory(memOverride, 'topic/deeper/deep.md', 'deep', 'deep body');

    const names = new Set((new MemoryManager(tmpDir)).loadMemories().map(m => m.name));
    assert.ok(names.has('flat'));
    assert.ok(names.has('nested'));
    assert.ok(names.has('deep'));
  });

  it('excludes the team/ subtree (rendered separately) and MEMORY.md at any level', () => {
    writeMemory(memOverride, 'keep.md', 'keep', 'keep body');
    writeMemory(memOverride, 'team/shared.md', 'shared', 'team body');
    fs.writeFileSync(path.join(memOverride, 'MEMORY.md'), '- [keep](keep.md) — toc\n', 'utf-8');
    fs.mkdirSync(path.join(memOverride, 'topic'), { recursive: true });
    fs.writeFileSync(path.join(memOverride, 'topic', 'MEMORY.md'), '- [x](x.md)\n', 'utf-8');

    const names = (new MemoryManager(tmpDir)).loadMemories().map(m => m.name);
    assert.deepEqual(names.sort(), ['keep']);
  });
});

describe('U016 — oversized memory-file diagnostic (getLargeMemoryFiles)', () => {
  beforeEach(() => {
    tmpDir = createTmpDir();
    memOverride = createTmpDir();
    process.env.AMI_MEMORY_DIR = memOverride;
  });
  afterEach(() => {
    cleanup(tmpDir);
    cleanup(memOverride);
    if (savedEnv === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = savedEnv;
  });

  it('flags files over the recommended character limit, largest first', () => {
    writeMemory(memOverride, 'small.md', 'small', 'tiny');
    const big = 'x'.repeat(MAX_MEMORY_CHARACTER_COUNT + 100);
    const bigger = 'y'.repeat(MAX_MEMORY_CHARACTER_COUNT + 5000);
    writeMemory(memOverride, 'big.md', 'big', big);
    writeMemory(memOverride, 'topic/bigger.md', 'bigger', bigger);

    const large = (new MemoryManager(tmpDir)).getLargeMemoryFiles();
    const bases = large.map(f => path.basename(f.path));
    assert.deepEqual(bases, ['bigger.md', 'big.md']);
    assert.ok(large[0].chars > large[1].chars);
  });

  it('returns nothing when all files are within the limit', () => {
    writeMemory(memOverride, 'ok.md', 'ok', 'within limit');
    assert.deepEqual((new MemoryManager(tmpDir)).getLargeMemoryFiles(), []);
  });

  it('surfaces a Memory Health Warning section in getMemoryContext', () => {
    writeMemory(memOverride, 'huge.md', 'huge', 'z'.repeat(MAX_MEMORY_CHARACTER_COUNT + 10));
    const ctx = (new MemoryManager(tmpDir)).getMemoryContext();
    assert.ok(ctx.includes('## Memory Health Warning'));
    assert.ok(ctx.includes('huge.md'));
    assert.ok(ctx.includes(`${MAX_MEMORY_CHARACTER_COUNT}-character`));
  });

  it('omits the warning when no file is oversized', () => {
    writeMemory(memOverride, 'fine.md', 'fine', 'fine body');
    const ctx = (new MemoryManager(tmpDir)).getMemoryContext();
    assert.ok(!ctx.includes('Memory Health Warning'));
  });
});
