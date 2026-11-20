import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  MemoryManager,
  additionalInstructionDirsEnabled,
} from '../src/memory';

/**
 * U016 / wave-77 — memory.ts parity additions:
 *  - additionalInstructionDirsEnabled + MemoryManager additional-directory
 *    instruction loading (parity: the reference CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD
 *    + getAdditionalDirectoriesForClaudeMd — --add-dir dirs contribute CLAUDE.md).
 *  - projectIsInGitRepo + describeProjectMemory git-aware project-memory description
 *    (parity: the reference MemoryFileSelector "Checked in at" vs "Saved in").
 */

const ENV_FLAG = 'AMI_ADDITIONAL_DIRECTORIES_CLAUDE_MD';

let cwd: string;
let addDir: string;
let savedFlag: string | undefined;

beforeEach(() => {
  cwd = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w77-cwd-'));
  addDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-w77-add-'));
  savedFlag = process.env[ENV_FLAG];
  delete process.env[ENV_FLAG];
});

afterEach(() => {
  fs.rmSync(cwd, { recursive: true, force: true });
  fs.rmSync(addDir, { recursive: true, force: true });
  if (savedFlag === undefined) delete process.env[ENV_FLAG];
  else process.env[ENV_FLAG] = savedFlag;
});

describe('additionalInstructionDirsEnabled (gap: CLAUDE_CODE_ADDITIONAL_DIRECTORIES_CLAUDE_MD flag)', () => {
  it('is OFF by default and ON only for a truthy value', () => {
    assert.equal(additionalInstructionDirsEnabled({}), false);
    assert.equal(additionalInstructionDirsEnabled({ [ENV_FLAG]: '' }), false);
    assert.equal(additionalInstructionDirsEnabled({ [ENV_FLAG]: '0' }), false);
    assert.equal(additionalInstructionDirsEnabled({ [ENV_FLAG]: 'false' }), false);
    assert.equal(additionalInstructionDirsEnabled({ [ENV_FLAG]: '1' }), true);
    assert.equal(additionalInstructionDirsEnabled({ [ENV_FLAG]: 'true' }), true);
  });
});

describe('loadProjectInstructions additional directories (gaps: --add-dir CLAUDE.md + additionalInstructionDirs knob)', () => {
  it('loads an additional dir instruction file only when the env flag is enabled', () => {
    fs.writeFileSync(path.join(cwd, 'AGENTS.md'), 'CWD RULES');
    fs.writeFileSync(path.join(addDir, 'CLAUDE.md'), 'ADDED DIR RULES');
    const mgr = new MemoryManager(cwd, { additionalInstructionDirs: [addDir] });

    // Flag OFF (default): added dir contributes nothing.
    let out = mgr.loadProjectInstructions();
    assert.match(out, /CWD RULES/);
    assert.doesNotMatch(out, /ADDED DIR RULES/);

    // Flag ON: added dir instruction file is pulled in, labelled "(added dir)".
    process.env[ENV_FLAG] = '1';
    out = mgr.loadProjectInstructions();
    assert.match(out, /CWD RULES/);
    assert.match(out, /ADDED DIR RULES/);
    assert.match(out, /\(added dir\)/);
  });

  it('does nothing when no additional dirs were supplied even if the flag is on', () => {
    fs.writeFileSync(path.join(cwd, 'AGENTS.md'), 'CWD RULES');
    fs.writeFileSync(path.join(addDir, 'CLAUDE.md'), 'ADDED DIR RULES');
    process.env[ENV_FLAG] = '1';
    const mgr = new MemoryManager(cwd); // no opts
    const out = mgr.loadProjectInstructions();
    assert.match(out, /CWD RULES/);
    assert.doesNotMatch(out, /ADDED DIR RULES/);
  });

  it('does not double-load an additional dir that equals cwd', () => {
    fs.writeFileSync(path.join(cwd, 'AGENTS.md'), 'UNIQUE CWD RULES');
    process.env[ENV_FLAG] = '1';
    const mgr = new MemoryManager(cwd, { additionalInstructionDirs: [cwd] });
    const out = mgr.loadProjectInstructions();
    const matches = out.match(/UNIQUE CWD RULES/g) ?? [];
    assert.equal(matches.length, 1);
  });

  it('fires the instructionsLoaded hook with loadReason additional_directory', () => {
    fs.writeFileSync(path.join(addDir, 'CLAUDE.md'), 'ADDED');
    process.env[ENV_FLAG] = '1';
    const mgr = new MemoryManager(cwd, { additionalInstructionDirs: [addDir] });
    const events: Array<{ loadReason?: string; filePath?: string }> = [];
    mgr.setHookCallback((ev, data) => { if (ev === 'instructionsLoaded') events.push(data); });
    mgr.loadProjectInstructions();
    const added = events.find(e => e.loadReason === 'additional_directory');
    assert.ok(added, 'expected an additional_directory instructionsLoaded event');
    assert.equal(added?.filePath, path.join(addDir, 'CLAUDE.md'));
  });

  it('surfaces added-dir instructions through getMemoryContext (the engine-consumed path)', () => {
    fs.writeFileSync(path.join(addDir, 'CLAUDE.md'), 'ADDED VIA CONTEXT');
    const mgr = new MemoryManager(cwd, { additionalInstructionDirs: [addDir] });
    // Off by default — getMemoryContext (what the engine/system-prompt calls) omits it.
    assert.doesNotMatch(mgr.getMemoryContext(), /ADDED VIA CONTEXT/);
    // Opted in — the added dir's instructions reach the assembled memory context.
    process.env[ENV_FLAG] = '1';
    assert.match(mgr.getMemoryContext(), /ADDED VIA CONTEXT/);
  });

  it('honors the exclude list for additional dir files', () => {
    fs.writeFileSync(path.join(addDir, 'CLAUDE.md'), 'ADDED DIR RULES');
    process.env[ENV_FLAG] = '1';
    process.env.AMI_MEMORY_EXCLUDES = '**/CLAUDE.md';
    try {
      const mgr = new MemoryManager(cwd, { additionalInstructionDirs: [addDir] });
      const out = mgr.loadProjectInstructions();
      assert.doesNotMatch(out, /ADDED DIR RULES/);
    } finally {
      delete process.env.AMI_MEMORY_EXCLUDES;
    }
  });
});

describe('projectIsInGitRepo + describeProjectMemory (gap: git-aware "Checked in at" vs "Saved in")', () => {
  it('projectIsInGitRepo is true when a .git dir exists, false otherwise', () => {
    const noGit = new MemoryManager(cwd);
    assert.equal(noGit.projectIsInGitRepo(), false);
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    assert.equal(new MemoryManager(cwd).projectIsInGitRepo(), true);
  });

  it('describeProjectMemory returns null when no project instruction file exists', () => {
    assert.equal(new MemoryManager(cwd).describeProjectMemory(), null);
  });

  it('describeProjectMemory says "Checked in at" inside a repo', () => {
    fs.mkdirSync(path.join(cwd, '.git'), { recursive: true });
    fs.writeFileSync(path.join(cwd, 'AGENTS.md'), '# rules');
    const d = new MemoryManager(cwd).describeProjectMemory();
    assert.ok(d);
    assert.equal(d?.checkedIn, true);
    assert.equal(d?.relativePath, './AGENTS.md');
    assert.equal(d?.description, 'Checked in at ./AGENTS.md');
  });

  it('describeProjectMemory says "Saved in" outside a repo', () => {
    fs.writeFileSync(path.join(cwd, 'CLAUDE.md'), '# rules');
    const d = new MemoryManager(cwd).describeProjectMemory();
    assert.ok(d);
    assert.equal(d?.checkedIn, false);
    assert.equal(d?.description, 'Saved in ./CLAUDE.md');
  });
});
