/**
 * U016 / wave w81 — MemoryManager parity additions:
 *
 *   1. Persisted/env CONSENT to load outside-workspace `@imports` (parity: the reference
 *      Code's `hasClaudeMdExternalIncludesApproved` external-includes dialog). The
 *      default posture is a hard block; an explicit, recorded user decision (project
 *      config flag or the AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS env), optionally scoped to
 *      trusted roots, is the sanctioned escape hatch. Covers
 *      `resolveExternalMemoryImportConsent` + the `resolveAtImports` approval branch.
 *
 *   2. Nested-worktree de-duplication of checked-in Project memory (parity: the reference
 *      Code the reference's markdown-memory module): from a worktree nested inside its own main repo, the
 *      walk continues past the worktree root to load the main repo's gitignored Local
 *      files while SKIPPING the main repo's checked-in Project files.
 *
 * Red-green: with the fix reverted, test (1b/1c/1d) fall back to the "import blocked"
 * marker and test (2a) never loads the main repo's CLAUDE.local.md.
 *
 * Run: cd core/core && node --import tsx --test test/memory-u016-w81.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  MemoryManager,
  resolveExternalMemoryImportConsent,
} from '../src/memory';
import { mergeConfigs } from '../src/config';

let ROOT: string;
let CWD: string;
let dataDir: string;
let memDir: string;
const savedEnv: Record<string, string | undefined> = {};

function stashEnv(...keys: string[]): void {
  for (const k of keys) savedEnv[k] = process.env[k];
}
function restoreEnv(): void {
  for (const [k, v] of Object.entries(savedEnv)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
}

beforeEach(() => {
  ROOT = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'si-w81-')));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'ws-')));
  dataDir = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'data-')));
  memDir = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'mem-')));
  stashEnv(
    'AMI_MEMORY_DIR', 'SUPERINFERENCE_DATA_DIR', 'CLAUDE_CONFIG_DIR',
    'AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS', 'AMI_EXTERNAL_MEMORY_IMPORT_ROOTS',
    'AMI_DISABLE_MEMORY_PROJECT', 'AMI_DISABLE_MEMORY_LOCAL',
  );
  // Isolate every config/store read from the developer's real ~/.ami.
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  process.env.AMI_MEMORY_DIR = memDir;
  delete process.env.CLAUDE_CONFIG_DIR;
  delete process.env.AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS;
  delete process.env.AMI_EXTERNAL_MEMORY_IMPORT_ROOTS;
  delete process.env.AMI_DISABLE_MEMORY_PROJECT;
  delete process.env.AMI_DISABLE_MEMORY_LOCAL;
});

afterEach(() => {
  restoreEnv();
  try { fs.rmSync(ROOT, { recursive: true, force: true }); } catch { /* ignore */ }
});

function writeProjectConfig(cwd: string, cfg: Record<string, unknown>): void {
  const dir = path.join(cwd, '.ami');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'config.json'), JSON.stringify(cfg), 'utf-8');
}

// ---------------------------------------------------------------------------
// 1. External-import consent
// ---------------------------------------------------------------------------
describe('resolveExternalMemoryImportConsent — env/config waterfall', () => {
  it('defaults to blocked (no consent, no roots)', () => {
    const r = resolveExternalMemoryImportConsent(CWD, {});
    assert.equal(r.approved, false);
    assert.deepEqual(r.roots, []);
  });

  it('grants consent via the env override and unions + absolutizes roots', () => {
    const r = resolveExternalMemoryImportConsent(CWD, {
      AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS: '1',
      AMI_EXTERNAL_MEMORY_IMPORT_ROOTS: `${ROOT}, ./rel`,
    });
    assert.equal(r.approved, true);
    assert.ok(r.roots.includes(ROOT));
    assert.ok(r.roots.includes(path.resolve('./rel')));
  });

  it('treats a falsey env value as no consent', () => {
    assert.equal(resolveExternalMemoryImportConsent(CWD, { AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS: 'false' }).approved, false);
    assert.equal(resolveExternalMemoryImportConsent(CWD, { AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS: '0' }).approved, false);
  });

  it('reads the persisted project-config approval flag', () => {
    writeProjectConfig(CWD, { externalMemoryImportsApproved: true, externalMemoryImportRoots: [ROOT] });
    const r = resolveExternalMemoryImportConsent(CWD, {});
    assert.equal(r.approved, true);
    assert.ok(r.roots.includes(ROOT));
  });
});

describe('mergeConfigs — external-import consent semantics', () => {
  it('approval is last-layer-wins; roots ACCUMULATE across layers', () => {
    const merged = mergeConfigs(
      {},
      {},
      { externalMemoryImportsApproved: false, externalMemoryImportRoots: ['/a'] }, // project
      {},                                                                          // global
      { externalMemoryImportsApproved: true, externalMemoryImportRoots: ['/b'] },  // local (higher)
      null,
    );
    assert.equal(merged.externalMemoryImportsApproved, true);
    assert.deepEqual(merged.externalMemoryImportRoots, ['/a', '/b']);
  });
});

describe('resolveAtImports — external-import consent gate', () => {
  it('BLOCKS an outside-workspace @import by default (no consent)', () => {
    const shared = path.join(ROOT, 'shared.md');
    fs.writeFileSync(shared, 'SHARED-STANDARDS-BODY');
    const out = new MemoryManager(CWD).resolveAtImports('see @../shared.md', path.join(CWD, 'CLAUDE.md'));
    assert.match(out, /import blocked — outside workspace/i);
    assert.doesNotMatch(out, /SHARED-STANDARDS-BODY/);
  });

  it('LOADS the external @import once consent is granted via env (red-green)', () => {
    const shared = path.join(ROOT, 'shared.md');
    fs.writeFileSync(shared, 'SHARED-STANDARDS-BODY');
    process.env.AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS = '1';
    const out = new MemoryManager(CWD).resolveAtImports('see @../shared.md', path.join(CWD, 'CLAUDE.md'));
    assert.match(out, /SHARED-STANDARDS-BODY/);
    assert.doesNotMatch(out, /import blocked/i);
  });

  it('LOADS the external @import when the project config approves it', () => {
    const shared = path.join(ROOT, 'shared.md');
    fs.writeFileSync(shared, 'CONFIG-APPROVED-BODY');
    writeProjectConfig(CWD, { externalMemoryImportsApproved: true });
    const out = new MemoryManager(CWD).resolveAtImports('see @../shared.md', path.join(CWD, 'CLAUDE.md'));
    assert.match(out, /CONFIG-APPROVED-BODY/);
  });

  it('honors a trusted-roots allowlist: inside allowed, outside still blocked', () => {
    const allowedRoot = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'allowed-')));
    const otherRoot = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'other-')));
    const inside = path.join(allowedRoot, 'ok.md');
    const outside = path.join(otherRoot, 'nope.md');
    fs.writeFileSync(inside, 'INSIDE-ROOT-BODY');
    fs.writeFileSync(outside, 'OUTSIDE-ROOT-BODY');
    process.env.AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS = '1';
    process.env.AMI_EXTERNAL_MEMORY_IMPORT_ROOTS = allowedRoot;

    const okOut = new MemoryManager(CWD).resolveAtImports(`@${inside}`, path.join(CWD, 'CLAUDE.md'));
    assert.match(okOut, /INSIDE-ROOT-BODY/);

    const blockedOut = new MemoryManager(CWD).resolveAtImports(`@${outside}`, path.join(CWD, 'CLAUDE.md'));
    assert.match(blockedOut, /import blocked — outside workspace/i);
    assert.doesNotMatch(blockedOut, /OUTSIDE-ROOT-BODY/);
  });

  it('fires a distinct instructionsLoaded hook reason when an external import is approved', () => {
    const shared = path.join(ROOT, 'shared.md');
    fs.writeFileSync(shared, 'HOOK-BODY');
    process.env.AMI_ALLOW_EXTERNAL_MEMORY_IMPORTS = '1';
    const reasons: string[] = [];
    const mm = new MemoryManager(CWD);
    mm.setHookCallback((_e, d) => { if (d && d.loadReason) reasons.push(d.loadReason); });
    mm.resolveAtImports('@../shared.md', path.join(CWD, 'CLAUDE.md'));
    assert.ok(reasons.includes('import-allowed-external-approved'));
  });

  it('still inlines a normal in-workspace @import (regression)', () => {
    fs.writeFileSync(path.join(CWD, 'child.md'), 'IN-WORKSPACE-BODY');
    const out = new MemoryManager(CWD).resolveAtImports('@./child.md', path.join(CWD, 'CLAUDE.md'));
    assert.match(out, /IN-WORKSPACE-BODY/);
  });
});

// ---------------------------------------------------------------------------
// 2. Nested-worktree de-duplication
// ---------------------------------------------------------------------------
describe('loadProjectInstructions — nested worktree Local-file loading', () => {
  // Build main/ (real clone root) and main/.claude/worktrees/wt (linked worktree).
  function buildNestedWorktree(): { main: string; wt: string } {
    const main = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'main-')));
    fs.mkdirSync(path.join(main, '.git'), { recursive: true });            // main = .git DIR
    fs.writeFileSync(path.join(main, 'CLAUDE.md'), 'MAIN-PROJECT-BODY');
    fs.writeFileSync(path.join(main, 'CLAUDE.local.md'), 'MAIN-LOCAL-BODY');

    const wt = path.join(main, '.claude', 'worktrees', 'wt');
    fs.mkdirSync(wt, { recursive: true });
    // Linked-worktree marker: .git FILE pointing into <main>/.git/worktrees/<name>.
    fs.writeFileSync(path.join(wt, '.git'), `gitdir: ${path.join(main, '.git', 'worktrees', 'wt')}\n`);
    fs.writeFileSync(path.join(wt, 'CLAUDE.md'), 'WT-PROJECT-BODY');
    fs.writeFileSync(path.join(wt, 'CLAUDE.local.md'), 'WT-LOCAL-BODY');
    return { main, wt };
  }

  it('loads the main repo Local file but SKIPS the main repo Project file (red-green)', () => {
    const { wt } = buildNestedWorktree();
    const out = new MemoryManager(wt).loadProjectInstructions();
    assert.match(out, /WT-PROJECT-BODY/);   // worktree's own Project
    assert.match(out, /WT-LOCAL-BODY/);     // worktree's own Local
    assert.match(out, /MAIN-LOCAL-BODY/);   // main repo's gitignored Local — the fix
    assert.doesNotMatch(out, /MAIN-PROJECT-BODY/); // main repo's checked-in Project — de-duped
  });

  it('a normal clone (no nesting) stops at its own .git and loads its own files only', () => {
    const repo = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'repo-')));
    fs.mkdirSync(path.join(repo, '.git'), { recursive: true });
    const parent = path.dirname(repo);
    fs.writeFileSync(path.join(repo, 'CLAUDE.md'), 'REPO-PROJECT-BODY');
    fs.writeFileSync(path.join(repo, 'CLAUDE.local.md'), 'REPO-LOCAL-BODY');
    // A file in the PARENT (above the git root) must never be loaded.
    fs.writeFileSync(path.join(parent, 'CLAUDE.local.md'), 'PARENT-ABOVE-GIT-BODY');

    const out = new MemoryManager(repo).loadProjectInstructions();
    assert.match(out, /REPO-PROJECT-BODY/);
    assert.match(out, /REPO-LOCAL-BODY/);
    assert.doesNotMatch(out, /PARENT-ABOVE-GIT-BODY/);
  });

  it('a submodule (.git modules pointer) is NOT treated as a nested worktree', () => {
    const main = fs.realpathSync(fs.mkdtempSync(path.join(ROOT, 'smain-')));
    fs.mkdirSync(path.join(main, '.git'), { recursive: true });
    fs.writeFileSync(path.join(main, 'CLAUDE.local.md'), 'PARENT-REPO-LOCAL-BODY');

    const sub = path.join(main, 'vendor', 'sub');
    fs.mkdirSync(sub, { recursive: true });
    fs.writeFileSync(path.join(sub, '.git'), `gitdir: ${path.join(main, '.git', 'modules', 'sub')}\n`);
    fs.writeFileSync(path.join(sub, 'CLAUDE.md'), 'SUB-PROJECT-BODY');

    const out = new MemoryManager(sub).loadProjectInstructions();
    assert.match(out, /SUB-PROJECT-BODY/);
    assert.doesNotMatch(out, /PARENT-REPO-LOCAL-BODY/); // submodule must not borrow parent files
  });
});
