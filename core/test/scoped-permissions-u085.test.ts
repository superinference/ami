import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'node:path';

import {
  evaluateToolCeiling,
  isPathWithinRoots,
  buildSkillSandboxCeiling,
  type ToolCapabilityCeiling,
} from '../src/tools/scoped-permissions';
import { isReadOnlyBashCommand } from '../src/tools/bash-cache';

// ===========================================================================
// U085 (medium) — least-privilege capability ceiling for forked sub-agents
// (parity with Claude Code's createAutoMemCanUseTool). Pure building block.
// ===========================================================================

describe('evaluateToolCeiling — undefined / inert', () => {
  it('returns undefined when no ceiling is set', () => {
    assert.equal(
      evaluateToolCeiling(undefined, 'bash', { command: 'rm -rf /' }, isReadOnlyBashCommand),
      undefined,
    );
  });

  it('returns undefined when the ceiling restricts neither bash nor writes', () => {
    assert.equal(
      evaluateToolCeiling({}, 'bash', { command: 'rm -rf /' }, isReadOnlyBashCommand),
      undefined,
    );
  });
});

describe('evaluateToolCeiling — read-only bash ceiling', () => {
  const ceiling: ToolCapabilityCeiling = { readOnlyBashOnly: true };

  it('allows (undefined) a read-only bash command', () => {
    assert.equal(
      evaluateToolCeiling(ceiling, 'bash', { command: 'ls -la' }, isReadOnlyBashCommand),
      undefined,
    );
  });

  it('denies a non-read-only bash command (write/redirect/mutate)', () => {
    const d = evaluateToolCeiling(ceiling, 'bash', { command: 'echo hi > out.txt' }, isReadOnlyBashCommand);
    assert.ok(d, 'expected a denial');
    assert.match(d!.reason, /read-only shell commands/i);
  });

  it('denies a destructive bash command', () => {
    const d = evaluateToolCeiling(ceiling, 'bash', { command: 'rm -rf build' }, isReadOnlyBashCommand);
    assert.ok(d);
  });

  it('does not deny a non-bash tool under a read-only-bash-only ceiling', () => {
    assert.equal(
      evaluateToolCeiling(ceiling, 'file_read', { file_path: '/x' }, isReadOnlyBashCommand),
      undefined,
    );
  });

  it('ignores a bash call with no string command (nothing to classify)', () => {
    assert.equal(
      evaluateToolCeiling(ceiling, 'bash', {}, isReadOnlyBashCommand),
      undefined,
    );
  });

  it('uses the injected predicate (independent of the real classifier)', () => {
    // Force the predicate to report "not read-only" so even `ls` is denied.
    const d = evaluateToolCeiling(ceiling, 'bash', { command: 'ls' }, () => false);
    assert.ok(d);
    // And force "read-only" so even a write passes.
    assert.equal(
      evaluateToolCeiling(ceiling, 'bash', { command: 'rm -rf /' }, () => true),
      undefined,
    );
  });
});

describe('evaluateToolCeiling — write-root confinement', () => {
  const root = path.resolve('/work/memory');
  const ceiling: ToolCapabilityCeiling = { writeRoots: [root], baseDir: '/work' };

  it('allows an edit WITHIN a confined root', () => {
    assert.equal(
      evaluateToolCeiling(ceiling, 'file_edit', { file_path: path.join(root, 'notes.md') }, isReadOnlyBashCommand),
      undefined,
    );
  });

  it('allows the root directory itself', () => {
    assert.equal(
      evaluateToolCeiling(ceiling, 'file_write', { file_path: root }, isReadOnlyBashCommand),
      undefined,
    );
  });

  it('denies an edit OUTSIDE every confined root', () => {
    const d = evaluateToolCeiling(ceiling, 'file_edit', { file_path: '/work/src/app.ts' }, isReadOnlyBashCommand);
    assert.ok(d);
    assert.match(d!.reason, /confined to/i);
  });

  it('denies a sibling that shares a path prefix but not a boundary', () => {
    // /work/memory-other must NOT be treated as inside /work/memory.
    const d = evaluateToolCeiling(ceiling, 'file_write', { file_path: '/work/memory-other/x' }, isReadOnlyBashCommand);
    assert.ok(d);
  });

  it('resolves a relative file_path against baseDir', () => {
    // ./memory/a.txt → /work/memory/a.txt → inside the root.
    assert.equal(
      evaluateToolCeiling(ceiling, 'file_edit', { file_path: 'memory/a.txt' }, isReadOnlyBashCommand),
      undefined,
    );
    // ./src/a.txt → /work/src/a.txt → outside.
    assert.ok(evaluateToolCeiling(ceiling, 'file_edit', { file_path: 'src/a.txt' }, isReadOnlyBashCommand));
  });

  it('denies a write tool with no resolvable file_path', () => {
    const d = evaluateToolCeiling(ceiling, 'notebook_edit', {}, isReadOnlyBashCommand);
    assert.ok(d);
  });

  it('empty writeRoots = no writes anywhere', () => {
    const d = evaluateToolCeiling({ writeRoots: [] }, 'file_write', { file_path: '/anything' }, isReadOnlyBashCommand);
    assert.ok(d);
    assert.match(d!.reason, /\(no directory\)/);
  });

  it('does not confine a non-write tool', () => {
    assert.equal(
      evaluateToolCeiling(ceiling, 'bash', { command: 'ls' }, isReadOnlyBashCommand),
      undefined,
    );
  });
});

describe('isPathWithinRoots', () => {
  it('returns false for an empty root list', () => {
    assert.equal(isPathWithinRoots('/a/b', []), false);
  });
  it('matches the root itself and descendants, not prefix-siblings', () => {
    const roots = [path.resolve('/a/b')];
    assert.equal(isPathWithinRoots('/a/b', roots), true);
    assert.equal(isPathWithinRoots('/a/b/c', roots), true);
    assert.equal(isPathWithinRoots('/a/bc', roots), false);
    assert.equal(isPathWithinRoots('/a', roots), false);
  });
  it('resolves relatives against baseDir', () => {
    const roots = [path.resolve('/base/w')];
    assert.equal(isPathWithinRoots('w/x', roots, '/base'), true);
    assert.equal(isPathWithinRoots('y/x', roots, '/base'), false);
  });
});

describe('buildSkillSandboxCeiling', () => {
  it('returns undefined when nothing is declared', () => {
    assert.equal(buildSkillSandboxCeiling(undefined, undefined, '/cwd'), undefined);
    assert.equal(buildSkillSandboxCeiling(false, undefined, '/cwd'), undefined);
  });

  it('builds a read-only-bash ceiling', () => {
    const c = buildSkillSandboxCeiling(true, undefined, '/cwd');
    assert.deepEqual(c, { readOnlyBashOnly: true });
  });

  it('resolves relative write roots against cwd and keeps absolute ones', () => {
    const c = buildSkillSandboxCeiling(undefined, ['./mem', '/abs/dir'], '/cwd');
    assert.ok(c);
    assert.equal(c!.baseDir, '/cwd');
    assert.deepEqual(c!.writeRoots, [path.resolve('/cwd', './mem'), path.normalize('/abs/dir')]);
  });

  it('combines both restrictions', () => {
    const c = buildSkillSandboxCeiling(true, [], '/cwd');
    assert.ok(c);
    assert.equal(c!.readOnlyBashOnly, true);
    assert.deepEqual(c!.writeRoots, []);
  });
});
