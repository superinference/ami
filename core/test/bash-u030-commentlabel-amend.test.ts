import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  bashTool,
  extractBashCommentLabel,
  detectGitCommit,
  detectGitCommitKind,
} from '../src/tools/bash';
import type { ToolContext } from '../src/types';

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// extractBashCommentLabel — parity with Claude Code BashTool/commentLabel
// ---------------------------------------------------------------------------
describe('extractBashCommentLabel', () => {
  it('returns the leading comment text stripped of # and whitespace', () => {
    assert.equal(extractBashCommentLabel('# list files\nls -la'), 'list files');
  });

  it('handles a single-line command that is only a comment', () => {
    assert.equal(extractBashCommentLabel('# just a note'), 'just a note');
  });

  it('strips multiple leading hashes and inner spacing', () => {
    assert.equal(extractBashCommentLabel('###   build step\nmake'), 'build step');
  });

  it('trims surrounding whitespace before the #', () => {
    assert.equal(extractBashCommentLabel('   # run tests\nnpm test'), 'run tests');
  });

  it('returns undefined for a #! shebang (not a label)', () => {
    assert.equal(extractBashCommentLabel('#!/usr/bin/env bash\necho hi'), undefined);
  });

  it('returns undefined when the first line is not a comment', () => {
    assert.equal(extractBashCommentLabel('ls -la # trailing comment'), undefined);
  });

  it('returns undefined for an empty comment (# with nothing after)', () => {
    assert.equal(extractBashCommentLabel('#\nls'), undefined);
    assert.equal(extractBashCommentLabel('#   \nls'), undefined);
  });

  it('only inspects the FIRST line', () => {
    assert.equal(extractBashCommentLabel('ls\n# second line comment'), undefined);
  });
});

// ---------------------------------------------------------------------------
// detectGitCommitKind — amend vs committed vs not-a-commit
// ---------------------------------------------------------------------------
describe('detectGitCommitKind', () => {
  it('classifies a plain commit as "committed"', () => {
    assert.equal(detectGitCommitKind('git commit -m "x"'), 'committed');
  });

  it('classifies --amend as "amended"', () => {
    assert.equal(detectGitCommitKind('git commit --amend'), 'amended');
    assert.equal(detectGitCommitKind('git commit --amend --no-edit'), 'amended');
    assert.equal(detectGitCommitKind('git commit --amend=reword'), 'amended');
  });

  it('classifies amend even with global options before the subcommand', () => {
    assert.equal(detectGitCommitKind('git -C /tmp commit --amend -m y'), 'amended');
  });

  it('returns null for a non-commit git command', () => {
    assert.equal(detectGitCommitKind('git status'), null);
    assert.equal(detectGitCommitKind('git log --amend-ish'), null);
  });

  it('returns null when a non-git word carries --amend', () => {
    assert.equal(detectGitCommitKind('echo --amend'), null);
  });

  it('does not let a later chained invocation bleed --amend into a plain commit', () => {
    // First `git commit` is a plain commit; the `--amend` belongs to a later `git`.
    assert.equal(detectGitCommitKind('git commit -m first && git commit --amend'), 'committed');
  });

  it('keeps detectGitCommit a faithful predicate over the kind', () => {
    assert.equal(detectGitCommit('git commit --amend'), true);
    assert.equal(detectGitCommit('git commit -m x'), true);
    assert.equal(detectGitCommit('git status'), false);
  });
});

// ---------------------------------------------------------------------------
// E2E: git-commit block message is amend-aware (through bashTool.execute)
// ---------------------------------------------------------------------------
describe('bashTool – git commit block is amend-aware', () => {
  it('blocks a plain git commit without the amend note', async () => {
    const r = await bashTool.execute({ command: 'git commit -m "x"' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /Use the git_commit tool/);
    assert.doesNotMatch(r.output, /--amend/);
  });

  it('blocks git commit --amend with a dedicated amend note', async () => {
    const r = await bashTool.execute({ command: 'git commit --amend --no-edit' }, ctx());
    assert.equal(r.isError, true);
    assert.match(r.output, /Use the git_commit tool/);
    assert.match(r.output, /amend/i);
    assert.match(r.output, /rewrites the last commit/i);
  });
});

// ---------------------------------------------------------------------------
// dangerouslyDisableSandbox description surfaces the enforced-mode policy gate
// ---------------------------------------------------------------------------
describe('bashTool – dangerouslyDisableSandbox description wording', () => {
  it('states the flag is ignored in enforced container mode and needs confirmation', () => {
    const props = bashTool.inputSchema.properties as Record<string, { description?: string }>;
    const desc = props.dangerouslyDisableSandbox?.description ?? '';
    assert.match(desc, /ignored in enforced container-sandbox mode/i);
    assert.match(desc, /requires explicit confirmation/i);
  });
});
