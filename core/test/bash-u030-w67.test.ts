import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  bashTool,
  buildBashDescription,
} from '../src/tools/bash';
import {
  isGitInstructionsDisabled,
  DISABLE_GIT_INSTRUCTIONS_ENV,
} from '../src/tools/tool-gates';

// ---------------------------------------------------------------------------
// Unit U030 / wave w67 — two additive parity features on the bash tool:
//   A. AMI_DISABLE_GIT_INSTRUCTIONS knob: drop the git/commit sentence from the
//      bash description (parity: Claude's shouldIncludeGitInstructions() /
//      getCommitAndPRInstructions() gate in prompt.ts).
//   B. Affordance hints in the description: run `ls` to verify the parent dir
//      before creating files, and separate multiple commands with `&&`/`;` not
//      newlines (parity: Claude prompt.ts:L297-L302, L332).
// ---------------------------------------------------------------------------

// ---- Feature A: AMI_DISABLE_GIT_INSTRUCTIONS --------------------------------

describe('isGitInstructionsDisabled — env gate', () => {
  const SAVE = process.env[DISABLE_GIT_INSTRUCTIONS_ENV];
  afterEach(() => {
    if (SAVE === undefined) delete process.env[DISABLE_GIT_INSTRUCTIONS_ENV];
    else process.env[DISABLE_GIT_INSTRUCTIONS_ENV] = SAVE;
  });

  it('defaults to false (git guidance stays on)', () => {
    delete process.env[DISABLE_GIT_INSTRUCTIONS_ENV];
    assert.equal(isGitInstructionsDisabled(), false);
  });

  it('is true when the env var is truthy', () => {
    process.env[DISABLE_GIT_INSTRUCTIONS_ENV] = '1';
    assert.equal(isGitInstructionsDisabled(), true);
  });

  it('stays false for an explicit falsey token', () => {
    process.env[DISABLE_GIT_INSTRUCTIONS_ENV] = 'false';
    assert.equal(isGitInstructionsDisabled(), false);
  });
});

describe('buildBashDescription — git-instructions toggle', () => {
  it('includes the git/commit guidance by default', () => {
    const d = buildBashDescription(false);
    assert.match(d, /git_commit tool/);
    assert.match(d, /Do not sleep between commands\./);
  });

  it('drops the git sentence when gitDisabled but keeps the rest', () => {
    const d = buildBashDescription(false, true);
    assert.ok(!/git_commit tool/.test(d), d);
    assert.ok(!/Never amend/.test(d), d);
    assert.match(d, /Do not sleep between commands\./);
    assert.match(d, /Execute a bash command/);
  });
});

describe('bashTool.description reflects AMI_DISABLE_GIT_INSTRUCTIONS at advertise time', () => {
  const SAVE = process.env[DISABLE_GIT_INSTRUCTIONS_ENV];
  afterEach(() => {
    if (SAVE === undefined) delete process.env[DISABLE_GIT_INSTRUCTIONS_ENV];
    else process.env[DISABLE_GIT_INSTRUCTIONS_ENV] = SAVE;
  });

  it('shows git guidance by default', () => {
    delete process.env[DISABLE_GIT_INSTRUCTIONS_ENV];
    assert.match(bashTool.description, /git_commit tool/);
  });

  it('drops git guidance when the knob is set', () => {
    process.env[DISABLE_GIT_INSTRUCTIONS_ENV] = '1';
    assert.ok(!/git_commit tool/.test(bashTool.description));
  });
});

// ---- Feature B: affordance hints -------------------------------------------

describe('buildBashDescription — affordance hints', () => {
  it('tells the model to run `ls` to verify the parent directory before creating files', () => {
    const d = buildBashDescription(false);
    assert.match(d, /run `ls` to verify the parent directory exists/);
  });

  it('tells the model to separate multiple commands with && or ; (never newlines)', () => {
    const d = buildBashDescription(false);
    assert.match(d, /separate them with `&&`/);
    assert.match(d, /do NOT use newlines/);
  });
});
