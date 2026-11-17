/**
 * U021 session.ts parity addition — resume/session-picker "all projects" scope
 * toggle (parity: Claude Code's resume picker `showAllProjects`).
 *
 * `isResumeAllProjectsEnabled` is the env-knob predicate that flips the DEFAULT
 * `/sessions` + `/resume` scope from the current project to every project's store
 * under the global projects root. It reads the AMI knob `AMI_RESUME_ALL_PROJECTS`
 * and the Claude-compat alias `CLAUDE_CODE_SHOW_ALL_PROJECTS`, with the same
 * truthiness grammar as the other session env knobs. Pure: the env object is
 * injected so no global state is mutated.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { isResumeAllProjectsEnabled } from '../src/session';

describe('isResumeAllProjectsEnabled', () => {
  it('is false when neither knob is set', () => {
    assert.equal(isResumeAllProjectsEnabled({}), false);
  });

  it('is true for a truthy AMI_RESUME_ALL_PROJECTS', () => {
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: '1' }), true);
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: 'true' }), true);
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: 'on' }), true);
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: 'yes' }), true);
  });

  it('is true for the Claude-compat CLAUDE_CODE_SHOW_ALL_PROJECTS alias', () => {
    assert.equal(isResumeAllProjectsEnabled({ CLAUDE_CODE_SHOW_ALL_PROJECTS: 'true' }), true);
  });

  it('is false for an explicitly falsy value', () => {
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: '0' }), false);
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: 'false' }), false);
    assert.equal(isResumeAllProjectsEnabled({ AMI_RESUME_ALL_PROJECTS: '' }), false);
  });

  it('defaults to reading process.env when no arg is passed', () => {
    const saved = process.env.AMI_RESUME_ALL_PROJECTS;
    const savedAlias = process.env.CLAUDE_CODE_SHOW_ALL_PROJECTS;
    try {
      delete process.env.AMI_RESUME_ALL_PROJECTS;
      delete process.env.CLAUDE_CODE_SHOW_ALL_PROJECTS;
      assert.equal(isResumeAllProjectsEnabled(), false);
      process.env.AMI_RESUME_ALL_PROJECTS = '1';
      assert.equal(isResumeAllProjectsEnabled(), true);
    } finally {
      if (saved === undefined) delete process.env.AMI_RESUME_ALL_PROJECTS;
      else process.env.AMI_RESUME_ALL_PROJECTS = saved;
      if (savedAlias === undefined) delete process.env.CLAUDE_CODE_SHOW_ALL_PROJECTS;
      else process.env.CLAUDE_CODE_SHOW_ALL_PROJECTS = savedAlias;
    }
  });
});
