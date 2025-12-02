import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { categorizePrompt, type Intent, type Scope } from '../src/prompt-categorizer';

// ---------------------------------------------------------------------------
// Intent detection
// ---------------------------------------------------------------------------
describe('categorizePrompt — intent', () => {
  // explain
  it('detects "explain" intent', () => {
    assert.equal(categorizePrompt('explain this code').intent, 'explain');
  });

  it('detects "what does" intent', () => {
    assert.equal(categorizePrompt('what does this function do?').intent, 'explain');
  });

  it('detects "how does" intent', () => {
    assert.equal(categorizePrompt('how does the parser work?').intent, 'explain');
  });

  it('detects "why" intent', () => {
    assert.equal(categorizePrompt('why is this variable used?').intent, 'explain');
  });

  it('detects "understand" intent', () => {
    assert.equal(categorizePrompt('help me understand the architecture').intent, 'explain');
  });

  // troubleshoot
  it('detects "fix" intent', () => {
    assert.equal(categorizePrompt('fix this bug').intent, 'troubleshoot');
  });

  it('detects "bug" intent', () => {
    assert.equal(categorizePrompt('there is a bug in the login flow').intent, 'troubleshoot');
  });

  it('detects "error" intent', () => {
    assert.equal(categorizePrompt('I get an error when running tests').intent, 'troubleshoot');
  });

  it('detects "broken" intent', () => {
    assert.equal(categorizePrompt('the build is broken').intent, 'troubleshoot');
  });

  it('detects "crash" intent (exact word)', () => {
    assert.equal(categorizePrompt('the app crash on startup').intent, 'troubleshoot');
  });

  it('"crashes" does not match troubleshoot (word boundary)', () => {
    // "crashes" does not match \bcrash\b — this is expected behavior
    assert.equal(categorizePrompt('the app crashes on startup').intent, 'other');
  });

  it('detects "fail" intent', () => {
    assert.equal(categorizePrompt('tests fail intermittently').intent, 'troubleshoot');
  });

  it('detects "not working" intent', () => {
    assert.equal(categorizePrompt('the button is not working').intent, 'troubleshoot');
  });

  // generate
  it('detects "create" intent', () => {
    assert.equal(categorizePrompt('create a new React component').intent, 'generate');
  });

  it('detects "generate" intent', () => {
    assert.equal(categorizePrompt('generate unit tests').intent, 'generate');
  });

  it('detects "write" intent', () => {
    assert.equal(categorizePrompt('write a function to parse CSV').intent, 'generate');
  });

  it('detects "build" intent', () => {
    assert.equal(categorizePrompt('build a REST API endpoint').intent, 'generate');
  });

  it('detects "implement" intent', () => {
    assert.equal(categorizePrompt('implement the auth middleware').intent, 'generate');
  });

  it('detects "add" intent', () => {
    assert.equal(categorizePrompt('add a loading spinner').intent, 'generate');
  });

  it('detects "new" intent', () => {
    assert.equal(categorizePrompt('new feature: dark mode').intent, 'generate');
  });

  // refactor
  it('detects "refactor" intent', () => {
    assert.equal(categorizePrompt('refactor the database layer').intent, 'refactor');
  });

  it('detects "clean" intent', () => {
    assert.equal(categorizePrompt('clean up the utility functions').intent, 'refactor');
  });

  it('detects "improve" intent (when no troubleshoot keyword present)', () => {
    assert.equal(categorizePrompt('improve the code structure').intent, 'refactor');
  });

  it('"improve the error handling" matches troubleshoot first (error keyword)', () => {
    // "error" appears and troubleshoot patterns are checked before refactor
    assert.equal(categorizePrompt('improve the error handling').intent, 'troubleshoot');
  });

  it('detects "optimize" intent', () => {
    assert.equal(categorizePrompt('optimize the query performance').intent, 'refactor');
  });

  it('detects "simplify" intent', () => {
    assert.equal(categorizePrompt('simplify the routing logic').intent, 'refactor');
  });

  // review
  it('detects "review" intent', () => {
    assert.equal(categorizePrompt('review my pull request').intent, 'review');
  });

  it('detects "check" intent', () => {
    assert.equal(categorizePrompt('check for security issues').intent, 'review');
  });

  it('detects "audit" intent', () => {
    assert.equal(categorizePrompt('audit the dependencies').intent, 'review');
  });

  it('detects "evaluate" intent', () => {
    assert.equal(categorizePrompt('evaluate the architecture').intent, 'review');
  });

  it('detects "assess" intent', () => {
    assert.equal(categorizePrompt('assess the test coverage').intent, 'review');
  });

  // git_ops
  it('detects "commit" intent', () => {
    assert.equal(categorizePrompt('commit the changes').intent, 'git_ops');
  });

  it('detects "push" intent', () => {
    assert.equal(categorizePrompt('push to origin').intent, 'git_ops');
  });

  it('detects "pull" intent', () => {
    assert.equal(categorizePrompt('pull the latest changes').intent, 'git_ops');
  });

  it('detects "merge" intent', () => {
    assert.equal(categorizePrompt('merge feature branch').intent, 'git_ops');
  });

  it('detects "branch" intent (without generate keywords)', () => {
    assert.equal(categorizePrompt('switch to the feature branch').intent, 'git_ops');
  });

  it('"create a new branch" matches generate first (create keyword)', () => {
    // "create" is checked before "branch" in INTENT_PATTERNS
    assert.equal(categorizePrompt('create a new branch').intent, 'generate');
  });

  it('detects "rebase" intent', () => {
    assert.equal(categorizePrompt('rebase onto main').intent, 'git_ops');
  });

  it('detects "git" intent', () => {
    assert.equal(categorizePrompt('run git status').intent, 'git_ops');
  });

  // research
  it('detects "research" intent', () => {
    assert.equal(categorizePrompt('research best practices for auth').intent, 'research');
  });

  it('detects "analyze" intent', () => {
    assert.equal(categorizePrompt('analyze the performance bottleneck').intent, 'research');
  });

  it('detects "compare" intent', () => {
    assert.equal(categorizePrompt('compare React and Vue').intent, 'research');
  });

  it('detects "study" intent', () => {
    assert.equal(categorizePrompt('study the migration guide').intent, 'research');
  });

  it('detects "investigate" intent', () => {
    assert.equal(categorizePrompt('investigate the memory leak').intent, 'research');
  });

  it('detects "paper" intent', () => {
    assert.equal(categorizePrompt('read the paper on attention mechanisms').intent, 'research');
  });

  // other (fallback)
  it('returns "other" for unrecognized intents', () => {
    assert.equal(categorizePrompt('hello there').intent, 'other');
  });

  it('returns "other" for empty string', () => {
    assert.equal(categorizePrompt('').intent, 'other');
  });

  // Priority: first match wins
  it('first matching intent wins when multiple keywords present', () => {
    // "explain" comes before "fix" in the pattern list
    const result = categorizePrompt('explain how to fix this');
    assert.equal(result.intent, 'explain');
  });
});

// ---------------------------------------------------------------------------
// Scope detection
// ---------------------------------------------------------------------------
describe('categorizePrompt — scope', () => {
  // selection
  it('detects "this function" scope as selection', () => {
    assert.equal(categorizePrompt('explain this function').scope, 'selection');
  });

  it('detects "this method" scope as selection', () => {
    assert.equal(categorizePrompt('refactor this method').scope, 'selection');
  });

  it('detects "this block" scope as selection', () => {
    assert.equal(categorizePrompt('optimize this block of code').scope, 'selection');
  });

  it('detects "selected" scope as selection', () => {
    assert.equal(categorizePrompt('explain the selected code').scope, 'selection');
  });

  it('detects "the code above" scope as selection', () => {
    assert.equal(categorizePrompt('what does the code above do?').scope, 'selection');
  });

  // current_file
  it('detects "this file" scope as current_file', () => {
    assert.equal(categorizePrompt('review this file').scope, 'current_file');
  });

  it('detects "current file" scope as current_file', () => {
    assert.equal(categorizePrompt('optimize the current file').scope, 'current_file');
  });

  // few_files
  it('detects "these files" scope as few_files', () => {
    assert.equal(categorizePrompt('refactor these files').scope, 'few_files');
  });

  it('detects "both files" scope as few_files', () => {
    assert.equal(categorizePrompt('compare both files').scope, 'few_files');
  });

  it('detects "all files in" scope as few_files', () => {
    assert.equal(categorizePrompt('check all files in src/').scope, 'few_files');
  });

  // codebase
  it('detects "codebase" scope as codebase', () => {
    assert.equal(categorizePrompt('search the codebase for unused imports').scope, 'codebase');
  });

  it('detects "project" scope as codebase', () => {
    assert.equal(categorizePrompt('add tests for the project').scope, 'codebase');
  });

  it('detects "repo" scope as codebase', () => {
    assert.equal(categorizePrompt('audit the repo for secrets').scope, 'codebase');
  });

  it('detects "repository" scope as codebase', () => {
    assert.equal(categorizePrompt('scan the repository').scope, 'codebase');
  });

  it('detects "everywhere" scope as codebase', () => {
    assert.equal(categorizePrompt('rename everywhere').scope, 'codebase');
  });

  it('detects "all" scope as codebase', () => {
    assert.equal(categorizePrompt('fix all linting errors').scope, 'codebase');
  });

  // default scope is codebase
  it('defaults to "codebase" scope when no scope keyword found', () => {
    assert.equal(categorizePrompt('hello').scope, 'codebase');
  });

  // Priority: first scope match wins
  it('first matching scope wins', () => {
    // "this function" (selection) comes before "codebase" patterns
    const result = categorizePrompt('explain this function in the codebase');
    assert.equal(result.scope, 'selection');
  });
});

// ---------------------------------------------------------------------------
// Combined intent + scope
// ---------------------------------------------------------------------------
describe('categorizePrompt — combined', () => {
  it('returns correct intent and scope together', () => {
    const result = categorizePrompt('refactor this function');
    assert.equal(result.intent, 'refactor');
    assert.equal(result.scope, 'selection');
  });

  it('returns other/codebase for ambiguous input', () => {
    const result = categorizePrompt('do something');
    assert.equal(result.intent, 'other');
    assert.equal(result.scope, 'codebase');
  });
});
