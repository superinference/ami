import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { categorizePrompt, PROMPT_INTENTS, PROMPT_SCOPES, selectTier, maxTier, PROMPT_TIERS, TIER_RANK, type Intent, type Scope, type PromptTier } from '../src/prompt-categorizer';

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

describe('PROMPT_INTENTS', () => {
  it('contains all 8 intents', () => {
    assert.equal(PROMPT_INTENTS.length, 8);
    for (const intent of ['explain', 'troubleshoot', 'generate', 'refactor', 'review', 'git_ops', 'research', 'other'] as const) {
      assert.ok(PROMPT_INTENTS.includes(intent), `missing intent: ${intent}`);
    }
  });

  it('every categorizePrompt result uses a value from PROMPT_INTENTS', () => {
    for (const input of ['explain this', 'fix the bug', 'create a file', 'do something']) {
      const result = categorizePrompt(input);
      assert.ok((PROMPT_INTENTS as readonly string[]).includes(result.intent));
    }
  });
});

describe('PROMPT_SCOPES', () => {
  it('contains all 4 scopes', () => {
    assert.equal(PROMPT_SCOPES.length, 4);
    for (const scope of ['selection', 'current_file', 'few_files', 'codebase'] as const) {
      assert.ok(PROMPT_SCOPES.includes(scope), `missing scope: ${scope}`);
    }
  });
});

// ---------------------------------------------------------------------------
// Tier selection
// ---------------------------------------------------------------------------
describe('selectTier', () => {
  const tier = (msg: string, sig = {}) => selectTier(msg, categorizePrompt(msg), sig);

  it('classifies a trivial chat question as conversational', () => {
    assert.equal(tier('what is 2+2'), 'conversational');
    assert.equal(tier('hello there'), 'conversational');
    assert.equal(tier('explain how promises work'), 'conversational');
    assert.equal(tier('why is the sky blue?'), 'conversational');
  });

  it('never goes conversational when context files are attached', () => {
    // non-work intent + files → standard (not conversational, not full)
    assert.equal(selectTier('what is 2+2', categorizePrompt('what is 2+2'), { hasContextFiles: true }), 'standard');
  });

  it('never goes conversational when the message carries code tokens', () => {
    assert.equal(tier('explain foo.ts'), 'standard');
    assert.equal(tier('what does getUserById do'), 'standard');           // camelCase
    assert.equal(tier('explain the user_id field'), 'standard');          // snake_case
    assert.equal(tier('what is `const x = 1`'), 'standard');              // inline code
    assert.equal(tier('explain src/app/main.py'), 'standard');           // path
    assert.equal(tier('what does render() return'), 'standard');         // call syntax
  });

  it('never goes conversational for work intents', () => {
    assert.equal(tier('fix the bug'), 'standard');
    assert.equal(tier('create a helper'), 'standard');
    assert.equal(tier('refactor this'), 'standard');
    assert.equal(tier('review the change'), 'standard');
    assert.equal(tier('commit the work'), 'standard');
  });

  it('does not go conversational for long messages even if chatty', () => {
    const long = 'explain ' + 'word '.repeat(40);
    assert.equal(tier(long), 'standard');
  });

  it('caps at standard (never full) on a tiny context window', () => {
    const msg = 'refactor the entire codebase everywhere';
    assert.equal(selectTier(msg, categorizePrompt(msg), { contextWindow: 8000 }), 'standard');
    // a chat turn on a tiny window stays conversational (even leaner, desirable)
    assert.equal(selectTier('what is 2+2', categorizePrompt('what is 2+2'), { contextWindow: 8000 }), 'conversational');
  });

  it('promotes to full on explicit breadth markers', () => {
    assert.equal(tier('refactor this across the codebase'), 'full');
    assert.equal(tier('migrate everything to the new API'), 'full');
    assert.equal(tier('update every file in the project'), 'full');
  });

  it('promotes to full for non-explain work on attached files', () => {
    assert.equal(selectTier('refactor this', categorizePrompt('refactor this'), { hasContextFiles: true }), 'full');
    // explain + files stays standard (reading, not broad editing)
    assert.equal(selectTier('explain this', categorizePrompt('explain this'), { hasContextFiles: true }), 'standard');
  });

  it('defaults ambiguous work to standard', () => {
    assert.equal(tier('add a test'), 'standard');
  });

  it('treats contextWindow <= 0 as unknown (not a tiny window)', () => {
    // contextWindow 0 fails the `> 0` guard → tinyWindow false → normal routing.
    assert.equal(selectTier('what is 2+2', categorizePrompt('what is 2+2'), { contextWindow: 0 }), 'conversational');
    assert.equal(selectTier('refactor across the codebase', categorizePrompt('refactor across the codebase'), { contextWindow: 0 }), 'full');
  });

  it('a large contextWindow is not a tiny window', () => {
    assert.equal(selectTier('refactor across the codebase', categorizePrompt('refactor across the codebase'), { contextWindow: 200000 }), 'full');
  });

  it('a very long (>280 char) chatty message is not conversational', () => {
    const longChat = 'why ' + 'really '.repeat(60) + 'though';
    assert.ok(longChat.length > 280);
    assert.equal(selectTier(longChat, categorizePrompt(longChat)), 'standard');
  });
});

describe('tier helpers', () => {
  it('PROMPT_TIERS and TIER_RANK are consistent and ordered', () => {
    assert.deepEqual([...PROMPT_TIERS], ['conversational', 'standard', 'full']);
    assert.ok(TIER_RANK.conversational < TIER_RANK.standard);
    assert.ok(TIER_RANK.standard < TIER_RANK.full);
  });

  it('maxTier returns the higher-capability tier (ratchet primitive)', () => {
    assert.equal(maxTier('conversational', 'standard'), 'standard');
    assert.equal(maxTier('full', 'conversational'), 'full');
    assert.equal(maxTier('standard', 'standard'), 'standard');
    assert.equal(maxTier('conversational', 'conversational'), 'conversational');
    const t: PromptTier = 'full';
    assert.equal(maxTier(t, 'full'), 'full');
  });
});
