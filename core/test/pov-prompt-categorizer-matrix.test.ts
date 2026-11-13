import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import {
  categorizePrompt,
  selectTier,
  maxTier,
  PROMPT_INTENTS,
  PROMPT_SCOPES,
  PROMPT_TIERS,
  TIER_RANK,
  type Intent,
  type Scope,
  type PromptTier,
  type PromptCategory,
  type TierSignals,
} from '../src/prompt-categorizer';

// ---------------------------------------------------------------------------
// POV: core/core/src/prompt-categorizer.ts — behavioral regression matrix (round 3).
//
// Rounds 1-2 and prompt-categorizer.test.ts cover the happy-path keyword hits.
// This file locks the CONTRACT at its boundaries by driving the real functions:
//   * exhaustive intent/scope keyword matrix with EVERY documented alternative,
//   * first-match-wins PRECEDENCE across the real pattern ordering,
//   * selectTier's three classification boundaries (conversational/standard/full)
//     computed from first principles — each code-token family, each breadth
//     alternative, the exact context-window threshold, and the isShort char/word
//     edges — never from reading the source, always from the returned value,
//   * TIER_RANK / maxTier ordering as an exhaustive 3x3.
//
// Expected values are hand-derived from the documented spec (conservative tiering:
// default UP to standard; drop to conversational only for short, code-free,
// explain/other chat; promote to full only on breadth or work-on-attached-files,
// never full on a <32768 window) and asserted against the real output.
// ---------------------------------------------------------------------------

// =====================================================================
// 1. Intent keyword matrix — every alternative in INTENT_PATTERNS order.
// =====================================================================
describe('categorizePrompt — intent keyword matrix', () => {
  // Each phrase isolates ONE keyword and avoids any higher-priority keyword,
  // so the first-match-wins loop must land on the stated intent.
  const cases: Array<[string, Intent]> = [
    // explain (index 0)
    ['explain the layout', 'explain'],
    ['what does the loop do', 'explain'],
    ['how does the scheduler run', 'explain'],
    ['why is it slow', 'explain'],
    ['help me understand it', 'explain'],
    // troubleshoot (index 1)
    ['fix the login', 'troubleshoot'],
    ['there is a bug here', 'troubleshoot'],
    ['an error appears', 'troubleshoot'],
    ['the pipe is broken', 'troubleshoot'],
    ['it will crash', 'troubleshoot'],
    ['the tests fail', 'troubleshoot'],
    ['the toggle is not working', 'troubleshoot'],
    // generate (index 2)
    ['create a module', 'generate'],
    ['generate the tests', 'generate'],
    ['write a parser', 'generate'],
    ['build the endpoint', 'generate'],
    ['implement the guard', 'generate'],
    ['add a spinner', 'generate'],
    ['new theme please', 'generate'],
    // refactor (index 3)
    ['refactor the layer', 'refactor'],
    ['clean the helpers', 'refactor'],
    ['improve the structure', 'refactor'],
    ['optimize the loop', 'refactor'],
    ['simplify the routing', 'refactor'],
    // review (index 4)
    ['review the diff', 'review'],
    ['check the config', 'review'],
    ['audit the deps', 'review'],
    ['evaluate the design', 'review'],
    ['assess the coverage', 'review'],
    // git_ops (index 5)
    ['commit the work', 'git_ops'],
    ['push the tag', 'git_ops'],
    ['pull the latest', 'git_ops'],
    ['merge the branch', 'git_ops'],
    ['switch the branch', 'git_ops'],
    ['rebase onto trunk', 'git_ops'],
    ['run git status', 'git_ops'],
    // research (index 6)
    ['research the options', 'research'],
    ['analyze the trace', 'research'],
    ['compare the frameworks', 'research'],
    ['study the guide', 'research'],
    ['investigate the leak', 'research'],
    ['read the paper', 'research'],
  ];

  for (const [msg, expected] of cases) {
    it(`"${msg}" -> ${expected}`, () => {
      assert.equal(categorizePrompt(msg).intent, expected);
    });
  }

  it('falls back to "other" when no keyword matches', () => {
    for (const msg of ['hello there', 'thanks a lot', 'good morning', '']) {
      assert.equal(categorizePrompt(msg).intent, 'other');
    }
  });
});

// =====================================================================
// 2. Word-boundary negatives — \b means plural/substring forms miss.
// =====================================================================
describe('categorizePrompt — word-boundary negatives', () => {
  it('"crashes" does not match \\bcrash\\b', () => {
    assert.equal(categorizePrompt('the server crashes').intent, 'other');
  });

  it('"branches" does not match \\bbranch\\b as git_ops on its own', () => {
    // "branches" is not \bbranch\b; with no other keyword this is 'other'.
    assert.equal(categorizePrompt('the tree branches out').intent, 'other');
  });

  it('a bare substring inside a word does not trigger the intent', () => {
    // "addendum" contains "add" but \badd\b requires a boundary.
    assert.equal(categorizePrompt('the addendum section').intent, 'other');
  });
});

// =====================================================================
// 3. Intent precedence — first pattern in list order wins on co-occurrence.
// =====================================================================
describe('categorizePrompt — intent precedence (first match wins)', () => {
  const cases: Array<[string, Intent, string]> = [
    // explain(0) beats troubleshoot(1)
    ['explain why it has an error', 'explain', 'explain < troubleshoot'],
    // troubleshoot(1) beats generate(2)
    ['add a fix for the crash', 'troubleshoot', 'troubleshoot < generate'],
    // troubleshoot(1) beats refactor(3): "improve" + "error"
    ['improve the error handling', 'troubleshoot', 'troubleshoot < refactor'],
    // generate(2) beats git_ops(5): "create" + "branch"
    ['create a new branch', 'generate', 'generate < git_ops'],
    // generate(2) beats review(4): "add" + "review"
    ['add a review step', 'generate', 'generate < review'],
    // refactor(3) beats review(4): "clean" + "check"
    ['clean up and check it', 'refactor', 'refactor < review'],
    // review(4) beats git_ops(5): "review" + "commit"
    ['review the commit', 'review', 'review < git_ops'],
    // git_ops(5) beats research(6): "merge" + "compare"
    ['merge then compare', 'git_ops', 'git_ops < research'],
  ];

  for (const [msg, expected, label] of cases) {
    it(`${label}: "${msg}" -> ${expected}`, () => {
      assert.equal(categorizePrompt(msg).intent, expected);
    });
  }
});

// =====================================================================
// 4. Scope keyword matrix — every alternative in SCOPE_PATTERNS order.
// =====================================================================
describe('categorizePrompt — scope keyword matrix', () => {
  const cases: Array<[string, Scope]> = [
    // selection (index 0)
    ['explain this function', 'selection'],
    ['refactor this method', 'selection'],
    ['optimize this block', 'selection'],
    ['explain the selected part', 'selection'],
    ['what does the code above do', 'selection'],
    // current_file (index 1)
    ['review this file', 'current_file'],
    ['optimize the current file', 'current_file'],
    // few_files (index 2)
    ['refactor these files', 'few_files'],
    ['compare both files', 'few_files'],
    ['check all files in that dir', 'few_files'],
    // codebase (index 3)
    ['search the codebase', 'codebase'],
    ['scan the project', 'codebase'],
    ['audit the repo', 'codebase'],
    ['scan the repository', 'codebase'],
    ['rename everywhere', 'codebase'],
    ['fix all of it', 'codebase'],
  ];

  for (const [msg, expected] of cases) {
    it(`"${msg}" -> ${expected}`, () => {
      assert.equal(categorizePrompt(msg).scope, expected);
    });
  }

  it('defaults to "codebase" when no scope keyword is present', () => {
    for (const msg of ['hello', 'do the thing', '']) {
      assert.equal(categorizePrompt(msg).scope, 'codebase');
    }
  });
});

// =====================================================================
// 5. Scope precedence — first pattern in list order wins.
// =====================================================================
describe('categorizePrompt — scope precedence (first match wins)', () => {
  const cases: Array<[string, Scope, string]> = [
    // selection(0) beats codebase(3)
    ['explain this function in the codebase', 'selection', 'selection < codebase'],
    // current_file(1) beats codebase(3)
    ['change this file across the project', 'current_file', 'current_file < codebase'],
    // current_file(1) beats few_files(2): "all files in" also present but
    // "this file" matches earlier in the ordered list.
    ['look at all files in this file', 'current_file', 'current_file < few_files'],
    // few_files(2) beats codebase(3): "these files" + "project"
    ['refactor these files in the project', 'few_files', 'few_files < codebase'],
    // selection(0) beats current_file(1)
    ['this function in this file', 'selection', 'selection < current_file'],
  ];

  for (const [msg, expected, label] of cases) {
    it(`${label}: "${msg}" -> ${expected}`, () => {
      assert.equal(categorizePrompt(msg).scope, expected);
    });
  }
});

// =====================================================================
// 6. Range invariant — every result uses only declared categories.
// =====================================================================
describe('categorizePrompt — codomain invariant', () => {
  const probes = [
    '', 'hello', 'explain this function', 'fix all bugs in the repo',
    'commit these files', 'research the codebase', 'refactor this method',
    'build a new thing', 'audit the current file', 'why does this fail',
  ];

  it('intent is always a member of PROMPT_INTENTS', () => {
    for (const p of probes) {
      assert.ok((PROMPT_INTENTS as readonly string[]).includes(categorizePrompt(p).intent), p);
    }
  });

  it('scope is always a member of PROMPT_SCOPES', () => {
    for (const p of probes) {
      assert.ok((PROMPT_SCOPES as readonly string[]).includes(categorizePrompt(p).scope), p);
    }
  });

  it('PROMPT_INTENTS / PROMPT_SCOPES hold the documented members', () => {
    assert.deepEqual([...PROMPT_INTENTS], ['explain', 'troubleshoot', 'generate', 'refactor', 'review', 'git_ops', 'research', 'other']);
    assert.deepEqual([...PROMPT_SCOPES], ['selection', 'current_file', 'few_files', 'codebase']);
  });
});

// =====================================================================
// 7. selectTier — conversational boundary (explain/other, short, code-free).
// =====================================================================
describe('selectTier — conversational classification', () => {
  const cat = (intent: Intent): PromptCategory => ({ intent, scope: 'codebase' });

  it('short code-free explain/other chat -> conversational', () => {
    assert.equal(selectTier('hello there friend', cat('other')), 'conversational');
    assert.equal(selectTier('why is it so', cat('explain')), 'conversational');
    assert.equal(selectTier('what is the plan', cat('other')), 'conversational');
  });

  it('work intents are NEVER conversational even when short/code-free', () => {
    for (const intent of ['troubleshoot', 'generate', 'refactor', 'review', 'git_ops'] as Intent[]) {
      assert.equal(selectTier('do it now', cat(intent)), 'standard', intent);
    }
  });

  it('research intent is not conversational (short, code-free)', () => {
    // research is neither a CONVERSATIONAL_INTENT nor a WORK_INTENT -> standard.
    assert.equal(selectTier('look into it', cat('research')), 'standard');
  });

  it('empty message with other intent -> conversational', () => {
    assert.equal(selectTier('', cat('other')), 'conversational');
  });
});

// =====================================================================
// 8. selectTier — each CODE_TOKEN family forces non-conversational.
// =====================================================================
describe('selectTier — code tokens defeat conversational', () => {
  const cat: PromptCategory = { intent: 'explain', scope: 'codebase' };

  // Each probe is short + explain intent so it WOULD be conversational except
  // for the single code-token family it carries.
  const probes: Array<[string, string]> = [
    ['look at ```x``` here', 'fenced code block'],
    ['look at `x` here', 'inline code span'],
    ['look at notes.ts here', 'known extension'],
    ['look at /a/b here', 'path separators'],
    ['look at render() here', 'function call'],
    ['look at user_id here', 'snake_case'],
    ['look at fooBar here', 'camelCase'],
    ['look at a.b.c here', 'dotted path'],
    ['look at a => b here', 'arrow operator'],
    ['look at a && b here', 'logical-and operator'],
    ['look at <div> here', 'tag'],
  ];

  for (const [msg, label] of probes) {
    it(`${label}: "${msg}" -> standard (not conversational)`, () => {
      assert.equal(selectTier(msg, cat), 'standard');
    });
  }

  it('hasContextFiles alone (non-work intent) forces standard', () => {
    const sig: TierSignals = { hasContextFiles: true };
    assert.equal(selectTier('why though', cat, sig), 'standard');
  });
});

// =====================================================================
// 9. selectTier — isShort boundary (char length and word count edges).
// =====================================================================
describe('selectTier — isShort boundary', () => {
  const cat: PromptCategory = { intent: 'other', scope: 'codebase' };

  it('a single 280-char word is short -> conversational', () => {
    const msg = 'a'.repeat(280);
    assert.equal(msg.length, 280);
    assert.equal(selectTier(msg, cat), 'conversational');
  });

  it('a single 281-char word is NOT short -> standard', () => {
    const msg = 'a'.repeat(281);
    assert.equal(msg.length, 281);
    assert.equal(selectTier(msg, cat), 'standard');
  });

  it('exactly 30 words (<=280 chars) is short -> conversational', () => {
    const msg = Array(30).fill('ok').join(' '); // 30 words, 89 chars
    assert.equal(msg.split(/\s+/).filter(Boolean).length, 30);
    assert.ok(msg.length <= 280);
    assert.equal(selectTier(msg, cat), 'conversational');
  });

  it('31 words is NOT short -> standard', () => {
    const msg = Array(31).fill('ok').join(' ');
    assert.equal(msg.split(/\s+/).filter(Boolean).length, 31);
    assert.equal(selectTier(msg, cat), 'standard');
  });
});

// =====================================================================
// 10. selectTier — breadth markers promote to full (each alternative).
// =====================================================================
describe('selectTier — breadth markers promote to full', () => {
  // Breadth takes precedence over the conversational short-circuit, so even
  // an 'other'/'explain' intent with breadth lands on full.
  const cat: PromptCategory = { intent: 'other', scope: 'codebase' };
  const probes = [
    'touch the codebase',
    'change the whole repo',
    'update the entire repo',
    'update the entire project',
    'update the entire codebase',
    'rename everywhere now',
    'edit all files today',
    'touch every file now',
    'walk across the repo',
    'walk across the codebase',
    'walk across the project',
    'do a project-wide change',
    'migrate to the new API',
    'plan the migration',
  ];

  for (const msg of probes) {
    it(`"${msg}" -> full`, () => {
      assert.equal(selectTier(msg, cat), 'full');
    });
  }

  it('breadth + work-on-attached-files still -> full', () => {
    const sig: TierSignals = { hasContextFiles: true };
    assert.equal(selectTier('migrate everything', { intent: 'refactor', scope: 'codebase' }, sig), 'full');
  });
});

// =====================================================================
// 11. selectTier — work intent on attached files promotes to full.
// =====================================================================
describe('selectTier — hasContextFiles + WORK intent -> full', () => {
  const sig: TierSignals = { hasContextFiles: true };

  it('every WORK intent with attached files (no breadth) -> full', () => {
    for (const intent of ['troubleshoot', 'generate', 'refactor', 'review', 'git_ops'] as Intent[]) {
      assert.equal(selectTier('do this', { intent, scope: 'current_file' }, sig), 'full', intent);
    }
  });

  it('non-work intents with attached files stay standard (reading, not editing)', () => {
    for (const intent of ['explain', 'research', 'other'] as Intent[]) {
      assert.equal(selectTier('look', { intent, scope: 'current_file' }, sig), 'standard', intent);
    }
  });
});

// =====================================================================
// 12. selectTier — contextWindow threshold is exactly 32768.
// =====================================================================
describe('selectTier — tiny context window caps at standard', () => {
  const heavyCat: PromptCategory = { intent: 'refactor', scope: 'codebase' };
  const heavyMsg = 'refactor across the codebase';
  const chatCat: PromptCategory = { intent: 'other', scope: 'codebase' };

  it('window 32767 is tiny -> heavy work capped at standard', () => {
    assert.equal(selectTier(heavyMsg, heavyCat, { contextWindow: 32767 }), 'standard');
  });

  it('window 32768 is NOT tiny -> heavy work reaches full', () => {
    assert.equal(selectTier(heavyMsg, heavyCat, { contextWindow: 32768 }), 'full');
  });

  it('window 0 is treated as unknown (not tiny) -> full', () => {
    assert.equal(selectTier(heavyMsg, heavyCat, { contextWindow: 0 }), 'full');
  });

  it('negative window is treated as unknown (not tiny) -> full', () => {
    assert.equal(selectTier(heavyMsg, heavyCat, { contextWindow: -1 }), 'full');
  });

  it('large window -> full', () => {
    assert.equal(selectTier(heavyMsg, heavyCat, { contextWindow: 200000 }), 'full');
  });

  it('tiny window still allows the even-leaner conversational tier', () => {
    assert.equal(selectTier('what is up', chatCat, { contextWindow: 16000 }), 'conversational');
  });
});

// =====================================================================
// 13. selectTier — default routing + codomain invariant.
// =====================================================================
describe('selectTier — default + codomain', () => {
  it('ambiguous short work (no breadth, no files) defaults to standard', () => {
    assert.equal(selectTier('add a test', { intent: 'generate', scope: 'codebase' }, {}), 'standard');
  });

  it('result is always a member of PROMPT_TIERS across a mixed matrix', () => {
    const msgs = ['hi', 'refactor everything in the repo', 'fix render() now', 'explain it', Array(40).fill('x').join(' ')];
    const intents: Intent[] = ['explain', 'other', 'generate', 'refactor', 'review', 'git_ops', 'troubleshoot', 'research'];
    const sigs: TierSignals[] = [{}, { hasContextFiles: true }, { contextWindow: 8000 }, { contextWindow: 128000 }];
    for (const m of msgs) {
      for (const intent of intents) {
        for (const sig of sigs) {
          const t = selectTier(m, { intent, scope: 'codebase' }, sig);
          assert.ok((PROMPT_TIERS as readonly string[]).includes(t), `${m}|${intent}`);
        }
      }
    }
  });
});

// =====================================================================
// 14. Tier helpers — TIER_RANK ordering + maxTier exhaustive 3x3.
// =====================================================================
describe('tier helpers — TIER_RANK and maxTier', () => {
  it('PROMPT_TIERS is ordered ascending by TIER_RANK', () => {
    assert.deepEqual([...PROMPT_TIERS], ['conversational', 'standard', 'full']);
    assert.equal(TIER_RANK.conversational, 0);
    assert.equal(TIER_RANK.standard, 1);
    assert.equal(TIER_RANK.full, 2);
    assert.ok(TIER_RANK.conversational < TIER_RANK.standard);
    assert.ok(TIER_RANK.standard < TIER_RANK.full);
  });

  it('maxTier returns the higher-ranked tier for all 9 ordered pairs', () => {
    for (const a of PROMPT_TIERS) {
      for (const b of PROMPT_TIERS) {
        // Spec: the tier with the greater TIER_RANK; ties return the first arg.
        const expected: PromptTier = TIER_RANK[a] >= TIER_RANK[b] ? a : b;
        assert.equal(maxTier(a, b), expected, `maxTier(${a}, ${b})`);
      }
    }
  });

  it('maxTier is commutative in capability (same rank result regardless of order)', () => {
    for (const a of PROMPT_TIERS) {
      for (const b of PROMPT_TIERS) {
        assert.equal(TIER_RANK[maxTier(a, b)], TIER_RANK[maxTier(b, a)], `${a},${b}`);
      }
    }
  });
});
