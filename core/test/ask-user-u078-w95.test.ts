import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  askUserQuestionTool,
  askUserToAutoClassifierInput,
} from '../src/tools/ask-user';
import { toolSearchTool, setSearchableTools } from '../src/tools/tool-search';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U078 (w95) — AskUserQuestion parity surfaces:
//   1. a dedicated `searchHint` for tool_search discovery (distinct from the long
//      DESCRIPTION), and
//   2. a `toAutoClassifierInput` projection (pipe-joined question texts) for the
//      auto-mode security classifier.
// Parity: the reference's AskUserQuestionTool `searchHint` (L111) and
// `toAutoClassifierInput` (L152-154).
// ---------------------------------------------------------------------------

describe('AskUserQuestion searchHint', () => {
  it('exposes a concise capability phrase distinct from the description', () => {
    assert.equal(
      askUserQuestionTool.searchHint,
      'prompt the user with a multiple-choice question',
    );
    // The hint must NOT just duplicate the long description.
    assert.notEqual(askUserQuestionTool.searchHint, askUserQuestionTool.description);
  });

  it('makes the tool discoverable by a hint-only term via tool_search (E2E)', async () => {
    // "prompt" appears ONLY in the searchHint, not in the name or the long
    // description — so a match proves the curated hint drove discovery.
    const term = 'prompt';
    assert.ok(!askUserQuestionTool.description.toLowerCase().includes(term));
    assert.ok(!askUserQuestionTool.name.toLowerCase().includes(term));

    setSearchableTools([askUserQuestionTool]);
    try {
      const ctx = {} as ToolContext;
      const res = await toolSearchTool.execute({ query: term }, ctx);
      assert.equal(res.isError, false);
      assert.ok(
        res.output.includes('AskUserQuestion'),
        `expected tool_search to surface AskUserQuestion via its searchHint, got: ${res.output}`,
      );
      const matches = (res.metadata as { matches?: string[] } | undefined)?.matches ?? [];
      assert.ok(matches.includes('AskUserQuestion'));
    } finally {
      setSearchableTools([]);
    }
  });
});

describe('AskUserQuestion toAutoClassifierInput', () => {
  it('is wired as the tool-definition hook', () => {
    assert.equal(typeof askUserQuestionTool.toAutoClassifierInput, 'function');
    assert.equal(askUserQuestionTool.toAutoClassifierInput, askUserToAutoClassifierInput);
  });

  it('pipe-joins the question texts of a multi-question input', () => {
    const input = {
      questions: [
        { question: 'Which DB?', options: [{ label: 'pg', description: 'x' }] },
        { question: 'Which ORM?', options: [{ label: 'prisma', description: 'y' }] },
      ],
    };
    assert.equal(
      askUserQuestionTool.toAutoClassifierInput?.(input),
      'Which DB? | Which ORM?',
    );
  });

  it('projects the single-question shorthand', () => {
    assert.equal(
      askUserToAutoClassifierInput({ question: 'Proceed?' }),
      'Proceed?',
    );
  });

  it('prefers the questions[] array over the shorthand when both are present', () => {
    assert.equal(
      askUserToAutoClassifierInput({
        question: 'ignored shorthand',
        questions: [{ question: 'real one' }],
      }),
      'real one',
    );
  });

  it('drops non-string / empty / blank question texts', () => {
    assert.equal(
      askUserToAutoClassifierInput({
        questions: [
          { question: 'Keep?' },
          { question: 42 },
          { question: '   ' },
          { question: '' },
          { noQuestion: true },
          null,
        ],
      }),
      'Keep?',
    );
  });

  it('returns "" (classifier skip) when there is no question text', () => {
    assert.equal(askUserToAutoClassifierInput({}), '');
    assert.equal(askUserToAutoClassifierInput({ questions: [] }), '');
    assert.equal(askUserToAutoClassifierInput({ question: 42 }), '');
    assert.equal(askUserToAutoClassifierInput({ question: '  ' }), '');
    assert.equal(askUserToAutoClassifierInput({ questions: 'nope' }), '');
  });
});
