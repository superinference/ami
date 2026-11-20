import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { askUserQuestionTool } from '../src/tools/ask-user';

// ---------------------------------------------------------------------------
// Richer question-field guidance (parity with the reference's AskUserQuestionTool
// questionSchema: concrete example + "end with a question mark" + multiSelect
// phrasing hint). Asserts the runtime tool object the model is actually shown,
// not source text.
// ---------------------------------------------------------------------------

describe('askUserQuestionTool – question/option field guidance', () => {
  const questions = askUserQuestionTool.inputSchema.properties.questions as any;
  const questionDesc: string = questions.items.properties.question.description;
  const optionDesc: string = questions.items.properties.options.items.properties.description.description;

  it('question field tells the model to end with a question mark', () => {
    assert.match(questionDesc, /end with a\s+question mark/i);
  });

  it('question field includes a concrete example question', () => {
    assert.match(questionDesc, /Example:/);
    assert.match(questionDesc, /\?"/); // the example is itself a question
  });

  it('question field gives a multiSelect phrasing hint', () => {
    assert.match(questionDesc, /multiSelect is true/i);
    assert.match(questionDesc, /phrase it accordingly/i);
  });

  it('option description field mentions trade-offs/implications context', () => {
    assert.match(optionDesc, /trade-offs or implications/i);
  });
});
