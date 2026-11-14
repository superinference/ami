import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { askUserQuestionTool } from '../src/tools/ask-user';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('askUserQuestionTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(askUserQuestionTool.name, 'AskUserQuestion');
  });

  it('is read-only', () => {
    assert.equal(askUserQuestionTool.isReadOnly, true);
  });

  it('has a description', () => {
    assert.ok(askUserQuestionTool.description.length > 0);
  });

  it('schema supports both single-question and multi-question formats', () => {
    const props = askUserQuestionTool.inputSchema.properties;
    assert.ok('questions' in props, 'supports multi-question array');
    assert.ok('question' in props, 'supports single-question shorthand');
    const questionsItems = props.questions.items;
    assert.ok(questionsItems.required?.includes('question'));
    assert.ok(questionsItems.required?.includes('options'));
  });

  it('schema includes question, options, and allowFreeText properties', () => {
    const props = askUserQuestionTool.inputSchema.properties;
    assert.ok('question' in props);
    assert.ok('options' in props);
    assert.ok('allowFreeText' in props);
  });

  it('options is an array type with items', () => {
    const optionsProp = askUserQuestionTool.inputSchema.properties.options;
    assert.equal(optionsProp.type, 'array');
    assert.ok(optionsProp.items !== undefined);
  });

  it('allowFreeText is a boolean', () => {
    const prop = askUserQuestionTool.inputSchema.properties.allowFreeText;
    assert.equal(prop.type, 'boolean');
  });

  it('enforces cardinality bounds in the schema (1-4 questions, 2-4 options)', () => {
    const q = askUserQuestionTool.inputSchema.properties.questions as any;
    assert.equal(q.minItems, 1);
    assert.equal(q.maxItems, 4);
    assert.equal(q.items.properties.options.minItems, 2);
    assert.equal(q.items.properties.options.maxItems, 4);
    assert.equal(q.items.properties.header.maxLength, 12);
  });

  it('options carry a stable machine key distinct from the label', () => {
    const q = askUserQuestionTool.inputSchema.properties.questions as any;
    assert.ok(q.items.properties.options.items.properties.key);
  });

  it('question supports a previewFormat enum (markdown|html)', () => {
    const q = askUserQuestionTool.inputSchema.properties.questions as any;
    assert.deepEqual(q.items.properties.previewFormat.enum, ['markdown', 'html']);
  });

  it('description carries recommended-first, auto-Other, and preview constraints', () => {
    const d = askUserQuestionTool.description;
    assert.match(d, /\(Recommended\)/);
    assert.match(d, /"Other"\/free-text choice is provided/);
    assert.match(d, /single-select questions only/);
  });
});

// ---------------------------------------------------------------------------
// Execute (engine-handled stub)
// ---------------------------------------------------------------------------

describe('askUserQuestionTool – execute', () => {
  it('returns a non-error message indicating engine handling', async () => {
    const result = await askUserQuestionTool.execute(
      {
        question: 'Which option?',
        options: [
          { label: 'A', description: 'Option A' },
          { label: 'B', description: 'Option B' },
        ],
      },
      ctx(),
    );
    assert.ok(!result.isError);
    assert.ok(result.output.includes('engine'));
  });

  it('works with various input shapes', async () => {
    const result = await askUserQuestionTool.execute({}, ctx());
    assert.ok(!result.isError);
    // The execute function is a stub — it always returns the same message
  });
});
