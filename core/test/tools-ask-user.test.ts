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

  it('schema requires question and options', () => {
    const req = askUserQuestionTool.inputSchema.required;
    assert.ok(req?.includes('question'));
    assert.ok(req?.includes('options'));
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
