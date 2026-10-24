import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { askUserQuestionTool } from '../src/tools/ask-user';
import type { ToolContext } from '../src/types';

function ctx(overrides: Partial<ToolContext> = {}): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// execute — engine-handled stub must be a pure, input-independent passthrough.
// These assertions discriminate real behavior: if execute ever gained logic
// that branched on input or returned isError, they would fail (red-green).
// ---------------------------------------------------------------------------

describe('askUserQuestionTool – execute behavior (wave1)', () => {
  it('returns the exact engine-handoff sentinel regardless of input', async () => {
    const expected = 'AskUserQuestion is handled by the engine directly.';
    const multi = await askUserQuestionTool.execute(
      {
        questions: [
          {
            question: 'Pick',
            options: [
              { label: 'A', description: 'a' },
              { label: 'B', description: 'b' },
            ],
            multiSelect: true,
          },
        ],
      },
      ctx(),
    );
    assert.equal(multi.output, expected);
    assert.equal(multi.isError, false);
  });

  it('is idempotent — malformed / empty / rich inputs all yield identical output', async () => {
    const a = await askUserQuestionTool.execute({}, ctx());
    const b = await askUserQuestionTool.execute(
      { question: 'x', options: [], answers: { x: 'y' }, allowFreeText: false } as never,
      ctx(),
    );
    const c = await askUserQuestionTool.execute(undefined as never, ctx());
    assert.deepEqual(a, b);
    assert.deepEqual(b, c);
    assert.equal(a.isError, false);
  });

  it('does not inspect or honor the abort signal (engine owns cancellation)', async () => {
    const aborted = new AbortController();
    aborted.abort();
    const result = await askUserQuestionTool.execute({}, ctx({ abortSignal: aborted.signal }));
    // A stub must still resolve successfully even when the signal is aborted.
    assert.equal(result.isError, false);
    assert.ok(result.output.includes('engine'));
  });
});

// ---------------------------------------------------------------------------
// Schema contract — pin the parts not already asserted by tools-ask-user.test.ts
// (multiSelect default, option/preview shape, annotations, metadata, answers,
// and the empty top-level `required`). These guard against silent schema drift.
// ---------------------------------------------------------------------------

describe('askUserQuestionTool – schema contract (wave1)', () => {
  it('top-level required is explicitly empty (all fields optional)', () => {
    assert.deepEqual(askUserQuestionTool.inputSchema.required, []);
  });

  it('per-question option items require label + description and allow a preview', () => {
    const optItems =
      askUserQuestionTool.inputSchema.properties.questions.items.properties.options.items;
    assert.deepEqual(optItems.required, ['label', 'description']);
    assert.ok('preview' in optItems.properties);
    assert.equal(optItems.properties.preview.type, 'string');
  });

  it('multiSelect defaults to false', () => {
    const ms = askUserQuestionTool.inputSchema.properties.questions.items.properties.multiSelect;
    assert.equal(ms.type, 'boolean');
    assert.equal(ms.default, false);
  });

  it('answers is a string-valued map', () => {
    const answers = askUserQuestionTool.inputSchema.properties.answers;
    assert.equal(answers.type, 'object');
    assert.equal(answers.additionalProperties.type, 'string');
  });

  it('annotations carry notes + preview per question', () => {
    const ann = askUserQuestionTool.inputSchema.properties.annotations;
    assert.equal(ann.type, 'object');
    const shape = ann.additionalProperties.properties;
    assert.equal(shape.notes.type, 'string');
    assert.equal(shape.preview.type, 'string');
  });

  it('metadata exposes a source identifier field', () => {
    const meta = askUserQuestionTool.inputSchema.properties.metadata;
    assert.equal(meta.type, 'object');
    assert.equal(meta.properties.source.type, 'string');
  });
});
