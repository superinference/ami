import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { askUserQuestionTool } from '../src/tools/ask-user';

// ---------------------------------------------------------------------------
// Rich preview-field usage guidance (parity with the reference's
// AskUserQuestionTool PREVIEW_FEATURE_PROMPT). The per-option `preview` field
// description must tell the model:
//   - what to use previews FOR (concrete artifacts: mockups / code / diagrams /
//     configuration variations),
//   - the side-by-side layout affordance (option list left, preview right),
//   - when NOT to use them (simple preference questions),
//   - the single-select-only restriction.
// Asserts the runtime tool object the model is actually shown, not source text.
// ---------------------------------------------------------------------------

describe('askUserQuestionTool – preview field usage guidance', () => {
  const questions = askUserQuestionTool.inputSchema.properties.questions as any;
  const previewDesc: string = questions.items.properties.options.items.properties.preview.description;

  it('restricts previews to single-select questions', () => {
    assert.match(previewDesc, /single-select\s+only/i);
  });

  it('describes the side-by-side layout affordance', () => {
    assert.match(previewDesc, /side-by-side/i);
    assert.match(previewDesc, /left/i);
    assert.match(previewDesc, /right/i);
  });

  it('lists concrete artifact use-cases', () => {
    assert.match(previewDesc, /mockups/i);
    assert.match(previewDesc, /code snippets/i);
  });

  it('tells the model when NOT to use previews', () => {
    assert.match(previewDesc, /do not|don't/i);
    assert.match(previewDesc, /simple preference/i);
  });
});
