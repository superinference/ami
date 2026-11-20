import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  validateAskUserQuestionInput,
  validateHtmlPreview,
  ASK_USER_UNIQUENESS_MESSAGE,
} from '../src/tools/ask-user';

// ---------------------------------------------------------------------------
// validateHtmlPreview — HTML-fragment safety (parity with the reference)
// ---------------------------------------------------------------------------

describe('validateHtmlPreview', () => {
  it('accepts an undefined preview (nothing to validate)', () => {
    assert.equal(validateHtmlPreview(undefined), null);
  });

  it('accepts a safe HTML fragment', () => {
    assert.equal(validateHtmlPreview('<div class="x">hi</div>'), null);
    assert.equal(validateHtmlPreview('<pre>code</pre>'), null);
  });

  it('rejects a full document (<html>/<body>/<!DOCTYPE>)', () => {
    assert.match(validateHtmlPreview('<html><body>x</body></html>')!, /HTML fragment/);
    assert.match(validateHtmlPreview('<!DOCTYPE html><div>x</div>')!, /HTML fragment/);
    assert.match(validateHtmlPreview('<  body >x</body>')!, /HTML fragment/);
  });

  it('rejects <script> and <style> tags', () => {
    assert.match(validateHtmlPreview('<div></div><script>alert(1)</script>')!, /<script> or <style>/);
    assert.match(validateHtmlPreview('<style>.x{}</style><p>y</p>')!, /<script> or <style>/);
  });

  it('rejects content with no tag at all', () => {
    assert.match(validateHtmlPreview('just plain text')!, /must contain HTML/);
  });
});

// ---------------------------------------------------------------------------
// validateAskUserQuestionInput — uniqueness + preview safety
// ---------------------------------------------------------------------------

describe('validateAskUserQuestionInput', () => {
  const okOptions = [
    { label: 'A', description: 'a' },
    { label: 'B', description: 'b' },
  ];

  it('accepts a valid multi-question input', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        { question: 'Q1?', options: okOptions },
        { question: 'Q2?', options: okOptions },
      ],
    });
    assert.equal(res, null);
  });

  it('accepts the single-question shorthand', () => {
    assert.equal(
      validateAskUserQuestionInput({ question: 'Pick?', options: okOptions }),
      null,
    );
  });

  it('rejects duplicate question texts', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        { question: 'Same?', options: okOptions },
        { question: 'Same?', options: okOptions },
      ],
    });
    assert.equal(res, ASK_USER_UNIQUENESS_MESSAGE);
  });

  it('rejects duplicate option labels within a question', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        {
          question: 'Pick?',
          options: [
            { label: 'Dup', description: 'first' },
            { label: 'Dup', description: 'second' },
          ],
        },
      ],
    });
    assert.equal(res, ASK_USER_UNIQUENESS_MESSAGE);
  });

  it('allows identical option labels ACROSS different questions', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        { question: 'Q1?', options: okOptions },
        { question: 'Q2?', options: okOptions },
      ],
    });
    assert.equal(res, null);
  });

  it('validates HTML previews only when previewFormat is "html"', () => {
    const badPreview = { label: 'A', description: 'a', preview: '<script>x</script><div>y</div>' };
    // Not validated when previewFormat is absent/markdown.
    assert.equal(
      validateAskUserQuestionInput({
        questions: [{ question: 'Q?', options: [badPreview, { label: 'B', description: 'b' }] }],
      }),
      null,
    );
    assert.equal(
      validateAskUserQuestionInput({
        questions: [{ question: 'Q?', previewFormat: 'markdown', options: [badPreview, { label: 'B', description: 'b' }] }],
      }),
      null,
    );
    // Validated (and rejected) when previewFormat is html.
    const res = validateAskUserQuestionInput({
      questions: [{ question: 'Q?', previewFormat: 'html', options: [badPreview, { label: 'B', description: 'b' }] }],
    });
    assert.ok(res);
    assert.match(res!, /Option "A" in question "Q\?": /);
    assert.match(res!, /<script> or <style>/);
  });

  it('accepts safe HTML previews under previewFormat html', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        {
          question: 'Q?',
          previewFormat: 'html',
          options: [
            { label: 'A', description: 'a', preview: '<div>ok</div>' },
            { label: 'B', description: 'b' },
          ],
        },
      ],
    });
    assert.equal(res, null);
  });

  it('ignores non-string questions/labels for uniqueness (no false dup)', () => {
    const res = validateAskUserQuestionInput({
      questions: [
        { options: okOptions },
        { options: okOptions },
      ],
    });
    assert.equal(res, null);
  });
});
