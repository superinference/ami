import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  IMAGE_MEDIA_TYPES,
  isSupportedImageMediaType,
} from '../src/types';
import type {
  ImageMediaType,
  RetryErrorKind,
  UserQuestionAnnotation,
  EngineEvent,
} from '../src/types';

// ---------------------------------------------------------------------------
// U006 / w91 — core type building blocks:
//   (gap D) a closed allowlist of provider-accepted image media types +
//           `isSupportedImageMediaType` guard used at the image mint boundary.
//   (gap B) `UserQuestionAnnotation` typed value for the user_question event.
//   (gap A) `RetryErrorKind` discriminator + optional `statusCode` on retry_attempt.
// ---------------------------------------------------------------------------

describe('IMAGE_MEDIA_TYPES / isSupportedImageMediaType (U006/w91)', () => {
  it('is the exact closed allowlist of provider-accepted image types', () => {
    assert.deepEqual([...IMAGE_MEDIA_TYPES], ['image/png', 'image/jpeg', 'image/gif', 'image/webp']);
  });

  it('accepts every allowlisted type and narrows to ImageMediaType', () => {
    for (const mt of IMAGE_MEDIA_TYPES) {
      assert.ok(isSupportedImageMediaType(mt));
      if (isSupportedImageMediaType(mt)) {
        const narrowed: ImageMediaType = mt; // compile-time narrowing proof
        assert.equal(narrowed, mt);
      }
    }
  });

  it('rejects unsupported/spoofed/absent declared types', () => {
    for (const bad of ['image/bmp', 'image/svg+xml', 'application/octet-stream', 'image/tiff', '', 'png']) {
      assert.equal(isSupportedImageMediaType(bad), false, `${bad} must be unsupported`);
    }
    assert.equal(isSupportedImageMediaType(undefined), false);
    assert.equal(isSupportedImageMediaType(null), false);
  });
});

describe('UserQuestionAnnotation + retry_attempt errorKind (U006/w91)', () => {
  it('a user_question event can carry typed per-question annotations', () => {
    const ann: UserQuestionAnnotation = { preview: '<div>A</div>', notes: 'user liked this' };
    const ev: EngineEvent = {
      type: 'user_question',
      toolCallId: 'tc1',
      question: 'Pick one',
      options: [
        { label: 'A', description: 'option a', preview: '<div>A</div>' },
        { label: 'B', description: 'option b' },
      ],
      allowFreeText: true,
      annotations: { 'Pick one': ann },
    };
    assert.equal(ev.type, 'user_question');
    if (ev.type === 'user_question') {
      assert.equal(ev.annotations?.['Pick one']?.preview, '<div>A</div>');
      assert.equal(ev.options[0].preview, '<div>A</div>');
    }
  });

  it('a retry_attempt event may omit statusCode and carry only errorKind', () => {
    const kind: RetryErrorKind = 'stream_idle';
    const ev: EngineEvent = { type: 'retry_attempt', attempt: 2, maxRetries: 5, delayMs: 100, errorKind: kind };
    assert.equal(ev.type, 'retry_attempt');
    if (ev.type === 'retry_attempt') {
      assert.equal(ev.errorKind, 'stream_idle');
      assert.equal(ev.statusCode, undefined);
    }
  });
});
