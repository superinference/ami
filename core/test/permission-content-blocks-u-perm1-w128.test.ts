import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  splitPermissionContentBlocks,
  composePermissionResultText,
  describePermissionDocuments,
  type PermissionContentBlock,
} from '../src/permission-content-blocks';
import type { PermissionPromptResult } from '../src/permissions';
import type { TextContent, ImageContent, DocumentContent } from '../src/types';

// U-perm1 / w128 — permission-decision contentBlocks.
//
// Parity: Claude Code's permission allow/deny/abort decisions carry an optional
// `contentBlocks: ContentBlockParam[]` (PermissionContext buildAllow / cancelAndAbort
// / handleUserAllow) so rich content (structured text, images, documents) flows back
// to the model with the permission result. AMI's PermissionPromptResult now carries
// the same blocks; splitPermissionContentBlocks is the single reduction into the three
// delivery channels (text / images / documents).

const txt = (text: string): TextContent => ({ type: 'text', text });
const img = (data: string): ImageContent => ({ type: 'image', source: { type: 'base64', media_type: 'image/png', data } });
const doc = (mt: string): DocumentContent => ({ type: 'document', source: { type: 'base64', media_type: mt, data: 'ZGF0YQ==' } });

test('split returns empty channels for undefined / null / empty input', () => {
  for (const input of [undefined, null, []] as const) {
    const s = splitPermissionContentBlocks(input);
    assert.equal(s.text, '');
    assert.deepEqual(s.images, []);
    assert.deepEqual(s.documents, []);
  }
});

test('split collects text blocks joined by newline in source order', () => {
  const s = splitPermissionContentBlocks([txt('first'), txt('second')]);
  assert.equal(s.text, 'first\nsecond');
  assert.equal(s.images.length, 0);
  assert.equal(s.documents.length, 0);
});

test('split separates image and document blocks into their own channels', () => {
  const blocks: PermissionContentBlock[] = [txt('why'), img('AAAA'), doc('application/pdf'), img('BBBB')];
  const s = splitPermissionContentBlocks(blocks);
  assert.equal(s.text, 'why');
  assert.deepEqual(s.images.map(i => i.source.data), ['AAAA', 'BBBB']);
  assert.equal(s.documents.length, 1);
  assert.equal(s.documents[0]!.source.media_type, 'application/pdf');
});

test('split ignores malformed / unknown blocks (fail-safe)', () => {
  const blocks = [
    null,
    undefined,
    'not-an-object',
    { type: 'text' }, // missing .text
    { type: 'bogus' },
    txt('kept'),
  ] as unknown as PermissionContentBlock[];
  const s = splitPermissionContentBlocks(blocks);
  assert.equal(s.text, 'kept');
  assert.equal(s.images.length, 0);
  assert.equal(s.documents.length, 0);
});

test('describePermissionDocuments: empty for none, names media types otherwise', () => {
  assert.equal(describePermissionDocuments([]), '');
  const one = describePermissionDocuments([doc('application/pdf')]);
  assert.match(one, /1 document attached/);
  assert.match(one, /application\/pdf/);
  const two = describePermissionDocuments([doc('application/pdf'), doc('text/plain')]);
  assert.match(two, /2 documents attached/);
  assert.match(two, /application\/pdf, text\/plain/);
});

test('composePermissionResultText joins base + text + doc note, dropping blanks', () => {
  const s = splitPermissionContentBlocks([txt('extra reason'), doc('application/pdf')]);
  const out = composePermissionResultText('Error: Permission denied', s);
  assert.equal(out, 'Error: Permission denied\nextra reason\n[1 document attached to this permission decision (application/pdf).]');
});

test('composePermissionResultText with no blocks returns just the base', () => {
  const out = composePermissionResultText('Error: Permission denied', splitPermissionContentBlocks([]));
  assert.equal(out, 'Error: Permission denied');
});

test('composePermissionResultText drops an empty base (allow fold with only blocks)', () => {
  const out = composePermissionResultText('', splitPermissionContentBlocks([txt('guidance')]));
  assert.equal(out, 'guidance');
});

test('PermissionPromptResult accepts contentBlocks on allow and deny shapes', () => {
  const allow: PermissionPromptResult = { action: 'allow_once', contentBlocks: [txt('ok'), img('AAAA')] };
  const deny: PermissionPromptResult = { action: 'deny', feedback: 'no', contentBlocks: [txt('because'), doc('application/pdf')] };
  assert.equal(allow.contentBlocks!.length, 2);
  assert.equal(deny.contentBlocks!.length, 2);
  // The field is structurally the content-block union — split consumes it directly.
  assert.equal(splitPermissionContentBlocks(allow.contentBlocks).images.length, 1);
  assert.equal(splitPermissionContentBlocks(deny.contentBlocks).documents.length, 1);
});
