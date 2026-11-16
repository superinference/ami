import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  CHANNEL_TAG,
  SAFE_CHANNEL_META_KEY,
  wrapChannelMessage,
  parseChannelMessage,
  displayChannelServerName,
  isChannelMessage,
} from '../src/channel-message';

describe('channel-message: wrapChannelMessage', () => {
  it('writes source first, then safe meta attributes, framing the body with newlines', () => {
    const out = wrapChannelMessage('slack', 'hello world', { user: 'alice', chat_id: 'C123' });
    assert.equal(out, `<${CHANNEL_TAG} source="slack" user="alice" chat_id="C123">\nhello world\n</${CHANNEL_TAG}>`);
  });

  it('serializes with no meta', () => {
    assert.equal(wrapChannelMessage('bridge', 'hi'), `<channel source="bridge">\nhi\n</channel>`);
  });

  it('drops attribute keys that are not plain identifiers (injection guard)', () => {
    const out = wrapChannelMessage('slack', 'x', { 'x" injected="y': 'z', ok_1: 'v' });
    assert.ok(!out.includes('injected'));
    assert.ok(out.includes('ok_1="v"'));
  });

  it('XML-escapes attribute values and the source so they cannot break out', () => {
    const out = wrapChannelMessage('a"b', 'body', { user: 'q"<&' });
    assert.ok(out.includes('source="a&quot;b"'));
    assert.ok(out.includes('user="q&quot;&lt;&amp;"'));
  });
});

describe('channel-message: parseChannelMessage', () => {
  it('returns null for non-channel text', () => {
    assert.equal(parseChannelMessage('just a message'), null);
    assert.equal(parseChannelMessage('<other source="x">y</other>'), null);
  });

  it('parses source, user, attrs and content', () => {
    const r = parseChannelMessage('<channel source="slack" user="alice" chat_id="C1">\nping\n</channel>');
    assert.ok(r);
    assert.equal(r.source, 'slack');
    assert.equal(r.user, 'alice');
    assert.equal(r.attrs.chat_id, 'C1');
    assert.equal(r.content, 'ping');
  });

  it('leaves user undefined when absent', () => {
    const r = parseChannelMessage('<channel source="bridge">\nhi\n</channel>');
    assert.ok(r);
    assert.equal(r.user, undefined);
    assert.deepEqual(r.attrs, {});
    assert.equal(r.content, 'hi');
  });

  it('round-trips escaped values through wrap -> parse', () => {
    const wrapped = wrapChannelMessage('a"b', 'multi\nline body', { user: 'q"<&>' });
    const r = parseChannelMessage(wrapped);
    assert.ok(r);
    assert.equal(r.source, 'a"b');
    assert.equal(r.user, 'q"<&>');
    assert.equal(r.content, 'multi\nline body');
  });

  it('handles a body without framing newlines', () => {
    const r = parseChannelMessage('<channel source="s">body</channel>');
    assert.ok(r);
    assert.equal(r.content, 'body');
  });
});

describe('channel-message: displayChannelServerName', () => {
  it('returns the leaf after the last colon for plugin-scoped names', () => {
    assert.equal(displayChannelServerName('plugin:slack-channel:slack'), 'slack');
  });
  it('returns the name unchanged when there is no colon', () => {
    assert.equal(displayChannelServerName('slack'), 'slack');
  });
});

describe('channel-message: isChannelMessage + SAFE_CHANNEL_META_KEY', () => {
  it('detects a channel wrapper without fully parsing', () => {
    assert.equal(isChannelMessage('<channel source="x">y</channel>'), true);
    assert.equal(isChannelMessage('nope'), false);
  });
  it('exposes the safe-key guard', () => {
    assert.equal(SAFE_CHANNEL_META_KEY.test('chat_id'), true);
    assert.equal(SAFE_CHANNEL_META_KEY.test('1bad'), false);
    assert.equal(SAFE_CHANNEL_META_KEY.test('has-dash'), false);
  });
});
