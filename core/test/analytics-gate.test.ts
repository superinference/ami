import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { AnalyticsTracker } from '../src/analytics';

describe('AnalyticsTracker telemetry gate', () => {
  let tracker: AnalyticsTracker;

  beforeEach(() => {
    tracker = new AnalyticsTracker();
  });

  it('allows all sources by default (no allowlist set)', () => {
    tracker.pushSource('plugin-x');
    tracker.log('event1', { key: 'val' });
    tracker.popSource();
    assert.equal(tracker.getEvents().length, 1);
    assert.equal(tracker.getDroppedEventCount(), 0);
  });

  it('allows logging with no source context when allowlist is not set', () => {
    tracker.log('event1');
    assert.equal(tracker.getEvents().length, 1);
  });

  it('blocks logging when allowlist is set and no source context exists', () => {
    tracker.setAllowedSources(['builtin']);
    tracker.log('event1');
    assert.equal(tracker.getEvents().length, 0);
    assert.equal(tracker.getDroppedEventCount(), 1);
  });

  it('blocks logging from sources not in the allowlist', () => {
    tracker.setAllowedSources(['builtin', 'core']);
    tracker.pushSource('untrusted-plugin');
    tracker.log('event1');
    tracker.popSource();
    assert.equal(tracker.getEvents().length, 0);
    assert.equal(tracker.getDroppedEventCount(), 1);
  });

  it('allows logging from sources in the allowlist', () => {
    tracker.setAllowedSources(['builtin', 'core']);
    tracker.pushSource('core');
    tracker.log('event1', { info: 'hello' });
    tracker.popSource();
    assert.equal(tracker.getEvents().length, 1);
    assert.equal(tracker.getEvents()[0].type, 'event1');
    assert.deepEqual(tracker.getEvents()[0].data, { info: 'hello' });
  });

  it('counts dropped events correctly across multiple calls', () => {
    tracker.setAllowedSources(['builtin']);
    tracker.pushSource('plugin-a');
    tracker.log('e1');
    tracker.log('e2');
    tracker.log('e3');
    tracker.popSource();
    assert.equal(tracker.getDroppedEventCount(), 3);
    assert.equal(tracker.getEvents().length, 0);
  });

  it('clears the allowlist when setAllowedSources(null) is called', () => {
    tracker.setAllowedSources(['builtin']);
    tracker.pushSource('plugin-a');
    tracker.log('blocked');
    assert.equal(tracker.getEvents().length, 0);

    tracker.setAllowedSources(null);
    tracker.log('allowed');
    assert.equal(tracker.getEvents().length, 1);
    assert.equal(tracker.getEvents()[0].type, 'allowed');
  });

  it('supports nested source contexts with push/pop', () => {
    tracker.setAllowedSources(['builtin']);
    tracker.pushSource('builtin');
    tracker.log('from-builtin');
    assert.equal(tracker.getEvents().length, 1);

    tracker.pushSource('user-plugin');
    tracker.log('from-user-plugin');
    assert.equal(tracker.getEvents().length, 1);
    assert.equal(tracker.getDroppedEventCount(), 1);

    tracker.popSource();
    assert.equal(tracker.currentSource(), 'builtin');
    tracker.log('back-to-builtin');
    assert.equal(tracker.getEvents().length, 2);
    tracker.popSource();
  });

  it('currentSource returns undefined when stack is empty', () => {
    assert.equal(tracker.currentSource(), undefined);
  });

  it('currentSource returns the top of the stack', () => {
    tracker.pushSource('a');
    tracker.pushSource('b');
    assert.equal(tracker.currentSource(), 'b');
    tracker.popSource();
    assert.equal(tracker.currentSource(), 'a');
    tracker.popSource();
    assert.equal(tracker.currentSource(), undefined);
  });

  it('popSource returns undefined when stack is empty', () => {
    assert.equal(tracker.popSource(), undefined);
  });

  it('popSource returns the popped source', () => {
    tracker.pushSource('x');
    assert.equal(tracker.popSource(), 'x');
  });

  it('preserves event data (timestamp, sessionId, data) when allowed', () => {
    tracker.setSessionId('sess-42');
    tracker.setAllowedSources(['engine']);
    tracker.pushSource('engine');
    tracker.log('tool_call', { tool: 'bash', cmd: 'ls' });
    tracker.popSource();

    const events = tracker.getEvents();
    assert.equal(events.length, 1);
    assert.equal(events[0].type, 'tool_call');
    assert.equal(events[0].sessionId, 'sess-42');
    assert.deepEqual(events[0].data, { tool: 'bash', cmd: 'ls' });
    assert.ok(events[0].timestamp);
  });

  it('mixes allowed and dropped events correctly', () => {
    tracker.setAllowedSources(['builtin']);
    tracker.pushSource('builtin');
    tracker.log('ok1');
    tracker.popSource();

    tracker.pushSource('bad-plugin');
    tracker.log('blocked1');
    tracker.log('blocked2');
    tracker.popSource();

    tracker.pushSource('builtin');
    tracker.log('ok2');
    tracker.popSource();

    assert.equal(tracker.getEvents().length, 2);
    assert.equal(tracker.getDroppedEventCount(), 2);
    assert.equal(tracker.getEvents()[0].type, 'ok1');
    assert.equal(tracker.getEvents()[1].type, 'ok2');
  });

  it('empty allowlist blocks everything', () => {
    tracker.setAllowedSources([]);
    tracker.pushSource('anything');
    tracker.log('event1');
    tracker.popSource();
    tracker.log('event2');
    assert.equal(tracker.getEvents().length, 0);
    assert.equal(tracker.getDroppedEventCount(), 2);
  });

  it('does not reset dropped count on setAllowedSources', () => {
    tracker.setAllowedSources(['a']);
    tracker.pushSource('b');
    tracker.log('dropped');
    assert.equal(tracker.getDroppedEventCount(), 1);

    tracker.setAllowedSources(null);
    assert.equal(tracker.getDroppedEventCount(), 1);
  });
});
