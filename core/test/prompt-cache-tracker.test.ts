import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { PromptCacheTracker, CACHE_MISS_WARN_THRESHOLD } from '../src/provider';

describe('CACHE_MISS_WARN_THRESHOLD', () => {
  it('is 3', () => {
    assert.equal(CACHE_MISS_WARN_THRESHOLD, 3);
  });
});

describe('PromptCacheTracker', () => {
  it('initial hit rate is 0 with no requests', () => {
    const tracker = new PromptCacheTracker();
    assert.equal(tracker.getHitRate(), 0);
  });

  it('initial total requests is 0', () => {
    const tracker = new PromptCacheTracker();
    assert.equal(tracker.getTotalRequests(), 0);
  });

  it('initial consecutive misses is 0', () => {
    const tracker = new PromptCacheTracker();
    assert.equal(tracker.getConsecutiveMisses(), 0);
  });

  it('recordCacheHit increments total requests', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheHit(1000, 800);
    assert.equal(tracker.getTotalRequests(), 1);
  });

  it('recordCacheMiss increments total requests', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(1000, 0);
    assert.equal(tracker.getTotalRequests(), 1);
  });

  it('getHitRate returns correct ratio after mixed hits and misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheHit(1000, 800);
    tracker.recordCacheHit(1200, 900);
    tracker.recordCacheMiss(1000, 0);
    tracker.recordCacheHit(1100, 700);
    // 3 hits, 1 miss → 0.75
    assert.equal(tracker.getHitRate(), 0.75);
  });

  it('getHitRate is 1.0 with all hits', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheHit(500, 400);
    tracker.recordCacheHit(600, 500);
    assert.equal(tracker.getHitRate(), 1.0);
  });

  it('getHitRate is 0.0 with all misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    tracker.recordCacheMiss(600, 0);
    assert.equal(tracker.getHitRate(), 0.0);
  });

  it('consecutive misses tracks sequential misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    assert.equal(tracker.getConsecutiveMisses(), 1);
    tracker.recordCacheMiss(600, 0);
    assert.equal(tracker.getConsecutiveMisses(), 2);
    tracker.recordCacheMiss(700, 0);
    assert.equal(tracker.getConsecutiveMisses(), 3);
  });

  it('recordCacheHit resets consecutive misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    tracker.recordCacheMiss(600, 0);
    assert.equal(tracker.getConsecutiveMisses(), 2);
    tracker.recordCacheHit(700, 500);
    assert.equal(tracker.getConsecutiveMisses(), 0);
  });

  it('resetConsecutiveMisses sets counter to 0', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    tracker.recordCacheMiss(600, 0);
    tracker.resetConsecutiveMisses();
    assert.equal(tracker.getConsecutiveMisses(), 0);
  });

  it('shouldWarnAboutMisses returns false initially', () => {
    const tracker = new PromptCacheTracker();
    assert.equal(tracker.shouldWarnAboutMisses(), false);
  });

  it('shouldWarnAboutMisses returns false with fewer than 3 consecutive misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    tracker.recordCacheMiss(600, 0);
    assert.equal(tracker.shouldWarnAboutMisses(), false);
  });

  it('shouldWarnAboutMisses returns true after 3 consecutive misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    tracker.recordCacheMiss(600, 0);
    tracker.recordCacheMiss(700, 0);
    assert.equal(tracker.shouldWarnAboutMisses(), true);
  });

  it('shouldWarnAboutMisses returns true after more than 3 consecutive misses', () => {
    const tracker = new PromptCacheTracker();
    for (let i = 0; i < 5; i++) tracker.recordCacheMiss(500, 0);
    assert.equal(tracker.shouldWarnAboutMisses(), true);
  });

  it('shouldWarnAboutMisses resets after a hit breaks the streak', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheMiss(500, 0);
    tracker.recordCacheMiss(600, 0);
    tracker.recordCacheMiss(700, 0);
    assert.equal(tracker.shouldWarnAboutMisses(), true);
    tracker.recordCacheHit(800, 600);
    assert.equal(tracker.shouldWarnAboutMisses(), false);
  });

  it('getTotalRequests accumulates across hits and misses', () => {
    const tracker = new PromptCacheTracker();
    tracker.recordCacheHit(100, 80);
    tracker.recordCacheMiss(200, 0);
    tracker.recordCacheHit(300, 200);
    tracker.recordCacheMiss(400, 0);
    tracker.recordCacheHit(500, 300);
    assert.equal(tracker.getTotalRequests(), 5);
  });
});
