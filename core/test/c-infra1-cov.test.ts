import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { APICallError } from 'ai';

import { AnalyticsTracker, TelemetryBatchSender } from '../src/analytics';
import {
  isGatewayRejection,
  classifyError,
  friendlyErrorMessage,
  type ClassifiedError,
} from '../src/error-classifier';
import { Profiler } from '../src/profiler';
import {
  parseRetryAfterHeader,
  parseRateLimitHeaders,
} from '../src/rate-limit-parser';

// ---------------------------------------------------------------------------
// analytics.ts — last gaps: slice trim branch, appendFile catch, interval arrow
// ---------------------------------------------------------------------------

describe('analytics coverage finishers', () => {
  const tmpDirs: string[] = [];
  after(() => {
    for (const d of tmpDirs) {
      try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
    }
  });

  it('trims in-memory events past the cap', () => {
    const t = new AnalyticsTracker();
    for (let i = 0; i < 1005; i++) t.log('evt', { i });
    const events = t.getEvents();
    assert.equal(events.length, 1000);
    // oldest trimmed away — first retained event is not i=0
    assert.notEqual((events[0].data as { i: number }).i, 0);
  });

  it('swallows append failures (catch path) when the log dir cannot be created', () => {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-analytics-'));
    tmpDirs.push(tmp);
    // Create a FILE where a directory is expected so mkdirSync(dirname) throws.
    const blocker = path.join(tmp, 'blocker');
    fs.writeFileSync(blocker, 'x');
    const t = new AnalyticsTracker(blocker); // logFile = <blocker>/analytics.jsonl
    assert.doesNotThrow(() => t.log('evt', { a: 1 }));
    // event still recorded in memory despite the write failure
    assert.equal(t.getEvents().length, 1);
  });

  it('fires the auto-flush interval callback', async () => {
    const sender = new TelemetryBatchSender({ sessionId: 's1', flushIntervalMs: 5 });
    sender.enqueue('t', { x: 1 });
    sender.startAutoFlush();
    // second call hits the early-return guard (timer already set)
    sender.startAutoFlush();
    // wait for the interval arrow () => this.flush() to run
    for (let i = 0; i < 40 && sender.totalSent === 0; i++) await delay(10);
    assert.ok(sender.totalSent >= 1, 'interval callback should have flushed the queue');
    sender.stopAutoFlush();
  });
});

// ---------------------------------------------------------------------------
// error-classifier.ts — last gaps: gateway "not supported"/"not recognized"
// arms, third header&&beta block, budget&&exceeded arms, friendly fallback.
// ---------------------------------------------------------------------------

describe('error-classifier coverage finishers', () => {
  it('detects "not supported" / "not recognized" beta rejections', () => {
    assert.equal(isGatewayRejection('anthropic-beta header is not supported'), true);
    assert.equal(isGatewayRejection('anthropic-beta value not recognized'), true);
  });

  it('detects the unsupported-header + beta block (and its false arm)', () => {
    // third block: "unsupported header" present AND "beta" present -> true
    assert.equal(isGatewayRejection('unsupported header for beta'), true);
    // same left arm but no "beta" -> false (exercises the && false branch)
    assert.equal(isGatewayRejection('unsupported header only'), false);
  });

  it('classifies budget-exceeded as plan_limit via SDK and string paths', () => {
    const sdkErr = new APICallError({
      message: 'monthly budget exceeded',
      url: 'https://api.example.com/v1/chat',
      requestBodyValues: {},
      statusCode: 500,
      responseHeaders: {},
      responseBody: '',
    });
    assert.equal(classifyError(sdkErr).category, 'plan_limit');
    assert.equal(classifyError('monthly budget exceeded').category, 'plan_limit');
  });

  it('falls back to the raw message when a category has no friendly text', () => {
    // Category present at runtime but absent from the friendly-message map and
    // not unknown/abort (exercises the `friendly ?? classified.message` arm).
    const classified = {
      category: 'future_category',
      message: 'raw underlying message',
      retryable: false,
      shouldCompact: false,
      shouldFallback: false,
    } as unknown as ClassifiedError;
    assert.equal(friendlyErrorMessage(classified), 'raw underlying message');
  });
});

// ---------------------------------------------------------------------------
// profiler.ts — last gap: endTurn without a prior beginTurn (|| Date.now()).
// ---------------------------------------------------------------------------

describe('profiler coverage finishers', () => {
  it('endTurn without beginTurn falls back to Date.now() for startTime', () => {
    const p = new Profiler();
    p.endTurn(7, {
      apiLatency: 100,
      toolExecutionTime: 10,
      compactionTime: 0,
      tokensProduced: 5,
      tokensConsumed: 5,
      ttftMs: 20,
    });
    const turn = p.getTurn(7);
    assert.ok(turn);
    assert.equal(turn.turnIndex, 7);
    // no recorded start -> duration is ~0 (startTime defaulted to now)
    assert.ok(turn.duration >= 0 && turn.duration < 1000);
  });
});

// ---------------------------------------------------------------------------
// rate-limit-parser.ts — last gaps: whitespace-only retry-after -> null,
// non-numeric valid Date reset header -> returns the Date.
// ---------------------------------------------------------------------------

describe('rate-limit-parser coverage finishers', () => {
  it('returns null for a whitespace-only retry-after header', () => {
    assert.equal(parseRetryAfterHeader('   '), null);
  });

  it('parses an HTTP-date reset header into a Date', () => {
    const info = parseRateLimitHeaders({
      'x-ratelimit-reset-requests': 'Wed, 21 Oct 2099 07:28:00 GMT',
    });
    assert.ok(info.resetAt instanceof Date);
    assert.equal(info.resetAt?.getUTCFullYear(), 2099);
  });
});
