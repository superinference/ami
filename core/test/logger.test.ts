import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { log, logToolCall, logApiCall, logApiResponse, logError, getLogPath } from '../src/logger';

// ---------------------------------------------------------------------------
// getLogPath
// ---------------------------------------------------------------------------
describe('getLogPath', () => {
  it('returns a path under os.tmpdir()', () => {
    const logPath = getLogPath();
    assert.ok(logPath.startsWith(os.tmpdir()));
  });

  it('returns a path ending with engine.log', () => {
    const logPath = getLogPath();
    assert.equal(path.basename(logPath), 'engine.log');
  });

  it('includes superinference/core in the path', () => {
    const logPath = getLogPath();
    assert.ok(logPath.includes(path.join('superinference', 'core')));
  });
});

// ---------------------------------------------------------------------------
// log
// ---------------------------------------------------------------------------
describe('log', () => {
  it('writes a line to the log file', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    log('test-component', 'hello world');
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('[test-component]'));
    assert.ok(newContent.includes('hello world'));
  });

  it('includes ISO timestamp', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    log('ts-test', 'timestamp check');
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    // ISO 8601 pattern: YYYY-MM-DDTHH:MM:SS
    assert.ok(/\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(newContent));
  });

  it('appends data as JSON when provided', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    log('data-test', 'with data', { foo: 'bar', num: 42 });
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('"foo":"bar"'));
    assert.ok(newContent.includes('"num":42'));
  });

  it('does not append data when data is undefined', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    log('no-data', 'no data here');
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    // The line should end with the message and newline, no JSON object
    assert.ok(newContent.includes('no data here\n'));
  });
});

// ---------------------------------------------------------------------------
// logToolCall
// ---------------------------------------------------------------------------
describe('logToolCall', () => {
  it('logs a successful tool call', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logToolCall('readFile', { path: '/tmp/test.ts' }, 'file contents', false, 123);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('[tool]'));
    assert.ok(newContent.includes('readFile'));
    assert.ok(newContent.includes('123ms'));
    assert.ok(newContent.includes('OK'));
  });

  it('logs an error tool call', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logToolCall('bash', { command: 'rm -rf /' }, 'permission denied', true, 50);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('ERROR'));
    assert.ok(newContent.includes('"isError":true'));
  });

  it('truncates long input to 500 chars', () => {
    const longInput = 'x'.repeat(1000);
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logToolCall('test', { data: longInput }, 'ok', false, 10);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    // The input field in the logged JSON should be at most 500 chars
    const match = newContent.match(/"input":"([^"]*)"/);
    assert.ok(match);
    assert.ok(match![1].length <= 500);
  });

  it('truncates long output to 1000 chars', () => {
    const longOutput = 'y'.repeat(2000);
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logToolCall('test', {}, longOutput, false, 10);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    const match = newContent.match(/"output":"([^"]*)"/);
    assert.ok(match);
    assert.ok(match![1].length <= 1000);
  });
});

// ---------------------------------------------------------------------------
// logApiCall
// ---------------------------------------------------------------------------
describe('logApiCall', () => {
  it('logs model, message count, tool count, and thinking flag', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logApiCall('claude-opus-4', 5, 3, true);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('[api]'));
    assert.ok(newContent.includes('model=claude-opus-4'));
    assert.ok(newContent.includes('messages=5'));
    assert.ok(newContent.includes('tools=3'));
    assert.ok(newContent.includes('thinking=true'));
  });

  it('logs thinking=false when disabled', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logApiCall('gpt-4o', 1, 0, false);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('thinking=false'));
  });
});

// ---------------------------------------------------------------------------
// logApiResponse
// ---------------------------------------------------------------------------
describe('logApiResponse', () => {
  it('logs finish reason, content length, and tool call count', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logApiResponse('stop', 1500, 2);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('[api]'));
    assert.ok(newContent.includes('finish=stop'));
    assert.ok(newContent.includes('content=1500chars'));
    assert.ok(newContent.includes('toolCalls=2'));
  });

  it('logs usage when provided', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logApiResponse('stop', 100, 0, { promptTokens: 500, completionTokens: 200 });
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('"promptTokens":500'));
    assert.ok(newContent.includes('"completionTokens":200'));
  });

  it('works without usage data', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logApiResponse('end_turn', 50, 0);
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('finish=end_turn'));
  });
});

// ---------------------------------------------------------------------------
// logError
// ---------------------------------------------------------------------------
describe('logError', () => {
  it('logs with ERROR prefix', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logError('engine', 'something broke');
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('[engine]'));
    assert.ok(newContent.includes('ERROR: something broke'));
  });

  it('logs error context when provided', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logError('provider', 'timeout', { url: 'https://api.example.com', status: 504 });
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('"url":"https://api.example.com"'));
    assert.ok(newContent.includes('"status":504'));
  });

  it('works without context', () => {
    const before = fs.existsSync(getLogPath()) ? fs.readFileSync(getLogPath(), 'utf-8') : '';
    logError('test', 'no context error');
    const after = fs.readFileSync(getLogPath(), 'utf-8');
    const newContent = after.slice(before.length);
    assert.ok(newContent.includes('ERROR: no context error'));
  });
});
