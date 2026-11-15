import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { log, logToolCall, logApiCall, logApiResponse, logError, getLogPath, fsHooks, __resetLoggerForTest } from '../src/logger';

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

// ---------------------------------------------------------------------------
// Security / resilience behaviors (hermetic — fs seam mocked, no disk writes)
// ---------------------------------------------------------------------------
describe('logger hardening', () => {
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
  });

  it('creates the log dir 0o700 and appends the file with mode 0o600', () => {
    __resetLoggerForTest();
    const mkdir = mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    const append = mock.method(fsHooks, 'appendFileSync', () => undefined);

    log('sec', 'hello');

    assert.deepEqual(mkdir.mock.calls[0].arguments[1], { recursive: true, mode: 0o700 });
    assert.deepEqual(append.mock.calls[0].arguments[2], { mode: 0o600 });
  });

  it('recreates the directory and retries once when an append fails', () => {
    __resetLoggerForTest();
    const mkdir = mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    let appendCalls = 0;
    const append = mock.method(fsHooks, 'appendFileSync', () => {
      appendCalls++;
      if (appendCalls === 1) throw new Error('ENOENT: directory removed');
    });

    log('heal', 'line after dir disappears');

    // First append throws, directory is recreated, second append succeeds.
    assert.equal(append.mock.calls.length, 2);
    // mkdirSync: once in ensureDir, once in the self-heal retry.
    assert.equal(mkdir.mock.calls.length, 2);
  });

  it('rotates continuously when the running size would exceed the cap', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    // Seed the in-process counter to exactly the cap (not over), so init does NOT
    // rotate but the next append pushes it over and must rotate mid-session.
    mock.method(fsHooks, 'statSync', () => ({ size: 10 * 1024 * 1024 }) as unknown as fs.Stats);
    const rename = mock.method(fsHooks, 'renameSync', () => undefined);
    mock.method(fsHooks, 'unlinkSync', () => undefined);
    mock.method(fsHooks, 'appendFileSync', () => undefined);

    log('big', 'this record pushes the file past the size cap');

    assert.equal(rename.mock.calls.length, 1);
  });

  it('escapes embedded newlines so one record stays one physical line', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    let written = '';
    mock.method(fsHooks, 'appendFileSync', (_f: fs.PathOrFileDescriptor, data: unknown) => {
      written = String(data);
    });

    log('inj', 'line1\n[2026-01-01T00:00:00.000Z] [forged] injected entry');

    // Exactly one physical newline: the trailing record terminator.
    assert.equal(written.split('\n').length - 1, 1);
    assert.ok(written.includes('line1\\n[2026-01-01'));
  });

  it('redacts credentials before the record touches disk', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    let written = '';
    mock.method(fsHooks, 'appendFileSync', (_f: fs.PathOrFileDescriptor, data: unknown) => {
      written = String(data);
    });

    const secret = 'sk-ant-' + 'a'.repeat(40);
    logError('provider', `auth failed with key ${secret}`);

    assert.ok(!written.includes(secret), 'raw secret must not be written');
    assert.ok(written.includes('[REDACTED]'), 'redaction placeholder expected');
  });

  it('redacts secrets embedded in tool input/output', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    let written = '';
    mock.method(fsHooks, 'appendFileSync', (_f: fs.PathOrFileDescriptor, data: unknown) => {
      written = String(data);
    });

    const token = 'ghp_' + 'b'.repeat(36);
    logToolCall('bash', { command: `curl -H "token: ${token}"` }, `used ${token}`, false, 12);

    assert.ok(!written.includes(token), 'secret in tool input/output must not be written');
    assert.ok(written.includes('[REDACTED]'));
  });
});
