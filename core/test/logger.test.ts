import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { log, logToolCall, logApiCall, logApiResponse, logError, getLogPath, getInMemoryErrors, fsHooks, __resetLoggerForTest } from '../src/logger';

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

// ---------------------------------------------------------------------------
// Severity levels + min-level verbosity gate (hermetic — fs seam mocked)
// ---------------------------------------------------------------------------
describe('log level filtering', () => {
  const ORIG = { ...process.env };
  function capture(): { written: string[] } {
    const state = { written: [] as string[] };
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', (_f: fs.PathOrFileDescriptor, data: unknown) => {
      state.written.push(String(data));
    });
    return state;
  }
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
    delete process.env.AMI_DEBUG_LOG_LEVEL;
    delete process.env.CLAUDE_CODE_DEBUG_LOG_LEVEL;
    process.env = { ...ORIG };
  });

  it("stamps the severity in uppercase and defaults to [DEBUG]", () => {
    __resetLoggerForTest();
    const s = capture();
    log('comp', 'hello');
    assert.ok(s.written[0].includes('[DEBUG] [comp]'));
  });

  it("drops 'verbose' records by default (min level is 'debug')", () => {
    __resetLoggerForTest();
    const s = capture();
    log('comp', 'noisy', undefined, 'verbose');
    assert.equal(s.written.length, 0);
  });

  it("AMI_DEBUG_LOG_LEVEL=verbose lets verbose records through", () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_LOG_LEVEL = 'verbose';
    const s = capture();
    log('comp', 'noisy', undefined, 'verbose');
    assert.equal(s.written.length, 1);
    assert.ok(s.written[0].includes('[VERBOSE]'));
  });

  it("AMI_DEBUG_LOG_LEVEL=warn suppresses debug/info but keeps errors", () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_LOG_LEVEL = 'WARN'; // case/space-insensitive
    const s = capture();
    log('comp', 'dbg', undefined, 'debug');
    log('comp', 'nfo', undefined, 'info');
    logError('comp', 'boom');
    assert.equal(s.written.length, 1);
    assert.ok(s.written[0].includes('[ERROR] [comp] ERROR: boom'));
  });

  it('falls back to the default on an invalid env value', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_LOG_LEVEL = 'bogus';
    const s = capture();
    log('comp', 'dbg', undefined, 'debug');
    assert.equal(s.written.length, 1);
  });

  it('honors the Claude-compatible CLAUDE_CODE_DEBUG_LOG_LEVEL env', () => {
    __resetLoggerForTest();
    process.env.CLAUDE_CODE_DEBUG_LOG_LEVEL = 'error';
    const s = capture();
    log('comp', 'dbg', undefined, 'debug');
    assert.equal(s.written.length, 0);
  });
});

// ---------------------------------------------------------------------------
// Component allow-filter (AMI_DEBUG_COMPONENTS)
// ---------------------------------------------------------------------------
describe('component debug filter', () => {
  const ORIG = { ...process.env };
  function capture(): { written: string[] } {
    const state = { written: [] as string[] };
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', (_f: fs.PathOrFileDescriptor, data: unknown) => {
      state.written.push(String(data));
    });
    return state;
  }
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
    delete process.env.AMI_DEBUG_COMPONENTS;
    process.env = { ...ORIG };
  });

  it('only persists listed components when the filter is set', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = 'api, tool';
    const s = capture();
    log('api', 'kept');
    log('tool', 'kept too');
    log('memory', 'dropped');
    assert.equal(s.written.length, 2);
    assert.ok(s.written[0].includes('[api]'));
    assert.ok(s.written[1].includes('[tool]'));
  });

  it("the '*' sentinel logs everything", () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = '*';
    const s = capture();
    log('anything', 'kept');
    assert.equal(s.written.length, 1);
  });

  it('an empty/whitespace filter logs everything', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = ' , ';
    const s = capture();
    log('anything', 'kept');
    assert.equal(s.written.length, 1);
  });
});

// ---------------------------------------------------------------------------
// Destination override (AMI_DEBUG_FILE / AMI_DEBUG_LOGS_DIR / Claude env)
// ---------------------------------------------------------------------------
describe('log destination override', () => {
  const ORIG = { ...process.env };
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
    delete process.env.AMI_DEBUG_FILE;
    delete process.env.AMI_DEBUG_LOGS_DIR;
    delete process.env.CLAUDE_CODE_DEBUG_LOGS_DIR;
    process.env = { ...ORIG };
  });

  it('getLogPath reflects AMI_DEBUG_LOGS_DIR before init', () => {
    __resetLoggerForTest();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-logdir-'));
    process.env.AMI_DEBUG_LOGS_DIR = dir;
    try {
      assert.equal(getLogPath(), path.join(dir, 'engine.log'));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('CLAUDE_CODE_DEBUG_LOGS_DIR is honored as a fallback', () => {
    __resetLoggerForTest();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cc-logdir-'));
    process.env.CLAUDE_CODE_DEBUG_LOGS_DIR = dir;
    try {
      assert.equal(getLogPath(), path.join(dir, 'engine.log'));
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('AMI_DEBUG_FILE overrides the full file path and wins over the dir env', () => {
    __resetLoggerForTest();
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-logfile-'));
    const file = path.join(dir, 'custom.log');
    process.env.AMI_DEBUG_FILE = file;
    process.env.AMI_DEBUG_LOGS_DIR = '/should/be/ignored';
    try {
      assert.equal(getLogPath(), file);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('actually writes to the overridden directory and mkdirs it', () => {
    __resetLoggerForTest();
    let mkdirTarget = '';
    let appendTarget = '';
    mock.method(fsHooks, 'mkdirSync', (p: fs.PathLike) => { mkdirTarget = String(p); return undefined as unknown as string; });
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', (p: fs.PathOrFileDescriptor) => { appendTarget = String(p); });
    process.env.AMI_DEBUG_LOGS_DIR = '/tmp/ami-override-xyz';
    log('comp', 'hi');
    assert.equal(mkdirTarget, '/tmp/ami-override-xyz');
    assert.equal(appendTarget, path.join('/tmp/ami-override-xyz', 'engine.log'));
    // getLogPath is now pinned to the resolved file for the session.
    assert.equal(getLogPath(), path.join('/tmp/ami-override-xyz', 'engine.log'));
  });

  it('rotation targets the overridden file', () => {
    __resetLoggerForTest();
    const renamed: Array<[string, string]> = [];
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => ({ size: 10 * 1024 * 1024 }) as unknown as fs.Stats);
    mock.method(fsHooks, 'renameSync', (a: fs.PathLike, b: fs.PathLike) => { renamed.push([String(a), String(b)]); });
    mock.method(fsHooks, 'unlinkSync', () => undefined);
    mock.method(fsHooks, 'appendFileSync', () => undefined);
    process.env.AMI_DEBUG_FILE = '/tmp/ami-rot/custom.log';
    log('big', 'overflow record');
    assert.equal(renamed.length, 1);
    assert.deepEqual(renamed[0], ['/tmp/ami-rot/custom.log', '/tmp/ami-rot/custom.log.prev']);
  });
});

// ---------------------------------------------------------------------------
// In-memory recent-errors ring buffer
// ---------------------------------------------------------------------------
describe('in-memory recent-errors ring', () => {
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
  });

  it('captures errors (newest last) with a timestamp and component tag', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', () => undefined);
    logError('engine', 'first');
    logError('provider', 'second');
    const errs = getInMemoryErrors();
    assert.equal(errs.length, 2);
    assert.ok(errs[0].error.includes('[engine] first'));
    assert.ok(errs[1].error.includes('[provider] second'));
    assert.ok(/\d{4}-\d{2}-\d{2}T/.test(errs[1].timestamp));
  });

  it('redacts secrets before storing them in the ring', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', () => undefined);
    const secret = 'sk-ant-' + 'c'.repeat(40);
    logError('provider', `auth failed ${secret}`);
    const errs = getInMemoryErrors();
    assert.ok(!errs[0].error.includes(secret));
    assert.ok(errs[0].error.includes('[REDACTED]'));
  });

  it('captures the error even when the disk write is filtered out', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = 'nothing-matches';
    let appended = 0;
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', () => { appended++; });
    try {
      logError('engine', 'while filtered');
      assert.equal(appended, 0, 'disk write was filtered out');
      assert.equal(getInMemoryErrors().length, 1, 'but the ring still captured it');
    } finally {
      delete process.env.AMI_DEBUG_COMPONENTS;
    }
  });

  it('is bounded at 100 entries (oldest dropped)', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', () => undefined);
    for (let i = 0; i < 150; i++) logError('e', `err-${i}`);
    const errs = getInMemoryErrors();
    assert.equal(errs.length, 100);
    assert.ok(errs[0].error.includes('err-50'), 'oldest surviving is err-50');
    assert.ok(errs[99].error.includes('err-149'), 'newest is err-149');
  });

  it('returns a defensive copy (mutating it does not corrupt the ring)', () => {
    __resetLoggerForTest();
    mock.method(fsHooks, 'mkdirSync', () => undefined as unknown as string);
    mock.method(fsHooks, 'statSync', () => { throw new Error('ENOENT'); });
    mock.method(fsHooks, 'appendFileSync', () => undefined);
    logError('e', 'one');
    const first = getInMemoryErrors();
    first.push({ error: 'injected', timestamp: 'x' });
    assert.equal(getInMemoryErrors().length, 1);
  });
});
