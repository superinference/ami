import { describe, it, beforeEach, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { log, logToolCall, logApiCall, logApiResponse, logError, getLogPath, getInMemoryErrors, fsHooks, __resetLoggerForTest, parseDebugFilter, extractDebugCategories, shouldShowDebugCategories, readLogTail, summarizeValue, extractServerMessage, httpErrorContext, captureApiRequest, getLastApiRequest, recordDiagnostic, shouldSurfaceToTelemetry, withDiagnosticsTiming } from '../src/logger';

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

// ---------------------------------------------------------------------------
// Debug-filter spec parsing (include / exclude / isExclusive) + edge semantics
// ---------------------------------------------------------------------------
describe('parseDebugFilter', () => {
  beforeEach(() => { __resetLoggerForTest(); });

  it('returns null (show all) for undefined / empty / whitespace / star', () => {
    assert.equal(parseDebugFilter(undefined), null);
    assert.equal(parseDebugFilter(''), null);
    assert.equal(parseDebugFilter('   '), null);
    assert.equal(parseDebugFilter('*'), null);
    assert.equal(parseDebugFilter('  *  '), null);
  });

  it('drops stray empty tokens and returns null when none remain', () => {
    assert.equal(parseDebugFilter(' , '), null);
    assert.deepEqual(parseDebugFilter('api,,hooks'), {
      include: ['api', 'hooks'],
      exclude: [],
      isExclusive: false,
    });
  });

  it('builds an inclusive filter from a plain comma list (lowercased)', () => {
    assert.deepEqual(parseDebugFilter('Api, HOOKS'), {
      include: ['api', 'hooks'],
      exclude: [],
      isExclusive: false,
    });
  });

  it("builds an exclusive filter from '!'-prefixed tokens", () => {
    assert.deepEqual(parseDebugFilter('!1p,!file'), {
      include: [],
      exclude: ['1p', 'file'],
      isExclusive: true,
    });
  });

  it('falls back to null (show all) for a mixed include/exclude spec', () => {
    assert.equal(parseDebugFilter('api,!file'), null);
  });

  it('memoizes repeated lookups of the same spec string', () => {
    const a = parseDebugFilter('api,hooks');
    const b = parseDebugFilter('api,hooks');
    assert.equal(a, b); // same cached object reference
  });
});

describe('extractDebugCategories', () => {
  it("extracts a 'category: message' prefix", () => {
    assert.deepEqual(extractDebugCategories('API: calling model'), ['api']);
  });

  it('extracts a [CATEGORY] bracket prefix', () => {
    assert.deepEqual(extractDebugCategories('[Hooks] ran preToolUse'), ['hooks']);
  });

  it('extracts MCP server name first, avoiding the generic prefix matcher', () => {
    assert.deepEqual(extractDebugCategories('MCP server "github": connected'), ['mcp', 'github']);
  });

  it("recognises the '1p event:' marker", () => {
    const cats = extractDebugCategories('[ANT-ONLY] 1P event: tengu_timer');
    assert.ok(cats.includes('ant-only'));
    assert.ok(cats.includes('1p'));
  });

  it('bounds the secondary-category heuristic (rejects long / spaced fragments) and de-dupes', () => {
    // Secondary fragment contains a space → rejected.
    const spaced = extractDebugCategories('x: has space here: y');
    assert.ok(!spaced.some((c) => c.includes(' ')));
    // No colon → no category at all (uncategorized).
    assert.deepEqual(extractDebugCategories('just a plain message'), []);
  });
});

describe('shouldShowDebugCategories', () => {
  it('shows everything when there is no filter', () => {
    assert.equal(shouldShowDebugCategories([], null), true);
    assert.equal(shouldShowDebugCategories(['api'], null), true);
  });

  it('is fail-closed: uncategorized lines are dropped under any active filter', () => {
    assert.equal(shouldShowDebugCategories([], { include: ['api'], exclude: [], isExclusive: false }), false);
    assert.equal(shouldShowDebugCategories([], { include: [], exclude: ['file'], isExclusive: true }), false);
  });

  it('inclusive mode shows only matching categories', () => {
    const f = parseDebugFilter('api');
    assert.equal(shouldShowDebugCategories(['api'], f), true);
    assert.equal(shouldShowDebugCategories(['memory'], f), false);
  });

  it('exclusive mode shows everything except excluded categories', () => {
    const f = parseDebugFilter('!file');
    assert.equal(shouldShowDebugCategories(['api'], f), true);
    assert.equal(shouldShowDebugCategories(['file'], f), false);
  });
});

// Integration: the filter actually gates disk writes through log().
describe('debug-filter integration through log()', () => {
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

  it("exclusive '!'-spec drops the excluded component but keeps others", () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = '!memory';
    const s = capture();
    log('api', 'kept');
    log('memory', 'dropped');
    assert.equal(s.written.length, 1);
    assert.ok(s.written[0].includes('[api]'));
  });

  it('matches a category extracted from the message body, not just the component', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = 'mcp';
    const s = capture();
    // component 'conn' is not in the include list, but the message yields 'mcp'.
    log('conn', 'MCP server "github": connected');
    assert.equal(s.written.length, 1);
  });

  it('fail-closed: an uncategorized blank-component line is dropped under a filter', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = 'api';
    const s = capture();
    log('', 'a plain uncategorized line');
    assert.equal(s.written.length, 0);
  });

  it('a mixed include/exclude spec is invalid and logs everything', () => {
    __resetLoggerForTest();
    process.env.AMI_DEBUG_COMPONENTS = 'api,!memory';
    const s = capture();
    log('api', 'kept');
    log('memory', 'also kept');
    assert.equal(s.written.length, 2);
  });
});

// ---------------------------------------------------------------------------
// summarizeValue (U060 — human-readable value summarizer)
// ---------------------------------------------------------------------------
describe('summarizeValue', () => {
  it('renders arrays as Array[N]', () => {
    assert.equal(summarizeValue([1, 2, 3]), 'Array[3]');
    assert.equal(summarizeValue([]), 'Array[0]');
  });
  it('renders objects as Object{N keys}', () => {
    assert.equal(summarizeValue({ a: 1, b: 2 }), 'Object{2 keys}');
    assert.equal(summarizeValue({}), 'Object{0 keys}');
  });
  it('truncates long strings to 80 chars with an ellipsis', () => {
    const s = 'x'.repeat(100);
    assert.equal(summarizeValue(s), 'x'.repeat(80) + '…');
  });
  it('leaves short strings intact', () => {
    assert.equal(summarizeValue('hello'), 'hello');
  });
  it('stringifies primitives and null', () => {
    assert.equal(summarizeValue(42), '42');
    assert.equal(summarizeValue(true), 'true');
    assert.equal(summarizeValue(null), 'null');
    assert.equal(summarizeValue(undefined), 'undefined');
  });
});

// ---------------------------------------------------------------------------
// extractServerMessage + httpErrorContext (U060 — HTTP error enrichment)
// ---------------------------------------------------------------------------
describe('extractServerMessage', () => {
  it('returns a raw string body unchanged', () => {
    assert.equal(extractServerMessage('boom'), 'boom');
  });
  it('reads a top-level { message }', () => {
    assert.equal(extractServerMessage({ message: 'bad request' }), 'bad request');
  });
  it('reads a nested { error: { message } }', () => {
    assert.equal(extractServerMessage({ error: { message: 'nested' } }), 'nested');
  });
  it('returns undefined for unrecognized shapes', () => {
    assert.equal(extractServerMessage(42), undefined);
    assert.equal(extractServerMessage(null), undefined);
    assert.equal(extractServerMessage({ foo: 'bar' }), undefined);
  });
});

describe('httpErrorContext', () => {
  it('enriches an axios-style error (config.url + response.status + response.data)', () => {
    const err = { config: { url: 'https://api/x' }, response: { status: 503, data: { message: 'overloaded' } } };
    assert.equal(httpErrorContext(err), '[url=https://api/x,status=503,body=overloaded] ');
  });
  it('enriches a fetch-style error (url + status + body)', () => {
    const err = { url: 'https://api/y', status: 404, body: 'not found' };
    assert.equal(httpErrorContext(err), '[url=https://api/y,status=404,body=not found] ');
  });
  it('returns empty string when there is no url', () => {
    assert.equal(httpErrorContext({ status: 500 }), '');
    assert.equal(httpErrorContext(null), '');
    assert.equal(httpErrorContext('oops'), '');
  });
  it('omits status/body when absent', () => {
    assert.equal(httpErrorContext({ url: 'https://api/z' }), '[url=https://api/z] ');
  });
});

// ---------------------------------------------------------------------------
// logError enrichment / hard-fail / disable-error-reporting (U060)
// ---------------------------------------------------------------------------
describe('logError gating & enrichment', () => {
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
  beforeEach(() => { __resetLoggerForTest(); });
  afterEach(() => {
    mock.restoreAll();
    __resetLoggerForTest();
    process.env = { ...ORIG };
  });

  it('prepends HTTP context from context.httpError and does not dump the raw object to data', () => {
    const s = capture();
    logError('net', 'request failed', { httpError: { url: 'https://h/api', status: 500, body: 'srv' }, attempt: 2 });
    assert.equal(s.written.length, 1);
    const line = s.written[0];
    assert.ok(line.includes('[url=https://h/api,status=500,body=srv] request failed'));
    // restContext (attempt) is kept as structured data...
    assert.ok(line.includes('"attempt":2'));
    // ...but the raw httpError object is NOT serialized into the data blob.
    assert.ok(!line.includes('"httpError"'));
    // in-memory ring also carries the enriched text
    assert.ok(getInMemoryErrors()[0].error.includes('[url=https://h/api,status=500,body=srv]'));
  });

  it('AMI_LOG_HARD_FAIL makes logError throw (crash-on-logged-error)', () => {
    capture();
    process.env.AMI_LOG_HARD_FAIL = '1';
    assert.throws(() => logError('x', 'boom'), /\[HARD FAIL\] \[x\] boom/);
  });

  it('AMI_LOG_HARD_FAIL=off does not throw', () => {
    capture();
    process.env.AMI_LOG_HARD_FAIL = 'off';
    assert.doesNotThrow(() => logError('x', 'fine'));
  });

  it('DISABLE_ERROR_REPORTING suppresses both the ring and the disk write', () => {
    const s = capture();
    process.env.DISABLE_ERROR_REPORTING = '1';
    logError('x', 'silenced');
    assert.equal(s.written.length, 0);
    assert.equal(getInMemoryErrors().length, 0);
  });

  it('hard-fail fires even when error reporting is disabled', () => {
    capture();
    process.env.DISABLE_ERROR_REPORTING = '1';
    process.env.AMI_LOG_HARD_FAIL = '1';
    assert.throws(() => logError('x', 'boom'), /HARD FAIL/);
  });
});

// ---------------------------------------------------------------------------
// readLogTail (U060 — memory-bounded tail read)
// ---------------------------------------------------------------------------
describe('readLogTail', () => {
  afterEach(() => { mock.restoreAll(); __resetLoggerForTest(); });

  it('returns the last N lines from the trailing byte window', () => {
    const content = Array.from({ length: 50 }, (_, i) => `line${i}`).join('\n') + '\n';
    const buf = Buffer.from(content, 'utf-8');
    mock.method(fsHooks, 'statSync', () => ({ size: buf.length }) as fs.Stats);
    mock.method(fsHooks, 'openSync', () => 7);
    mock.method(fsHooks, 'readSync', (_fd: number, b: Buffer, off: number, len: number, pos: number) => {
      buf.copy(b, off, pos, pos + len);
      return len;
    });
    let closed = false;
    mock.method(fsHooks, 'closeSync', () => { closed = true; });
    const res = readLogTail({ maxLines: 5 });
    assert.ok(res.ok);
    if (res.ok) {
      assert.deepEqual(res.lines, ['line45', 'line46', 'line47', 'line48', 'line49']);
      assert.equal(res.size, buf.length);
    }
    assert.ok(closed, 'fd should be closed');
  });

  it('honors maxBytes by only reading the trailing window', () => {
    const content = 'aaaa\nbbbb\ncccc\ndddd\n';
    const buf = Buffer.from(content, 'utf-8');
    let requestedPos = -1;
    let requestedLen = -1;
    mock.method(fsHooks, 'statSync', () => ({ size: buf.length }) as fs.Stats);
    mock.method(fsHooks, 'openSync', () => 3);
    mock.method(fsHooks, 'readSync', (_fd: number, b: Buffer, off: number, len: number, pos: number) => {
      requestedPos = pos; requestedLen = len;
      buf.copy(b, off, pos, pos + len);
      return len;
    });
    mock.method(fsHooks, 'closeSync', () => undefined);
    const res = readLogTail({ maxBytes: 10, maxLines: 20 });
    assert.ok(res.ok);
    assert.equal(requestedLen, 10);
    assert.equal(requestedPos, buf.length - 10);
  });

  it('special-cases a missing file (ENOENT => missing:true)', () => {
    mock.method(fsHooks, 'statSync', () => { const e = new Error('nope') as NodeJS.ErrnoException; e.code = 'ENOENT'; throw e; });
    const res = readLogTail({ file: '/no/such/log' });
    assert.deepEqual(res, { ok: false, missing: true });
  });

  it('reports other read errors with missing:false + message', () => {
    mock.method(fsHooks, 'statSync', () => { const e = new Error('EACCES denied') as NodeJS.ErrnoException; e.code = 'EACCES'; throw e; });
    const res = readLogTail({ file: '/root/log' });
    assert.equal(res.ok, false);
    if (!res.ok) {
      assert.equal(res.missing, false);
      assert.ok(res.error && res.error.includes('EACCES'));
    }
  });
});

// ---------------------------------------------------------------------------
// captureApiRequest / getLastApiRequest (U060)
// ---------------------------------------------------------------------------
describe('captureApiRequest', () => {
  afterEach(() => { __resetLoggerForTest(); });

  it('returns null before any capture', () => {
    __resetLoggerForTest();
    assert.equal(getLastApiRequest(), null);
  });
  it('keeps only the most recent request and redacts string params', () => {
    captureApiRequest({ model: 'opus', authorization: 'Bearer sk-ant-api03-SECRETKEY', count: 3 });
    captureApiRequest({ model: 'sonnet', count: 9 });
    const last = getLastApiRequest();
    assert.ok(last);
    assert.equal(last!.params.model, 'sonnet');
    assert.equal(last!.params.count, 9);
    assert.ok(typeof last!.timestamp === 'string');
  });
  it('redacts credentials in captured string values', () => {
    captureApiRequest({ authorization: 'Bearer sk-ant-api03-DEADBEEFDEADBEEFDEADBEEF' });
    const last = getLastApiRequest();
    assert.ok(last);
    assert.ok(!String(last!.params.authorization).includes('DEADBEEFDEADBEEFDEADBEEF'));
  });
  it('returns a copy (mutating the result does not affect stored state)', () => {
    captureApiRequest({ model: 'opus' });
    const a = getLastApiRequest();
    a!.params.model = 'tampered';
    assert.equal(getLastApiRequest()!.params.model, 'opus');
  });
});

// ---------------------------------------------------------------------------
// shouldSurfaceToTelemetry / recordDiagnostic (U060 — severity bridge)
// ---------------------------------------------------------------------------
describe('shouldSurfaceToTelemetry', () => {
  it('surfaces warn and error only', () => {
    assert.equal(shouldSurfaceToTelemetry('error'), true);
    assert.equal(shouldSurfaceToTelemetry('warn'), true);
    assert.equal(shouldSurfaceToTelemetry('info'), false);
    assert.equal(shouldSurfaceToTelemetry('debug'), false);
    assert.equal(shouldSurfaceToTelemetry('verbose'), false);
  });
});

describe('recordDiagnostic', () => {
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
  beforeEach(() => { __resetLoggerForTest(); process.env.AMI_DEBUG_LOG_LEVEL = 'verbose'; });
  afterEach(() => { mock.restoreAll(); __resetLoggerForTest(); process.env = { ...ORIG }; });

  it('writes to the debug log and pushes warn/error onto the in-memory ring', () => {
    const s = capture();
    recordDiagnostic('error', 'boom_event', { code: 7 });
    assert.equal(s.written.length, 1);
    assert.ok(s.written[0].includes('boom_event'));
    const ring = getInMemoryErrors();
    assert.equal(ring.length, 1);
    assert.ok(ring[0].error.includes('[diag:error] boom_event'));
    assert.ok(ring[0].error.includes('"code":7'));
  });

  it('does NOT push info/debug events onto the ring (only the debug log)', () => {
    const s = capture();
    recordDiagnostic('info', 'quiet_event');
    assert.equal(s.written.length, 1);
    assert.equal(getInMemoryErrors().length, 0);
  });
});

// ---------------------------------------------------------------------------
// withDiagnosticsTiming (U060 — timing wrapper + slow-op detection)
// ---------------------------------------------------------------------------
describe('withDiagnosticsTiming', () => {
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
  beforeEach(() => { __resetLoggerForTest(); process.env.AMI_DEBUG_LOG_LEVEL = 'verbose'; });
  afterEach(() => { mock.restoreAll(); __resetLoggerForTest(); process.env = { ...ORIG }; });

  it('emits started + completed events and returns the result', async () => {
    const s = capture();
    const result = await withDiagnosticsTiming('op', async () => 42);
    assert.equal(result, 42);
    const joined = s.written.join('');
    assert.ok(joined.includes('op_started'));
    assert.ok(joined.includes('op_completed'));
    assert.ok(joined.includes('duration_ms'));
  });

  it('merges getData fields into the completed event', async () => {
    const s = capture();
    await withDiagnosticsTiming('op', async () => ({ n: 5 }), (r) => ({ extracted: r.n }));
    assert.ok(s.written.join('').includes('"extracted":5'));
  });

  it('emits a failed event and rethrows the original error', async () => {
    const s = capture();
    const boom = new Error('kaboom');
    await assert.rejects(() => withDiagnosticsTiming('op', async () => { throw boom; }), /kaboom/);
    const joined = s.written.join('');
    assert.ok(joined.includes('op_failed'));
    // the failure surfaces on the in-memory ring (error severity)
    assert.ok(getInMemoryErrors().some((e) => e.error.includes('op_failed')));
  });

  it('emits [SLOW OPERATION DETECTED] when over AMI_SLOW_OPERATION_THRESHOLD_MS', async () => {
    const s = capture();
    process.env.AMI_SLOW_OPERATION_THRESHOLD_MS = '0';
    await withDiagnosticsTiming('slowop', async () => {
      await new Promise((r) => setTimeout(r, 5));
      return 'done';
    });
    assert.ok(s.written.join('').includes('[SLOW OPERATION DETECTED] slowop'));
  });

  it('does NOT warn when under the threshold (off by default)', async () => {
    const s = capture();
    await withDiagnosticsTiming('fastop', async () => 'done');
    assert.ok(!s.written.join('').includes('SLOW OPERATION DETECTED'));
  });
});
