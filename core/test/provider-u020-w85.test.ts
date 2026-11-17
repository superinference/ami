import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { withRequestIdDebugTrace, REQUEST_ID_HEADER } from '../src/provider';
import { __resetLoggerForTest } from '../src/logger';

// ---------------------------------------------------------------------------
// U020 w85 — provider-SDK transport diagnostics (response status + errors)
//
// Parity gap: references/claude/claude-code-main/src/services/api/client.ts
// createStderrLogger (L73-86) forwards provider-SDK ERROR/WARN/INFO/DEBUG lines
// to the debug sink. AMI already traces the REQUEST (withRequestIdDebugTrace) and
// routes debug output to stderr when isDebugToStderr(); the missing legs are the
// RESPONSE status (INFO/DEBUG) and the transport ERROR (ERROR), which this test
// pins down. Completes the request→response→error trace triad on the one fetch
// wrapper that is already composed into every provider path.
// ---------------------------------------------------------------------------

function withDebugLog<T>(fn: (file: string) => Promise<T>): Promise<T> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u020-w85-'));
  const file = path.join(dir, 'engine.log');
  const prevFile = process.env.AMI_DEBUG_FILE;
  const prevLevel = process.env.AMI_DEBUG_LOG_LEVEL;
  const prevDisable = process.env.AMI_DISABLE_DEBUG_LOG;
  process.env.AMI_DEBUG_FILE = file;
  process.env.AMI_DEBUG_LOG_LEVEL = 'debug';
  delete process.env.AMI_DISABLE_DEBUG_LOG;
  __resetLoggerForTest();
  const restore = () => {
    if (prevFile === undefined) delete process.env.AMI_DEBUG_FILE; else process.env.AMI_DEBUG_FILE = prevFile;
    if (prevLevel === undefined) delete process.env.AMI_DEBUG_LOG_LEVEL; else process.env.AMI_DEBUG_LOG_LEVEL = prevLevel;
    if (prevDisable === undefined) delete process.env.AMI_DISABLE_DEBUG_LOG; else process.env.AMI_DISABLE_DEBUG_LOG = prevDisable;
    __resetLoggerForTest();
    fs.rmSync(dir, { recursive: true, force: true });
  };
  return fn(file).then(
    (v) => { restore(); return v; },
    (e) => { restore(); throw e; },
  );
}

describe('withRequestIdDebugTrace — response/error SDK diagnostics (w85)', () => {
  it('logs an [API RESPONSE] line with the status on a successful request', async () => {
    await withDebugLog(async (file) => {
      const base = (async () => new Response('{}', { status: 200 })) as unknown as typeof globalThis.fetch;
      const res = await withRequestIdDebugTrace(base, 'anthropic')('https://api.anthropic.com/v1/messages', {} as RequestInit);
      assert.equal((res as Response).status, 200, 'the live response must be returned unchanged');
      const contents = fs.readFileSync(file, 'utf8');
      assert.ok(contents.includes('[API RESPONSE]'), 'must emit the API RESPONSE marker');
      assert.ok(contents.includes('status=200'), 'must include the HTTP status');
      assert.ok(contents.includes('/v1/messages'), 'must include the pathname');
      assert.ok(contents.includes('source=anthropic'), 'must include the source label');
    });
  });

  it('logs the non-ok status at WARN level (so it surfaces even at a higher log level)', async () => {
    await withDebugLog(async (file) => {
      const base = (async () => new Response('bad', { status: 429 })) as unknown as typeof globalThis.fetch;
      await withRequestIdDebugTrace(base, 'openai')('https://host/v1/chat/completions', {} as RequestInit);
      const contents = fs.readFileSync(file, 'utf8');
      assert.match(contents, /\[WARN\].*\[API RESPONSE\].*status=429/, 'a non-ok status must be logged at WARN');
    });
  });

  it('logs an [API ERROR] line and re-throws the original error on a transport failure', async () => {
    await withDebugLog(async (file) => {
      const boom = Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
      const base = (async () => { throw boom; }) as unknown as typeof globalThis.fetch;
      await assert.rejects(
        () => withRequestIdDebugTrace(base, 'anthropic')('https://api.anthropic.com/v1/messages', {} as RequestInit),
        (e) => e === boom,
        'the original error object must propagate unchanged',
      );
      const contents = fs.readFileSync(file, 'utf8');
      assert.match(contents, /\[ERROR\].*\[API ERROR\].*socket hang up/, 'the transport error must be logged at ERROR');
      assert.ok(contents.includes('source=anthropic'), 'the error line must carry the source label');
    });
  });

  it('still returns the response even if the request id header is present', async () => {
    await withDebugLog(async (file) => {
      const base = (async () => new Response('{}', { status: 201 })) as unknown as typeof globalThis.fetch;
      const res = await withRequestIdDebugTrace(base, 'x')('https://h/p', { headers: { [REQUEST_ID_HEADER]: 'rid-1' } } as RequestInit);
      assert.equal((res as Response).status, 201);
      const contents = fs.readFileSync(file, 'utf8');
      assert.ok(contents.includes('status=201'));
    });
  });
});
