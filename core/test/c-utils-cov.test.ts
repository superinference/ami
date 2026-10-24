// ===========================================================================
// Final coverage closure for tool-utils.ts, web-utils.ts, workflow.ts.
//
// Each test drives a branch/function that the existing suites leave uncovered
// under the native-ESM coverage harness:
//   - tool-utils: buildToolDescriptionContext defensive catch arms, and
//     safeReadFile's post-resolveFilePath symlink re-check (TOCTOU swap guard).
//   - web-utils:  the successful redirect-follow recursion in httpGetInternal.
//   - workflow:   agentHandler's `subEngine.shutdown?.()` short-circuit when the
//     sub-engine exposes no shutdown method.
//
// ESM-safe: no readFileSync-of-source, no source.includes assertions, no
// namespace monkeypatching (seams are plain mutable export objects), temp dirs
// via mkdtempSync, mock server bound to port 0.
// ===========================================================================
import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as http from 'http';
import { EventEmitter } from 'node:events';

import {
  buildToolDescriptionContext,
  safeReadFile,
  _toolUtilsHooks,
} from '../src/tools/tool-utils';
import { httpGet, _webHooks, MAX_HTTP_BODY_BYTES } from '../src/tools/web-utils';
import { workflowTool } from '../src/tools/workflow';
import type { ToolContext } from '../src/types';

function mkTmp(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'si-cutils-'));
}

// ---------------------------------------------------------------------------
// tool-utils: buildToolDescriptionContext — defensive catch arms
// ---------------------------------------------------------------------------

describe('buildToolDescriptionContext — fs failure catch arms', () => {
  it('falls back to false when the existence probe throws', () => {
    const orig = _toolUtilsHooks.existsSync;
    _toolUtilsHooks.existsSync = () => {
      throw new Error('simulated fs failure');
    };
    try {
      const ctx = buildToolDescriptionContext('/some/project/dir');
      assert.equal(ctx.cwd, '/some/project/dir');
      assert.equal(ctx.projectName, 'dir');
      assert.equal(ctx.hasGit, false);
      assert.equal(ctx.hasPackageJson, false);
    } finally {
      _toolUtilsHooks.existsSync = orig;
    }
  });
});

// ---------------------------------------------------------------------------
// tool-utils: safeReadFile — symlink re-check (TOCTOU swap) outside workspace
// ---------------------------------------------------------------------------

describe('safeReadFile — symlink swapped to point outside the workspace', () => {
  const dirs: string[] = [];
  after(() => dirs.forEach((d) => fs.rmSync(d, { recursive: true, force: true })));

  it('rejects when the final realpath resolves outside the workspace', () => {
    const cwd = mkTmp();
    dirs.push(cwd);
    const outside = mkTmp();
    dirs.push(outside);

    // A real, in-workspace symlink chain so resolveFilePath (which uses the real
    // fs) passes, and lstat reports a symlink.
    const real = path.join(cwd, 'real.txt');
    fs.writeFileSync(real, 'data');
    fs.symlinkSync(real, path.join(cwd, 'link.txt'));

    // Simulate the TOCTOU race: between resolveFilePath's resolution and
    // safeReadFile's re-check, the symlink now resolves outside the workspace
    // (and is not a credential path, so the earlier arm does not fire).
    const orig = _toolUtilsHooks.realpathSync;
    _toolUtilsHooks.realpathSync = () => path.join(outside, 'swapped.txt');
    try {
      const r = safeReadFile('link.txt', cwd);
      assert.ok(r.error);
      assert.ok(r.error!.isError);
      assert.match(r.error!.output, /symlink resolving outside the workspace/);
    } finally {
      _toolUtilsHooks.realpathSync = orig;
    }
  });

  it('stringifies a non-Error thrown while re-reading a symlink', () => {
    const cwd = mkTmp();
    dirs.push(cwd);
    const real = path.join(cwd, 'real.txt');
    fs.writeFileSync(real, 'data');
    fs.symlinkSync(real, path.join(cwd, 'link.txt'));

    // A non-Error value forces the `String(err)` arm of the catch ternary.
    const notAnError = { reason: 'synthetic failure' };
    const orig = _toolUtilsHooks.realpathSync;
    _toolUtilsHooks.realpathSync = () => { throw notAnError; };
    try {
      const r = safeReadFile('link.txt', cwd);
      assert.ok(r.error);
      assert.ok(r.error!.isError);
      assert.match(r.error!.output, /cannot read/);
    } finally {
      _toolUtilsHooks.realpathSync = orig;
    }
  });
});

// ---------------------------------------------------------------------------
// web-utils: httpGet — successful redirect follow (recursion)
// ---------------------------------------------------------------------------

describe('httpGet — follows a safe redirect to completion', () => {
  let server: http.Server;
  let base: string;
  let port: number;

  before(async () => {
    server = http.createServer((req, res) => {
      if (req.url === '/redirect-ok') {
        // Target host is non-loopback so isBlockedRedirectTarget passes; the
        // seam supplies a loopback resolvedIP so the recursion connects back.
        res.writeHead(302, { Location: `http://redirect-target.invalid:${port}/ok` });
        res.end();
        return;
      }
      if (req.url === '/ok') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('final body after redirect');
        return;
      }
      res.writeHead(404);
      res.end('nope');
    });
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', () => r()));
    port = (server.address() as { port: number }).port;
    base = `http://127.0.0.1:${port}`;
  });

  after(async () => {
    (server as unknown as { closeAllConnections?: () => void }).closeAllConnections?.();
    await new Promise<void>((r) => server.close(() => r()));
  });

  it('returns the final body after following a 302', async () => {
    const origValidate = _webHooks.validateRedirectSafety;
    _webHooks.validateRedirectSafety = async () => ({ resolvedIP: '127.0.0.1' });
    try {
      const res = await httpGet(`${base}/redirect-ok`, new AbortController().signal, {
        resolvedIP: '127.0.0.1',
      });
      assert.equal(res.statusCode, 200);
      assert.equal(res.body, 'final body after redirect');
      assert.equal(res.finalUrl, `http://redirect-target.invalid:${port}/ok`);
    } finally {
      _webHooks.validateRedirectSafety = origValidate;
    }
  });
});

// ---------------------------------------------------------------------------
// web-utils: httpGet — defensive response arms via a synthetic response
// ---------------------------------------------------------------------------

describe('httpGet — synthetic response defensive arms', () => {
  it('defaults an absent statusCode to 0 and ignores data after settling', async () => {
    const orig = _webHooks.rawGet;
    _webHooks.rawGet = (_requester, _url, _options, callback) => {
      const res = new EventEmitter() as unknown as http.IncomingMessage & { destroy: () => void };
      // No statusCode set — drives `res.statusCode ?? 0`.
      (res as unknown as { statusCode?: number }).statusCode = undefined;
      (res as unknown as { headers: Record<string, string> }).headers = {};
      (res as unknown as { resume: () => void }).resume = () => {};
      (res as unknown as { destroy: () => void }).destroy = () => {};
      const req = new EventEmitter() as unknown as http.ClientRequest;
      (req as unknown as { destroy: () => void }).destroy = () => {};
      callback(res as http.IncomingMessage);
      process.nextTick(() => {
        // First chunk crosses the cap: settles + destroys.
        res.emit('data', Buffer.alloc(MAX_HTTP_BODY_BYTES + 16, 0x61));
        // Late chunk after settle: must hit the `if (settled) return` guard.
        res.emit('data', Buffer.from('late-bytes'));
      });
      return req;
    };
    try {
      const r = await httpGet('http://example.test/x', new AbortController().signal);
      assert.equal(r.statusCode, 0);
      assert.equal(r.truncated, true);
      assert.equal(Buffer.byteLength(r.body), MAX_HTTP_BODY_BYTES);
    } finally {
      _webHooks.rawGet = orig;
    }
  });
});

// ---------------------------------------------------------------------------
// workflow: agentHandler — shutdown?. short-circuit (no shutdown method)
// ---------------------------------------------------------------------------

describe('workflowTool — sub-engine without a shutdown method', () => {
  let tmpDir: string;
  before(() => { tmpDir = mkTmp(); });
  after(() => fs.rmSync(tmpDir, { recursive: true, force: true }));

  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: tmpDir,
      abortSignal: new AbortController().signal,
      ...overrides,
    } as ToolContext;
  }

  it('completes when the sub-engine exposes no shutdown()', async () => {
    const script = `export const meta = { name: 'no-shutdown-wf', description: 'test' }
const result = await agent('do something');
log(String(result));
`;
    const r = await workflowTool.execute(
      { script },
      ctx({
        _providerConfig: { baseUrl: 'http://test', apiKey: 'k', model: 'm' } as never,
        _engineFactory: (() => ({
          submit: async function* () {
            yield { type: 'text_delta', text: 'engine said hi' };
          },
          // Intentionally no shutdown() — exercises the optional-chaining arm.
        })) as never,
      }),
    );
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('engine said hi'));
  });

  it('passes a numeric maxConcurrent through to the workflow runner', async () => {
    const script = `export const meta = { name: 'maxconc-wf', description: 'test' }
log('ran with cap');
`;
    const r = await workflowTool.execute({ script, maxConcurrent: 2 }, ctx());
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('ran with cap'));
  });
});
