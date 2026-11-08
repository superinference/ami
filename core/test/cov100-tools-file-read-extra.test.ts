import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileReadTool } from '../src/tools/file-read';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

// Covers the resolveFilePath error short-circuit in fileReadTool.execute
// (file-read.ts lines 247-248: `const { resolved, error: pathError } =
// resolveFilePath(...); if (pathError) return pathError;`). This path fires
// whenever resolveFilePath reports a problem — an absolute path that lands
// outside the workspace, or a sensitive system path — before any access/stat
// happens. We assert the returned ToolResult IS that error verbatim.

let workspace: string;
let outside: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: workspace,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-ws-'));
  outside = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-out-'));
  getFileCache(workspace).clear();
});

afterEach(() => {
  fs.rmSync(workspace, { recursive: true, force: true });
  fs.rmSync(outside, { recursive: true, force: true });
});

describe('fileReadTool – resolveFilePath error short-circuit (lines 247-248)', () => {
  it('returns the "outside the workspace" error for an absolute path outside cwd', async () => {
    // The file genuinely exists and is readable — so if the resolveFilePath
    // guard did NOT fire, the read would succeed. The error proves 247-248 ran.
    const target = path.join(outside, 'secret.txt');
    fs.writeFileSync(target, 'top secret contents\n');

    const result = await fileReadTool.execute({ file_path: target }, ctx());

    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('outside the workspace directory'),
      `expected workspace-boundary error, got: ${result.output}`,
    );
    // The real file contents must NOT leak — the guard short-circuited the read.
    assert.ok(!result.output.includes('top secret contents'));
    // The error references the path the caller passed.
    assert.ok(result.output.includes(target));
  });

  it('returns the "sensitive system path" error for a blocked system file', async () => {
    // /etc/passwd is flagged by isSensitivePath inside resolveFilePath, which
    // returns its error before the later access/lstat checks — still routed out
    // through the lines 247-248 short-circuit.
    const result = await fileReadTool.execute({ file_path: '/etc/passwd' }, ctx());

    assert.equal(result.isError, true);
    assert.ok(
      result.output.includes('sensitive system path') ||
        result.output.includes('outside the workspace'),
      `expected sensitive/workspace block, got: ${result.output}`,
    );
    assert.ok(result.output.startsWith('Error:'));
  });

  it('returns a resolveFilePath error object, not a thrown exception', async () => {
    // Guards that the error path is a graceful ToolResult (isError:true), never
    // a rejected promise — the caller returns pathError directly.
    const target = path.join(outside, 'nested', 'deep.txt');
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, 'data');

    const result = await assert.doesNotReject(async () =>
      fileReadTool.execute({ file_path: target }, ctx()),
    ).then(() => fileReadTool.execute({ file_path: target }, ctx()));

    assert.equal(result.isError, true);
    assert.equal(typeof result.output, 'string');
  });
});
