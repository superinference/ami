import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { fileEditTool } from '../src/tools/file-edit';
import { getErrnoCode } from '../src/tools/tool-utils';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return { cwd: tmpDir, abortSignal: new AbortController().signal, filesRead: new Set<string>(), ...overrides };
}

beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fe-errno-')); });
afterEach(() => { fs.rmSync(tmpDir, { recursive: true, force: true }); });

describe('getErrnoCode', () => {
  it('extracts a string code or returns undefined', () => {
    assert.equal(getErrnoCode(Object.assign(new Error('x'), { code: 'EACCES' })), 'EACCES');
    assert.equal(getErrnoCode(new Error('no code')), undefined);
    assert.equal(getErrnoCode({ code: 42 }), undefined);
    assert.equal(getErrnoCode(null), undefined);
  });
});

describe('file_edit write – errno classification on reading existing content', () => {
  it('treats a genuinely absent file (ENOENT) as a new file and creates it', async () => {
    const p = path.join(tmpDir, 'brand-new.txt');
    const result = await fileEditTool.execute({ file_path: p, new_string: 'hello\n' }, ctx());
    assert.ok(!result.isError, result.output);
    assert.equal(fs.readFileSync(p, 'utf-8'), 'hello\n');
  });

  it('surfaces a non-ENOENT read error instead of silently overwriting an unreadable file', async () => {
    // An existing-but-unreadable file (EACCES) may hold real content that must not
    // be clobbered by treating the read failure as "file is new".
    const secret = path.join(tmpDir, 'secret.txt');
    fs.writeFileSync(secret, 'confidential contents that must survive\n');
    fs.chmodSync(secret, 0o000);
    let readable = true;
    try { fs.readFileSync(secret); } catch { readable = false; }
    if (readable) { fs.chmodSync(secret, 0o644); return; } // running as root — EACCES not enforced; skip

    const result = await fileEditTool.execute(
      { file_path: secret, new_string: 'overwrite' },
      ctx({ filesRead: new Set([secret]) }),
    );
    fs.chmodSync(secret, 0o644);
    assert.equal(result.isError, true);
    assert.ok(
      /EACCES/.test(result.output) || /cannot read existing file/.test(result.output),
      result.output,
    );
    // The original content must be intact (not clobbered).
    assert.equal(fs.readFileSync(secret, 'utf-8'), 'confidential contents that must survive\n');
  });
});
