import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  ACTION_VERBS,
  describeAction,
  outsideWorkspaceMessage,
  resolveFilePath,
  resolveSearchPath,
  validatePatternAndPath,
} from '../src/tools/tool-utils';
import { redactCredentials } from '../src/credential-guard';
import { grepTool } from '../src/tools/grep';
import { globTool } from '../src/tools/glob';
import { fileReadTool } from '../src/tools/file-read';
import { fileEditTool } from '../src/tools/file-edit';

// ---------------------------------------------------------------------------
// U069 (w78) — tool-utils.ts / credential-guard.ts Claude Code parity additions:
//  - ACTION_VERBS + describeAction: out-of-workspace denials name the operation
//    in human terms (Claude Code BashTool/pathValidation.ts ACTION_VERBS) in
//    ADDITION to the already-present allowed-roots directory list.
//  - Field-name secret coverage for Claude's debug SECRET_FIELD_NAMES that the
//    shape/assignment rules missed (session_ingress_token / environment_secret).
// ---------------------------------------------------------------------------

describe('ACTION_VERBS / describeAction', () => {
  it('maps each supported operation to a human verb phrase', () => {
    assert.equal(ACTION_VERBS.read, 'read files in');
    assert.equal(ACTION_VERBS.write, 'write files in');
    assert.equal(ACTION_VERBS.edit, 'edit files in');
    assert.equal(ACTION_VERBS.list, 'list files in');
    assert.equal(ACTION_VERBS.search, 'search for patterns in files in');
    assert.equal(ACTION_VERBS.glob, 'find files in');
  });

  it('describeAction returns the verb for a known operation', () => {
    assert.equal(describeAction('read'), 'read files in');
    assert.equal(describeAction('search'), 'search for patterns in files in');
  });

  it('describeAction falls back to a generic verb for unknown/absent op', () => {
    assert.equal(describeAction(undefined), 'access');
    assert.equal(describeAction('nonexistent-op'), 'access');
  });
});

describe('outsideWorkspaceMessage', () => {
  it('names the operation AND lists the allowed roots', () => {
    const msg = outsideWorkspaceMessage('/etc/passwd', '/home/u/proj', ['/home/u/extra'], 'read');
    assert.match(msg, /outside the workspace directory \(cwd: \/home\/u\/proj\)/);
    assert.match(msg, /read files in/);
    assert.match(msg, /Accessible roots for this session: '\/home\/u\/proj', '\/home\/u\/extra'/);
  });

  it('still produces a verb clause when there are no extra roots', () => {
    const msg = outsideWorkspaceMessage('/etc/passwd', '/home/u/proj', [], 'search');
    assert.match(msg, /search for patterns in files in/);
    assert.match(msg, /outside the workspace directory/);
  });

  it('uses the generic verb when no operation is supplied', () => {
    const msg = outsideWorkspaceMessage('/etc/passwd', '/home/u/proj', []);
    assert.match(msg, /may only access the allowed working directories/);
  });
});

describe('resolveFilePath threads the operation verb into denials', () => {
  let dir: string;
  it('read denial names the read operation', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w78-'));
    try {
      const r = resolveFilePath('/etc/hosts', path.join(dir, 'proj'), [], 'read');
      // /etc/hosts is outside the (non-existent) proj cwd.
      assert.ok(r.error, 'expected an out-of-workspace error');
      assert.match(r.error!.output, /read files in/);
      assert.match(r.error!.output, /outside the workspace directory/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('write denial names the write operation', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w78-'));
    try {
      const r = resolveFilePath('/etc/hosts', path.join(dir, 'proj'), [], 'write');
      assert.ok(r.error);
      assert.match(r.error!.output, /write files in/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });

  it('defaults to the generic verb when no operation is passed', () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'u069w78-'));
    try {
      const r = resolveFilePath('/etc/hosts', path.join(dir, 'proj'), []);
      assert.ok(r.error);
      assert.match(r.error!.output, /may only access the allowed working directories/);
    } finally {
      fs.rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('resolveSearchPath / validatePatternAndPath thread the operation verb', () => {
  it('resolveSearchPath search denial names the search operation', () => {
    const r = resolveSearchPath('/etc', '/home/u/proj', [], 'search');
    assert.ok(r.error, 'expected an out-of-workspace error');
    assert.match(r.error!, /search for patterns in files in/);
    assert.match(r.error!, /outside the workspace directory/);
  });

  it('validatePatternAndPath forwards the operation verb (glob)', () => {
    const v = validatePatternAndPath('*.ts', '/etc', '/home/u/proj', [], 'glob');
    assert.ok(v.error, 'expected an out-of-workspace error');
    assert.match(v.error!.output, /find files in/);
  });
});

describe('END-TO-END: real tools surface the operation verb in out-of-workspace denials', () => {
  let parent: string;
  let ws: string;
  let outside: string;
  const ctx = () => ({ cwd: ws, abortSignal: new AbortController().signal });

  function setup(): void {
    parent = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'u069w78-e2e-')));
    ws = fs.realpathSync(fs.mkdtempSync(path.join(parent, 'ws-')));
    outside = fs.realpathSync(fs.mkdtempSync(path.join(parent, 'outside-')));
  }
  function teardown(): void { fs.rmSync(parent, { recursive: true, force: true }); }

  it('grep names the search operation when the path escapes the workspace', async () => {
    setup();
    try {
      const r = await grepTool.execute({ pattern: 'anything', path: outside }, ctx());
      assert.equal(r.isError, true);
      assert.match(r.output, /search for patterns in files in/);
      assert.match(r.output, /outside the workspace directory/);
    } finally { teardown(); }
  });

  it('glob names the find operation when the path escapes the workspace', async () => {
    setup();
    try {
      const r = await globTool.execute({ pattern: '**/*.ts', path: outside }, ctx());
      assert.equal(r.isError, true);
      assert.match(r.output, /find files in/);
    } finally { teardown(); }
  });

  it('file_read names the read operation when the path escapes the workspace', async () => {
    setup();
    try {
      const target = path.join(outside, 'secret.txt');
      fs.writeFileSync(target, 'data');
      const r = await fileReadTool.execute({ file_path: target }, ctx());
      assert.equal(r.isError, true);
      assert.match(r.output, /read files in/);
    } finally { teardown(); }
  });

  it('file_edit names the write operation when the path escapes the workspace', async () => {
    setup();
    try {
      const target = path.join(outside, 'secret.txt');
      fs.writeFileSync(target, 'old');
      const r = await fileEditTool.execute(
        { file_path: target, old_string: 'old', new_string: 'new' },
        ctx(),
      );
      assert.equal(r.isError, true);
      assert.match(r.output, /write files in/);
    } finally { teardown(); }
  });
});

describe('credential-guard field-name coverage for Claude debug SECRET_FIELD_NAMES', () => {
  it('redacts a session_ingress_token JSON field value', () => {
    const out = redactCredentials('{"session_ingress_token":"abcd1234efgh5678 ok"}');
    assert.ok(!out.includes('abcd1234efgh5678'), 'session_ingress_token value must be redacted');
    assert.ok(out.includes('[REDACTED]'));
  });

  it('redacts an environment_secret JSON field value', () => {
    const out = redactCredentials('{"environment_secret":"abcd1234efgh5678 ok"}');
    assert.ok(!out.includes('abcd1234efgh5678'), 'environment_secret value must be redacted');
    assert.ok(out.includes('[REDACTED]'));
  });

  it('still redacts short opaque values (unconditional field names)', () => {
    const out = redactCredentials('{"session_ingress_token":"short ok"}');
    assert.ok(!out.includes('"short"'));
    assert.ok(out.includes('[REDACTED]'));
  });

  it('partial (fingerprint) mode keeps first-8/last-4 for a long value', () => {
    const secret = 'abcdefgh0000000000wxyz';
    const out = redactCredentials(`{"environment_secret":"${secret}"}`, { partial: true });
    assert.ok(out.includes('abcdefgh'), 'first 8 chars retained for correlation');
    assert.ok(out.includes('wxyz'), 'last 4 chars retained for correlation');
    assert.ok(!out.includes(secret), 'full secret never surfaced');
  });
});
