import { test, describe, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  isReadOnlyBashCommand,
  BashResultCache,
} from '../src/tools/bash-cache';
import {
  isTestRunnerCommand,
  looksLikeSuiteLog,
  compactTestLog,
  foldToolOutputForPrompt,
} from '../src/tools/test-log';
import {
  setSearchableTools,
  toolSearchTool,
} from '../src/tools/tool-search';
import {
  todoWriteTool,
  resetTodos,
  getTodos,
} from '../src/tools/todo-write';
import {
  isSensitivePath,
  safeReadFile,
  safeLstat,
  detectEncoding,
  decodeBuffer,
  encodeText,
  isNonUtfEncoding,
} from '../src/tools/tool-utils';
import type { ToolDefinition, ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// Temp dir helpers
// ---------------------------------------------------------------------------
const tmpDirs: string[] = [];
function mkTmp(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'tools-exec-'));
  tmpDirs.push(d);
  return fs.realpathSync(d);
}
after(() => {
  for (const d of tmpDirs) {
    try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ }
  }
});

// ===========================================================================
// bash-cache.ts
// ===========================================================================
describe('bash-cache: isReadOnlyBashCommand', () => {
  test('simple read-only command returns true (no write indicators)', () => {
    assert.equal(isReadOnlyBashCommand('ls -la'), true);
  });

  test('output redirection (>) is not read-only', () => {
    assert.equal(isReadOnlyBashCommand('echo hi > out.txt'), false);
  });

  test('append redirection (>>) is not read-only', () => {
    assert.equal(isReadOnlyBashCommand('echo hi >> out.txt'), false);
  });

  test('pipe to tee is not read-only', () => {
    assert.equal(isReadOnlyBashCommand('ls | tee out.txt'), false);
  });

  test('pipe to dd is not read-only', () => {
    assert.equal(isReadOnlyBashCommand('cat x | dd of=y'), false);
  });

  test('command starting with cat skips the write-indicator short-circuit', () => {
    // `cat` is whitelisted at the front so the WRITE_INDICATORS block is skipped
    // and the segment check classifies cat as read-only.
    assert.equal(isReadOnlyBashCommand('cat file'), true);
  });

  test('command starting with grep skips the write-indicator short-circuit', () => {
    assert.equal(isReadOnlyBashCommand('grep foo file'), true);
  });

  test('empty segment (trailing pipe) is skipped', () => {
    assert.equal(isReadOnlyBashCommand('ls |  '), true);
  });

  test('git read-only subcommand is allowed', () => {
    assert.equal(isReadOnlyBashCommand('git status'), true);
  });

  test('git write subcommand is rejected', () => {
    assert.equal(isReadOnlyBashCommand('git push origin main'), false);
  });

  test('git with only flags (no subcommand) is rejected', () => {
    assert.equal(isReadOnlyBashCommand('git --version'), false);
  });

  test('absolute path whose basename is read-only is allowed', () => {
    assert.equal(isReadOnlyBashCommand('/bin/ls -l'), true);
  });

  test('absolute path whose basename is not read-only is rejected', () => {
    assert.equal(isReadOnlyBashCommand('/usr/bin/python script.py'), false);
  });

  test('unknown bare command is rejected', () => {
    assert.equal(isReadOnlyBashCommand('python script.py'), false);
  });

  test('pipeline of two read-only commands is allowed', () => {
    assert.equal(isReadOnlyBashCommand('cat file | grep foo'), true);
  });
});

describe('bash-cache: BashResultCache', () => {
  test('get returns null on miss then value after set', () => {
    const c = new BashResultCache();
    assert.equal(c.get('ls', '/cwd'), null);
    c.set('ls', '/cwd', 'out');
    assert.equal(c.get('ls', '/cwd'), 'out');
    assert.equal(c.size, 1);
  });

  test('expired entry is evicted and returns null', () => {
    const c = new BashResultCache(200, -1); // ttl -1 => always expired
    c.set('ls', '/cwd', 'out');
    assert.equal(c.get('ls', '/cwd'), null);
    assert.equal(c.size, 0);
  });

  test('maxSize eviction removes the oldest entry', () => {
    const c = new BashResultCache(1, 30_000);
    c.set('a', '/cwd', '1');
    c.set('b', '/cwd', '2');
    assert.equal(c.size, 1);
    assert.equal(c.get('a', '/cwd'), null);
    assert.equal(c.get('b', '/cwd'), '2');
  });

  test('invalidateForCwd removes only matching cwd', () => {
    const c = new BashResultCache();
    c.set('ls', '/one', 'a');
    c.set('ls', '/two', 'b');
    c.invalidateForCwd('/one');
    assert.equal(c.get('ls', '/one'), null);
    assert.equal(c.get('ls', '/two'), 'b');
  });

  test('clear empties the cache', () => {
    const c = new BashResultCache();
    c.set('ls', '/cwd', 'x');
    c.clear();
    assert.equal(c.size, 0);
  });
});

// ===========================================================================
// test-log.ts
// ===========================================================================
describe('test-log', () => {
  test('isTestRunnerCommand detects runners and rejects others', () => {
    assert.equal(isTestRunnerCommand('go test ./...'), true);
    assert.equal(isTestRunnerCommand('npm test'), true);
    assert.equal(isTestRunnerCommand('ls -la'), false);
  });

  test('looksLikeSuiteLog: empty / already-compacted returns false', () => {
    assert.equal(looksLikeSuiteLog(''), false);
    assert.equal(looksLikeSuiteLog('Showing failing output only'), false);
  });

  test('looksLikeSuiteLog: verbose --- PASS/FAIL suite', () => {
    const verbose = Array.from({ length: 9 }, (_, i) => `--- PASS: Test${i} (0.01s)`).join('\n');
    assert.equal(looksLikeSuiteLog(verbose), true);
  });

  test('compactTestLog: JSON go-test suite drops passes and keeps failures (formatJsonSuite)', () => {
    const lines = [
      '[build] starting',           // action null, starts with '['
      'plain uninteresting line',   // action null, dropped
      '{"Action":"pass","Test":"TestA"}',
      '{"Action":"pass","Test":"TestB"}',
      '{"Action":"pass","Test":"TestD"}',
      '{"Action":"fail","Test":"TestC"}',
      '{"Action":"output","Test":"TestC","Output":"FAIL: boom"}',
      '{"Action":"output","Test":"TestB","Output":"ok passing quietly"}',
      '{"Action":"run","Test":"TestD"}',
      '{"Action":"skip","Test":"TestE"}',
    ].join('\n');
    const out = compactTestLog(lines);
    assert.match(out, /Tests: 3 passed, 1 failed\. Showing failing output only\./);
    assert.match(out, /\[build\] starting/);          // null-action '[' line kept
    assert.match(out, /FAIL: boom/);                  // failing output kept
    assert.match(out, /"Action":"fail"/);             // fail event kept
    assert.doesNotMatch(out, /ok passing quietly/);   // passing output dropped
    assert.doesNotMatch(out, /plain uninteresting/);  // null non-'[' dropped
  });

  test('compactTestLog: JSON suite over cap is truncated', () => {
    const header = Array.from({ length: 8 }, (_, i) => `{"Action":"fail","Test":"T${i}"}`);
    const bulk = Array.from({ length: 2000 }, (_, i) => `{"Action":"fail","Test":"Big${i}","Output":"FAIL line ${i} ${'x'.repeat(40)}"}`);
    const out = compactTestLog([...header, ...bulk].join('\n'));
    assert.match(out, /\.\.\.\[failing output truncated\]/);
  });

  test('compactTestLog: already-compacted passthrough', () => {
    const txt = 'Showing failing output only\nsome stuff';
    assert.equal(compactTestLog(txt), txt);
    assert.equal(compactTestLog(''), '');
  });

  test('compactTestLog: short non-suite output returned verbatim', () => {
    assert.equal(compactTestLog('tiny output'), 'tiny output');
  });

  test('foldToolOutputForPrompt: non-suite passes through, suite compacts', () => {
    assert.equal(foldToolOutputForPrompt('hello'), 'hello');
    const verbose = Array.from({ length: 9 }, (_, i) => `--- PASS: Test${i} (0.01s)`).join('\n') + '\n[x] FAIL something';
    const folded = foldToolOutputForPrompt(verbose);
    assert.match(folded, /Showing failing output only/);
  });
});

// ===========================================================================
// tool-search.ts
// ===========================================================================
function mkTool(name: string, description: string): ToolDefinition {
  return {
    name,
    description,
    inputSchema: { type: 'object', properties: {}, required: [] },
    isReadOnly: true,
    async execute() { return { output: '', isError: false }; },
  } as ToolDefinition;
}

describe('tool-search', () => {
  const longDesc = 'x'.repeat(150);
  const tools = [
    mkTool('file_read', 'Read a file from disk'),
    mkTool('grep', 'Search files for a pattern with ' + longDesc),
    mkTool('web_fetch', 'Fetch a URL from the network'),
  ];

  test('empty query errors', async () => {
    setSearchableTools(tools);
    const r = await toolSearchTool.execute({ query: '   ' }, {} as ToolContext);
    assert.equal(r.isError, true);
    assert.match(r.output, /must not be empty/);
  });

  test('select: found returns schema and invokes _loadDeferredTool', async () => {
    setSearchableTools(tools);
    let loaded = '';
    const ctx = { _loadDeferredTool: (n: string) => { loaded = n; } } as unknown as ToolContext;
    const r = await toolSearchTool.execute({ query: 'select:file_read' }, ctx);
    assert.equal(r.isError, false);
    assert.equal(loaded, 'file_read');
    const parsed = JSON.parse(r.output);
    assert.equal(parsed.name, 'file_read');
  });

  test('select: found without _loadDeferredTool (optional chaining no-op)', async () => {
    setSearchableTools(tools);
    const r = await toolSearchTool.execute({ query: 'select:grep' }, {} as ToolContext);
    assert.equal(r.isError, false);
    assert.match(r.output, /"name": "grep"/);
  });

  test('select: not found errors with available list', async () => {
    setSearchableTools(tools);
    const r = await toolSearchTool.execute({ query: 'select:nope' }, {} as ToolContext);
    assert.equal(r.isError, true);
    assert.match(r.output, /not found/);
    assert.match(r.output, /file_read/);
  });

  test('keyword search ranks matches and truncates long descriptions', async () => {
    setSearchableTools(tools);
    const r = await toolSearchTool.execute({ query: 'file' }, {} as ToolContext);
    assert.equal(r.isError, false);
    assert.match(r.output, /file_read/);
    // grep has a >100 char description -> ellipsis appended
    const r2 = await toolSearchTool.execute({ query: 'pattern' }, {} as ToolContext);
    assert.match(r2.output, /grep/);
    assert.match(r2.output, /\.\.\./);
  });

  test('no matches returns non-error message with available list', async () => {
    setSearchableTools(tools);
    const r = await toolSearchTool.execute({ query: 'zzzznomatch' }, {} as ToolContext);
    assert.equal(r.isError, false);
    assert.match(r.output, /No tools matched/);
  });

  test('missing query property is treated as empty', async () => {
    setSearchableTools(tools);
    const r = await toolSearchTool.execute({}, {} as ToolContext);
    assert.equal(r.isError, true);
  });
});

// ===========================================================================
// todo-write.ts
// ===========================================================================
describe('todo-write', () => {
  test('replace mode with all status icons; undefined todos defaults to empty first', async () => {
    resetTodos();
    // undefined todos -> [] ; merge undefined -> false
    const empty = await todoWriteTool.execute({});
    assert.match(empty.output, /0 items/);

    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'done thing', status: 'completed' },
        { id: '2', content: 'working', status: 'in_progress' },
        { id: '3', content: 'cancelled thing', status: 'cancelled' },
        { id: '4', content: 'todo thing', status: 'pending' },
      ],
    });
    assert.match(r.output, /\[x\] 1: done thing/);
    assert.match(r.output, /\[>\] 2: working/);
    assert.match(r.output, /\[-\] 3: cancelled thing/);
    assert.match(r.output, /\[ \] 4: todo thing/);
    assert.equal(getTodos().length, 4);
  });

  test('merge mode updates existing by id and appends new', async () => {
    resetTodos();
    await todoWriteTool.execute({ todos: [{ id: 'a', content: 'first', status: 'pending' }] });
    const r = await todoWriteTool.execute({
      todos: [
        { id: 'a', content: 'first updated', status: 'completed' },
        { id: 'b', content: 'second', status: 'pending' },
      ],
      merge: true,
    });
    assert.match(r.output, /first updated/);
    assert.match(r.output, /second/);
    assert.equal(getTodos().length, 2);
  });

  test('3+ completed triggers verification suggestion', async () => {
    resetTodos();
    const r = await todoWriteTool.execute({
      todos: [
        { id: '1', content: 'a', status: 'completed' },
        { id: '2', content: 'b', status: 'completed' },
        { id: '3', content: 'c', status: 'completed' },
      ],
    });
    assert.match(r.output, /Verification suggested/);
  });
});

// ===========================================================================
// tool-utils.ts — sensitive paths, symlink reads, encodings
// ===========================================================================
describe('tool-utils: isSensitivePath', () => {
  test('matches /etc system prefix', () => {
    assert.equal(isSensitivePath('/etc/passwd'), true);
  });

  test('matches HOME sensitive dir and HOME sensitive file; false otherwise', () => {
    const fakeHome = mkTmp();
    const savedHome = process.env.HOME;
    const savedProfile = process.env.USERPROFILE;
    try {
      process.env.HOME = fakeHome;
      delete process.env.USERPROFILE;
      // SENSITIVE_HOME_DIRS: path under ~/.ssh
      assert.equal(isSensitivePath(path.join(fakeHome, '.ssh', 'id_rsa')), true);
      // SENSITIVE_HOME_FILES: ~/.npmrc exact file
      assert.equal(isSensitivePath(path.join(fakeHome, '.npmrc')), true);
      // non-sensitive path inside home
      assert.equal(isSensitivePath(path.join(fakeHome, 'project', 'file.ts')), false);
    } finally {
      if (savedHome === undefined) delete process.env.HOME; else process.env.HOME = savedHome;
      if (savedProfile !== undefined) process.env.USERPROFILE = savedProfile;
    }
  });

  test('returns false when no HOME/USERPROFILE set', () => {
    const savedHome = process.env.HOME;
    const savedProfile = process.env.USERPROFILE;
    try {
      delete process.env.HOME;
      delete process.env.USERPROFILE;
      assert.equal(isSensitivePath('/tmp/somewhere/else'), false);
    } finally {
      if (savedHome !== undefined) process.env.HOME = savedHome;
      if (savedProfile !== undefined) process.env.USERPROFILE = savedProfile;
    }
  });
});

describe('tool-utils: safeReadFile symlink handling', () => {
  test('rejects a symlink targeting a credential file', () => {
    const cwd = mkTmp();
    const sshDir = path.join(cwd, '.ssh');
    fs.mkdirSync(sshDir);
    const cred = path.join(sshDir, 'id_rsa');
    fs.writeFileSync(cred, 'PRIVATE');
    const link = path.join(cwd, 'innocent.txt');
    fs.symlinkSync(cred, link);
    const r = safeReadFile('innocent.txt', cwd);
    assert.ok(r.error);
    assert.match(r.error.output, /symlink targeting a sensitive file/);
  });

  test('reads through a symlink that stays inside the workspace', () => {
    const cwd = mkTmp();
    const real = path.join(cwd, 'real.txt');
    fs.writeFileSync(real, 'hello symlink');
    const link = path.join(cwd, 'link.txt');
    fs.symlinkSync(real, link);
    const r = safeReadFile('link.txt', cwd);
    assert.equal(r.error, undefined);
    assert.equal(r.content, 'hello symlink');
  });

  test('reads a plain file and reports an error for a missing file', () => {
    const cwd = mkTmp();
    fs.writeFileSync(path.join(cwd, 'a.txt'), 'data');
    const ok = safeReadFile('a.txt', cwd);
    assert.equal(ok.content, 'data');
    const miss = safeReadFile('missing.txt', cwd);
    assert.ok(miss.error);
    assert.match(miss.error.output, /cannot read/);
  });

  test('propagates resolveFilePath error for out-of-workspace path', () => {
    const cwd = mkTmp();
    const r = safeReadFile('/etc/passwd', cwd);
    assert.ok(r.error);
  });
});

describe('tool-utils: safeLstat', () => {
  test('rejects a path outside the workspace', () => {
    const cwd = mkTmp();
    const r = safeLstat('/etc/hosts', cwd);
    assert.ok(r.error);
    assert.match(r.error.output, /outside the workspace/);
  });

  test('returns stats for a regular file', () => {
    const cwd = mkTmp();
    fs.writeFileSync(path.join(cwd, 'f.txt'), 'x');
    const r = safeLstat('f.txt', cwd);
    assert.equal(r.error, undefined);
    assert.ok(r.stat?.isFile());
  });

  test('reports an error when the file is missing', () => {
    const cwd = mkTmp();
    const r = safeLstat('nope.txt', cwd);
    assert.ok(r.error);
    assert.match(r.error.output, /cannot stat/);
  });
});

describe('tool-utils: encoding detection & decode', () => {
  test('UTF-16LE BOM round-trip', () => {
    const buf = Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from('hi', 'utf16le')]);
    assert.equal(detectEncoding(buf), 'utf-16le');
    const { text, encoding } = decodeBuffer(buf);
    assert.equal(text, 'hi');
    assert.equal(encoding, 'utf-16le');
    const re = encodeText(text, 'utf-16le');
    assert.deepEqual(re, buf);
  });

  test('UTF-16BE BOM decode', () => {
    const body = Buffer.from('hi', 'utf16le');
    body.swap16();
    const buf = Buffer.concat([Buffer.from([0xFE, 0xFF]), body]);
    assert.equal(detectEncoding(buf), 'utf-16be');
    const { text } = decodeBuffer(buf);
    assert.equal(text, 'hi');
  });

  test('UTF-8 BOM and plain UTF-8', () => {
    const bom = Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from('hi', 'utf-8')]);
    assert.equal(detectEncoding(bom), 'utf-8-bom');
    assert.equal(decodeBuffer(bom).text, 'hi');
    const plain = Buffer.from('plain', 'utf-8');
    assert.equal(detectEncoding(plain), 'utf-8');
    assert.equal(decodeBuffer(plain).text, 'plain');
  });

  test('encodeText round-trips every encoding', () => {
    // utf-8-bom
    const bom = encodeText('hi', 'utf-8-bom');
    assert.deepEqual(bom.subarray(0, 3), Buffer.from([0xEF, 0xBB, 0xBF]));
    assert.equal(decodeBuffer(bom).text, 'hi');
    // utf-16be
    const be = encodeText('hi', 'utf-16be');
    assert.deepEqual(be.subarray(0, 2), Buffer.from([0xFE, 0xFF]));
    assert.equal(decodeBuffer(be).text, 'hi');
    // utf-16le
    const le = encodeText('hi', 'utf-16le');
    assert.deepEqual(le.subarray(0, 2), Buffer.from([0xFF, 0xFE]));
    assert.equal(decodeBuffer(le).text, 'hi');
    // plain utf-8
    const plain = encodeText('hi', 'utf-8');
    assert.equal(plain.toString('utf-8'), 'hi');
  });
});

describe('tool-utils: isNonUtfEncoding', () => {
  test('plain ASCII is valid UTF-8', () => {
    assert.equal(isNonUtfEncoding(Buffer.from('hello world', 'utf-8')), false);
  });

  test('buffer with a BOM is never treated as legacy encoding', () => {
    const bom = Buffer.concat([Buffer.from([0xFF, 0xFE]), Buffer.from('x', 'utf16le')]);
    assert.equal(isNonUtfEncoding(bom), false);
  });

  test('valid multi-byte UTF-8 sequences (2/3/4 byte) are valid', () => {
    // é (2-byte), € (3-byte), 😀 (4-byte)
    assert.equal(isNonUtfEncoding(Buffer.from('é € 😀 café', 'utf-8')), false);
  });

  test('3+ invalid high-byte runs are flagged as legacy encoding', () => {
    // Windows-1252 / Shift-JIS style: isolated high bytes not forming valid UTF-8.
    const buf = Buffer.from([0x41, 0xE9, 0x42, 0xE8, 0x43, 0xFC, 0x44, 0xDF, 0x45]);
    assert.equal(isNonUtfEncoding(buf), true);
  });

  test('a single invalid high byte is tolerated (under the 3-run threshold)', () => {
    const buf = Buffer.from([0x41, 0x42, 0xFF, 0x43, 0x44]);
    assert.equal(isNonUtfEncoding(buf), false);
  });

  test('truncated 2-byte lead at buffer end counts as invalid', () => {
    const buf = Buffer.from([0x41, 0xC3, 0x42, 0xC3, 0x43, 0xC3]);
    assert.equal(isNonUtfEncoding(buf), true);
  });

  test('truncated 3-byte and 4-byte leads count as invalid runs', () => {
    const buf = Buffer.from([0xE2, 0x82, 0xE2, 0x82, 0xF0, 0x9F, 0x98]);
    // two truncated 3-byte (0xE2 0x82 then EOF-ish) + truncated 4-byte -> >=3 runs
    assert.equal(isNonUtfEncoding(buf), true);
  });
});
