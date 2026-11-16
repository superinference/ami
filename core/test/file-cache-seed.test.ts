import { describe, it, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  collectReadFileStatesFromTranscript,
  seedFileCacheFromTranscript,
  stripLineNumberPrefix,
} from '../src/file-cache-seed';
import { FileCache, getFileCache } from '../src/file-cache';
import type { Message } from '../src/types';

function makeTmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'file-cache-seed-test-'));
}
function cleanupDir(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true });
}

/** Build an assistant message with a single tool call. */
function toolCall(id: string, name: string, args: Record<string, unknown>): Message {
  return {
    role: 'assistant',
    content: null,
    tool_calls: [{ id, type: 'function', function: { name, arguments: JSON.stringify(args) } }],
  };
}
/** Build a tool result message. */
function toolResult(id: string, content: string, isError = false): Message {
  return { role: 'tool', tool_call_id: id, content, isError };
}

const dirs: string[] = [];
afterEach(() => {
  while (dirs.length) cleanupDir(dirs.pop()!);
});
function tmp(): string {
  const d = makeTmpDir();
  dirs.push(d);
  return d;
}

describe('stripLineNumberPrefix', () => {
  it('strips a tab-separated cat -n prefix', () => {
    assert.equal(stripLineNumberPrefix('  42\tconst x = 1;'), 'const x = 1;');
  });
  it('strips an arrow-separated prefix', () => {
    assert.equal(stripLineNumberPrefix('7→hello'), 'hello');
  });
  it('leaves a line without a numeric prefix unchanged', () => {
    assert.equal(stripLineNumberPrefix('no prefix here'), 'no prefix here');
    assert.equal(stripLineNumberPrefix('123 no-separator'), '123 no-separator');
  });
});

describe('collectReadFileStatesFromTranscript', () => {
  it('recovers a full file_read, stripping line numbers + system-reminders, paired with disk mtime', () => {
    const cwd = tmp();
    const fp = path.join(cwd, 'a.ts');
    fs.writeFileSync(fp, 'line one\nline two');
    const mtime = fs.statSync(fp).mtimeMs;
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: fp }),
      toolResult('t1', '1\tline one\n2\tline two\n<system-reminder>ignore me</system-reminder>'),
    ];
    const states = collectReadFileStatesFromTranscript(messages, cwd);
    assert.equal(states.length, 1);
    assert.equal(states[0].path, fp);
    assert.equal(states[0].content, 'line one\nline two');
    assert.equal(states[0].mtime, mtime);
  });

  it('expands a relative file_path against cwd', () => {
    const cwd = tmp();
    fs.writeFileSync(path.join(cwd, 'rel.ts'), 'hi');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: 'rel.ts' }),
      toolResult('t1', '1\thi'),
    ];
    const states = collectReadFileStatesFromTranscript(messages, cwd);
    assert.equal(states[0].path, path.join(cwd, 'rel.ts'));
  });

  it('skips ranged reads (offset/limit present)', () => {
    const cwd = tmp();
    fs.writeFileSync(path.join(cwd, 'r.ts'), 'x');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: path.join(cwd, 'r.ts'), offset: 5, limit: 10 }),
      toolResult('t1', '6\tx'),
    ];
    assert.equal(collectReadFileStatesFromTranscript(messages, cwd).length, 0);
  });

  it('skips a FILE_UNCHANGED_STUB dedup result (no content to cache)', () => {
    const cwd = tmp();
    fs.writeFileSync(path.join(cwd, 'u.ts'), 'x');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: path.join(cwd, 'u.ts') }),
      toolResult('t1', 'File unchanged since last read. The content from the earlier Read tool_result in this conversation is still current — refer to that instead of re-reading. (x, 1 lines)'),
    ];
    assert.equal(collectReadFileStatesFromTranscript(messages, cwd).length, 0);
  });

  it('skips a read whose file no longer exists on disk', () => {
    const cwd = tmp();
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: path.join(cwd, 'gone.ts') }),
      toolResult('t1', '1\tdeleted'),
    ];
    assert.equal(collectReadFileStatesFromTranscript(messages, cwd).length, 0);
  });

  it('recovers a successful file_edit by reading post-edit disk content', () => {
    const cwd = tmp();
    const fp = path.join(cwd, 'e.ts');
    fs.writeFileSync(fp, 'POST-EDIT CONTENT');
    const messages: Message[] = [
      toolCall('t1', 'file_edit', { file_path: fp, old_string: 'a', new_string: 'b' }),
      toolResult('t1', 'edited ok'),
    ];
    const states = collectReadFileStatesFromTranscript(messages, cwd);
    assert.equal(states.length, 1);
    assert.equal(states[0].content, 'POST-EDIT CONTENT');
    assert.equal(states[0].mtime, fs.statSync(fp).mtimeMs);
  });

  it('skips a FAILED file_edit (isError)', () => {
    const cwd = tmp();
    fs.writeFileSync(path.join(cwd, 'e.ts'), 'x');
    const messages: Message[] = [
      toolCall('t1', 'file_edit', { file_path: path.join(cwd, 'e.ts'), old_string: 'a', new_string: 'b' }),
      toolResult('t1', 'Error: old_string not found', true),
    ];
    assert.equal(collectReadFileStatesFromTranscript(messages, cwd).length, 0);
  });

  it('is last-wins when the same path is read then edited later', () => {
    const cwd = tmp();
    const fp = path.join(cwd, 'x.ts');
    fs.writeFileSync(fp, 'EDITED');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: fp }),
      toolResult('t1', '1\tORIGINAL'),
      toolCall('t2', 'file_edit', { file_path: fp, old_string: 'a', new_string: 'b' }),
      toolResult('t2', 'ok'),
    ];
    const states = collectReadFileStatesFromTranscript(messages, cwd);
    // Both appear in chronological order; the edit (disk) is last so it wins when applied.
    assert.equal(states[states.length - 1].content, 'EDITED');
  });

  it('ignores malformed tool-call arguments and non-file tools', () => {
    const cwd = tmp();
    const messages: Message[] = [
      { role: 'assistant', content: null, tool_calls: [{ id: 'bad', type: 'function', function: { name: 'file_read', arguments: '{not json' } }] },
      toolResult('bad', '1\tx'),
      toolCall('t2', 'bash', { command: 'ls' }),
      toolResult('t2', 'listing'),
    ];
    assert.equal(collectReadFileStatesFromTranscript(messages, cwd).length, 0);
  });
});

describe('seedFileCacheFromTranscript', () => {
  it('seeds the given cache so an unchanged full re-read is served from cache (dedup accelerator)', () => {
    const cwd = tmp();
    const fp = path.join(cwd, 'a.ts');
    fs.writeFileSync(fp, 'hello world');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: fp }),
      toolResult('t1', '1\thello world'),
    ];
    const cache = new FileCache();
    const n = seedFileCacheFromTranscript(messages, cwd, cache);
    assert.equal(n, 1);
    // The mtime-hit accelerator returns cached content without a disk re-read.
    const hit = cache.getCachedRead(fp, fs.statSync(fp).mtimeMs);
    assert.ok(hit);
    assert.equal(hit!.content, 'hello world');
    // Unchanged file: no stale-change reported.
    assert.equal(cache.hasChanged(fp), false);
  });

  it('a file changed on disk after the transcript read is reported as changed (forces re-read)', () => {
    const cwd = tmp();
    const fp = path.join(cwd, 'a.ts');
    fs.writeFileSync(fp, 'DISK NOW DIFFERS');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: fp }),
      toolResult('t1', '1\told transcript content'),
    ];
    const cache = new FileCache();
    seedFileCacheFromTranscript(messages, cwd, cache);
    // Seeded content != disk content under the same (just-statted) mtime => changed.
    assert.equal(cache.hasChanged(fp), true);
  });

  it('respects an entry-count knob on the target cache (Claude ASK_READ_FILE_STATE_CACHE_SIZE parity)', () => {
    const cwd = tmp();
    const messages: Message[] = [];
    for (let i = 0; i < 5; i++) {
      const fp = path.join(cwd, `f${i}.ts`);
      fs.writeFileSync(fp, `content ${i}`);
      messages.push(toolCall(`t${i}`, 'file_read', { file_path: fp }));
      messages.push(toolResult(`t${i}`, `1\tcontent ${i}`));
    }
    const bounded = new FileCache(16 * 1024 * 1024, 2); // entry cap = 2
    seedFileCacheFromTranscript(messages, cwd, bounded);
    assert.equal(bounded.getStats().files, 2);
  });

  it('defaults to the per-cwd shared cache when no target is passed', () => {
    const cwd = tmp();
    const fp = path.join(cwd, 'shared.ts');
    fs.writeFileSync(fp, 'shared content');
    const messages: Message[] = [
      toolCall('t1', 'file_read', { file_path: fp }),
      toolResult('t1', '1\tshared content'),
    ];
    const n = seedFileCacheFromTranscript(messages, cwd);
    assert.equal(n, 1);
    // getFileCache(cwd) is the same singleton; a re-read is now a cache hit.
    assert.equal(getFileCache(cwd).hasChanged(fp), false);
    getFileCache(cwd).clear();
  });
});
