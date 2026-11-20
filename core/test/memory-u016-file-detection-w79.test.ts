/**
 * U016 (w79) — memory / transcript file-access classifier.
 *
 * Gap (parity: the reference utils/memoryFileDetection.ts +
 * sessionFileAccessHooks.ts): given (toolName, toolInput), tell whether a tool
 * is touching a memory/session file so a consumer can re-label generic
 * file_read/grep/glob/file_edit activity as a memory operation. AMI had no such
 * classifier.
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import {
  detectSessionFileType,
  detectSessionPatternType,
  classifyToolSessionFileType,
  isMemoryFileAccess,
  type MemoryDirs,
} from '../src/memory-file-detection';
import { MemoryManager } from '../src/memory';

const dirs: MemoryDirs = {
  memoryDir: '/home/u/.ami/projects/proj/memory',
  transcriptDir: '/home/u/.ami/projects/proj/sessions',
};

describe('detectSessionFileType', () => {
  it('classifies a session-<id>.md inside the memory dir as session_memory', () => {
    assert.equal(
      detectSessionFileType('/home/u/.ami/projects/proj/memory/session-abc123.md', dirs),
      'session_memory',
    );
  });

  it('classifies a *.jsonl/*.json inside the transcript dir as session_transcript', () => {
    assert.equal(
      detectSessionFileType('/home/u/.ami/projects/proj/sessions/abc.jsonl', dirs),
      'session_transcript',
    );
    assert.equal(
      detectSessionFileType('/home/u/.ami/projects/proj/sessions/abc.json', dirs),
      'session_transcript',
    );
  });

  it('returns null for a non-session file and for files outside the dirs', () => {
    // A stored-memory topic file (not session-prefixed) is not a session file.
    assert.equal(
      detectSessionFileType('/home/u/.ami/projects/proj/memory/architecture.md', dirs),
      null,
    );
    // A session-looking name OUTSIDE the memory dir is not classified.
    assert.equal(detectSessionFileType('/tmp/session-x.md', dirs), null);
    // A .json outside the transcript dir is not a transcript.
    assert.equal(detectSessionFileType('/home/u/project/package.json', dirs), null);
    assert.equal(detectSessionFileType('', dirs), null);
  });

  it('skips transcript classification when no transcriptDir is configured', () => {
    assert.equal(
      detectSessionFileType('/home/u/.ami/projects/proj/sessions/abc.jsonl', { memoryDir: dirs.memoryDir }),
      null,
    );
  });
});

describe('detectSessionPatternType', () => {
  it('detects session-memory intent from a glob pattern', () => {
    assert.equal(detectSessionPatternType('session-*.md'), 'session_memory');
    assert.equal(detectSessionPatternType('**/session-*'), 'session_memory');
  });
  it('detects session-transcript intent from a glob pattern', () => {
    assert.equal(detectSessionPatternType('**/*.jsonl'), 'session_transcript');
    assert.equal(detectSessionPatternType('sessions/*.json'), 'session_transcript');
  });
  it('returns null for an unrelated or empty pattern', () => {
    assert.equal(detectSessionPatternType('**/*.ts'), null);
    assert.equal(detectSessionPatternType('package.json'), null);
    assert.equal(detectSessionPatternType(''), null);
  });
});

describe('classifyToolSessionFileType', () => {
  it('classifies file_read / file_edit by file_path', () => {
    assert.equal(
      classifyToolSessionFileType('file_read', { file_path: '/home/u/.ami/projects/proj/memory/session-9.md' }, dirs),
      'session_memory',
    );
    assert.equal(
      classifyToolSessionFileType('file_edit', { file_path: '/home/u/.ami/projects/proj/sessions/9.jsonl' }, dirs),
      'session_transcript',
    );
  });
  it('checks grep path first, then the glob/include filter (never the search regex)', () => {
    // Explicit path wins.
    assert.equal(
      classifyToolSessionFileType('grep', { pattern: 'anything', path: '/home/u/.ami/projects/proj/memory/session-1.md' }, dirs),
      'session_memory',
    );
    // Falls back to the glob file-filter.
    assert.equal(
      classifyToolSessionFileType('grep', { pattern: 'anything', glob: 'session-*.md' }, dirs),
      'session_memory',
    );
    // The search `pattern` alone must NOT trigger classification.
    assert.equal(
      classifyToolSessionFileType('grep', { pattern: 'session-foo.md' }, dirs),
      null,
    );
  });
  it('classifies glob by its pattern', () => {
    assert.equal(
      classifyToolSessionFileType('glob', { pattern: '**/*.jsonl' }, dirs),
      'session_transcript',
    );
  });
  it('returns null for unrelated tools and malformed input', () => {
    assert.equal(classifyToolSessionFileType('bash', { command: 'ls' }, dirs), null);
    assert.equal(classifyToolSessionFileType('file_read', null, dirs), null);
    assert.equal(classifyToolSessionFileType('file_read', { file_path: 123 }, dirs), null);
  });
});

describe('isMemoryFileAccess', () => {
  it('is true for session_memory access and for any read/edit within the memory dir', () => {
    assert.equal(
      isMemoryFileAccess('file_read', { file_path: '/home/u/.ami/projects/proj/memory/session-1.md' }, dirs),
      true,
    );
    // A stored-memory topic file (not session-prefixed) still counts as memory access.
    assert.equal(
      isMemoryFileAccess('file_edit', { file_path: '/home/u/.ami/projects/proj/memory/team/shared.md' }, dirs),
      true,
    );
  });
  it('is false for an ordinary project file and non-path tools', () => {
    assert.equal(
      isMemoryFileAccess('file_read', { file_path: '/home/u/project/src/index.ts' }, dirs),
      false,
    );
    // grep over a transcript glob is session_transcript, not session_memory, and
    // grep exposes no file_path → not counted as a direct memory file access.
    assert.equal(
      isMemoryFileAccess('grep', { glob: '**/*.jsonl' }, dirs),
      false,
    );
  });
});

describe('MemoryManager classifier integration (real workspace dirs)', () => {
  it('detects memory access against the manager-resolved memory dir', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-mfd-'));
    try {
      const mm = new MemoryManager(dir);
      const memFile = path.join(mm.getMemoryDir(), 'session-live.md');
      assert.equal(mm.classifyToolSessionFile('file_read', { file_path: memFile }), 'session_memory');
      assert.equal(mm.detectMemoryFileAccess('file_read', { file_path: memFile }), true);
      // A stored topic file is memory access but not a session file.
      const topic = path.join(mm.getMemoryDir(), 'conventions.md');
      assert.equal(mm.classifyToolSessionFile('file_read', { file_path: topic }), null);
      assert.equal(mm.detectMemoryFileAccess('file_read', { file_path: topic }), true);
      // An unrelated file is neither.
      assert.equal(mm.detectMemoryFileAccess('file_read', { file_path: path.join(dir, 'README.md') }), false);
    } finally {
      await fsp.rm(dir, { recursive: true, force: true });
    }
  });
});
