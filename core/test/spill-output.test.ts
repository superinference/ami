import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { spillToolText } from '../src/tools/spill-output';
import { projectStateDir } from '../src/reference-journal';

describe('spillToolText', () => {
  let tmpDir: string;

  before(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-test-'));
  });

  after(() => { fs.rmSync(tmpDir, { recursive: true }); });

  // Spill output now lands in the GLOBAL per-project state dir
  // (<dataDir>/projects/<cwd-slug>/tool-results), not <cwd>/.ami/tool-results.
  // The fallback tests therefore inject write failures by pointing the data dir
  // (SUPERINFERENCE_DATA_DIR) at an unwritable location for the duration of the call.
  const withDataDir = (dir: string, fn: () => void) => {
    const saved = process.env.SUPERINFERENCE_DATA_DIR;
    process.env.SUPERINFERENCE_DATA_DIR = dir;
    try { fn(); } finally {
      if (saved === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
      else process.env.SUPERINFERENCE_DATA_DIR = saved;
    }
  };

  it('returns text unchanged when under limit', () => {
    const text = 'short text';
    assert.equal(spillToolText(tmpDir, 'test', text, 1000), text);
  });

  it('truncates text and writes spill file when over limit', () => {
    const text = 'x'.repeat(5000);
    const result = spillToolText(tmpDir, 'big', text, 1000);
    assert.ok(result.length < text.length);
    assert.ok(result.includes('truncated'));
    assert.ok(result.includes('file_read'));

    const spillDir = projectStateDir(tmpDir, 'tool-results');
    assert.ok(fs.existsSync(spillDir));
    const files = fs.readdirSync(spillDir).filter(f => f.startsWith('big-'));
    assert.ok(files.length > 0);
    const spillContent = fs.readFileSync(path.join(spillDir, files[0]), 'utf-8');
    assert.equal(spillContent, text);
  });

  it('includes head and tail in truncated output', () => {
    const text = 'HEAD' + 'x'.repeat(5000) + 'TAIL';
    const result = spillToolText(tmpDir, 'ht', text, 1000);
    assert.ok(result.startsWith('HEAD'));
    assert.ok(result.endsWith('TAIL'));
  });

  it('reports omitted char count with persisted path on success', () => {
    const text = 'a'.repeat(3000);
    const result = spillToolText(tmpDir, 'cnt', text, 1000);
    assert.ok(result.includes('chars'));
    assert.ok(result.includes('persisted to'));
  });

  it('returns text exactly at limit', () => {
    const text = 'b'.repeat(500);
    assert.equal(spillToolText(tmpDir, 'eq', text, 500), text);
  });

  it('writes spill file on success and includes path in output', () => {
    const text = 'y'.repeat(2000);
    const result = spillToolText(tmpDir, 'happy', text, 500);
    const spillDir = projectStateDir(tmpDir, 'tool-results');
    assert.ok(fs.existsSync(spillDir));
    const files = fs.readdirSync(spillDir).filter(f => f.startsWith('happy-'));
    assert.ok(files.length > 0, 'spill file should be created');
    assert.ok(result.includes('persisted to'));
    assert.ok(result.includes(spillDir));
    const content = fs.readFileSync(path.join(spillDir, files[0]), 'utf-8');
    assert.equal(content, text);
  });

  it('gracefully falls back on disk write failure instead of crashing', () => {
    const text = 'HEAD_MARKER' + 'x'.repeat(5000) + 'TAIL_MARKER';
    // A read-only (0o555) data dir makes creating projects/<slug>/tool-results
    // under it fail regardless of uid — robust even when the suite runs as root
    // in a container (where `/nonexistent` is creatable by the root owner).
    const roData = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-nowrite-'));
    fs.chmodSync(roData, 0o555);
    try {
      withDataDir(roData, () => {
        const result = spillToolText(tmpDir, 'fail', text, 1000);
        assert.ok(result.length < text.length, 'output should be truncated');
        assert.ok(result.includes('truncated'), 'should mention truncation');
        assert.ok(!result.includes('persisted to /'), 'fallback must not contain spill file path');
        assert.ok(result.startsWith('HEAD_MARKER'), 'head content must be preserved');
        assert.ok(result.endsWith('TAIL_MARKER'), 'tail content must be preserved');
      });
    } finally {
      fs.chmodSync(roData, 0o755);
      fs.rmSync(roData, { recursive: true, force: true });
    }
  });

  it('falls back gracefully when mkdir fails (file blocks dir creation)', () => {
    // Put a regular file where the `projects` dir must be created, so the
    // recursive mkdir of projects/<slug>/tool-results fails with ENOTDIR.
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-block-'));
    fs.writeFileSync(path.join(dataDir, 'projects'), 'blocker');
    try {
      withDataDir(dataDir, () => {
        const text = 'HEAD' + 'q'.repeat(5000) + 'TAIL';
        // mkdirSync will fail with ENOTDIR because `projects` is a file
        const result = spillToolText(tmpDir, 'enotdir', text, 1000);
        assert.ok(result.includes('truncated'), 'result must mention truncation');
        assert.ok(!result.includes('persisted to /'), 'fallback must not contain spill file path');
        assert.ok(result.startsWith('HEAD'), 'head content must be preserved');
        assert.ok(result.endsWith('TAIL'), 'tail content must be preserved');
      });
    } finally {
      fs.rmSync(dataDir, { recursive: true });
    }
  });

  it('falls back gracefully when write fails (read-only spill dir)', () => {
    // Create the spill directory structure but make tool-results read-only
    const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-ro-'));
    withDataDir(dataDir, () => {
      const spillDir = projectStateDir(tmpDir, 'tool-results');
      fs.mkdirSync(spillDir, { recursive: true });
      fs.chmodSync(spillDir, 0o444);
      try {
        const text = 'FRONT' + 'w'.repeat(5000) + 'BACK';
        // writeFileSync will fail with EACCES on read-only directory
        const result = spillToolText(tmpDir, 'readonly', text, 1000);
        assert.ok(result.includes('truncated'), 'result must mention truncation');
        assert.ok(!result.includes('persisted to /'), 'fallback must not contain spill file path');
        assert.ok(result.startsWith('FRONT'), 'head content must be preserved');
        assert.ok(result.endsWith('BACK'), 'tail content must be preserved');
      } finally {
        fs.chmodSync(spillDir, 0o755);
      }
    });
    fs.rmSync(dataDir, { recursive: true });
  });

  it('falls back gracefully when cwd does not exist', () => {
    const text = 'r'.repeat(3000);
    // Read-only data dir → spill write fails regardless of uid (see note above).
    const roData = fs.mkdtempSync(path.join(os.tmpdir(), 'spill-nopath-'));
    fs.chmodSync(roData, 0o555);
    try {
      withDataDir(roData, () => {
        const result = spillToolText(tmpDir, 'nopath', text, 1000);
        assert.ok(result.includes('truncated') || result.includes('omitted'), 'result must mention truncation');
        assert.ok(result.includes('could not persist'), 'fallback must mention disk write failure');
        assert.ok(result.startsWith('r'), 'head content preserved');
        assert.ok(result.endsWith('r'), 'tail content preserved');
      });
    } finally {
      fs.chmodSync(roData, 0o755);
      fs.rmSync(roData, { recursive: true, force: true });
    }
  });
});
