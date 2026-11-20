/**
 * U016 — deny-by-default TEXT_FILE_EXTENSIONS allowlist for @include / @path imports.
 *
 * Parity with the reference (the reference's markdown-memory module `parseMemoryFileContent` /
 * `TEXT_FILE_EXTENSIONS`). AMI previously gated @include through a finite BINARY
 * DENYlist (and gated @path imports not at all), so any unlisted or multi-dotted
 * binary extension (`.so.1`, `.bin`, a novel `.xyz`) was silently inlined into the
 * system prompt. These tests pin the allowlist behavior:
 *   - a recognized text extension (and extensionless files) inline normally,
 *   - any other extension is refused with a visible `[non-text file skipped: …]`,
 *   - the gate applies to BOTH @include and @path.
 *
 * Red-green: against the old denylist, @path `@shot.png` was inlined (no check)
 * and @include of `.xyz` / `.so.1` were inlined (not on the binary denylist).
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import { MemoryManager, isTextImportPath } from '../src/memory';

let tmpDir: string;

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-mem-u016-txt-'));
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('U016 — isTextImportPath (deny-by-default extension gate)', () => {
  it('allows recognized text/source extensions', () => {
    for (const p of ['notes.md', 'a.ts', 'b.py', 'c.json', 'd.yaml', 'e.rs', 'f.sh', 'g.lock']) {
      assert.equal(isTextImportPath(p), true, `${p} should be allowed`);
    }
  });

  it('allows EXTENSIONLESS files (Dockerfile, Makefile, LICENSE)', () => {
    for (const p of ['Dockerfile', 'Makefile', 'LICENSE']) {
      assert.equal(isTextImportPath(p), true, `${p} should be allowed`);
    }
  });

  it('refuses binary / unknown extensions (incl. multi-dotted + novel)', () => {
    for (const p of ['logo.png', 'doc.pdf', 'pack.zip', 'lib.so', 'libatomic.so.1', 'dump.bin', 'weird.xyz']) {
      assert.equal(isTextImportPath(p), false, `${p} should be refused`);
    }
  });

  it('is case-insensitive on the extension', () => {
    assert.equal(isTextImportPath('README.MD'), true);
    assert.equal(isTextImportPath('IMG.PNG'), false);
  });
});

describe('U016 — resolveAtImports honors the text allowlist', () => {
  it('inlines a text @path but skips a binary @path with a visible marker', () => {
    fs.writeFileSync(path.join(tmpDir, 'snippet.md'), 'INLINED_TEXT');
    fs.writeFileSync(path.join(tmpDir, 'shot.png'), Buffer.from([0x89, 0x50, 0x4e, 0x47]));
    const base = path.join(tmpDir, 'root.md');

    const mm = new MemoryManager(tmpDir);
    const out = mm.resolveAtImports('see @./snippet.md and @./shot.png here', base);

    assert.ok(out.includes('INLINED_TEXT'), 'text @path must inline');
    assert.ok(out.includes('[non-text file skipped: shot.png]'), 'binary @path must be skipped');
    assert.ok(!out.includes('\x89'), 'raw PNG bytes must never reach the prompt');
  });

  it('skips a multi-dotted binary @path (.so.1) that a denylist would miss', () => {
    fs.writeFileSync(path.join(tmpDir, 'libatomic.so.1'), 'ELFDATA');
    const base = path.join(tmpDir, 'root.md');
    const mm = new MemoryManager(tmpDir);
    const out = mm.resolveAtImports('load @./libatomic.so.1', base);
    assert.ok(out.includes('[non-text file skipped: libatomic.so.1]'));
    assert.ok(!out.includes('ELFDATA'));
  });

  it('still inlines an extensionless @path target', () => {
    fs.writeFileSync(path.join(tmpDir, 'Dockerfile'), 'FROM scratch');
    const base = path.join(tmpDir, 'root.md');
    const mm = new MemoryManager(tmpDir);
    const out = mm.resolveAtImports('base @./Dockerfile', base);
    assert.ok(out.includes('FROM scratch'));
  });
});

describe('U016 — resolveIncludes honors the text allowlist', () => {
  it('skips a novel non-text @include extension a binary denylist would allow', async () => {
    fs.writeFileSync(path.join(tmpDir, 'data.xyz'), 'OPAQUE_BLOB');
    const mm = new MemoryManager(tmpDir);
    const out = await mm.resolveIncludes('@include data.xyz', path.join(tmpDir, 'root.md'));
    assert.ok(out.includes('[non-text file skipped: data.xyz]'));
    assert.ok(!out.includes('OPAQUE_BLOB'));
  });

  it('skips a multi-dotted binary @include (.so.1)', async () => {
    fs.writeFileSync(path.join(tmpDir, 'x.so.1'), 'ELFDATA');
    const mm = new MemoryManager(tmpDir);
    const out = await mm.resolveIncludes('@include x.so.1', path.join(tmpDir, 'root.md'));
    assert.ok(out.includes('[non-text file skipped: x.so.1]'));
    assert.ok(!out.includes('ELFDATA'));
  });

  it('still inlines a recognized text @include', async () => {
    fs.writeFileSync(path.join(tmpDir, 'snippet.md'), 'TEXT_OK');
    const mm = new MemoryManager(tmpDir);
    const out = await mm.resolveIncludes('@include snippet.md', path.join(tmpDir, 'root.md'));
    assert.ok(out.includes('TEXT_OK'));
  });
});
