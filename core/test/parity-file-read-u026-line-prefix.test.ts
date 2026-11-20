import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  fileReadTool,
  formatLineNumber,
} from '../src/tools/file-read';
import { fileEditTool, buildFileEditDescription } from '../src/tools/file-edit';
import {
  getLineNumberStyle,
  LINE_NUMBER_STYLE_ENV,
  DEFAULT_LINE_NUMBER_STYLE,
} from '../src/tools/file-read-limits';
import { stripLineNumberPrefix } from '../src/file-cache-seed';
import { getFileCache } from '../src/file-cache';
import type { ToolContext } from '../src/types';

let tmpDir: string;
let savedEnv: string | undefined;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-fr-u026-lnp-'));
  getFileCache(tmpDir).clear();
  savedEnv = process.env[LINE_NUMBER_STYLE_ENV];
  delete process.env[LINE_NUMBER_STYLE_ENV];
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
  if (savedEnv === undefined) delete process.env[LINE_NUMBER_STYLE_ENV];
  else process.env[LINE_NUMBER_STYLE_ENV] = savedEnv;
});

// ---------------------------------------------------------------------------
// Gap (utils/file.ts :: config knob to select the read line-number prefix
// format): getLineNumberStyle resolves compact by default + arrow on request,
// and a malformed value can never corrupt the format (falls back to compact).
// Mirrors the reference isCompactLinePrefixEnabled() (utils/file.ts).
// ---------------------------------------------------------------------------
describe('getLineNumberStyle – config knob', () => {
  it('defaults to compact when unset', () => {
    assert.equal(getLineNumberStyle({}), 'compact');
    assert.equal(DEFAULT_LINE_NUMBER_STYLE, 'compact');
  });

  it('selects arrow for the arrow token (case-insensitive, trimmed)', () => {
    assert.equal(getLineNumberStyle({ [LINE_NUMBER_STYLE_ENV]: 'arrow' }), 'arrow');
    assert.equal(getLineNumberStyle({ [LINE_NUMBER_STYLE_ENV]: 'ARROW' }), 'arrow');
    assert.equal(getLineNumberStyle({ [LINE_NUMBER_STYLE_ENV]: '  Arrow  ' }), 'arrow');
  });

  it('falls back to compact for any other/empty value', () => {
    assert.equal(getLineNumberStyle({ [LINE_NUMBER_STYLE_ENV]: '' }), 'compact');
    assert.equal(getLineNumberStyle({ [LINE_NUMBER_STYLE_ENV]: 'compact' }), 'compact');
    assert.equal(getLineNumberStyle({ [LINE_NUMBER_STYLE_ENV]: 'garbage' }), 'compact');
  });
});

// ---------------------------------------------------------------------------
// formatLineNumber: the single shared formatter the Read numbering sites use.
// ---------------------------------------------------------------------------
describe('formatLineNumber – shared prefix formatter', () => {
  it('compact: bare number + tab (no padding by default)', () => {
    assert.equal(formatLineNumber(1, 'hello', 'compact'), '1\thello');
    assert.equal(formatLineNumber(200, 'x', 'compact'), '200\tx');
  });

  it('compact: right-pads the gutter to the requested width (column alignment)', () => {
    assert.equal(formatLineNumber(1, 'a', 'compact', 3), '  1\ta');
    assert.equal(formatLineNumber(200, 'b', 'compact', 3), '200\tb');
  });

  it('arrow: number right-padded to width 6 then arrow', () => {
    assert.equal(formatLineNumber(1, 'hello', 'arrow'), '     1→hello');
    assert.equal(formatLineNumber(42, 'x', 'arrow'), '    42→x');
  });

  it('arrow: bare number (no padding) once it is already >= 6 digits', () => {
    assert.equal(formatLineNumber(123456, 'y', 'arrow'), '123456→y');
    assert.equal(formatLineNumber(1234567, 'z', 'arrow'), '1234567→z');
  });
});

// ---------------------------------------------------------------------------
// End-to-end: a real file_read honors the knob for its emitted line prefixes,
// and the default output is byte-for-byte the historical compact `N\t` format.
// ---------------------------------------------------------------------------
describe('fileReadTool – honors the line-number style knob', () => {
  it('default (compact) emits N\\t prefixes', async () => {
    const f = path.join(tmpDir, 'a.txt');
    fs.writeFileSync(f, 'first\nsecond\nthird\n');
    const res = await fileReadTool.execute({ file_path: f }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('1\tfirst'));
    assert.ok(res.output.includes('2\tsecond'));
    assert.ok(res.output.includes('3\tthird'));
    assert.ok(!res.output.includes('→'));
  });

  it('arrow style emits padded N→ prefixes (full read)', async () => {
    process.env[LINE_NUMBER_STYLE_ENV] = 'arrow';
    const f = path.join(tmpDir, 'b.txt');
    fs.writeFileSync(f, 'first\nsecond\nthird\n');
    const res = await fileReadTool.execute({ file_path: f }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('     1→first'), res.output);
    assert.ok(res.output.includes('     2→second'));
    assert.ok(res.output.includes('     3→third'));
    assert.ok(!/\b1\tfirst/.test(res.output));
  });

  it('arrow style applies to a ranged (offset/limit) read too', async () => {
    process.env[LINE_NUMBER_STYLE_ENV] = 'arrow';
    const f = path.join(tmpDir, 'c.txt');
    fs.writeFileSync(f, 'l0\nl1\nl2\nl3\nl4\n');
    const res = await fileReadTool.execute({ file_path: f, offset: 2, limit: 2 }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('     3→l2'), res.output);
    assert.ok(res.output.includes('     4→l3'));
  });

  it('arrow style applies to a tail read too', async () => {
    process.env[LINE_NUMBER_STYLE_ENV] = 'arrow';
    const f = path.join(tmpDir, 'd.txt');
    fs.writeFileSync(f, 'l1\nl2\nl3\nl4\nl5');
    const res = await fileReadTool.execute({ file_path: f, tail: 2 }, ctx());
    assert.equal(res.isError, undefined);
    assert.ok(res.output.includes('     4→l4'), res.output);
    assert.ok(res.output.includes('     5→l5'));
  });
});

// ---------------------------------------------------------------------------
// Round-trip: the existing stripLineNumberPrefix (file-cache-seed.ts) is the
// inverse of BOTH styles, so a transcript-seeded re-read reconstructs the raw
// source line regardless of the format the output was produced in.
// ---------------------------------------------------------------------------
describe('line-number prefix round-trip (format <-> strip)', () => {
  it('strips a compact-formatted line back to the source', () => {
    const line = formatLineNumber(12, "const x = 1;", 'compact', 3);
    assert.equal(stripLineNumberPrefix(line), 'const x = 1;');
  });

  it('strips an arrow-formatted line back to the source', () => {
    const line = formatLineNumber(12, 'const x = 1;', 'arrow');
    assert.equal(stripLineNumberPrefix(line), 'const x = 1;');
  });
});

// ---------------------------------------------------------------------------
// Gap (FileEditTool/prompt.ts :: configurable line-number prefix format): the
// file_edit description reflects whichever prefix format is active, so the model
// strips the correct prefix. Mirrors the reference getEditToolDescription().
// ---------------------------------------------------------------------------
describe('buildFileEditDescription – reflects the active prefix format', () => {
  it('compact: describes the "N\\t" tab prefix', () => {
    const d = buildFileEditDescription('compact');
    assert.match(d, /line number and a tab \("N\\t<content>"\)/);
    assert.match(d, /never the "N\\t" prefix/);
    assert.ok(!d.includes('→'));
  });

  it('arrow: describes the padded "N→" arrow prefix', () => {
    const d = buildFileEditDescription('arrow');
    assert.match(d, /arrow \("     N→<content>"\)/);
    assert.match(d, /never the "N→" prefix/);
    assert.ok(!d.includes('"N\\t<content>"'));
  });

  it('the registered file_edit tool uses the compact (default) description', () => {
    assert.equal(fileEditTool.description, buildFileEditDescription('compact'));
  });
});
