import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { searchSymbolsTool, _resetIndexer } from '../src/tools/search-symbols';
import type { ToolContext } from '../src/types';

let tmpDir: string;

function ctx(overrides?: Partial<ToolContext>): ToolContext {
  return {
    cwd: tmpDir,
    abortSignal: new AbortController().signal,
    ...overrides,
  };
}

beforeEach(() => {
  _resetIndexer();
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-search-symbols-u213-'));
  fs.writeFileSync(path.join(tmpDir, 'package.json'), '{}');

  // A file with many same-prefix functions so paging/truncation have material.
  const many = Array.from({ length: 40 }, (_, i) => `export function handler${i}(): void {}`).join('\n');
  fs.writeFileSync(path.join(tmpDir, 'handlers.ts'), many + '\n');

  // A file with mixed kinds for type-filter + grouping tests.
  fs.writeFileSync(
    path.join(tmpDir, 'models.ts'),
    `export function buildModel(): void {}
export class ModelStore {
  loadModel(): void {}
}
export interface ModelConfig { id: string; }
export type ModelId = string;
export const MODEL_CAP = 10;
`,
  );
});

afterEach(() => {
  _resetIndexer();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Schema: paging knobs + full type-enum coverage
// ---------------------------------------------------------------------------

describe('search_symbols – schema (U213)', () => {
  it('exposes limit and offset paging knobs', () => {
    const props = searchSymbolsTool.inputSchema.properties;
    assert.ok('limit' in props);
    assert.ok('offset' in props);
  });

  it('type enum covers every indexer-emitted kind (incl. method/import/export)', () => {
    const typeProp = searchSymbolsTool.inputSchema.properties.type as { enum: string[] };
    for (const kind of ['function', 'class', 'interface', 'type', 'variable', 'method', 'import', 'export']) {
      assert.ok(typeProp.enum.includes(kind), `enum missing ${kind}`);
    }
  });
});

// ---------------------------------------------------------------------------
// Count header + pluralization + group-by-file
// ---------------------------------------------------------------------------

describe('search_symbols – output formatting (U213)', () => {
  it('prepends a singular count header for one match', async () => {
    const res = await searchSymbolsTool.execute({ query: 'buildModel' }, ctx());
    assert.ok(!res.isError);
    assert.ok(res.output.startsWith('Found 1 symbol matching "buildModel":'), res.output);
  });

  it('prepends a pluralized count header for multiple matches', async () => {
    const res = await searchSymbolsTool.execute({ query: 'handler', limit: 5 }, ctx());
    assert.ok(!res.isError);
    assert.ok(/^Found 5 symbols matching "handler":/.test(res.output), res.output);
  });

  it('groups results under a per-file header with indented entries', async () => {
    const res = await searchSymbolsTool.execute({ query: 'Model' }, ctx());
    assert.ok(!res.isError);
    // Per-file header printed once, entries indented as "  name (type) - Line N".
    assert.ok(res.output.includes('\nmodels.ts:'), res.output);
    assert.match(res.output, /\n {2}ModelStore \(class\) - Line \d+/);
    // The flat "name (type) in path:line" form is gone (path not repeated per line).
    assert.ok(!/ in models\.ts:\d+/.test(res.output), res.output);
  });
});

// ---------------------------------------------------------------------------
// limit / offset paging + truncation disclosure
// ---------------------------------------------------------------------------

describe('search_symbols – paging (U213)', () => {
  it('limit caps the returned set and discloses truncation', async () => {
    const res = await searchSymbolsTool.execute({ query: 'handler', limit: 3 }, ctx());
    assert.ok(!res.isError);
    const entryLines = res.output.split('\n').filter(l => /^ {2}handler\d+ /.test(l));
    assert.equal(entryLines.length, 3);
    assert.match(res.output, /showing 3 of more matches/);
  });

  it('offset pages past earlier matches (different page than offset 0)', async () => {
    const page0 = await searchSymbolsTool.execute({ query: 'handler', limit: 3, offset: 0 }, ctx());
    const page1 = await searchSymbolsTool.execute({ query: 'handler', limit: 3, offset: 3 }, ctx());
    const names = (o: string) => o.split('\n').filter(l => /^ {2}handler\d+ /.test(l)).map(l => l.trim());
    const n0 = names(page0.output);
    const n1 = names(page1.output);
    assert.equal(n1.length, 3);
    // No overlap between the two pages.
    assert.equal(n0.filter(n => n1.includes(n)).length, 0, `${n0} vs ${n1}`);
  });

  it('offset past the end yields an actionable "lower the offset" message', async () => {
    const res = await searchSymbolsTool.execute({ query: 'buildModel', offset: 50 }, ctx());
    assert.ok(!res.isError);
    assert.match(res.output, /offset 50 skipped past them/);
  });

  it('limit 0 means "as many as allowed" (returns the full small set, no truncation)', async () => {
    const res = await searchSymbolsTool.execute({ query: 'handler', limit: 0 }, ctx());
    assert.ok(!res.isError);
    const entryLines = res.output.split('\n').filter(l => /^ {2}handler\d+ /.test(l));
    assert.equal(entryLines.length, 40);
    assert.ok(!/showing .* of more matches/.test(res.output), res.output);
  });
});

// ---------------------------------------------------------------------------
// Validation: paging knobs + type filter
// ---------------------------------------------------------------------------

describe('search_symbols – validation (U213)', () => {
  it('rejects a non-integer / negative limit with a validation error', async () => {
    const bad = await searchSymbolsTool.execute({ query: 'handler', limit: -4 }, ctx());
    assert.equal(bad.isError, true);
    assert.match(bad.output, /Invalid "limit"/);

    const bad2 = await searchSymbolsTool.execute({ query: 'handler', limit: 2.5 }, ctx());
    assert.equal(bad2.isError, true);
  });

  it('rejects a negative offset with a validation error', async () => {
    const bad = await searchSymbolsTool.execute({ query: 'handler', offset: -1 }, ctx());
    assert.equal(bad.isError, true);
    assert.match(bad.output, /Invalid "offset"/);
  });

  it('rejects an out-of-set type filter and lists the valid types', async () => {
    const res = await searchSymbolsTool.execute({ query: 'Model', type: 'enum' }, ctx());
    assert.equal(res.isError, true);
    assert.match(res.output, /Invalid "type" filter "enum"/);
    assert.match(res.output, /function, class, interface, type, variable, method, import, export/);
  });

  it('accepts a newly-allowed "method" type filter', async () => {
    const res = await searchSymbolsTool.execute({ query: 'loadModel', type: 'method' }, ctx());
    assert.ok(!res.isError, res.output);
    // loadModel is a class method; with the enum extended it is now requestable.
    assert.ok(res.output.includes('loadModel'), res.output);
  });
});

// ---------------------------------------------------------------------------
// Empty-result remediation guidance
// ---------------------------------------------------------------------------

describe('search_symbols – empty-result guidance (U213)', () => {
  it('suggests how to broaden the search when nothing matches', async () => {
    const res = await searchSymbolsTool.execute({ query: 'zzzNope', type: 'class' }, ctx());
    assert.ok(!res.isError);
    assert.match(res.output, /No symbols matching "zzzNope" \(type: class\) found\./);
    assert.match(res.output, /shorter or partial name|drop the type filter/);
  });
});
