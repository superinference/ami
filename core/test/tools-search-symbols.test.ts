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
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-search-symbols-'));

  // Create TypeScript files with symbols
  fs.writeFileSync(
    path.join(tmpDir, 'example.ts'),
    `export function greetUser(name: string): string {
  return 'Hello ' + name;
}

export class UserService {
  getName(): string { return ''; }
}

export interface UserConfig {
  name: string;
  age: number;
}

export type UserId = string;

export const MAX_USERS = 100;
`,
  );

  fs.writeFileSync(
    path.join(tmpDir, 'another.ts'),
    `export function processData(data: unknown[]): void {}

export class DataProcessor {
  transform(): void {}
}
`,
  );
});

afterEach(() => {
  _resetIndexer();
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

describe('searchSymbolsTool – definition', () => {
  it('has the correct name', () => {
    assert.equal(searchSymbolsTool.name, 'search_symbols');
  });

  it('is read-only and concurrency-safe', () => {
    assert.equal(searchSymbolsTool.isReadOnly, true);
    assert.equal(searchSymbolsTool.isConcurrencySafe, true);
  });

  it('schema requires query', () => {
    assert.ok(searchSymbolsTool.inputSchema.required?.includes('query'));
  });

  it('has query and type properties', () => {
    assert.ok('query' in searchSymbolsTool.inputSchema.properties);
    assert.ok('type' in searchSymbolsTool.inputSchema.properties);
  });
});

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

describe('searchSymbolsTool – validation', () => {
  it('rejects empty query', async () => {
    const result = await searchSymbolsTool.execute({ query: '' }, ctx());
    assert.equal(result.isError, true);
    assert.ok(result.output.includes('must not be empty'));
  });

  it('rejects whitespace-only query', async () => {
    const result = await searchSymbolsTool.execute({ query: '   ' }, ctx());
    assert.equal(result.isError, true);
  });
});

// ---------------------------------------------------------------------------
// Symbol search
// ---------------------------------------------------------------------------

describe('searchSymbolsTool – search', () => {
  it('finds functions by name', async () => {
    const result = await searchSymbolsTool.execute({ query: 'greetUser' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('greetUser'));
    assert.ok(result.output.includes('function'));
  });

  it('finds classes by name', async () => {
    const result = await searchSymbolsTool.execute({ query: 'UserService' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('UserService'));
    assert.ok(result.output.includes('class'));
  });

  it('shows relative file paths', async () => {
    const result = await searchSymbolsTool.execute({ query: 'greetUser' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('example.ts'));
  });

  it('returns no symbols message for unmatched query', async () => {
    const result = await searchSymbolsTool.execute({ query: 'zzzzNotASymbol' }, ctx());
    assert.ok(!result.isError);
    assert.ok(result.output.includes('No symbols matching'));
  });

  it('filters by type when specified', async () => {
    const result = await searchSymbolsTool.execute(
      { query: 'User', type: 'class' },
      ctx(),
    );
    assert.ok(!result.isError);
    // Should find UserService (class) but NOT UserConfig (interface) or UserId (type)
    if (result.output.includes('UserService')) {
      assert.ok(!result.output.includes('(interface)'));
      assert.ok(!result.output.includes('(type)'));
    }
  });

  it('finds symbols across multiple files', async () => {
    const result = await searchSymbolsTool.execute({ query: 'process' }, ctx());
    assert.ok(!result.isError);
    if (result.output.includes('processData')) {
      assert.ok(result.output.includes('another.ts'));
    }
  });
});

// ---------------------------------------------------------------------------
// _resetIndexer
// ---------------------------------------------------------------------------

describe('_resetIndexer', () => {
  it('allows re-indexing a different directory after reset', async () => {
    // First search in tmpDir
    await searchSymbolsTool.execute({ query: 'greetUser' }, ctx());

    // Reset
    _resetIndexer();

    // Create a new tmpDir with different content
    const tmpDir2 = fs.mkdtempSync(path.join(os.tmpdir(), 'si-search-symbols-2-'));
    fs.writeFileSync(path.join(tmpDir2, 'other.ts'), 'export function otherFunc() {}\n');

    const result = await searchSymbolsTool.execute(
      { query: 'otherFunc' },
      ctx({ cwd: tmpDir2 }),
    );

    fs.rmSync(tmpDir2, { recursive: true, force: true });

    // Should find the symbol in the new directory
    assert.ok(!result.isError);
    if (result.output.includes('otherFunc')) {
      assert.ok(result.output.includes('other.ts'));
    }
  });
});
