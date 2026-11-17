import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  buildConfigDiagnosticsTree,
  formatConfigDiagnosticsTree,
  type ConfigDiagnostic,
} from '../src/config';

describe('config diagnostics tree', () => {
  it('groups invalid-value diagnostics by file and nests per-field issues', () => {
    const diagnostics: ConfigDiagnostic[] = [
      {
        path: '/home/u/.ami/config.json',
        kind: 'invalid-value',
        message: '1 invalid config value(s)',
        sourceName: 'user',
        issues: [
          { field: 'permissionMode', message: 'must be one of ask|auto-allow|deny-all', tip: 'Valid values: ask, auto-allow, deny-all' },
        ],
      },
    ];
    const tree = buildConfigDiagnosticsTree(diagnostics);
    const key = 'user config (/home/u/.ami/config.json)';
    assert.ok(key in tree);
    const fileNode = tree[key] as Record<string, unknown>;
    assert.equal(
      fileNode.permissionMode,
      'must be one of ask|auto-allow|deny-all (Valid values: ask, auto-allow, deny-all)',
    );
  });

  it('nests a non-invalid-value diagnostic under its kind', () => {
    const diagnostics: ConfigDiagnostic[] = [
      { path: '/p/.ami/config.json', kind: 'parse-error', message: 'Unexpected token' },
    ];
    const tree = buildConfigDiagnosticsTree(diagnostics);
    const fileNode = tree['/p/.ami/config.json'] as Record<string, unknown>;
    assert.equal(fileNode['parse-error'], 'Unexpected token');
  });

  it('sorts file labels alphabetically for deterministic output', () => {
    const diagnostics: ConfigDiagnostic[] = [
      { path: '/z.json', kind: 'parse-error', message: 'z' },
      { path: '/a.json', kind: 'parse-error', message: 'a' },
    ];
    const tree = buildConfigDiagnosticsTree(diagnostics);
    assert.deepEqual(Object.keys(tree), ['/a.json', '/z.json']);
  });

  it('merges multiple diagnostics for the same file into one node', () => {
    const diagnostics: ConfigDiagnostic[] = [
      { path: '/c.json', kind: 'unknown-key', message: 'unknown: foo' },
      { path: '/c.json', kind: 'invalid-value', message: 'bad', issues: [{ field: 'maxTurns', message: 'must be a number' }] },
    ];
    const tree = buildConfigDiagnosticsTree(diagnostics);
    assert.equal(Object.keys(tree).length, 1);
    const node = tree['/c.json'] as Record<string, unknown>;
    assert.equal(node['unknown-key'], 'unknown: foo');
    assert.equal(node.maxTurns, 'must be a number');
  });

  it('formats the tree as a box-drawing string via treeify', () => {
    const diagnostics: ConfigDiagnostic[] = [
      { path: '/a.json', kind: 'parse-error', message: 'boom' },
    ];
    const out = formatConfigDiagnosticsTree(diagnostics);
    const lines = out.split('\n');
    assert.equal(lines[0], '└ /a.json');
    assert.equal(lines[1], '  └ parse-error: boom');
  });

  it('returns an empty string for no diagnostics', () => {
    assert.equal(formatConfigDiagnosticsTree([]), '');
  });

  it('forwards treeify options (colorize seam) through the formatter', () => {
    const diagnostics: ConfigDiagnostic[] = [
      { path: '/a.json', kind: 'parse-error', message: 'boom' },
    ];
    const out = formatConfigDiagnosticsTree(diagnostics, {
      colorize: (text, role) => (role === 'value' ? `*${text}*` : text),
    });
    assert.ok(out.includes('*boom*'));
  });
});
