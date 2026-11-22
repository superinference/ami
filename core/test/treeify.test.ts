import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { treeify, TREE_COLOR_ROLES, type TreeNode } from '../src/utils/treeify';

describe('treeify', () => {
  it('renders a flat object with last/branch glyphs and colons', () => {
    const out = treeify({ a: '1', b: '2' });
    const lines = out.split('\n');
    assert.equal(lines.length, 2);
    // First root child has no leading prefix and uses the branch glyph.
    assert.equal(lines[0], '├ a: 1');
    // Last key uses the lastBranch glyph.
    assert.equal(lines[1], '└ b: 2');
  });

  it('nests objects with a continuation spine for non-last siblings', () => {
    const out = treeify({ a: { x: '1' }, b: '2' });
    const lines = out.split('\n');
    // Non-last parent "a" keeps its vertical spine "│" for descendants.
    assert.equal(lines[0], '├ a');
    assert.equal(lines[1], '│ └ x: 1');
    assert.equal(lines[2], '└ b: 2');
  });

  it('drops the spine (uses empty) under the last key', () => {
    const out = treeify({ a: '1', b: { y: '2' } });
    const lines = out.split('\n');
    assert.equal(lines[0], '├ a: 1');
    assert.equal(lines[1], '└ b');
    // Last parent "b" descends with a blank continuation, not "│".
    assert.equal(lines[2], '  └ y: 2');
  });

  it('summarises arrays as [Array(n)] rather than recursing', () => {
    const out = treeify({ list: [1, 2, 3] as unknown as TreeNode[keyof TreeNode] });
    assert.equal(out, '└ list: [Array(3)]');
  });

  it('renders functions as [Function] by default', () => {
    const out = treeify({ fn: (() => 1) as unknown as TreeNode[keyof TreeNode] });
    assert.equal(out, '└ fn: [Function]');
  });

  it('filters out function-valued keys when hideFunctions is set', () => {
    const node: TreeNode = { keep: 'yes', fn: (() => 1) as unknown as TreeNode[keyof TreeNode] };
    const out = treeify(node, { hideFunctions: true });
    assert.equal(out, '└ keep: yes');
  });

  it('returns the (empty) sentinel for an empty object', () => {
    assert.equal(treeify({}), '(empty)');
  });

  it('coerces non-string primitives via String() when showValues', () => {
    const node: TreeNode = { n: 5, b: true, nul: null };
    const out = treeify(node);
    const lines = out.split('\n');
    assert.equal(lines[0], '├ n: 5');
    assert.equal(lines[1], '├ b: true');
    assert.equal(lines[2], '└ nul: null');
  });

  it('omits leaf values entirely when showValues is false', () => {
    const out = treeify({ a: '1', b: '2' }, { showValues: false });
    const lines = out.split('\n');
    assert.equal(lines[0], '├ a');
    assert.equal(lines[1], '└ b');
  });

  it('guards circular references at the child-descent site', () => {
    const node: TreeNode = { name: 'root' };
    node.self = node;
    const out = treeify(node);
    const lines = out.split('\n');
    assert.equal(lines[0], '├ name: root');
    assert.equal(lines[1], '└ self: [Circular]');
  });

  it('guards circular references at the node-entry site (shared child seen twice)', () => {
    const shared: TreeNode = { v: '1' };
    const node: TreeNode = { a: shared, b: shared };
    const out = treeify(node);
    const lines = out.split('\n');
    // First occurrence expands; second has already been visited -> [Circular].
    assert.equal(lines[0], '├ a');
    assert.equal(lines[1], '│ └ v: 1');
    assert.equal(lines[2], '└ b: [Circular]');
  });

  it('renders a single whitespace-only string key as just the value', () => {
    const out = treeify({ '': 'bare-value' });
    assert.equal(out, '└ bare-value');
  });

  it('suppresses the colon (and key text) for blank keys with an object value', () => {
    const out = treeify({ '': { x: '1' } });
    const lines = out.split('\n');
    // Blank key: no key text, no colon on the parent line.
    assert.equal(lines[0], '└');
    assert.equal(lines[1], '  └ x: 1');
  });

  it('suppresses the colon for a blank key with an array/leaf value', () => {
    const arrOut = treeify({ '': [1] as unknown as TreeNode[keyof TreeNode] });
    assert.equal(arrOut, '└ [Array(1)]');
    const leafOut = treeify({ '': 5 as unknown as TreeNode[keyof TreeNode] });
    assert.equal(leafOut, '└ 5');
  });

  it('applies the colorize seam to tree chars, keys, and values', () => {
    const calls: Array<[string, string]> = [];
    const out = treeify(
      { a: '1' },
      {
        colorize: (text, role) => {
          calls.push([role, text]);
          return `<${role}>${text}</${role}>`;
        },
      },
    );
    assert.equal(out, '<treeChar>└</treeChar> <key>a</key>: <value>1</value>');
    assert.ok(calls.some(([r]) => r === 'treeChar'));
    assert.ok(calls.some(([r]) => r === 'key'));
    assert.ok(calls.some(([r]) => r === 'value'));
  });

  it('colorizes the (empty) sentinel and [Circular] markers via the seam', () => {
    assert.equal(
      treeify({}, { colorize: (t, r) => `[${r}:${t}]` }),
      '[value:(empty)]',
    );
    const node: TreeNode = {};
    node.self = node;
    const out = treeify(node, { colorize: (t) => (t === '[Circular]' ? 'CYC' : t) });
    assert.equal(out, '└ self: CYC');
  });

  it('does not crash on deeply nested, mixed-type structures', () => {
    const node: TreeNode = {
      level1: { level2: { level3: { leaf: 'deep' } } },
      arr: [1, 2] as unknown as TreeNode[keyof TreeNode],
      count: 42,
    };
    const out = treeify(node);
    assert.ok(out.includes('leaf: deep'));
    assert.ok(out.includes('[Array(2)]'));
    assert.ok(out.includes('count: 42'));
  });

  it('exports TREE_COLOR_ROLES for palette consumers', () => {
    assert.deepEqual([...TREE_COLOR_ROLES], ['treeChar', 'key', 'value']);
  });

  it('blank separator when colorize strips the tree char (line becomes empty)', () => {
    // shouldAddColon is false for a whitespace key; when colorize empties the
    // tree-char, `line` is '' so separator() returns '' (not the spaced form).
    const out = treeify(
      { ' ': 5 as unknown as TreeNode[keyof TreeNode] },
      { colorize: (text, role) => (role === 'treeChar' ? '' : text) },
    );
    assert.equal(out, '5');
  });
});
