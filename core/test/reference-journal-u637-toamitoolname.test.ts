/**
 * U637 (w118) — reverse tool-name mapping (reference vocabulary → internal name),
 * the inverse of toReferenceToolName. Used to count memory accesses from the
 * persisted reference journal (which stores Read/Grep/… names).
 */
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { toAmiToolName, toReferenceToolName } from '../src/reference-journal';

describe('toAmiToolName', () => {
  it('maps reference names back to the canonical internal name', () => {
    assert.equal(toAmiToolName('Read'), 'file_read');
    assert.equal(toAmiToolName('Edit'), 'file_edit');
    assert.equal(toAmiToolName('Write'), 'file_write');
    assert.equal(toAmiToolName('Grep'), 'grep');
    assert.equal(toAmiToolName('Glob'), 'glob');
    assert.equal(toAmiToolName('Bash'), 'bash');
  });

  it('keeps the FIRST (canonical) internal name for a many-to-one reference name', () => {
    // grep / find_references / search_symbols all map to Grep; the reverse is grep.
    assert.equal(toReferenceToolName('find_references'), 'Grep');
    assert.equal(toAmiToolName('Grep'), 'grep');
  });

  it('passes through internal, unknown, and MCP names unchanged', () => {
    assert.equal(toAmiToolName('file_read'), 'file_read');
    assert.equal(toAmiToolName('nope'), 'nope');
    assert.equal(toAmiToolName('mcp__srv__do'), 'mcp__srv__do');
  });

  it('round-trips an internal name through the reference vocabulary', () => {
    for (const name of ['file_read', 'file_edit', 'file_write', 'grep', 'glob', 'bash']) {
      assert.equal(toAmiToolName(toReferenceToolName(name)), name);
    }
  });
});
