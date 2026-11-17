import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { deriveNotebookLanguage, deriveCellLanguage } from '../src/tools/notebook-edit';

// ---------------------------------------------------------------------------
// U098-w84 — notebook cell highlight-language derivation exported as the single
// canonical source reused by the permission-time cell preview (common/src/
// permission-preview.ts). Parity: Claude's NotebookEditToolDiff selects the
// highlight lexer by cell_type (markdown vs the notebook's language_info.name).
// These guard the exported helpers' behaviour and edge cases so a consumer can
// derive the cell language without re-implementing the metadata lookup.
// ---------------------------------------------------------------------------
describe('U098-w84 — exported notebook language derivation', () => {
  describe('deriveNotebookLanguage', () => {
    it('returns metadata.language_info.name when present', () => {
      assert.equal(deriveNotebookLanguage({ metadata: { language_info: { name: 'julia' } } }), 'julia');
    });

    it('defaults to python when language_info is absent', () => {
      assert.equal(deriveNotebookLanguage({ metadata: {} }), 'python');
    });

    it('defaults to python when metadata is absent or null', () => {
      assert.equal(deriveNotebookLanguage({}), 'python');
      assert.equal(deriveNotebookLanguage({ metadata: null }), 'python');
    });

    it('treats a blank/whitespace name as absent (python)', () => {
      assert.equal(deriveNotebookLanguage({ metadata: { language_info: { name: '   ' } } }), 'python');
    });

    it('ignores a non-string name', () => {
      assert.equal(deriveNotebookLanguage({ metadata: { language_info: { name: 42 } } }), 'python');
    });
  });

  describe('deriveCellLanguage', () => {
    it('maps a markdown cell to markdown regardless of notebook language', () => {
      assert.equal(deriveCellLanguage('markdown', 'julia'), 'markdown');
    });

    it('uses the notebook language for a code cell', () => {
      assert.equal(deriveCellLanguage('code', 'julia'), 'julia');
    });

    it('uses the notebook language when the cell type is unknown', () => {
      assert.equal(deriveCellLanguage(undefined, 'python'), 'python');
    });
  });
});
