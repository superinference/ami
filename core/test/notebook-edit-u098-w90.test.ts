import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { notebookEditTool } from '../src/tools/notebook-edit';

// U098 (w90) — tool introspection/affordance metadata for notebook_edit.
//
// Parity: Claude's NotebookEditTool exposes `userFacingName: () => 'Edit Notebook'`
// alongside a search affordance. AMI already carried `searchHint`, but the
// `userFacingName` affordance (typed on ToolDefinition) was never set on the tool,
// so the stable snake_case id `notebook_edit` had no readable display name for
// discovery/tooling. This locks in the affordance the `ami --introspect` manifest
// now surfaces (the wired consumer).
describe('U098/w90 notebook_edit affordance metadata', () => {
  it('exposes a static userFacingName of "Edit Notebook"', () => {
    assert.equal(typeof notebookEditTool.userFacingName, 'function');
    assert.equal(notebookEditTool.userFacingName!({}), 'Edit Notebook');
  });

  it('ignores the call arguments (the label is static)', () => {
    assert.equal(
      notebookEditTool.userFacingName!({ notebook_path: '/ws/x.ipynb', edit_mode: 'delete' }),
      'Edit Notebook',
    );
  });

  it('keeps the tool_search discovery hint carrying jupyter/ipynb synonyms', () => {
    assert.ok(
      typeof notebookEditTool.searchHint === 'string' &&
        notebookEditTool.searchHint.includes('jupyter') &&
        notebookEditTool.searchHint.includes('ipynb'),
    );
  });
});
