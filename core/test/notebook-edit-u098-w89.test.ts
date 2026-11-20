import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  describeToolActivity,
  createActivityDescriptionResolver,
} from '../src/tools/tool-activity-class';
import { notebookEditTool } from '../src/tools/notebook-edit';

// U098 (w89) — cell-aware live-progress activity hint for notebook_edit.
//
// Parity: the reference's NotebookEditTool `getActivityDescription` returns
// "Editing notebook <summary>" where the summary names the targeted cell. AMI's
// core `describeToolActivity` previously shared the plain file_edit case
// ("Editing <path>") for notebook_edit — it neither used the "notebook" verb
// (inconsistent with cli/src/subagent-roster.ts, which already does) nor named
// the targeted cell. These tests lock in the cell-aware phrase and prove the
// notebook_edit tool now EXPOSES the getActivityDescription hook that
// createActivityDescriptionResolver (used by task.ts progress) prefers.
describe('U098/w89 notebook_edit cell-aware activity hint', () => {
  it('names the notebook and the explicit cell_id', () => {
    assert.equal(
      describeToolActivity('notebook_edit', {
        notebook_path: '/ws/analysis.ipynb',
        cell_id: 'abc123',
      }),
      'Editing notebook /ws/analysis.ipynb abc123',
    );
  });

  it('derives the positional cell-N alias from a numeric cell_number', () => {
    assert.equal(
      describeToolActivity('notebook_edit', {
        notebook_path: '/ws/analysis.ipynb',
        cell_number: 2,
      }),
      'Editing notebook /ws/analysis.ipynb cell-2',
    );
  });

  it('accepts the quoted-number form providers sometimes emit', () => {
    assert.equal(
      describeToolActivity('notebook_edit', {
        notebook_path: '/ws/analysis.ipynb',
        cell_number: '3',
      }),
      'Editing notebook /ws/analysis.ipynb cell-3',
    );
  });

  it('prefers cell_id over cell_number when both are present', () => {
    assert.equal(
      describeToolActivity('notebook_edit', {
        notebook_path: '/ws/n.ipynb',
        cell_id: 'real-id',
        cell_number: 7,
      }),
      'Editing notebook /ws/n.ipynb real-id',
    );
  });

  it('falls back to the notebook path alone when no cell is targeted', () => {
    assert.equal(
      describeToolActivity('notebook_edit', { notebook_path: '/ws/n.ipynb' }),
      'Editing notebook /ws/n.ipynb',
    );
  });

  it('reads notebook_path in preference to a stray file_path', () => {
    assert.equal(
      describeToolActivity('notebook_edit', {
        notebook_path: '/ws/wins.ipynb',
        file_path: '/ws/loses.ipynb',
      }),
      'Editing notebook /ws/wins.ipynb',
    );
  });

  it('degrades to the bare verb with no path', () => {
    assert.equal(describeToolActivity('notebook_edit', {}), 'Editing notebook');
  });

  it('ignores a non-finite cell_number rather than printing cell-NaN', () => {
    assert.equal(
      describeToolActivity('notebook_edit', {
        notebook_path: '/ws/n.ipynb',
        cell_number: 'not-a-number',
      }),
      'Editing notebook /ws/n.ipynb',
    );
  });

  it('leaves the file_edit phrase unchanged (no "notebook" verb, no cell)', () => {
    assert.equal(
      describeToolActivity('file_edit', { file_path: 'src/a.ts' }),
      'Editing src/a.ts',
    );
    assert.equal(describeToolActivity('file_edit', {}), 'Editing file');
  });

  it('notebook_edit tool exposes a getActivityDescription hook (parity surface)', () => {
    assert.equal(typeof notebookEditTool.getActivityDescription, 'function');
    assert.equal(
      notebookEditTool.getActivityDescription!({
        notebook_path: '/ws/analysis.ipynb',
        cell_id: 'abc123',
      }),
      'Editing notebook /ws/analysis.ipynb abc123',
    );
    // Bare verb when no path is supplied.
    assert.equal(notebookEditTool.getActivityDescription!({}), 'Editing notebook');
  });

  it('is reached through createActivityDescriptionResolver (task.ts progress path)', () => {
    const resolve = createActivityDescriptionResolver([notebookEditTool]);
    assert.equal(
      resolve('notebook_edit', { notebook_path: '/ws/n.ipynb', cell_number: 4 }),
      'Editing notebook /ws/n.ipynb cell-4',
    );
  });
});
