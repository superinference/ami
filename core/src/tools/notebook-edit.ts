import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';

interface NotebookCell {
  cell_type: string;
  source: string | string[];
  metadata: Record<string, unknown>;
  id?: string;
  execution_count?: number | null;
  outputs?: unknown[];
}

interface NotebookContent {
  nbformat: number;
  nbformat_minor: number;
  metadata: Record<string, unknown>;
  cells: NotebookCell[];
}

export const notebookEditTool: ToolDefinition = {
  name: 'notebook_edit',
  description:
    'Edit a Jupyter notebook (.ipynb file). Can replace cell content, insert new cells, or delete cells.',
  inputSchema: {
    type: 'object',
    properties: {
      notebook_path: {
        type: 'string',
        description: 'Path to the .ipynb file',
      },
      cell_number: {
        type: 'number',
        description: '0-based cell index to edit',
      },
      new_source: {
        type: 'string',
        description: 'New source content for the cell',
      },
      cell_type: {
        type: 'string',
        description: 'Cell type: code or markdown',
        enum: ['code', 'markdown'],
      },
      edit_mode: {
        type: 'string',
        description: 'replace, insert, or delete',
        enum: ['replace', 'insert', 'delete'],
      },
    },
    required: ['notebook_path', 'new_source'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const notebookPath = input.notebook_path as string;
    const newSource = input.new_source as string;
    const cellNumber = input.cell_number as number | undefined;
    const cellType = (input.cell_type as string) ?? undefined;
    const editMode = (input.edit_mode as string) ?? 'replace';

    // --- Validate inputs ---
    if (!notebookPath || notebookPath.trim().length === 0) {
      return {
        output: 'Error: notebook_path must not be empty.',
        isError: true,
      };
    }

    if (editMode !== 'replace' && editMode !== 'insert' && editMode !== 'delete') {
      return {
        output: `Error: edit_mode must be "replace", "insert", or "delete". Got "${editMode}".`,
        isError: true,
      };
    }

    const resolved = path.isAbsolute(notebookPath)
      ? notebookPath
      : path.resolve(context.cwd, notebookPath);

    if (path.extname(resolved) !== '.ipynb') {
      return {
        output: 'Error: File must be a Jupyter notebook (.ipynb).',
        isError: true,
      };
    }

    // --- Read and parse the notebook ---
    let raw: string;
    try {
      raw = await fs.promises.readFile(resolved, 'utf-8');
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      if (msg.includes('ENOENT')) {
        return {
          output: `Error: Notebook file not found: ${resolved}`,
          isError: true,
        };
      }
      return {
        output: `Error reading notebook: ${msg}`,
        isError: true,
      };
    }

    let notebook: NotebookContent;
    try {
      notebook = JSON.parse(raw);
    } catch {
      return {
        output: 'Error: Notebook file is not valid JSON.',
        isError: true,
      };
    }

    // Validate minimal notebook structure
    if (!notebook || !Array.isArray(notebook.cells)) {
      return {
        output:
          'Error: Invalid notebook structure — missing "cells" array.',
        isError: true,
      };
    }

    // --- Determine the target cell index ---
    const totalCells = notebook.cells.length;

    // For insert mode with no cell_number, insert at the end
    const effectiveIndex =
      cellNumber !== undefined && cellNumber !== null
        ? cellNumber
        : editMode === 'insert'
          ? totalCells // insert at the end
          : 0; // default to first cell for replace

    // --- Perform the edit ---
    if (editMode === 'delete') {
      if (effectiveIndex < 0 || effectiveIndex >= totalCells) {
        return {
          output: `Error: cell_number ${effectiveIndex} is out of range (notebook has ${totalCells} cells, indices 0-${totalCells - 1}).`,
          isError: true,
        };
      }
      notebook.cells.splice(effectiveIndex, 1);
    } else if (editMode === 'insert') {
      if (effectiveIndex < 0 || effectiveIndex > totalCells) {
        return {
          output: `Error: cell_number ${effectiveIndex} is out of range for insert (valid range 0-${totalCells}).`,
          isError: true,
        };
      }

      const type = cellType ?? 'code';
      const newCell: NotebookCell = {
        cell_type: type,
        source: newSource,
        metadata: {},
      };

      if (type === 'code') {
        newCell.execution_count = null;
        newCell.outputs = [];
      }

      // Generate an ID for notebooks that support it (nbformat >= 4.5)
      if (
        notebook.nbformat > 4 ||
        (notebook.nbformat === 4 && notebook.nbformat_minor >= 5)
      ) {
        newCell.id = Math.random().toString(36).substring(2, 15);
      }

      notebook.cells.splice(effectiveIndex, 0, newCell);
    } else {
      // replace
      if (effectiveIndex < 0 || effectiveIndex >= totalCells) {
        return {
          output: `Error: cell_number ${effectiveIndex} is out of range (notebook has ${totalCells} cells, indices 0-${totalCells - 1}).`,
          isError: true,
        };
      }

      const target = notebook.cells[effectiveIndex];
      target.source = newSource;

      // Reset execution state for code cells
      if (target.cell_type === 'code') {
        target.execution_count = null;
        target.outputs = [];
      }

      // Optionally change the cell type
      if (cellType && cellType !== target.cell_type) {
        target.cell_type = cellType;
      }
    }

    // --- Write the modified notebook back ---
    try {
      const updatedJson = JSON.stringify(notebook, null, 1);
      await fs.promises.writeFile(resolved, updatedJson, 'utf-8');
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing notebook: ${msg}`,
        isError: true,
      };
    }

    // --- Build confirmation message ---
    const newTotal = notebook.cells.length;
    const modeLabel =
      editMode === 'delete'
        ? `Deleted cell ${effectiveIndex}`
        : editMode === 'insert'
          ? `Inserted new ${cellType ?? 'code'} cell at index ${effectiveIndex}`
          : `Replaced cell ${effectiveIndex}`;

    return {
      output: `${modeLabel} in ${resolved}. Notebook now has ${newTotal} cell(s).`,
    };
  },
};
