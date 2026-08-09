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
      cell_id: {
        type: 'string',
        description: 'Cell ID (from notebook metadata) to select the cell. Takes precedence over cell_number if both provided.',
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
    let cellNumber = input.cell_number as number | undefined;
    const cellId = input.cell_id as string | undefined;
    const cellType = (input.cell_type as string) ?? undefined;
    let editMode = (input.edit_mode as string) ?? 'replace';

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

    let resolved = path.isAbsolute(notebookPath)
      ? notebookPath
      : path.resolve(context.cwd, notebookPath);

    try {
      resolved = fs.realpathSync(resolved);
    } catch {
      // File may not exist yet — use the logical path for the check
    }

    if (!path.resolve(resolved).startsWith(path.resolve(context.cwd) + path.sep) &&
        path.resolve(resolved) !== path.resolve(context.cwd)) {
      return { output: `Error: path "${notebookPath}" is outside the workspace directory.`, isError: true };
    }

    if (path.extname(resolved) !== '.ipynb') {
      return {
        output: 'Error: File must be a Jupyter notebook (.ipynb).',
        isError: true,
      };
    }

    if (context.filesRead && !context.filesRead.has(resolved)) {
      return {
        output: `Error: You must read ${resolved} with file_read before editing it. This prevents edits based on stale content.`,
        isError: true,
      };
    }

    try {
      const { getFileCache } = require('../file-cache');
      const cache = getFileCache(context.cwd);
      if (cache?.hasChanged?.(resolved)) {
        return { output: 'Error: Notebook modified since last read. Read it again.', isError: true };
      }
    } catch {}

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

    // --- Resolve cell_id to cell_number ---
    if (cellId && cellNumber === undefined) {
      // Support cell-N alias (e.g. "cell-0", "cell-5")
      if (typeof cellId === 'string' && cellId.startsWith('cell-')) {
        const idx = parseInt(cellId.slice(5), 10);
        if (!isNaN(idx) && idx >= 0 && idx < notebook.cells.length) {
          cellNumber = idx;
        }
      }

      if (cellNumber === undefined) {
        const idx = notebook.cells.findIndex(c => c.id === cellId);
        if (idx === -1) {
          const available = notebook.cells
            .map((c, i) => c.id ? `${i}:${c.id}` : `${i}:(no id)`)
            .join(', ');
          return {
            output: `Error: cell_id "${cellId}" not found. Available cells: ${available}`,
            isError: true,
          };
        }
        cellNumber = idx;
      }
    }

    // --- Determine the target cell index ---
    const totalCells = notebook.cells.length;

    // For insert mode with cell_id match, insert AFTER the matched cell (Claude behavior)
    let effectiveIndex: number;
    if (cellNumber !== undefined && cellNumber !== null) {
      if (editMode === 'insert' && cellId) {
        effectiveIndex = cellNumber + 1;
      } else {
        effectiveIndex = cellNumber;
      }
    } else if (editMode === 'insert') {
      effectiveIndex = totalCells;
    } else {
      effectiveIndex = 0;
    }

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
      if (effectiveIndex < 0) {
        return {
          output: `Error: cell_number ${effectiveIndex} is out of range (notebook has ${totalCells} cells, indices 0-${totalCells - 1}).`,
          isError: true,
        };
      }
      if (effectiveIndex >= totalCells) {
        // Auto-convert replace to insert at end
        editMode = 'insert';
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
        if (
          notebook.nbformat > 4 ||
          (notebook.nbformat === 4 && notebook.nbformat_minor >= 5)
        ) {
          newCell.id = Math.random().toString(36).substring(2, 15);
        }
        notebook.cells.splice(effectiveIndex, 0, newCell);
      } else {
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
    }

    // --- Write the modified notebook back ---
    let updatedJson: string;
    try {
      updatedJson = JSON.stringify(notebook, null, 1);
      await fs.promises.writeFile(resolved, updatedJson, 'utf-8');
      const newStat = await fs.promises.stat(resolved);
      const { getFileCache } = require('../file-cache');
      getFileCache(context.cwd).set(resolved, updatedJson, newStat.mtimeMs);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing notebook: ${msg}`,
        isError: true,
      };
    }

    try {
      const { getLSPClient } = require('../lsp');
      const lsp = getLSPClient();
      lsp.notifyDidChange(resolved, updatedJson, context.cwd).catch(() => {});
      lsp.notifyDidSave(resolved, context.cwd).catch(() => {});
    } catch {}

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
