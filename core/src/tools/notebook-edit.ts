import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { getFileCache } from '../file-cache';
import {
  validateFilePath,
  validateFileWritePath,
  buildUnifiedDiff,
  decodeBuffer,
  encodeText,
  isNonUtfEncoding,
  detectLineEnding,
  convertToLineEnding,
  writeFileAtomicSync,
} from './tool-utils';

/** Normalize a notebook cell source (string or string[]) to a single string. */
function cellSourceToString(source: string | string[] | undefined): string {
  if (Array.isArray(source)) return source.join('');
  return source ?? '';
}

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
    const nullCheck = validateFilePath(notebookPath || '');
    if (nullCheck) return nullCheck;

    if (notebookPath && (notebookPath.startsWith('\\\\') || notebookPath.startsWith('//'))) {
      return { output: `Error: UNC paths are not allowed (potential NTLM credential leak): ${notebookPath}`, isError: true };
    }

    const symlinkCheck = validateFileWritePath(notebookPath);
    if (!symlinkCheck.valid) {
      return { output: `Error: ${symlinkCheck.error}`, isError: true };
    }

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
        output: 'Error: File must be a Jupyter notebook (.ipynb). For editing other file types, use the file_edit tool.',
        isError: true,
      };
    }

    if (context.filesRead && !context.filesRead.has(resolved)) {
      return {
        output: `Error: You must read ${resolved} with file_read before editing it. This prevents edits based on stale content.`,
        isError: true,
      };
    }

    if (getFileCache(context.cwd).hasChanged(resolved)) {
      getFileCache(context.cwd).delete(resolved);
      return { output: 'Error: Notebook modified since last read. Read it again.', isError: true };
    }

    // --- Read and parse the notebook ---
    // Read raw bytes (not a utf-8 string) so the notebook's original character
    // encoding (UTF-8, UTF-8+BOM, UTF-16LE/BE) and CRLF/LF line-ending
    // convention can be detected and round-tripped on write-back, instead of
    // silently normalizing every notebook to LF/UTF-8.
    let rawBuf: Buffer;
    try {
      rawBuf = await fs.promises.readFile(resolved);
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

    // Refuse to edit a notebook stored in a non-UTF legacy encoding: decoding it
    // as UTF-8 and writing it back would silently corrupt the bytes.
    if (isNonUtfEncoding(rawBuf)) {
      return {
        output: `Error: "${resolved}" appears to use a non-UTF encoding. Editing it as UTF-8 would silently corrupt the notebook.`,
        isError: true,
      };
    }

    const { text: raw, encoding: fileEncoding } = decodeBuffer(rawBuf);
    const lineEnding = detectLineEnding(raw);

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
      // Prefer an exact match on a cell's real `id` property. ONLY if no cell
      // owns that id do we fall back to treating a "cell-N" string as a
      // positional alias. The id match must win: a notebook cell whose genuine
      // id is literally "cell-5" (a legal nbformat id) must resolve to that
      // cell, not to positional index 5 — otherwise the wrong cell is edited.
      const byId = notebook.cells.findIndex(c => c.id === cellId);
      if (byId !== -1) {
        cellNumber = byId;
      } else if (typeof cellId === 'string' && cellId.startsWith('cell-')) {
        const idx = parseInt(cellId.slice(5), 10);
        if (!isNaN(idx) && idx >= 0 && idx < notebook.cells.length) {
          cellNumber = idx;
        }
      }

      if (cellNumber === undefined) {
        const available = notebook.cells
          .map((c, i) => c.id ? `${i}:${c.id}` : `${i}:(no id)`)
          .join(', ');
        return {
          output: `Error: cell_id "${cellId}" not found. Available cells: ${available}`,
          isError: true,
        };
      }
    }

    // Require an explicit cell selector for replace/delete. Without this, a missing
    // cell_number AND cell_id falls through to effectiveIndex = 0 and silently
    // replaces or DELETES cell 0 — a destructive, unintended edit. Insert has a
    // well-defined default (append at end), so it is exempt.
    if (editMode !== 'insert' && cellNumber === undefined && cellId === undefined) {
      return {
        output: `Error: cell_id or cell_number must be specified when edit_mode is "${editMode}" (not inserting a new cell).`,
        isError: true,
      };
    }

    // --- Determine the target cell index ---
    const totalCells = notebook.cells.length;
    // Unified diff of the replaced cell's source, appended to the output below.
    let replaceDiff: string | undefined;

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
        // Capture the old source BEFORE overwriting so the confirmation can show a
        // minimal unified diff of what changed in the cell (parity with Claude
        // Code's notebook-edit diff preview).
        replaceDiff = buildUnifiedDiff(
          cellSourceToString(target.source),
          newSource,
          `cell ${effectiveIndex}`,
        );
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
    // JSON.stringify always emits LF; re-apply the file's original line-ending
    // convention and character encoding, then write atomically (temp file +
    // fsync-flushed rename) so a crash or power loss mid-write cannot leave a
    // truncated, unparseable .ipynb on disk.
    let updatedJson: string;
    try {
      updatedJson = convertToLineEnding(JSON.stringify(notebook, null, 1), lineEnding);
      writeFileAtomicSync(resolved, encodeText(updatedJson, fileEncoding));
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing notebook: ${msg}`,
        isError: true,
      };
    }
    // Cache the decoded form (BOM stripped, original line endings preserved) so
    // it matches what the file-cache's decodeBuffer-based freshness check reads
    // back from disk — otherwise a preserved CRLF/BOM would be seen as a change.
    getFileCache(context.cwd).setWritten(resolved, updatedJson);

    if (!context.detachedMode) {
      try {
        const { getLSPClient } = require('../lsp');
        const lsp = getLSPClient();
        lsp.notifyDidChange(resolved, updatedJson, context.cwd).catch(() => {});
        lsp.notifyDidSave(resolved, context.cwd).catch(() => {});
      } catch {}
    }

    // --- Build confirmation message ---
    const newTotal = notebook.cells.length;
    const modeLabel =
      editMode === 'delete'
        ? `Deleted cell ${effectiveIndex}`
        : editMode === 'insert'
          ? `Inserted new ${cellType ?? 'code'} cell at index ${effectiveIndex}`
          : `Replaced cell ${effectiveIndex}`;

    const diffBlock = replaceDiff && replaceDiff !== '(no changes detected)'
      ? `\n\n${replaceDiff}`
      : '';
    return {
      output: `${modeLabel} in ${resolved}. Notebook now has ${newTotal} cell(s).${diffBlock}`,
    };
  },
};
