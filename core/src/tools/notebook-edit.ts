import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { getFileCache, describeFileChangeMessage } from '../file-cache';
import { trackFileHistory } from './file-history';
import {
  validateFilePath,
  validateFileWritePath,
  buildUnifiedDiff,
  decodeBuffer,
  encodeText,
  detectLineEnding,
  convertToLineEnding,
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
    'Replace, insert, or delete a specific cell in a Jupyter notebook (.ipynb file). ' +
    'Jupyter notebooks are interactive documents that combine code, text, and visualizations, ' +
    'commonly used for data analysis and scientific computing. The notebook_path must be an ' +
    'absolute path. Use edit_mode=insert to add a new cell after the selected cell (or at the ' +
    'end when no cell is selected); use edit_mode=delete to remove the selected cell.',
  inputSchema: {
    type: 'object',
    properties: {
      notebook_path: {
        type: 'string',
        description: 'The absolute path to the Jupyter notebook file to edit (must be absolute, not relative).',
      },
      cell_number: {
        type: 'number',
        description: '0-based cell index to edit. Ignored when cell_id resolves to a cell.',
      },
      cell_id: {
        type: 'string',
        description:
          'The ID of the cell to edit. Matched against the cell\'s actual id first, then against the ' +
          'positional "cell-N" alias. When inserting a new cell, it is inserted after the cell with this ID. ' +
          'Takes precedence over cell_number if both provided.',
      },
      new_source: {
        type: 'string',
        description: 'The new source for the cell.',
      },
      cell_type: {
        type: 'string',
        description:
          'The type of the cell (code or markdown). If not specified, it defaults to the current cell type. ' +
          'Required when edit_mode is insert.',
        enum: ['code', 'markdown'],
      },
      edit_mode: {
        type: 'string',
        description:
          'The type of edit to make. Defaults to "replace". "replace" overwrites the ' +
          'selected cell\'s source (resetting a code cell\'s execution state); "insert" ' +
          'adds a NEW cell AFTER the selected cell (identified by cell_id/cell_number), or ' +
          'at the end of the notebook when no cell is selected, and requires cell_type; ' +
          '"delete" removes the selected cell.',
        enum: ['replace', 'insert', 'delete'],
      },
    },
    required: ['notebook_path', 'new_source'],
    // Strict object (parity: Claude's z.strictObject): reject unknown/typo'd
    // parameters up front via the shared tool-input validator so a hallucinated
    // key fails loudly instead of being silently dropped.
    additionalProperties: false,
  },
  isReadOnly: false,
  // Discovery hint for tool_search using synonyms NOT already in the name/
  // description, so a deferred notebook_edit is findable by "jupyter"/"ipynb".
  searchHint: 'jupyter ipynb notebook cell code markdown edit insert delete',

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

    // FUNCTIONAL (Claude parity, errorCode 5): when inserting a new cell there is
    // no existing cell to inherit the type from, so cell_type is required. Without
    // this guard a bare insert silently defaulted to a code cell, which can produce
    // the wrong cell kind (e.g. markdown content persisted as a code cell).
    if (editMode === 'insert' && cellType === undefined) {
      return {
        output: 'Error: cell_type is required when using edit_mode=insert (specify "code" or "markdown").',
        isError: true,
      };
    }

    // FUNCTIONAL: reject an invalid cell_type up front. The input schema declares
    // the enum ['code', 'markdown'], but execute() previously accepted any string
    // and would persist a bogus cell_type into the notebook JSON (corrupting it for
    // Jupyter). Validate here so an unknown type fails loudly instead of silently
    // writing an invalid cell.
    if (cellType !== undefined && cellType !== 'code' && cellType !== 'markdown') {
      return {
        output: `Error: cell_type must be "code" or "markdown". Got "${cellType}".`,
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

    const nbCache = getFileCache(context.cwd, context.agentId);
    const nbChange = nbCache.describeChange(resolved);
    if (nbChange.changed) {
      nbCache.delete(resolved);
      return { output: `Error: ${describeFileChangeMessage(nbChange.reason, 'editing the notebook')}`, isError: true };
    }

    // --- Read and parse the notebook ---
    // Read as raw bytes so the original encoding (BOM / UTF-16) and line endings
    // can be preserved on write, mirroring file-edit.ts. Editing a UTF-16 or
    // BOM-prefixed / CRLF notebook as plain UTF-8 would otherwise silently rewrite
    // its encoding and churn every line.
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
    const { text: raw, encoding } = decodeBuffer(rawBuf);
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
    // Claude resolution order (NotebookEditTool L270-288): match the cell's ACTUAL
    // id first, and only fall back to the positional "cell-N" alias when no real id
    // matches. The alias regex is strictly anchored (/^cell-(\d+)$/) so a real id
    // like "cell-foo" or "cell-2x" is never mis-parsed as an index.
    if (cellId && cellNumber === undefined) {
      const byId = notebook.cells.findIndex(c => c.id === cellId);
      if (byId !== -1) {
        cellNumber = byId;
      } else {
        const aliasMatch = /^cell-(\d+)$/.exec(cellId);
        if (aliasMatch) {
          const idx = parseInt(aliasMatch[1], 10);
          if (idx >= 0 && idx < notebook.cells.length) {
            cellNumber = idx;
          } else {
            return {
              output: `Error: cell index ${idx} (from cell_id "${cellId}") does not exist (notebook has ${notebook.cells.length} cell(s)).`,
              isError: true,
            };
          }
        } else {
          const available = notebook.cells
            .map((c, i) => c.id ? `${i}:${c.id}` : `${i}:(no id)`)
            .join(', ');
          return {
            output: `Error: cell_id "${cellId}" not found. Available cells: ${available}`,
            isError: true,
          };
        }
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

      // cell_type is required for insert (validated above), so it is always defined here.
      const type = cellType as 'code' | 'markdown';
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

    // --- Write the modified notebook back (atomic temp + rename) ---
    // JSON.stringify cannot throw for this plain notebook object (no cycles / BigInt),
    // so it is computed outside the try to avoid an uncoverable catch arm.
    const updatedJson = JSON.stringify(notebook, null, 1);

    // Pre-edit history snapshot (recovery/undo fallback), mirroring file-edit.ts:
    // persist the ORIGINAL notebook bytes under <cwd>/.ami/file-history/ BEFORE the
    // destructive write so a bad notebook edit can be recovered. Shared helper —
    // best-effort, never fails the edit.
    await trackFileHistory(resolved, raw, context.cwd);

    // FUNCTIONAL: preserve read-only / unwritable-target semantics under the atomic
    // write below. The atomic rename only needs a writable *directory*, so a
    // read-only *file* (whose parent dir is writable) would be silently clobbered by
    // the rename — the opposite of the old direct-writeFile behaviour, which failed.
    // Guard with an explicit writability pre-check so an unwritable target still
    // surfaces an error. The notebook always exists here (it was read above), so this
    // only rejects a genuinely unwritable target (never a missing file).
    try {
      await fs.promises.access(resolved, fs.constants.W_OK);
    } catch {
      return {
        output: `Error writing notebook: target is read-only or not writable: ${resolved}`,
        isError: true,
      };
    }

    // FUNCTIONAL: write to a temp file in the same directory, fsync it, then
    // atomically rename it over the target. The fsync flushes the bytes to disk
    // BEFORE the rename, so a crash/power-loss mid-write can no longer leave a
    // truncated / corrupted .ipynb — the original stays intact until the rename
    // swaps the fully-durable copy in one operation. The temp lives in the same
    // dir so the rename is same-filesystem (atomic), and is hidden so it is never
    // picked up as a stray notebook. The original encoding (BOM / UTF-16) and line
    // endings detected on read are reapplied so an edit never silently rewrites them.
    const outputBytes = encodeText(convertToLineEnding(updatedJson, lineEnding), encoding);
    const tmpPath = path.join(
      path.dirname(resolved),
      `.${path.basename(resolved)}.${process.pid}.${Math.random().toString(36).slice(2)}.tmp`,
    );
    try {
      const fh = await fs.promises.open(tmpPath, 'w');
      try {
        await fh.writeFile(outputBytes);
        await fh.sync();
      } finally {
        await fh.close();
      }
      await fs.promises.rename(tmpPath, resolved);
    } catch (err: unknown) {
      // Best-effort cleanup of the temp file (it may not exist if open failed).
      try {
        await fs.promises.unlink(tmpPath);
      } catch { /* temp file never created — nothing to clean up */ }
      const msg = err instanceof Error ? err.message : String(err);
      return {
        output: `Error writing notebook: ${msg}`,
        isError: true,
      };
    }
    getFileCache(context.cwd, context.agentId).setWritten(resolved, updatedJson);

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
