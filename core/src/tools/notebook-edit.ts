import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { coerceNumber } from '../utils/coerce';
import { getFileCache, describeFileChangeMessage } from '../file-cache';
import { trackFileHistory } from './file-history';
import { captureLspBaseline } from './file-edit';
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

/** Derive the notebook's programming language from `metadata.language_info.name`,
 *  defaulting to 'python' when absent/blank. Parity: Claude's NotebookEditTool
 *  (L379 — `notebook.metadata.language_info?.name ?? 'python'`), so the result
 *  carries language context for the edited cell. */
function deriveNotebookLanguage(nb: NotebookContent): string {
  const li = nb.metadata?.language_info as { name?: unknown } | undefined;
  const name = li && typeof li.name === 'string' ? li.name.trim() : '';
  return name.length > 0 ? name : 'python';
}

/** Derive the syntax-highlight language for a single cell: a markdown cell is
 *  highlighted as 'markdown', any other (code) cell as the notebook's programming
 *  language. Parity: Claude's NotebookEditPermissionRequest (L40 —
 *  `cell_type === 'markdown' ? 'markdown' : 'python'`), generalized so a non-Python
 *  kernel's code cells get the correct language rather than a hardcoded 'python'. */
function deriveCellLanguage(cellType: string | undefined, notebookLanguage: string): string {
  return cellType === 'markdown' ? 'markdown' : notebookLanguage;
}

/** Structured, machine-readable result surface for `notebook_edit`, carried on
 *  {@link ToolResult.metadata} and described by the tool's `outputSchema`. Parity:
 *  Claude's NotebookEditTool output `data` (new_source / cell_type / language /
 *  edit_mode / cell_id / error / notebook_path / original_file / updated_file), so
 *  a resume/SDK/non-LLM caller — and attribution tracking — can branch on the
 *  before/after content and resolved language without parsing the human message. */
interface NotebookEditMetadata {
  notebook_path: string;
  new_source: string;
  cell_type: string;
  /** The notebook's programming language (metadata.language_info.name ?? 'python'). */
  language: string;
  /** The syntax-highlight language for the edited cell (markdown vs the notebook language). */
  cell_language: string;
  edit_mode: string;
  cell_id?: string;
  error: string;
  original_file: string;
  updated_file: string;
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
  // PARITY (additive): structured-result schema describing the machine-readable
  // outcome AMI carries on `ToolResult.metadata` ({@link NotebookEditMetadata}).
  // Mirrors Claude's NotebookEditTool `outputSchema` (new_source / cell_type /
  // language / edit_mode / cell_id / error / notebook_path / original_file /
  // updated_file) so a resume/SDK/non-LLM caller — and attribution tracking — can
  // branch on the before/after notebook content and resolved language without
  // parsing the human message. Object-rooted so it passes the provider's
  // object-root-only exposure guard (toolDefinitionsToOpenAI).
  outputSchema: {
    type: 'object',
    properties: {
      notebook_path: { type: 'string', description: 'Absolute path of the edited notebook.' },
      new_source: { type: 'string', description: 'The source written to (or that was targeted at) the cell.' },
      cell_type: { type: 'string', description: "Resolved type of the affected cell ('code' | 'markdown')." },
      language: { type: 'string', description: "The notebook's programming language (metadata.language_info.name, default 'python')." },
      cell_language: { type: 'string', description: "Syntax-highlight language for the affected cell (markdown vs the notebook language)." },
      edit_mode: { type: 'string', description: "The edit mode actually applied ('replace' | 'insert' | 'delete')." },
      cell_id: { type: 'string', description: 'Stable id of the affected cell, when the notebook format carries ids.' },
      error: { type: 'string', description: 'Error message when the operation failed, else empty.' },
      original_file: { type: 'string', description: 'Full notebook JSON before the edit (attribution).' },
      updated_file: { type: 'string', description: 'Full notebook JSON after the edit (attribution).' },
    },
    required: ['notebook_path', 'edit_mode', 'language', 'error'],
  },

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const notebookPath = input.notebook_path as string;
    const newSource = input.new_source as string;
    // PARITY (semanticNumber, utils/coerce.ts): models/providers frequently
    // serialize a numeric tool argument as a QUOTED string ("2"). A raw
    // `input.cell_number as number` kept that string, so range checks and array
    // indexing ran against `"2"` — mixing string/number comparisons and silently
    // mis-targeting cells. Coerce a finite number or numeric string to a real
    // integer; reject any other non-empty shape (e.g. "abc", {}) up front instead
    // of letting a garbage index fall through. Absent/null stays `undefined`.
    let cellNumber: number | undefined;
    const rawCellNumber = input.cell_number;
    if (rawCellNumber !== undefined && rawCellNumber !== null) {
      const coerced = coerceNumber(rawCellNumber, NaN, { integer: true });
      if (!Number.isFinite(coerced)) {
        return {
          output: `Error: cell_number must be a number. Got ${JSON.stringify(rawCellNumber)}.`,
          isError: true,
        };
      }
      cellNumber = coerced;
    }
    const cellId = input.cell_id as string | undefined;
    const cellType = (input.cell_type as string) ?? undefined;
    let editMode = (input.edit_mode as string) ?? 'replace';

    // --- Validate inputs ---
    const nullCheck = validateFilePath(notebookPath || '');
    if (nullCheck) return nullCheck;

    // PARITY (Claude write-path glob guard; mirrors file-edit.ts): a glob pattern
    // is never a valid write/edit target — the tool uses the path literally, so a
    // `*`/`?` wildcard can only be a model mistake that would create a literally
    // named file or silently miss the intended notebook. Reject with an actionable,
    // self-correcting message. Only the unambiguous wildcards `*`/`?` are flagged so
    // legitimate bracket/brace filenames stay editable (same superset divergence as
    // file-edit.ts).
    if (notebookPath && /[*?]/.test(notebookPath)) {
      return {
        output: `Error: Glob patterns are not allowed in write operations. Please specify an exact file path (got "${notebookPath}").`,
        isError: true,
      };
    }

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
      // FUNCTIONAL (Claude parity, errorCode 2): append a recovery affordance so a
      // wrong-file-type call is not a dead end — the model is told exactly which
      // tool to reach for instead. Mirrors Claude's NotebookEditTool message
      // ("For editing other file types, use the FileEdit tool.").
      return {
        output: 'Error: File must be a Jupyter notebook (.ipynb file). For editing other file types, use the file_edit tool.',
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
    // Source of the cell removed by a delete, captured BEFORE the splice so the
    // confirmation can surface what was deleted (parity with Claude's
    // NotebookEditToolDiff, which shows the deleted cell's source on delete).
    let deletedSource: string | undefined;
    // The stable id and resolved type of the cell actually touched by this edit,
    // surfaced in the confirmation message and the structured metadata so the model
    // (or a non-LLM/SDK caller) can correlate the edit back to a specific cell.
    // Parity: Claude's `new_cell_id` + `cell_type` result fields (NotebookEditTool
    // L379-389, L444-446).
    let affectedCellId: string | undefined;
    let affectedCellType: string | undefined;

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
      deletedSource = cellSourceToString(notebook.cells[effectiveIndex].source);
      affectedCellId = notebook.cells[effectiveIndex].id;
      affectedCellType = notebook.cells[effectiveIndex].cell_type;
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

      affectedCellId = newCell.id;
      affectedCellType = newCell.cell_type;
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
        affectedCellId = newCell.id;
        affectedCellType = newCell.cell_type;
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

        // Parity (NotebookEditTool L386-388): on replace the existing cell id is
        // kept, so surface it (and the resolved type) for correlation/attribution.
        affectedCellId = target.id;
        affectedCellType = target.cell_type;
      }
    }

    // --- Write the modified notebook back (atomic temp + rename) ---
    // JSON.stringify cannot throw for this plain notebook object (no cycles / BigInt),
    // so it is computed outside the try to avoid an uncoverable catch arm.
    const updatedJson = JSON.stringify(notebook, null, 1);

    // PARITY (additive, LSPDiagnosticRegistry.ts L51-56,L290-312,L372-379):
    // snapshot the file's diagnostics into the LSP baseline BEFORE the destructive
    // write — the per-file invalidation that makes cross-turn diagnostic dedup
    // correct for notebook edits. Without it a notebook edit never rolled its
    // per-file baseline, so a later `diagnostics` query would mis-dedup (surface
    // pre-existing noise, or wrongly suppress the edit's own new issues). Reuses
    // file_edit's shared, detached-aware, fire-and-forget helper (bounded-LRU
    // retention lives in DiagnosticTracker) — no duplicate LSP-baseline logic.
    captureLspBaseline(resolved, context);

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
    // Parity (NotebookEditTool L379): resolve the notebook's language once so the
    // confirmation and structured metadata both report it.
    const nbLanguage = deriveNotebookLanguage(notebook);
    const cellLanguage = deriveCellLanguage(affectedCellType, nbLanguage);
    // Parity (NotebookEditTool L384-389): surface the affected cell's stable id so
    // the model can correlate the edit to a specific cell across turns.
    const idSuffix = affectedCellId ? ` (cell id: ${affectedCellId})` : '';
    const modeLabel =
      editMode === 'delete'
        ? `Deleted cell ${effectiveIndex}${idSuffix}`
        : editMode === 'insert'
          ? `Inserted new ${affectedCellType ?? cellType ?? 'code'} cell at index ${effectiveIndex}${idSuffix}`
          : `Replaced cell ${effectiveIndex}${idSuffix}`;

    // Per-mode cell preview (parity with Claude's NotebookEditToolDiff, which
    // renders a distinct body per edit_mode): a unified diff of old→new source for
    // REPLACE, the new cell source for INSERT, and the removed cell source for
    // DELETE. Previously only replace surfaced a preview; insert/delete returned a
    // bare one-liner so the model/user never saw the cell that was added or removed.
    // Source previews are emitted verbatim (no `-`/`+`/`@@` diff markers) so they
    // read as plain cell bodies and are not mis-classified as unified diffs.
    let previewBlock = '';
    if (replaceDiff && replaceDiff !== '(no changes detected)') {
      previewBlock = `\n\n${replaceDiff}`;
    } else if (editMode === 'insert') {
      previewBlock = `\n\nInserted cell source:\n${newSource}`;
    } else if (editMode === 'delete' && deletedSource !== undefined) {
      previewBlock = `\n\nDeleted cell source:\n${deletedSource}`;
    }
    // PARITY (additive): attach the structured attribution surface (before/after
    // notebook content + resolved language/cell id) to the result metadata. The
    // shape is declared by this tool's `outputSchema` (advertised to the model via
    // the provider), so the human message stays unchanged while an SDK/non-LLM/
    // resume consumer can read the fields programmatically.
    const structured: NotebookEditMetadata = {
      notebook_path: resolved,
      new_source: editMode === 'delete' ? '' : newSource,
      cell_type: affectedCellType ?? cellType ?? 'code',
      language: nbLanguage,
      cell_language: cellLanguage,
      edit_mode: editMode,
      error: '',
      original_file: raw,
      updated_file: updatedJson,
    };
    if (affectedCellId !== undefined) structured.cell_id = affectedCellId;

    return {
      output: `${modeLabel} in ${resolved} (language: ${nbLanguage}). Notebook now has ${newTotal} cell(s).${previewBlock}`,
      metadata: { ...structured },
    };
  },
};
