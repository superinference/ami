// Split from file-read.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { ToolDefinition, ToolResult, ImageContent } from '../types';
import { sliceToReadTokenBudget, fileReadTokenBudgetMessage } from '../token-estimator';
import { getFileReadPromptFlags, type FileReadPromptFlags, type LineNumberStyle } from './file-read-limits';
import { readFileRange, tailFile } from './read-file-range';
import { FILE_IMAGES_KINDS, isPDFSupported } from './file-read-detect-image-format-from-buffer';
import type { FileImagesKind } from './file-read-detect-image-format-from-buffer';

/**
 * Parse a file_read output that carries extracted images, returning the kind, the
 * text, and the image blocks. Returns null for any other output. Mirrors the reference
 * Code's notebook/PDF image-output extraction.
 */
export function parseFileImagesOutput(output: string): { kind: FileImagesKind; text: string; images: ImageContent[] } | null {
  if (typeof output !== 'string') return null;
  const t = output.trimStart();
  if (!t.startsWith('{') || !t.includes('"type":"file_images"')) return null;
  try {
    const parsed = JSON.parse(output);
    if (parsed && parsed.type === 'file_images' && (FILE_IMAGES_KINDS as readonly string[]).includes(parsed.kind)
        && typeof parsed.text === 'string' && Array.isArray(parsed.images)) {
      const images = (parsed.images as unknown[]).filter((i): i is ImageContent =>
        !!i && typeof i === 'object'
        && (i as ImageContent).type === 'image'
        && !!(i as ImageContent).source
        && (i as ImageContent).source.type === 'base64'
        && typeof (i as ImageContent).source.media_type === 'string'
        && typeof (i as ImageContent).source.data === 'string');
      if (images.length > 0) return { kind: parsed.kind, text: parsed.text, images };
    }
  } catch {}
  return null;
}

/**
 * Extract the coordinate-mapping metadata note from a file_read image output, if
 * present, so the engine can surface it to the model alongside the image block.
 * Returns null for non-image outputs or images with no metadata.
 */
export function parseImageToolMetadata(output: string): string | null {
  if (typeof output !== 'string') return null;
  const t = output.trimStart();
  if (!t.startsWith('{') || !t.includes('"metadata"')) return null;
  try {
    const parsed = JSON.parse(output);
    if (parsed && parsed.type === 'image' && typeof parsed.metadata === 'string') {
      return parsed.metadata;
    }
  } catch {}
  return null;
}

/**
 * Actionable stub returned when a FULL re-read hits the unchanged-file cache.
 * Beyond stating the fact, it tells the model what to DO — reuse the earlier
 * Read tool_result instead of looping on re-reads — so a model does not re-issue
 * the read or stall. Mirrors the reference's FILE_UNCHANGED_STUB (FileReadTool/prompt.ts).
 */
export const FILE_UNCHANGED_STUB =
  'File unchanged since last read. The content from the earlier Read tool_result in this conversation is still current — refer to that instead of re-reading.';

/**
 * Marker prefixing the current working directory in a file-not-found error, so
 * the model (and any UI renderer keying off the marker) can self-correct a wrong
 * relative/absolute path assumption. Mirrors the reference's FILE_NOT_FOUND_CWD_NOTE
 * (utils/file.ts).
 */
export const FILE_NOT_FOUND_CWD_NOTE = 'Note: your current working directory is';

/**
 * Replace each run of leading tabs on every line with 2 spaces per tab so
 * rendered file content has consistent, tab-width-independent indentation — a
 * terminal expands a raw `\t` to a variable number of cells (commonly 8), which
 * misaligns the line-number gutter. Fast-paths the common tab-free case to avoid
 * the per-line scan. Mirrors the reference's convertLeadingTabsToSpaces (utils/file.ts).
 */
export function convertLeadingTabsToSpaces(content: string): string {
  if (!content.includes('\t')) return content;
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored ^\t+ per line via /m; linear
  return content.replace(/^\t+/gm, (tabs) => '  '.repeat(tabs.length));
}

/** Pad width used for the arrow line-number style (`     N→`), matching the reference. */
export const ARROW_PREFIX_WIDTH = 6;

/**
 * Prefix a single already-tab-normalized source line with its 1-based line number
 * in the active {@link LineNumberStyle}:
 *   - `compact` → `${num}\t${line}` (num right-padded to `compactWidth`, default 1
 *     i.e. no padding — matches the historical per-site output exactly);
 *   - `arrow`   → `${num.padStart(6)}→${line}` (or the bare number when it is
 *     already ≥6 digits), the legacy padded-arrow format.
 * The inverse (stripping EITHER prefix) is {@link stripLineNumberPrefix} in
 * file-cache-seed.ts, so a transcript-seeded re-read round-trips regardless of the
 * style the output was produced in. Mirrors the reference's addLineNumbers()
 * (utils/file.ts) compact/padded-arrow branches.
 */
export function formatLineNumber(
  lineNumber: number,
  line: string,
  style: LineNumberStyle,
  compactWidth = 1,
): string {
  if (style === 'arrow') {
    const s = String(lineNumber);
    return `${s.length >= ARROW_PREFIX_WIDTH ? s : s.padStart(ARROW_PREFIX_WIDTH)}→${line}`;
  }
  return `${String(lineNumber).padStart(compactWidth)}\t${line}`;
}

/**
 * Apply the per-read token budget to an assembled Read output, in ONE place so
 * the two over-budget behaviors never drift across the four read paths (full,
 * range, tail, notebook):
 *   - within budget → the output unchanged;
 *   - over budget + `errorOnBudget` → a hard {@link ToolResult} error carrying the
 *     actionable {@link fileReadTokenBudgetMessage} (the reference parity), so the model
 *     gets a targeted-re-read nudge instead of a file silently cut mid-structure;
 *   - over budget + default → the content truncated to the budget via
 *     {@link sliceToReadTokenBudget} with the caller's `truncationNote` appended
 *     (AMI's partial-content differentiator).
 * The caller must RETURN the result when `isError` is set (the error short-circuits
 * any later appends like the cyber-risk reminder).
 */
export function applyReadTokenBudget(
  output: string,
  estimatedTokens: number,
  maxTokens: number,
  ext: string,
  errorOnBudget: boolean,
  truncationNote: string,
): ToolResult {
  if (estimatedTokens <= maxTokens) return { output };
  if (errorOnBudget) {
    return { output: `Error: ${fileReadTokenBudgetMessage(estimatedTokens, maxTokens)}`, isError: true };
  }
  return { output: sliceToReadTokenBudget(output, maxTokens, ext) + truncationNote };
}

/**
 * Options that shape the Read tool's self-description/schema so it HONESTLY
 * reflects the runtime's real capabilities and the operator's prompt-shaping
 * knobs. Defaults mirror a fully-capable runtime with all guidance on, so the
 * advertised description equals the historical static string. Mirrors the reference
 * Code's prompt.ts (isPDFSupported) + limits.ts (includeMaxSizeInPrompt /
 * targetedRangeNudge) description gating.
 */
export interface FileReadDescriptionOptions extends FileReadPromptFlags {
  /** Whether the runtime can extract PDF text (see {@link isPDFSupported}). */
  pdfSupported: boolean;
}

/** Resolve the description options from the live runtime + environment. */
export function resolveFileReadDescriptionOptions(
  env: NodeJS.ProcessEnv = process.env,
): FileReadDescriptionOptions {
  return { pdfSupported: isPDFSupported(), ...getFileReadPromptFlags(env) };
}

/**
 * Build the Read tool's description. The PDF clause is included ONLY when the
 * runtime can actually read PDFs, so the tool never advertises a capability that
 * would fail with "Error reading PDF". Mirrors the reference's isPDFSupported()
 * gating of the PDF instruction block.
 */
export function buildFileReadDescription(opts: FileReadDescriptionOptions): string {
  const pdfClause = opts.pdfSupported
    ? 'Can read PDF files (.pdf) — for large PDFs (more than 10 pages) you MUST provide the pages parameter to read a specific page range (e.g. pages: "1-5"); maximum 20 pages per request. '
    : '';
  return 'Read a file from the filesystem. Returns file content with line numbers (cat -n format). '
    + 'For image files (.png, .jpg, .jpeg, .gif, .webp) returns base64-encoded content presented visually. '
    + pdfClause
    + 'Can read Jupyter notebooks (.ipynb) and returns all cells with their outputs, combining code, text, and visualizations. '
    + 'This tool reads files only, not directories — to list a directory, use an ls command via the bash tool.';
}

/**
 * Build the Read tool's inputSchema. The offset/limit targeted-range-read nudge
 * (gated by {@link FileReadPromptFlags.targetedRangeNudge}) and the concrete
 * 256KB size-cap figure in `allow_large` (gated by
 * {@link FileReadPromptFlags.includeMaxSizeInPrompt}) are surfaced only when the
 * corresponding knob is on. Mirrors the reference's limits.ts prompt toggles.
 */
export function buildFileReadInputSchema(opts: FileReadDescriptionOptions): ToolDefinition['inputSchema'] {
  const offsetNudge = opts.targetedRangeNudge
    ? ' Prefer reading the whole file by omitting offset/limit; use offset/limit only when you already know which part of the file you need (important for large files).'
    : '';
  const limitNudge = opts.targetedRangeNudge
    ? ' When you know the exact region you need, pass offset+limit to read just that part instead of the whole file — this matters most for large files.'
    : '';
  const sizeCap = opts.includeMaxSizeInPrompt ? ' 256KB' : '';
  return {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to read.',
      },
      offset: {
        type: 'number',
        description: `Line number to start reading from (0-based, non-negative integer). Defaults to 0.${offsetNudge}`,
      },
      limit: {
        type: 'number',
        description:
          `Maximum number of lines to read (positive integer). Defaults to 2000.${limitNudge}`,
      },
      tail: {
        type: 'number',
        description: 'Read the LAST N lines of the file instead of from the start (positive integer) — use for logs or appended files when only the end matters. Mutually exclusive with offset/limit. Only the final portion of a very large file is scanned, so tailing a huge log does not load the whole file.',
      },
      pages: {
        type: 'string',
        description: 'Page range for PDF files (e.g. "1-5", "3", "10-20", or open-ended "3-" for page 3 onward). Only applicable to PDF files. Max 20 pages per request.',
      },
      allow_large: {
        type: 'boolean',
        description: `Read a text file past the usual${sizeCap} size limit in one call. Still subject to the token budget.`,
      },
      cell_id: {
        type: 'string',
        description: 'For Jupyter notebooks (.ipynb) only: read just the single cell whose id matches (either the cell\'s own id or its synthetic "cell-N" index id). Overrides offset/limit cell windowing and includes that cell\'s full outputs.',
      },
    },
    required: ['file_path'],
  };
}

export const SUITE_LOG_MAX_BYTES = 8_000_000;

/**
 * For an oversize suite log, keep only the first 6 MB and the last 1 MB via the
 * shared byte-bounded readers ({@link readFileRange} / {@link tailFile}) so the
 * whole (possibly 32 MB) file is never loaded into memory. Both readers decode
 * exactly the bytes they read (no trailing-NUL padding from an over-allocated
 * buffer) and loop across short reads.
 */
export async function readSuiteHeadAndTail(resolved: string): Promise<string> {
  const head = await readFileRange(resolved, 0, 6_000_000);
  const tail = await tailFile(resolved, 1_000_000);
  return `${head?.content ?? ''}\n${tail.content}`;
}

export function parsePageRange(pages: string): { start: number; end: number; error?: string; openEnded?: boolean } {
  const trimmed = pages.trim();
  // eslint-disable-next-line security/detect-unsafe-regex
  if (/,/.test(trimmed)) {
    return { start: 0, end: 0, error: `Page lists like "${pages}" are not supported. Read each page or range separately (e.g. pages: "6", then pages: "9", then pages: "15").` };
  }
  // Open-ended "N-": read from page N onward. We cannot know the document's last
  // page until it is parsed, and every request is bounded by the 20-page window,
  // so interpret "N-" as a 20-page window starting at N (page N through N+19).
  // Mirrors the reference's open-ended parsePDFPageRange (firstPage N, lastPage ∞),
  // clamped to AMI's per-request page cap.
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored, single \d group + literal '-'
  const openMatch = trimmed.match(/^(\d+)-$/);
  if (openMatch) {
    const openStart = parseInt(openMatch[1], 10);
    if (openStart < 1) {
      return { start: 0, end: 0, error: `Invalid page range: start must be >= 1 and end >= start.` };
    }
    return { start: openStart, end: openStart + 19, openEnded: true };
  }
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored, disjoint \d and literal '-'
  const match = trimmed.match(/^(\d+)(?:-(\d+))?$/);
  if (!match) {
    return { start: 0, end: 0, error: `Invalid page range "${pages}". Use "3" or "1-5".` };
  }
  const start = parseInt(match[1], 10);
  const end = match[2] ? parseInt(match[2], 10) : start;
  if (start < 1 || end < start) {
    return { start: 0, end: 0, error: `Invalid page range: start must be >= 1 and end >= start.` };
  }
  if (end - start + 1 > 20) {
    return { start: 0, end: 0, error: `Page range too large (max 20 pages per request).` };
  }
  return { start, end };
}

/**
 * Validate and coerce a model-supplied `offset`/`limit` line parameter. Accepts a
 * real number or a numeric string (several providers serialize numbers as strings)
 * and rejects non-numeric, fractional, or out-of-range values with an actionable
 * error instead of silently casting them into arithmetic (which produced empty or
 * garbage windows). Returns `{}` (use the default) when the param is omitted.
 * Mirrors the reference's semanticNumber(z.number().int()...) schema coercion.
 */
export function coerceReadLineParam(
  value: unknown,
  opts: { name: string; min: number },
): { value?: number; error?: string } {
  if (value === undefined || value === null) return {};
  let n: number;
  if (typeof value === 'number') {
    n = value;
  } else if (typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value))) {
    n = Number(value);
  } else {
    return { error: `Error: ${opts.name} must be a number, got ${JSON.stringify(value)}.` };
  }
  if (!Number.isInteger(n)) {
    return { error: `Error: ${opts.name} must be an integer, got ${n}.` };
  }
  if (n < opts.min) {
    return { error: `Error: ${opts.name} must be >= ${opts.min}, got ${n}.` };
  }
  return { value: n };
}

/**
 * Machine-readable taxonomy of why a PDF read failed. Mirrors the reference's
 * `PDFError.reason` discriminated union (utils/pdf.ts) so a failure can be
 * classified by cause (empty 0-byte file, over-size, password/encryption,
 * corrupted bytes, renderer unavailable) rather than collapsed into one opaque
 * string. The reason drives the actionable message in {@link classifyPdfError}.
 */
export const PDF_ERROR_REASONS = ['empty', 'too_large', 'password_protected', 'corrupted', 'unavailable', 'unknown'] as const;

export type PdfErrorReason = (typeof PDF_ERROR_REASONS)[number];

/**
 * Classify a PDF-parse failure message into a {@link PdfErrorReason}. The reason
 * is the single source of truth the user-facing message in {@link classifyPdfError}
 * switches on, so the two never drift. Mirrors the reference's pdf.ts stderr-regex
 * branches: `/password/i` → password_protected, `/damaged|corrupt/i` → corrupted,
 * plus empty/too-large/unavailable detection; anything else → unknown.
 */
export function classifyPdfErrorReason(message: string): PdfErrorReason {
  if (/password|encrypt(ed|ion)?|\bdecrypt/i.test(message)) return 'password_protected';
  // A damaged/corrupt PDF gets its own actionable classification so the model/
  // user knows the bytes themselves are bad and a re-download/regeneration is
  // needed. Matches `damaged`/`corrupt` (incl. corrupted/corruption); bare
  // "invalid" is intentionally excluded so a benign parser message like
  // "Invalid xref table" stays the generic unknown error. Mirrors the reference's
  // pdf.ts corrupted branch.
  if (/damaged|corrupt(ed|ion)?/i.test(message)) return 'corrupted';
  // A 0-byte / empty document, surfaced distinctly so the model knows the file
  // itself carries no content (not a parser fault). Mirrors the reference's `empty` reason.
  if (/\bempty\b|0 bytes|zero[- ]?length/i.test(message)) return 'empty';
  if (/too large|exceeds the|maximum.*size|size limit/i.test(message)) return 'too_large';
  // The optional renderer/parser dependency is missing. Mirrors the reference's `unavailable`.
  if (/not installed|unavailable|cannot find module|poppler|pdftoppm/i.test(message)) return 'unavailable';
  return 'unknown';
}

/**
 * Classify a PDF-parse failure into an actionable, model-facing error string via
 * the typed {@link classifyPdfErrorReason} taxonomy. Each reason yields a distinct
 * message (e.g. password-protected → "provide an unprotected version") instead of
 * being buried in a generic "Error reading PDF" string. Mirrors the reference's
 * per-reason PDF error messages (utils/pdf.ts).
 */
export function classifyPdfError(message: string): string {
  switch (classifyPdfErrorReason(message)) {
    case 'password_protected':
      return 'Error: PDF is password-protected or encrypted. Please provide an unprotected version.';
    case 'corrupted':
      return 'Error: PDF file is corrupted or invalid. Please provide a valid PDF.';
    case 'empty':
      return 'Error: PDF file is empty (0 bytes).';
    case 'too_large':
      return `Error: ${message}`;
    case 'unavailable':
      return `Error: PDF support is unavailable: ${message}`;
    default:
      return `Error reading PDF: ${message}`;
  }
}
