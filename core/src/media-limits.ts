/**
 * API media size limits — the single source of the PDF/image/request ceilings
 * used by both the file-read tool (to refuse an oversized full read up front) and
 * the error classifier (to render actionable "too large" guidance when the API
 * rejects media). Mirrors the reference's `constants/apiLimits.ts`.
 *
 * Kept dependency-free so any layer can import a limit without pulling in the
 * heavy tool module graph.
 */

/**
 * Max pages in a PDF accepted for a FULL (un-ranged) read / request. A document
 * larger than this would dump an unbounded amount of text into context, so the
 * model must extract a specific range instead. Mirrors the reference's
 * `API_PDF_MAX_PAGES` (constants/apiLimits.ts).
 */
export const API_PDF_MAX_PAGES = 100;

/**
 * Target raw byte ceiling for a PDF / request payload before the API rejects it
 * as too large. Mirrors the reference's `PDF_TARGET_RAW_SIZE` (20 MB).
 */
export const PDF_TARGET_RAW_SIZE = 20 * 1024 * 1024; // 20 MB
