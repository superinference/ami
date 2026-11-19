/**
 * Permission-decision content blocks.
 *
 * Parity: Claude Code's permission allow/deny/abort decisions carry an optional
 * `contentBlocks: ContentBlockParam[]` (hooks/toolPermission/PermissionContext.ts
 * `buildAllow`, `cancelAndAbort`, `handleUserAllow`) so a hook or the prompt can
 * attach RICH content — a structured text block, an image, a document — that flows
 * back to the model alongside the permission result, instead of the model only ever
 * receiving a plain-text denial/approval line.
 *
 * AMI's permission result shape ({@link PermissionPromptResult.contentBlocks}) carries
 * the same optional blocks, using AMI's own content-block union
 * ({@link TextContent} | {@link ImageContent} | {@link DocumentContent}) rather than
 * the provider-specific `ContentBlockParam`. This module is the single canonical place
 * that SPLITS those heterogeneous blocks into the three delivery channels the engine
 * already has:
 *   - text  → folded into the (text) permission feedback the model receives,
 *   - images → queued as real multimodal blocks via the engine's pending-tool-image
 *              path (so an attached screenshot reaches a vision model),
 *   - documents → rendered as a short text note (AMI's tool-result pipeline has no
 *              document channel; the note names each attachment so the model knows it
 *              was provided out of band).
 *
 * Keeping the split in ONE helper means the deny path and the approve-with-content
 * path reuse the same reduction (no per-site re-implementation — AGENTS.md
 * No-Duplication / IMPLEMENTATION §0b.6).
 */

import type { TextContent, ImageContent, DocumentContent } from './types';

/** A single permission-decision content block (AMI's content-block union). */
export type PermissionContentBlock = TextContent | ImageContent | DocumentContent;

/** The three delivery channels a set of permission content blocks reduces to. */
export interface SplitPermissionContent {
  /** Concatenated text of every `text` block (blank when there are none), joined
   *  by a single newline in source order. */
  text: string;
  /** Every `image` block, in source order, for the engine's multimodal queue. */
  images: ImageContent[];
  /** Every `document` block, in source order (delivered as a text note). */
  documents: DocumentContent[];
}

/**
 * A short human/model-readable note naming the document attachments carried by a
 * permission decision — AMI's tool-result pipeline has no native document channel,
 * so a `document` block is surfaced to the model as this text line rather than
 * silently dropped. Returns `''` when there are no documents.
 */
export function describePermissionDocuments(documents: DocumentContent[]): string {
  if (documents.length === 0) return '';
  const types = documents.map(d => d.source.media_type).join(', ');
  const n = documents.length;
  return `[${n} document${n === 1 ? '' : 's'} attached to this permission decision (${types}).]`;
}

/**
 * Split a permission decision's content blocks into the text / image / document
 * delivery channels. A nullish or empty input yields empty channels so callers can
 * branch on `images.length` / `text` without a null guard. Unknown block shapes are
 * ignored (fail-safe — a malformed block from a hook cannot corrupt the result).
 */
export function splitPermissionContentBlocks(
  blocks: readonly PermissionContentBlock[] | undefined | null,
): SplitPermissionContent {
  const texts: string[] = [];
  const images: ImageContent[] = [];
  const documents: DocumentContent[] = [];
  for (const block of blocks ?? []) {
    if (!block || typeof block !== 'object') continue;
    if (block.type === 'text' && typeof block.text === 'string') {
      texts.push(block.text);
    } else if (block.type === 'image') {
      images.push(block);
    } else if (block.type === 'document') {
      documents.push(block);
    }
  }
  return { text: texts.join('\n'), images, documents };
}

/**
 * Build the combined text a permission decision contributes to the model-facing
 * result: the decision's own `base` message, then any `text` content blocks, then a
 * note naming any `document` attachments. Image blocks are NOT included here — they
 * are delivered separately via the multimodal queue (see {@link SplitPermissionContent.images}).
 * Empty segments are dropped so the result never has blank lines.
 */
export function composePermissionResultText(
  base: string,
  split: SplitPermissionContent,
): string {
  const docNote = describePermissionDocuments(split.documents);
  return [base, split.text, docNote].filter(seg => seg && seg.length > 0).join('\n');
}
