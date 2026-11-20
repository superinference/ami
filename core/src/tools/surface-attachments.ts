import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { isImagePath } from './file-read';
import { plural } from './tool-utils';

/**
 * Model-driven attachment surfacing (agent -> user). Mirrors the reference's
 * BriefTool / SendUserMessage attachments affordance (BriefTool/BriefTool.ts +
 * BriefTool/attachments.ts): the model attaches file paths — "photos,
 * screenshots, diffs, logs, or any file the user should see alongside your
 * message" — to an outgoing message, and the tool resolves each to
 * {path, size, isImage} and confirms delivery with
 * "Message delivered to user. (N attachment(s) included)".
 *
 * This is the REVERSE direction of common/src/attachments.ts, which turns
 * user-typed/dragged image paths into multimodal input blocks (user -> agent).
 * Like the reference (BriefTool/attachments.ts is deliberately separate from the
 * free-text imagePaste extractor), the model-provided-path resolver lives apart
 * from the user-input free-text resolver — model paths are clean (absolute or
 * cwd-relative, optional leading `~`), so they need no drag-and-drop un-escaping
 * / file:// / WSL normalization. The resolved records are emitted on
 * {@link SURFACE_ATTACHMENT_METADATA_KEY} so a frontend renders the attachments
 * to the user; the tool OUTPUT carries only the model-facing delivery
 * confirmation (parity with the reference's mapToolResultToToolResultBlockParam).
 */

/** Well-known {@link ToolResult.metadata} key carrying the resolved attachment
 *  records a frontend renders to the user (agent -> user surfacing). */
export const SURFACE_ATTACHMENT_METADATA_KEY = 'surfacedAttachments';

/** Message-intent label, mirroring the reference's BriefTool `status` enum. */
const INTENTS = ['normal', 'proactive'] as const;
type Intent = (typeof INTENTS)[number];

/** Upper bound on the number of attachments a single surfacing call may carry,
 *  so a runaway model call cannot stat an unbounded path list. */
const MAX_ATTACHMENTS = 50;

/** One resolved surfaced attachment (structurally matches common's
 *  `ResolvedAttachment`; emitted as plain JSON on the result metadata so no
 *  cross-layer type import is needed — core must not import common). */
export interface SurfacedAttachment {
  path: string;
  size: number;
  isImage: boolean;
}

/** Minimal `fs.statSync`-shaped seam so the resolver is testable (EACCES /
 *  unexpected-errno paths) without touching the real filesystem. */
export type StatFn = (p: string) => { isFile(): boolean; size: number };

/** Expand a model-provided attachment path: a leading `~` to the home dir, then
 *  resolve relative paths against the tool `cwd`. Mirrors the reference's `expandPath`
 *  used by BriefTool/attachments.ts (tilde + absolute/resolve), NOT the heavier
 *  free-text normalization the user-input side needs. */
export function expandAttachmentPath(raw: string, cwd: string): string {
  let s = raw.trim();
  if (s === '~' || s.startsWith('~/')) s = path.join(os.homedir(), s.slice(1));
  return path.isAbsolute(s) ? s : path.resolve(cwd, s);
}

/** Model-facing delivery confirmation, mirroring the reference's BriefTool
 *  `mapToolResultToToolResultBlockParam`:
 *  `Message delivered to user.` + ` (N attachment(s) included)` when N > 0. */
export function formatAttachmentDelivery(count: number): string {
  const suffix = count === 0 ? '' : ` (${count} ${plural(count, 'attachment')} included)`;
  return `Message delivered to user.${suffix}`;
}

/**
 * Validate + resolve a set of model-provided attachment paths into
 * {@link SurfacedAttachment} records. Returns the first actionable error
 * (not-a-regular-file / does-not-exist-with-cwd / permission-denied),
 * mirroring the reference's `validateAttachmentPaths` messages. `statFn` defaults to
 * `fs.statSync` (the operation — it yields the size) and is injectable for tests.
 */
export function resolveSurfacedAttachments(
  rawPaths: string[],
  cwd: string,
  statFn: StatFn = fs.statSync,
): { ok: true; resolved: SurfacedAttachment[] } | { ok: false; message: string } {
  const resolved: SurfacedAttachment[] = [];
  for (const rawPath of rawPaths) {
    if (rawPath.trim() === '') {
      return { ok: false, message: `Error: attachment "${rawPath}" is not a regular file.` };
    }
    const fullPath = expandAttachmentPath(rawPath, cwd);
    let stats: { isFile(): boolean; size: number };
    try {
      stats = statFn(fullPath);
    } catch (err) {
      const code = (err as NodeJS.ErrnoException | undefined)?.code;
      if (code === 'ENOENT') {
        return { ok: false, message: `Error: attachment "${rawPath}" does not exist. Current working directory: ${cwd}.` };
      }
      if (code === 'EACCES' || code === 'EPERM') {
        return { ok: false, message: `Error: attachment "${rawPath}" is not accessible (permission denied).` };
      }
      // Unexpected errno — surface it rather than silently dropping the attachment.
      return { ok: false, message: `Error: attachment "${rawPath}" could not be read (${code ?? 'unknown error'}).` };
    }
    if (!stats.isFile()) {
      return { ok: false, message: `Error: attachment "${rawPath}" is not a regular file.` };
    }
    resolved.push({ path: fullPath, size: stats.size, isImage: isImagePath(fullPath) });
  }
  return { ok: true, resolved };
}

export const surfaceAttachmentTool: ToolDefinition = {
  name: 'surface_attachment',
  description:
    'Surface one or more local files to the user alongside your message — your channel to show ' +
    'the user a file directly, not just describe it. Use for photos, screenshots, diffs, logs, or any ' +
    'file the user should see. Provide absolute or cwd-relative paths (a leading ~ expands to the home ' +
    "directory). Set status to 'proactive' when surfacing something unsolicited (a completion while the " +
    "user is away, a blocker) or 'normal' when replying to something they just said. The files are shown " +
    'to the user, not read into your context.',
  inputSchema: {
    type: 'object',
    properties: {
      message: {
        type: 'string',
        description: 'The message to show the user alongside the attachments. Supports markdown.',
      },
      attachments: {
        type: 'array',
        items: { type: 'string' },
        description:
          'File paths (absolute or relative to cwd; a leading ~ expands to home) to surface to the user. ' +
          'Use for photos, screenshots, diffs, logs, or any file the user should see alongside your message.',
      },
      status: {
        type: 'string',
        enum: [...INTENTS],
        description: "Intent label: 'normal' (reply) or 'proactive' (unsolicited surfacing). Default: normal.",
      },
    },
    required: ['message'],
  },
  isReadOnly: true,
  // A message send is non-deterministic / reaches outside the local closed world
  // (it surfaces to the user), so renderers/introspection can tag it open-world.
  isOpenWorld: true,
  isConcurrencySafe: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    // Strict input: reject unknown keys so a typo'd parameter is surfaced, not ignored.
    const allowed = new Set(['message', 'attachments', 'status']);
    const unknown = Object.keys(input).filter((k) => !allowed.has(k));
    if (unknown.length > 0) {
      return { output: `Error: unknown parameter(s): ${unknown.join(', ')}. Allowed: message, attachments, status.`, isError: true };
    }

    if (typeof input.message !== 'string' || input.message.trim() === '') {
      return { output: 'Error: "message" is required and must be a non-empty string.', isError: true };
    }

    const rawStatus = typeof input.status === 'string' ? input.status : undefined;
    if (rawStatus !== undefined && !(INTENTS as readonly string[]).includes(rawStatus)) {
      return { output: `Error: invalid status "${rawStatus}". Options: ${INTENTS.join(', ')}.`, isError: true };
    }

    const rawAttachments = input.attachments;
    if (rawAttachments !== undefined && !Array.isArray(rawAttachments)) {
      return { output: 'Error: "attachments" must be an array of file paths.', isError: true };
    }
    const rawList: unknown[] = Array.isArray(rawAttachments) ? rawAttachments : [];
    if (rawList.some((p) => typeof p !== 'string')) {
      return { output: 'Error: every entry in "attachments" must be a string path.', isError: true };
    }
    const paths = rawList as string[];
    if (paths.length > MAX_ATTACHMENTS) {
      return { output: `Error: too many attachments (${paths.length}); at most ${MAX_ATTACHMENTS} may be surfaced per message.`, isError: true };
    }

    const result = resolveSurfacedAttachments(paths, context.cwd);
    if (!result.ok) {
      return { output: result.message, isError: true };
    }

    return {
      output: formatAttachmentDelivery(result.resolved.length),
      metadata: {
        [SURFACE_ATTACHMENT_METADATA_KEY]: result.resolved,
        surfaceStatus: (rawStatus as Intent) ?? 'normal',
      },
    };
  },
};
