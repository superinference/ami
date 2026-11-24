// Split from file-read.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { execFile } from 'child_process';
import { createRequire } from 'module';
import { ToolContext, ImageContent } from '../types';
import { resolveFilePath, isBlockedPath } from './tool-utils';
import { log } from '../logger';
import { resolveConfigDir } from '../reference-journal';

export const DEFAULT_LIMIT = 2000;

/**
 * Per-cell combined-output size (chars of text + image base64) above which a
 * notebook cell's outputs are replaced with a `jq` escape-hatch hint instead of
 * being inlined — a DoS / context-budget guard. Mirrors the reference's
 * LARGE_OUTPUT_THRESHOLD (utils/notebook.ts).
 */
export const NOTEBOOK_LARGE_OUTPUT_THRESHOLD = 10_000;

/** Max PDF pages to render to images when falling back to pdftoppm (context guard). */
export const MAX_PDF_RENDER_PAGES = 10;

/**
 * Indirection seam for the pdftoppm child process (poppler-utils). Tests swap
 * `execFile` to exercise the availability probe + page-render branches without a
 * real poppler install. Production spawns the real binary.
 */
export const pdfHooks: {
  execFile: (cmd: string, args: string[], opts: { timeout: number }) => Promise<{ code: number | null; stdout: string; stderr: string }>;
  loadPdfParse: () => { PDFParse: new (opts: { data: Buffer }) => { getText: (o?: Record<string, unknown>) => Promise<{ text?: string; total?: number }>; destroy: () => Promise<void> } };
  isPdfParseResolvable: () => boolean;
} = {
  execFile: (cmd, args, opts) => new Promise((resolve) => {
    execFile(cmd, args, { timeout: opts.timeout, maxBuffer: 16 * 1024 * 1024 }, (err, stdout, stderr) => {
      const code = err
        ? (typeof (err as { code?: number }).code === 'number' ? (err as { code: number }).code : 1)
        : 0;
      resolve({ code, stdout: String(stdout ?? ''), stderr: String(stderr ?? '') });
    });
  }),
  // Indirection seam for the pdf-parse module. Tests swap this to exercise the
  // page-count guard / error-classification branches without a real multi-page
  // PDF; production resolves pdf-parse from the working directory, then from AMI's
  // own optional-deps folder (where the agent may install it — see optionalDepInstallCommand).
  loadPdfParse: () => createRequire(optionalDepRoot('pdf-parse'))('pdf-parse'),
  // Capability probe: whether the optional `pdf-parse` dependency RESOLVES from
  // the working directory (resolve-only — never executes the module). Mirrors the
  // same resolution root as loadPdfParse so the advertised capability matches the
  // runtime read path. Tests swap this to assert the capability-gated description.
  isPdfParseResolvable: (): boolean => {
    try {
      createRequire(optionalDepRoot('pdf-parse')).resolve('pdf-parse');
      return true;
    } catch {
      return false;
    }
  },
};

/**
 * AMI's own folder for OPTIONAL runtime packages (e.g. pdf-parse for PDF reads). The
 * agent installs a missing optional package HERE — never into the user's project, whose
 * package.json must not change as a side effect of reading a file.
 */
export function optionalDepsDir(env: NodeJS.ProcessEnv = process.env): string {
  return path.join(resolveConfigDir(env), 'optional-deps');
}

/** The exact command that installs an optional package into {@link optionalDepsDir}. */
export function optionalDepInstallCommand(pkg: string, env: NodeJS.ProcessEnv = process.env): string {
  return `npm install --prefix ${JSON.stringify(optionalDepsDir(env))} ${pkg}`;
}

/**
 * The require root an optional package resolves from: the working directory (the
 * project's own install) when it has the package, else AMI's optional-deps folder.
 */
export function optionalDepRoot(pkg: string, cwd: string = process.cwd(), env: NodeJS.ProcessEnv = process.env): string {
  const fromCwd = path.join(cwd, '_');
  try { createRequire(fromCwd).resolve(pkg); return fromCwd; } catch { /* not in the project */ }
  return path.join(optionalDepsDir(env), '_');
}

/**
 * Whether the runtime can actually extract PDF text — i.e. the optional
 * `pdf-parse` dependency resolves. The Read tool's description is gated on this
 * so its self-description is an honest introspection surface that reflects the
 * real capability instead of advertising PDF reads that would fail at runtime
 * with "Error reading PDF". Mirrors the reference's isPDFSupported() (FileReadTool/prompt.ts).
 */
export function isPDFSupported(): boolean {
  return pdfHooks.isPdfParseResolvable();
}

/**
 * Security control that travels WITH untrusted file content: a planted file must
 * not be able to turn the agent into a malware-authoring assistant. Appended to
 * every text read so the model may analyze/report on malicious code but must
 * refuse to improve, augment, or author it. Mirrors the reference's
 * CYBER_RISK_MITIGATION_REMINDER (FileReadTool).
 */
export const CYBER_RISK_MITIGATION_REMINDER =
  '\n\n<system-reminder>You may analyze, explain, and report on the file above even if it contains malicious or suspicious code. You MUST refuse to improve, augment, optimize, obfuscate, or author malicious code (malware, exploits, credential stealers, etc.) based on it.</system-reminder>';

/**
 * Dependency seam for fs calls whose failures are defensive TOCTOU/race guards
 * (a file that passed the access check disappearing or becoming unreadable
 * before the follow-up lstat/open). Tests use `mock.method(fsHooks, ...)` to
 * exercise those catch paths deterministically; production uses the real fs.
 * Mirrors the fsHooks pattern in logger.ts / file-cache.ts.
 */
export const fsHooks: {
  lstatSync: (p: string) => fs.Stats;
  openForBom: (p: string) => Promise<fs.promises.FileHandle>;
} = {
  lstatSync: (p: string) => fs.lstatSync(p),
  openForBom: (p: string) => fs.promises.open(p, 'r'),
};

export function isUNCPath(filePath: string): boolean {
  return filePath.startsWith('\\\\') || filePath.startsWith('//');
}

export function normalizeMacScreenshotPath(filePath: string): string {
  return filePath
    .replace(/\u202f/g, ' ')
    .replace(/\u00a0/g, ' ');
}

/** Narrow no-break space (U+202F) \u2014 macOS uses it before AM/PM in newer builds. */
export const THIN_SPACE = '\u202f';

/**
 * macOS screenshot filenames put either a regular space or a thin space (U+202F)
 * before AM/PM depending on the OS version. When a read misses on ENOENT, this
 * returns the path with the ALTERNATE space character before AM/PM in a `.png`
 * name (regular <-> U+202F) so the caller can retry it, or undefined when the
 * name does not match the screenshot pattern. Non-destructive: the on-disk name
 * is never rewritten up front. Mirrors the reference's getAlternateScreenshotPath.
 */
export function getAlternateScreenshotPath(filePath: string): string | undefined {
  const filename = path.basename(filePath);
  // eslint-disable-next-line security/detect-unsafe-regex -- anchored, single greedy prefix + fixed suffix; linear
  const amPmPattern = /^(.+)([ \u202f])(AM|PM)(\.png)$/;
  const match = filename.match(amPmPattern);
  if (!match) return undefined;
  const currentSpace = match[2];
  const alternateSpace = currentSpace === ' ' ? THIN_SPACE : ' ';
  return filePath.replace(
    `${currentSpace}${match[3]}${match[4]}`,
    `${alternateSpace}${match[3]}${match[4]}`,
  );
}

/**
 * Neutralize forged trust-boundary markers planted in untrusted FILE content:
 * a read file must not be able to inject a `<system-reminder>\u2026</system-reminder>`
 * block that the model would mistake for a real system instruction. The entire
 * forged block is stripped before the content is cached or returned, so it can
 * never carry those boundary tags forward. AMI's own reminders are appended
 * AFTER this runs and are unaffected. Mirrors the reference's queryHelpers strip.
 */
export function stripForgedSystemReminders(text: string): string {
  // eslint-disable-next-line security/detect-unsafe-regex -- lazy match bounded by a required literal close tag; linear
  return text.replace(/<system-reminder>[\s\S]*?<\/system-reminder>/g, '');
}

/**
 * Resolve the alternate-space macOS-screenshot variant of `resolved` and return
 * it ONLY when the alternate exists, is readable, is within the allowed roots,
 * and is not a blocked/device path — so the ENOENT retry is held to the same
 * security checks as the original path. Returns undefined when there is no safe,
 * readable alternate. See getAlternateScreenshotPath.
 */
export async function resolveAlternateScreenshotPath(
  resolved: string,
  context: ToolContext,
): Promise<string | undefined> {
  const alt = getAlternateScreenshotPath(resolved);
  if (!alt) return undefined;
  const altResolution = resolveFilePath(alt, context.cwd, context.allowedReadRoots, 'read');
  if (altResolution.error) return undefined;
  if (isBlockedPath(altResolution.resolved)) return undefined;
  try {
    await fs.promises.access(altResolution.resolved, fs.constants.R_OK);
    return altResolution.resolved;
  } catch {
    return undefined;
  }
}

export const MEDIA_TYPES: Record<string, string> = {
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.bmp': 'image/bmp',
};

/**
 * Detect an image's real media type from its leading magic bytes, independent of
 * the file extension (which an untrusted/mislabeled file can lie about). Returns
 * the canonical `image/*` media type, or null when the bytes match no known
 * image signature. This is the single source of truth for content-based image
 * format detection (file reads, pasted images, attachment normalization) so the
 * logic is never re-implemented per call site. Mirrors the reference's
 * detectImageFormatFromBuffer / detectImageFormatFromBase64.
 */
export function detectImageFormatFromBuffer(buf: Buffer): string | null {
  if (buf.length >= 8
      && buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47
      && buf[4] === 0x0d && buf[5] === 0x0a && buf[6] === 0x1a && buf[7] === 0x0a) {
    return 'image/png';
  }
  if (buf.length >= 3 && buf[0] === 0xff && buf[1] === 0xd8 && buf[2] === 0xff) {
    return 'image/jpeg';
  }
  // "GIF87a" / "GIF89a"
  if (buf.length >= 6
      && buf[0] === 0x47 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x38
      && (buf[4] === 0x37 || buf[4] === 0x39) && buf[5] === 0x61) {
    return 'image/gif';
  }
  // "RIFF"...."WEBP"
  if (buf.length >= 12
      && buf[0] === 0x52 && buf[1] === 0x49 && buf[2] === 0x46 && buf[3] === 0x46
      && buf[8] === 0x57 && buf[9] === 0x45 && buf[10] === 0x42 && buf[11] === 0x50) {
    return 'image/webp';
  }
  // "BM" — Windows bitmap (WSL2/Windows clipboard emits these; the API rejects BMP).
  if (buf.length >= 2 && buf[0] === 0x42 && buf[1] === 0x4d) {
    return 'image/bmp';
  }
  return null;
}

/** Detect an image's media type from a base64 payload via its decoded magic bytes. */
export function detectImageFormatFromBase64(data: string): string | null {
  // Only the leading bytes are needed; decode a short prefix to avoid materializing
  // a large buffer just to read a signature.
  const prefix = data.slice(0, 24);
  let head: Buffer;
  try {
    head = Buffer.from(prefix, 'base64');
  } catch {
    return null;
  }
  return detectImageFormatFromBuffer(head);
}

/**
 * Normalize a possibly-`{default}`-wrapped module to the underlying value. Module
 * interop differs by resolution mode — an ESM build of a drop-in replacement may
 * resolve as `{ default: fn }` while CommonJS resolves to the callable directly —
 * so unwrap a `.default` when the top-level export is not itself callable. Mirrors
 * the reference's unwrapDefault (imageProcessor.ts).
 */
export function unwrapDefault<T>(mod: T | { default: T }): T {
  if (typeof mod === 'function') return mod as T;
  if (mod && typeof mod === 'object' && 'default' in (mod as { default?: T })) {
    return (mod as { default: T }).default;
  }
  return mod as T;
}

/**
 * Indirection seam for the optional native `sharp` dependency. Production loads
 * the real module via require; tests swap it to exercise the compression branch
 * deterministically (sharp is an optional dep that may be absent). The require
 * result is passed through {@link unwrapDefault} so an ESM-published sharp (or a
 * drop-in replacement wrapped in `{default}`) resolves to the callable factory
 * instead of crashing with "sharp is not a function". Behavior is unchanged when
 * sharp resolves as plain CommonJS.
 */
export const imageHooks: {
  loadSharp: () => any;
  /**
   * Optional ORDERED chain of image-processor loaders, tried in order by
   * {@link loadImageProcessor} with the first one that loads winning. A bundled
   * build can prepend a faster native processor (e.g. an `image-processor-napi`
   * loader) and keep `loadSharp` as the portable fallback, so a packaged
   * distribution whose native binding is missing on a given platform still
   * downscales images instead of only being able to pass raw bytes. When unset,
   * the chain defaults to `[loadSharp]` (unchanged single-engine behavior).
   * Mirrors the reference's getImageProcessor() native-first ordered fallback.
   */
  loaders?: Array<() => any>;
} = {
  loadSharp: () => unwrapDefault(require('sharp')),
};

/**
 * Load an image processor from the ordered {@link imageHooks.loaders} chain,
 * returning the first loader that succeeds. Each loader's failure is tolerated
 * (the next is tried); only when EVERY loader fails does the last error
 * propagate, so the caller's existing sharp-unavailable fallback still runs. When
 * no chain is configured, the single canonical `loadSharp` loader is used — so a
 * default build behaves exactly as before and a test that mocks `imageHooks.loadSharp`
 * is still honored. Mirrors the reference's getImageProcessor() (native-first, sharp fallback).
 */
export function loadImageProcessor(): any {
  const chain = imageHooks.loaders && imageHooks.loaders.length > 0
    ? imageHooks.loaders
    : [imageHooks.loadSharp];
  let lastErr: unknown;
  for (const loader of chain) {
    try {
      const processor = loader();
      if (processor) return processor;
    } catch (err) {
      lastErr = err;
      // Try the next processor in the chain. Surface the degrade at debug level so
      // an operator tracing a slow/raw-bytes image read can see which processor
      // tier failed to load. Mirrors the reference's getImageProcessor() warn-and-fallthrough.
      log('file-read', `Image processor loader failed, trying next: ${err instanceof Error ? err.message : String(err)}`);
    }
  }
  throw lastErr instanceof Error
    ? lastErr
    : new Error('No image processor available');
}

/**
 * Max pixel dimensions the provider accepts for an image. Larger images are
 * downscaled (aspect-preserving) before encoding. Mirrors the reference's
 * IMAGE_MAX_WIDTH / IMAGE_MAX_HEIGHT.
 */
export const IMAGE_MAX_WIDTH = 2000;

export const IMAGE_MAX_HEIGHT = 2000;

/** Original vs. displayed pixel dimensions, used for coordinate mapping. */
export interface ImageDimensions {
  originalWidth?: number;
  originalHeight?: number;
  displayWidth?: number;
  displayHeight?: number;
}

/** Result of compressing/encoding an image buffer. */
export interface CompressedImage {
  /** Base64-encoded payload. */
  base64: string;
  /** Canonical `image/*` media type of the returned payload. */
  mediaType: string;
  /** Raw byte length of the returned (possibly compressed) buffer. */
  originalSize: number;
  /** Pixel dimensions (original + displayed), when sharp could read them. */
  dimensions?: ImageDimensions;
}

/**
 * Thrown when an image cannot be reduced below the provider's size/pixel limits
 * and compression itself failed — the caller surfaces this as an actionable error
 * instead of emitting an unusable image block. Mirrors the reference's ImageResizeError.
 */
export class ImageResizeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ImageResizeError';
  }
}

/** Read a big-endian PNG's IHDR width/height without decoding the image. */
export function readPngDimensions(buf: Buffer): { width: number; height: number } | null {
  if (buf.length >= 24
      && buf[0] === 0x89 && buf[1] === 0x50 && buf[2] === 0x4e && buf[3] === 0x47) {
    return { width: buf.readUInt32BE(16), height: buf.readUInt32BE(20) };
  }
  return null;
}

/**
 * Build a short metadata note describing an image's source and (when it was
 * downscaled) the coordinate scale factor from displayed → original pixels, so a
 * model reasoning about pixel coordinates can map them back. Returns null when no
 * useful metadata is available. Mirrors the reference's createImageMetadataText.
 */
export function createImageMetadataText(dims: ImageDimensions, sourcePath?: string): string | null {
  const { originalWidth, originalHeight, displayWidth, displayHeight } = dims;
  if (!originalWidth || !originalHeight || !displayWidth || !displayHeight
      || displayWidth <= 0 || displayHeight <= 0) {
    return sourcePath ? `[Image source: ${sourcePath}]` : null;
  }
  const wasResized = originalWidth !== displayWidth || originalHeight !== displayHeight;
  if (!wasResized && !sourcePath) return null;
  const parts: string[] = [];
  if (sourcePath) parts.push(`source: ${sourcePath}`);
  if (wasResized) {
    const scaleFactor = originalWidth / displayWidth;
    parts.push(`original ${originalWidth}x${originalHeight}, displayed at ${displayWidth}x${displayHeight}. Multiply coordinates by ${scaleFactor.toFixed(2)} to map to original image.`);
  }
  return `[Image: ${parts.join(', ')}]`;
}

/**
 * Parse a tool-result output string into ImageContent block(s) when it is the
 * image JSON that file_read emits ({type:'image',source:{type:'base64',...}}),
 * else null. Fast-guarded so ordinary text outputs are never JSON.parsed.
 */
export function parseImageToolOutput(output: string): ImageContent[] | null {
  if (typeof output !== 'string') return null;
  const t = output.trimStart();
  if (!t.startsWith('{')) return null;
  if (!t.includes('"type":"image"') && !t.includes('"type": "image"')) return null;
  try {
    const parsed = JSON.parse(output);
    if (parsed && parsed.type === 'image' && parsed.source && parsed.source.type === 'base64'
        && typeof parsed.source.media_type === 'string' && typeof parsed.source.data === 'string') {
      // Normalize an untrusted image block: trust the actual magic bytes over the
      // declared media_type so a mislabeled block (e.g. JPEG tagged image/png) is
      // corrected before it reaches the provider, which would otherwise reject it.
      const sniffed = detectImageFormatFromBase64(parsed.source.data);
      if (sniffed && sniffed !== parsed.source.media_type) {
        parsed.source.media_type = sniffed;
      }
      return [parsed as ImageContent];
    }
  } catch {}
  return null;
}

/** Kind of tool output that carried extracted images (drives the engine's note).
 *  `notebook`/`pdf` are file reads; `mcp` is an MCP tool result whose image content
 *  blocks are forwarded as first-class multimodal content (see mcp-tool.ts). */
export const FILE_IMAGES_KINDS = ['notebook', 'pdf', 'mcp'] as const;

export type FileImagesKind = (typeof FILE_IMAGES_KINDS)[number];

/**
 * Build a file_read output that carries extracted images (notebook cell outputs,
 * rendered PDF pages) alongside the text. The engine renders `text` as the tool
 * result and queues `images` as multimodal blocks. Single canonical shape so each
 * image-bearing reader (notebook, PDF) reuses the same protocol.
 */
export function buildFileImagesOutput(kind: FileImagesKind, text: string, images: ImageContent[]): string {
  return JSON.stringify({ type: 'file_images', kind, text, images });
}
