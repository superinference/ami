import * as fs from 'fs';
import * as path from 'path';
import type { Message } from './types';
import { FileCache, getFileCache, isFsInaccessible } from './file-cache';
import { FILE_UNCHANGED_STUB } from './tools/file-read';
import { decodeBuffer } from './tools/tool-utils';

/** A file-read/edit/write recovered from the transcript, resolved against disk. */
export interface SeededFileState {
  /** Absolute path (relative paths expanded against cwd). */
  path: string;
  /** Content the model saw (reads) or the post-edit disk bytes (edits/writes). */
  content: string;
  /** Current on-disk mtime, so change detection + the read dedup accelerator work. */
  mtime: number;
  /** Text encoding `content` was decoded from (edits/writes); `utf-8` for reads. */
  encoding: string;
}

/**
 * Strip a leading `cat -n`-style line-number prefix (optional indent, digits, then a
 * TAB or `→` separator) from a single line of file-read tool output, returning the
 * bare source line. Mirrors the reference's `stripLineNumberPrefix` so transcript-seeded
 * content matches the raw file bytes (needed for the cache's content-hash freshness check).
 */
export function stripLineNumberPrefix(line: string): string {
  const match = line.match(/^\s*\d+[→\t](.*)$/);
  return match ? match[1] : line;
}

const SYSTEM_REMINDER_RE = /<system-reminder>[\s\S]*?<\/system-reminder>/g;

/** Expand a tool-use `file_path` to an absolute path, like the reference's `expandPath`. */
function toAbsolute(filePath: string, cwd: string): string {
  return path.isAbsolute(filePath) ? filePath : path.resolve(cwd, filePath);
}

function parseArgs(raw: string): Record<string, unknown> | null {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
  } catch {
    // Malformed tool-call arguments — ignore this call.
  }
  return null;
}

/**
 * Rebuild read-file-state from a conversation transcript on resume / cold-restart.
 *
 * Walks the messages chronologically and, for every completed `file_read` (full reads
 * only — ranged offset/limit reads and `FILE_UNCHANGED_STUB` dedup results are skipped)
 * and every successful `file_edit` (edit OR write mode), recovers what the model last
 * saw of that file and pairs it with the file's CURRENT on-disk mtime:
 *
 * - **reads** reconstruct what the model saw (system-reminder blocks removed, line-number
 *   prefixes stripped, trimmed) and compare it, whitespace-tolerantly, against the file on
 *   disk: an UNCHANGED file is cached with its FAITHFUL disk bytes (so change detection is a
 *   true no-op and the unchanged-read dedup accelerator works on resume), while a file edited
 *   BETWEEN sessions is cached with the reconstructed bytes under the current mtime so
 *   hasChanged() reports a content change and the model must re-read before editing.
 * - **edits/writes** re-read the file from disk now, since the post-operation disk bytes
 *   ARE the state the model left it in (the edit input carries only old/new fragments).
 *
 * Files that no longer exist (ENOENT) or are momentarily inaccessible are skipped rather
 * than failing the whole restore; any other unexpected FS error is surfaced. Entries are
 * returned in chronological order so a later operation on the same path wins (last-wins),
 * matching the reference's `extractReadFilesFromMessages`.
 */
export function collectReadFileStatesFromTranscript(
  messages: readonly Message[],
  cwd: string,
): SeededFileState[] {
  // tool_call_id → absolute path, by tool kind. Populated as assistant tool_calls
  // are encountered; consumed when the matching tool result arrives later.
  const readIds = new Map<string, string>();
  const editIds = new Map<string, string>();
  const out: SeededFileState[] = [];

  for (const message of messages) {
    if (message.role === 'assistant' && message.tool_calls) {
      for (const call of message.tool_calls) {
        const name = call.function?.name;
        if (name !== 'file_read' && name !== 'file_edit') continue;
        const args = parseArgs(call.function.arguments);
        if (!args) continue;
        const filePath = typeof args.file_path === 'string'
          ? args.file_path
          : typeof args.path === 'string' ? args.path : undefined;
        if (!filePath) continue;
        if (name === 'file_read') {
          // Ranged reads never seed the cache — the model saw only a window.
          if (args.offset !== undefined || args.limit !== undefined) continue;
          readIds.set(call.id, toAbsolute(filePath, cwd));
        } else {
          editIds.set(call.id, toAbsolute(filePath, cwd));
        }
      }
      continue;
    }

    if (message.role !== 'tool') continue;
    const id = message.tool_call_id;

    const readPath = readIds.get(id);
    if (readPath !== undefined && typeof message.content === 'string'
      // Dedup stubs carry no file content — the earlier real read already captured it.
      && !message.content.startsWith(FILE_UNCHANGED_STUB)) {
      const reconstructed = message.content
        .replace(SYSTEM_REMINDER_RE, '')
        .split('\n')
        .map(stripLineNumberPrefix)
        .join('\n')
        .trim();
      const disk = readDisk(readPath);
      if (disk !== null) {
        // The transcript view can never round-trip to EXACT disk bytes — the `cat -n`
        // formatter does not encode whether the file ends in a trailing newline, and
        // system-reminder stripping leaves whitespace — so a hash of the reconstructed
        // text would MISMATCH an unchanged file's real bytes and spuriously report
        // "modified since read" on every resume (defeating both the unchanged-read
        // dedup accelerator and the read-before-edit baseline this seeding exists for).
        // Decide with a whitespace-tolerant compare instead: if the model's view still
        // matches disk, seed the FAITHFUL disk bytes (+ real encoding) so change
        // detection is a no-op for an untouched file; if it diverged (an edit made
        // BETWEEN sessions), seed the reconstructed bytes under the current mtime so
        // hasChanged() reports a content change and the model must re-read before
        // editing — never clobbering content it never saw. Mirrors the reference's
        // readFileState resume semantics (unchanged ⇒ dedup, changed ⇒ forced re-read).
        out.push(disk.content.trim() === reconstructed
          ? { path: readPath, content: disk.content, mtime: disk.mtime, encoding: disk.encoding }
          : { path: readPath, content: reconstructed, mtime: disk.mtime, encoding: 'utf-8' });
      }
    }

    const editPath = editIds.get(id);
    if (editPath !== undefined && message.isError !== true) {
      const disk = readDisk(editPath);
      if (disk !== null) out.push({ path: editPath, ...disk });
    }
  }

  return out;
}

/** Decoded disk content + mtime + encoding, or null when the file is gone/inaccessible. */
function readDisk(filePath: string): Omit<SeededFileState, 'path'> | null {
  try {
    const mtime = fs.statSync(filePath).mtimeMs;
    const { text, encoding } = decodeBuffer(fs.readFileSync(filePath));
    return { content: text, mtime, encoding };
  } catch (err) {
    if (isENOENTorInaccessible(err)) return null;
    throw err;
  }
}

function isENOENTorInaccessible(err: unknown): boolean {
  return (
    (!!err && typeof err === 'object' && (err as NodeJS.ErrnoException).code === 'ENOENT') ||
    isFsInaccessible(err)
  );
}

/**
 * Seed read-file-state recovered from a transcript INTO a file cache, so a `--resume` /
 * cold-restart keeps mtime-based read-before-edit state and the unchanged-read dedup
 * accelerator consistent across the restart. Seeds the directory-shared cache from
 * {@link getFileCache} by default; pass an explicit `target` to seed a bounded,
 * standalone cache instead — e.g. `new FileCache(MAX_CACHE_BYTES, 10)` for a short
 * "ask" operation that touches few files (the entry-count knob is the reference's
 * `ASK_READ_FILE_STATE_CACHE_SIZE` equivalent). Returns the number of entries seeded.
 * Wraps {@link collectReadFileStatesFromTranscript}; this is the resume consumer.
 */
export function seedFileCacheFromTranscript(
  messages: readonly Message[],
  cwd: string,
  target?: FileCache,
): number {
  const cache = target ?? getFileCache(cwd);
  const states = collectReadFileStatesFromTranscript(messages, cwd);
  for (const s of states) cache.set(s.path, s.content, s.mtime, s.encoding);
  return states.length;
}
