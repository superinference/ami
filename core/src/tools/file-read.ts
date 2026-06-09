import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { getFileCache } from '../file-cache';

const DEFAULT_LIMIT = 2000;
const BINARY_CHECK_BYTES = 8192;

/** Image extensions that should be returned as base64 content. */
const IMAGE_EXTENSIONS = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp']);

const MEDIA_TYPES: Record<string, string> = {
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
};

export const fileReadTool: ToolDefinition = {
  name: 'file_read',
  description:
    'Read a file from the filesystem. Returns file content with line numbers. For image files (.png, .jpg, .jpeg, .gif, .webp) returns base64-encoded content.',
  inputSchema: {
    type: 'object',
    properties: {
      file_path: {
        type: 'string',
        description: 'The absolute or relative path to the file to read.',
      },
      offset: {
        type: 'number',
        description: 'Line number to start reading from (0-based). Defaults to 0.',
      },
      limit: {
        type: 'number',
        description:
          'Maximum number of lines to read. Defaults to 2000.',
      },
    },
    required: ['file_path'],
  },
  isReadOnly: true,
  isConcurrencySafe: true,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const filePath = input.file_path as string;
    const offset = (input.offset as number) ?? 0;
    const limit = (input.limit as number) ?? DEFAULT_LIMIT;

    if (!filePath || filePath.trim().length === 0) {
      return { output: 'Error: file_path must not be empty.', isError: true };
    }

    const resolved = path.isAbsolute(filePath)
      ? filePath
      : path.resolve(context.cwd, filePath);

    if (!path.resolve(resolved).startsWith(path.resolve(context.cwd) + path.sep) &&
        path.resolve(resolved) !== path.resolve(context.cwd)) {
      return { output: `Error: path "${filePath}" is outside the workspace directory.`, isError: true };
    }

    // Check file exists
    try {
      await fs.promises.access(resolved, fs.constants.R_OK);
    } catch {
      return {
        output: `Error: File not found or not readable: ${resolved}`,
        isError: true,
      };
    }

    // Check if file is a directory
    const stat = await fs.promises.stat(resolved);
    if (stat.isDirectory()) {
      return {
        output: `Error: ${resolved} is a directory, not a file.`,
        isError: true,
      };
    }

    context.filesRead?.add(resolved);

    // Handle image files — return base64
    const ext = path.extname(resolved).toLowerCase();
    if (IMAGE_EXTENSIONS.has(ext)) {
      const imageBuffer = await fs.promises.readFile(resolved);
      const base64 = imageBuffer.toString('base64');
      const mediaType = MEDIA_TYPES[ext] || 'application/octet-stream';
      return {
        output: JSON.stringify({
          type: 'image',
          source: {
            type: 'base64',
            media_type: mediaType,
            data: base64,
          },
        }),
      };
    }

    // Handle PDF files — extract text
    if (ext === '.pdf') {
      try {
        const pdfParse = require('pdf-parse');
        const pdfBuffer = await fs.promises.readFile(resolved);
        const pdf = await pdfParse(pdfBuffer);
        const text = (pdf.text || '').trim();
        return {
          output: `File: ${resolved} (PDF, ${pdf.numpages} pages)\n\n${text.slice(0, 100000)}`,
          isError: false,
        };
      } catch (err) {
        return { output: `Error reading PDF: ${err instanceof Error ? err.message : String(err)}`, isError: true };
      }
    }

    // Check for binary content
    const isBinary = await checkBinary(resolved);
    if (isBinary) {
      return { output: '[Binary file]' };
    }

    // Check file cache — return short summary when unchanged
    const fileCache = getFileCache(context.cwd);
    const cached = fileCache.get(resolved);
    if (cached) {
      const lineCount = cached.content.split('\n').length;
      return { output: `File unchanged since last read: ${resolved} (${lineCount} lines)` };
    }

    // Read file content
    const content = await fs.promises.readFile(resolved, 'utf-8');

    if (content.length === 0) {
      return { output: '(empty file)' };
    }

    // Cache the file for future reads
    fileCache.set(resolved, content, stat.mtimeMs);

    const allLines = content.split('\n');
    const totalLines = allLines.length;

    // Apply offset and limit
    const startLine = Math.max(0, Math.min(offset, totalLines));
    const endLine = Math.min(startLine + limit, totalLines);
    const selectedLines = allLines.slice(startLine, endLine);

    // Calculate the width needed for line numbers
    const maxLineNumber = startLine + selectedLines.length;
    const lineNumberWidth = Math.max(
      String(maxLineNumber).length,
      1,
    );

    // Format with line numbers (1-based display)
    const numbered = selectedLines
      .map((line, i) => {
        const lineNum = String(startLine + i + 1).padStart(lineNumberWidth);
        return `${lineNum}\t${line}`;
      })
      .join('\n');

    const header = `File: ${resolved} (${totalLines} lines)`;
    const rangeInfo =
      startLine > 0 || endLine < totalLines
        ? `Showing lines ${startLine + 1}-${endLine} of ${totalLines}`
        : '';

    const parts = [header];
    if (rangeInfo) {
      parts.push(rangeInfo);
    }
    parts.push('');
    parts.push(numbered);

    return { output: parts.join('\n') };
  },
};

async function checkBinary(filePath: string): Promise<boolean> {
  const fd = await fs.promises.open(filePath, 'r');
  try {
    const buffer = Buffer.alloc(BINARY_CHECK_BYTES);
    const { bytesRead } = await fd.read(buffer, 0, BINARY_CHECK_BYTES, 0);
    for (let i = 0; i < bytesRead; i++) {
      if (buffer[i] === 0) {
        return true;
      }
    }
    return false;
  } finally {
    await fd.close();
  }
}
