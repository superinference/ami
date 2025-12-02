import { spawn, ChildProcess } from 'child_process';
import { ToolDefinition, ToolContext, ToolResult } from '../types';

const MAX_OUTPUT_LENGTH = 30000;
const DEFAULT_TIMEOUT_MS = 120000;

export const bashTool: ToolDefinition = {
  name: 'bash',
  description:
    'Execute a bash command. Use for running scripts, installing packages, compiling, git operations, and any shell task. IMPORTANT: Do not use bash for reading files (use file_read), editing files (use file_edit), or searching (use grep/glob). Use absolute paths. Quote paths with spaces. Git: new commits only (never amend unless asked), never --no-verify, never force-push, never commit unless explicitly asked. Do not sleep between commands. Chain dependent commands with &&.',
  inputSchema: {
    type: 'object',
    properties: {
      command: {
        type: 'string',
        description: 'The bash command to execute.',
      },
      timeout: {
        type: 'number',
        description:
          'Optional timeout in milliseconds. Defaults to 120000 (2 minutes).',
      },
      description: {
        type: 'string',
        description:
          'A short human-readable description of what this command does.',
      },
    },
    required: ['command'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const command = input.command as string;
    const timeout = (input.timeout as number) ?? DEFAULT_TIMEOUT_MS;

    if (!command || command.trim().length === 0) {
      return { output: 'Error: command must not be empty.', isError: true };
    }

    return new Promise<ToolResult>((resolve) => {
      let stdout = '';
      let stderr = '';
      let killed = false;
      let child: ChildProcess;

      try {
        child = spawn('bash', ['-c', command], {
          cwd: context.cwd,
          stdio: ['ignore', 'pipe', 'pipe'],
          env: { ...process.env },
          detached: true,
        });
      } catch (err: unknown) {
        const message =
          err instanceof Error ? err.message : String(err);
        resolve({
          output: `Error spawning process: ${message}`,
          isError: true,
        });
        return;
      }

      const killProcessGroup = () => {
        try {
          if (child.pid) process.kill(-child.pid, 'SIGKILL');
        } catch {
          child.kill('SIGKILL');
        }
      };

      const timeoutId = setTimeout(() => {
        killed = true;
        killProcessGroup();
      }, timeout);

      const onAbort = () => {
        killed = true;
        killProcessGroup();
      };

      if (context.abortSignal) {
        if (context.abortSignal.aborted) {
          killProcessGroup();
          clearTimeout(timeoutId);
          resolve({ output: 'Command aborted.', isError: true });
          return;
        }
        context.abortSignal.addEventListener('abort', onAbort, { once: true });
      }

      child.stdout!.on('data', (data: Buffer) => {
        const chunk = data.toString();
        if (stdout.length < MAX_OUTPUT_LENGTH * 2) stdout += chunk;
        if (context.onProgress) {
          context.onProgress(chunk);
        }
      });

      child.stderr!.on('data', (data: Buffer) => {
        const chunk = data.toString();
        if (stderr.length < MAX_OUTPUT_LENGTH * 2) stderr += chunk;
        if (context.onProgress) {
          context.onProgress(chunk);
        }
      });

      child.on('error', (err: Error) => {
        clearTimeout(timeoutId);
        if (context.abortSignal) {
          context.abortSignal.removeEventListener('abort', onAbort);
        }
        resolve({
          output: `Error executing command: ${err.message}`,
          isError: true,
        });
      });

      child.on('close', (code: number | null) => {
        clearTimeout(timeoutId);
        if (context.abortSignal) {
          context.abortSignal.removeEventListener('abort', onAbort);
        }

        if (killed) {
          const reason = context.abortSignal?.aborted
            ? 'Command aborted.'
            : `Command timed out after ${timeout}ms.`;
          resolve({
            output: formatOutput(stdout, stderr, null, reason),
            isError: true,
          });
          return;
        }

        resolve({
          output: formatOutput(stdout, stderr, code),
          isError: code !== 0,
        });
      });
    });
  },
};

function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) {
    return text;
  }
  const half = Math.floor((maxLength - 100) / 2);
  return (
    text.slice(0, half) +
    `\n\n... [truncated ${text.length - maxLength + 100} characters] ...\n\n` +
    text.slice(text.length - half)
  );
}

function formatOutput(
  stdout: string,
  stderr: string,
  exitCode: number | null,
  extra?: string,
): string {
  const parts: string[] = [];

  if (stdout.length > 0) {
    parts.push(truncate(stdout, MAX_OUTPUT_LENGTH));
  }

  if (stderr.length > 0) {
    const label = stdout.length > 0 ? '\n[stderr]\n' : '';
    const remaining = Math.max(
      1000,
      MAX_OUTPUT_LENGTH - (parts.join('').length),
    );
    parts.push(label + truncate(stderr, remaining));
  }

  if (extra) {
    parts.push(extra);
  }

  if (exitCode !== null && exitCode !== undefined) {
    parts.push(`Exit code: ${exitCode}`);
  }

  return parts.join('\n').trim() || '(no output)';
}
