import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { detectCommandChaining } from '../permissions';
import { execCommand } from '../utils/shell';

const MAX_OUTPUT_LENGTH = 30000;
const DEFAULT_TIMEOUT_MS = 120000;

const SENSITIVE_ENV_PATTERNS = [
  /_KEY$/i, /_SECRET$/i, /_TOKEN$/i, /_PASSWORD$/i, /_CREDENTIALS$/i,
  /^AWS_/i, /^AZURE_/i, /^GCP_/i,
  /^GOOGLE_APPLICATION_CREDENTIALS$/i,
  /^DATABASE_URL$/i, /^REDIS_URL$/i,
  /^SSH_AUTH_SOCK$/i, /^GIT_ASKPASS$/i,
  /^NPM_TOKEN$/i, /^DOCKER_/i, /^JWT_/i,
];

function scrubEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  for (const key of Object.keys(env)) {
    if (SENSITIVE_ENV_PATTERNS.some(p => p.test(key))) {
      delete env[key];
    }
  }
  env.GIT_EDITOR = 'false';
  env.GIT_PAGER = 'cat';
  env.EDITOR = 'false';
  env.VISUAL = 'false';
  env.PAGER = 'cat';
  env.DEBIAN_FRONTEND = 'noninteractive';
  env.GIT_TERMINAL_PROMPT = '0';
  env.AI_AGENT = 'superinference';
  return env;
}

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

    const result = await execCommand(command, {
      cwd: context.cwd,
      timeout,
      abortSignal: context.abortSignal,
      env: scrubEnv(),
      onData: context.onProgress ? (chunk) => context.onProgress!(chunk) : undefined,
    });

    if (result.exitCode === null && !result.stdout && result.stderr === 'Aborted') {
      return { output: 'Command aborted.', isError: true };
    }

    if (result.exitCode === null) {
      const reason = context.abortSignal?.aborted
        ? 'Command aborted.'
        : `Command timed out after ${timeout}ms.`;
      return {
        output: formatOutput(result.stdout, result.stderr, null, reason),
        isError: true,
      };
    }

    let output = formatOutput(result.stdout, result.stderr, result.exitCode);
    const chaining = detectCommandChaining(command);
    if (chaining.chained && chaining.count > 2) {
      output += '\n\n[Note: This command chains ' + chaining.count +
        ' commands via ' + chaining.operators.join(', ') +
        '. Consider using separate tool calls for better error handling and readability.]';
    }

    return {
      output,
      isError: result.exitCode !== 0,
    };
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
