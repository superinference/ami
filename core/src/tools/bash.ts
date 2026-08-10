import * as fs from 'fs';
import * as path from 'path';
import * as child_process from 'child_process';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { detectCommandChaining } from '../permissions';
import { execCommand } from '../utils/shell';
import { validateBashSecurity } from './bash-security';
import { shouldUseSandbox, wrapWithSandbox } from './bash-sandbox';

const MAX_OUTPUT_LENGTH = 30000;
const DEFAULT_TIMEOUT_MS = 120000;

function isReadOnlyBashCommand(command: string): boolean {
  try {
    const { PermissionManager } = require('../permissions');
    const pm = new PermissionManager();
    return pm.classifyBashCommand(command) === 'safe';
  } catch {
    return false;
  }
}

function looksLikeHungPrompt(output: string): boolean {
  const lastLine = output.trim().split('\n').pop() ?? '';
  return /\(y\/n\)|\[y\/n\]|\(yes\/no\)|password:|passphrase:|Press Enter|Continue\?|Overwrite\?|Are you sure/i.test(lastLine);
}

function detectGitCommit(command: string): boolean {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  return /\bgit\s+commit\b/.test(stripped);
}

export function detectSelfKill(command: string): string | null {
  const stripped = command.replace(/"[^"]*"|'[^']*'/g, '');
  const pid = process.pid;
  const ppid = process.ppid;
  if (new RegExp(`\\bkill\\s+(-\\w+\\s+)*${pid}\\b`).test(stripped))
    return 'This would kill AMI itself (PID match).';
  if (new RegExp(`\\bkill\\s+(-\\w+\\s+)*${ppid}\\b`).test(stripped))
    return 'This would kill AMI\'s parent process.';
  return null;
}

function detectBlockedSleep(command: string, runInBackground: boolean, hasCustomTimeout: boolean): string | null {
  if (runInBackground || hasCustomTimeout) return null;
  const match = command.match(/^\s*sleep\s+(\d+)/);
  if (match && parseInt(match[1]) > 10) {
    return `Blocking sleep for ${match[1]}s detected. Use run_in_background: true for long waits, or schedule_wakeup for timed delays.`;
  }
  return null;
}

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
  env.PIP_PROGRESS_BAR = 'off';
  env.TQDM_DISABLE = '1';
  env.PYTHONDONTWRITEBYTECODE = '1';
  env.MANPAGER = 'cat';
  return env;
}

export const bashTool: ToolDefinition = {
  name: 'bash',
  description:
    'Execute a bash command. Use for running scripts, installing packages, compiling, git operations, and any shell task. IMPORTANT: Do not use bash for reading files (use file_read), editing files (use file_edit), or searching (use grep/glob). Use absolute paths. Quote paths with spaces. Git: new commits only (never amend unless asked), never --no-verify, never force-push, never commit unless explicitly asked. Do not sleep between commands. Chain dependent commands with &&. In assistant/detached mode, commands exceeding 15s are auto-backgrounded — use run_in_background for intentionally long tasks.',
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
      run_in_background: {
        type: 'boolean',
        description:
          'Run this command in the background. Returns immediately with a task ID. ' +
          'Use task_output to check results later, task_kill to stop it, or task_list to see all background tasks.',
      },
      description: {
        type: 'string',
        description:
          'A short human-readable description of what this command does.',
      },
      dangerouslyDisableSandbox: {
        type: 'boolean',
        description:
          'Override sandbox mode and run commands without resource restrictions. Use only when the sandbox interferes with legitimate operations.',
      },
    },
    required: ['command'],
  },
  isReadOnly: false,
  isConcurrencySafe: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const command = input.command as string;
    const MAX_TIMEOUT_MS = 3_600_000; // 1 hour
    const timeout = Math.min((input.timeout as number) || DEFAULT_TIMEOUT_MS, MAX_TIMEOUT_MS);

    if (!command || command.trim().length === 0) {
      return { output: 'Error: command must not be empty.', isError: true };
    }

    if (detectGitCommit(command)) {
      return {
        output:
          'Error: Use the git_commit tool instead of running git commit via bash. ' +
          'The git_commit tool ensures proper Co-Authored-By attribution is always included.\n\n' +
          'Example: git_commit({ message: "your commit message", files: ["file1.ts", "file2.ts"] })',
        isError: true,
      };
    }

    const selfKill = detectSelfKill(command);
    if (selfKill) {
      return {
        output: `Error: ${selfKill} Use kill <pid> with the specific child process PID instead.`,
        isError: true,
      };
    }

    const secCheck = validateBashSecurity(command);
    if (!secCheck.safe) {
      return {
        output: `Error: ${secCheck.message}`,
        isError: true,
      };
    }

    const sleepBlock = detectBlockedSleep(command, !!input.run_in_background, !!input.timeout);
    if (sleepBlock) return { output: `Error: ${sleepBlock}`, isError: true };

    const readOnly = isReadOnlyBashCommand(command);

    if (input.run_in_background && context.processManager) {
      const desc = (input.description as string) || command.slice(0, 80);
      const taskId = context.processManager.spawn(command, {
        cwd: context.cwd,
        description: desc,
        env: scrubEnv(),
      });
      const task = context.processManager.get(taskId);
      return {
        output: `[background: true]\nBackground task started.\n  Task ID: ${taskId}\n  PID: ${task?.pid ?? 'unknown'}\n  Command: ${command}\n\nUse task_output({ task_id: "${taskId}" }) to check status and output.\nUse task_kill({ task_id: "${taskId}" }) to stop it.\nUse task_list() to see all background tasks.`,
        isError: false,
      };
    }

    const disableSandbox = input.dangerouslyDisableSandbox === true;
    const useSandbox = !disableSandbox && shouldUseSandbox(command);
    const effectiveCommand = useSandbox ? wrapWithSandbox(command, context.cwd) : command;

    const result = await execCommand(effectiveCommand, {
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
      if (context.abortSignal?.aborted) {
        return {
          output: formatOutput(result.stdout, result.stderr, null, 'Command aborted.'),
          isError: true,
        };
      }
      const combinedOutput = result.stdout + result.stderr;
      if (looksLikeHungPrompt(combinedOutput)) {
        const lastLine = combinedOutput.trim().split('\n').pop();
        return {
          output: `Command appears to be waiting for interactive input. Last line: "${lastLine}"\nSuggestion: Use 'echo y | ${command}' or 'yes | ${command}' to auto-respond, or run with run_in_background: true.`,
          isError: true,
        };
      }
      if (context.processManager) {
        const taskId = context.processManager.spawn(command, {
          cwd: context.cwd,
          description: `[auto-bg] ${command.slice(0, 60)}`,
          env: scrubEnv(),
        });
        return {
          output: `Command timed out after ${timeout}ms. Auto-moved to background as task ${taskId}. Use task_output to check progress.`,
        };
      }
      return {
        output: formatOutput(result.stdout, result.stderr, null, `Command timed out after ${timeout}ms.`),
        isError: true,
      };
    }

    // CWD escape check
    let stdoutExtra = '';
    if (result.stdout.includes('cd ') || command.includes('cd ')) {
      try {
        const checkCwd = child_process.execSync('pwd', { cwd: context.cwd, encoding: 'utf-8', timeout: 5000 }).trim();
        if (!checkCwd.startsWith(context.cwd)) {
          stdoutExtra = '\n[Warning: Command attempted to change directory outside project. CWD reset.]';
        }
      } catch {}
    }

    // Persist large output to disk instead of truncating
    let stdout = result.stdout + stdoutExtra;
    if (stdout.length > MAX_OUTPUT_LENGTH) {
      const spillDir = path.join(context.cwd, '.superinference', 'tool-results');
      fs.mkdirSync(spillDir, { recursive: true });
      const spillFile = path.join(spillDir, `bash-${Date.now()}.txt`);
      fs.writeFileSync(spillFile, stdout, 'utf-8');
      const headLen = Math.floor(MAX_OUTPUT_LENGTH * 0.67);
      const tailLen = MAX_OUTPUT_LENGTH - headLen - 200;
      stdout = stdout.slice(0, headLen) + `\n\n[... ${stdout.length - headLen - tailLen} chars persisted to ${spillFile} — use file_read to view ...]\n\n` + stdout.slice(-tailLen);
    }

    const noOutputExpected = /^\s*(mkdir|touch|mv|cp|rm|chmod|chown)\b/.test(command);
    if (noOutputExpected && !stdout.trim() && result.exitCode === 0) {
      stdout = '[Command completed successfully (no output expected)]';
    }

    let output = formatOutput(stdout, result.stderr, result.exitCode === 0 ? 0 : null);
    const chaining = detectCommandChaining(command);
    if (chaining.chained && chaining.count > 2) {
      output += '\n\n[Note: This command chains ' + chaining.count +
        ' commands via ' + chaining.operators.join(', ') +
        '. Consider using separate tool calls for better error handling and readability.]';
    }

    let exitCodeInfo: ExitCodeInfo | null = null;
    if (result.exitCode !== 0) {
      exitCodeInfo = interpretExitCode(command, result.exitCode);
      if (exitCodeInfo) output += `\n${exitCodeInfo.message}`;
      if (!exitCodeInfo?.isNonError) {
        const hint = diagnoseError(output);
        if (hint) output += `\n\n[Hint: ${hint}]`;
      }
    }

    const isRealError = result.exitCode !== 0 && !exitCodeInfo?.isNonError;
    const exitInfo = result.exitCode !== 0 ? `\n[exit code: ${result.exitCode}${exitCodeInfo ? ' ' + exitCodeInfo.message : ''}]` : '';
    output = `${output}${exitInfo}`.trim();

    return {
      output,
      isError: isRealError,
      metadata: { readOnly },
    };
  },
};

const ERROR_HINTS: Array<{ pattern: RegExp; hint: string }> = [
  { pattern: /Transform failed|SyntaxError:.*Unexpected/i, hint: 'Your code has a syntax error (missing bracket, semicolon, or mismatched quotes). Use file_read to check the file you last edited.' },
  { pattern: /Cannot find module|ERR_MODULE_NOT_FOUND/i, hint: 'Module not found. Check the import path with list_dir or glob, and try running via npm test instead of raw node commands.' },
  { pattern: /ENOENT.*no such file/i, hint: 'File or directory does not exist. Use list_dir to check what files are available.' },
  { pattern: /TypeError:.*is not a function/i, hint: 'A function call is wrong. Use file_read to check the export names and signatures in the imported module.' },
  { pattern: /ReferenceError:.*is not defined/i, hint: 'A variable or import is missing. Check your imports and variable declarations with file_read.' },
];

function diagnoseError(output: string): string | null {
  for (const { pattern, hint } of ERROR_HINTS) {
    if (pattern.test(output)) return hint;
  }
  return null;
}

interface ExitCodeInfo {
  message: string;
  isNonError?: boolean;
}

function interpretExitCode(command: string, exitCode: number): ExitCodeInfo | null {
  const parts = command.trim().split(/[|;&]\s*/);
  const lastCmd = parts[parts.length - 1].trim().split(/\s+/)[0];
  const base = lastCmd || command.trim().split(/\s+/)[0];
  if (exitCode === 1 && (base === 'grep' || base === 'rg')) return { message: '(no matches found — not an error)', isNonError: true };
  if (exitCode === 1 && base === 'diff') return { message: '(files differ — not an error)', isNonError: true };
  if (exitCode === 1 && (base === 'test' || base === '[')) return { message: '(condition is false)', isNonError: true };
  if (exitCode === 1 && base === 'find') return { message: '(some paths were inaccessible)', isNonError: true };
  if (exitCode === 2 && base === 'grep') return { message: '(error in grep pattern or file access)' };
  if (exitCode === 126) return { message: '(permission denied — command not executable)' };
  if (exitCode === 127) return { message: '(command not found)' };
  if (exitCode === 128 + 9) return { message: '(killed by SIGKILL)' };
  if (exitCode === 128 + 15) return { message: '(killed by SIGTERM)' };
  return null;
}

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
