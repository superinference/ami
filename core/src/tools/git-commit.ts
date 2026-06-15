import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { execCommand } from '../utils/shell';
import { scanForSecrets } from './tool-utils';

const CO_AUTHOR_TRAILER = 'Co-Authored-By: AMI <ami@superinference.org>';
const TRAILER_REGEX = /Co-Authored-By:\s*AMI\s*<ami@superinference\.org>/i;

function buildCommitEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  env.GIT_EDITOR = 'false';
  env.GIT_PAGER = 'cat';
  env.GIT_TERMINAL_PROMPT = '0';
  env.AI_AGENT = 'superinference';
  return env;
}

function buildFinalMessage(message: string): string {
  const trimmed = message.trim();
  if (TRAILER_REGEX.test(trimmed)) {
    return trimmed;
  }
  return trimmed + '\n\n' + CO_AUTHOR_TRAILER;
}

export const gitCommitTool: ToolDefinition = {
  name: 'git_commit',
  description:
    'Create a git commit with automatic Co-Authored-By attribution for AMI. ' +
    'Use this instead of running git commit via bash. ' +
    'Optionally stage files before committing. The Co-Authored-By trailer is always appended automatically.',
  inputSchema: {
    type: 'object',
    properties: {
      message: {
        type: 'string',
        description: 'The commit message. The Co-Authored-By trailer is appended automatically — do not include it.',
      },
      files: {
        type: 'array',
        items: { type: 'string' },
        description:
          'Files to stage before committing. If omitted, commits whatever is already staged. ' +
          'Pass ["."] to stage all tracked modified files.',
      },
    },
    required: ['message'],
  },
  isReadOnly: false,

  async execute(
    input: Record<string, unknown>,
    context: ToolContext,
  ): Promise<ToolResult> {
    const message = input.message as string;
    const files = input.files as string[] | undefined;
    const env = buildCommitEnv();
    const execOpts = { cwd: context.cwd, timeout: 30_000, abortSignal: context.abortSignal, env };

    if (!message || message.trim().length === 0) {
      return { output: 'Error: commit message must not be empty.', isError: true };
    }

    // Guard against flag injection via files array
    if (files) {
      const flagEntry = files.find(f => typeof f === 'string' && f.startsWith('-') && f !== '.' && f !== '..');
      if (flagEntry) {
        return {
          output: `Error: files entries must be file paths, not flags ("${flagEntry}").`,
          isError: true,
        };
      }
    }

    // Stage files if specified
    if (files && files.length > 0) {
      let addCommand: string;
      if (files.length === 1 && files[0] === '.') {
        addCommand = 'git add -u';
      } else {
        const escaped = files.map(f => `"${f.replace(/"/g, '\\"')}"`).join(' ');
        addCommand = `git add -- ${escaped}`;
      }

      const addResult = await execCommand(addCommand, execOpts);
      if (addResult.exitCode !== 0) {
        const err = (addResult.stderr || addResult.stdout).trim();
        return { output: `Error staging files: ${err}`, isError: true };
      }
    }

    // Check that there are staged changes
    const diffResult = await execCommand('git diff --cached --quiet', execOpts);
    if (diffResult.exitCode === 0) {
      return {
        output: 'Error: nothing staged for commit. Stage files with the "files" parameter, or use bash to run git add first.',
        isError: true,
      };
    }

    const secrets = scanForSecrets(message);
    if (secrets.length > 0) {
      return { output: `Warning: Potential secrets detected in commit message: ${secrets.join(', ')}. Remove secrets before committing.`, isError: true };
    }

    // Build the final commit message with trailer
    const finalMessage = buildFinalMessage(message);

    // Write message to temp file to avoid shell escaping issues
    const tmpDir = os.tmpdir();
    const tmpFile = path.join(tmpDir, `ami-commit-${Date.now()}-${process.pid}.txt`);

    try {
      fs.writeFileSync(tmpFile, finalMessage, 'utf-8');

      const commitResult = await execCommand(`git commit -F "${tmpFile}"`, execOpts);
      if (commitResult.exitCode !== 0) {
        const err = (commitResult.stderr || commitResult.stdout).trim();
        return { output: `Error: git commit failed:\n${err}`, isError: true };
      }

      // Get the commit summary
      const logResult = await execCommand('git log --oneline -1', execOpts);
      const commitLine = logResult.stdout.trim();

      const parts: string[] = [];
      if (commitResult.stdout.trim()) {
        parts.push(commitResult.stdout.trim());
      }
      if (commitLine) {
        parts.push(commitLine);
      }

      return { output: parts.join('\n'), isError: false };
    } finally {
      try { fs.unlinkSync(tmpFile); } catch { /* already cleaned or never written */ }
    }
  },
};

export { buildFinalMessage, CO_AUTHOR_TRAILER, TRAILER_REGEX };
