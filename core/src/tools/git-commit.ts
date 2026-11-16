import * as child_process from 'child_process';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { ToolDefinition, ToolContext, ToolResult } from '../types';
import { execCommand, resolveTmpBase } from '../utils/shell';
import { scanForSecrets } from './tool-utils';

const CO_AUTHOR_TRAILER = 'Co-Authored-By: AMI <ami@superinference.org>';
const TRAILER_REGEX = /Co-Authored-By:\s*AMI\s*<ami@superinference\.org>/i;

/**
 * Secret-bearing FILE guard. Mirrors the sensitive-name/ext/pattern list that
 * permissions.ts applies to reads so the sanctioned commit path can never stage
 * and push a credential file (`.env`, `credentials.json`, `id_rsa`, `*.pem`, …).
 * These constants are local because the permissions.ts equivalents are
 * function-scoped and not exported; keep the two lists in sync if either moves.
 */
const SECRET_FILE_NAMES = new Set([
  'id_rsa', 'id_ed25519', 'id_ecdsa', 'id_dsa', '.netrc',
  'credentials.json',
]);
const SECRET_FILE_EXTS = ['.pem', '.key', '.p12', '.pfx', '.keystore'];
/** Suffixes that make an `.env`-prefixed file a safe template, not a real secret. */
const SAFE_ENV_SUFFIX_REGEX = /\.(example|sample|template|dist|defaults?)$/i;
const SECRET_FILE_PATTERNS = [/^secrets\.ya?ml$/i];

/** True when a staged file's basename matches a known secret-file shape. */
function isSecretFile(basename: string): boolean {
  if (/^\.env($|\.)/i.test(basename) && !SAFE_ENV_SUFFIX_REGEX.test(basename)) return true;
  if (SECRET_FILE_NAMES.has(basename)) return true;
  if (SECRET_FILE_EXTS.some(ext => basename.toLowerCase().endsWith(ext))) return true;
  return SECRET_FILE_PATTERNS.some(p => p.test(basename));
}

/**
 * Strip foreign / disallowed attribution from a commit message before the AMI
 * trailer is applied. Removes any `Co-Authored-By:` trailer that is NOT AMI's
 * own and any AI-tool provenance footer (lines carrying the 🤖 marker), so an
 * external assistant's byline can never land in a commit — matching the repo's
 * attribution policy regardless of what the drafted message contained.
 */
function stripDisallowedAttribution(message: string): string {
  const kept = message.split('\n').filter(line => {
    if (/^\s*Co-Authored-By:/i.test(line) && !TRAILER_REGEX.test(line)) return false;
    if (line.includes('🤖')) return false;
    return true;
  });
  return kept.join('\n').replace(/\n{3,}/g, '\n\n').trimEnd();
}

/**
 * Dependency seam for tests (mirrors the fsHooks pattern in config.ts). The
 * temp-file write/cleanup routes through this object so tests can simulate an
 * unlink failure (e.g. the temp file already removed) via
 * `mock.method(_gitCommitFs, 'unlinkSync', ...)`. Defaults are the real fs calls,
 * so production behaviour is unchanged.
 */
export const _gitCommitFs = {
  writeFileSync: fs.writeFileSync as typeof fs.writeFileSync,
  unlinkSync: fs.unlinkSync as typeof fs.unlinkSync,
  execFileSync: child_process.execFileSync as typeof child_process.execFileSync,
};

function buildCommitEnv(): NodeJS.ProcessEnv {
  const env = { ...process.env };
  env.GIT_EDITOR = 'false';
  env.GIT_PAGER = 'cat';
  env.GIT_TERMINAL_PROMPT = '0';
  env.AI_AGENT = 'superinference';
  return env;
}

function buildFinalMessage(message: string): string {
  // Always remove foreign/disallowed attribution first — a drafted message must
  // never carry another assistant's byline, even when the AMI trailer is opted out.
  const trimmed = stripDisallowedAttribution(message.trim());
  // Attribution opt-out for user projects that don't want the byline.
  if (process.env.SUPERINFERENCE_DISABLE_CO_AUTHOR === '1') {
    return trimmed;
  }
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
    'Optionally stage files before committing. The Co-Authored-By trailer is appended automatically unless SUPERINFERENCE_DISABLE_CO_AUTHOR=1 is set.',
  inputSchema: {
    type: 'object',
    properties: {
      message: {
        type: 'string',
        description: 'The commit message. The Co-Authored-By trailer is appended automatically (unless SUPERINFERENCE_DISABLE_CO_AUTHOR=1) — do not include it.',
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
      const safeFiles = files.filter(f => typeof f === 'string');
      try {
        if (safeFiles.length === 1 && safeFiles[0] === '.') {
          _gitCommitFs.execFileSync('git', ['add', '-u'], { cwd: context.cwd, timeout: 30_000, env, stdio: 'pipe' });
        } else {
          _gitCommitFs.execFileSync('git', ['add', '--', ...safeFiles], { cwd: context.cwd, timeout: 30_000, env, stdio: 'pipe' });
        }
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
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

    // Secret-bearing FILE guard: refuse to commit staged paths that look like
    // credential files (.env, credentials.json, id_rsa, *.pem, …). git_commit
    // stages/commits directly via git, bypassing the bash permission layer, so
    // this is the only place that can stop a credential file reaching a remote.
    const stagedResult = await execCommand('git diff --cached --name-only', execOpts);
    const secretFiles = stagedResult.stdout
      .split('\n')
      .map(p => p.trim())
      .filter(Boolean)
      .filter(p => isSecretFile(path.basename(p)));
    if (secretFiles.length > 0) {
      return {
        output:
          `Error: refusing to commit files that likely contain secrets: ${secretFiles.join(', ')}. ` +
          'Unstage them (git restore --staged <file>) or add them to .gitignore before committing.',
        isError: true,
      };
    }

    const secrets = scanForSecrets(message);
    if (secrets.length > 0) {
      return { output: `Warning: Potential secrets detected in commit message: ${secrets.map(s => s.label).join(', ')}. Remove secrets before committing.`, isError: true };
    }

    // Build the final commit message with trailer
    const finalMessage = buildFinalMessage(message);

    // Write message to temp file to avoid shell escaping issues. Use an
    // unpredictable randomUUID name and the 'wx' (O_EXCL) flag so a pre-planted
    // symlink or a predictable-path attacker can't hijack the write target.
    // Honor the AMI_TMPDIR / CLAUDE_CODE_TMPDIR override so operators with custom
    // temp requirements can relocate the commit-message temp file.
    const tmpDir = resolveTmpBase();
    const tmpFile = path.join(tmpDir, `ami-commit-${crypto.randomUUID()}.txt`);

    try {
      try {
        _gitCommitFs.writeFileSync(tmpFile, finalMessage, { encoding: 'utf-8', flag: 'wx' });
      } catch (e) {
        const err = e instanceof Error ? e.message : String(e);
        return { output: `Error writing commit message file: ${err}`, isError: true };
      }

      // In detached/unattended mode, bypass user hooks and GPG signing so a
      // pre-commit hook or a passphrase prompt can't hang the run. In the
      // interactive TUI we deliberately keep the user's hooks/signing active.
      const unattendedFlags = context.detachedMode
        ? ' -c core.hooksPath=/dev/null -c commit.gpgsign=false'
        : '';
      const commitResult = await execCommand(`git${unattendedFlags} commit -F "${tmpFile}"`, execOpts);
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
      try { _gitCommitFs.unlinkSync(tmpFile); } catch { /* already cleaned or never written */ }
    }
  },
};

export { buildFinalMessage, CO_AUTHOR_TRAILER, TRAILER_REGEX, isSecretFile, stripDisallowedAttribution };
