import { execSync } from 'child_process';

/** Hard ceiling so a wedged git invocation can never hang a caller. */
const GIT_BRANCH_TIMEOUT_MS = 2_000;

/** Non-interactive git env: never block on credential / terminal prompts. */
const GIT_NO_PROMPT_ENV = {
  GIT_TERMINAL_PROMPT: '0',
  GIT_ASKPASS: '',
} as const;

/**
 * Return the current git branch name for `cwd`, or `undefined` when `cwd` is not
 * a git working tree (or git is unavailable). Shared helper so callers that need
 * to stamp the branch (e.g. persisted sessions) don't each re-spawn git with
 * slightly different flags. A detached HEAD yields `undefined` (abbrev-ref prints
 * "HEAD", which is not a branch name).
 */
export function getGitBranch(cwd: string): string | undefined {
  try {
    const out = execSync('git rev-parse --abbrev-ref HEAD', {
      cwd,
      encoding: 'utf-8',
      timeout: GIT_BRANCH_TIMEOUT_MS,
      stdio: ['ignore', 'pipe', 'ignore'],
      env: { ...process.env, ...GIT_NO_PROMPT_ENV },
    }).trim();
    if (!out || out === 'HEAD') return undefined;
    return out;
  } catch {
    return undefined;
  }
}
