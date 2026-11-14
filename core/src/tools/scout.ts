import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { projectStateDir } from '../reference-journal';
import { GIT_NO_PROMPT_ENV, redactSecrets } from './tool-utils';
import * as child_process from 'child_process';
import * as path from 'path';
import * as fs from 'fs';

const DEFAULT_CLONE_TIMEOUT_MS = 60000;

/** Strip `user:password@` userinfo from any URL so credentials in a repo URL never
 *  leak into an error message or spawned-command echo. */
export function stripUrlCredentials(text: string): string {
  return text.replace(/\/\/[^/\s@]+@/g, '//');
}

/** Normalize a git URL for identity comparison: drop embedded credentials, a
 *  trailing `.git`, and trailing slashes so equivalent URLs compare equal. */
export function normalizeRepoUrl(url: string): string {
  return stripUrlCredentials(url.trim()).replace(/\/+$/, '').replace(/\.git$/i, '').replace(/\/+$/, '');
}

/**
 * Confirm the cached clone at `localPath` is the SAME repository we were asked
 * for by comparing its `origin` remote to `repoUrl`. The cache directory name is
 * a sanitized slug, so distinct URLs can collide onto one path — a present `.git`
 * alone is NOT proof the cache holds the requested repo. Returns false (→ re-clone)
 * when the remote cannot be read or does not match. Mirrors Claude Code's
 * validate-repo-at-path check before trusting a cached clone.
 */
export function cachedCloneMatches(localPath: string, repoUrl: string): boolean {
  try {
    const origin = (scoutHooks.execFileSync('git', ['-C', localPath, 'remote', 'get-url', 'origin'], {
      encoding: 'utf-8', timeout: 5000, stdio: 'pipe',
    }) as string).trim();
    return normalizeRepoUrl(origin) === normalizeRepoUrl(repoUrl);
  } catch {
    return false;
  }
}

/** Resolve the clone timeout from input (`timeout_ms`) or env, falling back to the default. */
export function scoutCloneTimeoutMs(input: Record<string, unknown>, env: NodeJS.ProcessEnv = process.env): number {
  const fromInput = Number(input.timeout_ms);
  if (Number.isFinite(fromInput) && fromInput > 0) return fromInput;
  const fromEnv = Number((env.AMI_SCOUT_CLONE_TIMEOUT_MS ?? '').trim());
  return Number.isFinite(fromEnv) && fromEnv > 0 ? fromEnv : DEFAULT_CLONE_TIMEOUT_MS;
}

/**
 * Indirection seam for process spawning and filesystem access. Tests swap these
 * for fakes to drive the clone/find/grep error paths deterministically — in
 * particular the non-Error throw arm of the catch. Production uses the real
 * child_process/fs functions, so behavior is unchanged.
 */
export const scoutHooks = {
  execFileSync: child_process.execFileSync,
  execSync: child_process.execSync,
  existsSync: fs.existsSync,
  mkdirSync: fs.mkdirSync,
  readFileSync: fs.readFileSync,
  rmSync: fs.rmSync,
};

export const scoutTool: ToolDefinition = {
  name: 'scout',
  description: 'Research an upstream dependency or external repository. Clones the repo to a managed cache dir and returns an overview without polluting your workspace.',
  inputSchema: {
    type: 'object',
    properties: {
      repo: { type: 'string', description: 'Repository URL or shorthand (e.g., "owner/repo" for GitHub).' },
      query: { type: 'string', description: 'What to look for in the repository.' },
      timeout_ms: { type: 'number', description: 'Clone timeout in milliseconds (default 60000). Raise for a large repo on a slow link.' },
    },
    required: ['repo'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const repo = input.repo as string;
    const query = input.query as string | undefined;
    const repoUrl =
      repo.includes('://') || repo.startsWith('/') || repo.startsWith('.')
        ? repo
        : `https://github.com/${repo}`;
    const cacheDir = projectStateDir(context.cwd, 'scout-cache');
    const repoName = repo.replace(/[^a-zA-Z0-9_-]/g, '_');
    const localPath = path.join(cacheDir, repoName);

    const abort = () => {
      if (context.abortSignal?.aborted) {
        return { output: `Scout of ${repo} was cancelled.`, isError: true } as ToolResult;
      }
      return undefined;
    };

    const cloneTimeout = scoutCloneTimeoutMs(input);

    try {
      let aborted = abort();
      if (aborted) return aborted;
      // A prior clone killed mid-flight (timeout/abort) can leave a partial dir with
      // no .git. Treat a dir without .git as invalid cache: purge it and re-clone,
      // instead of trusting it and failing every subsequent find/grep. Also purge a
      // cache whose origin remote does not match the requested repo — the sanitized
      // cache slug can collide across distinct URLs, so a present .git is not proof
      // the cache holds the repo we were asked for.
      if (scoutHooks.existsSync(localPath)
          && (!scoutHooks.existsSync(path.join(localPath, '.git')) || !cachedCloneMatches(localPath, repoUrl))) {
        scoutHooks.rmSync(localPath, { recursive: true, force: true });
      }
      if (!scoutHooks.existsSync(localPath)) {
        scoutHooks.mkdirSync(cacheDir, { recursive: true });
        // Fail fast on auth prompts (private/unauthenticated repo) and honour cancellation
        // instead of blocking up to the timeout on an interactive credential prompt.
        try {
          scoutHooks.execFileSync('git', ['clone', '--depth', '1', repoUrl, localPath], {
            timeout: cloneTimeout,
            stdio: 'pipe',
            env: { ...process.env, ...GIT_NO_PROMPT_ENV },
          });
        } catch (cloneErr) {
          // Clean up any partial clone so the next attempt starts fresh.
          if (scoutHooks.existsSync(localPath)) {
            try { scoutHooks.rmSync(localPath, { recursive: true, force: true }); } catch { /* best effort */ }
          }
          const e = cloneErr as { signal?: string; code?: string | number; message?: string };
          if (e?.signal === 'SIGTERM' || e?.code === 'ETIMEDOUT') {
            return {
              output: `Error scouting ${stripUrlCredentials(repo)}: clone timed out after ${cloneTimeout}ms. Retry with a higher timeout_ms (e.g. ${cloneTimeout * 2}) for a large repo or slow link.`,
              isError: true,
            };
          }
          throw cloneErr;
        }
      }

      aborted = abort();
      if (aborted) return aborted;
      const files = scoutHooks.execSync('find . -type f -not -path "./.git/*" | head -50', {
        cwd: localPath, encoding: 'utf-8', timeout: 5000,
      }).trim();

      let readme = '';
      for (const f of ['README.md', 'readme.md', 'README.rst', 'README']) {
        const rp = path.join(localPath, f);
        if (scoutHooks.existsSync(rp)) { readme = (scoutHooks.readFileSync(rp, 'utf-8') as string).slice(0, 3000); break; }
      }

      let searchResults = '';
      if (query) {
        const abortedBeforeGrep = abort();
        if (abortedBeforeGrep) return abortedBeforeGrep;
        try {
          const grepOut = scoutHooks.execFileSync(
            'grep', ['-rn', '--include=*.ts', '--include=*.js', '--include=*.py', '--include=*.go', '--include=*.rs', '--', query, '.'],
            { cwd: localPath, encoding: 'utf-8', timeout: 10000, maxBuffer: 1024 * 1024 },
          );
          searchResults = grepOut.split('\n').slice(0, 20).join('\n').trim();
        } catch { /* grep returns non-zero when no matches */ }
      }

      return {
        output: `## Scout: ${repo}\n\n### Cache path:\n${localPath}\n\nRead files with file_read using this directory as the prefix.\n\n### Files:\n${files}\n\n### README:\n${readme}\n\n${searchResults ? `### Search results for "${query}":\n${searchResults}` : ''}`,
      };
    } catch (err) {
      // Redact any credentials embedded in a repo URL (https://user:token@host/…)
      // before surfacing git's failure message / command echo.
      const rawMsg = err instanceof Error ? err.message : String(err);
      const safeMsg = redactSecrets(stripUrlCredentials(rawMsg));
      return { output: `Error scouting ${stripUrlCredentials(repo)}: ${safeMsg}`, isError: true };
    }
  },
};
