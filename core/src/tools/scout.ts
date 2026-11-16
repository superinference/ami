import { ToolDefinition, ToolResult, ToolContext } from '../types';
import { projectStateDir } from '../reference-journal';
import { redactSecrets } from './tool-utils';
import { GIT_NO_PROMPT_ENV, validateGitSource, isScpLikeGitSource } from '../plugins';
import * as child_process from 'child_process';
import * as path from 'path';
import * as fs from 'fs';

const DEFAULT_CLONE_TIMEOUT_MS = 60000;
/** Hard caps on how much of each section scout emits. Named so the
 *  truncation-visibility markers below stay in sync with the slice limits. */
const FILES_CAP = 50;
const README_CAP = 3000;
const GREP_MATCH_CAP = 20;

/** Default `--include` globs for the in-repo search when the caller gives no
 *  `glob`/`file_type`. Mirrors scout's original hardcoded extension set. */
const DEFAULT_SCOUT_INCLUDES = ['*.ts', '*.js', '*.py', '*.go', '*.rs'];

/** Map a language name (Claude's `rg --type`) to its `--include` globs. Keeps a
 *  scout of a Java/Ruby/C/Markdown repo from silently matching nothing. */
const FILE_TYPE_GLOBS: Record<string, string[]> = {
  js: ['*.js', '*.jsx', '*.mjs', '*.cjs'],
  ts: ['*.ts', '*.tsx'],
  py: ['*.py'], python: ['*.py'],
  go: ['*.go'],
  rust: ['*.rs'], rs: ['*.rs'],
  java: ['*.java'],
  c: ['*.c', '*.h'],
  cpp: ['*.cpp', '*.cc', '*.cxx', '*.hpp', '*.hh'],
  ruby: ['*.rb'], rb: ['*.rb'],
  php: ['*.php'],
  md: ['*.md', '*.markdown'], markdown: ['*.md', '*.markdown'],
  json: ['*.json'],
  yaml: ['*.yaml', '*.yml'], yml: ['*.yaml', '*.yml'],
  sh: ['*.sh', '*.bash'], shell: ['*.sh', '*.bash'],
};

/**
 * Build the `grep` argv for scout's in-repo search, honouring the optional
 * caller filters: `glob` (single `--include` override, Claude's `rg --glob`),
 * `file_type` (language name → `--include` globs, Claude's `rg --type`), and
 * `case_insensitive` (`-i`, Claude's `rg -i`). With no filters it falls back to
 * scout's default extension set. `query` is placed after `--` so it can never be
 * read as a flag; args are passed to `execFileSync` (no shell), so a glob/type
 * containing shell metacharacters cannot inject.
 */
export function scoutGrepArgs(query: string, input: Record<string, unknown>): string[] {
  const args = ['-rn'];
  if (input.case_insensitive === true) args.push('-i');
  const glob = typeof input.glob === 'string' && input.glob.trim() ? input.glob.trim() : undefined;
  const fileType =
    typeof input.file_type === 'string' && input.file_type.trim()
      ? input.file_type.trim().toLowerCase()
      : undefined;
  let includes: string[];
  if (glob) includes = [glob];
  else if (fileType) includes = FILE_TYPE_GLOBS[fileType] ?? [`*.${fileType}`];
  else includes = DEFAULT_SCOUT_INCLUDES;
  for (const inc of includes) args.push(`--include=${inc}`);
  args.push('--', query, '.');
  return args;
}

/**
 * Resolve the search result window from the caller's optional `limit`/`offset`
 * so the model can page past scout's default cap instead of being stuck with a
 * silent first-20 peek. `limit` defaults to {@link GREP_MATCH_CAP}; `limit: 0`
 * is the unlimited escape hatch (Claude's `head_limit: 0`). `offset` skips the
 * first N matches (Claude's `offset`). Mirrors GrepTool's applyHeadLimit knobs.
 */
export function scoutSearchWindow(input: Record<string, unknown>): { limit: number; offset: number } {
  const rawLimit = Number(input.limit);
  const limit = Number.isFinite(rawLimit) && rawLimit >= 0 ? Math.floor(rawLimit) : GREP_MATCH_CAP;
  const rawOffset = Number(input.offset);
  const offset = Number.isFinite(rawOffset) && rawOffset > 0 ? Math.floor(rawOffset) : 0;
  return { limit, offset };
}

/**
 * Resolve the user-supplied `repo` into a source `git clone` understands. Only a
 * bare GitHub shorthand (`owner/repo`) is expanded to an HTTPS URL; a full remote
 * URL (`https://`, `http://`, `ssh://`, `git://`, `file://`), an scp-style SSH
 * remote (`git@host:owner/repo.git`), or a local path (`/…`, `./…`) is handed to
 * git verbatim — its trailing `.git` and host port are preserved by git itself.
 * The scp carve-out is essential: without it `git@host:owner/repo.git` would be
 * mangled into the broken URL `https://github.com/git@host:owner/repo.git`.
 */
export function resolveScoutRepoUrl(repo: string): string {
  if (
    repo.includes('://') ||
    repo.startsWith('/') ||
    repo.startsWith('.') ||
    isScpLikeGitSource(repo)
  ) {
    return repo;
  }
  return `https://github.com/${repo}`;
}

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
      repo: { type: 'string', description: 'Repository to research. Accepts: a GitHub shorthand ("owner/repo"), a full URL (https://, http://, ssh://, git://), an scp-style SSH remote ("git@host:owner/repo.git"), or a local path. A trailing ".git" and host port are preserved.' },
      query: { type: 'string', description: 'What to look for in the repository.' },
      glob: { type: 'string', description: 'Glob pattern to restrict the search to (e.g. "*.java", "*.rb"). Overrides the default extension set.' },
      file_type: { type: 'string', description: 'Language to search (e.g. "java", "ruby", "markdown"). Maps to the matching file extensions.' },
      case_insensitive: { type: 'boolean', description: 'Match the query case-insensitively (grep -i). Default false.' },
      limit: { type: 'number', description: 'Max search matches to return (default 20). Pass 0 for unlimited.' },
      offset: { type: 'number', description: 'Skip the first N search matches before applying limit, to page past earlier results.' },
      timeout_ms: { type: 'number', description: 'Clone timeout in milliseconds (default 60000). Raise for a large repo on a slow link.' },
    },
    required: ['repo'],
  },
  isReadOnly: true,
  async execute(input: Record<string, unknown>, context: ToolContext): Promise<ToolResult> {
    const repo = input.repo as string;
    const query = input.query as string | undefined;
    // Gate the source through the SAME validator every other git-clone call site uses:
    // reject transport-helper RCE (`ext::`, `fd::`, `file::…`), argument injection
    // (sources starting with `-`), NUL bytes, and non-allowlisted URL schemes BEFORE
    // handing anything to `git clone`. GitHub shorthand and local paths pass through.
    const sourceCheck = validateGitSource(repo);
    if (!sourceCheck.ok) {
      return { output: `Error scouting ${stripUrlCredentials(String(repo))}: ${sourceCheck.reason}`, isError: true };
    }
    const repoUrl = resolveScoutRepoUrl(repo);
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
            // Ignore stdin (fd 0) while still capturing stdout/stderr so git can never
            // block the process waiting on an interactive credential/host-key prompt;
            // combined with GIT_NO_PROMPT_ENV (terminal prompt off, no askpass, ssh
            // BatchMode) an unauthenticated source fails fast instead of stalling until
            // the timeout.
            stdio: ['ignore', 'pipe', 'pipe'],
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
      // Ask for one more than the cap so an over-cap listing is detectable, then
      // emit an explicit marker instead of silently dropping the overflow.
      const allFiles = scoutHooks.execSync(`find . -type f -not -path "./.git/*" | head -${FILES_CAP + 1}`, {
        cwd: localPath, encoding: 'utf-8', timeout: 5000,
      }).trim().split('\n').filter(Boolean);
      const filesTruncated = allFiles.length > FILES_CAP;
      let files = allFiles.slice(0, FILES_CAP).join('\n');
      if (filesTruncated) {
        files += `\n… (showing first ${FILES_CAP}; more files exist — explore the cache dir with file_read/grep)`;
      }

      let readme = '';
      for (const f of ['README.md', 'readme.md', 'README.rst', 'README']) {
        const rp = path.join(localPath, f);
        if (scoutHooks.existsSync(rp)) {
          const full = scoutHooks.readFileSync(rp, 'utf-8') as string;
          readme = full.slice(0, README_CAP);
          if (full.length > README_CAP) readme += `\n… (README truncated to first ${README_CAP} chars — read the full file with file_read)`;
          break;
        }
      }

      let searchResults = '';
      if (query) {
        const abortedBeforeGrep = abort();
        if (abortedBeforeGrep) return abortedBeforeGrep;
        try {
          const grepOut = scoutHooks.execFileSync(
            'grep', scoutGrepArgs(query, input),
            { cwd: localPath, encoding: 'utf-8', timeout: 10000, maxBuffer: 1024 * 1024 },
          );
          const matches = grepOut.split('\n').map(l => l.trimEnd()).filter(Boolean);
          const { limit, offset } = scoutSearchWindow(input);
          const windowed = limit === 0 ? matches.slice(offset) : matches.slice(offset, offset + limit);
          searchResults = windowed.join('\n').trim();
          const truncated = limit !== 0 && matches.length - offset > limit;
          if (truncated) {
            searchResults += offset > 0
              ? `\n… (showing matches ${offset + 1}-${offset + windowed.length} of ${matches.length} — raise limit or advance offset to see more)`
              : `\n… (showing first ${windowed.length} of ${matches.length} matches — raise limit or narrow the query to see more)`;
          }
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
