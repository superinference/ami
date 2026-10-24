import { ToolDefinition, ToolResult, ToolContext } from '../types';
import * as child_process from 'child_process';
import * as path from 'path';
import * as fs from 'fs';

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
};

export const scoutTool: ToolDefinition = {
  name: 'scout',
  description: 'Research an upstream dependency or external repository. Clones the repo to a managed cache dir and returns an overview without polluting your workspace.',
  inputSchema: {
    type: 'object',
    properties: {
      repo: { type: 'string', description: 'Repository URL or shorthand (e.g., "owner/repo" for GitHub).' },
      query: { type: 'string', description: 'What to look for in the repository.' },
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
    const cacheDir = path.join(context.cwd, '.superinference', 'scout-cache');
    const repoName = repo.replace(/[^a-zA-Z0-9_-]/g, '_');
    const localPath = path.join(cacheDir, repoName);

    try {
      if (!scoutHooks.existsSync(localPath)) {
        scoutHooks.mkdirSync(cacheDir, { recursive: true });
        scoutHooks.execFileSync('git', ['clone', '--depth', '1', repoUrl, localPath], { timeout: 60000, stdio: 'pipe' });
      }

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
      return { output: `Error scouting ${repo}: ${err instanceof Error ? err.message : String(err)}`, isError: true };
    }
  },
};
