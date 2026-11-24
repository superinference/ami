// Split from memory.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { canonicalGitRoot, projectSlug, resolveConfigDir } from './reference-journal';
import * as path from 'path';
import type { AgentMemoryScope } from './skills';
import { MEMORY_INDEX_FILENAME } from './memory-memory-diagnostic';
import { AGENT_MEMORY_DIRNAME, AGENT_MEMORY_LOCAL_DIRNAME, AGENT_REMOTE_MEMORY_DIR_ENV } from './memory-parse-frontmatter';

/**
 * Sanitize an agent type name for use as a single on-disk directory component.
 * Parity with the reference's `sanitizeAgentTypeForPath`: colons (invalid on
 * Windows, used by plugin-namespaced agent types like `my-plugin:my-agent`) map
 * to dashes. AMI additionally neutralizes path separators and `..` traversal runs
 * so an attacker-controlled agent type can never escape its memory directory.
 */
export function sanitizeAgentTypeForPath(agentType: string): string {
  return agentType
    .replace(/:/g, '-')
    .replace(/[/\\]/g, '-')
    .replace(/\.{2,}/g, '-');
}

/** Resolve the LOCAL-scope agent memory dir, honoring {@link AGENT_REMOTE_MEMORY_DIR_ENV}. */
export function localAgentMemoryDir(dirName: string, gitRoot: string, env: NodeJS.ProcessEnv): string {
  const remote = env[AGENT_REMOTE_MEMORY_DIR_ENV];
  if (remote && remote.trim() !== '') {
    return path.join(remote.trim(), 'projects', projectSlug(gitRoot), AGENT_MEMORY_LOCAL_DIRNAME, dirName);
  }
  return path.join(gitRoot, '.ami', AGENT_MEMORY_LOCAL_DIRNAME, dirName);
}

/**
 * Absolute directory holding a given agent type's persistent memory for `scope`.
 * Parity with the reference's `getAgentMemoryDir`. `project`/`local` scopes key on
 * the CANONICAL git root (not raw cwd) so every worktree of a repo shares one
 * agent-memory store rather than fragmenting it per-worktree (AMI's memory-keying
 * convention; a superset over the reference's raw-cwd keying).
 */
export function getAgentMemoryDir(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  const dirName = sanitizeAgentTypeForPath(agentType);
  const gitRoot = canonicalGitRoot(cwd);
  switch (scope) {
    case 'user':
      return path.join(resolveConfigDir(env), AGENT_MEMORY_DIRNAME, dirName);
    case 'project':
      return path.join(gitRoot, '.ami', AGENT_MEMORY_DIRNAME, dirName);
    case 'local':
      return localAgentMemoryDir(dirName, gitRoot, env);
  }
}

/** Absolute path to an agent type's MEMORY.md entrypoint for `scope`. */
export function getAgentMemoryEntrypoint(
  agentType: string,
  scope: AgentMemoryScope,
  cwd: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  return path.join(getAgentMemoryDir(agentType, scope, cwd, env), MEMORY_INDEX_FILENAME);
}

export const AGENT_MEMORY_SCOPE_NOTE: Record<AgentMemoryScope, string> = {
  user: '- This memory is user-scope and applies across every project — keep learnings general, not project-specific.',
  project: '- This memory is project-scope and shared with your team via version control — tailor learnings to this project.',
  local: '- This memory is local-scope (NOT checked into version control) — tailor learnings to this project and this machine.',
};
