/**
 * Single source of truth for the AMI product version and client identity.
 *
 * MUST equal every package.json version in the monorepo — enforced by
 * test/version-sync.test.ts, which fails CI if this string drifts from the
 * canonical manifest. Import AMI_VERSION / AMI_CLIENT_INFO everywhere instead of
 * hardcoding a version string (the old `'1.0.0'` MCP clientInfo bug) or
 * re-reading package.json from library code, so a release bump only touches the
 * manifests + this constant and nothing silently falls out of sync.
 */
export const AMI_VERSION = '0.7.4';

/** The client name AMI advertises to MCP servers and HTTP peers. */
export const AMI_CLIENT_NAME = 'ami';

/** clientInfo sent in the MCP `initialize` handshake (name + real version). */
export const AMI_CLIENT_INFO = { name: AMI_CLIENT_NAME, version: AMI_VERSION } as const;

/**
 * Resolve the originating entrypoint/surface tag for this process — the frontend
 * or embedding host that spawned AMI (`cli`, `vscode`, `mcp`, `sdk`, a CI action,
 * …). The single source for reading this knob, so every surface-identifying site
 * (User-Agent, git-commit trailer) agrees rather than re-reading the env ad hoc.
 *
 * Parity with Claude Code's entrypoint knob (`CLAUDE_CODE_ENTRYPOINT`, read by
 * `getMCPUserAgent`/`getUserAgent` in `src/utils/http.ts`): AMI prefers its own
 * `AMI_ENTRYPOINT`, then honors `CLAUDE_CODE_ENTRYPOINT` for cross-tool
 * compatibility. Returns `undefined` for an unset/blank value so UA builders can
 * omit the tag entirely; callers that need a concrete default apply their own
 * (e.g. the git-commit surface defaults to `'cli'`).
 */
export function resolveEntrypoint(env: NodeJS.ProcessEnv = process.env): string | undefined {
  const raw = env.AMI_ENTRYPOINT ?? env.CLAUDE_CODE_ENTRYPOINT;
  const trimmed = raw?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

/** Read a trimmed, non-blank env value under the AMI name then a CLAUDE fallback. */
function readSurfaceKnob(env: NodeJS.ProcessEnv, amiKey: string, claudeKey: string): string | undefined {
  const raw = env[amiKey] ?? env[claudeKey];
  const trimmed = raw?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Centralized, version-stamped `User-Agent` string for every outbound HTTP
 * request AMI makes (skill-catalog fetch, plugin download, update check, MCP
 * HTTP/SSE transport, …).
 *
 * Mirrors Claude Code's `getClaudeCodeUserAgent()` / `getMCPUserAgent()`
 * (`src/utils/http.ts`), which return `claude-code/${VERSION}` plus a
 * parenthesized suffix tagging the originating surface. AMI's form is
 * `ami/<version>` with a parenthesized suffix composed of, in order:
 *   1. an optional free-form `component` (which AMI subsystem — e.g.
 *      `skill-discovery`), so a specific fetch site stays identifiable;
 *   2. the originating entrypoint ({@link resolveEntrypoint}: `AMI_ENTRYPOINT`
 *      then `CLAUDE_CODE_ENTRYPOINT`), so downstream servers see which frontend
 *      drove the request (TUI vs detached CLI vs MCP vs an embedding host);
 *   3. `agent-sdk/<ver>` from `AMI_AGENT_SDK_VERSION` (or Claude's
 *      `CLAUDE_AGENT_SDK_VERSION`) when AMI is driven by an embedding SDK;
 *   4. `client-app/<id>` from `AMI_AGENT_SDK_CLIENT_APP` (or Claude's
 *      `CLAUDE_AGENT_SDK_CLIENT_APP`) so an embedding application can tag itself.
 * Each piece is appended only when present, so with nothing set the UA stays the
 * bare `ami/<version>`. Callers import this ONE helper rather than re-spelling a
 * bespoke UA per fetch site (AGENTS.md No-Duplication).
 */
export function amiUserAgent(component?: string, env: NodeJS.ProcessEnv = process.env): string {
  const parts: string[] = [];
  if (component) parts.push(component);
  const entrypoint = resolveEntrypoint(env);
  if (entrypoint) parts.push(entrypoint);
  const sdkVersion = readSurfaceKnob(env, 'AMI_AGENT_SDK_VERSION', 'CLAUDE_AGENT_SDK_VERSION');
  if (sdkVersion) parts.push(`agent-sdk/${sdkVersion}`);
  const clientApp = readSurfaceKnob(env, 'AMI_AGENT_SDK_CLIENT_APP', 'CLAUDE_AGENT_SDK_CLIENT_APP');
  if (clientApp) parts.push(`client-app/${clientApp}`);
  const base = `${AMI_CLIENT_NAME}/${AMI_VERSION}`;
  return parts.length > 0 ? `${base} (${parts.join(', ')})` : base;
}
