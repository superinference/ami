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
 * Centralized, version-stamped `User-Agent` string for every outbound HTTP
 * request AMI makes (skill-catalog fetch, plugin download, update check, …).
 *
 * Mirrors Claude Code's `getClaudeCodeUserAgent()` (`src/utils/userAgent.ts`),
 * which returns `claude-code/${VERSION}` — a single leaf so a version bump is
 * reflected everywhere and remote endpoints can attribute traffic. AMI's form is
 * `ami/<version>`, with an optional component suffix so a specific subsystem
 * stays identifiable in server logs (e.g. `ami/0.7.4 (skill-discovery)`) without
 * hardcoding an unversioned per-site string. Callers import this ONE helper
 * rather than re-spelling a bespoke UA per fetch site (AGENTS.md No-Duplication).
 */
export function amiUserAgent(component?: string): string {
  const base = `${AMI_CLIENT_NAME}/${AMI_VERSION}`;
  return component ? `${base} (${component})` : base;
}
