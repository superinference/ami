// Split from skills.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import { mcpToolNamespacePrefix } from './skills-classify-agent-mcp-server-specs';

/**
 * True when a fully-qualified tool name belongs to `serverName`'s MCP namespace —
 * i.e. it is `mcp__<server>__<tool>` with a non-empty `<tool>` segment. A blank
 * server name, or a non-string tool name, never matches. Shared by
 * {@link deriveMcpToolAllowlist} and the skill-tool server-wildcard expansion so
 * "which tools does this server advertise" is decided in exactly one place.
 */
export function isMcpToolOfServer(toolName: string, serverName: string): boolean {
  if (typeof toolName !== 'string' || typeof serverName !== 'string') return false;
  const server = serverName.trim();
  if (server.length === 0) return false;
  const prefix = mcpToolNamespacePrefix(server);
  return toolName.length > prefix.length && toolName.startsWith(prefix);
}
