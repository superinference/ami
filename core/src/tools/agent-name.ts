// Extracted leaf: agent-name sanitization. Dependency-free so tools/team.ts
// imports it without a team -> task cycle.

export const AGENT_NAME_RESERVED_CHARS = /[@*]/g;

export function sanitizeAgentName(name: string): string {
  return name
    .replace(AGENT_NAME_RESERVED_CHARS, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '');
}
