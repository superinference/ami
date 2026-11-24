// Split from team.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { TEAM_LEAD_NAME } from './send-message';
import { PERMISSION_MODES, normalizePermissionMode, PermissionMode } from '../permissions';
import { sanitizeAgentName } from './agent-name';
import { AgentColorName, parseAgentColor } from '../skills';
import { getTeamDir, isBackendType, teamDebug, teamFilePath } from './team-team-member';
import type { TeamAllowedPath, TeamFile, TeamMember } from './team-team-member';

/**
 * Absolute path of a team's on-disk `team.json`, derived through the canonical
 * {@link getTeamDir} (so the read boundary re-sanitizes the name). Returns null for
 * a name that does not normalize to a safe slug (parity: the reference's
 * `getTeamFilePath(teamName)`).
 */
export function getTeamFilePath(cwd: string, name: string): string | null {
  const dir = getTeamDir(cwd, name);
  return dir ? teamFilePath(dir) : null;
}

/**
 * Guarded reader for a persisted team file. Returns the parsed {@link TeamFile},
 * or null when the file is missing, unreadable, not valid JSON, or structurally
 * invalid (missing name / leadAgentId / members array). Mirrors the reference's
 * swarm `readTeamFile` (null on ENOENT / parse failure) so a resumed session
 * degrades gracefully instead of throwing on a missing or corrupt team file.
 */
export function readTeamFile(dir: string): TeamFile | null {
  const file = teamFilePath(dir);
  let raw: string;
  try {
    raw = fs.readFileSync(file, 'utf8');
  } catch (err) {
    // ENOENT is the clean "no team here yet" case — degrade to null silently.
    // Any OTHER read error (EACCES/permission, EISDIR, I/O) is an unexpected
    // condition worth a diagnostic before degrading (parity: the reference's readTeamFile
    // treats ENOENT as a clean null and logs non-ENOENT errors). The team concept
    // still degrades to null so a resume never throws on an unreadable file.
    const code = (err as NodeJS.ErrnoException | undefined)?.code;
    if (code !== 'ENOENT') {
      teamDebug(`readTeamFile: unreadable team file ${file} (${code ?? 'unknown error'}) — degrading to null`);
    }
    return null;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    // Corrupt JSON is distinct from a missing file: the file exists but is damaged
    // (partial write, hand-edit). Log it so the lifecycle is observable, then
    // degrade to null rather than throwing (parity: ENOENT-vs-corruption distinction).
    teamDebug(`readTeamFile: corrupt JSON in ${file} — degrading to null`);
    return null;
  }
  if (!parsed || typeof parsed !== 'object') {
    teamDebug(`readTeamFile: non-object team file ${file} — degrading to null`);
    return null;
  }
  const t = parsed as Partial<TeamFile>;
  if (typeof t.name !== 'string' || typeof t.leadAgentId !== 'string' || !Array.isArray(t.members)) {
    teamDebug(`readTeamFile: invalid team file shape in ${file} (missing name/leadAgentId/members) — degrading to null`);
    return null; // wrong shape
  }
  // Boundary validation of persisted enum fields (parity: the reference validates
  // backendType via isPaneBackend and coerces unknown values when deserializing).
  // A stale/foreign file may carry an invalid permissionMode or backendType; coerce
  // each bad value to undefined in place rather than trusting it downstream.
  const isStr = (v: unknown): v is string => typeof v === 'string';
  for (const raw of t.members as unknown[]) {
    if (!raw || typeof raw !== 'object') continue;
    const member = raw as TeamMember;
    if (member.permissionMode !== undefined && !normalizePermissionMode(member.permissionMode)) {
      member.permissionMode = undefined;
    }
    if (member.backendType !== undefined && !isBackendType(member.backendType)) {
      member.backendType = undefined;
    }
    // A pane id must be a non-empty string (parity: the reference only honors a real
    // tmuxPaneId when deriving isHidden); coerce a non-string/empty to undefined.
    if (member.tmuxPaneId !== undefined && (!isStr(member.tmuxPaneId) || member.tmuxPaneId.length === 0)) {
      member.tmuxPaneId = undefined;
    }
    // Subscriptions must be an array of strings; drop a malformed value so routing
    // never trusts a non-list (parity: the reference stores subscriptions as string[]).
    if (member.subscriptions !== undefined && (!Array.isArray(member.subscriptions) || !member.subscriptions.every(isStr))) {
      member.subscriptions = undefined;
    }
  }
  // Team-level hiddenPaneIds must be a string array (parity: discovery coerces
  // `hiddenPaneIds ?? []` into a Set); a corrupt non-array is coerced to undefined.
  if (t.hiddenPaneIds !== undefined && (!Array.isArray(t.hiddenPaneIds) || !t.hiddenPaneIds.every(isStr))) {
    t.hiddenPaneIds = undefined;
  }
  return t as TeamFile;
}

/**
 * Recover a member record by its (case-sensitive) name. Returns null when the
 * member is absent — e.g. it was removed from the team between sessions. Mirrors
 * the reference's resume-time `members.find(m => m.name === agentName)` lookup with
 * its explicit removed-member branch (callers tolerate the null rather than throw).
 */
export function findTeamMemberByName(team: TeamFile, name: string): TeamMember | null {
  return team.members.find(m => m.name === name) ?? null;
}

/**
 * Load a persisted team by name from a workspace. Returns the team plus its
 * on-disk directory, or null when no readable/valid team file exists there. Used
 * by the resumed-session reconnection path so team state survives a restart.
 */
export function loadTeamFromDisk(cwd: string, name: string): { team: TeamFile; dir: string } | null {
  // Re-sanitize at the READ boundary (parity: the reference's readTeamFile/getTeamFilePath
  // apply sanitizeName on every access). `name` may be an untrusted value rehydrated
  // from a persisted transcript/team file via loadTeamFromSession, so route it
  // through the canonical getTeamFilePath rather than joining it verbatim. A name
  // that cannot normalize to a safe slug degrades to "no team here" instead of
  // escaping the teams root.
  const file = getTeamFilePath(cwd, name);
  if (!file) return null;
  const dir = path.dirname(file);
  const team = readTeamFile(dir);
  if (!team) return null;
  return { team, dir };
}

/** A teammate pre-declared in team_create's `members` input (post-validation). */
export interface DeclaredMember {
  name: string;
  agentType?: string;
  model?: string;
  permissionMode?: PermissionMode;
  color?: AgentColorName;
  subscriptions?: string[];
}

/**
 * Parse and validate the optional `members` roster supplied to team_create (parity:
 * the reference lets the caller name the agents that participate at creation). Each
 * entry may be a bare name string or an object `{ name, agent_type?, model?,
 * permission_mode? }`. Names are sanitized with the canonical {@link sanitizeAgentName}
 * (shared helper — no duplicate reserved-char logic); the lead name and duplicates
 * are skipped. An invalid `permission_mode` on any entry is a hard error (returned
 * in `error`) so a bad knob fails the create rather than being silently dropped.
 */
export function parseDeclaredMembers(raw: unknown): { members: DeclaredMember[]; error?: string } {
  if (raw === undefined || raw === null) return { members: [] };
  if (!Array.isArray(raw)) return { members: [], error: 'members must be an array of names or member objects.' };
  const out: DeclaredMember[] = [];
  const seen = new Set<string>([TEAM_LEAD_NAME]);
  for (const item of raw) {
    let rawName: string | undefined;
    let agentType: string | undefined;
    let model: string | undefined;
    let permRaw: string | undefined;
    let colorRaw: unknown;
    let subsRaw: unknown;
    if (typeof item === 'string') {
      rawName = item;
    } else if (item && typeof item === 'object') {
      const o = item as Record<string, unknown>;
      rawName = typeof o.name === 'string' ? o.name : undefined;
      agentType = typeof o.agent_type === 'string' ? o.agent_type : undefined;
      model = typeof o.model === 'string' ? o.model : undefined;
      permRaw = typeof o.permission_mode === 'string' ? o.permission_mode : undefined;
      colorRaw = o.color;
      subsRaw = o.subscriptions;
    }
    if (!rawName) continue;
    const name = sanitizeAgentName(rawName);
    if (!name || seen.has(name)) continue;
    let permissionMode: PermissionMode | undefined;
    if (permRaw !== undefined) {
      const valid = normalizePermissionMode(permRaw);
      if (!valid) {
        return { members: [], error: `member "${name}" has an invalid permission_mode; must be one of: ${PERMISSION_MODES.join(', ')}.` };
      }
      permissionMode = valid;
    }
    // Explicit display color (parity: the reference's per-agent `color`). An unknown or
    // 'automatic' value normalizes to undefined so the teammate falls back to the
    // round-robin palette assignment made on spawn.
    const color = parseAgentColor(colorRaw);
    // Per-member subscriptions (parity: the reference's per-member `subscriptions: string[]`
    // declaring which topics/channels a teammate receives). Only a clean array of
    // non-empty strings is kept; anything else is dropped so routing never trusts a
    // malformed list. An empty result normalizes to undefined.
    let subscriptions: string[] | undefined;
    if (Array.isArray(subsRaw)) {
      const subs = subsRaw.filter((s): s is string => typeof s === 'string' && s.length > 0);
      if (subs.length > 0) subscriptions = subs;
    }
    seen.add(name);
    out.push({ name, agentType, model, permissionMode, color, subscriptions });
  }
  return { members: out };
}

/**
 * Parse and validate the optional `team_allowed_paths` roster supplied to
 * team_create (parity: the reference's `teamAllowedPaths` config knob). Each entry
 * is either a bare path string (grants ALL tools — `toolName: '*'` — under that
 * path) or an object `{ path, tool_name? }` to scope the grant to a single tool.
 * An entry without a non-empty `path` is a hard error (returned in `error`) so a
 * malformed knob fails the create rather than being silently dropped. Returns the
 * normalized {@link TeamAllowedPath} list, or `undefined` when none were supplied.
 */
export function parseTeamAllowedPaths(raw: unknown): { paths?: TeamAllowedPath[]; error?: string } {
  if (raw === undefined || raw === null) return {};
  if (!Array.isArray(raw)) return { error: 'team_allowed_paths must be an array of path strings or { path, tool_name } objects.' };
  const out: TeamAllowedPath[] = [];
  for (const item of raw) {
    let p: string | undefined;
    let toolName = '*';
    if (typeof item === 'string') {
      p = item;
    } else if (item && typeof item === 'object') {
      const o = item as Record<string, unknown>;
      p = typeof o.path === 'string' ? o.path : undefined;
      if (typeof o.tool_name === 'string' && o.tool_name) toolName = o.tool_name;
    }
    if (!p) return { error: 'each team_allowed_paths entry must have a non-empty path.' };
    out.push({ path: p, toolName });
  }
  return { paths: out.length > 0 ? out : undefined };
}

/**
 * The subdirectory under a team's dir that holds durable, cross-worker SHARED
 * knowledge — the team scratchpad (parity: the reference's coordinator
 * `scratchpadDir`). {@link teamCreateTool} provisions it on create; teammates
 * read/write it WITHOUT a permission prompt via {@link teamScratchpadRules}. Kept
 * as the single canonical name for the dir so the create path, the path resolver,
 * and the permission-rule builder never drift.
 */
export const TEAM_SCRATCHPAD_SUBDIR = 'memory';

/**
 * The next permission mode in a fixed cycle (mirrors the reference's
 * getNextPermissionMode, used to toggle a single teammate's mode). An unknown
 * current value starts the cycle at the first mode.
 */
export function getNextPermissionMode(current: string | undefined): PermissionMode {
  const normalized = current ? normalizePermissionMode(current) : null;
  const idx = normalized ? PERMISSION_MODES.indexOf(normalized) : -1;
  return PERMISSION_MODES[(idx + 1) % PERMISSION_MODES.length];
}
