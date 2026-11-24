// Split from plugins.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { PLUGIN_HOST_METHOD_SPECS, canonicalizePluginCapabilities } from './plugins-plugin-capabilities';
import type { PluginCapability, PluginGateDecision, PluginGateSubject } from './plugins-plugin-capabilities';

/**
 * The deny-by-default capability gate (Orca parity: `gatePluginHostCall`). Returns
 * `granted: true` ONLY when the method is known AND either needs no capability or
 * the subject holds the one it needs. An unknown method, a null grant (disabled /
 * stale consent), or a missing capability all deny with a specific code. PURE — no
 * I/O; the host enforces the decision at the call site.
 */
export function gatePluginHostCall(subject: PluginGateSubject, method: string): PluginGateDecision {
  const spec = PLUGIN_HOST_METHOD_SPECS[method];
  if (!spec) {
    return { granted: false, code: 'unknown_method', error: `unknown host method: ${method}` };
  }
  // Unknown, disabled and stale-consent plugins must be indistinguishable from an
  // ungranted capability set (Orca parity): any capability-bearing call is refused.
  if (subject.grantedCapabilities === null) {
    return { granted: false, code: 'consent_required', error: 'plugin is not enabled with current consent' };
  }
  if (spec.capability !== null && !subject.grantedCapabilities.includes(spec.capability)) {
    return {
      granted: false,
      code: 'capability_denied',
      error: `plugin does not have the "${spec.capability}" capability`,
    };
  }
  return { granted: true };
}

/**
 * Plugin execution tiers, ordered LEAST -> MOST authority (Orca parity:
 * PLUGIN_EXECUTION_PROFILES). A higher tier may carry everything below it:
 *  - `data`          — themes/syntaxes/static assets; acts on nothing.
 *  - `declarative`   — commands/skills/capabilities; no code execution.
 *  - `hooks`         — ships hooks.json / MCP servers; runs code at lifecycle events.
 *  - `trusted-worker`— ships a Node `main` entry with full host authority.
 */
export const PLUGIN_EXECUTION_TIERS = ['data', 'declarative', 'hooks', 'trusted-worker'] as const;

export type PluginExecutionTier = (typeof PLUGIN_EXECUTION_TIERS)[number];

export function pluginTierRank(tier: PluginExecutionTier): number {
  return PLUGIN_EXECUTION_TIERS.indexOf(tier);
}

/** True when a bundle running at `tier` is allowed to carry what needs `required`
 *  (Orca parity: `pluginExecutionAllows`). */
export function pluginTierAllows(tier: PluginExecutionTier, required: PluginExecutionTier): boolean {
  return pluginTierRank(tier) >= pluginTierRank(required);
}

/** What a bundle declares, for deriving its least execution tier. */
export interface PluginExecutionSubject {
  capabilities?: readonly unknown[];
  hasHooks?: boolean;
  hasMcpServers?: boolean;
  hasCommands?: boolean;
  hasSkills?: boolean;
  hasMain?: boolean;
}

/**
 * The LEAST execution tier that can run what a bundle declares (Orca parity:
 * `derivePluginExecutionProfile`). A Node `main` entry => `trusted-worker`; hooks or
 * MCP servers (code run at lifecycle events) => `hooks`; declared capabilities or
 * commands/skills => `declarative`; otherwise `data`. Risk-proportional: the tier is
 * the floor of authority the bundle needs, never more.
 */
export function derivePluginExecutionTier(subject: PluginExecutionSubject): PluginExecutionTier {
  if (subject.hasMain) return 'trusted-worker';
  if (subject.hasHooks || subject.hasMcpServers) return 'hooks';
  if (canonicalizePluginCapabilities(subject.capabilities).length > 0) return 'declarative';
  if (subject.hasCommands || subject.hasSkills) return 'declarative';
  return 'data';
}

/** A bundle's granted reach: its capability set + execution tier. The unit a
 *  consent fingerprint is computed over and an update's growth is measured against. */
export interface PluginAccessProfile {
  capabilities: PluginCapability[];
  tier: PluginExecutionTier;
}

/**
 * A deterministic fingerprint of a bundle's granted reach (Orca parity: the consent
 * fingerprint / `capabilityHash`). Two bundles with the same capabilities + tier
 * produce the same fingerprint regardless of capability order, so a content-only
 * update (same reach) re-enables with NO prompt; an update that widens reach changes
 * the fingerprint and MUST re-prompt. SHA-256 over the canonicalized profile.
 */
export function computePluginConsentFingerprint(profile: PluginAccessProfile): string {
  const canonical = JSON.stringify({
    capabilities: canonicalizePluginCapabilities([...profile.capabilities]),
    tier: profile.tier,
  });
  return crypto.createHash('sha256').update(canonical).digest('hex');
}

/** What an update asks for BEYOND the installed version. Empty capabilities + null
 *  tier means the update keeps its existing approval (Orca parity:
 *  `PluginUpdateAccessGrowth`). */
export interface PluginAccessGrowth {
  capabilities: PluginCapability[];
  /** The new tier, only when it is strictly above the installed one; else null. */
  tier: PluginExecutionTier | null;
}

/**
 * Compute the access an update would GAIN over the installed version (Orca parity:
 * `pluginUpdateAccessGrowth`): capabilities not already held, and the new tier when
 * it exceeds the installed one. A same-or-narrower update yields empty growth.
 */
export function pluginUpdateAccessGrowth(
  installed: PluginAccessProfile,
  next: PluginAccessProfile,
): PluginAccessGrowth {
  const held = new Set(canonicalizePluginCapabilities([...installed.capabilities]));
  const capabilities = canonicalizePluginCapabilities([...next.capabilities]).filter((c) => !held.has(c));
  const tier = pluginTierAllows(installed.tier, next.tier) ? null : next.tier;
  return { capabilities, tier };
}

/**
 * Whether an update WIDENS a bundle's reach and therefore must re-prompt for
 * consent (Orca parity: `pluginUpdateGrowsAccess`). True when it gains any
 * capability or moves to a higher execution tier; false for a content-only update.
 */
export function pluginUpdateGrowsAccess(installed: PluginAccessProfile, next: PluginAccessProfile): boolean {
  const g = pluginUpdateAccessGrowth(installed, next);
  return g.capabilities.length > 0 || g.tier !== null;
}

// ===========================================================================
// Plugin supply-chain integrity — deterministic content tree-hash, a hash
// re-verify gate, a stricter clone-URL allowlist, a kill-list, and a lockfile
// pinning source+commit+treehash+consent (Orca parity: main/plugins/artifacts/
// plugin-content-integrity.ts `verifyNow`, installation/plugin-install-lockfile.ts
// `isAllowedPluginGitUrl`, catalog/plugin-kill-list.ts). AMI git-clones user
// sources and never re-checked the on-disk bytes before running hooks; these PURE
// validators let a consumer hash-address an install, RE-hash it before running any
// hook/MCP command (catching post-install tampering / TOCTOU), reject a dangerous
// clone URL, revoke a known-bad bundle, and pin an install to a reviewed state.
// ===========================================================================

/** Matches a plugin content tree-hash: a legacy 128-bit SHA-256 prefix (32 hex) or
 *  the full digest (64 hex). (Orca parity: PLUGIN_CONTENT_HASH_PATTERN.) */
export const PLUGIN_CONTENT_HASH_PATTERN = /^(?:[0-9a-f]{32}|[0-9a-f]{64})$/;

/** Matches a git object id: SHA-1 (40 hex) or SHA-256 (64 hex). (Orca parity.) */
export const PLUGIN_COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

/** Options for {@link hashPluginTree}: base-names to skip entirely so a consumer's
 *  own state sidecars (and `.git`) never perturb the content digest. */
export interface PluginTreeHashOptions {
  ignore?: readonly string[];
}

/** Base-names {@link hashPluginTree} skips by default: version-control metadata. A
 *  consumer that writes state sidecars into the bundle passes its own superset. */
export const DEFAULT_PLUGIN_TREE_HASH_IGNORE: readonly string[] = ['.git'];

/**
 * A DETERMINISTIC SHA-256 over a plugin's file tree (Orca parity: `hashPluginTree`).
 * Walks the tree in a stable (name-sorted) order independent of filesystem readdir
 * order, folding each entry's RELATIVE path, type, (for files) permission bits +
 * byte length + content, and (for symlinks) the link TARGET string — symlinks are
 * recorded, NEVER followed, so a swapped-in link can't make two different trees hash
 * alike nor pull in out-of-tree bytes. Entries whose base-name is in `ignore`
 * (default `.git`) are skipped. Same bytes => same hash; any change flips it.
 */
export function hashPluginTree(rootDir: string, opts: PluginTreeHashOptions = {}): string {
  const ignore = new Set(opts.ignore ?? DEFAULT_PLUGIN_TREE_HASH_IGNORE);
  const hash = crypto.createHash('sha256');
  const root = path.resolve(rootDir);
  const walk = (dir: string, rel: string): void => {
    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    entries.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    for (const e of entries) {
      if (ignore.has(e.name)) continue;
      const relPath = rel ? `${rel}/${e.name}` : e.name;
      const full = path.join(dir, e.name);
      if (e.isSymbolicLink()) {
        let target = '';
        try {
          target = fs.readlinkSync(full);
        } catch {
          /* unreadable link — hash an empty target */
        }
        hash.update(`L\0${relPath}\0${target}\0`);
      } else if (e.isDirectory()) {
        hash.update(`D\0${relPath}\0`);
        walk(full, relPath);
      } else if (e.isFile()) {
        let data: Buffer;
        try {
          data = fs.readFileSync(full);
        } catch {
          data = Buffer.alloc(0);
        }
        let mode = 0;
        try {
          mode = fs.statSync(full).mode & 0o777;
        } catch {
          /* unreadable — mode 0 */
        }
        hash.update(`F\0${relPath}\0${mode.toString(8)}\0${data.length}\0`);
        hash.update(data);
      }
    }
  };
  walk(root, '');
  return hash.digest('hex');
}

/** True when a freshly-computed tree hash matches an expected one, accepting a
 *  legacy 32-hex prefix against a full 64-hex digest (Orca parity:
 *  `pluginTreeHashMatches`). */
export function pluginTreeHashMatches(actual: string, expected: string): boolean {
  return actual === expected || (expected.length === 32 && actual.startsWith(expected));
}

/** The result of a content-integrity re-verify. */
export type PluginIntegrityResult = { ok: true } | { ok: false; error: string };

/**
 * RE-hash a plugin's on-disk tree and compare it to an expected content hash (Orca
 * parity: `verifyHashAddressedPluginContent` / `verifyNow`). The intended use is an
 * UNSHARED check run immediately BEFORE executing a plugin's hook/MCP command, so a
 * post-install edit or a TOCTOU swap of an approved tree is caught at run time, not
 * trusted from install time. Returns `ok: false` with a descriptive error on a
 * malformed expected hash or a mismatch.
 */
export function verifyPluginTreeHash(
  rootDir: string,
  expected: string,
  opts?: PluginTreeHashOptions,
): PluginIntegrityResult {
  if (!PLUGIN_CONTENT_HASH_PATTERN.test(expected)) {
    return { ok: false, error: `invalid expected content hash: ${expected}` };
  }
  const actual = hashPluginTree(rootDir, opts);
  if (!pluginTreeHashMatches(actual, expected)) {
    return { ok: false, error: `content hash mismatch (expected ${expected}, got ${actual})` };
  }
  return { ok: true };
}

/** Shared SSH/URL authority safety (Orca parity: `isSafeGitAuthority`): ssh reads a
 *  leading '-' as an option (`-oProxyCommand=...`), and `..` is never a real host. */
export function isSafePluginCloneAuthority(user: string, host: string): boolean {
  return !user.startsWith('-') && !host.startsWith('-') && !host.includes('..');
}

/** Largest kill-list we will accept (hostile-count cap, Orca parity). */
export const PLUGIN_KILL_LIST_ENTRY_LIMIT = 4_096;

/** One revocation: the plugin/skill key to revoke, a reason, and an optional HTTPS
 *  advisory URL (Orca parity: `PluginKillListEntry`). */
export interface PluginKillListEntry {
  key: string;
  reason: string;
  advisoryUrl?: string;
}

/** A parsed kill-list: a versioned, timestamped set of {@link PluginKillListEntry}s. */
export interface PluginKillList {
  version: 1;
  generatedAt: string;
  entries: PluginKillListEntry[];
}

/** The revocation for `key`, or null when it is not on the kill-list (Orca parity:
 *  `findKilledPlugin`). */
export function findKilledPlugin(killList: PluginKillList, key: string): PluginKillListEntry | null {
  return killList.entries.find((e) => e.key === key) ?? null;
}

/**
 * One lockfile record pinning an installed bundle to a reviewed state (Orca parity:
 * `pluginLockEntrySchema`): its source, the exact commit it resolved to, the content
 * tree-hash of the installed bytes, and the consent fingerprint that was approved —
 * so a later install/update whose commit or hash DIFFERS is visible, and a reach
 * change (fingerprint) can be re-prompted.
 */
export interface PluginLockEntry {
  pluginKey: string;
  version: string;
  /** The install source label (git URL, local path, archive URL). */
  source: string;
  /** The commit the git source resolved to; null for a local/archive source. */
  resolvedCommit: string | null;
  /** Deterministic {@link hashPluginTree} of the installed tree. */
  contentHash: string;
  /** The {@link computePluginConsentFingerprint} approved at install. */
  consentFingerprint: string;
  installedAt: number;
}

/** The lockfile: version-stamped, keyed by plugin key (Orca parity: `PluginLockfile`). */
export interface PluginLockfile {
  version: 1;
  plugins: Record<string, PluginLockEntry>;
}

/** A fresh, empty lockfile. */
export function emptyPluginLockfile(): PluginLockfile {
  return { version: 1, plugins: {} };
}

export function parsePluginLockEntry(key: string, val: unknown): PluginLockEntry | null {
  if (!val || typeof val !== 'object' || Array.isArray(val)) return null;
  const o = val as Record<string, unknown>;
  // Key integrity: the record's own pluginKey must match the map key (Orca parity).
  const pluginKey = typeof o.pluginKey === 'string' ? o.pluginKey : key;
  if (pluginKey !== key) return null;
  const contentHash = typeof o.contentHash === 'string' ? o.contentHash : '';
  if (!PLUGIN_CONTENT_HASH_PATTERN.test(contentHash)) return null;
  const consentFingerprint =
    typeof o.consentFingerprint === 'string' && o.consentFingerprint.trim() !== '' ? o.consentFingerprint.trim() : '';
  if (!consentFingerprint) return null;
  const resolvedCommit =
    typeof o.resolvedCommit === 'string' && PLUGIN_COMMIT_PATTERN.test(o.resolvedCommit) ? o.resolvedCommit : null;
  const version = typeof o.version === 'string' && o.version.trim() !== '' ? o.version : '0.0.0';
  const source = typeof o.source === 'string' ? o.source : '';
  const installedAt =
    typeof o.installedAt === 'number' && Number.isFinite(o.installedAt) && o.installedAt >= 0 ? o.installedAt : 0;
  return { pluginKey, version, source, resolvedCommit, contentHash, consentFingerprint, installedAt };
}

/**
 * Parse a raw lockfile value (Orca parity: `parsePluginLockfile`). A corrupt
 * lockfile must not block installs — a bad envelope OR a bad entry yields an empty
 * lockfile / drops just that entry rather than throwing, since the hash-addressed
 * verification independently anchors integrity. Only entries whose own `pluginKey`
 * matches their map key, with a valid content hash + consent fingerprint, survive.
 */
export function parsePluginLockfile(raw: unknown): PluginLockfile {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return emptyPluginLockfile();
  const o = raw as Record<string, unknown>;
  if (o.version !== 1 || !o.plugins || typeof o.plugins !== 'object' || Array.isArray(o.plugins)) {
    return emptyPluginLockfile();
  }
  const plugins: Record<string, PluginLockEntry> = {};
  for (const [key, val] of Object.entries(o.plugins as Record<string, unknown>)) {
    const entry = parsePluginLockEntry(key, val);
    if (entry) plugins[key] = entry;
  }
  return { version: 1, plugins };
}

/** Insert/replace a lock entry, keyed by its pluginKey (Orca parity: `upsertPluginLock`). */
export function upsertPluginLock(lock: PluginLockfile, entry: PluginLockEntry): PluginLockfile {
  return { version: 1, plugins: { ...lock.plugins, [entry.pluginKey]: entry } };
}

/** Remove a lock entry by plugin key (Orca parity: `removePluginLock`). */
export function removePluginLock(lock: PluginLockfile, pluginKey: string): PluginLockfile {
  const plugins = { ...lock.plugins };
  delete plugins[pluginKey];
  return { version: 1, plugins };
}
