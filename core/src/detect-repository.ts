import { getRemoteOriginUrl } from './utils/git';
import { log } from './logger';

/**
 * Structured identity of a git repository parsed from its remote URL: the clone
 * host (`github.com` or a GHE hostname, with the HTTPS port preserved where one
 * is present), the owner (org/user), and the repo name (which may contain dots,
 * e.g. `cc.kurs.web`). This is the building block repo-scoped features need to
 * construct web URLs (PR links), key repo-scoped state, or auto-detect the
 * current GitHub repo — AMI previously had no such resolver, only the PII-free
 * `getRepoRemoteHash` identity and scout's per-clone URL cache.
 *
 * Parity with Claude Code's `detectRepository.ts` (`ParsedRepository`).
 */
export type ParsedRepository = {
  host: string;
  owner: string;
  name: string;
};

/**
 * Per-cwd memo of the resolved repository, INCLUDING the negative result: a cwd
 * with no `origin` remote (or where git fails) caches `null`, so a repeated probe
 * of the same checkout never re-spawns git. Mirrors Claude Code's
 * `repositoryWithHostCache` (both success and failure are memoised).
 */
const repositoryWithHostCache = new Map<string, ParsedRepository | null>();

/**
 * Invalidate the cwd-keyed repository cache (both positive and negative entries).
 * Call after a checkout's `origin` remote changes (add/rename/remove) so the next
 * detection re-reads git instead of serving a stale (possibly negative) result.
 * Parity with Claude Code's `clearRepositoryCaches()`.
 */
export function clearRepositoryCaches(): void {
  repositoryWithHostCache.clear();
}

/**
 * Resolve the `{host, owner, name}` of the repository checked out at `cwd` (any
 * host — github.com or a GHE instance) from its `origin` remote URL, memoised
 * per cwd with a fail-soft fallback: a checkout with no remote, an unparseable
 * remote, or a git failure caches (and returns) `null` rather than throwing.
 * Callers that need to construct URLs against a specific GitHub host use this
 * variant. Parity with Claude Code's `detectCurrentRepositoryWithHost()`.
 */
export function detectCurrentRepositoryWithHost(
  cwd: string = process.cwd(),
): ParsedRepository | null {
  if (repositoryWithHostCache.has(cwd)) {
    return repositoryWithHostCache.get(cwd) ?? null;
  }

  const remoteUrl = getRemoteOriginUrl(cwd);
  log('detect-repository', `Git remote URL: ${remoteUrl ?? '(none)'}`);
  if (!remoteUrl) {
    log('detect-repository', 'No git remote URL found');
    repositoryWithHostCache.set(cwd, null);
    return null;
  }

  const parsed = parseGitRemote(remoteUrl);
  log(
    'detect-repository',
    `Parsed repository: ${parsed ? `${parsed.host}/${parsed.owner}/${parsed.name}` : '(null)'}`,
  );
  repositoryWithHostCache.set(cwd, parsed);
  return parsed;
}

/**
 * Resolve the current repository as `owner/name`, restricted to github.com
 * hosts (GHE hosts return `null` to avoid breaking downstream consumers that
 * assume a github.com repo — use {@link detectCurrentRepositoryWithHost} for GHE
 * support). Parity with Claude Code's `detectCurrentRepository()`.
 */
export function detectCurrentRepository(cwd: string = process.cwd()): string | null {
  const result = detectCurrentRepositoryWithHost(cwd);
  if (!result) return null;
  if (result.host !== 'github.com') return null;
  return `${result.owner}/${result.name}`;
}

/**
 * Synchronously return the ALREADY-CACHED github.com repository for `cwd` as
 * `owner/name`, or `null` when it has not been resolved yet or the host is not
 * github.com. Unlike {@link detectCurrentRepository} this NEVER spawns git —
 * call {@link detectCurrentRepositoryWithHost}/{@link detectCurrentRepository}
 * first to populate the cache. Parity with Claude Code's `getCachedRepository()`.
 */
export function getCachedRepository(cwd: string = process.cwd()): string | null {
  const parsed = repositoryWithHostCache.get(cwd);
  if (!parsed || parsed.host !== 'github.com') return null;
  return `${parsed.owner}/${parsed.name}`;
}

/**
 * Parse a git remote URL into `{host, owner, name}` components, accepting any
 * host. Supports:
 *   - SSH scp-style:   `git@host:owner/repo.git`
 *   - URL form:        `https://host/owner/repo.git`, `ssh://git@host/owner/repo`,
 *                      `git://host/owner/repo`, with or without a trailing `.git`
 *
 * Repo names may contain dots (e.g. `cc.kurs.web`). For URL form, a `:port`
 * suffix is preserved ONLY for http(s) hosts — SSH/git ports are not usable for
 * constructing web URLs, so they are dropped. SSH-config aliases (e.g.
 * `github.com-work`) are rejected via {@link looksLikeRealHostname}. Returns
 * `null` for anything that does not match a recognized remote shape.
 *
 * Parity with Claude Code's `parseGitRemote()`.
 */
export function parseGitRemote(input: string): ParsedRepository | null {
  const trimmed = input.trim();

  // SSH scp-style: git@host:owner/repo.git
  const sshMatch = trimmed.match(/^git@([^:]+):([^/]+)\/([^/]+?)(?:\.git)?$/);
  if (sshMatch?.[1] && sshMatch[2] && sshMatch[3]) {
    if (!looksLikeRealHostname(sshMatch[1])) return null;
    return { host: sshMatch[1], owner: sshMatch[2], name: sshMatch[3] };
  }

  // URL form: https://host/owner/repo.git, ssh://git@host/owner/repo, git://host/owner/repo.
  // Parse structurally (not with one nested-quantifier regex) to stay ReDoS-safe —
  // the same reason normalizeGitRemoteUrl strips userinfo in code.
  const schemeMatch = trimmed.match(/^(https?|ssh|git):\/\/(.+)$/);
  if (schemeMatch?.[1] && schemeMatch[2]) {
    const protocol = schemeMatch[1];
    const afterScheme = schemeMatch[2];
    const firstSlash = afterScheme.indexOf('/');
    if (firstSlash < 0) return null;
    // Authority may be "user@host[:port]"; drop the optional userinfo.
    let authority = afterScheme.slice(0, firstSlash);
    const at = authority.lastIndexOf('@');
    if (at >= 0) authority = authority.slice(at + 1);
    const hostWithPort = authority;
    const hostWithoutPort = hostWithPort.split(':')[0] ?? '';
    if (!looksLikeRealHostname(hostWithoutPort)) return null;
    // Path must be exactly owner/repo (optionally with a trailing .git).
    const segments = afterScheme.slice(firstSlash + 1).split('/');
    if (segments.length !== 2) return null;
    const owner = segments[0];
    const name = segments[1].replace(/\.git$/, '');
    if (!owner || !name) return null;
    // Only preserve the port for http(s) — SSH/git ports are not HTTPS ports and
    // cannot be used to build web URLs.
    const host =
      protocol === 'https' || protocol === 'http' ? hostWithPort : hostWithoutPort;
    return { host, owner, name };
  }

  return null;
}

/**
 * Parse a git remote URL OR a plain `owner/repo` shorthand into `owner/repo`,
 * restricted to github.com hosts (GHE URLs return `null` — use
 * {@link parseGitRemote} for GHE support). A trailing `.git` on the shorthand's
 * repo segment is stripped. Returns `null` for anything unrecognized.
 *
 * Parity with Claude Code's `parseGitHubRepository()`.
 */
export function parseGitHubRepository(input: string): string | null {
  const trimmed = input.trim();

  // Try a full remote URL first; only github.com results are returned.
  const parsed = parseGitRemote(trimmed);
  if (parsed) {
    if (parsed.host !== 'github.com') return null;
    return `${parsed.owner}/${parsed.name}`;
  }

  // Otherwise accept a plain `owner/repo` shorthand (no scheme / no userinfo).
  if (!trimmed.includes('://') && !trimmed.includes('@') && trimmed.includes('/')) {
    const parts = trimmed.split('/');
    if (parts.length === 2 && parts[0] && parts[1]) {
      const repo = parts[1].replace(/\.git$/, '');
      return `${parts[0]}/${repo}`;
    }
  }

  log('detect-repository', `Could not parse repository from: ${trimmed}`);
  return null;
}

/**
 * Whether `host` looks like a real domain rather than an SSH-config alias. A bare
 * dot check is insufficient because aliases like `github.com-work` contain a dot;
 * the last segment (the TLD) must be purely alphabetic — real TLDs (`com`, `org`,
 * `io`, `net`) never contain hyphens or digits, whereas `com-work` does.
 */
function looksLikeRealHostname(host: string): boolean {
  if (!host.includes('.')) return false;
  const lastSegment = host.split('.').pop();
  if (!lastSegment) return false;
  return /^[a-zA-Z]+$/.test(lastSegment);
}
