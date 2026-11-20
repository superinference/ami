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
 * Parity with the reference's entrypoint knob (`CLAUDE_CODE_ENTRYPOINT`, read by
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
 * Mirrors the reference's `getClaudeCodeUserAgent()` / `getMCPUserAgent()`
 * (`src/utils/http.ts`), which return `claude-code/${VERSION}` plus a
 * parenthesized suffix tagging the originating surface. AMI's form is
 * `ami/<version>` with a parenthesized suffix composed of, in order:
 *   1. an optional free-form `component` (which AMI subsystem — e.g.
 *      `skill-discovery`), so a specific fetch site stays identifiable;
 *   2. the originating entrypoint ({@link resolveEntrypoint}: `AMI_ENTRYPOINT`
 *      then `CLAUDE_CODE_ENTRYPOINT`), so downstream servers see which frontend
 *      drove the request (TUI vs detached CLI vs MCP vs an embedding host);
 *   3. `agent-sdk/<ver>` from `AMI_AGENT_SDK_VERSION` (or the reference's
 *      `CLAUDE_AGENT_SDK_VERSION`) when AMI is driven by an embedding SDK;
 *   4. `client-app/<id>` from `AMI_AGENT_SDK_CLIENT_APP` (or the reference's
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

// ---------------------------------------------------------------------------
// Loose, prerelease / build-metadata aware semver parsing + precedence ordering
// ---------------------------------------------------------------------------

/**
 * A version parsed into its SemVer 2.0.0 components. Prerelease identifiers are
 * split on `.`; a purely-numeric identifier becomes a `number` (compared
 * numerically per §11), anything else stays a `string`. Build metadata is kept
 * for completeness but is IGNORED in precedence.
 */
export interface ParsedSemver {
  major: number;
  minor: number;
  patch: number;
  /** Dot-separated prerelease identifiers; `[]` for a final release. */
  prerelease: Array<string | number>;
  /** Dot-separated build-metadata identifiers; `[]` when absent. */
  build: string[];
}

/** Parse a single dotted-numeric core segment: `/^\d+$/` → number, else null. */
function parseCoreSegment(seg: string | undefined, loose: boolean): number | null {
  if (seg === undefined || seg === '') return loose ? 0 : null;
  if (!/^\d+$/.test(seg)) return null;
  return parseInt(seg, 10);
}

/**
 * Loosely parse a `[v|=]MAJOR[.MINOR[.PATCH]][-prerelease][+build]` version into
 * its components, or `null` when the core is not dotted-numeric / is malformed.
 *
 * This is the `{ loose: true }` parsing option AMI previously lacked and the
 * *prerelease / build-metadata aware* half of the reference's `utils/semver.ts`
 * (which delegates to npm `semver` with `{ loose: true }`). Implemented
 * dependency-free so it stays a self-contained core building block (core must
 * not pull runtime deps). Loose tolerances: a leading `v`/`=` and surrounding
 * whitespace are stripped, and a missing minor/patch defaults to 0 (`1`, `1.4`).
 *
 * This is DELIBERATELY distinct from `skill-catalog.ts`'s {@link ../skill-catalog.compareSemver}
 * which is prerelease-AGNOSTIC (a release-floor comparator that compares only the
 * `major.minor.patch` triple); here prerelease ordering is honored per SemVer §11.
 */
export function parseSemver(raw: string, opts: { loose?: boolean } = {}): ParsedSemver | null {
  if (typeof raw !== 'string') return null;
  const loose = opts.loose ?? true;
  // Strip a loose leading `v`/`=` and any surrounding whitespace (anchored,
  // linear — no nested quantifiers, ReDoS-safe).
  let s = raw.trim().replace(/^[=v\s]+/, '');
  if (s === '') return null;

  // Split off `+build` metadata, then `-prerelease`.
  let build: string[] = [];
  const plus = s.indexOf('+');
  if (plus >= 0) {
    build = s.slice(plus + 1).split('.');
    s = s.slice(0, plus);
    if (build.some(id => id === '')) return null;
  }
  let prerelease: Array<string | number> = [];
  const dash = s.indexOf('-');
  if (dash >= 0) {
    const preRaw = s.slice(dash + 1);
    s = s.slice(0, dash);
    const ids = preRaw.split('.');
    if (ids.some(id => id === '')) return null; // e.g. "1.0.0-" / "1.0.0-a."
    prerelease = ids.map(id => (/^\d+$/.test(id) ? parseInt(id, 10) : id));
  }

  const coreSegs = s.split('.');
  if (coreSegs.length > 3) return null; // not a MAJOR.MINOR.PATCH core
  const major = parseCoreSegment(coreSegs[0], loose);
  const minor = parseCoreSegment(coreSegs[1], loose);
  const patch = parseCoreSegment(coreSegs[2], loose);
  if (major === null || minor === null || patch === null) return null;

  return { major, minor, patch, prerelease, build };
}

const ZERO_SEMVER: ParsedSemver = { major: 0, minor: 0, patch: 0, prerelease: [], build: [] };

/** Compare two prerelease identifier lists per SemVer §11 (numeric < alphanumeric,
 *  numeric compared numerically, alphanumeric lexically, more fields win on tie). */
function comparePrerelease(a: Array<string | number>, b: Array<string | number>): -1 | 0 | 1 {
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const x = a[i];
    const y = b[i];
    const xn = typeof x === 'number';
    const yn = typeof y === 'number';
    if (xn && yn) {
      if (x !== y) return x < y ? -1 : 1;
    } else if (xn) {
      return -1; // numeric identifiers have lower precedence than alphanumeric
    } else if (yn) {
      return 1;
    } else if (x !== y) {
      return x < y ? -1 : 1; // both strings — ASCII lexical
    }
  }
  if (a.length !== b.length) return a.length < b.length ? -1 : 1;
  return 0;
}

/**
 * Full SemVer 2.0.0 precedence comparison (prerelease-aware, build-metadata
 * ignored). Returns -1 / 0 / 1 for `a` < / = / > `b`. Unparseable operands
 * degrade to `0.0.0` (graceful fallback, never throws), mirroring the reference's
 * loose `order()`.
 */
export function orderSemver(a: string, b: string): -1 | 0 | 1 {
  const pa = parseSemver(a) ?? ZERO_SEMVER;
  const pb = parseSemver(b) ?? ZERO_SEMVER;
  if (pa.major !== pb.major) return pa.major < pb.major ? -1 : 1;
  if (pa.minor !== pb.minor) return pa.minor < pb.minor ? -1 : 1;
  if (pa.patch !== pb.patch) return pa.patch < pb.patch ? -1 : 1;
  const aPre = pa.prerelease.length > 0;
  const bPre = pb.prerelease.length > 0;
  if (!aPre && !bPre) return 0;
  if (!aPre) return 1; // a is a release, b is a prerelease → a is greater
  if (!bPre) return -1;
  return comparePrerelease(pa.prerelease, pb.prerelease);
}

/**
 * Whether `candidate` is strictly newer than `base` by full SemVer precedence —
 * the prerelease-aware "is there a newer release?" predicate for update-freshness
 * checks (so `1.0.0` correctly outranks `1.0.0-rc.1`, which a prerelease-agnostic
 * triple compare cannot distinguish).
 */
export function isSemverNewer(candidate: string, base: string): boolean {
  return orderSemver(candidate, base) > 0;
}
