// Split from analytics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { isEnvTruthy } from './privacy-level';
import { getFileExtensionForAnalytics } from './analytics-truncate-tool-input-value';
import type { ProcessMetrics, ProcessMetricsDeps } from './analytics-telemetry-content-dedup';

/**
 * Samples process memory + CPU usage and derives a CPU% figure from the delta since
 * the PREVIOUS sample — the first sample has no `cpuPercent` (no baseline yet). State
 * is per-instance (not module-global) so independent trackers don't corrupt each
 * other's deltas. Best-effort: a reader that throws yields `undefined`. Mirrors
 * the reference's buildProcessMetrics (metadata.ts) CPU%-delta pattern.
 */
export class ProcessMetricsSampler {
  private _prevCpu: NodeJS.CpuUsage | null = null;
  private _prevWallMs: number | null = null;
  private readonly _mem: () => NodeJS.MemoryUsage;
  private readonly _cpu: () => NodeJS.CpuUsage;
  private readonly _uptime: () => number;
  private readonly _now: () => number;
  private readonly _constrained: () => number | undefined;

  constructor(deps: ProcessMetricsDeps = {}) {
    this._mem = deps.memoryUsage ?? (() => process.memoryUsage());
    this._cpu = deps.cpuUsage ?? (() => process.cpuUsage());
    this._uptime = deps.uptime ?? (() => process.uptime());
    this._now = deps.now ?? Date.now;
    // `process.constrainedMemory` is a Node 20+ (and Bun) API; absent on older/other
    // runtimes, so bind it only when present and default to the no-constraint reader.
    this._constrained = deps.constrainedMemory
      ?? (typeof process.constrainedMemory === 'function'
        ? () => process.constrainedMemory!()
        : () => undefined);
  }

  sample(): ProcessMetrics | undefined {
    try {
      const mem = this._mem();
      const cpu = this._cpu();
      const now = this._now();
      let cpuPercent: number | undefined;
      if (this._prevCpu && this._prevWallMs !== null) {
        const wallDeltaMs = now - this._prevWallMs;
        if (wallDeltaMs > 0) {
          const deltaUs = (cpu.user - this._prevCpu.user) + (cpu.system - this._prevCpu.system);
          cpuPercent = (deltaUs / (wallDeltaMs * 1000)) * 100;
        }
      }
      this._prevCpu = cpu;
      this._prevWallMs = now;
      // cgroup/container memory ceiling: 0 (unconstrained), undefined (unsupported), or a
      // non-finite reading are all treated as "no ceiling" — only a real positive limit is
      // surfaced, so an unconstrained host never reports a meaningless 0-byte ceiling.
      let constrained: number | undefined;
      try {
        const c = this._constrained();
        if (typeof c === 'number' && Number.isFinite(c) && c > 0) constrained = c;
      } catch { /* reader unsupported — leave undefined */ }
      return {
        uptime: this._uptime(),
        rss: mem.rss,
        heapTotal: mem.heapTotal,
        heapUsed: mem.heapUsed,
        external: mem.external,
        arrayBuffers: mem.arrayBuffers,
        cpuUserUs: cpu.user,
        cpuSystemUs: cpu.system,
        ...(cpuPercent !== undefined ? { cpuPercent } : {}),
        ...(constrained !== undefined ? { constrainedMemory: constrained } : {}),
      };
    } catch {
      return undefined;
    }
  }
}

export const CI_CONTEXT_VARS: Record<string, string> = {
  'ci.actor': 'GITHUB_ACTOR',
  'ci.actor_id': 'GITHUB_ACTOR_ID',
  'ci.repository': 'GITHUB_REPOSITORY',
  'ci.repository_id': 'GITHUB_REPOSITORY_ID',
  'ci.repository_owner': 'GITHUB_REPOSITORY_OWNER',
  'ci.repository_owner_id': 'GITHUB_REPOSITORY_OWNER_ID',
};

/**
 * Capture CI context when running under GitHub Actions (gated on GITHUB_ACTIONS):
 * actor / repository / owner (+ their ids). Returns null outside CI so the
 * dimensions are attached only when meaningful. Mirrors the reference's
 * githubActionsMetadata capture; provider-neutral (no Anthropic coupling).
 */
export function getCiContext(env: NodeJS.ProcessEnv = process.env): Record<string, string> | null {
  if (!isEnvTruthy(env.GITHUB_ACTIONS)) return null;
  const out: Record<string, string> = { ci: 'true' };
  for (const [attr, envVar] of Object.entries(CI_CONTEXT_VARS)) {
    const v = env[envVar];
    if (v) out[attr] = v;
  }
  return out;
}

/**
 * Whether the process is running in a Continuous-Integration environment — the canonical
 * `CI` env knob read through {@link isEnvTruthy} so `1`/`true`/`yes` all count and an
 * explicit `0`/`false` does not (parity: the reference's `env.isCI = isEnvTruthy(process.env.CI)`).
 * AGENTS.md's build scripts already *assume* CI-awareness (`CI=true skips local-only steps`);
 * this makes the flag a first-class, testable read instead of scattered ad-hoc `process.env.CI`
 * checks. `env` is injectable so callers/tests pass a fixed environment.
 */
export function isCI(env: NodeJS.ProcessEnv = process.env): boolean {
  return isEnvTruthy(env.CI);
}

/** Injectable filesystem probes for {@link detectDeploymentEnvironment} (deterministic tests). */
export interface DeploymentEnvDeps {
  /** Reads a hypervisor/identity file (default `fs.readFileSync`); used for AWS EC2 detection. */
  readFileSync?: (p: string) => string;
  /** Existence probe (default `fs.existsSync`); used for the `/.dockerenv` container marker. */
  existsSync?: (p: string) => boolean;
  /** Host platform, for the `unknown-<platform>` fallback (default `process.platform`). */
  platform?: NodeJS.Platform;
}

/**
 * Classify the runtime deployment environment from its env knobs + container markers,
 * returning a canonical lower-case name (parity: the reference's `detectDeploymentEnvironment`,
 * utils/env.ts). The precedence is fixed — most specific first — so e.g. GitHub Actions is
 * reported as `github-actions` rather than the generic `ci`, and a cloud dev container
 * (`codespaces`/`gitpod`/`replit`) wins over the orchestration/`docker` fallback:
 *  1. cloud dev environments (codespaces, gitpod, replit, glitch)
 *  2. cloud platforms (vercel, railway, render, netlify, heroku, fly.io, cloudflare-pages,
 *     deno-deploy, aws-lambda/fargate/ecs/ec2, gcp-cloud-run/gcp, azure-*, digitalocean,
 *     huggingface-spaces)
 *  3. CI/CD platforms (github-actions, gitlab-ci, circleci, buildkite, generic `ci`)
 *  4. container orchestration (kubernetes, docker)
 *  5. `unknown-<platform>` else `unknown`.
 * Pure + injectable (`env`, fs probes, platform) so it is deterministic in tests, and
 * provider-neutral (no vendor coupling). Never throws — fs probes are isolated.
 */
export function detectDeploymentEnvironment(
  env: NodeJS.ProcessEnv = process.env,
  deps: DeploymentEnvDeps = {},
): string {
  const read = deps.readFileSync ?? ((p: string) => fs.readFileSync(p, { encoding: 'utf8' }));
  const exists = deps.existsSync ?? ((p: string) => fs.existsSync(p));
  const platform = deps.platform ?? process.platform;

  // 1. Cloud development environments.
  if (isEnvTruthy(env.CODESPACES)) return 'codespaces';
  if (env.GITPOD_WORKSPACE_ID) return 'gitpod';
  if (env.REPL_ID || env.REPL_SLUG) return 'replit';
  if (env.PROJECT_DOMAIN) return 'glitch';

  // 2. Cloud platforms.
  if (isEnvTruthy(env.VERCEL)) return 'vercel';
  if (env.RAILWAY_ENVIRONMENT_NAME || env.RAILWAY_SERVICE_NAME) return 'railway';
  if (isEnvTruthy(env.RENDER)) return 'render';
  if (isEnvTruthy(env.NETLIFY)) return 'netlify';
  if (env.DYNO) return 'heroku';
  if (env.FLY_APP_NAME || env.FLY_MACHINE_ID) return 'fly.io';
  if (isEnvTruthy(env.CF_PAGES)) return 'cloudflare-pages';
  if (env.DENO_DEPLOYMENT_ID) return 'deno-deploy';
  if (env.AWS_LAMBDA_FUNCTION_NAME) return 'aws-lambda';
  if (env.AWS_EXECUTION_ENV === 'AWS_ECS_FARGATE') return 'aws-fargate';
  if (env.AWS_EXECUTION_ENV === 'AWS_ECS_EC2') return 'aws-ecs';
  try {
    if (read('/sys/hypervisor/uuid').trim().toLowerCase().startsWith('ec2')) return 'aws-ec2';
  } catch { /* ENOENT off EC2 — ignore */ }
  if (env.K_SERVICE) return 'gcp-cloud-run';
  if (env.GOOGLE_CLOUD_PROJECT) return 'gcp';
  if (env.WEBSITE_SITE_NAME || env.WEBSITE_SKU) return 'azure-app-service';
  if (env.AZURE_FUNCTIONS_ENVIRONMENT) return 'azure-functions';
  if (env.APP_URL?.includes('ondigitalocean.app')) return 'digitalocean-app-platform';
  if (env.SPACE_CREATOR_USER_ID) return 'huggingface-spaces';

  // 3. CI/CD platforms (specific before the generic `ci`).
  if (isEnvTruthy(env.GITHUB_ACTIONS)) return 'github-actions';
  if (isEnvTruthy(env.GITLAB_CI)) return 'gitlab-ci';
  if (env.CIRCLECI) return 'circleci';
  if (env.BUILDKITE) return 'buildkite';
  if (isEnvTruthy(env.CI)) return 'ci';

  // 4. Container orchestration.
  if (env.KUBERNETES_SERVICE_HOST) return 'kubernetes';
  try {
    if (exists('/.dockerenv')) return 'docker';
  } catch { /* probe failed — ignore */ }

  // 5. Platform-specific fallback.
  if (platform === 'darwin') return 'unknown-darwin';
  if (platform === 'linux') return 'unknown-linux';
  if (platform === 'win32') return 'unknown-win32';
  return 'unknown';
}

/** Filename holding the stable per-install telemetry id, co-located with the log dir. */
export const USER_ID_FILE = 'telemetry-user-id';

/**
 * Read (or lazily create + persist) a stable, locally-generated install id so a
 * self-hosted collector can correlate activity across sessions without AMI ever
 * sending any real identity. When `dir` is given the id is persisted to
 * `<dir>/telemetry-user-id` (0o600); when it is absent, or persistence fails, an
 * ephemeral per-process id is returned. Mirrors the reference's getOrCreateUserID.
 */
export function getOrCreateUserId(dir?: string): string {
  const genId = () => crypto.randomBytes(32).toString('hex');
  if (!dir) return genId();
  const file = path.join(dir, USER_ID_FILE);
  try {
    const existing = fs.readFileSync(file, 'utf8').trim();
    if (/^[0-9a-f]{64}$/.test(existing)) return existing;
  } catch { /* not created yet */ }
  const id = genId();
  try {
    fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
    fs.writeFileSync(file, id + '\n', { mode: 0o600 });
  } catch { /* best effort — return the generated id even if it could not be saved */ }
  return id;
}

/** Fixed number of privacy-preserving user buckets (cardinality ceiling). */
export const NUM_USER_BUCKETS = 30;

/**
 * Map a user id into one of `numBuckets` buckets via SHA-256 so unique users can
 * be estimated by counting distinct buckets WITHOUT exporting the raw id — privacy
 * plus bounded cardinality (mirrors the reference's getUserBucket, NUM_USER_BUCKETS=30).
 */
export function getUserBucket(userId: string, numBuckets: number = NUM_USER_BUCKETS): number {
  const n = Number.isFinite(numBuckets) && numBuckets >= 1 ? Math.floor(numBuckets) : NUM_USER_BUCKETS;
  const hash = crypto.createHash('sha256').update(userId).digest('hex');
  return parseInt(hash.slice(0, 8), 16) % n;
}

/**
 * Derive a low-cardinality `Nxx` range from an HTTP status (100..599), else
 * undefined — so a numeric status is exported as a bounded dimension instead of
 * one series per code (mirrors the reference's http_status_range).
 */
export function bucketHttpStatus(status: number | string | undefined | null): string | undefined {
  if (status === undefined || status === null || status === '') return undefined;
  const n = typeof status === 'number' ? status : Number(status);
  if (!Number.isFinite(n) || n < 100 || n > 599) return undefined;
  return `${Math.floor(n / 100)}xx`;
}

/**
 * When event data carries a numeric HTTP `status`, rename it to `http_status` and
 * add a low-cardinality `http_status_range` (`Nxx`) before export — avoiding a
 * high-cardinality raw-status dimension. A non-HTTP `status` (e.g. 'ok') is left
 * untouched. Returns the original object when no transform applies.
 */
export function deriveHttpStatusFields(data: Record<string, unknown>): Record<string, unknown> {
  const range = bucketHttpStatus(data.status as number | string | undefined);
  if (range === undefined) return data;
  const { status, ...rest } = data;
  return { ...rest, http_status: String(status), http_status_range: range };
}

// ---------------------------------------------------------------------------
// Attribute cardinality-reduction normalization (the reference datadog.ts parity):
// before an event leaves the machine, high-cardinality identifiers are collapsed
// to a bounded vocabulary so a self-hosted collector's tag/series count stays
// finite (mirrors datadog.ts L196-217 model→known-or-'other' + dev-version trunc).
// ---------------------------------------------------------------------------

/**
 * Low-cardinality model FAMILY vocabulary kept verbatim in analytics; any model id
 * not matching one of these collapses to 'other' (mirrors the reference's model→known-cost
 * -set-or-'other' reduction). Provider-neutral — no vendor coupling.
 */
export const KNOWN_ANALYTICS_MODEL_FAMILIES = [
  'opus', 'sonnet', 'haiku', 'gpt', 'o1', 'o3', 'o4',
  'gemini', 'grok', 'llama', 'mistral', 'deepseek', 'qwen',
] as const;

/**
 * Collapse a raw model id to its low-cardinality FAMILY (`opus`, `gpt`, …) or
 * 'other' when it matches no known family — a bounded telemetry dimension that
 * can't explode a collector's series count as new model ids ship. A trailing
 * Anthropic `[1m]` long-context marker is stripped first (the reference parity). Empty
 * input passes through unchanged. Match is longest-family-first so `gpt-4o` does
 * not shadow `o4`/`o1`/`o3` when those tokens appear.
 */
export function normalizeModelForAnalytics(model: string): string {
  if (!model) return model;
  const lc = model.toLowerCase().replace(/\[1m\]$/i, '');
  // Prefer the longest family token so multi-char families win over 2-char ones.
  const ordered = [...KNOWN_ANALYTICS_MODEL_FAMILIES].sort((a, b) => b.length - a.length);
  for (const fam of ordered) {
    if (lc.includes(fam)) return fam;
  }
  return 'other';
}

/** Matches a `-dev.YYYYMMDD.tHHMMSS.sha<hex>` suffix whose timestamp+sha is cardinality noise. */
export const DEV_VERSION_RE = /^(\d+\.\d+\.\d+-dev\.\d{8})\.t\d+\.sha[a-f0-9]+$/i;

/**
 * Truncate a dev build version to `base-dev.YYYYMMDD`, dropping the per-build
 * timestamp + commit sha that would otherwise make every build a unique tag — e.g.
 * `2.0.53-dev.20251124.t173302.sha526cc6a` → `2.0.53-dev.20251124`. A release
 * version (or any string not matching the dev pattern) is returned unchanged.
 * Mirrors the reference's datadog.ts version-truncation cardinality reduction.
 */
export function truncateDevVersion(version: string): string {
  return version.replace(DEV_VERSION_RE, '$1');
}

/**
 * Static file-extension → programming-language map for analytics segmentation.
 * Bounded + provider-neutral (no highlight.js dependency); an unmapped/absent
 * extension yields 'unknown' (matching the reference's getLanguageName fallback), keeping
 * the `file_language` dimension's cardinality finite.
 */
export const EXTENSION_LANGUAGE: Record<string, string> = {
  ts: 'TypeScript', tsx: 'TypeScript', mts: 'TypeScript', cts: 'TypeScript',
  js: 'JavaScript', jsx: 'JavaScript', mjs: 'JavaScript', cjs: 'JavaScript',
  py: 'Python', rb: 'Ruby', go: 'Go', rs: 'Rust', java: 'Java',
  kt: 'Kotlin', kts: 'Kotlin', swift: 'Swift', scala: 'Scala',
  c: 'C', h: 'C', cc: 'C++', cpp: 'C++', cxx: 'C++', hpp: 'C++', hh: 'C++',
  cs: 'C#', php: 'PHP', pl: 'Perl', pm: 'Perl', lua: 'Lua', r: 'R',
  sh: 'Shell', bash: 'Shell', zsh: 'Shell', fish: 'Shell',
  html: 'HTML', htm: 'HTML', css: 'CSS', scss: 'SCSS', sass: 'Sass', less: 'Less',
  json: 'JSON', yaml: 'YAML', yml: 'YAML', toml: 'TOML', xml: 'XML',
  md: 'Markdown', markdown: 'Markdown', sql: 'SQL',
  dart: 'Dart', ex: 'Elixir', exs: 'Elixir', erl: 'Erlang', clj: 'Clojure',
  hs: 'Haskell', ml: 'OCaml', fs: 'F#', vue: 'Vue', svelte: 'Svelte',
  ipynb: 'Jupyter',
};

/**
 * Resolve the programming language of a file for analytics from its extension,
 * returning 'unknown' when there is no (mapped) extension — a bounded dimension that
 * never leaks the raw path. Reuses {@link getFileExtensionForAnalytics} so the same
 * extension-parsing rules apply. Mirrors the reference's getLanguageName (telemetry use).
 */
export function getLanguageForAnalytics(filePath: string): string {
  const ext = getFileExtensionForAnalytics(filePath);
  if (!ext) return 'unknown';
  return EXTENSION_LANGUAGE[ext] ?? 'unknown';
}

// ---------------------------------------------------------------------------
// Structured file-operation analytics event (the reference fileOperationAnalytics.ts
// parity): a typed read/write/edit record carrying an operation + tool enum, a
// privacy-preserving truncated path hash, and (size-guarded) a full content hash,
// so file activity becomes a queryable structured analytics record without the raw
// path or content ever leaving the machine. Reuses the existing {@link hashFilePath}
// / {@link hashFileContent} (+ {@link MAX_CONTENT_HASH_SIZE} guard) — no duplicate hashing.
// ---------------------------------------------------------------------------

/** The file operation being recorded. */
export const FILE_OPERATION_KINDS = ['read', 'write', 'edit'] as const;

export type FileOperationKind = (typeof FILE_OPERATION_KINDS)[number];

/** AMI canonical file-tool names (vs the reference's FileReadTool/FileWriteTool/FileEditTool). */
export const FILE_OPERATION_TOOLS = ['file_read', 'file_write', 'file_edit'] as const;

export type FileOperationTool = (typeof FILE_OPERATION_TOOLS)[number];

/** Inputs for a structured file-operation analytics event. */
export interface FileOperationParams {
  operation: FileOperationKind;
  tool: FileOperationTool;
  /** Raw path — hashed (never logged verbatim). */
  filePath: string;
  /** File content — hashed only when present AND within {@link MAX_CONTENT_HASH_SIZE}. */
  content?: string;
  /** Distinguishes a newly-created file from an updated one (write/edit only). */
  type?: 'create' | 'update';
}

// ---------------------------------------------------------------------------
// Local git commit/PR operation classification (the reference gitOperationTracking.ts
// parity): detect `git commit` (+ --amend), `git push`, `gh pr <action>`,
// `glab mr create`, and curl-based PR creation in a raw command string (succeeded
// only, exitCode===0) so the usage view can count local commit/PR activity. Pure
// text matching on the raw argv — identical for any shell.
// ---------------------------------------------------------------------------

/** The set of git/PR operations a single command can be classified as. */
export const GIT_OPERATION_NAMES = ['commit', 'commit_amend', 'push', 'pr_create', 'pr_edit', 'pr_merge', 'pr_comment', 'pr_close', 'pr_ready'] as const;

export type GitOperationName = (typeof GIT_OPERATION_NAMES)[number];

/** Result of classifying one (successful) command for git/PR usage counters. */
export interface GitOperationClassification {
  /** All distinct operations matched (in a stable order). */
  operations: GitOperationName[];
  /** Amount to add to the local commit counter (0 or 1). */
  commitDelta: number;
  /** Amount to add to the local PR counter (one per pr-create source matched). */
  prDelta: number;
}

/**
 * Build a regex matching `git <subcmd>` while tolerating git's global options
 * between `git` and the subcommand (e.g. `git -c commit.gpgsign=false commit`,
 * `git -C path push`, `git --git-dir=path commit`). Mirrors the reference's gitCmdRe.
 */
export function gitSubcmdRe(subcmd: string, suffix = ''): RegExp {
  return new RegExp(`\\bgit(?:\\s+-[cC]\\s+\\S+|\\s+--\\S+=\\S+)*\\s+${subcmd}\\b${suffix}`);
}

export const GH_PR_ACTIONS: readonly { re: RegExp; op: GitOperationName; isCreate: boolean }[] = [
  { re: /\bgh\s+pr\s+create\b/, op: 'pr_create', isCreate: true },
  { re: /\bgh\s+pr\s+edit\b/, op: 'pr_edit', isCreate: false },
  { re: /\bgh\s+pr\s+merge\b/, op: 'pr_merge', isCreate: false },
  { re: /\bgh\s+pr\s+comment\b/, op: 'pr_comment', isCreate: false },
  { re: /\bgh\s+pr\s+close\b/, op: 'pr_close', isCreate: false },
  { re: /\bgh\s+pr\s+ready\b/, op: 'pr_ready', isCreate: false },
];

/** curl with an explicit or implicit (`-d`) POST — a PR-create candidate. */
export const CURL_POST_RE = /\bcurl\b/;

export const CURL_EXPLICIT_POST_RE = /-X\s*POST\b|--request\s*=?\s*POST\b|\s-d\s/i;
