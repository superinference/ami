// Split from analytics.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as os from 'os';
import * as crypto from 'crypto';
import { detectWslVersion } from './tools/bash-sandbox';
import { sleep } from './utils/sleep';
import { isEnvTruthy } from './privacy-level';

// ---------------------------------------------------------------------------
// Hash-based content deduplication (the reference betaSessionTracing.ts parity):
// large/repeated content (system prompts, tool schemas) is logged in full only the
// first time its SHA-256 is seen; thereafter only a compact stand-in is emitted.
// ---------------------------------------------------------------------------

/** Minimum string length (chars) before a field is eligible for content dedup. */
export const DEDUP_MIN_CONTENT_LEN = 1024;

/** How many leading chars of a de-duplicated value are kept as a preview. */
export const DEDUP_PREVIEW_LEN = 500;

/**
 * Session-scoped content-deduplication cache. For each long string field, the full
 * value is emitted the FIRST time its 12-char SHA-256 prefix is seen; every later
 * occurrence of the same content is collapsed to a `<key>` preview plus `<key>_hash`
 * and `<key>_length` — so large repeated bodies cost one full log per unique hash
 * per session, not one per event. Mirrors the reference's `seenHashes` dedup. Reset per
 * session via {@link reset}.
 */
export class TelemetryContentDedup {
  private _seen = new Set<string>();
  constructor(
    private readonly _minLen: number = DEDUP_MIN_CONTENT_LEN,
    private readonly _previewLen: number = DEDUP_PREVIEW_LEN,
  ) {}

  /** Forget all hashes (start of a new session). */
  reset(): void {
    this._seen.clear();
  }

  /** Number of distinct content hashes seen this session. */
  get seenCount(): number {
    return this._seen.size;
  }

  /**
   * De-duplicate every long string field of `data`. Short strings and non-strings
   * pass through untouched. Returns the original object when nothing changed (so a
   * caller can cheaply detect a no-op).
   */
  dedupe(data: Record<string, unknown>): Record<string, unknown> {
    let changed = false;
    const out: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(data)) {
      if (typeof value === 'string' && value.length >= this._minLen) {
        const hash = crypto.createHash('sha256').update(value).digest('hex').slice(0, 12);
        if (this._seen.has(hash)) {
          out[key] = value.slice(0, this._previewLen);
          out[`${key}_hash`] = hash;
          out[`${key}_length`] = value.length;
          changed = true;
          continue;
        }
        this._seen.add(hash);
      }
      out[key] = value;
    }
    return changed ? out : data;
  }
}

/** Env vars consulted (precedence order) for the otelHeadersHelper command. */
export const OTEL_HEADERS_HELPER_ENV_VARS = [
  'AMI_OTEL_HEADERS_HELPER',
  'CLAUDE_CODE_OTEL_HEADERS_HELPER',
] as const;

/**
 * Resolve the otelHeadersHelper COMMAND from the environment: the AMI-native
 * `AMI_OTEL_HEADERS_HELPER` first, then the Claude-compatible
 * `CLAUDE_CODE_OTEL_HEADERS_HELPER` (parity: the reference's `otelHeadersHelper` setting).
 * Returns `undefined` when neither is set or the value is blank. Sourcing the command
 * from the operator-controlled environment — rather than a cloned repo's project/local
 * settings — is the AMI-native equivalent of the reference's trust gate: an env var cannot be
 * injected by an untrusted workspace, so there is no "execute before trust" exposure.
 */
export function resolveOtelHeadersHelperCommand(env: NodeJS.ProcessEnv = process.env): string | undefined {
  for (const name of OTEL_HEADERS_HELPER_ENV_VARS) {
    const raw = env[name];
    if (raw !== undefined && raw.trim() !== '') return raw.trim();
  }
  return undefined;
}

/** Injected dependencies for {@link OtelHeadersHelper} (command runner + clock + env). */
export interface OtelHeadersHelperDeps {
  /** Run the helper command and return its stdout (throws on non-zero exit / spawn failure). */
  exec?: (command: string) => string;
  env?: NodeJS.ProcessEnv;
  now?: () => number;
}

/**
 * Validate that `parsed` is a JSON object of string→string header pairs (throwing
 * the same actionable errors the reference's getOtelHeadersFromHelper does). Exported so a
 * direct caller can reuse the validation without the caching wrapper.
 */
export function validateOtelHeaders(parsed: unknown): Record<string, string> {
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new Error('otelHeadersHelper must return a JSON object with string key-value pairs');
  }
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value !== 'string') {
      throw new Error(`otelHeadersHelper returned non-string value for key "${key}": ${typeof value}`);
    }
  }
  return parsed as Record<string, string>;
}

// ---------------------------------------------------------------------------
// Bounded graceful shutdown drain (the reference HybridTransport.ts parity).
// ---------------------------------------------------------------------------

/** Default best-effort drain window (ms) given to queued telemetry on close. */
export const CLOSE_GRACE_MS = 3000;

/**
 * Await the first of `promise` (settled, success OR failure) or a `ms` timeout —
 * so a best-effort drain cannot block shutdown beyond `ms`. Mirrors the reference's
 * `Promise.race([flush(), timeout(CLOSE_GRACE_MS)])` graceful-close bound. The sleep
 * is injectable for deterministic tests.
 *
 * The default grace sleep is the shared {@link sleep} with `{ unref: true }` so the
 * LOSING arm (the grace timer, when the flush wins the race) never keeps the event
 * loop alive — parity with the reference's `sleep(500, undefined, { unref: true })` grace
 * race before exit (bridgeMain.ts). Telemetry drain is strictly best-effort: a
 * pending grace timer must never delay process shutdown.
 */
export async function raceWithTimeout(
  promise: Promise<unknown>,
  ms: number,
  sleepImpl: (ms: number) => Promise<void> = (m) => sleep(m, undefined, { unref: true }),
): Promise<void> {
  await Promise.race([promise.then(() => undefined, () => undefined), sleepImpl(ms)]);
}

// ---------------------------------------------------------------------------
// OTEL exporter / protocol / cadence configuration knobs (the reference parity)
// ---------------------------------------------------------------------------

/** OTLP wire protocols AMI recognizes (only `http/json` is actually emitted). */
export const OTLP_PROTOCOLS = ['http/json', 'http/protobuf', 'grpc'] as const;

export type OtlpProtocol = (typeof OTLP_PROTOCOLS)[number];

/**
 * Parse an `OTEL_*_EXPORTER` value into an ordered exporter-type list. Per the OTEL
 * spec the literal `none` means "no automatically configured exporter for this
 * signal", so `none` tokens are filtered out (a bare `none` yields `[]` → disabled).
 * Blanks are dropped. An unset/empty value yields `[]` so the caller can apply its
 * own default. Mirrors the reference's parseExporterTypes.
 */
export function parseExporterTypes(value: string | undefined): string[] {
  return (value || '')
    .trim()
    .split(',')
    .map(t => t.trim())
    .filter(t => t !== '' && t !== 'none');
}

/**
 * Resolve the OTLP wire protocol for one signal, honoring the per-signal
 * `OTEL_EXPORTER_OTLP_{SIGNAL}_PROTOCOL` knob then the generic
 * `OTEL_EXPORTER_OTLP_PROTOCOL`, defaulting to `http/json` when unset. Throws on an
 * unrecognized value (mirrors the reference's switch default). Case/whitespace-insensitive.
 */
export function resolveOtlpProtocol(
  env: NodeJS.ProcessEnv,
  signal: 'logs' | 'metrics' | 'traces',
): OtlpProtocol {
  const perSignal = env[`OTEL_EXPORTER_OTLP_${signal.toUpperCase()}_PROTOCOL`];
  const raw = (perSignal ?? env.OTEL_EXPORTER_OTLP_PROTOCOL ?? '').trim().toLowerCase();
  if (!raw) return 'http/json';
  if ((OTLP_PROTOCOLS as readonly string[]).includes(raw)) return raw as OtlpProtocol;
  throw new Error(`Unknown OTLP protocol: ${raw}`);
}

/**
 * Whether telemetry EGRESS (OTLP POST / console dump) must be suppressed because
 * this is a test run. Defaults to on under `NODE_ENV==='test'` (so a test harness
 * never ships real telemetry — mirrors the reference's logOTelEvent NODE_ENV guard); an
 * explicit `AMI_TELEMETRY_ALLOW_IN_TEST` opts back in. Local in-memory/disk records
 * are unaffected — only off-machine/stdout egress is gated.
 */
export function isTelemetrySuppressedForTest(env: NodeJS.ProcessEnv): boolean {
  if (env.NODE_ENV !== 'test') return false;
  return !isEnvTruthy(env.AMI_TELEMETRY_ALLOW_IN_TEST);
}

/** Per-signal export-cadence defaults (ms): logs/traces fast, metrics slow. */
export const DEFAULT_EXPORT_INTERVALS: Record<'logs' | 'metrics' | 'traces', number> = {
  logs: 5000,
  metrics: 60000,
  traces: 5000,
};

export const EXPORT_INTERVAL_ENV_KEYS: Record<'logs' | 'metrics' | 'traces', string> = {
  logs: 'OTEL_LOGS_EXPORT_INTERVAL',
  metrics: 'OTEL_METRIC_EXPORT_INTERVAL',
  traces: 'OTEL_TRACES_EXPORT_INTERVAL',
};

/**
 * Resolve the export flush interval (ms) for a signal from its env knob, falling
 * back to {@link DEFAULT_EXPORT_INTERVALS} for an unset / non-positive / unparseable
 * value. Lets an operator tune export cadence per signal (the reference parity).
 */
export function resolveExportIntervalMs(
  signal: 'logs' | 'metrics' | 'traces',
  env: NodeJS.ProcessEnv,
): number {
  const raw = env[EXPORT_INTERVAL_ENV_KEYS[signal]];
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : DEFAULT_EXPORT_INTERVALS[signal];
}

/** Default overall shutdown-flush budget (ms) for the whole OTLP drain. */
export const DEFAULT_FLUSH_TIMEOUT_MS = 5000;

/**
 * Resolve the overall flush/shutdown budget (ms) the OTLP drain may block for,
 * from `AMI_OTEL_FLUSH_TIMEOUT_MS`, defaulting to {@link DEFAULT_FLUSH_TIMEOUT_MS}.
 * A literal `0` is honored (meaning "no time to flush"); a non-finite/negative
 * value falls back to the default (mirrors the reference's CLAUDE_CODE_OTEL_FLUSH_TIMEOUT_MS).
 */
export function resolveFlushTimeoutMs(env: NodeJS.ProcessEnv): number {
  const raw = env.AMI_OTEL_FLUSH_TIMEOUT_MS;
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isFinite(n) && n >= 0 ? Math.floor(n) : DEFAULT_FLUSH_TIMEOUT_MS;
}

/**
 * Actionable, numbered remediation shown when the OTLP flush exceeds its budget —
 * so a user pointing AMI at an unreachable/slow collector gets a self-correcting
 * hint instead of silence (mirrors the reference's flush-timeout guidance; AGENTS.md
 * requires failures to surface a remedy).
 */
export function formatFlushTimeoutGuidance(timeoutMs: number): string {
  return [
    `Telemetry flush exceeded its ${timeoutMs}ms budget; remaining events were re-queued.`,
    'To resolve this, you can:',
    '  1. Increase the timeout by setting AMI_OTEL_FLUSH_TIMEOUT_MS to a larger value.',
    '  2. Check that your OTLP collector endpoint is reachable and healthy.',
    '  3. Disable telemetry by unsetting the OTLP endpoint (or OTEL_LOGS_EXPORTER=none).',
  ].join('\n');
}

// ---------------------------------------------------------------------------
// Telemetry environment fingerprint, stable install id, and low-cardinality
// derivations attached to OTLP resource / log attributes. All are
// provider-neutral and let a self-hosted collector segment and estimate usage
// without exporting raw high-cardinality identifiers.
// ---------------------------------------------------------------------------

export const JETBRAINS_IDES = [
  'intellij', 'pycharm', 'webstorm', 'phpstorm', 'rubymine', 'clion',
  'goland', 'datagrip', 'rider', 'appcode', 'jetbrains', 'androidstudio',
];

/**
 * Resolve a provider-neutral terminal identifier from the environment (never the
 * raw terminal path — just a stable low-cardinality name), so a self-hosted
 * dashboard can segment usage by terminal. Returns null when nothing identifiable
 * is present (so the `terminal.type` attribute is attached only when resolved).
 * Mirrors the reference's detectTerminal fallbacks, env-driven for testability.
 */
export function detectTerminalType(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): string | null {
  if (env.CURSOR_TRACE_ID) return 'cursor';
  const askpass = env.VSCODE_GIT_ASKPASS_MAIN;
  if (askpass?.includes('cursor')) return 'cursor';
  if (askpass?.includes('windsurf')) return 'windsurf';
  const bundleId = env.__CFBundleIdentifier?.toLowerCase();
  if (bundleId?.includes('vscodium')) return 'codium';
  if (bundleId?.includes('windsurf')) return 'windsurf';
  if (bundleId) {
    for (const ide of JETBRAINS_IDES) {
      if (bundleId.includes(ide)) return ide;
    }
  }
  if (env.VisualStudioVersion) return 'visualstudio';
  if (env.TERMINAL_EMULATOR === 'JetBrains-JediTerm') return platform === 'darwin' ? 'pycharm' : 'pycharm';
  if (env.TERM === 'xterm-ghostty') return 'ghostty';
  if (env.TERM?.includes('kitty')) return 'kitty';
  if (env.TERM_PROGRAM) return env.TERM_PROGRAM;
  if (env.TMUX) return 'tmux';
  if (env.STY) return 'screen';
  if (env.KONSOLE_VERSION) return 'konsole';
  if (env.GNOME_TERMINAL_SERVICE) return 'gnome-terminal';
  if (env.XTERM_VERSION) return 'xterm';
  if (env.VTE_VERSION) return 'vte-based';
  if (env.ALACRITTY_LOG) return 'alacritty';
  if (env.WT_SESSION) return 'windows-terminal';
  if (env.WSL_DISTRO_NAME) return `wsl-${env.WSL_DISTRO_NAME}`;
  if (env.TERM) return env.TERM;
  return null;
}

/** Accepted values for the {@link getHostPlatformForAnalytics} override knob. */
export const HOST_PLATFORM_OVERRIDES = ['win32', 'darwin', 'linux'] as const;

/**
 * Host platform reported in telemetry. `AMI_HOST_PLATFORM` (win32|darwin|linux)
 * overrides the detected `process.platform` — essential for AMI's Docker-sandboxed
 * mode, where process.platform reports the container OS but the real host differs
 * (mirrors the reference's getHostPlatformForAnalytics / CLAUDE_CODE_HOST_PLATFORM knob).
 */
export function getHostPlatformForAnalytics(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
): string {
  const override = env.AMI_HOST_PLATFORM;
  if (override === 'win32' || override === 'darwin' || override === 'linux') return override;
  return platform;
}

/**
 * Resolve the `wsl.version` telemetry dimension (`'1'` | `'2'`) when running under
 * the Windows Subsystem for Linux, else null (so the attribute is attached only on
 * WSL). Detection requires a WSL env marker (`WSL_DISTRO_NAME` / `WSL_INTEROP`) so
 * the dimension is deterministic off-WSL (never a false positive from a stray kernel
 * string), then classifies the generation via the shared {@link detectWslVersion}
 * kernel classifier (single source of truth, reused — no duplicate WSL logic). A
 * marker present with an untagged kernel assumes WSL2 (the modern default), matching
 * bash-sandbox's `currentWslVersion`. Mirrors the reference's instrumentation.ts WSL
 * resource special-case (`wsl.version`).
 */
export function detectWslForAnalytics(
  env: NodeJS.ProcessEnv = process.env,
  kernel: string = os.release(),
): string | null {
  if (!env.WSL_DISTRO_NAME && !env.WSL_INTEROP) return null;
  const v = detectWslVersion(kernel);
  if (v === 'wsl1') return '1';
  return '2'; // 'wsl2' or marker-present-but-untagged → modern WSL2 default
}

/**
 * Vendor-neutral environment fingerprint for telemetry: host platform (honoring
 * the {@link getHostPlatformForAnalytics} override), CPU arch, Node version, and —
 * only when running under WSL — the `wsl.version` generation. Attached to every OTLP
 * resource so a collector can segment by environment.
 */
export function getEnvironmentFingerprint(
  env: NodeJS.ProcessEnv = process.env,
  info: { platform?: NodeJS.Platform; arch?: string; nodeVersion?: string; kernel?: string } = {},
): Record<string, string> {
  const out: Record<string, string> = {
    'host.platform': getHostPlatformForAnalytics(env, info.platform ?? process.platform),
    'host.arch': info.arch ?? process.arch,
    'node.version': info.nodeVersion ?? process.version,
  };
  const wsl = detectWslForAnalytics(env, info.kernel);
  if (wsl) out['wsl.version'] = wsl;
  return out;
}

/** Point-in-time process resource metrics attached to analytics events (opt-in). */
export interface ProcessMetrics {
  uptime: number;
  rss: number;
  heapTotal: number;
  heapUsed: number;
  external: number;
  /** Memory held by allocated `ArrayBuffer`/`SharedArrayBuffer` (included in `external`);
   *  surfaced separately so a buffer leak is distinguishable from other native memory. */
  arrayBuffers: number;
  cpuUserUs: number;
  cpuSystemUs: number;
  /** CPU utilization since the previous sample (undefined on the first sample). */
  cpuPercent?: number;
  /** The cgroup/container memory ceiling in bytes (`process.constrainedMemory()`), when the
   *  process runs under a constraint (>0). Undefined when unconstrained or unsupported — so a
   *  container's memory headroom is observable alongside `rss` (parity: the reference's
   *  buildProcessMetrics `constrainedMemory`). */
  constrainedMemory?: number;
}

/** Injectable process readers for {@link ProcessMetricsSampler} (deterministic tests). */
export interface ProcessMetricsDeps {
  memoryUsage?: () => NodeJS.MemoryUsage;
  cpuUsage?: () => NodeJS.CpuUsage;
  uptime?: () => number;
  now?: () => number;
  /** The cgroup memory ceiling reader (`process.constrainedMemory`). Returns 0/undefined when
   *  unconstrained; a reader that throws is treated as unsupported. */
  constrainedMemory?: () => number | undefined;
}
