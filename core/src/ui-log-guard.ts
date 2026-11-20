export const LOG_TIERS = ['builtin', 'prepend', 'user', 'append', 'core', 'engine'] as const;
export type LogTier = (typeof LOG_TIERS)[number];

// ---------------------------------------------------------------------------
// Terminal-safety sanitization (FUNCTIONAL ADDITION)
// ---------------------------------------------------------------------------
// Log messages flow straight into a terminal / UI. Raw, attacker-influenced text
// (tool output, file contents, remote data) can carry ANSI escape sequences,
// C0/C1 control characters, carriage-return line overwrites, OSC title/hyperlink
// injections and embedded secrets. These are stripped/redacted/bounded before a
// message is stored so it can never drive the terminal or leak credentials.
// All transforms are additive: plain text passes through unchanged.

export interface SanitizeLogOptions {
  /** Maximum stored length in UTF-16 code units. Longer messages are truncated
   *  with a visible marker. Default 10_000. Non-positive disables the cap. */
  maxLength?: number;
  /** Redact common secret shapes (API keys, tokens, key=value). Default true. */
  redactSecrets?: boolean;
  /** Strip ANSI/OSC/DCS escape sequences. Default true. */
  stripAnsi?: boolean;
  /** Strip C0/C1 control characters. Default true. */
  stripControl?: boolean;
  /** Keep real newlines (\n). When false they are collapsed to spaces. Default true. */
  allowNewlines?: boolean;
}

export const DEFAULT_MAX_LOG_LENGTH = 10_000;
const TRUNCATION_MARKER = '…[truncated]';
export const SECRET_REDACTION = '[redacted]';

// ESC-introduced sequences: CSI (ESC [ … final), OSC (ESC ] … BEL|ST),
// DCS/SOS/PM/APC (ESC P|X|^|_ … ST), 2-char escapes, and lone 8-bit CSI (0x9B).
// eslint-disable-next-line no-control-regex
const ANSI_PATTERN = /(?:\u001B\[|\u009B)[0-?]*[ -/]*[@-~]|\u001B\][^\u0007\u001B\u009C]*(?:\u0007|\u001B\\|\u009C|$)|\u001B[PX^_][^\u001B\u009C]*(?:\u001B\\|\u009C|$)|\u001B[@-Z\\-_]/g;

// C0 controls 0x00-0x1F and DEL 0x7F, and C1 controls 0x80-0x9F.
// Tab (0x09) and newline (0x0A) are handled separately so they can be preserved.
// eslint-disable-next-line no-control-regex
const C0_C1_PATTERN = /[\u0000-\u0008\u000B-\u001F\u007F-\u009F]/g;

interface SecretRule {
  readonly re: RegExp;
  /** When set, keep this leading capture group and redact the rest. */
  readonly keepGroup?: boolean;
}

// Ordered, high-signal secret shapes. Conservative on purpose: prefixed vendor
// tokens and explicit key=value pairs only, to avoid mangling ordinary prose.
const SECRET_RULES: readonly SecretRule[] = [
  // Vendor-prefixed tokens (OpenAI, Anthropic, GitHub, Slack, Stripe, Google).
  { re: /\b(?:sk|pk|rk)-[A-Za-z0-9_-]{16,}\b/g },
  { re: /\bsk-ant-[A-Za-z0-9_-]{16,}\b/g },
  { re: /\bgh[pousr]_[A-Za-z0-9]{20,}\b/g },
  { re: /\bxox[baprs]-[A-Za-z0-9-]{10,}\b/g },
  { re: /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g },
  { re: /\bAIza[A-Za-z0-9_-]{20,}\b/g },
  { re: /\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b/g }, // JWT
  // GCP / Vertex AI service-account emails (…@….iam.gserviceaccount.com).
  { re: /\b[a-z0-9-]+@[a-z0-9-]+\.iam\.gserviceaccount\.com\b/gi },
  // Authorization: Bearer / Basic <token>
  { re: /\b(Bearer|Basic|Token)\s+[A-Za-z0-9._~+/=-]{8,}/gi, keepGroup: true },
  // Cloud-provider environment variables (AWS_*, GOOGLE_*) → redact the value.
  { re: /\b(AWS[_-][A-Za-z0-9_]+\s*[=:]\s*)["']?[^"',\s)}\]]+["']?/gi, keepGroup: true },
  { re: /\b(GOOGLE[_-][A-Za-z0-9_]+\s*[=:]\s*)["']?[^"',\s)}\]]+["']?/gi, keepGroup: true },
  // key = value / key: value for sensitive key names.
  {
    re: /\b((?:api[_-]?key|secret|password|passwd|pwd|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key)\s*[=:]\s*)["']?[A-Za-z0-9._~+/=-]{6,}["']?/gi,
    keepGroup: true,
  },
];

/**
 * Sanitize a single log message so it is safe to render in a terminal/UI:
 * strips ANSI/OSC/DCS escape sequences, removes C0/C1 control characters
 * (normalizing CR and CRLF), redacts common secret shapes, and bounds length.
 * Plain text is returned unchanged.
 */
export function sanitizeLogMessage(message: string, opts: SanitizeLogOptions = {}): string {
  const {
    maxLength = DEFAULT_MAX_LOG_LENGTH,
    redactSecrets = true,
    stripAnsi = true,
    stripControl = true,
    allowNewlines = true,
  } = opts;

  // Defensive coercion: runtime callers may pass non-strings despite the type.
  let out = typeof message === 'string' ? message : String(message ?? '');

  if (stripAnsi) {
    out = out.replace(ANSI_PATTERN, '');
  }

  if (stripControl) {
    // Normalize CRLF/CR to LF first so carriage-return line overwrites cannot
    // repaint the terminal, then drop remaining C0/C1 controls (keeping \n, \t).
    out = out.replace(/\r\n?/g, '\n').replace(C0_C1_PATTERN, '');
  }

  if (!allowNewlines) {
    out = out.replace(/\n/g, ' ');
  }

  if (redactSecrets) {
    for (const rule of SECRET_RULES) {
      out = rule.keepGroup
        ? out.replace(rule.re, (_m, keep: string) => keep + SECRET_REDACTION)
        : out.replace(rule.re, SECRET_REDACTION);
    }
  }

  if (maxLength > 0 && out.length > maxLength) {
    // Keep the marker only when the cap is large enough to hold both it and
    // some content; for tiny caps a hard slice still respects the bound.
    const marker = maxLength > TRUNCATION_MARKER.length ? TRUNCATION_MARKER : '';
    out = out.slice(0, maxLength - marker.length) + marker;
  }

  return out;
}

export interface LogEntry {
  message: string;
  tier: LogTier;
  source: string;
  timestamp: number;
  level: 'debug' | 'info' | 'warn' | 'error';
}

export function isProtectedLogTier(tier: LogTier): boolean {
  return tier === 'prepend' || tier === 'append' || tier === 'builtin' || tier === 'engine';
}

export function canModifyLog(logTier: LogTier, modifierTier: LogTier): boolean {
  if (!isProtectedLogTier(logTier)) return true;
  const tierOrder: Record<LogTier, number> = {
    engine: 0,
    builtin: 1,
    prepend: 2,
    user: 3,
    append: 4,
    core: 5,
  };
  return tierOrder[modifierTier] <= tierOrder[logTier];
}

export class UILogGuard {
  private _logs: LogEntry[] = [];
  private _maxLogs: number;
  private _suppressed = 0;
  private _sanitize: boolean;
  private _sanitizeOpts: SanitizeLogOptions;

  constructor(maxLogs: number = 50_000, sanitizeOpts?: SanitizeLogOptions | false) {
    this._maxLogs = maxLogs;
    // FUNCTIONAL ADDITION: messages (and sources) are sanitized for the terminal
    // by default. Pass `false` to opt out, or an options object to tune it.
    this._sanitize = sanitizeOpts !== false;
    this._sanitizeOpts = sanitizeOpts ? sanitizeOpts : {};
  }

  addLog(message: string, tier: LogTier, source: string, level: LogEntry['level'] = 'info'): LogEntry {
    // FUNCTIONAL ADDITION: strip terminal control/escape sequences, redact
    // secrets and bound length before the message/source is ever stored or shown.
    const safeMessage = this._sanitize ? sanitizeLogMessage(message, this._sanitizeOpts) : message;
    // Source labels render inline too, so make them single-line and control-free.
    const safeSource = this._sanitize
      ? sanitizeLogMessage(source, { ...this._sanitizeOpts, allowNewlines: false, redactSecrets: false })
      : source;
    const entry: LogEntry = { message: safeMessage, tier, source: safeSource, timestamp: Date.now(), level };
    // FUNCTIONAL FIX (parity with the reference's addToInMemoryErrorLog): ring-buffer
    // eviction of the OLDEST entry when full, rather than dropping the incoming
    // one. The newest (most diagnostically relevant) logs are always retained;
    // a long-running session keeps recording instead of freezing on the first N.
    // `_suppressed` becomes a rotated-out counter.
    if (this._logs.length >= this._maxLogs) {
      this._logs.shift();
      this._suppressed++;
    }
    this._logs.push(entry);
    return entry;
  }

  checkModification(logIndex: number, modifierTier: LogTier): boolean {
    const entry = this._logs[logIndex];
    if (!entry) return false;
    return canModifyLog(entry.tier, modifierTier);
  }

  getLogsByTier(tier: LogTier): LogEntry[] {
    return this._logs.filter(e => e.tier === tier);
  }

  getLogsByLevel(level: LogEntry['level']): LogEntry[] {
    return this._logs.filter(e => e.level === level);
  }

  getLogsBySource(source: string): LogEntry[] {
    return this._logs.filter(e => e.source === source);
  }

  getLogs(): readonly LogEntry[] {
    return this._logs;
  }

  getLogCount(): number {
    return this._logs.length;
  }

  getSuppressedCount(): number {
    return this._suppressed;
  }

  clear(): void {
    this._logs = [];
    this._suppressed = 0;
  }
}
