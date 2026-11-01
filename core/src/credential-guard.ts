const CREDENTIAL_PATTERNS = [
  /sk-ant-[a-zA-Z0-9-]{20,}/g,
  /sk-[a-zA-Z0-9]{20,}/g,
  /AIza[a-zA-Z0-9_-]{35}/g,
  /ghp_[a-zA-Z0-9]{36}/g,
  /gho_[a-zA-Z0-9]{36}/g,
  /github_pat_[a-zA-Z0-9_]{22,}/g,
  /glpat-[a-zA-Z0-9_-]{20,}/g,
  /xoxb-[0-9]+-[a-zA-Z0-9]+/g,
  /xoxp-[0-9]+-[a-zA-Z0-9]+/g,
  /AKIA[A-Z0-9]{16}/g,
  /eyJ[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}/g,
  /nvapi-[a-zA-Z0-9_-]{20,}/g,
  // PEM private-key blocks (RSA / EC / OPENSSH / DSA / PGP / unlabelled).
  /-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----/g,
  // Bearer tokens in Authorization headers / curl examples.
  /Bearer\s+[a-zA-Z0-9_\-.=+/]{20,}/g,
];

export const REDACT_PLACEHOLDER = '[REDACTED]';

// ---------------------------------------------------------------------------
// Entropy-based detection for generic assigned secrets
// ---------------------------------------------------------------------------

/** Shannon entropy (bits/char) of a string. Random secrets score high (~4-6); */
export function shannonEntropy(s: string): number {
  if (!s) return 0;
  const freq = new Map<string, number>();
  for (const ch of s) freq.set(ch, (freq.get(ch) ?? 0) + 1);
  let entropy = 0;
  for (const count of freq.values()) {
    const p = count / s.length;
    entropy -= p * Math.log2(p);
  }
  return entropy;
}

/**
 * A token looks like a high-entropy secret: long enough and random enough that
 * it is implausibly a word, path, or identifier. Threshold 3.5 bits/char keeps
 * out repeated/low-variety strings while catching base64/hex secrets.
 */
export function isHighEntropySecret(token: string): boolean {
  return token.length >= 16 && shannonEntropy(token) >= 3.5;
}

// Only flag high-entropy values when they are ASSIGNED to a secret-ish key, so
// random hashes / UUIDs / git SHAs in normal output are not over-redacted.
const ASSIGNED_SECRET_RE =
  /(?:secret|token|password|passwd|api[_-]?key|access[_-]?key|auth[_-]?token|client[_-]?secret)["']?\s*[:=]\s*["']?([A-Za-z0-9+/_.\-=]{16,})/gi;

export function containsCredential(text: string): boolean {
  if (!text || text.length < 10) return false;
  if (CREDENTIAL_PATTERNS.some(pattern => {
    pattern.lastIndex = 0;
    return pattern.test(text);
  })) return true;
  // Contextual high-entropy secret.
  ASSIGNED_SECRET_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = ASSIGNED_SECRET_RE.exec(text)) !== null) {
    if (isHighEntropySecret(m[1])) return true;
  }
  return false;
}

export function redactCredentials(text: string): string {
  if (!text || text.length < 10) return text;
  let result = text;
  for (const pattern of CREDENTIAL_PATTERNS) {
    pattern.lastIndex = 0;
    result = result.replace(new RegExp(pattern.source, pattern.flags), REDACT_PLACEHOLDER);
  }
  // Redact only the assigned secret value (keep the key for context), and only
  // when it is actually high-entropy.
  result = result.replace(new RegExp(ASSIGNED_SECRET_RE.source, ASSIGNED_SECRET_RE.flags), (full, secret) =>
    isHighEntropySecret(secret) ? full.replace(secret, REDACT_PLACEHOLDER) : full,
  );
  return result;
}

export function findCredentials(text: string): Array<{ type: string; start: number; length: number }> {
  if (!text || text.length < 10) return [];
  const found: Array<{ type: string; start: number; length: number }> = [];

  const typeMap: Array<[RegExp, string]> = [
    [/sk-ant-[a-zA-Z0-9-]{20,}/g, 'anthropic_key'],
    [/sk-[a-zA-Z0-9]{20,}/g, 'openai_key'],
    [/AIza[a-zA-Z0-9_-]{35}/g, 'google_key'],
    [/ghp_[a-zA-Z0-9]{36}/g, 'github_pat'],
    [/gho_[a-zA-Z0-9]{36}/g, 'github_oauth'],
    [/github_pat_[a-zA-Z0-9_]{22,}/g, 'github_fine_pat'],
    [/glpat-[a-zA-Z0-9_-]{20,}/g, 'gitlab_pat'],
    [/xoxb-[0-9]+-[a-zA-Z0-9]+/g, 'slack_bot'],
    [/xoxp-[0-9]+-[a-zA-Z0-9]+/g, 'slack_user'],
    [/AKIA[A-Z0-9]{16}/g, 'aws_access_key'],
    [/eyJ[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}\.[a-zA-Z0-9_-]{20,}/g, 'jwt'],
    [/-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----/g, 'private_key'],
    [/Bearer\s+[a-zA-Z0-9_\-.=+/]{20,}/g, 'bearer_token'],
  ];

  for (const [pattern, type] of typeMap) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      found.push({ type, start: match.index, length: match[0].length });
    }
  }

  // Contextual high-entropy secrets (value only).
  ASSIGNED_SECRET_RE.lastIndex = 0;
  let em: RegExpExecArray | null;
  while ((em = ASSIGNED_SECRET_RE.exec(text)) !== null) {
    const secret = em[1];
    if (!isHighEntropySecret(secret)) continue;
    found.push({ type: 'generic_secret', start: em.index + em[0].lastIndexOf(secret), length: secret.length });
  }

  return found.sort((a, b) => a.start - b.start);
}

export class CredentialGuard {
  private _detections: Array<{ type: string; source: string; timestamp: number }> = [];
  private _maxDetections = 1000;

  scan(text: string, source: string): boolean {
    const creds = findCredentials(text);
    if (creds.length === 0) return false;
    for (const cred of creds) {
      if (this._detections.length < this._maxDetections) {
        this._detections.push({ type: cred.type, source, timestamp: Date.now() });
      }
    }
    return true;
  }

  redact(text: string): string {
    return redactCredentials(text);
  }

  getDetections(): ReadonlyArray<{ type: string; source: string; timestamp: number }> {
    return this._detections;
  }

  getDetectionCount(): number {
    return this._detections.length;
  }

  clear(): void {
    this._detections = [];
  }
}
