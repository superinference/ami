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
  // AWS access-key family: long-term (AKIA), temporary/STS session (ASIA),
  // plus A3T*/ABIA/ACCA prefixes. Body is base32 (A-Z,2-7), 16 chars.
  /(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16}/g,
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

// A secret value ASSIGNED to a secret-ish key (`token: x`, `api_key="x"`, …).
// Group 1 = key name, group 2 = separator (incl. opening quote), group 3 = value.
// The lookbehind keeps `secret` from matching the tail of `client_secret`, and the
// value class deliberately excludes quotes/whitespace so a trailing quote is left
// intact. Unlike the old pattern this accepts short values (no `{16,}` floor) so
// a short/opaque token under a secret-named key is caught (see redaction tiers).
const SECRET_ASSIGN_RE =
  /(?<![A-Za-z0-9_])(password|passwd|secret|token|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|credential|api[_-]?key|access[_-]?key)(["']?\s*[:=]\s*["']?)([A-Za-z0-9+/_.\-=]+)/gi;

// Field names that are UNCONDITIONALLY secret: the assigned value is redacted
// regardless of length or entropy (a short/opaque session token must not leak just
// because it is low-entropy). The weaker `*_key` family (api_key/access_key) keeps
// the entropy gate below, since those names also appear with non-secret values.
const UNCONDITIONAL_SECRET_KEY =
  /^(?:password|passwd|secret|token|access[_-]?token|auth[_-]?token|client[_-]?secret|private[_-]?key|credential)$/i;

/** Whether the value assigned to a secret-ish key should be redacted. */
function assignedSecretShouldRedact(key: string, value: string): boolean {
  if (UNCONDITIONAL_SECRET_KEY.test(key)) return true;
  return isHighEntropySecret(value);
}

export function containsCredential(text: string): boolean {
  if (!text || text.length < 10) return false;
  if (CREDENTIAL_PATTERNS.some(pattern => {
    pattern.lastIndex = 0;
    return pattern.test(text);
  })) return true;
  // Secret value assigned to a secret-ish key.
  SECRET_ASSIGN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = SECRET_ASSIGN_RE.exec(text)) !== null) {
    if (assignedSecretShouldRedact(m[1], m[3])) return true;
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
  // Redact the assigned secret value (keep the key + separator for context).
  result = result.replace(
    new RegExp(SECRET_ASSIGN_RE.source, SECRET_ASSIGN_RE.flags),
    (full: string, key: string, sep: string, value: string) =>
      assignedSecretShouldRedact(key, value) ? `${key}${sep}${REDACT_PLACEHOLDER}` : full,
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
    [/(?:A3T[A-Z0-9]|AKIA|ASIA|ABIA|ACCA)[A-Z2-7]{16}/g, 'aws_access_key'],
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

  // Secret values assigned to secret-ish keys (value only).
  SECRET_ASSIGN_RE.lastIndex = 0;
  let em: RegExpExecArray | null;
  while ((em = SECRET_ASSIGN_RE.exec(text)) !== null) {
    const key = em[1];
    const value = em[3];
    if (!assignedSecretShouldRedact(key, value)) continue;
    const valueStart = em.index + em[1].length + em[2].length;
    found.push({ type: 'generic_secret', start: valueStart, length: value.length });
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
