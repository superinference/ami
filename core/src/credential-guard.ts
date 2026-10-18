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
];

export const REDACT_PLACEHOLDER = '[REDACTED]';

export function containsCredential(text: string): boolean {
  if (!text || text.length < 10) return false;
  return CREDENTIAL_PATTERNS.some(pattern => {
    pattern.lastIndex = 0;
    return pattern.test(text);
  });
}

export function redactCredentials(text: string): string {
  if (!text || text.length < 10) return text;
  let result = text;
  for (const pattern of CREDENTIAL_PATTERNS) {
    pattern.lastIndex = 0;
    result = result.replace(new RegExp(pattern.source, pattern.flags), REDACT_PLACEHOLDER);
  }
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
  ];

  for (const [pattern, type] of typeMap) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(text)) !== null) {
      found.push({ type, start: match.index, length: match[0].length });
    }
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
