import * as crypto from 'crypto';

export interface RequestSignature {
  nonce: string;
  timestamp: number;
  signature: string;
}

export function generateNonce(bytes: number = 16): string {
  return crypto.randomBytes(bytes).toString('hex');
}

export function signRequest(
  method: string,
  path: string,
  body: string,
  timestamp: number,
  secret: string,
): string {
  const payload = `${method.toUpperCase()}\n${path}\n${timestamp}\n${body}`;
  return crypto.createHmac('sha256', secret).update(payload).digest('hex');
}

export function createRequestSignature(
  method: string,
  path: string,
  body: string,
  secret: string,
): RequestSignature {
  const nonce = generateNonce();
  const timestamp = Date.now();
  const payload = `${method.toUpperCase()}\n${path}\n${timestamp}\n${nonce}\n${body}`;
  const signature = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  return { nonce, timestamp, signature };
}

export function verifyRequestSignature(
  sig: RequestSignature,
  method: string,
  path: string,
  body: string,
  secret: string,
  maxAgeMs: number = 300_000,
): { valid: boolean; reason?: string } {
  const now = Date.now();
  if (now - sig.timestamp > maxAgeMs) {
    return { valid: false, reason: 'signature expired' };
  }
  if (sig.timestamp > now + 60_000) {
    return { valid: false, reason: 'signature timestamp in future' };
  }
  const payload = `${method.toUpperCase()}\n${path}\n${sig.timestamp}\n${sig.nonce}\n${body}`;
  const expected = crypto.createHmac('sha256', secret).update(payload).digest('hex');
  const valid = crypto.timingSafeEqual(Buffer.from(sig.signature, 'hex'), Buffer.from(expected, 'hex'));
  return valid ? { valid: true } : { valid: false, reason: 'signature mismatch' };
}

export class NonceTracker {
  private _seen = new Set<string>();
  private _maxSize: number;
  private _timestamps = new Map<string, number>();

  constructor(maxSize: number = 10_000) {
    this._maxSize = maxSize;
  }

  isReplay(nonce: string): boolean {
    return this._seen.has(nonce);
  }

  record(nonce: string): boolean {
    if (this._seen.has(nonce)) return false;
    if (this._seen.size >= this._maxSize) {
      this.evictOldest();
    }
    this._seen.add(nonce);
    this._timestamps.set(nonce, Date.now());
    return true;
  }

  private evictOldest(): void {
    let oldest: string | null = null;
    let oldestTs = Infinity;
    for (const [nonce, ts] of this._timestamps) {
      if (ts < oldestTs) {
        oldestTs = ts;
        oldest = nonce;
      }
    }
    if (oldest) {
      this._seen.delete(oldest);
      this._timestamps.delete(oldest);
    }
  }

  size(): number {
    return this._seen.size;
  }

  clear(): void {
    this._seen.clear();
    this._timestamps.clear();
  }
}
