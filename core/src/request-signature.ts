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
  // The supplied signature is attacker-controlled: a forged/truncated/odd-length
  // or non-hex value makes Buffer.from(...,'hex') silently truncate and
  // crypto.timingSafeEqual throw a RangeError on a length mismatch. Reject such
  // input up front (constant-length hex only) and defensively wrap the compare so
  // a malformed signature returns {valid:false} instead of crashing the verifier.
  if (typeof sig.signature !== 'string' || !/^[0-9a-f]+$/i.test(sig.signature) || sig.signature.length !== expected.length) {
    return { valid: false, reason: 'signature malformed' };
  }
  try {
    const valid = crypto.timingSafeEqual(Buffer.from(sig.signature, 'hex'), Buffer.from(expected, 'hex'));
    return valid ? { valid: true } : { valid: false, reason: 'signature mismatch' };
  } catch {
    return { valid: false, reason: 'signature malformed' };
  }
}

/**
 * Replay guard with constant-memory, O(1) FIFO eviction.
 *
 * At capacity every new nonce evicts the oldest in O(1) via a fixed-size ring
 * buffer (write index points at the slot to overwrite, which is always the
 * oldest entry) plus a Set for O(1) membership. This replaces the previous
 * Set + parallel timestamp Map whose evictOldest() did an O(n) linear scan on
 * every record() once full — a degenerate boundary cost an attacker could
 * sustain under a replay flood, exactly the scenario this guard defends.
 */
export class NonceTracker {
  private _seen = new Set<string>();
  private _maxSize: number;
  private _ring: Array<string | undefined>;
  private _writeIdx = 0;

  constructor(maxSize: number = 10_000) {
    this._maxSize = Math.max(1, maxSize);
    this._ring = new Array<string | undefined>(this._maxSize);
  }

  isReplay(nonce: string): boolean {
    return this._seen.has(nonce);
  }

  record(nonce: string): boolean {
    if (this._seen.has(nonce)) return false;
    // The slot at _writeIdx holds the oldest live nonce once the ring is full;
    // overwriting it evicts that entry in O(1).
    const evicted = this._ring[this._writeIdx];
    if (evicted !== undefined) {
      this._seen.delete(evicted);
    }
    this._ring[this._writeIdx] = nonce;
    this._writeIdx = (this._writeIdx + 1) % this._maxSize;
    this._seen.add(nonce);
    return true;
  }

  size(): number {
    return this._seen.size;
  }

  clear(): void {
    this._seen.clear();
    this._ring = new Array<string | undefined>(this._maxSize);
    this._writeIdx = 0;
  }
}
