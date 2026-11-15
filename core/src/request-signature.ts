import * as crypto from 'crypto';

export interface RequestSignature {
  nonce: string;
  timestamp: number;
  signature: string;
}

/**
 * HMAC hash algorithms this module is willing to sign/verify with. Algorithm
 * agility is intentionally a *closed allowlist*: the algorithm is always chosen
 * by the caller (signer and verifier), NEVER derived from the attacker-controlled
 * {@link RequestSignature} payload — so there is no algorithm-confusion /
 * downgrade surface. An unknown algorithm is a programmer error and throws.
 */
export const SUPPORTED_SIGNATURE_ALGORITHMS = ['sha256', 'sha384', 'sha512'] as const;
export type SignatureAlgorithm = (typeof SUPPORTED_SIGNATURE_ALGORITHMS)[number];

function assertAlgorithm(algorithm: string): asserts algorithm is SignatureAlgorithm {
  if (!(SUPPORTED_SIGNATURE_ALGORITHMS as readonly string[]).includes(algorithm)) {
    throw new Error(
      `unsupported signature algorithm: ${algorithm} ` +
        `(allowed: ${SUPPORTED_SIGNATURE_ALGORITHMS.join(', ')})`,
    );
  }
}

/**
 * A non-empty string secret is mandatory. An empty (or non-string) HMAC key
 * provides essentially no security — the resulting MAC is a public function of
 * the message that anyone can recompute — so signing with one is rejected.
 */
function assertSecret(secret: unknown): asserts secret is string {
  if (typeof secret !== 'string' || secret.length === 0) {
    throw new Error('request signature secret must be a non-empty string');
  }
}

/**
 * Deterministic, injection-proof canonicalization (FUNCTIONAL CHANGE — signature
 * format).
 *
 * The previous canonical form joined fields with a literal `\n`:
 * `METHOD\nPATH\nTS\nNONCE\nBODY`. Because `path`, `body` and the
 * attacker-controlled `sig.nonce` may themselves contain `\n`, the boundaries
 * were ambiguous: distinct logical requests could collapse to the *same* byte
 * string and therefore share one valid signature (canonicalization injection).
 * For example `(method="POST\nA", path="B")` and `(method="POST", path="A\nB")`
 * both produced `POST\nA\nB\n...`, and an attacker could shift bytes across the
 * nonce/body boundary to reuse a signature for a different body.
 *
 * Each field is now length-prefixed (`<utf8-byte-length>:<field>`), which makes
 * the encoding an injective (collision-free) function of the field vector — no
 * delimiter inside a field can be reinterpreted as a field separator. Signer and
 * verifier share this helper so round-trips remain exact.
 */
function canonicalString(fields: Array<string | number>): string {
  return fields
    .map((f) => {
      const s = String(f);
      return `${Buffer.byteLength(s, 'utf8')}:${s}`;
    })
    .join('|');
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
  algorithm: SignatureAlgorithm = 'sha256',
): string {
  assertSecret(secret);
  assertAlgorithm(algorithm);
  const payload = canonicalString([method.toUpperCase(), path, timestamp, body]);
  return crypto.createHmac(algorithm, secret).update(payload).digest('hex');
}

export function createRequestSignature(
  method: string,
  path: string,
  body: string,
  secret: string,
  algorithm: SignatureAlgorithm = 'sha256',
): RequestSignature {
  assertSecret(secret);
  assertAlgorithm(algorithm);
  const nonce = generateNonce();
  const timestamp = Date.now();
  const payload = canonicalString([method.toUpperCase(), path, timestamp, nonce, body]);
  const signature = crypto.createHmac(algorithm, secret).update(payload).digest('hex');
  return { nonce, timestamp, signature };
}

export function verifyRequestSignature(
  sig: RequestSignature,
  method: string,
  path: string,
  body: string,
  secret: string,
  maxAgeMs: number = 300_000,
  algorithm: SignatureAlgorithm = 'sha256',
): { valid: boolean; reason?: string } {
  // The secret and algorithm are server-side configuration, never attacker input.
  // An empty secret or unknown algorithm is a deployment/programmer error — fail
  // loudly rather than silently verifying against a weak/public key. (An unknown
  // algorithm throws; an empty secret is reported, preserving the "verifier does
  // not throw on *attacker* input" contract below.)
  assertAlgorithm(algorithm);
  if (typeof secret !== 'string' || secret.length === 0) {
    return { valid: false, reason: 'invalid secret' };
  }

  // `sig` may be a deserialized, fully attacker-controlled object. Validate the
  // replay-protection fields up front: a non-finite / non-integer / negative
  // timestamp would silently BYPASS the freshness window (every `NaN`
  // comparison below is false), and an empty / non-string nonce defeats
  // NonceTracker replay tracking. Reject both explicitly.
  if (
    typeof sig.timestamp !== 'number' ||
    !Number.isFinite(sig.timestamp) ||
    !Number.isInteger(sig.timestamp) ||
    sig.timestamp < 0
  ) {
    return { valid: false, reason: 'invalid timestamp' };
  }
  if (typeof sig.nonce !== 'string' || sig.nonce.length === 0) {
    return { valid: false, reason: 'invalid nonce' };
  }

  const now = Date.now();
  if (now - sig.timestamp > maxAgeMs) {
    return { valid: false, reason: 'signature expired' };
  }
  if (sig.timestamp > now + 60_000) {
    return { valid: false, reason: 'signature timestamp in future' };
  }
  const payload = canonicalString([method.toUpperCase(), path, sig.timestamp, sig.nonce, body]);
  const expected = crypto.createHmac(algorithm, secret).update(payload).digest('hex');
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
