/**
 * Canonical id validators — a self-contained core building block (imports
 * nothing from common/cli). AMI's own session/task/agent ids are deliberately
 * opaque, format-specific strings validated by their own purpose-built guards
 * ({@link ../session isValidSessionId}, `isSafeSegment`, the worktree id
 * regexes) — this module is NOT a replacement for those. It is the ONE canonical
 * validator for a value that genuinely claims the RFC-4122 `8-4-4-4-12` UUID
 * shape: a JSON-Schema `format: "uuid"` instance, a model-supplied tool argument
 * declared as a uuid, or a transcript `uuid`/`parentUuid` field (AMI mints those
 * via `crypto.randomUUID`, so they ARE canonical UUIDs).
 *
 * Parity: the reference's `validateUuid` (references/claude/claude-code-main/
 * src/utils/uuid.ts:L12-L17): accept `unknown`, type-guard to a string, match the
 * anchored canonical regex, and return the narrowed string or `null` — never
 * throw. Case-insensitive so an upper-case UUID is accepted too.
 */

/**
 * Anchored RFC-4122 canonical form: 8-4-4-4-12 hex groups. `^…$` anchors reject a
 * UUID embedded in surrounding text; the `i` flag accepts upper-case hex.
 */
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Validate that an UNKNOWN, untrusted value is a canonical UUID. Returns the value
 * narrowed to `string` when it is a well-formed `8-4-4-4-12` hex UUID, or `null`
 * for a non-string or any string that is not exactly a canonical UUID (empty,
 * too long/short, wrong grouping, non-hex, or a UUID with surrounding text).
 *
 * This is a BOUNDARY guard: callers use it to narrow a caller/model-supplied id
 * argument up front instead of letting a malformed value fall through to a
 * guaranteed lookup-miss. Mirrors the reference's `validateUuid(maybeUuid): UUID | null`.
 */
export function validateUuid(maybeUuid: unknown): string | null {
  if (typeof maybeUuid !== 'string') return null;
  return UUID_RE.test(maybeUuid) ? maybeUuid : null;
}

/**
 * --- Human-facing short id formatter ---------------------------------------
 *
 * A short identifier a PERSON has to read off a screen (or a phone text) and
 * possibly type back. Unlike {@link validateUuid}'s opaque hex UUIDs, a
 * human-surfaced code has two deliberate formatting constraints that a raw
 * hash/`randomBytes` id does not meet:
 *   1. CONFUSABLE-FREE alphabet — it drops `l` (looks like `1`/`I` in many
 *      fonts) so a reader can't mistype it, and uses letters only so a phone
 *      user never has to toggle keyboard modes (hex alternates a-f/0-9).
 *   2. PROFANITY-SAFE — five random letters can accidentally spell something
 *      you would not want sent to a user's phone; the generated code is checked
 *      against a substring blocklist and re-hashed with a salt until clean.
 *
 * Parity: the reference's `shortRequestId` / `ID_ALPHABET` / `ID_AVOID_SUBSTRINGS`
 * (references/claude/claude-code-main/src/services/mcp/channelPermissions.ts
 * :L77-L152). This is the ONE canonical confusable-safe, profanity-safe short-id
 * minter for any human-surfaced generated identifier (request/notification codes,
 * vanity session/worktree labels) — callers reuse it instead of hand-rolling a
 * blocklist (AGENTS.md No-Duplication Rule).
 */

/**
 * 25-letter alphabet: `a`-`z` minus `l` (looks like `1`/`I` in many fonts).
 * `25^5 ≈ 9.8M` space. Exported so a caller that needs the character set (e.g.
 * a reply-matching regex `[a-km-z]{5}`) reuses the exact alphabet rather than
 * re-deriving it. Parity: the reference's `ID_ALPHABET`.
 */
export const HUMAN_ID_ALPHABET = 'abcdefghijkmnopqrstuvwxyz';

/** Number of letters in a generated short code. Parity: the reference's 5-letter code. */
export const HUMAN_ID_LENGTH = 5;

/**
 * Substring blocklist — five random letters can spell things you do not want in
 * a message to someone's phone. Non-exhaustive; covers the
 * send-to-your-boss-by-accident tier. If a generated code contains any of these
 * it is re-hashed with a salt. Frozen so a consumer can read but not mutate the
 * shared list. Parity: the reference's `ID_AVOID_SUBSTRINGS`.
 */
export const ID_AVOID_SUBSTRINGS: readonly string[] = Object.freeze([
  'fuck', 'shit', 'cunt', 'cock', 'dick', 'twat', 'piss', 'crap', 'bitch',
  'whore', 'ass', 'tit', 'cum', 'fag', 'dyke', 'nig', 'kike', 'rape', 'nazi',
  'damn', 'poo', 'pee', 'wank', 'anus',
]);

/**
 * Maximum re-hash attempts when a candidate spells a blocklisted substring.
 * ~13.9k blocked codes out of 9.8M ≈ 1-in-700 hit rate, so `(1/700)^10` is
 * negligible; after the cap we return the last candidate rather than loop
 * forever. Parity: the reference's `salt < 10` bound.
 */
const HUMAN_ID_MAX_SALT = 10;

/**
 * FNV-1a hash of `input` → uint32 → base-25 encode over {@link HUMAN_ID_ALPHABET}.
 * Not cryptographic — a stable, letters-only short code. Deterministic: the same
 * input always yields the same code (so the same request maps to one display id).
 */
function hashToHumanId(input: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  h = h >>> 0;
  let s = '';
  for (let i = 0; i < HUMAN_ID_LENGTH; i++) {
    s += HUMAN_ID_ALPHABET[h % 25];
    h = Math.floor(h / 25);
  }
  return s;
}

/**
 * Derive a confusable-free, profanity-safe short code (5 letters from a 25-char
 * alphabet) from an arbitrary source string (e.g. a tool-use id, a structured
 * request id, a session uuid). Deterministic for a given input; re-hashes with a
 * `:salt` suffix (up to {@link HUMAN_ID_MAX_SALT} times) if the candidate spells
 * a blocklisted substring, so a code surfaced to a human never accidentally reads
 * as a slur. Mirrors the reference's `shortRequestId(toolUseID)`.
 */
export function shortRequestId(source: string): string {
  let candidate = hashToHumanId(source);
  for (let salt = 0; salt < HUMAN_ID_MAX_SALT; salt++) {
    if (!ID_AVOID_SUBSTRINGS.some((bad) => candidate.includes(bad))) {
      return candidate;
    }
    candidate = hashToHumanId(`${source}:${salt}`);
  }
  return candidate;
}
