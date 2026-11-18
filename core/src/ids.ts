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
 * Parity: Claude Code's `validateUuid` (references/claude/claude-code-main/
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
 * guaranteed lookup-miss. Mirrors Claude's `validateUuid(maybeUuid): UUID | null`.
 */
export function validateUuid(maybeUuid: unknown): string | null {
  if (typeof maybeUuid !== 'string') return null;
  return UUID_RE.test(maybeUuid) ? maybeUuid : null;
}
