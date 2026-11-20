// Canonical temp-FILE-path generator (AGENTS.md No-Duplication Rule). Every AMI
// site that needs a throwaway file path should derive it here instead of
// re-spelling `path.join(tmpBase, prefix + <id> + ext)` with an ad-hoc id, so the
// two id strategies (unpredictable-random vs deterministic-content-hash) live in
// ONE place.
//
// Parity with the reference's `generateTempFilePath` (utils/tempfile.ts): a subtle
// dual-mode algorithm. The default id is an unpredictable `randomUUID()` (so a
// pre-planted symlink / predictable-path attacker cannot hijack the write
// target); when `options.contentHash` is supplied the id is instead the first 16
// hex chars of `sha256(contentHash)` so the SAME content yields the SAME path
// across process boundaries. The content-hash mode exists specifically for
// prompt-cache stability: when a temp path ends up inside content sent to the
// model (e.g. a sandbox deny-list embedded in a tool description), a per-process
// random path would change on every subprocess spawn and silently invalidate the
// Anthropic prompt-cache prefix; a content-derived path stays stable.

import { createHash, randomUUID } from 'crypto';
import * as path from 'path';
import { resolveTmpBase } from './shell';

/**
 * Build a temp-file path under the AMI temp base ({@link resolveTmpBase}, which
 * honors `AMI_TMPDIR`/`CLAUDE_CODE_TMPDIR` — a superset of the reference's plain
 * `os.tmpdir()`), using an unpredictable random id by default or a deterministic
 * content-hash id when `options.contentHash` is given.
 *
 * @param prefix     filename prefix (default `'ami-tmp'`).
 * @param extension  filename extension including the dot (default `'.md'`). An
 *                   empty string produces an extension-less name.
 * @param options.contentHash When provided, the id is `sha256(contentHash)`
 *                   sliced to 16 hex chars, so identical content → identical path
 *                   across processes (prompt-cache-stable). Omit it for a unique
 *                   per-call path.
 */
export function generateTempFilePath(
  prefix: string = 'ami-tmp',
  extension: string = '.md',
  options?: { contentHash?: string },
): string {
  const id = options?.contentHash !== undefined
    ? createHash('sha256').update(options.contentHash).digest('hex').slice(0, 16)
    : randomUUID();
  return path.join(resolveTmpBase(), `${prefix}-${id}${extension}`);
}
