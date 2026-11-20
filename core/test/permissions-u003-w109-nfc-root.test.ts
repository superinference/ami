import { test } from 'node:test';
import assert from 'node:assert/strict';
import { PermissionManager } from '../src/permissions';

// ---------------------------------------------------------------------------
// U003 / w109 - NFC Unicode normalization of granted read roots.
//
// Parity: the reference canonicalizes every allowlist/project-root path with
// `realpathSync(path).normalize('NFC')` (bootstrap/state.ts setOriginalCwd /
// setProjectRoot / setCwdState). AMI already NFC-normalizes the CANDIDATE path
// under check (resolveForCheck -> realpathNFC, expandPath), but the granted read
// ROOTS stored by registerAllowedRoot were only `path.normalize`d - never
// NFC-normalized. On an NFD-storing/paste source a root granted in decomposed
// form therefore could not byte-match an NFC-normalized candidate, so a file the
// user explicitly granted was still gated as "outside the workspace".
//
// [FUNCTIONAL CHANGE] registerAllowedRoot / removeAllowedRoot / the workspace
// containment comparison now apply `.normalize('NFC')`, mirroring the candidate
// path's canonicalization so both sides compare in one Unicode form.
// ---------------------------------------------------------------------------

// "cafe" with a DECOMPOSED (NFD) accented e = 'e' + U+0301 combining acute.
const NFD_DIR = '/parity-nfc-root/café';
// Same directory, PRECOMPOSED (NFC) accented e = U+00E9 - the identical path,
// but a DISTINCT byte string until NFC-normalized.
const NFC_DIR = '/parity-nfc-root/café';

test('NFD and NFC spellings are distinct byte strings (test-fixture sanity)', () => {
  assert.notEqual(NFD_DIR, NFC_DIR);
  assert.equal(NFD_DIR.normalize('NFC'), NFC_DIR);
});

test('registerAllowedRoot canonicalizes the stored root to NFC', () => {
  const pm = new PermissionManager('ask');
  pm.registerAllowedRoot(NFD_DIR);
  const roots = pm.getAllowedReadRoots();
  assert.ok(
    roots.includes(NFC_DIR),
    `expected the NFD-spelled root to be stored in NFC form (${JSON.stringify(roots)})`,
  );
  // The decomposed spelling must NOT linger as a second, distinct key.
  assert.ok(!roots.includes(NFD_DIR), 'the decomposed spelling must not remain as a separate key');
});

test('a read root granted in NFD form matches an NFC-canonicalized candidate', () => {
  const pm = new PermissionManager('ask');
  // Grant the root using the DECOMPOSED spelling.
  pm.registerAllowedRoot(NFD_DIR);
  // A file under that root (addressed in the decomposed spelling) must classify
  // as an in-workspace read, NOT a prompt - the candidate is NFC-canonicalized by
  // resolveForCheck, so the stored root must be canonical too for the prefix match.
  const candidate = `${NFD_DIR}/secret.txt`;
  const verdict = pm.classifyReadAccess(candidate, '/some/unrelated/cwd');
  assert.equal(verdict, 'allow');
});

test('removeAllowedRoot drops an NFD-registered root when given the NFC spelling', () => {
  const pm = new PermissionManager('ask');
  pm.registerAllowedRoot(NFD_DIR);
  // Removal via the precomposed spelling must still find the root, because both
  // are canonicalized to the same NFC key.
  const removed = pm.removeAllowedRoot(NFC_DIR);
  assert.equal(removed, true);
  assert.deepEqual(pm.getAllowedReadRoots(), []);
});
