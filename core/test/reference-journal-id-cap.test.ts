/**
 * the reference-parity (U058): filename-length boundary guard on the sanitized session/
 * agent id used in journal filenames — truncate + stable hash suffix, mirroring
 * the projectSlug cap. A long id must never overflow a filesystem path component
 * (ENAMETOOLONG) and two long ids differing only past the cap must not collide.
 *
 * Run: cd core/core && node --import tsx --test test/reference-journal-id-cap.test.ts
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'path';
import {
  journalPathFor,
  subagentJournalPathFor,
  MAX_SESSION_ID_LENGTH,
} from '../src/reference-journal';

const ENV = { SUPERINFERENCE_DATA_DIR: '/data' } as unknown as NodeJS.ProcessEnv;
const CWD = '/home/me/ui';

/** The final path component of a built journal path. */
function fileComponent(p: string): string {
  return path.basename(p);
}

describe('session/agent id filename length cap (U058)', () => {
  it('leaves a short session id unchanged', () => {
    assert.equal(fileComponent(journalPathFor(CWD, 'sess1', ENV)), 'sess1.jsonl');
  });

  it('caps a very long session id so the filename component stays bounded', () => {
    const longId = 'x'.repeat(5000);
    const comp = fileComponent(journalPathFor(CWD, longId, ENV));
    // component = <=MAX_SESSION_ID_LENGTH id chars + '.jsonl'
    assert.ok(
      comp.length <= MAX_SESSION_ID_LENGTH + '.jsonl'.length,
      `filename component length ${comp.length} must be bounded`,
    );
    // stays within a single fs path component limit (255 bytes)
    assert.ok(comp.length < 255, `must be < 255 bytes, got ${comp.length}`);
  });

  it('caps a very long subagent id too (agent- prefix accounted for)', () => {
    const longId = 'a'.repeat(5000);
    const comp = fileComponent(subagentJournalPathFor(CWD, longId, ENV));
    assert.ok(comp.startsWith('agent-'), comp);
    assert.ok(comp.length < 255, `must be < 255 bytes, got ${comp.length}`);
  });

  it('is deterministic — same long id yields the same filename', () => {
    const longId = 'z'.repeat(4096);
    assert.equal(journalPathFor(CWD, longId, ENV), journalPathFor(CWD, longId, ENV));
  });

  it('two long ids differing only past the cap get distinct files (hash disambiguates)', () => {
    const base = 'q'.repeat(MAX_SESSION_ID_LENGTH + 50);
    const a = journalPathFor(CWD, base + 'ALPHA', ENV);
    const b = journalPathFor(CWD, base + 'BETA', ENV);
    assert.notEqual(a, b, 'the hash suffix must disambiguate colliding long prefixes');
  });

  it('still neutralizes traversal / separators before capping', () => {
    const p = journalPathFor(CWD, '../../etc/passwd', ENV);
    const base = path.join('/data', 'projects', '-home-me-ui');
    assert.ok(p.startsWith(base), p);
    assert.ok(!path.basename(p).includes('/'), 'no separator survives');
  });
});
