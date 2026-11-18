/**
 * U253-w112 — near-duplicate index-pointer resolution (gap: "Contradiction/drift
 * resolution at the source — merge + delete contradicted facts").
 *
 * Claude's consolidation prompt resolves drift by "merging new signal into
 * existing topic files rather than creating near-duplicates". The structural
 * counterpart in AMI: the MEMORY.md index is ONE pointer per topic file, so
 * `computeIndexAppend` must skip a second row that points at the same target file
 * even when its title/hook differ — not just byte-identical rows. These tests lock
 * that behavior and the `indexEntryTarget` parser it relies on.
 */

import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { computeIndexAppend, indexEntryTarget, MemoryManager } from '../src/memory';

describe('U253-w112 — indexEntryTarget parser', () => {
  it('extracts the markdown link target', () => {
    assert.equal(indexEntryTarget('- [Auth](auth.md) — JWT approach'), 'auth.md');
    assert.equal(indexEntryTarget('- [Nested](sub/dir/topic.md) — x'), 'sub/dir/topic.md');
  });

  it('trims surrounding whitespace inside the link', () => {
    assert.equal(indexEntryTarget('- [T]( spaced.md ) — x'), 'spaced.md');
  });

  it('returns null for a line without a link', () => {
    assert.equal(indexEntryTarget('just some text mentioning auth.md'), null);
    assert.equal(indexEntryTarget(''), null);
  });
});

describe('U253-w112 — computeIndexAppend near-duplicate (same-target) guard', () => {
  it('appends a pointer to a NEW target file', () => {
    assert.equal(
      computeIndexAppend('- [a](a.md) — x\n', '- [b](b.md) — y'),
      '- [a](a.md) — x\n- [b](b.md) — y\n',
    );
  });

  it('skips a second pointer to the SAME target even with a different title/hook', () => {
    // Same target (auth.md), different title AND hook → near-duplicate → skip.
    assert.equal(
      computeIndexAppend('- [Auth](auth.md) — JWT approach\n', '- [Authentication](auth.md) — session cookies'),
      null,
    );
  });

  it('still skips a byte-identical row (exact dedup unchanged)', () => {
    assert.equal(computeIndexAppend('- [a](a.md) — x\n', '- [a](a.md) — x'), null);
  });

  it('does NOT false-match when a hook merely mentions the filename', () => {
    // The existing row points at other.md; its hook mentions "auth.md" as text —
    // a per-line target parse must not treat that as an auth.md pointer.
    assert.equal(
      computeIndexAppend('- [Other](other.md) — see also auth.md\n', '- [Auth](auth.md) — real pointer'),
      '- [Other](other.md) — see also auth.md\n- [Auth](auth.md) — real pointer\n',
    );
  });

  it('appends link-less entries as before (no target → no near-dup check)', () => {
    assert.equal(
      computeIndexAppend('- plain note\n', '- another plain note'),
      '- plain note\n- another plain note\n',
    );
  });
});

describe('U253-w112 — addToMemoryIndex end-to-end does not fragment a topic', () => {
  let tmp: string;
  beforeEach(() => { tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'near-dup-idx-')); });
  afterEach(() => { fs.rmSync(tmp, { recursive: true, force: true }); });

  it('keeps a single pointer per topic file across re-adds with a changed hook', async () => {
    const mgr = new MemoryManager(tmp);
    await mgr.addToMemoryIndex(tmp, '- [Deploy](deploy.md) — GitHub Actions');
    // A later pass learns more about the same topic file and re-indexes with a
    // richer hook: the index must stay ONE row for deploy.md, not fragment.
    await mgr.addToMemoryIndex(tmp, '- [Deploy pipeline](deploy.md) — GitHub Actions + canary');
    const idx = await mgr.readMemoryIndex(tmp);
    assert.equal(idx.filter((l) => l.includes('deploy.md')).length, 1);
    assert.deepEqual(idx, ['- [Deploy](deploy.md) — GitHub Actions']);
  });
});
