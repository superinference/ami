import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  computeAgentCharContribution,
  normalizeAttributionPath,
  buildAttributionData,
  recordAgentFileContribution,
  getSessionFileAttribution,
} from '../src/tools/attribution-store';

const dirs: string[] = [];
function mkTmp(prefix: string): string {
  const d = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(d);
  return d;
}
after(() => {
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// computeAgentCharContribution — char-level common prefix/suffix (gap: char-level)
// ---------------------------------------------------------------------------

describe('U264 — computeAgentCharContribution', () => {
  it('counts the whole new content for a brand-new file', () => {
    assert.equal(computeAgentCharContribution('', 'abcdef'), 6);
  });

  it('counts the whole old content for a full deletion', () => {
    assert.equal(computeAgentCharContribution('abcdef', ''), 6);
  });

  it('scores a same-length replacement a naive length delta would miss', () => {
    // "Esc" -> "esc" differs in exactly one char; abs(len) would be 0.
    assert.equal(computeAgentCharContribution('Esc', 'esc'), 1);
  });

  it('isolates an interior change region (prefix + suffix shared)', () => {
    // shared prefix "foo(", shared suffix ")", changed middle "a" -> "bbb".
    assert.equal(computeAgentCharContribution('foo(a)', 'foo(bbb)'), 3);
  });

  it('takes max(oldChanged, newChanged) when the replacement shrinks the file', () => {
    // "foo(bbb)" -> "foo(a)": old changed region "bbb" (3) > new "a" (1).
    assert.equal(computeAgentCharContribution('foo(bbb)', 'foo(a)'), 3);
  });

  it('returns 0 when nothing changed', () => {
    assert.equal(computeAgentCharContribution('identical', 'identical'), 0);
  });
});

// ---------------------------------------------------------------------------
// normalizeAttributionPath — realpath + forward-slash repo-relative key (gap: path norm)
// ---------------------------------------------------------------------------

describe('U264 — normalizeAttributionPath', () => {
  it('returns a relative input unchanged', () => {
    assert.equal(normalizeAttributionPath('src/a.ts', '/repo'), 'src/a.ts');
  });

  it('computes a forward-slash repo-relative key for an absolute path under the root', () => {
    const root = mkTmp('u264-norm-');
    const file = path.join(root, 'src', 'a.ts');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, 'x');
    assert.equal(normalizeAttributionPath(file, root), 'src/a.ts');
  });

  it('reconciles symlinked paths so the key still matches the realpath root', () => {
    const real = mkTmp('u264-real-');
    const linkParent = mkTmp('u264-link-');
    const link = path.join(linkParent, 'linked');
    fs.symlinkSync(real, link);
    const file = path.join(real, 'f.ts');
    fs.writeFileSync(file, 'x');
    // Access the file THROUGH the symlink; realpath resolution must reconcile it
    // back to the real root so the relative key is still 'f.ts'.
    assert.equal(normalizeAttributionPath(path.join(link, 'f.ts'), real), 'f.ts');
  });

  it('falls back to the raw relative form for a not-yet-created file under the root', () => {
    const root = mkTmp('u264-missing-');
    // File does not exist (realpath of the file fails) but the root does.
    assert.equal(normalizeAttributionPath(path.join(root, 'new.ts'), root), 'new.ts');
  });

  it('returns the original absolute path when it is outside the repo root', () => {
    const root = mkTmp('u264-out-');
    assert.equal(normalizeAttributionPath('/elsewhere/x.ts', root), '/elsewhere/x.ts');
  });
});

// ---------------------------------------------------------------------------
// buildAttributionData — versioned schema + surfaceBreakdown + excludedGenerated
// ---------------------------------------------------------------------------

describe('U264 — buildAttributionData', () => {
  it('builds a version:1 payload with per-file agent/human chars and surface breakdown', () => {
    const data = buildAttributionData({
      surface: 'cli',
      sessions: ['s1'],
      stagedFiles: ['a.ts', 'b.ts'],
      contributions: [
        { path: 'a.ts', agentContribution: 30, totalChars: 100 },
        { path: 'b.ts', agentContribution: 50, totalChars: 50 },
      ],
    });
    assert.equal(data.version, 1);
    assert.deepEqual(data.sessions, ['s1']);
    assert.deepEqual(data.summary.surfaces, ['cli']);
    assert.equal(data.summary.agentChars, 80);
    assert.equal(data.summary.humanChars, 70); // (100-30) + (50-50)
    assert.equal(data.summary.agentPercent, Math.round((100 * 80) / 150));
    assert.deepEqual(data.files['a.ts'], {
      agentChars: 30,
      humanChars: 70,
      percent: 30,
      surface: 'cli',
    });
    assert.deepEqual(data.files['b.ts'], {
      agentChars: 50,
      humanChars: 0,
      percent: 100,
      surface: 'cli',
    });
    assert.equal(data.surfaceBreakdown['cli'].agentChars, 80);
    assert.equal(data.surfaceBreakdown['cli'].percent, Math.round((100 * 80) / 150));
    assert.deepEqual(data.excludedGenerated, []);
  });

  it('excludes generated staged files from authorship and lists them', () => {
    const data = buildAttributionData({
      surface: 'cli',
      sessions: ['s1'],
      stagedFiles: ['src/a.ts', 'dist/bundle.js', 'package-lock.json'],
      contributions: [
        { path: 'src/a.ts', agentContribution: 10, totalChars: 20 },
        // A tracked-but-generated path must not count toward authorship.
        { path: 'dist/bundle.js', agentContribution: 999, totalChars: 1000 },
      ],
    });
    assert.ok(data.excludedGenerated.includes('dist/bundle.js'));
    assert.ok(data.excludedGenerated.includes('package-lock.json'));
    assert.equal(Object.prototype.hasOwnProperty.call(data.files, 'dist/bundle.js'), false);
    assert.equal(data.summary.agentChars, 10);
  });

  it('clamps negative/degenerate inputs and yields a zero-signal payload when empty', () => {
    const data = buildAttributionData({
      surface: 'cli',
      sessions: [],
      stagedFiles: [],
      contributions: [{ path: 'weird.ts', agentContribution: -5, totalChars: -10 }],
    });
    assert.equal(data.files['weird.ts'].agentChars, 0);
    assert.equal(data.files['weird.ts'].humanChars, 0);
    assert.equal(data.summary.agentPercent, 0);
    assert.equal(data.summary.humanChars, 0);
  });

  it('omits the surfaceBreakdown entry when there are no tracked files at all', () => {
    const data = buildAttributionData({
      surface: 'cli',
      sessions: [],
      stagedFiles: ['only-generated.min.js'],
      contributions: [],
    });
    assert.deepEqual(data.surfaceBreakdown, {});
    assert.deepEqual(data.files, {});
    assert.ok(data.excludedGenerated.includes('only-generated.min.js'));
  });
});

// ---------------------------------------------------------------------------
// recordAgentFileContribution debug-hint path (both added>0 and deletion-only)
// ---------------------------------------------------------------------------

describe('U264 — recordAgentFileContribution emits the debug hint without throwing', () => {
  it('records a positive contribution (added > 0 branch)', () => {
    const sid = 'u264-rec-' + Math.random();
    recordAgentFileContribution(sid, 'f.ts', 'hello', 5, 1);
    assert.equal(getSessionFileAttribution(sid)['f.ts'].agentContribution, 5);
  });

  it('records a deletion-only edit (added <= 0 branch, no inflation)', () => {
    const sid = 'u264-del-' + Math.random();
    recordAgentFileContribution(sid, 'f.ts', 'he', 0, 2);
    assert.equal(getSessionFileAttribution(sid)['f.ts'].agentContribution, 0);
  });
});
