import { describe, it, before, after, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  suggestSimilarPaths,
  suggestPathUnderCwd,
  suggestPathCorrection,
} from '../src/tools/path-suggest';
import { getInMemoryErrors, __resetLoggerForTest } from '../src/logger';
import { fileReadTool } from '../src/tools/file-read';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U218 — core/core/src/tools/path-suggest.ts parity superset.
// Each block maps to a labeled ADDITIVE feature and is red-green validated.
// ---------------------------------------------------------------------------

describe('U218 security: hidden/dotfile + gitignore filtering of suggestions', () => {
  // A near-miss of a SECRET dotfile must never be surfaced — doing so leaks the
  // existence of credential files (.env, keys) the model/user has no business
  // learning about. Mirrors Claude Code's suggestion pipeline filtering ignored
  // files out of results (hooks/fileSuggestions.ts:289-295).
  it('does NOT suggest a dotfile neighbour for a near-miss hidden path', () => {
    const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-dot-')));
    try {
      fs.writeFileSync(path.join(tmp, '.env'), 'SECRET=1');
      const got = suggestSimilarPaths(path.join(tmp, '.en'), tmp);
      assert.ok(
        !got.some(p => p.endsWith('.env')),
        `expected .env to be filtered, got ${JSON.stringify(got)}`,
      );
      assert.deepEqual(got, []);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  // A non-dotfile that is GITIGNORED must also be filtered (e.g. a build-emitted
  // `secret-config.json` listed in .gitignore). The dotfile rule alone misses
  // these, so the gitignore matcher is exercised distinctly here.
  it('does NOT suggest a gitignored (non-dotfile) neighbour', () => {
    const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-gi-')));
    try {
      fs.mkdirSync(path.join(tmp, '.git'), { recursive: true }); // project root marker
      fs.writeFileSync(path.join(tmp, '.gitignore'), 'secret-config.json\n');
      fs.writeFileSync(path.join(tmp, 'secret-config.json'), '{}');
      const got = suggestSimilarPaths(path.join(tmp, 'secret-config.jsom'), tmp);
      assert.ok(
        !got.some(p => p.endsWith('secret-config.json')),
        `expected gitignored file filtered, got ${JSON.stringify(got)}`,
      );
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  // Regression guard: ordinary (tracked, non-hidden) near-miss files are STILL
  // suggested — the filter must not swallow legitimate "did you mean" hits.
  it('still suggests ordinary tracked near-miss files', () => {
    const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-ok-')));
    try {
      fs.mkdirSync(path.join(tmp, '.git'), { recursive: true });
      fs.writeFileSync(path.join(tmp, '.gitignore'), 'dist/\n');
      fs.writeFileSync(path.join(tmp, 'index.ts'), '');
      const got = suggestSimilarPaths(path.join(tmp, 'indx.ts'), tmp);
      assert.ok(
        got.some(p => p.endsWith('index.ts')),
        `expected index.ts suggested, got ${JSON.stringify(got)}`,
      );
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

describe('U218 security: suggestPathCorrection withholds dotfile case-variants', () => {
  it('does NOT return a dotfile as an exact-case correction', () => {
    const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-pc-')));
    try {
      fs.writeFileSync(path.join(tmp, '.env'), 'SECRET=1');
      const r = suggestPathCorrection(path.join(tmp, '.ENV'), tmp);
      assert.ok(
        !r.suggestions.some(p => p.endsWith('.env')),
        `expected .env withheld, got ${JSON.stringify(r)}`,
      );
      assert.notEqual(r.kind, 'exact-case');
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});

describe('U218 security: workspace-boundary containment for path enumeration', () => {
  // suggestSimilarPaths must not enumerate a directory OUTSIDE the caller's cwd.
  // An out-of-tree target would otherwise disclose sibling-filesystem contents.
  // Mirrors Claude's cwd-scoped suggestion enumeration (hooks/fileSuggestions.ts
  // :243-316).
  it('returns [] when the target directory is outside cwd', () => {
    const cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-cwd-')));
    const outside = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-out-')));
    try {
      fs.writeFileSync(path.join(outside, 'index.ts'), '');
      const got = suggestSimilarPaths(path.join(outside, 'indx.ts'), cwd);
      assert.deepEqual(got, []);
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
      fs.rmSync(outside, { recursive: true, force: true });
    }
  });

  // In-tree enumeration (dir under cwd, and dir === cwd) still works.
  it('still enumerates a directory nested under cwd', () => {
    const cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-in-')));
    try {
      fs.mkdirSync(path.join(cwd, 'src'), { recursive: true });
      fs.writeFileSync(path.join(cwd, 'src', 'index.ts'), '');
      const got = suggestSimilarPaths(path.join(cwd, 'src', 'indx.ts'), cwd);
      assert.ok(got.some(p => p.endsWith('index.ts')), JSON.stringify(got));
    } finally {
      fs.rmSync(cwd, { recursive: true, force: true });
    }
  });
});

describe('U218 same-stem / different-extension suggestion (findSimilarFile parity)', () => {
  let tmp: string;

  before(() => {
    tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-stem-')));
    // config.toml shares the stem of the missing config.ts but the extension
    // differs by > maxDistance (3) characters, so a pure levenshtein scan drops
    // it (distance 'config.ts' vs 'config.toml' = 4).
    fs.writeFileSync(path.join(tmp, 'config.toml'), '');
    fs.writeFileSync(path.join(tmp, 'notes.markdown'), '');
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  // RED (revert): with the levenshtein-only filter (dist <= budget, budget=3)
  // 'config.toml' is never surfaced for a missing 'config.ts'.
  it('suggests same-stem file whose extension differs beyond the distance cap', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'config.ts'), tmp);
    assert.ok(
      got.some(p => p.endsWith('config.toml')),
      `expected config.toml among ${JSON.stringify(got)}`,
    );
  });

  it('suggests same-stem file for a long, very different extension', () => {
    const got = suggestSimilarPaths(path.join(tmp, 'notes.md'), tmp);
    assert.ok(
      got.some(p => p.endsWith('notes.markdown')),
      `expected notes.markdown among ${JSON.stringify(got)}`,
    );
  });

  it('ranks same-stem matches ahead of plain typo matches', () => {
    const d = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-rank-')));
    try {
      fs.writeFileSync(path.join(d, 'config.toml'), ''); // same stem, far ext
      fs.writeFileSync(path.join(d, 'configg.ts'), ''); // typo, distance 1
      const got = suggestSimilarPaths(path.join(d, 'config.ts'), d);
      assert.equal(path.basename(got[0]), 'config.toml');
    } finally {
      fs.rmSync(d, { recursive: true, force: true });
    }
  });
});

describe('U218 suggestPathUnderCwd (dropped-repo-folder recovery)', () => {
  let tmp: string;
  let repo: string;

  before(() => {
    tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-cwd-')));
    repo = path.join(tmp, 'currentRepo');
    fs.mkdirSync(path.join(repo, 'src'), { recursive: true });
    fs.writeFileSync(path.join(repo, 'foobar.txt'), 'hello');
    fs.writeFileSync(path.join(repo, 'src', 'app.ts'), 'x');
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  // RED (revert / before implementation): suggestPathUnderCwd did not exist.
  it('re-anchors a sibling-of-repo path under cwd when it exists there', () => {
    const requested = path.join(tmp, 'foobar.txt'); // sibling of repo, missing
    assert.equal(suggestPathUnderCwd(requested, repo), path.join(repo, 'foobar.txt'));
  });

  it('re-anchors a nested sibling path under cwd', () => {
    const requested = path.join(tmp, 'src', 'app.ts');
    assert.equal(suggestPathUnderCwd(requested, repo), path.join(repo, 'src', 'app.ts'));
  });

  it('returns undefined when the corrected path does not exist under cwd', () => {
    const requested = path.join(tmp, 'missing.txt');
    assert.equal(suggestPathUnderCwd(requested, repo), undefined);
  });

  it('returns undefined when the requested path is already under cwd', () => {
    const requested = path.join(repo, 'nope.txt');
    assert.equal(suggestPathUnderCwd(requested, repo), undefined);
  });

  it('returns undefined when the requested path is outside cwd parent', () => {
    const other = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-other-')));
    try {
      assert.equal(suggestPathUnderCwd(path.join(other, 'x.txt'), repo), undefined);
    } finally {
      fs.rmSync(other, { recursive: true, force: true });
    }
  });
});

describe('U218 integration — file_read surfaces dropped-repo-folder correction', () => {
  let tmp: string;
  let repo: string;

  function ctx(overrides?: Partial<ToolContext>): ToolContext {
    return {
      cwd: repo,
      abortSignal: new AbortController().signal,
      ...overrides,
    };
  }

  before(() => {
    tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-fr-')));
    repo = path.join(tmp, 'currentRepo');
    fs.mkdirSync(repo, { recursive: true });
    fs.writeFileSync(path.join(repo, 'foobar.txt'), 'the real file');
  });
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  // RED (revert the file-read wiring): the message lacks the cwd correction note.
  it('includes a "Did you mean" note pointing under cwd for a sibling-of-repo path', async () => {
    const requested = path.join(tmp, 'foobar.txt'); // sibling of repo, missing
    // A broader read root (tmp) is permitted, so the sibling path passes the
    // workspace boundary check and reaches the not-found suggestion branch.
    const result = await fileReadTool.execute(
      { file_path: requested },
      ctx({ allowedReadRoots: [tmp] }),
    );
    assert.equal(result.isError, true);
    const out = String(result.output);
    assert.ok(out.includes('File not found or not readable'), out);
    assert.ok(out.includes('your current working directory is'), out);
    assert.ok(out.includes(path.join(repo, 'foobar.txt')), out);
  });
});

describe('U218 error observability on failed directory scan', () => {
  beforeEach(() => __resetLoggerForTest());
  afterEach(() => __resetLoggerForTest());

  // RED (revert the logScanError call): a non-ENOENT scan error is swallowed and
  // never appears in the in-memory error ring.
  it('logs a non-ENOENT (ENOTDIR) directory scan failure', () => {
    const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-enotdir-')));
    try {
      const notADir = path.join(tmp, 'file');
      fs.writeFileSync(notADir, 'x');
      // dirname of the requested path is a FILE → readdirSync throws ENOTDIR.
      const got = suggestSimilarPaths(path.join(notADir, 'child.ts'), tmp);
      assert.deepEqual(got, []);
      const errs = getInMemoryErrors();
      assert.ok(
        errs.some(e => e.error.includes('suggestSimilarPaths') && e.error.includes('directory scan failed')),
        `expected a logged scan error, got ${JSON.stringify(errs)}`,
      );
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });

  // A plain missing parent directory (ENOENT) is the expected case and must stay
  // quiet — no log noise.
  it('does NOT log an ENOENT (missing parent directory) scan', () => {
    const tmp = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'psug-enoent-')));
    try {
      const got = suggestSimilarPaths(path.join(tmp, 'nope', 'child.ts'), tmp);
      assert.deepEqual(got, []);
      const errs = getInMemoryErrors();
      assert.equal(
        errs.some(e => e.error.includes('directory scan failed')),
        false,
      );
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  });
});
