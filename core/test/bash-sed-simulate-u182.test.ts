import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  parseSedEditCommand,
  canSimulateSedEditFaithfully,
  computeSedEdit,
  applySimulatedSedEdit,
  applySedSubstitution,
} from '../src/tools/bash-sed-parser';
import { bashTool } from '../src/tools/bash';
import type { ToolContext } from '../src/types';

// ---------------------------------------------------------------------------
// U182 — _simulatedSedEdit exactness guarantee: the approved write uses the
// JS-computed newContent (applySedSubstitution), not a re-run of the real sed
// binary. parseSedEditCommand now records the backup suffix; computeSedEdit /
// applySimulatedSedEdit give preview and write a single source of truth, gated
// by canSimulateSedEditFaithfully so only substitutions the applier reproduces
// exactly are written in-memory (others fall back to the real binary).
// ---------------------------------------------------------------------------

describe('parseSedEditCommand – inPlaceSuffix', () => {
  it('omits inPlaceSuffix for a plain -i (shape unchanged)', () => {
    assert.deepEqual(parseSedEditCommand("sed -i 's/foo/bar/' file.txt"), {
      filePath: 'file.txt',
      pattern: 'foo',
      replacement: 'bar',
      flags: '',
      extendedRegex: false,
    });
  });

  it('omits inPlaceSuffix for --in-place', () => {
    assert.equal(parseSedEditCommand("sed --in-place 's/a/b/' f")?.inPlaceSuffix, undefined);
  });

  it('records the inline -i.bak suffix', () => {
    assert.equal(parseSedEditCommand("sed -i.bak 's/a/b/' f")?.inPlaceSuffix, '.bak');
  });

  it('records a separate dotted backup suffix', () => {
    assert.equal(parseSedEditCommand("sed -i .bak 's/a/b/' f")?.inPlaceSuffix, '.bak');
  });

  it('records a BSD empty backup suffix', () => {
    assert.equal(parseSedEditCommand("sed -i '' 's/a/b/' f")?.inPlaceSuffix, '');
  });
});

describe('canSimulateSedEditFaithfully', () => {
  const info = (cmd: string) => parseSedEditCommand(cmd)!;

  it('accepts a plain substitution', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/foo/bar/g' f")), true);
  });

  it('accepts BRE metachar escapes the applier converts', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a\\+/b/' f")), true);
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/\\(a\\)/b/' f")), true);
  });

  it('accepts escaped literal metacharacters and \\& in the replacement', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a\\.b/c/' f")), true);
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a/\\&/' f")), true);
  });

  it('rejects a replacement backreference', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/\\(a\\)/[\\1]/' f")), false);
  });

  it('rejects GNU case-conversion escapes in the replacement', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a/\\U&/' f")), false);
  });

  it('rejects a literal $ in the replacement (JS-special)', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a/$x/' f")), false);
  });

  it('rejects a BRE interval in the pattern', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a\\{2\\}/b/' f")), false);
  });

  it('rejects a POSIX bracket expression in the pattern', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/[[:digit:]]/x/g' f")), false);
  });

  it('rejects a literal backslash in the replacement', () => {
    assert.equal(canSimulateSedEditFaithfully(info("sed -i 's/a/\\\\/' f")), false);
  });
});

describe('computeSedEdit', () => {
  it('returns the JS-computed before/after for a faithful edit', () => {
    const sim = computeSedEdit("sed -i 's/foo/bar/g' f.txt", '/work', () => 'foo foo');
    assert.ok(sim);
    assert.equal(sim.filePath, '/work/f.txt');
    assert.equal(sim.oldContent, 'foo foo');
    assert.equal(sim.newContent, 'bar bar');
    // Single source of truth: identical to a direct applier call.
    assert.equal(sim.newContent, applySedSubstitution('foo foo', parseSedEditCommand("sed -i 's/foo/bar/g' f.txt")!));
  });

  it('resolves an absolute path unchanged', () => {
    const sim = computeSedEdit("sed -i 's/a/b/' /etc/x", '/work', () => 'a');
    assert.equal(sim?.filePath, '/etc/x');
  });

  it('returns null when the command is not a simple sed edit', () => {
    assert.equal(computeSedEdit('echo hi', '/work', () => 'x'), null);
  });

  it('returns null for an unfaithful substitution (backreference)', () => {
    assert.equal(computeSedEdit("sed -i 's/\\(a\\)/[\\1]/' f", '/work', () => 'a'), null);
  });

  it('returns null when the file is unreadable', () => {
    assert.equal(computeSedEdit("sed -i 's/a/b/' f", '/work', () => null), null);
  });

  it('returns null when nothing changes', () => {
    assert.equal(computeSedEdit("sed -i 's/zzz/b/' f", '/work', () => 'abc'), null);
  });
});

describe('applySimulatedSedEdit', () => {
  it('writes the JS-computed content and returns the simulation', () => {
    const files = new Map<string, string>([['/work/f', 'foo foo']]);
    const sim = applySimulatedSedEdit("sed -i 's/foo/bar/g' f", '/work', {
      readFile: p => files.get(p) ?? null,
      writeFile: (p, c) => files.set(p, c),
    });
    assert.ok(sim);
    assert.equal(files.get('/work/f'), 'bar bar');
  });

  it('writes a backup with the original content for -i.bak', () => {
    const files = new Map<string, string>([['/work/f', 'foo']]);
    const sim = applySimulatedSedEdit("sed -i.bak 's/foo/bar/' f", '/work', {
      readFile: p => files.get(p) ?? null,
      writeFile: (p, c) => files.set(p, c),
    });
    assert.equal(sim?.inPlaceSuffix, '.bak');
    assert.equal(files.get('/work/f'), 'bar');
    assert.equal(files.get('/work/f.bak'), 'foo');
  });

  it('does NOT write and returns null for an unfaithful substitution', () => {
    const files = new Map<string, string>([['/work/f', 'ab']]);
    let wrote = false;
    const sim = applySimulatedSedEdit("sed -i 's/\\(a\\)b/[\\1]/' f", '/work', {
      readFile: p => files.get(p) ?? null,
      writeFile: () => { wrote = true; },
    });
    assert.equal(sim, null);
    assert.equal(wrote, false);
    assert.equal(files.get('/work/f'), 'ab');
  });

  it('falls back (null) when a backup suffix contains a GNU filename wildcard', () => {
    const files = new Map<string, string>([['/work/f', 'foo']]);
    const sim = applySimulatedSedEdit("sed -i'bak/*' 's/foo/bar/' f", '/work', {
      readFile: p => files.get(p) ?? null,
      writeFile: (p, c) => files.set(p, c),
    });
    assert.equal(sim, null);
    assert.equal(files.get('/work/f'), 'foo');
  });

  it('returns null (shell fallback) when the write throws', () => {
    const sim = applySimulatedSedEdit("sed -i 's/foo/bar/' f", '/work', {
      readFile: () => 'foo',
      writeFile: () => { throw new Error('EACCES'); },
    });
    assert.equal(sim, null);
  });
});

describe('bashTool.execute – simulated sed edit (E2E)', () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'u182-'));
  after(() => fs.rmSync(tmp, { recursive: true, force: true }));

  const ctx = (): ToolContext => ({ cwd: tmp, abortSignal: new AbortController().signal });

  it('writes the JS-computed content without running sed (metadata flag)', async () => {
    const fp = path.join(tmp, 'conf.ini');
    fs.writeFileSync(fp, 'timeout = 30\n');
    const result = await bashTool.execute({ command: `sed -i 's/30/60/' ${fp}` }, ctx());
    assert.equal(result.isError, undefined);
    assert.equal(result.output, '');
    assert.equal(result.metadata?.sedSimulatedEdit, true);
    assert.equal(result.metadata?.filePath, fp);
    assert.equal(fs.readFileSync(fp, 'utf8'), 'timeout = 60\n');
  });

  it('creates a backup file for -i.bak via the simulated path', async () => {
    const fp = path.join(tmp, 'app.txt');
    fs.writeFileSync(fp, 'alpha\n');
    const result = await bashTool.execute({ command: `sed -i.bak 's/alpha/beta/' ${fp}` }, ctx());
    assert.equal(result.metadata?.sedSimulatedEdit, true);
    assert.equal(fs.readFileSync(fp, 'utf8'), 'beta\n');
    assert.equal(fs.readFileSync(fp + '.bak', 'utf8'), 'alpha\n');
  });

  it('falls back to the real sed binary for an unfaithful backreference edit', async () => {
    const fp = path.join(tmp, 'ref.txt');
    fs.writeFileSync(fp, 'ab\n');
    const result = await bashTool.execute({ command: `sed -i 's/\\(a\\)b/[\\1]/' ${fp}` }, ctx());
    // Not the simulated path — real sed handled it.
    assert.notEqual(result.metadata?.sedSimulatedEdit, true);
    // Real sed resolves the backreference; it is NOT the literal JS output "[\\1]".
    const written = fs.readFileSync(fp, 'utf8');
    assert.equal(written, '[a]\n');
  });
});
