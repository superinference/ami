import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { parseSedEditCommand, applySedSubstitution } from '../src/tools/bash-sed-parser';

describe('parseSedEditCommand', () => {
  it('parses a basic s///  substitution', () => {
    assert.deepEqual(parseSedEditCommand("sed -i 's/foo/bar/' file.txt"), {
      filePath: 'file.txt',
      pattern: 'foo',
      replacement: 'bar',
      flags: '',
      extendedRegex: false,
    });
  });

  it('captures substitution flags', () => {
    const info = parseSedEditCommand("sed -i 's/foo/bar/gI' file.txt");
    assert.equal(info?.flags, 'gI');
  });

  it('accepts a numeric occurrence flag', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/2' f")?.flags, '2');
  });

  it('handles --in-place long flag', () => {
    assert.equal(parseSedEditCommand("sed --in-place 's/a/b/' f")?.filePath, 'f');
  });

  it('consumes a BSD empty backup suffix after -i', () => {
    const info = parseSedEditCommand("sed -i '' 's/foo/bar/' file.txt");
    assert.equal(info?.filePath, 'file.txt');
  });

  it('consumes a dotted backup suffix after -i', () => {
    const info = parseSedEditCommand("sed -i .bak 's/foo/bar/' file.txt");
    assert.equal(info?.filePath, 'file.txt');
  });

  it('handles the inline -i.bak suffix form', () => {
    const info = parseSedEditCommand("sed -i.bak 's/a/b/' x");
    assert.equal(info?.filePath, 'x');
  });

  it('marks -E as extended regex', () => {
    assert.equal(parseSedEditCommand("sed -E -i 's/a+/b/' x")?.extendedRegex, true);
  });

  it('marks -r as extended regex', () => {
    assert.equal(parseSedEditCommand("sed -r -i 's/a+/b/' x")?.extendedRegex, true);
  });

  it('marks --regexp-extended as extended regex', () => {
    assert.equal(parseSedEditCommand("sed --regexp-extended -i 's/a/b/' x")?.extendedRegex, true);
  });

  it('supports -e expression form', () => {
    const info = parseSedEditCommand("sed -i -e 's/a/b/' x");
    assert.equal(info?.pattern, 'a');
    assert.equal(info?.filePath, 'x');
  });

  it('supports --expression= form', () => {
    const info = parseSedEditCommand("sed -i --expression=s/a/b/ x");
    assert.equal(info?.pattern, 'a');
  });

  it('keeps an escaped delimiter inside the pattern', () => {
    const info = parseSedEditCommand("sed -i 's/a\\/b/c/' x");
    assert.equal(info?.pattern, 'a\\/b');
  });

  it('rejects when there is no -i flag', () => {
    assert.equal(parseSedEditCommand("sed -n 's/a/b/p' x"), null);
  });

  it('rejects a non-substitution script (delete)', () => {
    assert.equal(parseSedEditCommand("sed -i '/foo/d' x"), null);
  });

  it('rejects more than one file argument', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/' a b"), null);
  });

  it('rejects a pipeline', () => {
    assert.equal(parseSedEditCommand("cat x | sed -i 's/a/b/' x"), null);
  });

  it('rejects a command chain', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/' x ; echo done"), null);
  });

  it('rejects an output redirection', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/' x > y"), null);
  });

  it('rejects a command substitution in an argument', () => {
    assert.equal(parseSedEditCommand("sed -i \"s/$(whoami)/x/\" f"), null);
  });

  it('rejects a backtick substitution in an argument', () => {
    assert.equal(parseSedEditCommand('sed -i "s/`id`/x/" f'), null);
  });

  it('rejects a non-sed command', () => {
    assert.equal(parseSedEditCommand("awk '{print}' x"), null);
  });

  it('rejects an unknown flag', () => {
    assert.equal(parseSedEditCommand("sed -z -i 's/a/b/' x"), null);
  });

  it('rejects when the expression is missing its parts', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a' x"), null);
  });

  it('rejects an unsupported sed command (y///)', () => {
    assert.equal(parseSedEditCommand("sed -i 'y/abc/xyz/' x"), null);
  });

  it('rejects invalid flags', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/z' x"), null);
  });

  it('rejects a trailing extra delimiter in flags', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/g/' x"), null);
  });

  it('rejects -e with no following expression', () => {
    assert.equal(parseSedEditCommand('sed -i -e'), null);
  });

  it('rejects a second -e expression', () => {
    assert.equal(parseSedEditCommand("sed -i -e 's/a/b/' -e 's/c/d/' x"), null);
  });

  it('rejects when no file path is given', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a/b/'"), null);
  });

  it('rejects -i with no expression at all', () => {
    assert.equal(parseSedEditCommand('sed -i'), null);
  });

  it('rejects a second --expression= script', () => {
    assert.equal(parseSedEditCommand('sed -i --expression=s/a/b/ --expression=s/c/d/ x'), null);
  });

  it('rejects unparseable input', () => {
    assert.equal(parseSedEditCommand(''), null);
  });

  it('rejects an expression ending in a dangling escape', () => {
    assert.equal(parseSedEditCommand("sed -i 's/a\\' x"), null);
  });

  it('keeps an escaped char in the flags section as malformed-safe', () => {
    // backslash inside the (empty) flag region is preserved then rejected by the
    // flag validator, exercising the escape branch in the flags state.
    assert.equal(parseSedEditCommand("sed -i 's/a/b/\\n' x"), null);
  });
});

describe('applySedSubstitution', () => {
  const info = (cmd: string) => {
    const r = parseSedEditCommand(cmd);
    assert.ok(r, `expected parse for ${cmd}`);
    return r;
  };

  it('replaces the first match without the g flag', () => {
    assert.equal(applySedSubstitution('foo foo', info("sed -i 's/foo/bar/' f")), 'bar foo');
  });

  it('replaces all matches with the g flag', () => {
    assert.equal(applySedSubstitution('foo foo\nfoo', info("sed -i 's/foo/bar/g' f")), 'bar bar\nbar');
  });

  it('is case-insensitive with the i flag', () => {
    assert.equal(applySedSubstitution('FOO', info("sed -i 's/foo/bar/i' f")), 'bar');
  });

  it('honors the multiline m flag for ^ anchors', () => {
    assert.equal(
      applySedSubstitution('a\nb', info("sed -i 's/^/> /mg' f")),
      '> a\n> b',
    );
  });

  it('honors the uppercase M multiline flag', () => {
    assert.equal(
      applySedSubstitution('a\nb', info("sed -i 's/^/> /Mg' f")),
      '> a\n> b',
    );
  });

  it('honors the uppercase I case-insensitive flag', () => {
    assert.equal(applySedSubstitution('ABC', info("sed -i 's/abc/x/I' f")), 'x');
  });

  it('expands & to the whole match', () => {
    assert.equal(applySedSubstitution('foo', info("sed -i 's/foo/[&]/' f")), '[foo]');
  });

  it('treats \\& as a literal ampersand', () => {
    assert.equal(applySedSubstitution('foo', info("sed -i 's/foo/a\\&b/' f")), 'a&b');
  });

  it('converts BRE \\+ to a quantifier and keeps bare + literal', () => {
    assert.equal(applySedSubstitution('aaa a+', info("sed -i 's/a\\+/X/' f")), 'X a+');
  });

  it('treats bare + as literal in BRE mode', () => {
    assert.equal(applySedSubstitution('a+b', info("sed -i 's/a+b/X/' f")), 'X');
  });

  it('uses + as a quantifier in extended-regex mode', () => {
    assert.equal(applySedSubstitution('aaa', info("sed -E -i 's/a+/X/' f")), 'X');
  });

  it('converts BRE groups \\( \\) to JS capture groups in the pattern', () => {
    // The group matches; sed-style \1 backrefs in the replacement are NOT
    // translated to $1 (matching the reference), so they stay literal.
    assert.equal(applySedSubstitution('ab', info("sed -i 's/\\(a\\)b/[\\1]/' f")), '[\\1]');
  });

  it('preserves a literal backslash pair in BRE', () => {
    assert.equal(applySedSubstitution('a\\b', info("sed -i 's/a\\\\b/X/' f")), 'X');
  });

  it('unescapes an escaped delimiter in the pattern', () => {
    assert.equal(applySedSubstitution('a/b', info("sed -i 's/a\\/b/X/' f")), 'X');
  });

  it('returns content unchanged on an invalid regex', () => {
    // An unterminated BRE group yields an invalid JS regex → original returned.
    const bad = { filePath: 'f', pattern: '(', replacement: 'x', flags: '', extendedRegex: true };
    assert.equal(applySedSubstitution('abc', bad), 'abc');
  });

  it('returns content unchanged when nothing matches', () => {
    assert.equal(applySedSubstitution('hello', info("sed -i 's/zzz/x/' f")), 'hello');
  });
});
