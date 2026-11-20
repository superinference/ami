import { describe, it, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  detectPathExpansionHazard,
  pathExpansionHazardReason,
  containsUnquotedExpansion,
  PermissionManager,
} from '../src/permissions';

// ---------------------------------------------------------------------------
// U003-w72 — TOCTOU path-expansion hardening + robust unquoted-expansion walker
// for core/core/src/permissions.ts, mirroring the reference's pathValidation and
// readOnlyValidation.
//
//  1. detectPathExpansionHazard / pathExpansionHazardReason: a path kept LITERAL
//     during permission validation but rewritten by the shell at execution —
//     `$VAR`/`${VAR}`/`$(cmd)` (Unix), `%VAR%` (Windows), a leading `=` (zsh
//     equals-expansion), OR an unsupported tilde variant (~user/~+/~-/~N) — is a
//     TOCTOU gap and must require manual approval. Mirrors the reference's
//     pathValidation.ts L393-L436 blanket-reject + its distinct reason strings.
//
//  2. PermissionManager.classifyReadAccess downgrades such a path from 'allow'
//     to 'prompt' even when it resolves INSIDE the workspace, and
//     PermissionManager.pathExpansionHazardReason surfaces the reason.
//
//  3. containsUnquotedExpansion now uses a quote-state walker (correct single-
//     quote escape semantics) and covers every `$`-expansion shape incl. the
//     special parameters `$@ $* $# $? $! $$ $- $0-$9` the old regex missed, while
//     correctly treating a backslash-escaped `\$` as literal. Mirrors the reference's
//     containsUnquotedExpansion char-walk (readOnlyValidation.ts L1600-L1669).
// ---------------------------------------------------------------------------

describe('detectPathExpansionHazard — shell-expansion syntax', () => {
  const shellExpansion = [
    '$HOME/.ssh/id_rsa',
    '${HOME}/data',
    '$(echo /etc)/passwd',
    'dir/$SECRET',
    '%TEMP%\\file',
    '%USERPROFILE%',
    '=rg',
    '=cmd/x',
  ];
  for (const p of shellExpansion) {
    it(`flags ${p}`, () => {
      assert.equal(detectPathExpansionHazard(p), 'shell-expansion');
    });
  }

  it('trims surrounding whitespace before the leading `=` check', () => {
    assert.equal(detectPathExpansionHazard('  =rg'), 'shell-expansion');
  });
});

describe('detectPathExpansionHazard — unsupported tilde variants', () => {
  const tildeVariants = ['~root/.ssh', '~user/file', '~+', '~-', '~1', '~+/x'];
  for (const p of tildeVariants) {
    it(`flags ${p}`, () => {
      assert.equal(detectPathExpansionHazard(p), 'tilde-variant');
    });
  }
});

describe('detectPathExpansionHazard — inert paths', () => {
  const inert = ['~', '~/data', '~\\data', '/abs/path/file.ts', 'src/index.ts', './a/b.ts', '../sibling/x'];
  for (const p of inert) {
    it(`returns null for ${p}`, () => {
      assert.equal(detectPathExpansionHazard(p), null);
    });
  }
});

describe('pathExpansionHazardReason', () => {
  it('gives a tilde-specific reason', () => {
    assert.match(pathExpansionHazardReason('tilde-variant'), /tilde expansion variants/i);
  });
  it('gives a shell-expansion-specific reason', () => {
    assert.match(pathExpansionHazardReason('shell-expansion'), /shell expansion syntax/i);
  });
  it('the two reasons are distinct', () => {
    assert.notEqual(
      pathExpansionHazardReason('tilde-variant'),
      pathExpansionHazardReason('shell-expansion'),
    );
  });
});

describe('PermissionManager.classifyReadAccess — expansion-hazard paths are gated', () => {
  let CWD = '';
  before(() => { CWD = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'w72-'))); });
  after(() => { try { fs.rmSync(CWD, { recursive: true, force: true }); } catch { /* ignore */ } });

  const pm = () => new PermissionManager('ask');

  it("downgrades an in-workspace '$VAR' path from allow to prompt (TOCTOU)", () => {
    // A relative path resolves INSIDE the workspace, so WITHOUT the hazard guard it
    // would be 'allow'. The `$SECRET` makes it a TOCTOU hazard → 'prompt'.
    assert.equal(pm().classifyReadAccess('src/$SECRET', CWD), 'prompt');
  });

  it("downgrades an in-workspace '=cmd' path from allow to prompt", () => {
    assert.equal(pm().classifyReadAccess('=notes.txt', CWD), 'prompt');
  });

  it('gates an unsupported tilde variant to prompt', () => {
    assert.equal(pm().classifyReadAccess('~root/data', CWD), 'prompt');
  });

  it('still allows a plain in-workspace relative path', () => {
    assert.equal(pm().classifyReadAccess('src/index.ts', CWD), 'allow');
  });

  it('a forbidden credential path still wins as deny over the hazard check', () => {
    // `$`-bearing but ALSO a credential basename — deny must take priority.
    assert.equal(pm().classifyReadAccess(path.join(os.homedir(), '.ssh', 'id_rsa'), CWD), 'deny');
  });

  it('pathExpansionHazardReason surfaces the matching reason / null', () => {
    const m = pm();
    assert.match(m.pathExpansionHazardReason('dir/$X') ?? '', /shell expansion/i);
    assert.match(m.pathExpansionHazardReason('~root/x') ?? '', /tilde expansion/i);
    assert.equal(m.pathExpansionHazardReason('src/plain.ts'), null);
  });
});

describe('containsUnquotedExpansion — quote-state walker + special parameters', () => {
  it('preserves the existing variable/glob/substitution detections', () => {
    assert.equal(containsUnquotedExpansion('echo $HOME'), true);
    assert.equal(containsUnquotedExpansion('echo ${HOME}'), true);
    assert.equal(containsUnquotedExpansion('echo $(whoami)'), true);
    assert.equal(containsUnquotedExpansion('ls *.ts'), true);
    assert.equal(containsUnquotedExpansion('cat file?.txt'), true);
    assert.equal(containsUnquotedExpansion('ls [abc]'), true);
  });

  it('tolerates quoted expansions (single and double)', () => {
    assert.equal(containsUnquotedExpansion('echo "$HOME"'), false);
    assert.equal(containsUnquotedExpansion("echo '$HOME'"), false);
    assert.equal(containsUnquotedExpansion('echo "$HOME is safe"'), false);
    assert.equal(containsUnquotedExpansion("echo 'literal *'"), false);
    assert.equal(containsUnquotedExpansion('echo hello world'), false);
  });

  it('now flags the special parameters the old regex missed', () => {
    for (const cmd of ['echo $?', 'echo $$', 'echo $@', 'echo $*', 'echo $#', 'echo $!', 'echo $1', 'echo $-']) {
      assert.equal(containsUnquotedExpansion(cmd), true, `${cmd} should be flagged`);
    }
  });

  it('treats a backslash-escaped `\\$` as a literal (not an expansion)', () => {
    assert.equal(containsUnquotedExpansion('echo \\$HOME'), false);
  });

  it('is not desynced by a single-quoted backslash before a later expansion', () => {
    // In bash `\` is literal inside single quotes, so the quote closes correctly
    // and the trailing unquoted $VAR is still detected.
    assert.equal(containsUnquotedExpansion("echo '\\' $VAR"), true);
  });
});
