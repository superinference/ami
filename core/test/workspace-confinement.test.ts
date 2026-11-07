/**
 * WORKSPACE CONFINEMENT — exhaustive regression guard.
 *
 * Locks the behavior reported as a bug: the agent must NOT read files outside the
 * directory it was launched in (the "launch cwd") unless the USER explicitly grants it.
 * Covers the full matrix on PermissionManager:
 *   - INSIDE the launch cwd            -> 'allow' / isPathAllowed=true
 *   - OUTSIDE the launch cwd           -> 'prompt' (needs user consent) / isPathAllowed=false
 *   - FORBIDDEN (credential/system)    -> 'deny' (never, even granted/inside) / isPathAllowed=false
 *   - GRANTED via registerAllowedRoot  -> 'allow' for that root (but forbidden still wins)
 * plus the classic escape corner cases: ".." traversal, prefix-siblings, symlinks,
 * tilde expansion, trailing slashes, and the file-tool vs classifyReadAccess consistency.
 *
 * Run: cd core/core && node --import tsx --test test/workspace-confinement.test.ts
 */
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { PermissionManager } from '../src/permissions';

// A real on-disk launch cwd so realpathSync (symlink resolution) exercises real inodes.
let CWD = '';
let OUTSIDE = '';   // a sibling dir OUTSIDE the cwd (same parent)
let PARENT = '';

before(() => {
  PARENT = fs.mkdtempSync(path.join(os.tmpdir(), 'wsconf-'));
  CWD = fs.realpathSync(fs.mkdtempSync(path.join(PARENT, 'workspace-')));
  OUTSIDE = fs.realpathSync(fs.mkdtempSync(path.join(PARENT, 'sibling-')));
});
after(() => { try { fs.rmSync(PARENT, { recursive: true, force: true }); } catch { /* ignore */ } });

const pm = () => new PermissionManager('ask');

// ---------------------------------------------------------------------------
describe('workspace confinement — INSIDE the launch cwd is allowed', () => {
  const cases = [
    ['relative file', () => 'src/index.ts'],
    ['nested relative', () => 'a/b/c/deep.ts'],
    ['dot-slash', () => './file.ts'],
    ['the cwd itself', () => CWD],
    ['absolute file under cwd', () => path.join(CWD, 'pkg', 'x.ts')],
    ['"." current dir', () => '.'],
    ['".." that stays inside cwd', () => 'sub/../also-inside.ts'],
    ['deep absolute under cwd', () => path.join(CWD, 'a', 'b', 'c', 'd', 'e.ts')],
  ] as const;
  for (const [name, mk] of cases) {
    it(`allows ${name}`, () => {
      const p = mk();
      assert.equal(pm().classifyReadAccess(p, CWD), 'allow', `${p} should be allow`);
      assert.equal(pm().isPathAllowed(p, CWD), true, `${p} isPathAllowed`);
    });
  }
});

// ---------------------------------------------------------------------------
describe('workspace confinement — OUTSIDE the launch cwd needs consent (prompt)', () => {
  const cases = [
    ['a sibling directory file', () => path.join(OUTSIDE, 'notes.txt')],
    ['the parent directory', () => PARENT],
    ['a parent-dir file', () => path.join(PARENT, 'x.txt')],
    ['".." escaping to parent', () => path.join(CWD, '..', 'escape.txt')],
    ['"../.." escaping two up', () => path.join(CWD, '..', '..', 'far.txt')],
    ['an absolute /tmp path', () => path.join(os.tmpdir(), 'elsewhere-xyz.txt')],
    ['a home path outside cwd', () => path.join(os.homedir(), 'some-other-project', 'a.ts')],
    ["another user's home", () => '/home/someoneelse/notes.txt'],
    ['a tilde path (expands to home, not cwd)', () => '~/projects/x.ts'],
    ['an unrelated absolute path', () => '/data/private/file.txt'],
  ] as const;
  for (const [name, mk] of cases) {
    it(`prompts for ${name}`, () => {
      const p = mk();
      assert.equal(pm().classifyReadAccess(p, CWD), 'prompt', `${p} should be prompt`);
      assert.equal(pm().isPathAllowed(p, CWD), false, `${p} isPathAllowed=false until granted`);
    });
  }
});

// ---------------------------------------------------------------------------
describe('workspace confinement — FORBIDDEN paths are denied (even inside cwd / granted)', () => {
  const forbidden = [
    ['/etc/passwd', '/etc/passwd'],
    ['/etc exact', '/etc'],
    ['/usr/bin/node', '/usr/bin/node'],
    ['/bin/bash', '/bin/bash'],
    ['/var/log/syslog', '/var/log/syslog'],
    ['/sys/x', '/sys/class/net'],
    ['/proc/x', '/proc/cpuinfo'],
    ['/dev/null', '/dev/null'],
  ] as const;
  for (const [name, p] of forbidden) {
    it(`denies system path ${name}`, () => {
      assert.equal(pm().classifyReadAccess(p, CWD), 'deny');
      assert.equal(pm().isPathAllowed(p, CWD), false);
    });
  }

  it('denies ~/.ssh/id_rsa (credential) even though it is in home', () => {
    assert.equal(pm().classifyReadAccess(path.join(os.homedir(), '.ssh', 'id_rsa'), CWD), 'deny');
  });

  // Sensitive files are forbidden even INSIDE the workspace.
  const sensitiveInCwd = ['.env', '.env.local', '.env.production', 'id_rsa', 'id_ed25519',
    'server.pem', 'private.key', 'cert.p12', 'store.pfx', 'credentials.json', 'secrets.yaml', '.netrc'];
  for (const name of sensitiveInCwd) {
    it(`denies sensitive file "${name}" even inside the cwd`, () => {
      const p = path.join(CWD, name);
      assert.equal(pm().classifyReadAccess(p, CWD), 'deny', `${name} must be forbidden in-workspace`);
      assert.equal(pm().isPathAllowed(p, CWD), false);
    });
  }

  it('a forbidden file under a GRANTED root is STILL denied (grant never overrides forbidden)', () => {
    const m = pm();
    m.registerAllowedRoot(OUTSIDE);
    assert.equal(m.classifyReadAccess(path.join(OUTSIDE, '.env'), CWD), 'deny');
    assert.equal(m.classifyReadAccess(path.join(OUTSIDE, 'sub', 'id_rsa'), CWD), 'deny');
  });
});

// ---------------------------------------------------------------------------
describe('workspace confinement — user GRANTS (registerAllowedRoot / --add-dir / constructor)', () => {
  it('a granted directory makes files under it allowed', () => {
    const m = pm();
    assert.equal(m.classifyReadAccess(path.join(OUTSIDE, 'a.txt'), CWD), 'prompt'); // before
    m.registerAllowedRoot(OUTSIDE);
    assert.equal(m.classifyReadAccess(path.join(OUTSIDE, 'a.txt'), CWD), 'allow');  // after
    assert.equal(m.classifyReadAccess(path.join(OUTSIDE, 'deep', 'b.txt'), CWD), 'allow');
    assert.equal(m.isPathAllowed(path.join(OUTSIDE, 'a.txt'), CWD), true);
  });

  it('the granted directory itself is allowed', () => {
    const m = pm();
    m.registerAllowedRoot(OUTSIDE);
    assert.equal(m.classifyReadAccess(OUTSIDE, CWD), 'allow');
  });

  it('a sibling of a granted dir (NOT under it) stays prompt — no prefix-sibling leak', () => {
    const m = pm();
    const granted = path.join(PARENT, 'granted');
    m.registerAllowedRoot(granted);
    // "granted-evil" shares the string prefix "granted" but is a different dir.
    assert.equal(m.classifyReadAccess(path.join(PARENT, 'granted-evil', 'x'), CWD), 'prompt');
    assert.equal(m.classifyReadAccess(path.join(granted, 'x'), CWD), 'allow');
  });

  it('seeding grants via the constructor (3rd arg) works', () => {
    const m = new PermissionManager('ask', [], [OUTSIDE]);
    assert.equal(m.classifyReadAccess(path.join(OUTSIDE, 'x'), CWD), 'allow');
  });

  it('registerAllowedRoot is idempotent and reflected in getAllowedReadRoots', () => {
    const m = pm();
    m.registerAllowedRoot(OUTSIDE);
    m.registerAllowedRoot(OUTSIDE);
    assert.equal(m.getAllowedReadRoots().filter(r => r === path.normalize(OUTSIDE)).length, 1);
  });

  it('a relative grant resolves to an absolute root', () => {
    const m = pm();
    m.registerAllowedRoot('some/relative/dir');
    assert.ok(m.getAllowedReadRoots().every(r => path.isAbsolute(r)));
  });

  it('empty / non-string grants are ignored (no throw)', () => {
    const m = pm();
    m.registerAllowedRoot('');
    // @ts-expect-error — defensive: non-string must not crash
    m.registerAllowedRoot(undefined);
    assert.equal(m.getAllowedReadRoots().length, 0);
  });

  it('granting one root does not widen access to a DIFFERENT outside dir', () => {
    const m = pm();
    m.registerAllowedRoot(path.join(PARENT, 'granted-a'));
    assert.equal(m.classifyReadAccess(path.join(PARENT, 'granted-b', 'x'), CWD), 'prompt');
  });
});

// ---------------------------------------------------------------------------
describe('workspace confinement — prefix-sibling and traversal corner cases', () => {
  it('a cwd-prefix sibling is NOT treated as inside (e.g. /ws vs /ws-evil)', () => {
    const sibling = CWD + '-evil';
    assert.equal(pm().classifyReadAccess(path.join(sibling, 'secret'), CWD), 'prompt');
  });

  it('a ".." chain that lands back inside cwd is allowed', () => {
    assert.equal(pm().classifyReadAccess(path.join(CWD, 'a', '..', 'b.ts'), CWD), 'allow');
  });

  it('a ".." chain that escapes then would re-enter a sibling is prompt', () => {
    const p = path.join(CWD, '..', path.basename(OUTSIDE), 'x.txt');
    assert.equal(pm().classifyReadAccess(p, CWD), 'prompt');
  });

  it('trailing-slash cwd still confines correctly', () => {
    assert.equal(pm().classifyReadAccess(path.join(CWD, 'x.ts'), CWD + path.sep), 'allow');
    assert.equal(pm().classifyReadAccess(path.join(OUTSIDE, 'x.ts'), CWD + path.sep), 'prompt');
  });
});

// ---------------------------------------------------------------------------
describe('workspace confinement — SYMLINK escapes are resolved (realpath), not string-matched', () => {
  it('a symlink INSIDE cwd pointing OUTSIDE resolves to outside -> prompt', () => {
    const link = path.join(CWD, 'link-to-sibling');
    try { fs.symlinkSync(OUTSIDE, link); } catch { return; } // skip if symlinks unsupported
    const target = path.join(link, 'secret.txt');
    fs.writeFileSync(path.join(OUTSIDE, 'secret.txt'), 'x');
    // Without realpath this would pass the cwd prefix check; with it, it resolves outside.
    assert.equal(pm().classifyReadAccess(target, CWD), 'prompt');
    assert.equal(pm().isPathAllowed(target, CWD), false);
  });

  it('a symlink inside cwd pointing to /etc resolves to a forbidden path -> deny', () => {
    const link = path.join(CWD, 'link-to-etc');
    try { fs.symlinkSync('/etc', link); } catch { return; }
    assert.equal(pm().classifyReadAccess(path.join(link, 'passwd'), CWD), 'deny');
  });

  it('a symlink inside cwd pointing to another IN-cwd file stays allow', () => {
    const real = path.join(CWD, 'real.txt');
    fs.writeFileSync(real, 'x');
    const link = path.join(CWD, 'link-inside');
    try { fs.symlinkSync(real, link); } catch { return; }
    assert.equal(pm().classifyReadAccess(link, CWD), 'allow');
  });
});

// ---------------------------------------------------------------------------
describe('workspace confinement — isPathAllowed and classifyReadAccess stay consistent', () => {
  it('isPathAllowed is true IFF classifyReadAccess is "allow"', () => {
    const m = pm();
    m.registerAllowedRoot(OUTSIDE);
    const probes = [
      path.join(CWD, 'x.ts'),
      path.join(OUTSIDE, 'y.ts'),           // granted -> allow
      path.join(PARENT, 'z.ts'),            // outside -> prompt
      '/etc/passwd',                        // forbidden -> deny
      path.join(CWD, '.env'),               // forbidden-in-cwd -> deny
    ];
    for (const p of probes) {
      const cls = m.classifyReadAccess(p, CWD);
      assert.equal(m.isPathAllowed(p, CWD), cls === 'allow', `${p}: isPathAllowed must match (allow==${cls})`);
    }
  });
});
