import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  isAgentConfigPath,
  hasSuspiciousPathPattern,
  getPathsForPermissionCheck,
  PermissionManager,
} from '../src/permissions';

test('isAgentConfigPath flags the agent own-config / IDE / git dirs and settings files', () => {
  assert.equal(isAgentConfigPath('/repo/.ami/settings.json'), true);
  assert.equal(isAgentConfigPath('/repo/.claude/commands/x.toml'), true);
  assert.equal(isAgentConfigPath('/repo/.ami-plugin/plugin.json'), true);
  assert.equal(isAgentConfigPath('/repo/.vscode/tasks.json'), true);
  assert.equal(isAgentConfigPath('/repo/.idea/workspace.xml'), true);
  assert.equal(isAgentConfigPath('/repo/.git/config'), true);
  assert.equal(isAgentConfigPath('/repo/anywhere/permissions.json'), true);
  assert.equal(isAgentConfigPath('/repo/src/app.ts'), false);
});

test('hasSuspiciousPathPattern catches canonicalization-bypass vectors', () => {
  assert.equal(hasSuspiciousPathPattern('settings.json::$DATA'), true);
  assert.equal(hasSuspiciousPathPattern('.bashrc:$DATA'), true);
  assert.equal(hasSuspiciousPathPattern('GIT~1/config'), true);
  assert.equal(hasSuspiciousPathPattern('\\\\?\\C:\\Users\\x'), true);
  assert.equal(hasSuspiciousPathPattern('//./C:/x'), true);
  assert.equal(hasSuspiciousPathPattern('.git.'), true);
  assert.equal(hasSuspiciousPathPattern('settings.json '), true);
  assert.equal(hasSuspiciousPathPattern('.git.CON'), true);
  assert.equal(hasSuspiciousPathPattern('path/.../file'), true);
  assert.equal(hasSuspiciousPathPattern('\\\\server\\share'), true);
  assert.equal(hasSuspiciousPathPattern('//server/share'), true);
  // legitimate paths are not flagged
  assert.equal(hasSuspiciousPathPattern('/repo/src/app.ts'), false);
  assert.equal(hasSuspiciousPathPattern('/repo/a.b.c/file.ts'), false);
  assert.equal(hasSuspiciousPathPattern(''), false);
});

test('getPathsForPermissionCheck returns original + intermediate + final symlink targets', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-link-'));
  const real = path.join(dir, 'real.txt');
  const mid = path.join(dir, 'mid.lnk');
  const top = path.join(dir, 'top.lnk');
  fs.writeFileSync(real, 'secret');
  fs.symlinkSync(real, mid);
  fs.symlinkSync(mid, top);
  const paths = getPathsForPermissionCheck(top);
  assert.ok(paths.includes(path.normalize(top)));
  assert.ok(paths.includes(path.normalize(mid)));
  assert.ok(paths.includes(path.normalize(real)));
  // a plain (non-symlink) file yields just itself
  assert.deepEqual(getPathsForPermissionCheck(real), [path.normalize(real)]);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('getPathsForPermissionCheck survives circular symlink loops', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-loop-'));
  const a = path.join(dir, 'a');
  const b = path.join(dir, 'b');
  fs.symlinkSync(b, a);
  fs.symlinkSync(a, b);
  const paths = getPathsForPermissionCheck(a);
  assert.ok(paths.length >= 2 && paths.length < 40);
  fs.rmSync(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Symlink-chain forbidden-path check (gaps permissions2 #28/#29): a credential/
// system path at ANY hop of a symlink chain must be denied, even when the FINAL
// realpath is an innocuous file. RED without the isForbiddenAtAnyHop wiring into
// isPathAllowed/classifyReadAccess: the chain's final target is innocuous and
// inside the workspace, so the old single-realpath check returns allow.
// ---------------------------------------------------------------------------
test('forbidden credential path at an intermediate symlink hop is denied (chain laundering)', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-hop-'));
  const plain = path.join(dir, 'plain.txt');
  const cred = path.join(dir, 'id_rsa');   // intermediate hop: forbidden basename
  const top = path.join(dir, 'top.txt');   // innocuous entry name
  fs.writeFileSync(plain, 'not-a-secret');
  fs.symlinkSync(plain, cred);             // id_rsa -> plain.txt
  fs.symlinkSync(cred, top);               // top.txt -> id_rsa -> plain.txt
  const pm = new PermissionManager();
  // The final realpath is plain.txt (innocuous, in-workspace), but the chain
  // passes through id_rsa — it must be a hard deny, not allow.
  assert.equal(pm.classifyReadAccess(top, dir), 'deny');
  assert.equal(pm.isPathAllowed(top, dir), false);
  // Negative control: an all-innocuous chain inside the workspace stays allowed.
  const okLink = path.join(dir, 'ok.txt');
  fs.symlinkSync(plain, okLink);           // ok.txt -> plain.txt (no forbidden hop)
  assert.equal(pm.classifyReadAccess(okLink, dir), 'allow');
  assert.equal(pm.isPathAllowed(okLink, dir), true);
  fs.rmSync(dir, { recursive: true, force: true });
});

// GAP (medium): NFC Unicode normalization of the realpathSync output in
// resolveForCheck (wired through the shared realpathNFC helper). An existing file
// inside the workspace must resolve and classify as 'allow' - proving the realpath
// canonicalization is driven end-to-end by the permission check. The NFD->NFC
// discrimination itself is unit-tested cross-platform against realpathNFC in
// utils-path-hardening.test.ts (reproducing an NFD-storing realpath requires
// macOS); here we lock in that the wiring still resolves + allows a real
// in-workspace file through the NFC-canonicalizing path.
test('classifyReadAccess resolves a real in-workspace file through NFC realpath canonicalization', () => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-nfc-')));
  const sub = path.join(dir, 'src');
  fs.mkdirSync(sub);
  fs.writeFileSync(path.join(sub, 'app.ts'), 'x');
  const pm = new PermissionManager();
  // Resolves the real path (realpathNFC -> fs.realpathSync + NFC) and finds it
  // inside the workspace boundary.
  assert.equal(pm.classifyReadAccess('src/app.ts', dir), 'allow');
  assert.equal(pm.isPathAllowed('src/app.ts', dir), true);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('acceptEdits prompts for agent-config and suspicious paths, allows normal source edits', async () => {
  const pm = new PermissionManager('acceptEdits');
  assert.equal(await pm.check('file_edit', { file_path: '/repo/.ami/settings.json' }), 'ask');
  assert.equal(await pm.check('file_edit', { file_path: '/repo/.vscode/tasks.json' }), 'ask');
  assert.equal(await pm.check('file_write', { file_path: '/repo/x.txt::$DATA' }), 'ask');
  assert.equal(await pm.check('file_edit', { file_path: '/repo/src/app.ts' }), 'allow');
});
