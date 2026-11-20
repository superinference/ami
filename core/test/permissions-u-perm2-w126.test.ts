import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { PermissionManager } from '../src/permissions';
import { SCRATCHPAD_DIR_ENV } from '../src/scratchpad';

// ===========================================================================
// UNIT U-perm2 (w126)
//  gap 1 — scratchpad read+write carve-out in the permission layer. The system
//    prompt already tells the model SUPERINFERENCE_SCRATCHPAD_DIR is "writable
//    without a permission prompt"; this wires that promise into PermissionManager
//    (check / classifyReadAccess / isPathAllowed). Parity: the reference's getScratchpadDir
//    allowlist (utils/permissions/filesystem.ts L384-424).
//  gap 2 — edit-access-implies-read: an out-of-workspace path with a file_edit ALLOW
//    rule is readable too, but read-specific deny/ask rules win first. Parity:
//    filesystem.ts L1081-1134.
// ===========================================================================

async function withScratchpad<T>(dir: string | undefined, fn: () => T | Promise<T>): Promise<T> {
  const prev = process.env[SCRATCHPAD_DIR_ENV];
  if (dir === undefined) delete process.env[SCRATCHPAD_DIR_ENV];
  else process.env[SCRATCHPAD_DIR_ENV] = dir;
  try {
    return await fn();
  } finally {
    if (prev === undefined) delete process.env[SCRATCHPAD_DIR_ENV];
    else process.env[SCRATCHPAD_DIR_ENV] = prev;
  }
}

// A scratchpad dir OUTSIDE the launch workspace, so a plain workspace-containment
// allow cannot explain the verdicts below — only the carve-out can.
function makeScratchAndCwd(): { scratch: string; cwd: string; cleanup: () => void } {
  const scratch = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-sp-')));
  const cwd = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-cwd-')));
  return { scratch, cwd, cleanup: () => { fs.rmSync(scratch, { recursive: true, force: true }); fs.rmSync(cwd, { recursive: true, force: true }); } };
}

// --- gap 1: read carve-out (classifyReadAccess / isPathAllowed) ---------------

test('scratchpad read outside the workspace is auto-allowed (no prompt)', async () => {
  const { scratch, cwd, cleanup } = makeScratchAndCwd();
  try {
    const pm = new PermissionManager();
    const f = path.join(scratch, 'notes.md');
    // Control: feature OFF → the out-of-workspace read is gated to a prompt.
    await withScratchpad(undefined, () => {
      assert.equal(pm.classifyReadAccess(f, cwd), 'prompt');
      assert.equal(pm.isPathAllowed(f, cwd), false);
    });
    // Feature ON → auto-allowed.
    await withScratchpad(scratch, () => {
      assert.equal(pm.classifyReadAccess(f, cwd), 'allow');
      assert.equal(pm.isPathAllowed(f, cwd), true);
    });
  } finally {
    cleanup();
  }
});

test('scratchpad WRITE is auto-allowed in ask mode (file_write / file_edit)', async () => {
  const { scratch, cwd, cleanup } = makeScratchAndCwd();
  const prevCwd = process.cwd();
  try {
    process.chdir(cwd);
    const pm = new PermissionManager('ask');
    const f = path.join(scratch, 'out.txt');
    await withScratchpad(undefined, async () => {
      assert.equal(await pm.check('file_write', { file_path: f }), 'ask');
    });
    await withScratchpad(scratch, async () => {
      assert.equal(await pm.check('file_write', { file_path: f }), 'allow');
      assert.equal(await pm.check('file_edit', { file_path: f }), 'allow');
      assert.equal(await pm.check('notebook_edit', { file_path: path.join(scratch, 'n.ipynb') }), 'allow');
    });
  } finally {
    process.chdir(prevCwd);
    cleanup();
  }
});

test('scratchpad WRITE is auto-allowed even in deny-all mode', async () => {
  const { scratch, cwd, cleanup } = makeScratchAndCwd();
  const prevCwd = process.cwd();
  try {
    process.chdir(cwd);
    const pm = new PermissionManager('deny-all');
    const f = path.join(scratch, 'x.txt');
    await withScratchpad(scratch, async () => {
      assert.equal(await pm.check('file_write', { file_path: f }), 'allow');
    });
    // Control: a non-scratchpad write in deny-all still denies.
    await withScratchpad(scratch, async () => {
      assert.equal(await pm.check('file_write', { file_path: path.join(cwd, 'y.txt') }), 'deny');
    });
  } finally {
    process.chdir(prevCwd);
    cleanup();
  }
});

test('an explicit deny rule still wins over the scratchpad carve-out', async () => {
  const { scratch, cwd, cleanup } = makeScratchAndCwd();
  const prevCwd = process.cwd();
  try {
    process.chdir(cwd);
    const pm = new PermissionManager('ask');
    pm.addRule({ tool: 'file_write', action: 'deny' });
    const f = path.join(scratch, 'x.txt');
    await withScratchpad(scratch, async () => {
      assert.equal(await pm.check('file_write', { file_path: f }), 'deny');
    });
  } finally {
    process.chdir(prevCwd);
    cleanup();
  }
});

test('a credential path INSIDE a mis-pointed scratchpad is NOT laundered (forbidden wins)', async () => {
  const { scratch, cwd, cleanup } = makeScratchAndCwd();
  const prevCwd = process.cwd();
  try {
    process.chdir(cwd);
    const pm = new PermissionManager('ask');
    const cred = path.join(scratch, 'id_rsa'); // forbidden basename inside scratchpad
    await withScratchpad(scratch, async () => {
      // read: forbidden guard runs before the carve-out → deny.
      assert.equal(pm.classifyReadAccess(cred, cwd), 'deny');
      assert.equal(pm.isPathAllowed(cred, cwd), false);
      // write: carve-out declines the forbidden path, so it falls back to a prompt.
      assert.equal(await pm.check('file_write', { file_path: cred }), 'ask');
    });
  } finally {
    process.chdir(prevCwd);
    cleanup();
  }
});

// --- gap 2: edit-access-implies-read -----------------------------------------

test('edit-access implies read: a file_edit allow rule makes an out-of-workspace path readable', () => {
  const { cwd, cleanup } = makeScratchAndCwd();
  try {
    const target = path.join(os.tmpdir(), 'ami-editgrant-out', 'result.txt');
    const pm = new PermissionManager();
    // Control: no edit rule → out-of-workspace read is a prompt.
    assert.equal(pm.classifyReadAccess(target, cwd), 'prompt');
    // Grant edit access to the dir → reads are now implied-allowed.
    pm.addRule({ tool: 'file_edit', pattern: `${path.join(os.tmpdir(), 'ami-editgrant-out')}/**`, action: 'allow' });
    assert.equal(pm.classifyReadAccess(target, cwd), 'allow');
  } finally {
    cleanup();
  }
});

test('edit-implied read is suppressed by a read-specific deny/ask rule (read rules win first)', () => {
  const base = path.join(os.tmpdir(), 'ami-editgrant2');
  const target = path.join(base, 'result.txt');
  const { cwd, cleanup } = makeScratchAndCwd();
  try {
    const pm = new PermissionManager();
    pm.addRule({ tool: 'file_edit', pattern: `${base}/**`, action: 'allow' });
    assert.equal(pm.classifyReadAccess(target, cwd), 'allow'); // baseline: implied
    // A read-specific ASK rule must prevent the implied-allow (can't silently read).
    pm.addRule({ tool: 'file_read', pattern: `${base}/**`, action: 'ask' });
    assert.notEqual(pm.classifyReadAccess(target, cwd), 'allow');
  } finally {
    cleanup();
  }
});

test('edit-implied read requires an ALLOW rule: a file_edit deny does not imply read', () => {
  const base = path.join(os.tmpdir(), 'ami-editdeny');
  const target = path.join(base, 'r.txt');
  const { cwd, cleanup } = makeScratchAndCwd();
  try {
    const pm = new PermissionManager();
    pm.addRule({ tool: 'file_edit', pattern: `${base}/**`, action: 'deny' });
    assert.equal(pm.classifyReadAccess(target, cwd), 'prompt');
  } finally {
    cleanup();
  }
});
