import { test } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import {
  SCRATCHPAD_DIR_ENV,
  resolveScratchpadDir,
  isScratchpadEnabled,
  isScratchpadPath,
  describeScratchpad,
} from '../src/scratchpad';

// ===========================================================================
// UNIT U-perm2 (w126) gap 1 — canonical scratchpad-dir resolution + containment.
// Parity: the reference's isScratchpadEnabled / getScratchpadDir / isScratchpadPath
// (utils/permissions/filesystem.ts L293-424). AMI superset: the dir is operator-set
// via SUPERINFERENCE_SCRATCHPAD_DIR (the same knob the system-prompt scratchpad
// section already uses), so prompt + permission carve-out share one source of truth.
// ===========================================================================

function envWith(dir: string | undefined): NodeJS.ProcessEnv {
  const e: NodeJS.ProcessEnv = {};
  if (dir !== undefined) e[SCRATCHPAD_DIR_ENV] = dir;
  return e;
}

test('resolveScratchpadDir: unset / blank → null (feature off by default)', () => {
  assert.equal(resolveScratchpadDir(envWith(undefined)), null);
  assert.equal(resolveScratchpadDir(envWith('   ')), null);
  assert.equal(isScratchpadEnabled(envWith(undefined)), false);
});

test('resolveScratchpadDir: absolutizes + normalizes + strips trailing sep', () => {
  assert.equal(resolveScratchpadDir(envWith('/tmp/scratch/')), '/tmp/scratch');
  assert.equal(resolveScratchpadDir(envWith('/tmp/scratch')), '/tmp/scratch');
  // a relative value is anchored against cwd
  assert.equal(resolveScratchpadDir(envWith('rel/dir')), path.resolve('rel/dir'));
  assert.equal(isScratchpadEnabled(envWith('/tmp/scratch')), true);
});

test('isScratchpadPath: dir itself + descendants match; siblings do not', () => {
  const env = envWith('/tmp/scratch');
  assert.equal(isScratchpadPath('/tmp/scratch', env), true);
  assert.equal(isScratchpadPath('/tmp/scratch/notes.md', env), true);
  assert.equal(isScratchpadPath('/tmp/scratch/a/b/c.txt', env), true);
  assert.equal(isScratchpadPath('/tmp/scratchpad-other/x', env), false); // prefix-but-not-child
  assert.equal(isScratchpadPath('/tmp/other/x', env), false);
});

test('isScratchpadPath: a traversal payload escaping the dir does NOT match (normalize first)', () => {
  const env = envWith('/tmp/scratch');
  // textually starts with the prefix but collapses to /etc/passwd → must NOT match
  assert.equal(isScratchpadPath('/tmp/scratch/../../etc/passwd', env), false);
  assert.equal(isScratchpadPath('/tmp/scratch/sub/../ok.txt', env), true); // stays inside
});

test('isScratchpadPath: feature off → never matches', () => {
  assert.equal(isScratchpadPath('/tmp/scratch/x', envWith(undefined)), false);
});

test('describeScratchpad: reports env var, enabled flag + resolved dir', () => {
  assert.deepEqual(describeScratchpad(envWith(undefined)), {
    envVar: SCRATCHPAD_DIR_ENV,
    enabled: false,
    dir: null,
  });
  assert.deepEqual(describeScratchpad(envWith('/tmp/scratch/')), {
    envVar: SCRATCHPAD_DIR_ENV,
    enabled: true,
    dir: '/tmp/scratch',
  });
});

test('isScratchpadPath: works against a real temp dir on disk', () => {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'ami-scratch-')));
  try {
    const env = envWith(dir);
    const f = path.join(dir, 'work', 'file.txt');
    assert.equal(isScratchpadPath(f, env), true);
    assert.equal(isScratchpadPath(path.join(os.tmpdir(), 'elsewhere.txt'), env), false);
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
