import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  acquireSessionLock,
  checkSessionLock,
  formatSessionLockConflict,
  SESSION_LOCK_FILE,
} from '../src/session';

// ---------------------------------------------------------------------------
// U021 — user-facing lock-conflict message naming the holder PID
// (parity: references/claude/claude-code-main/src/utils/nativeInstaller/pidLock.ts
//  L248-254 — "Cannot acquire lock for <name> - held by PID <pid>")
// ---------------------------------------------------------------------------

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ami-lockmsg-'));
}

/** A foreign lock whose PID reads as a live OTHER process. */
function plantForeignLock(dir: string, pid: number): void {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, SESSION_LOCK_FILE),
    JSON.stringify({ pid, startedAt: Date.now(), execPath: '/x' }),
  );
}

describe('formatSessionLockConflict', () => {
  it('names the session (dir basename) and the holder PID actionably', () => {
    const msg = formatSessionLockConflict('/home/u/.ami/sessions/my-sess', 4242);
    assert.match(msg, /my-sess/);
    assert.match(msg, /PID 4242/);
    assert.match(msg, /in use/i);
  });

  it('tolerates a trailing slash and falls back to the raw dir when basename is empty', () => {
    assert.match(formatSessionLockConflict('/a/b/sess/', 7), /sess/);
    assert.match(formatSessionLockConflict('/', 7), /PID 7/);
  });
});

describe('acquireSessionLock surfaces the holder PID as a message', () => {
  it('blocked acquire carries an actionable conflict message naming the PID', () => {
    const dir = tmpDir();
    plantForeignLock(dir, 999_999);
    const res = acquireSessionLock(dir, { isPidAlive: () => true, getProcessCommand: () => 'ami' });
    assert.equal(res.acquired, false);
    assert.equal(res.pid, 999_999);
    // RED before the fix: res.message was undefined (holder PID never surfaced).
    assert.equal(typeof res.message, 'string');
    assert.match(res.message!, /PID 999999/);
    assert.equal(res.message, formatSessionLockConflict(dir, 999_999));
  });

  it('a successful acquire carries no conflict message', () => {
    const dir = tmpDir();
    const res = acquireSessionLock(dir);
    assert.equal(res.acquired, true);
    assert.equal(res.message, undefined);
    res.release();
  });
});

describe('checkSessionLock blocked result carries the conflict message', () => {
  it('blocked probe names the holder PID', () => {
    const dir = tmpDir();
    plantForeignLock(dir, 888_888);
    const res = checkSessionLock(dir, { isPidAlive: () => true, getProcessCommand: () => 'ami' });
    assert.equal(res.kind, 'blocked');
    if (res.kind === 'blocked') {
      // RED before the fix: the blocked variant had no `message` field.
      assert.match(res.message, /PID 888888/);
      assert.equal(res.message, formatSessionLockConflict(dir, 888_888));
    }
  });
});
