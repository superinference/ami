/**
 * U021-w69 — session.ts parity additions:
 *  - SessionKind classification enum + env override knob (envSessionKind) +
 *    isBgSession predicate (parity: Claude concurrentSessions.ts SessionKind);
 *  - save() stamps `kind` (caller > env > 'interactive'); list() surfaces it;
 *  - loadLatest({ skipBackground }) skips bg/daemon sessions so a no-arg
 *    --continue auto-picks the most-recent FOREGROUND session;
 *  - safeAppendTranscript recreate-parent-dir-and-retry on a transient append
 *    failure (parity: Claude appendToFile mkdir-and-retry).
 */
import { describe, it, after, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  SESSION_KINDS,
  envSessionKind,
  isBgSession,
  safeAppendTranscript,
  type Session,
  type SessionKind,
} from '../src/session';
import { fsHooks } from '../src/utils/fs-atomic';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021w69-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function makeSession(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [{ role: 'user', content: 'hi' }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'b' },
    ...over,
  };
}

describe('SESSION_KINDS / envSessionKind', () => {
  it('lists exactly the four parity kinds', () => {
    assert.deepEqual([...SESSION_KINDS], ['interactive', 'bg', 'daemon', 'daemon-worker']);
  });

  it('reads the AMI knob when valid', () => {
    assert.equal(envSessionKind({ AMI_SESSION_KIND: 'bg' } as NodeJS.ProcessEnv), 'bg');
    assert.equal(envSessionKind({ AMI_SESSION_KIND: ' daemon-worker ' } as NodeJS.ProcessEnv), 'daemon-worker');
  });

  it('falls back to the CLAUDE_CODE knob for cross-tool parity', () => {
    assert.equal(envSessionKind({ CLAUDE_CODE_SESSION_KIND: 'daemon' } as NodeJS.ProcessEnv), 'daemon');
  });

  it('prefers the AMI knob over the CLAUDE_CODE one', () => {
    assert.equal(
      envSessionKind({ AMI_SESSION_KIND: 'bg', CLAUDE_CODE_SESSION_KIND: 'daemon' } as NodeJS.ProcessEnv),
      'bg',
    );
  });

  it('returns undefined for an unset or invalid value', () => {
    assert.equal(envSessionKind({} as NodeJS.ProcessEnv), undefined);
    assert.equal(envSessionKind({ AMI_SESSION_KIND: 'nonsense' } as NodeJS.ProcessEnv), undefined);
    assert.equal(envSessionKind({ AMI_SESSION_KIND: '' } as NodeJS.ProcessEnv), undefined);
  });
});

describe('isBgSession', () => {
  it('is true for bg/daemon/daemon-worker', () => {
    for (const k of ['bg', 'daemon', 'daemon-worker'] as SessionKind[]) {
      assert.equal(isBgSession({ kind: k }), true, k);
    }
  });
  it('is false for interactive or an absent kind', () => {
    assert.equal(isBgSession({ kind: 'interactive' }), false);
    assert.equal(isBgSession({}), false);
  });
});

describe('save() stamps kind + list() surfaces it', () => {
  it('defaults to interactive when no kind and no env override', () => {
    const prev = process.env.AMI_SESSION_KIND;
    delete process.env.AMI_SESSION_KIND;
    try {
      const sm = new SessionManager(process.cwd(), mkdir());
      sm.save(makeSession('session-k1'));
      assert.equal(sm.load('session-k1')?.kind, 'interactive');
      assert.equal(sm.list()[0].kind, 'interactive');
    } finally {
      if (prev !== undefined) process.env.AMI_SESSION_KIND = prev;
    }
  });

  it('honors a caller-supplied kind over the default', () => {
    const sm = new SessionManager(process.cwd(), mkdir());
    sm.save(makeSession('session-k2', { kind: 'bg' }));
    assert.equal(sm.load('session-k2')?.kind, 'bg');
  });

  it('applies the env override when the session carries no kind', () => {
    const prev = process.env.AMI_SESSION_KIND;
    process.env.AMI_SESSION_KIND = 'daemon';
    try {
      const sm = new SessionManager(process.cwd(), mkdir());
      sm.save(makeSession('session-k3'));
      assert.equal(sm.load('session-k3')?.kind, 'daemon');
    } finally {
      if (prev === undefined) delete process.env.AMI_SESSION_KIND;
      else process.env.AMI_SESSION_KIND = prev;
    }
  });
});

describe('loadLatest({ skipBackground })', () => {
  function seed(): SessionManager {
    const sm = new SessionManager(process.cwd(), mkdir());
    // Oldest -> newest, with the NEWEST being a bg session.
    sm.save(makeSession('session-old-fg', { kind: 'interactive' }));
    sm.save(makeSession('session-new-bg', { kind: 'bg' }));
    // Nudge mtimes so new-bg is unambiguously the most-recent file.
    const dir = sm.getSessionDir();
    const now = Date.now();
    fs.utimesSync(path.join(dir, 'session-old-fg.json'), new Date(now - 10_000), new Date(now - 10_000));
    fs.utimesSync(path.join(dir, 'session-new-bg.json'), new Date(now), new Date(now));
    return sm;
  }

  it('without the flag returns the most-recent session even if it is bg', () => {
    const sm = seed();
    assert.equal(sm.loadLatest()?.id, 'session-new-bg');
  });

  it('with skipBackground skips the bg session and returns the newest foreground one', () => {
    const sm = seed();
    // RED on revert: the old loadLatest ignored kind and returned session-new-bg.
    assert.equal(sm.loadLatest({ skipBackground: true })?.id, 'session-old-fg');
  });

  it('with skipBackground returns null when every session is background', () => {
    const sm = new SessionManager(process.cwd(), mkdir());
    sm.save(makeSession('session-bg-only', { kind: 'bg' }));
    assert.equal(sm.loadLatest({ skipBackground: true }), null);
  });
});

describe('safeAppendTranscript — recreate-parent-dir-and-retry', () => {
  it('still lands the entry when the first append throws transiently', () => {
    const dir = mkdir();
    const file = path.join(dir, 'sub', 'transcript.jsonl');
    const realOpen = fsHooks.openSync;
    let calls = 0;
    const m = mock.method(fsHooks, 'openSync', ((...args: Parameters<typeof fs.openSync>) => {
      calls++;
      if (calls === 1) {
        const err: NodeJS.ErrnoException = new Error('transient ENOENT');
        err.code = 'ENOENT';
        throw err;
      }
      return (realOpen as (...a: unknown[]) => number)(...args);
    }) as typeof fs.openSync);
    try {
      // RED on revert: the single-attempt version drops the entry (file never created).
      safeAppendTranscript(file, { type: 'x', timestamp: 1 });
    } finally {
      m.mock.restore();
    }
    assert.ok(calls >= 2, 'the fallback must retry openSync after the first failure');
    const content = fs.readFileSync(file, 'utf-8').trim();
    assert.deepEqual(JSON.parse(content), { type: 'x', timestamp: 1 });
  });

  it('swallows (never throws, no file) when both the append and its retry fail', () => {
    const dir = mkdir();
    const file = path.join(dir, 'always-fail.jsonl');
    const m = mock.method(fsHooks, 'openSync', (() => {
      const err: NodeJS.ErrnoException = new Error('persistent EIO');
      err.code = 'EIO';
      throw err;
    }) as typeof fs.openSync);
    try {
      assert.doesNotThrow(() => safeAppendTranscript(file, { type: 'x', timestamp: 1 }));
    } finally {
      m.mock.restore();
    }
    assert.equal(fs.existsSync(file), false);
  });

  it('writes normally when the first append succeeds (no retry)', () => {
    const dir = mkdir();
    const file = path.join(dir, 'ok.jsonl');
    safeAppendTranscript(file, { type: 'a', timestamp: 1 });
    safeAppendTranscript(file, { type: 'b', timestamp: 2 });
    const lines = fs.readFileSync(file, 'utf-8').trim().split('\n');
    assert.equal(lines.length, 2);
    assert.deepEqual(JSON.parse(lines[0]), { type: 'a', timestamp: 1 });
    assert.deepEqual(JSON.parse(lines[1]), { type: 'b', timestamp: 2 });
  });
});
