/**
 * U021 (w81) — Schema/structural validation of a restored persisted session.
 *
 * Parity: the reference's safeParse-before-apply on restored persisted state
 * (sessionRestore.ts extractTodosFromTranscript / restoreSessionStateFromLog —
 * it validates untrusted on-disk data with a zod schema and drops malformed
 * input rather than trusting it). AMI's readSessionFile previously did a raw
 * `JSON.parse(raw) as Session` cast with no structural check, so a tampered /
 * corrupt blob (non-object top level, missing id, non-array messages, or a
 * garbage "message" entry) was rehydrated and handed to the engine as-is.
 *
 * These drive the REAL public load()/loadAsync() path (write a crafted file to a
 * temp session dir, then resume it), so they exercise the whole wired read path.
 *
 * Run: cd core/core && node --import tsx --test test/session-schema-validation-u021.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { SessionManager } from '../src/session';
import type { Session } from '../src/session';

let dir: string;
beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sess-schema-')); });
afterEach(() => { try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* ignore */ } });

/** Write a raw JSON blob directly to the session store, bypassing save() so we can
 *  plant deliberately malformed/tampered on-disk content. */
function plant(id: string, raw: unknown): void {
  fs.writeFileSync(path.join(dir, `${id}.json`), JSON.stringify(raw));
}

function validSession(id: string): Record<string, unknown> {
  return {
    id,
    messages: [{ role: 'user', content: 'hello' }],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'b' },
  };
}

describe('readSessionFile structural validation (U021)', () => {
  it('rejects a top-level non-object blob (array) as not-found', () => {
    const mgr = new SessionManager(dir, dir);
    plant('arr', [1, 2, 3]);
    assert.equal(mgr.load('arr'), null, 'an array top level must not be cast-trusted as a Session');
  });

  it('rejects a top-level JSON string/number/null as not-found', () => {
    const mgr = new SessionManager(dir, dir);
    plant('str', 'not a session');
    plant('num', 42);
    plant('nul', null);
    assert.equal(mgr.load('str'), null);
    assert.equal(mgr.load('num'), null);
    assert.equal(mgr.load('nul'), null);
  });

  it('rejects a session blob with no usable top-level id', () => {
    const mgr = new SessionManager(dir, dir);
    const s = validSession('noid');
    delete s.id;
    plant('noid', s);
    assert.equal(mgr.load('noid'), null, 'a session without a string id must be dropped');
    // Empty/whitespace id is likewise unusable.
    const s2 = validSession('blank');
    s2.id = '   ';
    plant('blank', s2);
    assert.equal(mgr.load('blank'), null);
  });

  it('coerces a non-array messages field to an empty array', () => {
    const mgr = new SessionManager(dir, dir);
    const s = validSession('badmsgs');
    s.messages = 'totally not an array';
    plant('badmsgs', s);
    const loaded = mgr.load('badmsgs');
    assert.ok(loaded, 'the session itself is still resumable');
    assert.ok(Array.isArray(loaded!.messages), 'messages must be coerced to an array');
    assert.equal(loaded!.messages.length, 0);
  });

  it('drops tampered/garbage message entries, keeping only well-formed turns', () => {
    const mgr = new SessionManager(dir, dir);
    const s = validSession('mixed');
    s.messages = [
      { role: 'user', content: 'keep me' },
      { role: 'hacker', content: 'unknown role' },   // bad role
      { content: 'no role at all' },                  // missing role
      42,                                             // not an object
      null,                                           // not an object
      ['nested'],                                     // array, not a message
      { role: 'assistant', content: 'keep me too' },
    ];
    plant('mixed', s);
    const loaded = mgr.load('mixed');
    assert.ok(loaded);
    assert.deepEqual(
      loaded!.messages.map(m => m.role),
      ['user', 'assistant'],
      'only well-formed message records survive',
    );
  });

  it('accepts all four valid roles', () => {
    const mgr = new SessionManager(dir, dir);
    const s = validSession('roles');
    s.messages = [
      { role: 'system', content: 'sys' },
      { role: 'user', content: 'u' },
      { role: 'assistant', content: 'a' },
      { role: 'tool', content: 'r', tool_call_id: 't1' },
    ];
    plant('roles', s);
    const loaded = mgr.load('roles');
    assert.ok(loaded);
    assert.deepEqual(loaded!.messages.map(m => m.role), ['system', 'user', 'assistant', 'tool']);
  });

  it('round-trips a normal saved session unchanged (regression lock)', () => {
    const mgr = new SessionManager(dir, dir);
    const session: Session = {
      id: 'ok' as Session['id'],
      messages: [
        { role: 'user', content: 'what is 2+2' },
        { role: 'assistant', content: '4' },
      ],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      provider: { model: 'm', baseUrl: 'b' },
    };
    mgr.save(session);
    const loaded = mgr.load('ok');
    assert.ok(loaded);
    assert.equal(loaded!.messages.length, 2);
    assert.equal(loaded!.messages[0].content, 'what is 2+2');
  });

  it('still drops an unknown settings.permissionMode (existing allowlist preserved)', () => {
    const mgr = new SessionManager(dir, dir);
    const s = validSession('perm');
    s.settings = { permissionMode: 'totally-bogus-mode' };
    plant('perm', s);
    const loaded = mgr.load('perm');
    assert.ok(loaded);
    assert.equal(loaded!.settings?.permissionMode, undefined, 'bogus permission mode must be dropped');
  });

  it('applies the same validation on the async load path (loadAsync)', async () => {
    const mgr = new SessionManager(dir, dir);
    // non-object → null
    plant('aarr', [1, 2, 3]);
    assert.equal(await mgr.loadAsync('aarr'), null);
    // garbage messages dropped
    const s = validSession('amixed');
    s.messages = [{ role: 'user', content: 'a' }, { role: 'nope' }, 7];
    plant('amixed', s);
    const loaded = await mgr.loadAsync('amixed');
    assert.ok(loaded);
    assert.deepEqual(loaded!.messages.map(m => m.role), ['user']);
  });
});
