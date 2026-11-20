/**
 * Unit tests for the U021 agent-identity session additions (parity: the reference's
 * conversationRecovery session metadata agentName/agentColor/agentSetting +
 * restoreAgentFromSession's 'inherit' model sentinel / --agent override precedence):
 *
 *  - Session.agent ({ name, color, setting, model }) round-trips through save/load;
 *  - list() surfaces the agent identity on each entry so the resume picker can show
 *    which agent/persona a session ran under;
 *  - resolveAgentModelOverride implements the restore precedence: a user-supplied
 *    --agent override wins (no saved model applied); an existing model override wins;
 *    the 'inherit' sentinel (and an empty/absent model) means "use the main-loop
 *    model, don't override"; otherwise the saved agent model is applied.
 */
import { describe, it, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

import {
  SessionManager,
  resolveAgentModelOverride,
  AGENT_MODEL_INHERIT,
  type Session,
} from '../src/session';
import type { Message } from '../src/types';
import { asSessionId } from '../src/types';

const tmps: string[] = [];
function mkdir(): string {
  const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-u021-agent-'));
  tmps.push(d);
  return d;
}
after(() => {
  for (const d of tmps) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* ignore */ } }
});

function session(id: string, over: Partial<Session> = {}): Session {
  return {
    id: asSessionId(id),
    messages: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    provider: { model: 'm', baseUrl: 'u' },
    ...over,
  } as Session;
}
const user = (content: string): Message => ({ role: 'user', content } as Message);

describe('Session.agent round-trips through save/load', () => {
  it('persists and restores the full agent identity block', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('ag', {
      messages: [user('hi')],
      agent: { name: 'Researcher', color: 'cyan', setting: 'research', model: 'claude-opus-4-8' },
    }));
    const loaded = sm.load('ag')!;
    assert.deepEqual(loaded.agent, {
      name: 'Researcher', color: 'cyan', setting: 'research', model: 'claude-opus-4-8',
    });
  });

  it('round-trips the inherit model sentinel', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('ag2', { messages: [user('hi')], agent: { setting: 'x', model: AGENT_MODEL_INHERIT } }));
    assert.equal(sm.load('ag2')!.agent?.model, 'inherit');
  });

  it('a session with no agent block loads without one (back-compat)', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('ag3', { messages: [user('hi')] }));
    assert.equal(sm.load('ag3')!.agent, undefined);
  });

  it('drops a tampered non-object agent field on load (sanitize-before-trust)', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    // Hand-write a session whose `agent` is a string, not an identity record.
    fs.writeFileSync(path.join(dir, 'tampered.json'), JSON.stringify({
      id: 'tampered', messages: [{ role: 'user', content: 'hi' }],
      createdAt: 'x', updatedAt: 'x', provider: { model: 'm', baseUrl: 'u' },
      agent: 'not-a-record',
    }));
    assert.equal(sm.load('tampered')!.agent, undefined);
    // and the picker projection never crashes on it either.
    const entry = sm.list().find(e => e.id === asSessionId('tampered'))!;
    assert.equal(entry.agent, undefined);
  });
});

describe('list() surfaces agent identity', () => {
  it('carries name/color/setting onto the full-parse list entry', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('withagent', {
      messages: [user('do work')],
      agent: { name: 'Builder', color: 'green', setting: 'build', model: 'inherit' },
    }));
    const entry = sm.list().find(e => e.id === asSessionId('withagent'))!;
    assert.deepEqual(entry.agent, { name: 'Builder', color: 'green', setting: 'build' });
  });

  it('omits agent on an entry for a session that recorded none', () => {
    const dir = mkdir();
    const sm = new SessionManager('/tmp/proj', dir);
    sm.save(session('noagent', { messages: [user('do work')] }));
    const entry = sm.list().find(e => e.id === asSessionId('noagent'))!;
    assert.equal(entry.agent, undefined);
  });
});

describe('resolveAgentModelOverride precedence', () => {
  it('a user-supplied --agent override wins: no saved model is applied', () => {
    assert.equal(
      resolveAgentModelOverride({ savedModel: 'claude-opus-4-8', userPassedAgentOverride: true }),
      null,
    );
  });

  it('an existing main-loop model override wins over the saved model', () => {
    assert.equal(
      resolveAgentModelOverride({ savedModel: 'claude-opus-4-8', existingModelOverride: true }),
      null,
    );
  });

  it("the 'inherit' sentinel means use the main-loop model (no override)", () => {
    assert.equal(resolveAgentModelOverride({ savedModel: AGENT_MODEL_INHERIT }), null);
    assert.equal(resolveAgentModelOverride({ savedModel: '  inherit  ' }), null);
  });

  it('an absent / empty saved model applies no override', () => {
    assert.equal(resolveAgentModelOverride({}), null);
    assert.equal(resolveAgentModelOverride({ savedModel: '' }), null);
    assert.equal(resolveAgentModelOverride({ savedModel: '   ' }), null);
  });

  it('applies the saved agent model when nothing takes precedence', () => {
    assert.equal(
      resolveAgentModelOverride({ savedModel: 'claude-opus-4-8' }),
      'claude-opus-4-8',
    );
    // trims surrounding whitespace on the applied value
    assert.equal(
      resolveAgentModelOverride({ savedModel: '  claude-sonnet-4-5  ' }),
      'claude-sonnet-4-5',
    );
  });
});
