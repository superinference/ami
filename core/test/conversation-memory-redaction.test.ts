/**
 * Claude-parity (medium): human-turn predicate (excludes engine-injected meta
 * user messages) + credential-redaction guard on the memory-persistence path.
 *
 * Run: cd core/core && node --import tsx --test test/conversation-memory-redaction.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { extractConversationMeta, isHumanTurn } from '../src/conversation-summary';
import type { Message } from '../src/types';
import { MemoryManager } from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

describe('human-turn predicate (conversation-summary)', () => {
  const msgs: Message[] = [
    { role: 'user', content: 'real question' },
    { role: 'user', content: 'engine nudge', isMeta: true },
    { role: 'assistant', content: 'answer' },
    { role: 'tool', tool_call_id: 't1', content: 'tool out' },
  ];
  it('humanTurnCount excludes isMeta user messages', () => {
    const meta = extractConversationMeta(msgs);
    assert.equal(meta.userTurnCount, 2, 'raw user count includes meta');
    assert.equal(meta.humanTurnCount, 1, 'human count excludes the meta nudge');
  });
  it('isHumanTurn is true only for a non-meta user message', () => {
    assert.equal(isHumanTurn({ role: 'user', content: 'x' }), true);
    assert.equal(isHumanTurn({ role: 'user', content: 'x', isMeta: true }), false);
    assert.equal(isHumanTurn({ role: 'assistant', content: 'x' }), false);
    assert.equal(isHumanTurn({ role: 'tool', tool_call_id: 't', content: 'x' }), false);
  });
});

describe('credential redaction on memory persistence', () => {
  let tmpDir: string;
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mem-redact-')); });
  afterEach(() => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* ignore */ } });

  const SECRET = 'ghp_' + 'a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8'; // GitHub PAT shape

  it('saveMemory redacts a credential in the body before writing', () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveMemory('creds', `token is ${SECRET} keep safe`, 'has a secret', 'project');
    const memDir = projectStateDir(tmpDir, 'memory');
    const file = fs.readdirSync(memDir).find(f => f.endsWith('.md'))!;
    const raw = fs.readFileSync(path.join(memDir, file), 'utf-8');
    assert.ok(!raw.includes(SECRET), 'the raw secret must not be persisted');
    assert.ok(raw.includes('keep safe'), 'surrounding content is preserved');
  });

  it('saveSessionMemory redacts a credential inside a fact', () => {
    const mm = new MemoryManager(tmpDir);
    mm.saveSessionMemory('s1', [{ fact: `leaked ${SECRET}`, category: 'decision', confidence: 0.9 }]);
    const memDir = projectStateDir(tmpDir, 'memory');
    const raw = fs.readFileSync(path.join(memDir, 'session-s1.md'), 'utf-8');
    assert.ok(!raw.includes(SECRET), 'the raw secret must not be persisted in session memory');
  });
});
