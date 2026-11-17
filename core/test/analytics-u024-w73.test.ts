import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as crypto from 'crypto';

import {
  AnalyticsTracker,
  ANALYTICS_SOURCE_CRON,
  buildFileOperationEvent,
  classifyGitOperation,
  IncrementalContextTracker,
  hashFilePath,
  hashFileContent,
  MAX_CONTENT_HASH_SIZE,
  type FileOperationParams,
  type GitOperationClassification,
  type IncrementalContextDelta,
} from '../src/analytics';

// A tracker whose off-machine egress is irrelevant: we assert on the in-memory
// events (getEvents) which keep the raw data. NODE_ENV='test' suppresses OTLP egress.
function tracker(): AnalyticsTracker {
  return new AnalyticsTracker(undefined, { env: { NODE_ENV: 'test' } as NodeJS.ProcessEnv });
}

describe('U024 w73 — prompt.id per-prompt correlation dimension', () => {
  it('stamps prompt.id onto every event once set, and clears with undefined', () => {
    const t = tracker();
    t.log('before'); // no prompt id yet
    t.setPromptId('p-123');
    assert.equal(t.getPromptId(), 'p-123');
    t.log('during', { foo: 1 });
    t.setPromptId(undefined);
    assert.equal(t.getPromptId(), undefined);
    t.log('after');

    const evs = t.getEvents();
    assert.equal(evs.find(e => e.type === 'before')!.data['prompt.id'], undefined);
    assert.equal(evs.find(e => e.type === 'during')!.data['prompt.id'], 'p-123');
    assert.equal(evs.find(e => e.type === 'during')!.data.foo, 1);
    assert.equal(evs.find(e => e.type === 'after')!.data['prompt.id'], undefined);
  });

  it('does not overwrite an explicit prompt.id already present in the data', () => {
    const t = tracker();
    t.setPromptId('ambient');
    t.log('e', { 'prompt.id': 'explicit' });
    assert.equal(t.getEvents()[0].data['prompt.id'], 'explicit');
  });
});

describe('U024 w73 — structured file-operation analytics event', () => {
  it('builds privacy-preserving metadata: path hash + optional content hash + type', () => {
    const params: FileOperationParams = {
      operation: 'edit', tool: 'file_edit', filePath: '/home/u/secret/app.ts',
      content: 'const x = 1;', type: 'update',
    };
    const md = buildFileOperationEvent(params);
    assert.equal(md.operation, 'edit');
    assert.equal(md.tool, 'file_edit');
    assert.equal(md.file_path_hash, hashFilePath('/home/u/secret/app.ts'));
    assert.equal(md.content_hash, hashFileContent('const x = 1;'));
    assert.equal(md.type, 'update');
    // The raw path/content never appear.
    assert.ok(!JSON.stringify(md).includes('/home/u/secret'));
    assert.ok(!JSON.stringify(md).includes('const x = 1;'));
  });

  it('omits content_hash when content is absent OR over the size guard', () => {
    const noContent = buildFileOperationEvent({ operation: 'read', tool: 'file_read', filePath: '/a/b.py' });
    assert.equal('content_hash' in noContent, false);
    assert.equal('type' in noContent, false);

    const huge = 'x'.repeat(MAX_CONTENT_HASH_SIZE + 1);
    const over = buildFileOperationEvent({ operation: 'write', tool: 'file_write', filePath: '/a/c.bin', content: huge });
    assert.equal('content_hash' in over, false, 'oversized content must not be hashed');

    const exact = 'y'.repeat(MAX_CONTENT_HASH_SIZE);
    const atLimit = buildFileOperationEvent({ operation: 'write', tool: 'file_write', filePath: '/a/d.bin', content: exact });
    assert.equal(atLimit.content_hash, hashFileContent(exact));
  });

  it('logFileOperation routes through the tracker log pipeline', () => {
    const t = tracker();
    t.logFileOperation({ operation: 'write', tool: 'file_write', filePath: '/x/y.ts', content: 'abc', type: 'create' });
    const evs = t.getEventsByType('file_operation');
    assert.equal(evs.length, 1);
    assert.equal(evs[0].data.operation, 'write');
    assert.equal(evs[0].data.type, 'create');
    assert.equal(evs[0].data.file_path_hash, hashFilePath('/x/y.ts'));
  });
});

describe('U024 w73 — local git/PR operation classification + counters', () => {
  const classify = (cmd: string, code = 0): GitOperationClassification => classifyGitOperation(cmd, code);

  it('counts a successful commit and its amend variant', () => {
    const c = classify('git commit -m "x"');
    assert.deepEqual(c.operations, ['commit']);
    assert.equal(c.commitDelta, 1);
    assert.equal(c.prDelta, 0);

    const amend = classify('git commit --amend --no-edit');
    assert.deepEqual(amend.operations, ['commit', 'commit_amend']);
    assert.equal(amend.commitDelta, 1);
  });

  it('tolerates git global options between git and the subcommand', () => {
    const c = classify('git -c commit.gpgsign=false commit -m y');
    assert.ok(c.operations.includes('commit'));
    const p = classify('git -C /repo push origin main');
    assert.ok(p.operations.includes('push'));
  });

  it('classifies gh pr actions and glab mr create (PR-create increments prDelta)', () => {
    assert.deepEqual(classify('gh pr create --fill').operations, ['pr_create']);
    assert.equal(classify('gh pr create --fill').prDelta, 1);
    assert.equal(classify('gh pr merge 5').prDelta, 0);
    assert.deepEqual(classify('gh pr merge 5').operations, ['pr_merge']);
    assert.equal(classify('glab mr create -f').prDelta, 1);
  });

  it('detects curl-based PR creation but excludes sub-resources and non-POST', () => {
    assert.equal(classify('curl -X POST https://api.github.com/repos/o/r/pulls -d @body').prDelta, 1);
    assert.equal(classify('curl -d @b https://gitlab.com/api/v4/projects/1/merge_requests').prDelta, 1, 'implicit POST via -d');
    // Sub-resource: a comment on an existing PR is NOT a create.
    assert.equal(classify('curl -X POST https://api.github.com/repos/o/r/pulls/123/comments -d @c').prDelta, 0);
    // GET (no POST) to a PR endpoint is not a create.
    assert.equal(classify('curl https://api.github.com/repos/o/r/pulls').prDelta, 0);
  });

  it('a non-zero exit or a non-git command yields an empty classification', () => {
    assert.deepEqual(classify('git commit -m x', 1), { operations: [], commitDelta: 0, prDelta: 0 });
    assert.deepEqual(classify('ls -la'), { operations: [], commitDelta: 0, prDelta: 0 });
    assert.deepEqual(classify(''), { operations: [], commitDelta: 0, prDelta: 0 });
  });

  it('trackGitOperation increments counters and emits one event per operation', () => {
    const t = tracker();
    t.trackGitOperation('git commit --amend -m z', 0);
    t.trackGitOperation('gh pr create --fill', 0);
    t.trackGitOperation('ls', 0); // no-op
    t.trackGitOperation('git push', 1); // failed → no-op
    assert.equal(t.getCommitOperationCount(), 1);
    assert.equal(t.getPrOperationCount(), 1);
    const ops = t.getEventsByType('git_operation').map(e => e.data.operation);
    assert.deepEqual(ops, ['commit', 'commit_amend', 'pr_create']);
  });

  it('reset() zeros the usage counters', () => {
    const t = tracker();
    t.trackGitOperation('git commit -m a', 0);
    assert.equal(t.getCommitOperationCount(), 1);
    t.reset();
    assert.equal(t.getCommitOperationCount(), 0);
    assert.equal(t.getPrOperationCount(), 0);
  });
});

describe('U024 w73 — per-agent incremental context delta', () => {
  type Msg = { id: string; role: 'user' | 'assistant' };
  const hashOf = (m: Msg) => crypto.createHash('sha256').update(m.id).digest('hex');
  const isUser = (m: Msg) => m.role === 'user';

  it('first call reports all user messages and remembers the last message', () => {
    const tr = new IncrementalContextTracker();
    const msgs: Msg[] = [
      { id: 'u1', role: 'user' }, { id: 'a1', role: 'assistant' }, { id: 'u2', role: 'user' },
    ];
    const d = tr.delta('main', msgs, hashOf, isUser);
    assert.equal(d.startIndex, 0);
    assert.equal(d.count, 2);
    assert.deepEqual(d.newMessages.map(m => m.id), ['u1', 'u2']);
    assert.equal(tr.trackedSourceCount, 1);
  });

  it('a later call reports only the NEW user messages since the previous one', () => {
    const tr = new IncrementalContextTracker();
    const first: Msg[] = [{ id: 'u1', role: 'user' }, { id: 'a1', role: 'assistant' }];
    tr.delta('main', first, hashOf, isUser); // remembers a1 (last message)
    const second: Msg[] = [...first, { id: 'u2', role: 'user' }, { id: 'a2', role: 'assistant' }];
    const d = tr.delta('main', second, hashOf, isUser);
    assert.equal(d.startIndex, 2); // after a1
    assert.deepEqual(d.newMessages.map(m => m.id), ['u2']);
  });

  it('falls back to the whole array when the prior hash is gone (compaction)', () => {
    const tr = new IncrementalContextTracker();
    tr.delta('main', [{ id: 'u1', role: 'user' }, { id: 'a1', role: 'assistant' }], hashOf, isUser);
    // The conversation was compacted — a1 no longer present.
    const compacted: Msg[] = [{ id: 'u9', role: 'user' }];
    const d = tr.delta('main', compacted, hashOf, isUser);
    assert.equal(d.startIndex, 0);
    assert.deepEqual(d.newMessages.map(m => m.id), ['u9']);
  });

  it('tracks each source independently', () => {
    const tr = new IncrementalContextTracker();
    tr.delta('main', [{ id: 'm1', role: 'user' }], hashOf, isUser);
    const sub = tr.delta('subagent', [{ id: 's1', role: 'user' }], hashOf, isUser);
    assert.deepEqual(sub.newMessages.map(m => m.id), ['s1']);
    assert.equal(tr.trackedSourceCount, 2);
    // main unaffected by subagent's report
    const again = tr.delta('main', [{ id: 'm1', role: 'user' }], hashOf, isUser);
    assert.equal(again.count, 0, 'no new main messages');
  });

  it('an empty delta does NOT advance the remembered hash', () => {
    const tr = new IncrementalContextTracker();
    const msgs: Msg[] = [{ id: 'u1', role: 'user' }];
    tr.delta('main', msgs, hashOf, isUser); // remembers u1
    // Same array again → no new user messages.
    const d2 = tr.delta('main', msgs, hashOf, isUser);
    assert.equal(d2.count, 0);
    // A brand-new array whose only prior anchor is u1 still finds u1 and reports after it.
    const d3 = tr.delta('main', [...msgs, { id: 'u2', role: 'user' }], hashOf, isUser);
    assert.deepEqual(d3.newMessages.map(m => m.id), ['u2']);
  });

  it('reset(source) forgets one source; reset() forgets all', () => {
    const tr = new IncrementalContextTracker();
    tr.delta('a', [{ id: 'a1', role: 'user' }], hashOf, isUser);
    tr.delta('b', [{ id: 'b1', role: 'user' }], hashOf, isUser);
    tr.reset('a');
    assert.equal(tr.trackedSourceCount, 1);
    tr.reset();
    assert.equal(tr.trackedSourceCount, 0);
  });

  it('recordIncrementalContext emits a privacy-safe context_delta event (count only)', () => {
    const t = tracker();
    const msgs: Msg[] = [{ id: 'u1', role: 'user' }, { id: 'u2', role: 'user' }];
    const d: IncrementalContextDelta<Msg> = t.recordIncrementalContext('main', msgs, hashOf, isUser);
    assert.equal(d.count, 2);
    const evs = t.getEventsByType('context_delta');
    assert.equal(evs.length, 1);
    assert.equal(evs[0].data.source, 'main');
    assert.equal(evs[0].data.new_context_message_count, 2);
    // Raw message ids/content must NOT be in the event.
    assert.ok(!JSON.stringify(evs[0].data).includes('u1'));

    // A no-delta call emits nothing.
    t.recordIncrementalContext('main', msgs, hashOf, isUser);
    assert.equal(t.getEventsByType('context_delta').length, 1);
  });
});

describe('U024 w73 — cron invocation-source constant', () => {
  it('exposes the cron source tag value', () => {
    assert.equal(ANALYTICS_SOURCE_CRON, 'cron');
  });
});
