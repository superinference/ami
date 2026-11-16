import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { MemoryManager } from '../src/memory';
import { projectStateDir } from '../src/reference-journal';

let tmpDir: string;
let memoryDir: string;

function writeMemory(name: string, description: string, memType: string, body: string): void {
  const content = `---\nname: ${name}\ndescription: ${description}\ntype: ${memType}\n---\n\n${body}\n`;
  fs.writeFileSync(path.join(memoryDir, `${name}.md`), content, 'utf-8');
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memrel-'));
  memoryDir = projectStateDir(tmpDir, 'memory');
  fs.mkdirSync(memoryDir, { recursive: true });
});

afterEach(() => {
  fs.rmSync(tmpDir, { recursive: true, force: true });
});

describe('MemoryManager – selectRelevantMemories', () => {
  it('returns all memories when count <= maxResults', () => {
    writeMemory('mem1', 'user preferences', 'user', 'prefers dark mode');
    writeMemory('mem2', 'project context', 'project', 'working on auth');

    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('anything', 5);
    assert.equal(result.length, 2);
  });

  it('returns empty array when no memories exist', () => {
    fs.rmSync(memoryDir, { recursive: true, force: true });
    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('test query', 5);
    assert.equal(result.length, 0);
  });

  it('selects most relevant memories based on query', () => {
    writeMemory('auth-feedback', 'authentication approach', 'feedback', 'Always use JWT tokens for authentication. Never store passwords in plain text.');
    writeMemory('deploy-ref', 'deployment pipeline', 'reference', 'Deploy using GitHub Actions. Run terraform apply for infrastructure.');
    writeMemory('user-role', 'user is senior engineer', 'user', 'The user is a senior backend engineer with 10 years Go experience.');
    writeMemory('db-project', 'database migration', 'project', 'PostgreSQL migration from v14 to v16. Schema changes in progress.');
    writeMemory('testing-feedback', 'testing approach', 'feedback', 'Always write integration tests before unit tests. Use real database connections.');
    writeMemory('css-project', 'CSS framework choice', 'project', 'Using Tailwind CSS for all frontend components. Avoid inline styles.');
    writeMemory('api-ref', 'API documentation', 'reference', 'REST API docs at /api/docs. GraphQL playground at /graphql.');

    const mm = new MemoryManager(tmpDir);

    const authResult = mm.selectRelevantMemories('implement JWT authentication for the login endpoint', 3);
    assert.equal(authResult.length, 3);
    const authNames = authResult.map(m => m.name);
    assert.ok(authNames.includes('auth-feedback'), `Expected auth-feedback in results, got: ${authNames.join(', ')}`);

    const dbResult = mm.selectRelevantMemories('PostgreSQL database migration schema', 3);
    const dbNames = dbResult.map(m => m.name);
    assert.ok(dbNames.includes('db-project'), `Expected db-project in results, got: ${dbNames.join(', ')}`);

    const cssResult = mm.selectRelevantMemories('tailwind CSS frontend components styling', 3);
    const cssNames = cssResult.map(m => m.name);
    assert.ok(cssNames.includes('css-project'), `Expected css-project in results, got: ${cssNames.join(', ')}`);
  });

  it('handles empty query gracefully', () => {
    writeMemory('mem1', 'first', 'user', 'content');
    writeMemory('mem2', 'second', 'user', 'content');
    writeMemory('mem3', 'third', 'user', 'content');
    writeMemory('mem4', 'fourth', 'user', 'content');
    writeMemory('mem5', 'fifth', 'user', 'content');
    writeMemory('mem6', 'sixth', 'user', 'content');

    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('', 5);
    assert.equal(result.length, 5);
  });

  it('respects maxResults parameter', () => {
    for (let i = 0; i < 10; i++) {
      writeMemory(`mem${i}`, `memory number ${i}`, 'user', `content for memory ${i}`);
    }

    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('memory content', 3);
    assert.equal(result.length, 3);
  });

  it('handles query with no matching tokens', () => {
    writeMemory('mem1', 'golang development', 'user', 'prefers Go');
    writeMemory('mem2', 'python testing', 'project', 'uses pytest');

    const mm = new MemoryManager(tmpDir);
    const result = mm.selectRelevantMemories('xyznonexistent', 1);
    assert.equal(result.length, 1);
  });
});

describe('MemoryManager – selectRelevantMemories recentTools filter', () => {
  it('suppresses a pure usage/API reference memory for a tool in active use', () => {
    writeMemory('grep-ref', 'grep command reference', 'reference',
      'grep -rn pattern to search files. Usage reference for the grep tool.');
    writeMemory('grep-warn', 'grep known issues', 'reference',
      'Warning: grep is extremely slow on node_modules; avoid running it there.');
    writeMemory('unrelated', 'unrelated note', 'project', 'something else entirely about deploys');

    const mm = new MemoryManager(tmpDir);

    // Without recentTools, the pure reference is a candidate.
    const baseline = mm.selectRelevantMemories('how do I grep for a pattern', 5).map(m => m.name);
    assert.ok(baseline.includes('grep-ref'), `baseline should include grep-ref, got ${baseline.join(', ')}`);

    // With grep in active use, its usage reference is suppressed but the warning is kept.
    const filtered = mm.selectRelevantMemories('how do I grep for a pattern', 5, { recentTools: ['grep'] })
      .map(m => m.name);
    assert.ok(!filtered.includes('grep-ref'), `grep-ref should be suppressed, got ${filtered.join(', ')}`);
    assert.ok(filtered.includes('grep-warn'), 'grep warning memory must be preserved for an active tool');
    assert.ok(filtered.includes('unrelated'), 'unrelated memory is unaffected');
  });

  it('only suppresses reference-type memories, never project/user/feedback', () => {
    writeMemory('grep-project', 'grep workflow', 'project', 'We rely on grep in our build scripts.');
    writeMemory('grep-ref', 'grep reference', 'reference', 'grep usage: grep -rn pattern files.');
    const mm = new MemoryManager(tmpDir);
    const names = mm.selectRelevantMemories('grep', 5, { recentTools: ['grep'] }).map(m => m.name);
    assert.ok(names.includes('grep-project'), 'non-reference memory mentioning the tool is kept');
    assert.ok(!names.includes('grep-ref'), 'reference memory for the active tool is suppressed');
  });

  it('ignores empty/whitespace tool names and an empty recentTools list', () => {
    writeMemory('grep-ref', 'grep reference', 'reference', 'grep usage docs for the grep tool.');
    const mm = new MemoryManager(tmpDir);
    // Empty list → no filtering.
    assert.ok(mm.selectRelevantMemories('grep', 5, { recentTools: [] }).map(m => m.name).includes('grep-ref'));
    // Whitespace-only entries are ignored (no tool actually in use).
    assert.ok(mm.selectRelevantMemories('grep', 5, { recentTools: ['', '   '] }).map(m => m.name).includes('grep-ref'));
  });

  it('does not suppress a reference memory for a tool NOT in the recent list', () => {
    writeMemory('grep-ref', 'grep reference', 'reference', 'grep usage docs.');
    const mm = new MemoryManager(tmpDir);
    const names = mm.selectRelevantMemories('grep', 5, { recentTools: ['bash'] }).map(m => m.name);
    assert.ok(names.includes('grep-ref'), 'a reference for an inactive tool is not suppressed');
  });
});

describe('MemoryManager – selectRelevantMemories alreadySurfaced dedup', () => {
  it('filters already-surfaced paths before the count short-circuit', () => {
    writeMemory('a', 'first note', 'project', 'content about deploy pipelines');
    writeMemory('b', 'second note', 'project', 'content about deploy rollbacks');
    writeMemory('c', 'third note', 'project', 'content about deploy monitoring');
    const mm = new MemoryManager(tmpDir);

    const first = mm.selectRelevantMemories('deploy', 5);
    assert.equal(first.length, 3);
    const surfacedPath = first[0].path;

    const second = mm.selectRelevantMemories('deploy', 5, { alreadySurfaced: new Set([surfacedPath]) });
    assert.equal(second.length, 2, 'the surfaced memory is excluded');
    assert.ok(!second.some(m => m.path === surfacedPath));
  });

  it('filters already-surfaced paths before BM25 scoring when over the budget', () => {
    for (let i = 0; i < 7; i++) {
      writeMemory(`deploy${i}`, `deploy note ${i}`, 'project', `deploy runbook content number ${i} for the deploy pipeline`);
    }
    const mm = new MemoryManager(tmpDir);
    const first = mm.selectRelevantMemories('deploy pipeline runbook', 3);
    assert.equal(first.length, 3);
    const surfaced = new Set(first.map(m => m.path));

    const second = mm.selectRelevantMemories('deploy pipeline runbook', 3, { alreadySurfaced: surfaced });
    assert.equal(second.length, 3);
    for (const m of second) {
      assert.ok(!surfaced.has(m.path), 'no re-pick of an already-surfaced memory');
    }
  });

  it('treats an empty alreadySurfaced set as a no-op', () => {
    writeMemory('a', 'first note', 'project', 'content one');
    writeMemory('b', 'second note', 'project', 'content two');
    const mm = new MemoryManager(tmpDir);
    const res = mm.selectRelevantMemories('content', 5, { alreadySurfaced: new Set() });
    assert.equal(res.length, 2);
  });
});
