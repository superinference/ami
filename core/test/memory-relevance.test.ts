import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { MemoryManager } from '../src/memory';

let tmpDir: string;
let memoryDir: string;

function writeMemory(name: string, description: string, memType: string, body: string): void {
  const content = `---\nname: ${name}\ndescription: ${description}\ntype: ${memType}\n---\n\n${body}\n`;
  fs.writeFileSync(path.join(memoryDir, `${name}.md`), content, 'utf-8');
}

beforeEach(() => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'si-memrel-'));
  memoryDir = path.join(tmpDir, '.superinference', 'memory');
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
