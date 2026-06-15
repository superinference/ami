import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'os';
import * as fs from 'fs';
import * as path from 'path';
import { searchSessions } from '../src/session-search';

describe('searchSessions', () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'search-test-'));
    const sessDir = path.join(tmpDir, '.superinference', 'sessions');
    fs.mkdirSync(sessDir, { recursive: true });
    fs.writeFileSync(path.join(sessDir, 'session-1.json'), JSON.stringify({
      id: 'session-1',
      messages: [
        { role: 'user', content: 'fix the typescript error in utils' },
        { role: 'assistant', content: 'I found the issue in the type definition' },
      ],
    }));
    fs.writeFileSync(path.join(sessDir, 'session-2.json'), JSON.stringify({
      id: 'session-2',
      messages: [
        { role: 'user', content: 'deploy to production' },
      ],
    }));
  });

  it('finds matches by term', () => {
    const results = searchSessions(tmpDir, 'typescript');
    assert.ok(results.length > 0);
    assert.equal(results[0].sessionId, 'session-1');
  });

  it('returns empty for no matches', () => {
    const results = searchSessions(tmpDir, 'nonexistentterm12345');
    assert.equal(results.length, 0);
  });

  it('returns empty for empty query', () => {
    const results = searchSessions(tmpDir, '');
    assert.equal(results.length, 0);
  });

  it('respects limit', () => {
    const results = searchSessions(tmpDir, 'the', 1);
    assert.ok(results.length <= 1);
  });
});
