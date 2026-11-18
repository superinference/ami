import { describe, it, after, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFileSync } from 'child_process';
import {
  recordAgentFileContribution,
  recordUserPrompt,
  getSessionPromptCount,
} from '../src/tools/attribution-store';
import {
  gitCommitTool,
  buildAttributionNote,
  aggregateAgentContributionPercent,
  countMemoriesRecalled,
  formatContributionByline,
  ATTRIBUTION_NOTES_REF,
  type CommitFileContribution,
  type CommitAttributionNote,
} from '../src/tools/git-commit';
import type { ToolContext } from '../src/types';
import { journalPathFor, buildAssistantEntry, appendEntry } from '../src/reference-journal';

function ctx(cwd: string, overrides?: Partial<ToolContext>): ToolContext {
  return { cwd, abortSignal: new AbortController().signal, ...overrides };
}

function git(cwd: string, ...args: string[]): string {
  return execFileSync('git', args, {
    cwd,
    encoding: 'utf-8',
    env: { ...process.env, GIT_TERMINAL_PROMPT: '0', GIT_ASKPASS: '', GIT_CONFIG_NOSYSTEM: '1' },
    stdio: ['pipe', 'pipe', 'pipe'],
  }).trim();
}

const dirs: string[] = [];
function mkTmp(prefix: string): string {
  const dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), prefix)));
  dirs.push(dir);
  return dir;
}
function mkRepo(prefix: string): string {
  const dir = mkTmp(prefix);
  git(dir, 'init', '-q', '-b', 'main');
  git(dir, 'config', 'user.email', 't@t.t');
  git(dir, 'config', 'user.name', 'T');
  return dir;
}

function contrib(p: Partial<CommitFileContribution>): CommitFileContribution {
  return { path: 'f', agentContribution: 0, totalChars: 0, ratio: 0, ...p };
}

after(() => {
  for (const d of dirs) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* best effort */ } }
});

describe('formatContributionByline', () => {
  it('renders the full byline with model and plural memories', () => {
    assert.equal(
      formatContributionByline({ contributionPercent: 93, promptCount: 3, memoriesRecalled: 2, model: 'opus-4-8' }),
      '93% 3-shotted by opus-4-8, 2 memories recalled',
    );
  });

  it('uses the singular "memory" at a count of one', () => {
    assert.equal(
      formatContributionByline({ contributionPercent: 50, promptCount: 1, memoriesRecalled: 1, model: 'opus-4-8' }),
      '50% 1-shotted by opus-4-8, 1 memory recalled',
    );
  });

  it('omits the memory clause at zero memories', () => {
    assert.equal(
      formatContributionByline({ contributionPercent: 80, promptCount: 2, memoriesRecalled: 0, model: 'opus-4-8' }),
      '80% 2-shotted by opus-4-8',
    );
  });

  it('omits the " by <model>" clause when the model is unknown', () => {
    assert.equal(
      formatContributionByline({ contributionPercent: 40, promptCount: 2, memoriesRecalled: 0, model: undefined }),
      '40% 2-shotted',
    );
  });

  it('returns the empty string (all-zero fallback) when no signal is present', () => {
    assert.equal(
      formatContributionByline({ contributionPercent: 0, promptCount: 0, memoriesRecalled: 0, model: 'opus-4-8' }),
      '',
    );
  });

  it('renders when only a percent is present (prompts/memories zero)', () => {
    assert.equal(
      formatContributionByline({ contributionPercent: 10, promptCount: 0, memoriesRecalled: 0, model: undefined }),
      '10% 0-shotted',
    );
  });
});

describe('aggregateAgentContributionPercent', () => {
  it('returns 0 for an empty list', () => {
    assert.equal(aggregateAgentContributionPercent([]), 0);
  });

  it('aggregates authored chars over total chars across files', () => {
    assert.equal(
      aggregateAgentContributionPercent([
        contrib({ agentContribution: 30, totalChars: 100 }),
        contrib({ agentContribution: 70, totalChars: 100 }),
      ]),
      50,
    );
  });

  it('returns 0 when every file is empty (zero total size)', () => {
    assert.equal(
      aggregateAgentContributionPercent([contrib({ agentContribution: 0, totalChars: 0 })]),
      0,
    );
  });

  it('clamps to 100 and ignores negative/overflowing contributions', () => {
    assert.equal(
      aggregateAgentContributionPercent([
        contrib({ agentContribution: 999, totalChars: 100 }),
        contrib({ agentContribution: -5, totalChars: 0 }),
      ]),
      100,
    );
  });
});

describe('recordUserPrompt / getSessionPromptCount', () => {
  it('starts at zero and counts each recorded prompt for a session', () => {
    const sid = 'sess-' + Math.random().toString(36).slice(2);
    assert.equal(getSessionPromptCount(sid), 0);
    recordUserPrompt(sid);
    recordUserPrompt(sid);
    recordUserPrompt(sid);
    assert.equal(getSessionPromptCount(sid), 3);
  });

  it('shares a global bucket for an undefined session id', () => {
    const before = getSessionPromptCount(undefined);
    recordUserPrompt(undefined);
    assert.equal(getSessionPromptCount(undefined), before + 1);
  });
});

describe('countMemoriesRecalled', () => {
  let memDir: string;
  const prev = process.env.AMI_MEMORY_DIR;
  beforeEach(() => {
    memDir = mkTmp('ami-memdir-');
    process.env.AMI_MEMORY_DIR = memDir;
  });
  after(() => {
    if (prev === undefined) delete process.env.AMI_MEMORY_DIR;
    else process.env.AMI_MEMORY_DIR = prev;
  });

  // Build a real reference journal (assistant tool_use lines) for a session so the
  // counter exercises the true persisted transcript path, not a synthetic stub.
  function writeJournal(
    cwd: string,
    sessionId: string,
    toolUses: { name: string; input: unknown }[],
    extra: string[] = [],
  ): void {
    const journalPath = journalPathFor(cwd, sessionId);
    const entry = buildAssistantEntry({
      uuid: 'u1',
      parentUuid: null,
      timestamp: new Date().toISOString(),
      sessionId,
      cwd,
      gitBranch: 'main',
      isSidechain: false,
      messageId: `${sessionId}:1`,
      model: 'claude-opus-4-8',
      stopReason: 'tool_use',
      usage: { promptTokens: 1, completionTokens: 1 },
      content: toolUses.map((t, i) => ({ type: 'tool_use' as const, id: `t${i}`, name: t.name, input: t.input })),
    });
    appendEntry(journalPath, entry);
    for (const line of extra) appendEntry(journalPath, JSON.parse(line));
  }

  it('returns 0 when the session id is undefined or no journal exists yet', () => {
    const cwd = mkTmp('ami-cwd-');
    assert.equal(countMemoriesRecalled(cwd, undefined), 0);
    assert.equal(countMemoriesRecalled(cwd, 'no-such-session'), 0);
  });

  it('counts read/edit/write/grep memory accesses from the session journal, skipping non-memory and non-allowlisted tools', () => {
    const cwd = mkTmp('ami-cwd-');
    const sessionId = 'sess-mem-1';
    const memA = path.join(memDir, 'topic.md');
    const memSession = path.join(memDir, 'session-7.md');
    const notMem = path.join(cwd, 'src.ts');
    writeJournal(cwd, sessionId, [
      { name: 'file_read', input: { file_path: memA } },       // read inside memory dir → counts
      { name: 'file_edit', input: { file_path: memA } },       // edit inside memory dir → counts
      { name: 'file_write', input: { file_path: memSession } },// write inside memory dir → counts
      { name: 'grep', input: { path: memSession } },           // grep a session-*.md → session_memory → counts
      { name: 'file_read', input: { file_path: notMem } },     // ordinary project file → skip
      { name: 'bash', input: { command: `cat ${memA}` } },     // not in the allowlist → skip
    ]);
    assert.equal(countMemoriesRecalled(cwd, sessionId), 4);
  });

  it('ignores user entries and malformed journal lines', () => {
    const cwd = mkTmp('ami-cwd-');
    const sessionId = 'sess-mem-2';
    const memA = path.join(memDir, 'topic.md');
    writeJournal(
      cwd,
      sessionId,
      [{ name: 'file_read', input: { file_path: memA } }],
      [JSON.stringify({ type: 'user', message: { content: 'hi' } })],
    );
    // Append a non-JSON line directly — must be skipped, not throw.
    fs.appendFileSync(journalPathFor(cwd, sessionId), 'not-json\n');
    assert.equal(countMemoriesRecalled(cwd, sessionId), 1);
  });
});

describe('buildAttributionNote contribution stats', () => {
  it('populates the computed signals and a rendered byline', () => {
    const note = buildAttributionNote({
      surface: 'cli',
      model: 'opus-4-8',
      trailerApplied: true,
      files: ['a.ts'],
      fileContributions: [contrib({ path: 'a.ts', agentContribution: 50, totalChars: 100, ratio: 0.5 })],
      contributionPercent: 50,
      promptCount: 2,
      memoriesRecalled: 1,
    });
    assert.equal(note.contributionPercent, 50);
    assert.equal(note.promptCount, 2);
    assert.equal(note.memoriesRecalled, 1);
    assert.equal(note.byline, '50% 2-shotted by opus-4-8, 1 memory recalled');
  });

  it('omits every zero signal and the byline when there is no data', () => {
    const note = buildAttributionNote({
      surface: 'cli',
      model: undefined,
      trailerApplied: false,
      files: ['a.ts'],
    });
    assert.equal(note.contributionPercent, undefined);
    assert.equal(note.promptCount, undefined);
    assert.equal(note.memoriesRecalled, undefined);
    assert.equal(note.byline, undefined);
  });
});

describe('git_commit writes contribution stats into the attribution note (integration)', () => {
  it('records percent, N-shotted, memories, and byline on the commit note', async () => {
    const repo = mkRepo('ami-u105cs-');
    const memDir = mkTmp('ami-u105mem-');
    const prevMem = process.env.AMI_MEMORY_DIR;
    const prevNotes = process.env.AMI_COMMIT_ATTRIBUTION_NOTES;
    const prevModel = process.env.AMI_MODEL;
    process.env.AMI_MEMORY_DIR = memDir;
    process.env.AMI_COMMIT_ATTRIBUTION_NOTES = '1';
    process.env.AMI_MODEL = 'claude-opus-4-8-20260115';
    const sessionId = 'u105cs-' + Math.random().toString(36).slice(2);
    try {
      // A tracked, agent-authored file.
      const rel = 'feature.ts';
      const abs = path.join(repo, rel);
      const content = 'export const x = 1;\n';
      fs.writeFileSync(abs, content);
      // Stored under the realpath-normalized repo-relative key the edit tool uses.
      recordAgentFileContribution(sessionId, rel, content, content.length, Date.now());

      // Two user prompts ("2-shotted").
      recordUserPrompt(sessionId);
      recordUserPrompt(sessionId);

      // A memory file the agent recalled this session — recorded as a real
      // assistant tool_use in the session journal (the counter's data source).
      const memFile = path.join(memDir, 'notes.md');
      fs.writeFileSync(memFile, 'remember');
      appendEntry(
        journalPathFor(repo, sessionId),
        buildAssistantEntry({
          uuid: 'u1', parentUuid: null, timestamp: new Date().toISOString(),
          sessionId, cwd: repo, gitBranch: 'main', isSidechain: false,
          messageId: `${sessionId}:1`, model: 'claude-opus-4-8', stopReason: 'tool_use',
          usage: { promptTokens: 1, completionTokens: 1 },
          content: [{ type: 'tool_use', id: 't0', name: 'file_read', input: { file_path: memFile } }],
        }),
      );

      const res = await gitCommitTool.execute(
        { message: 'add feature', files: [rel] },
        ctx(repo, { sessionId }),
      );
      assert.equal(res.isError, false, res.output);

      const noteJson = git(repo, 'notes', `--ref=${ATTRIBUTION_NOTES_REF}`, 'show', 'HEAD');
      const note = JSON.parse(noteJson) as CommitAttributionNote;
      assert.equal(note.promptCount, 2);
      assert.equal(note.memoriesRecalled, 1);
      assert.equal(note.contributionPercent, 100);
      assert.equal(note.model, 'opus-4-8');
      assert.equal(note.byline, '100% 2-shotted by opus-4-8, 1 memory recalled');
    } finally {
      if (prevMem === undefined) delete process.env.AMI_MEMORY_DIR; else process.env.AMI_MEMORY_DIR = prevMem;
      if (prevNotes === undefined) delete process.env.AMI_COMMIT_ATTRIBUTION_NOTES; else process.env.AMI_COMMIT_ATTRIBUTION_NOTES = prevNotes;
      if (prevModel === undefined) delete process.env.AMI_MODEL; else process.env.AMI_MODEL = prevModel;
    }
  });
});
