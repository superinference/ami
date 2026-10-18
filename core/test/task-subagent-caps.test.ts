import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  checkAgentCaps,
  DEFAULT_MAX_AGENTS_PER_SESSION,
  DEFAULT_MAX_CONCURRENT_AGENTS,
} from '../src/tools/task';

describe('DEFAULT_MAX_AGENTS_PER_SESSION', () => {
  it('defaults to 50', () => {
    assert.equal(DEFAULT_MAX_AGENTS_PER_SESSION, 50);
  });
});

describe('DEFAULT_MAX_CONCURRENT_AGENTS', () => {
  it('defaults to 10', () => {
    assert.equal(DEFAULT_MAX_CONCURRENT_AGENTS, 10);
  });
});

describe('checkAgentCaps — session cap', () => {
  it('allows spawn when under session cap', () => {
    const ctx = { _agentSpawnCount: { value: 5 } };
    assert.equal(checkAgentCaps(ctx), null);
  });

  it('allows spawn at 0 with no prior context', () => {
    assert.equal(checkAgentCaps({}), null);
  });

  it('blocks spawn when session cap is reached (default 50)', () => {
    const ctx = { _agentSpawnCount: { value: 50 } };
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('session cap'));
    assert.ok(err!.includes('50'));
  });

  it('blocks spawn when over session cap', () => {
    const ctx = { _agentSpawnCount: { value: 60 } };
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('session cap'));
  });

  it('respects custom session cap', () => {
    const ctx = { _agentSpawnCount: { value: 3 }, _maxAgentsPerSession: 3 };
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('3'));
  });

  it('allows spawn when custom cap is higher', () => {
    const ctx = { _agentSpawnCount: { value: 50 }, _maxAgentsPerSession: 100 };
    assert.equal(checkAgentCaps(ctx), null);
  });

  it('spawn count is cumulative — reported count includes past spawns', () => {
    const ctx = { _agentSpawnCount: { value: 49 }, _maxAgentsPerSession: 50 };
    assert.equal(checkAgentCaps(ctx), null);
    ctx._agentSpawnCount.value = 50;
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('50'));
  });
});

describe('checkAgentCaps — concurrent cap', () => {
  function makeProcessManager(agents: Array<{ status: string }>) {
    const processes = new Map<string, any>();
    agents.forEach((a, i) => {
      processes.set(`agent-${i}`, { command: `[agent] task ${i}`, status: a.status });
    });
    return { processes };
  }

  it('allows spawn when no process manager is present', () => {
    const ctx = { _agentSpawnCount: { value: 0 } };
    assert.equal(checkAgentCaps(ctx), null);
  });

  it('allows spawn when under concurrent cap', () => {
    const pm = makeProcessManager([
      { status: 'running' },
      { status: 'running' },
    ]);
    const ctx = { _agentSpawnCount: { value: 2 }, processManager: pm };
    assert.equal(checkAgentCaps(ctx), null);
  });

  it('blocks spawn when concurrent cap is reached (default 10)', () => {
    const running = Array.from({ length: 10 }, () => ({ status: 'running' }));
    const pm = makeProcessManager(running);
    const ctx = { _agentSpawnCount: { value: 10 }, processManager: pm };
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('Concurrent'));
    assert.ok(err!.includes('10'));
  });

  it('completed tasks do not count toward concurrent cap', () => {
    const agents = [
      { status: 'running' },
      { status: 'completed' },
      { status: 'running' },
      { status: 'failed' },
      { status: 'completed' },
    ];
    const pm = makeProcessManager(agents);
    const ctx = { _agentSpawnCount: { value: 5 }, processManager: pm };
    assert.equal(checkAgentCaps(ctx), null);
  });

  it('respects custom concurrent cap', () => {
    const running = Array.from({ length: 3 }, () => ({ status: 'running' }));
    const pm = makeProcessManager(running);
    const ctx = { _agentSpawnCount: { value: 3 }, processManager: pm, _maxConcurrentAgents: 3 };
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('3'));
  });

  it('non-agent processes do not count toward concurrent cap', () => {
    const processes = new Map<string, any>();
    processes.set('bg-1', { command: 'npm test', status: 'running' });
    processes.set('bg-2', { command: 'node server.js', status: 'running' });
    const pm = { processes };
    const ctx = { _agentSpawnCount: { value: 0 }, processManager: pm };
    assert.equal(checkAgentCaps(ctx), null);
  });
});

describe('checkAgentCaps — session cap checked before concurrent cap', () => {
  it('session cap error takes precedence', () => {
    const running = Array.from({ length: 10 }, () => ({ status: 'running' }));
    const pm = { processes: new Map(running.map((a, i) => [`agent-${i}`, { command: `[agent] task`, status: a.status }])) };
    const ctx = { _agentSpawnCount: { value: 50 }, processManager: pm };
    const err = checkAgentCaps(ctx);
    assert.ok(err !== null);
    assert.ok(err!.includes('session cap'));
  });
});
