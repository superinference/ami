import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { HookManager, EventConcurrencyLimiter, MAX_CONCURRENT_PER_EVENT, sanitizeHookEnv, SENSITIVE_ENV_PATTERNS, type HookContext } from '../src/hooks';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makeContext(overrides: Partial<HookContext> = {}): HookContext {
  return {
    messages: [{ role: 'user', content: 'hello' }],
    turnCount: 1,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// onPostSampling / executePostSampling
// ---------------------------------------------------------------------------
describe('HookManager – postSampling', () => {
  it('onPostSampling registers a hook', () => {
    const hm = new HookManager();
    let called = false;
    hm.onPostSampling(async () => { called = true; });
    // Not called until executed
    assert.equal(called, false);
  });

  it('executePostSampling calls registered hooks with context', async () => {
    const hm = new HookManager();
    let receivedCtx: HookContext | undefined;
    hm.onPostSampling(async (ctx) => { receivedCtx = ctx; });

    const ctx = makeContext({ turnCount: 7 });
    await hm.executePostSampling(ctx);

    assert.ok(receivedCtx);
    assert.equal(receivedCtx!.turnCount, 7);
    assert.deepEqual(receivedCtx!.messages, ctx.messages);
  });
});

// ---------------------------------------------------------------------------
// onStop / executeStop
// ---------------------------------------------------------------------------
describe('HookManager – stop', () => {
  it('onStop and executeStop work', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onStop(async () => { called = true; });
    await hm.executeStop(makeContext());
    assert.ok(called);
  });
});

// ---------------------------------------------------------------------------
// onError / executeError
// ---------------------------------------------------------------------------
describe('HookManager – error', () => {
  it('onError and executeError work', async () => {
    const hm = new HookManager();
    let called = false;
    hm.onError(async () => { called = true; });
    await hm.executeError(makeContext());
    assert.ok(called);
  });
});

// ---------------------------------------------------------------------------
// Multiple hooks execute in order
// ---------------------------------------------------------------------------
describe('HookManager – ordering', () => {
  it('multiple hooks execute in registration order', async () => {
    const hm = new HookManager();
    const order: number[] = [];
    hm.onPostSampling(async () => { order.push(1); });
    hm.onPostSampling(async () => { order.push(2); });
    hm.onPostSampling(async () => { order.push(3); });

    await hm.executePostSampling(makeContext());
    assert.deepEqual(order, [1, 2, 3]);
  });
});

// ---------------------------------------------------------------------------
// HookContext population
// ---------------------------------------------------------------------------
describe('HookManager – context', () => {
  it('hooks receive correct HookContext fields', async () => {
    const hm = new HookManager();
    let receivedCtx: HookContext | undefined;
    hm.onPostSampling(async (ctx) => { receivedCtx = ctx; });

    const ctx: HookContext = {
      messages: [
        { role: 'user', content: 'hi' },
        { role: 'assistant', content: 'hello' },
      ],
      lastAssistantMessage: { role: 'assistant', content: 'hello' },
      toolResults: [
        { toolName: 'bash', output: 'ok', isError: false },
      ],
      turnCount: 3,
    };

    await hm.executePostSampling(ctx);

    assert.ok(receivedCtx);
    assert.equal(receivedCtx!.messages.length, 2);
    assert.deepEqual(receivedCtx!.lastAssistantMessage, { role: 'assistant', content: 'hello' });
    assert.equal(receivedCtx!.toolResults!.length, 1);
    assert.equal(receivedCtx!.toolResults![0].toolName, 'bash');
    assert.equal(receivedCtx!.turnCount, 3);
  });
});

// ---------------------------------------------------------------------------
// Error handling – hooks are non-blocking
// ---------------------------------------------------------------------------
describe('HookManager – error handling', () => {
  it('hook errors are caught and swallowed (non-blocking)', async () => {
    const hm = new HookManager();
    const order: string[] = [];

    hm.onPostSampling(async () => { order.push('before'); });
    hm.onPostSampling(async () => { throw new Error('boom'); });
    hm.onPostSampling(async () => { order.push('after'); });

    // Should not throw
    await hm.executePostSampling(makeContext());
    assert.deepEqual(order, ['before', 'after']);
  });

  it('error hooks swallow errors too', async () => {
    const hm = new HookManager();
    hm.onError(async () => { throw new Error('double fault'); });
    // Should not throw
    await hm.executeError(makeContext());
  });

  it('stop hooks swallow errors too', async () => {
    const hm = new HookManager();
    hm.onStop(async () => { throw new Error('stop failure'); });
    // Should not throw
    await hm.executeStop(makeContext());
  });
});

// ---------------------------------------------------------------------------
// Async hooks
// ---------------------------------------------------------------------------
describe('HookManager – async hooks', () => {
  it('async hooks work and are awaited', async () => {
    const hm = new HookManager();
    let value = 0;
    hm.onPostSampling(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 10));
      value = 42;
    });
    await hm.executePostSampling(makeContext());
    assert.equal(value, 42);
  });

  it('async hooks execute sequentially', async () => {
    const hm = new HookManager();
    const order: number[] = [];
    hm.onPostSampling(async () => {
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      order.push(1);
    });
    hm.onPostSampling(async () => {
      order.push(2);
    });
    await hm.executePostSampling(makeContext());
    assert.deepEqual(order, [1, 2]);
  });
});

// ---------------------------------------------------------------------------
// Empty hook manager
// ---------------------------------------------------------------------------
describe('HookManager – empty', () => {
  it('empty hook manager executes without error', async () => {
    const hm = new HookManager();
    await hm.executePostSampling(makeContext());
    await hm.executeStop(makeContext());
    await hm.executeError(makeContext());
  });
});

// ---------------------------------------------------------------------------
// EventConcurrencyLimiter
// ---------------------------------------------------------------------------
describe('EventConcurrencyLimiter', () => {
  it('exports MAX_CONCURRENT_PER_EVENT = 12', () => {
    assert.equal(MAX_CONCURRENT_PER_EVENT, 12);
  });

  it('allows up to max concurrent acquires', async () => {
    const limiter = new EventConcurrencyLimiter(3);
    await limiter.acquire('test');
    await limiter.acquire('test');
    await limiter.acquire('test');
    assert.equal(limiter.getActive('test'), 3);
    limiter.release('test');
    assert.equal(limiter.getActive('test'), 2);
    limiter.release('test');
    limiter.release('test');
    assert.equal(limiter.getActive('test'), 0);
  });

  it('queues acquires beyond the limit', async () => {
    const limiter = new EventConcurrencyLimiter(2);
    await limiter.acquire('ev');
    await limiter.acquire('ev');
    assert.equal(limiter.getActive('ev'), 2);

    let thirdResolved = false;
    const thirdPromise = limiter.acquire('ev').then(() => { thirdResolved = true; });
    assert.equal(limiter.getWaiting('ev'), 1);
    assert.equal(thirdResolved, false);

    limiter.release('ev');
    await thirdPromise;
    assert.equal(thirdResolved, true);
    assert.equal(limiter.getActive('ev'), 2);
    assert.equal(limiter.getWaiting('ev'), 0);

    limiter.release('ev');
    limiter.release('ev');
  });

  it('tracks events independently', async () => {
    const limiter = new EventConcurrencyLimiter(1);
    await limiter.acquire('a');
    await limiter.acquire('b');
    assert.equal(limiter.getActive('a'), 1);
    assert.equal(limiter.getActive('b'), 1);
    limiter.release('a');
    limiter.release('b');
  });

  it('hooks execute through concurrency gate', async () => {
    const hm = new HookManager();
    const log: string[] = [];
    hm.onPostSampling(async () => { log.push('hook1'); });
    hm.onPostSampling(async () => { log.push('hook2'); });
    await hm.executePostSampling(makeContext());
    assert.deepEqual(log, ['hook1', 'hook2']);
  });
});

// ---------------------------------------------------------------------------
// Hook args exec form + effort env
// ---------------------------------------------------------------------------
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

describe('HookManager — args exec form', () => {
  let tmpDir: string;
  const cleanup = () => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} };

  it('preToolUse hook with args spawns without shell', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-args-'));
    const hooksDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(hooksDir, { recursive: true });
    fs.writeFileSync(path.join(hooksDir, 'hooks.json'), JSON.stringify({
      hooks: [{
        event: 'preToolUse',
        hook: {
          type: 'command',
          command: 'echo',
          args: ['echo', 'exec-form-worked'],
          timeout: 5000,
        },
      }],
    }));

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.loadFromFile(tmpDir);

    const result = await hm.executePreToolUse({
      messages: [],
      turnCount: 0,
      toolName: 'bash',
      toolInput: { command: 'ls' },
    });
    assert.equal(result.action, 'allow');
    cleanup();
  });
});

describe('HookManager — SUPERINFERENCE_EFFORT in hook env', () => {
  let tmpDir: string;
  const cleanup = () => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} };

  it('hook receives SUPERINFERENCE_EFFORT env var', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-effort-'));
    const hooksDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(hooksDir, { recursive: true });

    const markerFile = path.join(tmpDir, 'effort.txt');
    fs.writeFileSync(path.join(hooksDir, 'hooks.json'), JSON.stringify({
      hooks: [{
        event: 'preToolUse',
        hook: {
          type: 'command',
          command: `echo "$SUPERINFERENCE_EFFORT" > ${markerFile}`,
          timeout: 5000,
        },
      }],
    }));

    const hm = new HookManager();
    hm.setWorkspaceTrusted(true);
    hm.setEffortLevel('max');
    hm.loadFromFile(tmpDir);

    await hm.executePreToolUse({
      messages: [],
      turnCount: 0,
      toolName: 'bash',
      toolInput: { command: 'ls' },
    });

    const written = fs.readFileSync(markerFile, 'utf-8').trim();
    assert.equal(written, 'max');
    cleanup();
  });
});

// ---------------------------------------------------------------------------
// hook env sanitization — buildHookEnv strips sensitive vars via sanitizeHookEnv
// ---------------------------------------------------------------------------
describe('hook env sanitization', () => {
  let tmpDir: string;
  const cleanup = () => { try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {} };

  it('hook api key stripping — ANTHROPIC_API_KEY and OPENAI_API_KEY not passed to subprocess', async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'hooks-sanitize-'));
    const hooksDir = path.join(tmpDir, '.superinference');
    fs.mkdirSync(hooksDir, { recursive: true });

    const markerFile = path.join(tmpDir, 'env-dump.txt');
    fs.writeFileSync(path.join(hooksDir, 'hooks.json'), JSON.stringify({
      hooks: [{
        event: 'preToolUse',
        hook: {
          type: 'command',
          command: `env > ${markerFile}`,
          timeout: 5000,
        },
      }],
    }));

    // Inject sensitive keys into process.env temporarily
    const origAnthropic = process.env.ANTHROPIC_API_KEY;
    const origOpenai = process.env.OPENAI_API_KEY;
    process.env.ANTHROPIC_API_KEY = 'sk-ant-test-should-not-leak';
    process.env.OPENAI_API_KEY = 'sk-test-should-not-leak';

    try {
      const hm = new HookManager();
      hm.setWorkspaceTrusted(true);
      hm.loadFromFile(tmpDir);

      await hm.executePreToolUse({
        messages: [],
        turnCount: 0,
        toolName: 'bash',
        toolInput: { command: 'ls' },
      });

      const envContent = fs.readFileSync(markerFile, 'utf-8');
      assert.ok(!envContent.includes('sk-ant-test-should-not-leak'),
        'ANTHROPIC_API_KEY value must not appear in hook subprocess env');
      assert.ok(!envContent.includes('sk-test-should-not-leak'),
        'OPENAI_API_KEY value must not appear in hook subprocess env');
      // SUPERINFERENCE_* vars must still be present (set after sanitization)
      assert.ok(envContent.includes('SUPERINFERENCE_HOOK_EVENT='),
        'SUPERINFERENCE_HOOK_EVENT must be present in hook env');
      assert.ok(envContent.includes('SUPERINFERENCE_CWD='),
        'SUPERINFERENCE_CWD must be present in hook env');
    } finally {
      if (origAnthropic !== undefined) process.env.ANTHROPIC_API_KEY = origAnthropic;
      else delete process.env.ANTHROPIC_API_KEY;
      if (origOpenai !== undefined) process.env.OPENAI_API_KEY = origOpenai;
      else delete process.env.OPENAI_API_KEY;
      cleanup();
    }
  });
});

// ---------------------------------------------------------------------------
// sanitizeHookEnv — unit tests for the env sanitization function
// ---------------------------------------------------------------------------
describe('sanitizeHookEnv', () => {
  it('strips ANTHROPIC_API_KEY from hook environment', () => {
    const env: Record<string, string> = {
      ANTHROPIC_API_KEY: 'sk-ant-test-key',
      SAFE_VAR: 'safe-value',
      PATH: '/usr/bin',
    };
    const result = sanitizeHookEnv(env);
    assert.equal(result.ANTHROPIC_API_KEY, undefined, 'ANTHROPIC_API_KEY must be stripped');
    assert.equal(result.SAFE_VAR, 'safe-value');
    assert.equal(result.PATH, '/usr/bin');
  });

  it('strips OPENAI_API_KEY', () => {
    const env: Record<string, string> = { OPENAI_API_KEY: 'sk-test', KEEP: '1' };
    const result = sanitizeHookEnv(env);
    assert.equal(result.OPENAI_API_KEY, undefined);
    assert.equal(result.KEEP, '1');
  });

  it('strips all provider-specific API keys and tokens', () => {
    const env: Record<string, string> = {
      GOOGLE_API_KEY: 'gkey',
      AZURE_API_KEY: 'akey',
      AWS_SECRET_ACCESS_KEY: 'awskey',
      GITHUB_TOKEN: 'ghtoken',
      GH_TOKEN: 'ghtoken2',
      NPM_TOKEN: 'npmtoken',
      REPLICATE_API_TOKEN: 'rtoken',
      HF_TOKEN: 'hftoken',
      HUGGING_FACE_HUB_TOKEN: 'hfhtoken',
      SAFE_VARIABLE: 'safe',
    };
    const result = sanitizeHookEnv(env);
    for (const key of Object.keys(env)) {
      if (key === 'SAFE_VARIABLE') {
        assert.equal(result[key], 'safe');
      } else {
        assert.equal(result[key], undefined, `${key} must be stripped`);
      }
    }
  });

  it('strips keys matching suffix patterns (_SECRET, _PASSWORD, _TOKEN, _CREDENTIALS)', () => {
    const env: Record<string, string> = {
      MY_SECRET: 'sec',
      DB_PASSWORD: 'pass',
      CUSTOM_TOKEN: 'tok',
      APP_CREDENTIALS: 'creds',
      CUSTOM_API_KEY: 'apikey',
      MY_PRIVATE_KEY: 'privkey',
      CUSTOM_SECRET_VALUE: 'secval',
      NORMAL: 'val',
    };
    const result = sanitizeHookEnv(env);
    assert.equal(result.MY_SECRET, undefined, 'MY_SECRET must be stripped');
    assert.equal(result.DB_PASSWORD, undefined, 'DB_PASSWORD must be stripped');
    assert.equal(result.CUSTOM_TOKEN, undefined, 'CUSTOM_TOKEN must be stripped');
    assert.equal(result.APP_CREDENTIALS, undefined, 'APP_CREDENTIALS must be stripped');
    assert.equal(result.CUSTOM_API_KEY, undefined, 'CUSTOM_API_KEY must be stripped');
    assert.equal(result.MY_PRIVATE_KEY, undefined, 'MY_PRIVATE_KEY must be stripped');
    assert.equal(result.CUSTOM_SECRET_VALUE, undefined, 'CUSTOM_SECRET_VALUE must be stripped');
    assert.equal(result.NORMAL, 'val');
  });

  it('preserves SUPERINFERENCE_ prefixed variables', () => {
    const env: Record<string, string> = {
      SUPERINFERENCE_PROJECT_DIR: '/tmp/project',
      SUPERINFERENCE_SESSION_ID: 'sess-123',
      SUPERINFERENCE_EFFORT: 'high',
    };
    const result = sanitizeHookEnv(env);
    assert.equal(result.SUPERINFERENCE_PROJECT_DIR, '/tmp/project');
    assert.equal(result.SUPERINFERENCE_SESSION_ID, 'sess-123');
    assert.equal(result.SUPERINFERENCE_EFFORT, 'high');
  });

  it('preserves PATH, HOME, SHELL, TERM, and other safe environment vars', () => {
    const env: Record<string, string> = {
      PATH: '/usr/bin:/usr/local/bin',
      HOME: '/home/testuser',
      SHELL: '/bin/bash',
      TERM: 'xterm-256color',
      EDITOR: 'vim',
      LANG: 'en_US.UTF-8',
    };
    const result = sanitizeHookEnv(env);
    assert.equal(result.PATH, '/usr/bin:/usr/local/bin');
    assert.equal(result.HOME, '/home/testuser');
    assert.equal(result.SHELL, '/bin/bash');
    assert.equal(result.TERM, 'xterm-256color');
    assert.equal(result.EDITOR, 'vim');
    assert.equal(result.LANG, 'en_US.UTF-8');
  });

  it('handles undefined values in env', () => {
    const env = { KEEP: 'yes', UNDEF: undefined } as Record<string, string | undefined>;
    const result = sanitizeHookEnv(env);
    assert.equal(result.KEEP, 'yes');
    assert.equal(result.UNDEF, undefined);
  });

  it('is case-insensitive for known keys', () => {
    const env: Record<string, string> = { anthropic_api_key: 'low', Openai_Api_Key: 'mixed' };
    const result = sanitizeHookEnv(env);
    assert.equal(result.anthropic_api_key, undefined);
    assert.equal(result.Openai_Api_Key, undefined);
  });

  it('SENSITIVE_ENV_PATTERNS is a read-only array of RegExp', () => {
    assert.ok(Array.isArray(SENSITIVE_ENV_PATTERNS));
    assert.ok(SENSITIVE_ENV_PATTERNS.length > 0);
    for (const p of SENSITIVE_ENV_PATTERNS) {
      assert.ok(p instanceof RegExp);
    }
  });
});
