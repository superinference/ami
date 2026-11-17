import { describe, it, afterEach, mock } from 'node:test';
import * as assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { grepTool, grepHooks } from '../src/tools/grep';
import { globTool, globHooks, GLOB_RIPGREP_ENV } from '../src/tools/glob';
import type { ToolContext, SandboxPolicy } from '../src/types';

// ---------------------------------------------------------------------------
// U006 — sandbox.ripgrep { command, args } config override, wired end-to-end
// through the grep and glob search tools. A config-level override must win over
// the USE_BUILTIN_RIPGREP / AMI_RIPGREP_PATH env waterfall: the search tools must
// spawn the override's `command` and PREPEND its extra `args`.
// ---------------------------------------------------------------------------

function ctx(sandbox?: SandboxPolicy): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
    ...(sandbox ? { sandbox } : {}),
  };
}

/** A fake spawned grep/rg child that emits an immediate clean close with no output. */
function makeChild() {
  const child: any = new EventEmitter();
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = () => true;
  setImmediate(() => {
    child.stdout.emit('end');
    child.stderr.emit('end');
    child.emit('close', 0);
  });
  return child;
}

afterEach(() => {
  mock.restoreAll();
});

describe('grep honors sandbox.ripgrep config override', () => {
  it('spawns the override command and prepends override args over the env path', async () => {
    const prev = process.env.AMI_RIPGREP_PATH;
    process.env.AMI_RIPGREP_PATH = '/env/path/rg';
    let spawned: { command: string; args: string[] } | undefined;
    mock.method(grepHooks, 'spawn', ((command: string, args: string[]) => {
      spawned = { command, args };
      return makeChild() as never;
    }) as never);
    try {
      await grepTool.execute(
        { pattern: 'needle' },
        ctx({ mode: 'off', ripgrep: { command: '/custom/rg', args: ['--mmap', '--threads=2'] } }),
      );
    } finally {
      if (prev === undefined) delete process.env.AMI_RIPGREP_PATH;
      else process.env.AMI_RIPGREP_PATH = prev;
    }
    assert.ok(spawned, 'grep must spawn a search process');
    assert.equal(spawned!.command, '/custom/rg');
    // Override args are prepended ahead of the generated ripgrep flags.
    assert.equal(spawned!.args[0], '--mmap');
    assert.equal(spawned!.args[1], '--threads=2');
  });

  it('falls back to the env waterfall when no override is set', async () => {
    const prev = process.env.AMI_RIPGREP_PATH;
    process.env.AMI_RIPGREP_PATH = '/env/path/rg';
    let command: string | undefined;
    mock.method(grepHooks, 'spawn', ((cmd: string) => {
      command = cmd;
      return makeChild() as never;
    }) as never);
    try {
      await grepTool.execute({ pattern: 'needle' }, ctx({ mode: 'off' }));
    } finally {
      if (prev === undefined) delete process.env.AMI_RIPGREP_PATH;
      else process.env.AMI_RIPGREP_PATH = prev;
    }
    assert.equal(command, '/env/path/rg');
  });
});

describe('glob honors sandbox.ripgrep config override', () => {
  it('runs the override command with override args prepended to `--files`', async () => {
    const prev = process.env[GLOB_RIPGREP_ENV];
    process.env[GLOB_RIPGREP_ENV] = 'true';
    let spawned: { command: string; args: string[] } | undefined;
    mock.method(globHooks, 'execFileImpl', ((command: string, args: string[], _opts: unknown, cb: (e: unknown, out: string, err: string) => void) => {
      spawned = { command, args };
      setImmediate(() => cb(null, 'a.ts\nb.ts\n', ''));
      return { kill: () => true } as never;
    }) as never);
    try {
      await globTool.execute(
        { pattern: '**/*.ts' },
        ctx({ mode: 'off', ripgrep: { command: '/custom/rg', args: ['--no-mmap'] } }),
      );
    } finally {
      if (prev === undefined) delete process.env[GLOB_RIPGREP_ENV];
      else process.env[GLOB_RIPGREP_ENV] = prev;
    }
    assert.ok(spawned, 'glob must spawn ripgrep when the G5 route is enabled');
    assert.equal(spawned!.command, '/custom/rg');
    assert.equal(spawned!.args[0], '--no-mmap');
    assert.ok(spawned!.args.includes('--files'));
  });
});
