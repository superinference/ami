import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as path from 'path';

import {
  detectRuntime,
  resolveRuntime,
  buildContainerArgs,
} from '../src/mcp/container';
import type { ContainerConfig } from '../src/mcp/container';

const SRC_PATH = path.resolve(__dirname, '..', 'src', 'mcp', 'container.ts');
const SRC = fs.readFileSync(SRC_PATH, 'utf-8');

// ---------------------------------------------------------------------------
// ContainerConfig interface
// ---------------------------------------------------------------------------

describe('ContainerConfig interface', () => {
  it('requires image field', () => {
    assert.ok(SRC.includes('image: string'));
  });

  it('has optional name field', () => {
    assert.ok(SRC.includes('name?: string'));
  });

  it('has optional runtime field with correct union type', () => {
    assert.ok(SRC.includes("runtime?: 'docker' | 'podman' | 'auto'"));
  });

  it('has optional extraArgs field', () => {
    assert.ok(SRC.includes('extraArgs?: string[]'));
  });

  it('is usable as a type (compile-time check)', () => {
    const cfg: ContainerConfig = { image: 'alpine:latest' };
    assert.equal(cfg.image, 'alpine:latest');
  });

  it('accepts all optional fields', () => {
    const cfg: ContainerConfig = {
      image: 'cyberillo/kali-mcp-server:latest',
      name: 'si-kali-mcp',
      runtime: 'auto',
      extraArgs: ['--network', 'host'],
    };
    assert.equal(cfg.name, 'si-kali-mcp');
    assert.equal(cfg.runtime, 'auto');
    assert.deepEqual(cfg.extraArgs, ['--network', 'host']);
  });
});

// ---------------------------------------------------------------------------
// detectRuntime
// ---------------------------------------------------------------------------

describe('detectRuntime', () => {
  it('exports detectRuntime as a function', () => {
    assert.equal(typeof detectRuntime, 'function');
  });

  it('checks podman before docker', () => {
    const loopMatch = SRC.match(/for\s*\(\s*const\s+\w+\s+of\s+\[([^\]]+)\]/);
    assert.ok(loopMatch, 'expected for-of loop over runtime candidates');
    const items = loopMatch![1];
    const podmanIdx = items.indexOf('podman');
    const dockerIdx = items.indexOf('docker');
    assert.ok(podmanIdx < dockerIdx, 'podman must be checked before docker');
  });

  it('uses "command -v" to probe for each runtime', () => {
    assert.ok(SRC.includes('command -v ${cmd}'));
  });

  it('returns null when no runtime is found (source check)', () => {
    const fnBody = SRC.slice(
      SRC.indexOf('function detectRuntime'),
      SRC.indexOf('\n}\n', SRC.indexOf('function detectRuntime')) + 3,
    );
    assert.ok(fnBody.includes('return null'));
  });

  it('returns a string when a runtime is available', () => {
    const result = detectRuntime();
    if (result !== null) {
      assert.equal(typeof result, 'string');
      assert.ok(result === 'docker' || result === 'podman');
    }
  });
});

// ---------------------------------------------------------------------------
// resolveRuntime
// ---------------------------------------------------------------------------

describe('resolveRuntime', () => {
  it('exports resolveRuntime as a function', () => {
    assert.equal(typeof resolveRuntime, 'function');
  });

  it('falls back to detectRuntime when preference is auto', () => {
    const fnBody = SRC.slice(
      SRC.indexOf('function resolveRuntime'),
      SRC.indexOf('\n}\n', SRC.indexOf('function resolveRuntime')) + 3,
    );
    assert.ok(fnBody.includes("preference !== 'auto'"));
    assert.ok(fnBody.includes('return detectRuntime()'));
  });

  it('falls back to detectRuntime when preference is undefined', () => {
    const result = resolveRuntime(undefined);
    const expected = detectRuntime();
    assert.equal(result, expected);
  });

  it('falls back to detectRuntime when preference is auto', () => {
    const result = resolveRuntime('auto');
    const expected = detectRuntime();
    assert.equal(result, expected);
  });

  it('tries only the preferred runtime when preference is specific', () => {
    const fnBody = SRC.slice(
      SRC.indexOf('function resolveRuntime'),
      SRC.indexOf('\n}\n', SRC.indexOf('function resolveRuntime')) + 3,
    );
    assert.ok(fnBody.includes('command -v ${preference}'));
  });

  it('returns null when the preferred runtime is not installed', () => {
    const fnBody = SRC.slice(
      SRC.indexOf('function resolveRuntime'),
      SRC.indexOf('\n}\n', SRC.indexOf('function resolveRuntime')) + 3,
    );
    assert.ok(fnBody.includes('return null'));
  });
});

// ---------------------------------------------------------------------------
// buildContainerArgs
// ---------------------------------------------------------------------------

describe('buildContainerArgs', () => {
  it('exports buildContainerArgs as a function', () => {
    assert.equal(typeof buildContainerArgs, 'function');
  });

  it('returns base args: run -i --rm <image>', () => {
    const args = buildContainerArgs({ image: 'alpine:latest' });
    assert.deepEqual(args, ['run', '-i', '--rm', 'alpine:latest']);
  });

  it('includes --name when name is specified', () => {
    const args = buildContainerArgs({ image: 'alpine', name: 'my-container' });
    assert.deepEqual(args, ['run', '-i', '--rm', '--name', 'my-container', 'alpine']);
  });

  it('includes extraArgs before image', () => {
    const args = buildContainerArgs({
      image: 'alpine',
      extraArgs: ['--network', 'host', '-e', 'FOO=bar'],
    });
    assert.deepEqual(args, ['run', '-i', '--rm', '--network', 'host', '-e', 'FOO=bar', 'alpine']);
  });

  it('includes both name and extraArgs in correct order', () => {
    const args = buildContainerArgs({
      image: 'cyberillo/kali-mcp-server:latest',
      name: 'si-kali-mcp',
      extraArgs: ['--network', 'host'],
    });
    assert.deepEqual(args, [
      'run', '-i', '--rm',
      '--name', 'si-kali-mcp',
      '--network', 'host',
      'cyberillo/kali-mcp-server:latest',
    ]);
  });

  it('handles empty extraArgs', () => {
    const args = buildContainerArgs({ image: 'alpine', extraArgs: [] });
    assert.deepEqual(args, ['run', '-i', '--rm', 'alpine']);
  });

  it('always starts with run -i --rm', () => {
    const args = buildContainerArgs({ image: 'any-image' });
    assert.equal(args[0], 'run');
    assert.equal(args[1], '-i');
    assert.equal(args[2], '--rm');
  });

  it('image is always the last argument', () => {
    const args = buildContainerArgs({
      image: 'test-image',
      name: 'test',
      extraArgs: ['--flag'],
    });
    assert.equal(args[args.length - 1], 'test-image');
  });

  it('does not add --name when name is undefined', () => {
    const args = buildContainerArgs({ image: 'alpine' });
    assert.ok(!args.includes('--name'));
  });

  it('produces correct args for cyberillo/kali-mcp-server config', () => {
    const args = buildContainerArgs({
      image: 'cyberillo/kali-mcp-server:latest',
      name: 'si-kali-mcp',
      runtime: 'auto',
    });
    assert.deepEqual(args, [
      'run', '-i', '--rm',
      '--name', 'si-kali-mcp',
      'cyberillo/kali-mcp-server:latest',
    ]);
  });
});
