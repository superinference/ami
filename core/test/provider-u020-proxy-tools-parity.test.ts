// Unit U020 — additive provider.ts parity gaps:
//  - NO_PROXY `*.domain` glob form matching in resolveProxyForUrl/hostMatchesNoProxy
//  - proxy env-var precedence: lowercase preferred over uppercase (curl/the reference parity)
//  - identity-keyed tool-schema cache so convertToolsForSDK converts each tool once
import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';

import { resolveProxyForUrl, convertToolsForSDK } from '../src/provider';
import type { ToolDefinition } from '../src/types';

describe('resolveProxyForUrl — NO_PROXY `*.domain` glob form', () => {
  it('bypasses the proxy for a subdomain matching a `*.domain` entry', () => {
    const env = { HTTPS_PROXY: 'http://p', NO_PROXY: '*.example.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.example.com/v1'), undefined);
  });

  it('bypasses the proxy for the apex host of a `*.domain` entry (same as a leading dot)', () => {
    const env = { HTTPS_PROXY: 'http://p', NO_PROXY: '*.example.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://example.com/v1'), undefined);
  });

  it('still proxies a host that only partially resembles a `*.domain` entry', () => {
    const env = { HTTPS_PROXY: 'http://p', NO_PROXY: '*.example.com' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://notexample.com/v1'), 'http://p');
  });

  it('a bare `*.` entry matches nothing (no accidental bypass-all)', () => {
    const env = { HTTPS_PROXY: 'http://p', NO_PROXY: '*.' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.example.com/v1'), 'http://p');
  });
});

describe('resolveProxyForUrl — lowercase env precedence (curl/the reference parity)', () => {
  it('prefers https_proxy over HTTPS_PROXY for an https target', () => {
    const env = { https_proxy: 'http://low:1', HTTPS_PROXY: 'http://up:2' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.anthropic.com/v1'), 'http://low:1');
  });

  it('prefers http_proxy over HTTP_PROXY for an http target', () => {
    const env = { http_proxy: 'http://low:1', HTTP_PROXY: 'http://up:2' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'http://internal.host/v1'), 'http://low:1');
  });

  it('prefers no_proxy over NO_PROXY when deciding a bypass', () => {
    // lowercase no_proxy bypasses the host; uppercase NO_PROXY would NOT.
    const env = {
      HTTPS_PROXY: 'http://p',
      no_proxy: 'api.anthropic.com',
      NO_PROXY: 'unrelated.host',
    } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.anthropic.com/v1'), undefined);
  });

  it('falls back to the uppercase variant when the lowercase one is unset', () => {
    const env = { HTTPS_PROXY: 'http://up:2' } as NodeJS.ProcessEnv;
    assert.equal(resolveProxyForUrl(env, 'https://api.anthropic.com/v1'), 'http://up:2');
  });
});

describe('convertToolsForSDK — identity-keyed schema cache', () => {
  function mkTool(name: string): ToolDefinition {
    return {
      name,
      description: `desc ${name}`,
      inputSchema: { type: 'object', properties: { x: { type: 'string' } } } as ToolDefinition['inputSchema'],
      isReadOnly: true,
    };
  }

  it('reuses the exact same converted entry for a repeated tool identity', () => {
    const tool = mkTool('grep');
    const first = convertToolsForSDK([tool]);
    const second = convertToolsForSDK([tool]);
    // Same tool object -> schema converted once, same immutable entry reused.
    assert.strictEqual((first as any).grep.inputSchema, (second as any).grep.inputSchema);
    assert.strictEqual((first as any).grep, (second as any).grep);
  });

  it('converts distinct tool objects independently (no cross-contamination)', () => {
    const a = mkTool('a');
    const b = mkTool('b');
    const set = convertToolsForSDK([a, b]);
    assert.notStrictEqual((set as any).a.inputSchema, (set as any).b.inputSchema);
    assert.equal((set as any).a.description, 'desc a');
    assert.equal((set as any).b.description, 'desc b');
  });

  it('still produces one SDK entry per tool name with a defined inputSchema', () => {
    const set = convertToolsForSDK([mkTool('one'), mkTool('two')]);
    assert.deepEqual(Object.keys(set).sort(), ['one', 'two']);
    assert.ok((set as any).one.inputSchema);
  });
});
