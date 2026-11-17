/**
 * U045 / w85 parity — interactive per-host sandbox network-egress approval
 * (deny-by-default outbound with on-demand allow). Covers the confirmed gaps:
 *
 *  - Interactive per-host sandbox network egress approval (REPL.tsx SandboxAskCallback):
 *    requestSandboxHostAccess / createSandboxAskCallback route a non-allowlisted host
 *    through the permission-prompt handler and record a session grant on allow.
 *  - Interactive per-host network-access permission prompt (structuredIO.ts
 *    createSandboxAskCallback): the EXACT "Allow network connection to <host>?" wording.
 *  - Runtime per-host sandbox network-access approval honored by the HOST (unshare)
 *    backend: wrapWithUnshare folds getGrantedSandboxHosts() into the effective
 *    outbound allowlist and drops a granted host from the denylist (parity with the
 *    container backend's wrapWithDocker), so an on-demand grant is reachable on the
 *    host backend too — not only in container mode.
 *  - End-to-end wiring: web_fetch's egress gate (AMI_SANDBOX_NETWORK_ASK) asks before
 *    connecting and refuses a denied host (deny-by-default), sharing the same grant
 *    store so a later sandboxed command reaches an approved host.
 *
 * No real container/unshare is started — the produced command STRING is asserted.
 *
 * Run: cd core/core && node --import tsx --test test/bash-sandbox-u045-w85-network-ask.test.ts
 */
import { describe, it, beforeEach, before, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as http from 'http';

import {
  requestSandboxHostAccess,
  createSandboxAskCallback,
  sandboxNetworkAskMessage,
  isSandboxNetworkAskEnabled,
  SANDBOX_NETWORK_ASK_ENV,
  recordSandboxHostGrant,
  isSandboxHostGranted,
  getGrantedSandboxHosts,
  clearSandboxPermissionGrants,
} from '../src/tools/sandbox-permission-registry';
import {
  wrapWithUnshare,
  dedupeHostList,
  buildDeniedDomainRules,
  type SandboxConfig,
} from '../src/tools/bash-sandbox';
import { webFetchTool } from '../src/tools/web-fetch';
import type { PermissionPromptHandler, PermissionPromptResult } from '../src/permissions';
import type { ToolContext } from '../src/types';

/** A permission handler that records its prompts and returns a scripted verdict. */
function handlerReturning(
  action: PermissionPromptResult['action'] | (() => never),
): { handler: PermissionPromptHandler; prompts: string[] } {
  const prompts: string[] = [];
  const handler: PermissionPromptHandler = {
    async showPrompt(_tool, inputSummary) {
      prompts.push(inputSummary);
      if (typeof action === 'function') action();
      return { action: action as PermissionPromptResult['action'] };
    },
  };
  return { handler, prompts };
}

beforeEach(() => clearSandboxPermissionGrants());

// ---------------------------------------------------------------------------
// prompt wording + env gate (gap #3, structuredIO createSandboxAskCallback)
// ---------------------------------------------------------------------------
describe('sandboxNetworkAskMessage + env gate', () => {
  it('uses the exact Claude wording "Allow network connection to <host>?"', () => {
    assert.equal(sandboxNetworkAskMessage('example.com'), 'Allow network connection to example.com?');
    assert.equal(sandboxNetworkAskMessage('  trim.me  '), 'Allow network connection to trim.me?');
  });

  it('isSandboxNetworkAskEnabled honors truthy/falsy env values', () => {
    for (const v of ['1', 'true', 'YES', 'on', ' On ']) {
      assert.equal(isSandboxNetworkAskEnabled({ [SANDBOX_NETWORK_ASK_ENV]: v }), true, v);
    }
    for (const v of ['', '0', 'false', 'no', 'off', undefined as unknown as string]) {
      assert.equal(isSandboxNetworkAskEnabled({ [SANDBOX_NETWORK_ASK_ENV]: v }), false, String(v));
    }
  });
});

// ---------------------------------------------------------------------------
// requestSandboxHostAccess (gaps #1, #3)
// ---------------------------------------------------------------------------
describe('requestSandboxHostAccess', () => {
  it('returns false for an empty host (fail-closed) without prompting', async () => {
    const { handler, prompts } = handlerReturning('allow_once');
    assert.equal(await requestSandboxHostAccess('   ', handler), false);
    assert.deepEqual(prompts, []);
  });

  it('returns true for an already-granted host WITHOUT re-prompting', async () => {
    recordSandboxHostGrant('api.example.com');
    const { handler, prompts } = handlerReturning(() => { throw new Error('must not prompt'); });
    assert.equal(await requestSandboxHostAccess('API.example.com', handler), true);
    assert.deepEqual(prompts, []);
  });

  it('returns false (fail-closed) when no handler is available', async () => {
    assert.equal(await requestSandboxHostAccess('new.example.com', undefined), false);
    assert.equal(isSandboxHostGranted('new.example.com'), false);
  });

  it('prompts with the exact wording, records the grant, and returns true on allow', async () => {
    const { handler, prompts } = handlerReturning('allow_once');
    assert.equal(await requestSandboxHostAccess('grant.example.com', handler), true);
    assert.deepEqual(prompts, ['Allow network connection to grant.example.com?']);
    assert.equal(isSandboxHostGranted('grant.example.com'), true);
    assert.ok(getGrantedSandboxHosts().includes('grant.example.com'));
  });

  it('returns false and records NO grant on deny', async () => {
    const { handler } = handlerReturning('deny');
    assert.equal(await requestSandboxHostAccess('deny.example.com', handler), false);
    assert.equal(isSandboxHostGranted('deny.example.com'), false);
  });

  it('returns false and records NO grant on cancelled', async () => {
    const { handler } = handlerReturning('cancelled');
    assert.equal(await requestSandboxHostAccess('cancel.example.com', handler), false);
    assert.equal(isSandboxHostGranted('cancel.example.com'), false);
  });

  it('treats a throwing handler as a deny (fail-closed)', async () => {
    const { handler } = handlerReturning(() => { throw new Error('stream closed'); });
    assert.equal(await requestSandboxHostAccess('boom.example.com', handler), false);
    assert.equal(isSandboxHostGranted('boom.example.com'), false);
  });

  it('createSandboxAskCallback delegates to requestSandboxHostAccess', async () => {
    const { handler } = handlerReturning('allow_once');
    const cbAllow = createSandboxAskCallback(handler);
    assert.equal(await cbAllow({ host: 'cb.example.com', port: 443 }), true);
    assert.equal(isSandboxHostGranted('cb.example.com'), true);

    const { handler: denyH } = handlerReturning('deny');
    const cbDeny = createSandboxAskCallback(denyH);
    assert.equal(await cbDeny({ host: 'cb-deny.example.com' }), false);
    assert.equal(isSandboxHostGranted('cb-deny.example.com'), false);
  });
});

// ---------------------------------------------------------------------------
// host (unshare) backend grant folding (gap #5 host side)
// ---------------------------------------------------------------------------
function baseCfg(over: Partial<SandboxConfig> = {}): SandboxConfig {
  return {
    allowNetwork: false,
    readOnlyPaths: [],
    writablePaths: [],
    maxMemoryMB: 512,
    maxProcesses: 8192,
    timeout: 30000,
    ...over,
  };
}

describe('dedupeHostList', () => {
  it('trims, drops empties, de-dupes, and preserves first-seen order', () => {
    assert.deepEqual(dedupeHostList(['a.com', ' a.com ', '', '   ', 'b.com', 'a.com']), ['a.com', 'b.com']);
  });
});

describe('wrapWithUnshare folds granted hosts into the allowlist', () => {
  it('no grant + no allowedDomains → fully isolated (no iptables allowlist rules)', () => {
    const out = wrapWithUnshare('echo hi', '/tmp', baseCfg());
    assert.ok(!out.includes('iptables'), 'an un-granted, allowlist-less sandbox must stay fully isolated');
  });

  it('a granted host opens an on-demand allowlist on the host backend', () => {
    recordSandboxHostGrant('ondemand.example.com');
    const out = wrapWithUnshare('curl https://ondemand.example.com', '/tmp', baseCfg());
    assert.ok(out.includes("getent ahosts 'ondemand.example.com'"), 'granted host must be resolved into the allowlist');
    assert.ok(out.includes('-j ACCEPT'), 'granted host must be ACCEPTed');
    assert.ok(out.includes('iptables -A OUTPUT -j DROP'), 'allowlist mode still default-denies the rest');
  });

  it('a granted host WINS over the denylist (dropped from deny, added to allow)', () => {
    const cfg = baseCfg({ allowedDomains: ['good.com'], deniedDomains: ['evil.com'] });

    const ungranted = wrapWithUnshare('curl https://evil.com', '/tmp', cfg);
    assert.ok(
      ungranted.includes(buildDeniedDomainRules(['evil.com'])),
      'without a grant the denied host must still be DROPped',
    );

    clearSandboxPermissionGrants();
    recordSandboxHostGrant('evil.com');
    const granted = wrapWithUnshare('curl https://evil.com', '/tmp', cfg);
    assert.ok(
      !granted.includes(buildDeniedDomainRules(['evil.com'])),
      'a granted host must be removed from the denylist (deny rules empty for it)',
    );
    assert.ok(granted.includes("getent ahosts 'evil.com'"), 'the granted host must appear in the allowlist');
  });
});

// ---------------------------------------------------------------------------
// end-to-end wiring through web_fetch (gaps #1, #3 consumer)
// ---------------------------------------------------------------------------
describe('web_fetch per-host egress gate (AMI_SANDBOX_NETWORK_ASK)', () => {
  let server: http.Server;
  let port: number;

  before(async () => {
    server = http.createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('EGRESS_OK');
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
    port = (server.address() as { port: number }).port;
  });
  after(() => { server.close(); });

  function ctx(over?: Partial<ToolContext>): ToolContext {
    return {
      cwd: process.cwd(),
      abortSignal: new AbortController().signal,
      _allowLocalhostForTesting: true,
      ...over,
    } as ToolContext;
  }
  const url = () => `http://127.0.0.1:${port}/`;

  it('gate OFF → fetches without any prompt even with no handler', async () => {
    delete process.env[SANDBOX_NETWORK_ASK_ENV];
    const r = await webFetchTool.execute({ url: url() }, ctx());
    assert.ok(!r.isError, r.output);
    assert.ok(r.output.includes('EGRESS_OK'));
  });

  it('gate ON + no handler → refuses the connection (deny-by-default)', async () => {
    process.env[SANDBOX_NETWORK_ASK_ENV] = '1';
    try {
      const r = await webFetchTool.execute({ url: url() }, ctx());
      assert.equal(r.isError, true);
      assert.ok(r.output.includes('was not approved'), r.output);
    } finally {
      delete process.env[SANDBOX_NETWORK_ASK_ENV];
    }
  });

  it('gate ON + handler ALLOW → fetches and records the grant (shared with bash sandbox)', async () => {
    process.env[SANDBOX_NETWORK_ASK_ENV] = '1';
    try {
      const { handler, prompts } = handlerReturning('allow_once');
      const r = await webFetchTool.execute({ url: url() }, ctx({ _permissionPromptHandler: handler }));
      assert.ok(!r.isError, r.output);
      assert.ok(r.output.includes('EGRESS_OK'));
      assert.deepEqual(prompts, ['Allow network connection to 127.0.0.1?']);
      assert.equal(isSandboxHostGranted('127.0.0.1'), true);
    } finally {
      delete process.env[SANDBOX_NETWORK_ASK_ENV];
    }
  });

  it('gate ON + handler DENY → refuses the connection', async () => {
    process.env[SANDBOX_NETWORK_ASK_ENV] = '1';
    try {
      const { handler } = handlerReturning('deny');
      const r = await webFetchTool.execute({ url: url() }, ctx({ _permissionPromptHandler: handler }));
      assert.equal(r.isError, true);
      assert.ok(r.output.includes('was not approved'), r.output);
    } finally {
      delete process.env[SANDBOX_NETWORK_ASK_ENV];
    }
  });

  it('gate ON + host already granted → fetches without re-prompting', async () => {
    process.env[SANDBOX_NETWORK_ASK_ENV] = '1';
    try {
      recordSandboxHostGrant('127.0.0.1');
      const { handler, prompts } = handlerReturning(() => { throw new Error('must not prompt'); });
      const r = await webFetchTool.execute({ url: url() }, ctx({ _permissionPromptHandler: handler }));
      assert.ok(!r.isError, r.output);
      assert.ok(r.output.includes('EGRESS_OK'));
      assert.deepEqual(prompts, []);
    } finally {
      delete process.env[SANDBOX_NETWORK_ASK_ENV];
    }
  });
});
