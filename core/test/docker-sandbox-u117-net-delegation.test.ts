/**
 * U117 parity — per-host sandbox network-egress permission DELEGATION + the
 * `/add-dir` → sandbox mount sync. Covers three confirmed gaps:
 *
 *  1. add-dir mount sync (add-dir.tsx:L86-94): PermissionManager.getEffectiveSandboxPolicy
 *     folds the live allowed read roots into filesystem.allowWrite so a runtime-widened
 *     directory is actually mounted into the bash container, not just permission-allowed.
 *  2. Separate sandbox (network-host) permission-delegation registry with allow/deny
 *     (useSwarmPermissionPoller: registry + combined clear on /clear): a session-scoped
 *     grant store, processSandboxPermissionResponse (allow records a grant / deny does
 *     not), and clearSandboxPermissionGrants.
 *  3. Runtime per-host escalation (inProcessTeammateHelpers:L97-102 + teammateMailbox
 *     sandbox_permission_request/response): the control-message protocol
 *     (parse/summarize/relay), blocked-host detection (extractBlockedHost + the egress
 *     note), and the effect — wrapWithDocker stops blackholing a granted deny-host.
 *
 * No real container is started — the host command STRING produced by wrapWithDocker
 * is asserted directly.
 *
 * Run: cd core/core && node --import tsx --test test/docker-sandbox-u117-net-delegation.test.ts
 */
import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  wrapWithDocker,
  annotateStderrWithSandboxFailures,
  extractBlockedHost,
} from '../src/tools/docker-sandbox';
import {
  recordSandboxHostGrant,
  isSandboxHostGranted,
  getGrantedSandboxHosts,
  processSandboxPermissionResponse,
  clearSandboxPermissionGrants,
} from '../src/tools/sandbox-permission-registry';
import {
  parseStructuredMessage,
  summarizeStructuredMessage,
  relayIncomingMessage,
} from '../src/tools/send-message';
import { PermissionManager } from '../src/permissions';
import type { SandboxPolicy } from '../src/types';

function tmpProject(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'u117-deleg-'));
}

// ---------------------------------------------------------------------------
// Gap 1 — /add-dir → sandbox mount sync (getEffectiveSandboxPolicy)
// ---------------------------------------------------------------------------
describe('U117 gap1 — getEffectiveSandboxPolicy folds allowed read roots into allowWrite', () => {
  it('returns the base policy unchanged when no extra roots are granted', () => {
    const pm = new PermissionManager();
    const base: SandboxPolicy = { mode: 'container', projectDir: '/proj' };
    assert.equal(pm.getEffectiveSandboxPolicy(base), base);
  });

  it('returns undefined unchanged (host bash — no container to widen)', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('/extra/dir');
    assert.equal(pm.getEffectiveSandboxPolicy(undefined), undefined);
  });

  it('folds each granted root into filesystem.allowWrite, preserving existing entries', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('/data/added-one');
    pm.registerAllowedRoot('/data/added-two');
    const base: SandboxPolicy = {
      mode: 'container',
      projectDir: '/proj',
      filesystem: { allowWrite: ['/pre/existing'] },
    };
    const eff = pm.getEffectiveSandboxPolicy(base)!;
    assert.deepEqual(eff.filesystem?.allowWrite, [
      '/pre/existing',
      path.normalize('/data/added-one'),
      path.normalize('/data/added-two'),
    ]);
    // Base is not mutated.
    assert.deepEqual(base.filesystem?.allowWrite, ['/pre/existing']);
  });

  it('de-dupes a root already present in allowWrite', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('/data/dir');
    const base: SandboxPolicy = {
      mode: 'container',
      projectDir: '/proj',
      filesystem: { allowWrite: [path.normalize('/data/dir')] },
    };
    const eff = pm.getEffectiveSandboxPolicy(base)!;
    assert.deepEqual(eff.filesystem?.allowWrite, [path.normalize('/data/dir')]);
  });

  it('removeAllowedRoot narrows the projected mount set back', () => {
    const pm = new PermissionManager();
    pm.registerAllowedRoot('/data/dir');
    pm.removeAllowedRoot('/data/dir');
    const base: SandboxPolicy = { mode: 'container', projectDir: '/proj' };
    assert.equal(pm.getEffectiveSandboxPolicy(base), base);
  });

  it('END-TO-END: an added root reaches the container as a writable -v mount', () => {
    const project = tmpProject();
    const extra = tmpProject();
    try {
      const pm = new PermissionManager();
      pm.registerAllowedRoot(extra);
      const eff = pm.getEffectiveSandboxPolicy({ mode: 'container', projectDir: project })!;
      const wrapped = wrapWithDocker('ls', project, {
        image: 'img',
        projectDir: project,
        allowNetwork: false,
        allowWritePaths: eff.filesystem?.allowWrite,
      });
      // The widened dir is bound writable (no :ro suffix) inside the container.
      assert.match(wrapped, new RegExp(`-v '${extra}:${extra}'`));
    } finally {
      fs.rmSync(project, { recursive: true, force: true });
      fs.rmSync(extra, { recursive: true, force: true });
    }
  });
});

// ---------------------------------------------------------------------------
// Gap 2 — sandbox-host permission-delegation registry + combined clear
// ---------------------------------------------------------------------------
describe('U117 gap2 — sandbox-host grant registry', () => {
  beforeEach(() => clearSandboxPermissionGrants());

  it('records and queries a grant (case/space-insensitive)', () => {
    assert.equal(isSandboxHostGranted('api.example.com'), false);
    recordSandboxHostGrant('  API.Example.COM ');
    assert.equal(isSandboxHostGranted('api.example.com'), true);
    assert.deepEqual(getGrantedSandboxHosts(), ['api.example.com']);
  });

  it('ignores empty host input', () => {
    recordSandboxHostGrant('   ');
    assert.deepEqual(getGrantedSandboxHosts(), []);
  });

  it('processSandboxPermissionResponse records a grant on allow, nothing on deny', () => {
    assert.equal(processSandboxPermissionResponse({ requestId: 'r1', host: 'allow.me', allow: true }), true);
    assert.equal(isSandboxHostGranted('allow.me'), true);
    assert.equal(processSandboxPermissionResponse({ requestId: 'r2', host: 'deny.me', allow: false }), false);
    assert.equal(isSandboxHostGranted('deny.me'), false);
  });

  it('clearSandboxPermissionGrants drops all grants (combined /clear parity)', () => {
    recordSandboxHostGrant('a.com');
    recordSandboxHostGrant('b.com');
    clearSandboxPermissionGrants();
    assert.deepEqual(getGrantedSandboxHosts(), []);
  });
});

// ---------------------------------------------------------------------------
// Gap 3 — control-message protocol + blocked-host detection + grant effect
// ---------------------------------------------------------------------------
describe('U117 gap3 — sandbox_permission protocol', () => {
  it('parses a sandbox_permission_request (host required, requestId optional)', () => {
    assert.deepEqual(
      parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_request', host: 'x.io' })),
      { type: 'sandbox_permission_request', requestId: undefined, host: 'x.io' },
    );
    assert.deepEqual(
      parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_request' })),
      { error: 'sandbox_permission_request requires a host' },
    );
  });

  it('parses a sandbox_permission_response (requestId + host required; allow coerced)', () => {
    assert.deepEqual(
      parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', host: 'x.io', allow: 'true' })),
      { type: 'sandbox_permission_response', requestId: 'r', host: 'x.io', allow: true },
    );
    // Claude-compat `approved` field is accepted.
    assert.deepEqual(
      parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', host: 'x.io', approved: true })),
      { type: 'sandbox_permission_response', requestId: 'r', host: 'x.io', allow: true },
    );
    assert.deepEqual(
      parseStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', host: 'x.io', allow: true })),
      { error: 'sandbox_permission_response requires a requestId' },
    );
  });

  it('summarizes both message directions concisely (no raw JSON)', () => {
    assert.equal(
      summarizeStructuredMessage(JSON.stringify({ type: 'sandbox_permission_request', host: 'x.io' }), 'worker'),
      '[Sandbox Egress Request] from worker — approve outbound network access to host "x.io"?',
    );
    assert.equal(
      summarizeStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', host: 'x.io', allow: true })),
      '[Sandbox Egress Granted] host "x.io"',
    );
    assert.equal(
      summarizeStructuredMessage(JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', host: 'x.io', allow: false })),
      '[Sandbox Egress Denied] host "x.io"',
    );
  });

  it('relayIncomingMessage: a worker poll APPLIES a response (records grant) and DROPS it', () => {
    clearSandboxPermissionGrants();
    const relay = relayIncomingMessage(
      JSON.stringify({ type: 'sandbox_permission_response', requestId: 'r', host: 'granted.io', allow: true }),
      'main',
    );
    assert.equal(relay.drop, true);
    assert.equal(isSandboxHostGranted('granted.io'), true);
  });

  it('relayIncomingMessage: a leader sees the request as a summary (not dropped, not raw)', () => {
    const relay = relayIncomingMessage(
      JSON.stringify({ type: 'sandbox_permission_request', host: 'need.io' }),
      'worker-1',
    );
    assert.equal(relay.drop, false);
    assert.match(relay.text ?? '', /Sandbox Egress Request.*need\.io/);
  });

  it('extractBlockedHost pulls the host from stderr, else from a command URL, else null', () => {
    assert.equal(extractBlockedHost('curl https://api.example.com/x', 'curl: (6) Could not resolve host: api.example.com'), 'api.example.com');
    assert.equal(extractBlockedHost('wget http://pkgs.internal/p', 'failed to connect to pkgs.internal port 80'), 'pkgs.internal');
    assert.equal(extractBlockedHost('curl https://fallback.host/z', 'network is unreachable'), 'fallback.host');
    assert.equal(extractBlockedHost('echo hi', 'network is unreachable'), null);
    // Loopback / host-gateway are never surfaced as an escalation candidate.
    assert.equal(extractBlockedHost('curl http://localhost:3000', 'failed to connect to localhost port 3000'), null);
  });

  it('annotate names the blocked host + escalation affordance and records it as a violation', () => {
    const project = tmpProject();
    try {
      const out = annotateStderrWithSandboxFailures(
        'curl https://api.example.com/v1',
        'curl: (6) Could not resolve host: api.example.com',
        { allowNetwork: false, projectDir: project },
      );
      assert.match(out, /host "api\.example\.com"/);
      assert.match(out, /sandbox_permission_request/);
    } finally {
      fs.rmSync(project, { recursive: true, force: true });
    }
  });

  it('GRANT EFFECT: wrapWithDocker stops blackholing a host once it is granted', () => {
    const project = tmpProject();
    try {
      clearSandboxPermissionGrants();
      const cfg = {
        image: 'img',
        projectDir: project,
        allowNetwork: true,
        deniedHosts: ['blocked.example.com'],
      };
      // Before a grant: the denied host is blackholed via --add-host h:0.0.0.0.
      const before = wrapWithDocker('curl https://blocked.example.com', project, cfg);
      assert.match(before, /--add-host 'blocked\.example\.com:0\.0\.0\.0'/);
      // After the leader grants egress: the host is no longer blackholed.
      recordSandboxHostGrant('blocked.example.com');
      const after = wrapWithDocker('curl https://blocked.example.com', project, cfg);
      assert.doesNotMatch(after, /blocked\.example\.com:0\.0\.0\.0/);
    } finally {
      clearSandboxPermissionGrants();
      fs.rmSync(project, { recursive: true, force: true });
    }
  });
});
