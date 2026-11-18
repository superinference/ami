/**
 * U003 / w104 parity — persistToSettings on a per-host sandbox network-egress grant
 * (allow-once vs allow-and-remember-host).
 *
 * Mirrors Claude's `{ allow, persistToSettings }` network-egress response
 * (SandboxPermissionRequest.tsx): the dialog's "yes, don't ask again" option writes
 * the specific host to durable settings so it is never re-prompted, distinct from a
 * plain "yes" (allow once). AMI's registry grants were previously SESSION-only; this
 * adds a remembered tier wired through the SAME `getGrantedSandboxHosts()` the sandbox
 * backends fold into the effective outbound allowlist.
 *
 * RED (pre-fix): `allow_pattern` was treated identically to `allow_once` (session
 * grant), nothing was written to disk, and `/clear` forgot every host — so the
 * "remembered across a restart, never re-prompted" assertions below fail.
 *
 * Run: cd core/core && node --import tsx --test test/sandbox-permission-persist-u003.test.ts
 */
import { describe, it, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  requestSandboxHostAccess,
  persistSandboxHostGrant,
  recordSandboxHostGrant,
  isSandboxHostGranted,
  getGrantedSandboxHosts,
  clearSandboxPermissionGrants,
} from '../src/tools/sandbox-permission-registry';
import type { PermissionPromptHandler, PermissionPromptResult } from '../src/permissions';

/** A permission handler that records its prompts and returns a scripted verdict. */
function handlerReturning(
  action: PermissionPromptResult['action'],
): { handler: PermissionPromptHandler; prompts: string[] } {
  const prompts: string[] = [];
  const handler: PermissionPromptHandler = {
    async showPrompt(_tool, inputSummary) {
      prompts.push(inputSummary);
      return { action };
    },
  };
  return { handler, prompts };
}

let dataDir: string;
let prevEnv: string | undefined;

beforeEach(() => {
  // Isolate the durable store under a fresh temp data dir per test.
  dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ami-sbx-persist-'));
  prevEnv = process.env.SUPERINFERENCE_DATA_DIR;
  process.env.SUPERINFERENCE_DATA_DIR = dataDir;
  // Reset session grants + the lazy-load flag so this test reads its own store.
  clearSandboxPermissionGrants();
});

afterEach(() => {
  clearSandboxPermissionGrants();
  if (prevEnv === undefined) delete process.env.SUPERINFERENCE_DATA_DIR;
  else process.env.SUPERINFERENCE_DATA_DIR = prevEnv;
  try {
    fs.rmSync(dataDir, { recursive: true, force: true });
  } catch {
    /* best effort */
  }
});

const storePath = () => path.join(dataDir, 'sandbox-hosts.json');

describe('U003 persistSandboxHostGrant (allow-and-remember)', () => {
  it('writes the host to the durable store and grants it', () => {
    persistSandboxHostGrant('Persist.Example.com');
    assert.equal(isSandboxHostGranted('persist.example.com'), true);
    assert.ok(fs.existsSync(storePath()), 'store file should be created');
    const parsed = JSON.parse(fs.readFileSync(storePath(), 'utf-8')) as { hosts: string[] };
    assert.deepEqual(parsed.hosts, ['persist.example.com']); // normalized (lowercased/trimmed)
  });

  it('is idempotent — a repeated remember does not duplicate the on-disk entry', () => {
    persistSandboxHostGrant('dup.example.com');
    persistSandboxHostGrant('  DUP.example.com  ');
    const parsed = JSON.parse(fs.readFileSync(storePath(), 'utf-8')) as { hosts: string[] };
    assert.deepEqual(parsed.hosts, ['dup.example.com']);
  });

  it('ignores empty/whitespace hosts and writes no store', () => {
    persistSandboxHostGrant('   ');
    assert.equal(fs.existsSync(storePath()), false);
  });

  it('accumulates multiple remembered hosts, sorted', () => {
    persistSandboxHostGrant('zeta.example.com');
    persistSandboxHostGrant('alpha.example.com');
    const parsed = JSON.parse(fs.readFileSync(storePath(), 'utf-8')) as { hosts: string[] };
    assert.deepEqual(parsed.hosts, ['alpha.example.com', 'zeta.example.com']);
  });
});

describe('U003 remembered hosts survive /clear (restart parity)', () => {
  it('persisted host is still granted after clear; session grant is dropped', () => {
    persistSandboxHostGrant('remembered.example.com');
    recordSandboxHostGrant('session-only.example.com');
    assert.equal(isSandboxHostGranted('session-only.example.com'), true);

    clearSandboxPermissionGrants(); // simulate /clear (or a fresh process)

    assert.equal(isSandboxHostGranted('remembered.example.com'), true, 'remembered host survives');
    assert.equal(isSandboxHostGranted('session-only.example.com'), false, 'session grant dropped');
    assert.deepEqual(getGrantedSandboxHosts(), ['remembered.example.com']);
  });

  it('a corrupt/missing store is fail-soft (nothing remembered, no throw)', () => {
    fs.writeFileSync(storePath(), '{ not valid json');
    clearSandboxPermissionGrants(); // force a reload of the corrupt file
    assert.deepEqual(getGrantedSandboxHosts(), []);
    assert.equal(isSandboxHostGranted('anything.example.com'), false);
  });

  it('a non-array hosts field is ignored', () => {
    fs.writeFileSync(storePath(), JSON.stringify({ hosts: 'nope' }));
    clearSandboxPermissionGrants();
    assert.deepEqual(getGrantedSandboxHosts(), []);
  });
});

describe('U003 requestSandboxHostAccess honors persistToSettings via verdict', () => {
  it('allow_pattern ("always allow this host") remembers across a restart, never re-prompts', async () => {
    const first = handlerReturning('allow_pattern');
    assert.equal(await requestSandboxHostAccess('always.example.com', first.handler), true);
    assert.deepEqual(first.prompts, ['Allow network connection to always.example.com?']);
    assert.ok(fs.existsSync(storePath()), 'allow_pattern persists to the store');

    clearSandboxPermissionGrants(); // simulate a new session

    // A fresh request for the SAME host must NOT re-prompt — it is remembered.
    const second = handlerReturning('deny');
    assert.equal(await requestSandboxHostAccess('always.example.com', second.handler), true);
    assert.deepEqual(second.prompts, [], 'remembered host is not re-prompted');
  });

  it('allow_once grants only for this session — forgotten after a restart, re-prompts', async () => {
    const first = handlerReturning('allow_once');
    assert.equal(await requestSandboxHostAccess('once.example.com', first.handler), true);
    assert.equal(fs.existsSync(storePath()), false, 'allow_once does not persist');

    clearSandboxPermissionGrants(); // simulate a new session

    // The host is forgotten → a fresh request prompts again (and here is denied).
    const second = handlerReturning('deny');
    assert.equal(await requestSandboxHostAccess('once.example.com', second.handler), false);
    assert.deepEqual(second.prompts, ['Allow network connection to once.example.com?']);
  });

  it('allow_session grants for this session only (does not persist)', async () => {
    const { handler } = handlerReturning('allow_session');
    assert.equal(await requestSandboxHostAccess('sess.example.com', handler), true);
    assert.equal(fs.existsSync(storePath()), false);
  });
});
