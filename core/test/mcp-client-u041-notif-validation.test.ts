/**
 * mcp-client-u041-notif-validation.test.ts — runtime schema validation of inbound
 * server→client MCP notification params at the trust boundary (U041).
 *
 * Covers the consolidated "schema-validated inbound notification handling" parity
 * gaps (Claude Code validates inbound notification params with a zod schema in
 * setNotificationHandler before dispatch; AMI mirrors that with a plain-TS
 * structural validator, matching core/'s no-zod convention): a malformed /
 * wrong-typed notification payload from an untrusted server MUST be rejected at the
 * boundary and surfaced as a `notification-error`, never fed to a handler or the
 * generic `notification` event.
 *
 * ESM-SAFE: pure-function unit tests + a real spawned stdio server driving the
 * live read loop (no source-text reads, no namespace monkeypatching).
 */

import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { McpClient, validateMcpNotificationParams } from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

const liveClients: McpClient[] = [];
afterEach(() => {
  while (liveClients.length) {
    try { liveClients.pop()!.disconnect(); } catch { /* ignore */ }
  }
});
function track(c: McpClient): McpClient { liveClients.push(c); c.on('error', () => {}); return c; }

describe('validateMcpNotificationParams — pure validator', () => {
  it('accepts a well-formed resources/updated (string uri)', () => {
    assert.deepEqual(
      validateMcpNotificationParams('notifications/resources/updated', { uri: 'file:///a' }),
      { ok: true },
    );
  });

  it('rejects resources/updated with a non-string / empty uri', () => {
    for (const bad of [{ uri: 123 }, { uri: '' }, {}, null, 'x', [] as unknown]) {
      const r = validateMcpNotificationParams('notifications/resources/updated', bad);
      assert.equal(r.ok, false, `expected reject for ${JSON.stringify(bad)}`);
    }
  });

  it('accepts a well-formed progress (string or numeric token, optional fields)', () => {
    assert.deepEqual(
      validateMcpNotificationParams('notifications/progress', { progressToken: 'tok', progress: 1, total: 4, message: 'q' }),
      { ok: true },
    );
    assert.deepEqual(
      validateMcpNotificationParams('notifications/progress', { progressToken: 7 }),
      { ok: true },
    );
  });

  it('rejects progress with a missing token or wrong-typed optional fields', () => {
    assert.equal(validateMcpNotificationParams('notifications/progress', { progress: 1 }).ok, false);
    assert.equal(validateMcpNotificationParams('notifications/progress', { progressToken: 't', progress: 'nope' }).ok, false);
    assert.equal(validateMcpNotificationParams('notifications/progress', { progressToken: 't', total: 'nope' }).ok, false);
    assert.equal(validateMcpNotificationParams('notifications/progress', { progressToken: 't', message: 5 }).ok, false);
  });

  it('accepts a message with a valid/absent level and rejects a bad level or logger', () => {
    assert.deepEqual(validateMcpNotificationParams('notifications/message', { data: 'x' }), { ok: true });
    assert.deepEqual(validateMcpNotificationParams('notifications/message', { level: 'error', logger: 'db', data: 'x' }), { ok: true });
    assert.equal(validateMcpNotificationParams('notifications/message', { level: 'LOUD', data: 'x' }).ok, false);
    assert.equal(validateMcpNotificationParams('notifications/message', { logger: 9, data: 'x' }).ok, false);
    assert.equal(validateMcpNotificationParams('notifications/message', 'not-an-object').ok, false);
  });

  it('accepts a cancelled with a token requestId and rejects a missing/wrong one', () => {
    assert.deepEqual(validateMcpNotificationParams('notifications/cancelled', { requestId: 3 }), { ok: true });
    assert.deepEqual(validateMcpNotificationParams('notifications/cancelled', { requestId: 'r-1' }), { ok: true });
    assert.equal(validateMcpNotificationParams('notifications/cancelled', {}).ok, false);
    assert.equal(validateMcpNotificationParams('notifications/cancelled', { requestId: true }).ok, false);
  });

  it('accepts any params for a method with no params contract', () => {
    assert.deepEqual(validateMcpNotificationParams('custom/anything', { whatever: true }), { ok: true });
    assert.deepEqual(validateMcpNotificationParams('notifications/tools/list_changed', undefined), { ok: true });
  });
});

describe('dispatchNotification — wired trust-boundary rejection', () => {
  it('drops a wrong-typed notification and surfaces notification-error instead of acting', () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', 'process.stdin.resume()'] }));
    const notifErrors: Array<{ method: string; err: Error }> = [];
    const resourceUpdates: string[] = [];
    client.on('notification-error', (method: string, err: Error) => notifErrors.push({ method, err }));
    client.on('resource-updated', (uri: string) => resourceUpdates.push(uri));

    const dispatch = (n: unknown) =>
      (client as unknown as { handleNotification: (n: unknown) => void }).handleNotification(n);

    // Malformed: numeric uri must NOT reach the resource-updated handler.
    dispatch({ jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 42 } });
    // Well-formed: must be dispatched normally.
    dispatch({ jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 'file:///ok' } });

    assert.deepEqual(resourceUpdates, ['file:///ok'], 'only the valid uri is emitted');
    assert.equal(notifErrors.length, 1);
    assert.equal(notifErrors[0].method, 'notifications/resources/updated');
    assert.match(notifErrors[0].err.message, /uri must be/);
  });

  it('does not emit the generic notification event for a rejected payload', () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', 'process.stdin.resume()'] }));
    const generic: string[] = [];
    const errs: string[] = [];
    client.on('notification', (m: string) => generic.push(m));
    client.on('notification-error', (m: string) => errs.push(m));
    (client as unknown as { handleNotification: (n: unknown) => void }).handleNotification(
      { jsonrpc: '2.0', method: 'notifications/progress', params: { progress: 1 } },
    );
    assert.deepEqual(generic, [], 'malformed payload must not reach the generic notification channel');
    assert.deepEqual(errs, ['notifications/progress']);
  });
});

describe('inbound validation — real spawned stdio server (end-to-end)', () => {
  // On notifications/initialized the server pushes, in one write: a MALFORMED
  // resources/updated (numeric uri), a MALFORMED progress (no token), and a VALID
  // resources/updated. The live read loop must reject the two malformed frames and
  // deliver only the valid one.
  const mixedServer = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 's', version: '1' }, capabilities: {} } }) + '\\n');
        } else if (msg.method === 'notifications/initialized') {
          process.stdout.write(
            JSON.stringify({ jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 7 } }) + '\\n' +
            JSON.stringify({ jsonrpc: '2.0', method: 'notifications/progress', params: { progress: 1, total: 2 } }) + '\\n' +
            JSON.stringify({ jsonrpc: '2.0', method: 'notifications/resources/updated', params: { uri: 'file://real' } }) + '\\n'
          );
        }
      } catch {}
    });
  `;

  it('rejects malformed frames and delivers the valid one through the read loop', async () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', mixedServer] }));
    const notifErrors: string[] = [];
    client.on('notification-error', (method: string) => notifErrors.push(method));
    const gotValid = new Promise<string>((resolve) => client.on('resource-updated', resolve));
    await client.connect();
    const uri = await Promise.race([
      gotValid,
      new Promise<never>((_, rej) => setTimeout(() => rej(new Error('valid resource-updated never arrived')), 2000)),
    ]);
    assert.equal(uri, 'file://real');
    assert.ok(notifErrors.includes('notifications/resources/updated'), 'malformed uri rejected');
    assert.ok(notifErrors.includes('notifications/progress'), 'tokenless progress rejected');
  });
});
