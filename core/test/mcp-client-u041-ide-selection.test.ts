/**
 * mcp-client-u041-ide-selection.test.ts — IDE-as-MCP-server editor-context
 * notifications (U041).
 *
 * Parity with the reference's hooks/useIdeSelection.ts (`SelectionChangedSchema` +
 * `selectionChangeHandler`) and hooks/useIdeAtMentioned.ts (`AtMentionedSchema` +
 * the 0-based→1-based line conversion at L57-61). An IDE acting as an MCP server
 * sends `selection_changed` / `at_mentioned` notifications; AMI validates the
 * untrusted params at the trust boundary, normalizes them, and surfaces them as
 * typed `ide-selection` / `ide-at-mentioned` events for the engine/frontend to
 * fold the user's editor context into the turn.
 *
 * ESM-SAFE: pure-function unit tests + a real spawned stdio server driving the
 * live read loop (no source-text reads, no namespace monkeypatching).
 */

import { describe, it, after, afterEach } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  McpClient,
  validateMcpNotificationParams,
  normalizeIdeSelection,
  normalizeIdeAtMention,
  type McpIdeSelection,
  type McpIdeAtMention,
} from '../src/mcp/client';

after(() => { setTimeout(() => process.exit(0), 200); });

const liveClients: McpClient[] = [];
afterEach(() => {
  while (liveClients.length) {
    try { liveClients.pop()!.disconnect(); } catch { /* ignore */ }
  }
});
function track(c: McpClient): McpClient { liveClients.push(c); c.on('error', () => {}); return c; }

describe('validateMcpNotificationParams — selection_changed', () => {
  it('accepts a well-formed selection with start/end points, text and filePath', () => {
    assert.equal(
      validateMcpNotificationParams('selection_changed', {
        selection: { start: { line: 1, character: 0 }, end: { line: 3, character: 2 } },
        text: 'abc',
        filePath: '/a.ts',
      }).ok,
      true,
    );
  });

  it('accepts a null selection (editor cleared its range)', () => {
    assert.equal(validateMcpNotificationParams('selection_changed', { selection: null }).ok, true);
  });

  it('accepts an omitted selection', () => {
    assert.equal(validateMcpNotificationParams('selection_changed', {}).ok, true);
  });

  it('rejects a non-object params', () => {
    assert.equal(validateMcpNotificationParams('selection_changed', 7).ok, false);
  });

  it('rejects a non-object/non-null selection', () => {
    const r = validateMcpNotificationParams('selection_changed', { selection: 'nope' });
    assert.equal(r.ok, false);
    assert.match((r as { error: string }).error, /selection must be an object or null/);
  });

  it('rejects a selection endpoint missing numeric line/character', () => {
    const r = validateMcpNotificationParams('selection_changed', {
      selection: { start: { line: 1, character: 0 }, end: { line: '3', character: 2 } },
    });
    assert.equal(r.ok, false);
    assert.match((r as { error: string }).error, /selection\.end must have numeric line and character/);
  });

  it('rejects a non-string text / filePath', () => {
    assert.equal(validateMcpNotificationParams('selection_changed', { text: 5 }).ok, false);
    assert.equal(validateMcpNotificationParams('selection_changed', { filePath: 5 }).ok, false);
  });
});

describe('validateMcpNotificationParams — at_mentioned', () => {
  it('accepts a well-formed at-mention with 0-based line range', () => {
    assert.equal(
      validateMcpNotificationParams('at_mentioned', { filePath: '/a.ts', lineStart: 0, lineEnd: 4 }).ok,
      true,
    );
  });

  it('accepts a filePath without a line range', () => {
    assert.equal(validateMcpNotificationParams('at_mentioned', { filePath: '/a.ts' }).ok, true);
  });

  it('rejects a missing / empty / non-string filePath', () => {
    assert.equal(validateMcpNotificationParams('at_mentioned', {}).ok, false);
    assert.equal(validateMcpNotificationParams('at_mentioned', { filePath: '' }).ok, false);
    assert.equal(validateMcpNotificationParams('at_mentioned', { filePath: 5 }).ok, false);
  });

  it('rejects a non-numeric lineStart / lineEnd', () => {
    assert.equal(validateMcpNotificationParams('at_mentioned', { filePath: '/a.ts', lineStart: 'x' }).ok, false);
    assert.equal(validateMcpNotificationParams('at_mentioned', { filePath: '/a.ts', lineEnd: 'x' }).ok, false);
  });
});

describe('normalizeIdeSelection — parity with the reference selectionChangeHandler', () => {
  it('computes lineCount = end.line - start.line + 1 for a multi-line range', () => {
    const sel = normalizeIdeSelection({
      selection: { start: { line: 2, character: 4 }, end: { line: 5, character: 7 } },
      text: 'hi',
      filePath: '/a.ts',
    });
    assert.deepEqual(sel, { lineCount: 4, lineStart: 2, text: 'hi', filePath: '/a.ts' });
  });

  it('decrements lineCount by one when the selection ends at column 0', () => {
    const sel = normalizeIdeSelection({
      selection: { start: { line: 2, character: 0 }, end: { line: 5, character: 0 } },
    });
    assert.equal(sel?.lineCount, 3);
    assert.equal(sel?.lineStart, 2);
  });

  it('returns null for a null selection (no concrete range)', () => {
    assert.equal(normalizeIdeSelection({ selection: null, text: '' }), null);
  });
});

describe('normalizeIdeAtMention — 0-based→1-based conversion', () => {
  it('adds +1 only to defined line numbers', () => {
    assert.deepEqual(
      normalizeIdeAtMention({ filePath: '/a.ts', lineStart: 0, lineEnd: 4 }),
      { filePath: '/a.ts', lineStart: 1, lineEnd: 5 },
    );
  });

  it('leaves undefined line numbers undefined', () => {
    assert.deepEqual(
      normalizeIdeAtMention({ filePath: '/a.ts' }),
      { filePath: '/a.ts', lineStart: undefined, lineEnd: undefined },
    );
  });
});

describe('dispatchNotification — typed IDE events (wired)', () => {
  it('emits ide-selection for a valid selection_changed and nothing for null-range', () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', 'process.stdin.resume()'] }));
    const selections: McpIdeSelection[] = [];
    client.on('ide-selection', (s: McpIdeSelection) => selections.push(s));
    const dispatch = (n: unknown) =>
      (client as unknown as { handleNotification: (n: unknown) => void }).handleNotification(n);

    dispatch({
      jsonrpc: '2.0',
      method: 'selection_changed',
      params: { selection: { start: { line: 0, character: 0 }, end: { line: 2, character: 3 } }, filePath: '/x.ts' },
    });
    // null-range selection must NOT emit.
    dispatch({ jsonrpc: '2.0', method: 'selection_changed', params: { selection: null } });

    assert.equal(selections.length, 1);
    assert.deepEqual(selections[0], { lineCount: 3, lineStart: 0, text: undefined, filePath: '/x.ts' });
  });

  it('emits ide-at-mentioned with 1-based lines for a valid at_mentioned', () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', 'process.stdin.resume()'] }));
    const mentions: McpIdeAtMention[] = [];
    client.on('ide-at-mentioned', (m: McpIdeAtMention) => mentions.push(m));
    (client as unknown as { handleNotification: (n: unknown) => void }).handleNotification({
      jsonrpc: '2.0',
      method: 'at_mentioned',
      params: { filePath: '/y.ts', lineStart: 9, lineEnd: 11 },
    });
    assert.deepEqual(mentions, [{ filePath: '/y.ts', lineStart: 10, lineEnd: 12 }]);
  });

  it('rejects a malformed selection_changed and emits no typed event', () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', 'process.stdin.resume()'] }));
    const selections: McpIdeSelection[] = [];
    const errs: string[] = [];
    client.on('ide-selection', (s: McpIdeSelection) => selections.push(s));
    client.on('notification-error', (m: string) => errs.push(m));
    (client as unknown as { handleNotification: (n: unknown) => void }).handleNotification({
      jsonrpc: '2.0',
      method: 'selection_changed',
      params: { selection: { start: { line: 0, character: 0 }, end: { line: 'bad', character: 0 } } },
    });
    assert.deepEqual(selections, []);
    assert.deepEqual(errs, ['selection_changed']);
  });
});

describe('IDE selection — real spawned stdio server (end-to-end)', () => {
  // On notifications/initialized the server pushes a valid selection_changed and a
  // valid at_mentioned; the live read loop must validate, normalize and surface
  // both as typed events.
  const ideServer = `
    const readline = require('readline');
    const rl = readline.createInterface({ input: process.stdin });
    rl.on('line', (line) => {
      try {
        const msg = JSON.parse(line);
        if (msg.method === 'initialize') {
          process.stdout.write(JSON.stringify({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: '2024-11-05', serverInfo: { name: 'ide', version: '1' }, capabilities: {} } }) + '\\n');
        } else if (msg.method === 'notifications/initialized') {
          process.stdout.write(
            JSON.stringify({ jsonrpc: '2.0', method: 'selection_changed', params: { selection: { start: { line: 3, character: 0 }, end: { line: 6, character: 2 } }, filePath: '/live.ts' } }) + '\\n' +
            JSON.stringify({ jsonrpc: '2.0', method: 'at_mentioned', params: { filePath: '/live.ts', lineStart: 3, lineEnd: 6 } }) + '\\n'
          );
        }
      } catch {}
    });
  `;

  it('delivers normalized ide-selection and ide-at-mentioned through the read loop', async () => {
    const client = track(new McpClient({ command: 'node', args: ['-e', ideServer] }));
    const gotSel = new Promise<McpIdeSelection>((resolve) => client.on('ide-selection', resolve));
    const gotMention = new Promise<McpIdeAtMention>((resolve) => client.on('ide-at-mentioned', resolve));
    await client.connect();
    const [sel, mention] = await Promise.race([
      Promise.all([gotSel, gotMention]),
      new Promise<never>((_, rej) => setTimeout(() => rej(new Error('IDE events never arrived')), 2000)),
    ]);
    assert.deepEqual(sel, { lineCount: 4, lineStart: 3, text: undefined, filePath: '/live.ts' });
    assert.deepEqual(mention, { filePath: '/live.ts', lineStart: 4, lineEnd: 7 });
  });
});
