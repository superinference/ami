import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { endConversationTool } from '../src/tools/end-conversation';
import type { ToolContext } from '../src/types';

const stubContext: ToolContext = {
  cwd: '/tmp',
  abortSignal: new AbortController().signal,
};

describe('endConversationTool', () => {
  it('has correct name', () => {
    assert.equal(endConversationTool.name, 'EndConversation');
  });

  it('has description mentioning abuse', () => {
    assert.ok(endConversationTool.description.includes('abusive'));
  });

  it('is read-only', () => {
    assert.equal(endConversationTool.isReadOnly, true);
  });

  it('has required reason parameter', () => {
    assert.deepEqual(endConversationTool.inputSchema.required, ['reason']);
    assert.ok(endConversationTool.inputSchema.properties?.reason);
  });

  it('returns output with reason', async () => {
    const result = await endConversationTool.execute(
      { reason: 'Abusive language' },
      stubContext,
    );
    assert.equal(result.output, 'Conversation ended: Abusive language');
  });

  it('sets endConversation metadata to true', async () => {
    const result = await endConversationTool.execute(
      { reason: 'Jailbreak attempt' },
      stubContext,
    );
    assert.equal(result.metadata?.endConversation, true);
    assert.equal(result.metadata?.reason, 'Jailbreak attempt');
  });

  it('uses default reason when empty', async () => {
    const result = await endConversationTool.execute(
      { reason: '' },
      stubContext,
    );
    assert.equal(result.output, 'Conversation ended: Policy violation');
    assert.equal(result.metadata?.reason, 'Policy violation');
  });

  it('handles long reason string', async () => {
    const longReason = 'x'.repeat(1000);
    const result = await endConversationTool.execute(
      { reason: longReason },
      stubContext,
    );
    assert.ok(result.output.includes(longReason));
  });

  it('is not an error result', async () => {
    const result = await endConversationTool.execute(
      { reason: 'test' },
      stubContext,
    );
    assert.equal(result.isError, undefined);
  });
});
