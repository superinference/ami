import { describe, it } from 'node:test';
import * as assert from 'node:assert/strict';
import { askUserQuestionTool } from '../src/tools/ask-user';
import { HookManager } from '../src/hooks';
import {
  WorkflowRuntime,
  executeWorkflow,
} from '../src/tools/workflow-runtime';
import type { ToolContext } from '../src/types';

function ctx(): ToolContext {
  return {
    cwd: process.cwd(),
    abortSignal: new AbortController().signal,
  };
}

// ---------------------------------------------------------------------------
// 1. AskUserQuestion — preview, annotations, metadata.source
// ---------------------------------------------------------------------------

describe('askUserQuestionTool — new schema fields', () => {
  it('options items include preview field', () => {
    const questions = askUserQuestionTool.inputSchema.properties.questions;
    const optionProps = questions.items.properties.options.items.properties;
    assert.ok('preview' in optionProps, 'options items should have preview');
    assert.equal(optionProps.preview.type, 'string');
  });

  it('schema includes annotations field', () => {
    const props = askUserQuestionTool.inputSchema.properties;
    assert.ok('annotations' in props, 'schema should have annotations');
    assert.equal(props.annotations.type, 'object');
    assert.ok(props.annotations.additionalProperties);
    const innerProps = props.annotations.additionalProperties.properties;
    assert.ok('notes' in innerProps);
    assert.ok('preview' in innerProps);
  });

  it('schema includes metadata with source field', () => {
    const props = askUserQuestionTool.inputSchema.properties;
    assert.ok('metadata' in props, 'schema should have metadata');
    assert.equal(props.metadata.type, 'object');
    assert.ok('source' in props.metadata.properties);
    assert.equal(props.metadata.properties.source.type, 'string');
  });

  it('schema includes answers field', () => {
    const props = askUserQuestionTool.inputSchema.properties;
    assert.ok('answers' in props, 'schema should have answers');
    assert.equal(props.answers.type, 'object');
  });

  it('execute still works with new fields present in input', async () => {
    const result = await askUserQuestionTool.execute(
      {
        questions: [{
          question: 'Pick one?',
          header: 'Choice',
          options: [
            { label: 'A', description: 'Option A', preview: '```\nA code\n```' },
            { label: 'B', description: 'Option B' },
          ],
          multiSelect: false,
        }],
        annotations: { 'Pick one?': { notes: 'user note', preview: 'A code' } },
        metadata: { source: 'remember' },
      },
      ctx(),
    );
    assert.ok(!result.isError);
  });
});

// ---------------------------------------------------------------------------
// 2. HookManager — prompt and agent hook runners
// ---------------------------------------------------------------------------

describe('HookManager — prompt and agent hooks', () => {
  it('setEngineFactory/setProviderConfig/setCwd are callable', () => {
    const hm = new HookManager();
    hm.setEngineFactory((_config: any) => ({
      submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
      shutdown: () => {},
    }));
    hm.setProviderConfig({ model: 'test' });
    hm.setCwd('/tmp');
    assert.ok(true);
  });

  it('prompt hook calls engine when factory is set', async () => {
    const hm = new HookManager();
    let receivedPrompt = '';
    hm.setEngineFactory((_config: any) => ({
      submit: async function* (prompt: string) {
        receivedPrompt = prompt;
        yield { type: 'text_delta', text: 'ALLOW' };
      },
      shutdown: () => {},
    }));
    hm.setProviderConfig({ model: 'test-model' });
    hm.setCwd('/tmp');

    hm.loadFromFile('/nonexistent');

    hm.onPreToolUse(async () => ({ action: 'allow' as const }));

    const hookEntry = {
      event: 'postSampling',
      hook: { type: 'prompt' as const, prompt: 'Check safety' },
    };
    (hm as any).processHookEntry(hookEntry, '/tmp');

    await hm.executePostSampling({
      messages: [{ role: 'user', content: 'test' }],
      turnCount: 1,
    });
    assert.ok(receivedPrompt.includes('Check safety'));
  });

  it('agent hook calls engine when factory is set', async () => {
    const hm = new HookManager();
    let called = false;
    hm.setEngineFactory((_config: any) => ({
      submit: async function* () {
        called = true;
        yield { type: 'text_delta', text: 'agent result' };
      },
      shutdown: () => {},
    }));
    hm.setProviderConfig({ model: 'test-model' });

    const hookEntry = {
      event: 'postSampling',
      hook: { type: 'agent' as const, prompt: 'Analyze this' },
    };
    (hm as any).processHookEntry(hookEntry, '/tmp');

    await hm.executePostSampling({
      messages: [{ role: 'user', content: 'test' }],
      turnCount: 1,
    });
    assert.ok(called, 'agent hook should call engine');
  });

  it('prompt hook returns empty string when no engine factory', async () => {
    const hm = new HookManager();
    const result = await (hm as any).runPromptHook(
      { type: 'prompt', prompt: 'test' },
      '{}',
    );
    assert.equal(result, '');
  });

  it('agent hook returns empty string when no engine factory', async () => {
    const hm = new HookManager();
    const result = await (hm as any).runAgentHook(
      { type: 'agent', prompt: 'test' },
      '{}',
    );
    assert.equal(result, '');
  });

  it('prompt hook respects model override from config', async () => {
    const hm = new HookManager();
    let usedConfig: any = null;
    hm.setEngineFactory((config: any) => {
      usedConfig = config;
      return {
        submit: async function* () { yield { type: 'text_delta', text: 'ok' }; },
        shutdown: () => {},
      };
    });
    hm.setProviderConfig({ model: 'default-model' });

    await (hm as any).runPromptHook(
      { type: 'prompt', prompt: 'test', model: 'override-model' },
      '{}',
    );
    assert.equal(usedConfig.provider.model, 'override-model');
  });
});

// ---------------------------------------------------------------------------
// 3. Workflow Runtime — nested workflow() function
// ---------------------------------------------------------------------------

describe('WorkflowRuntime — nested workflow()', () => {
  it('context includes workflow function', () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'result',
    });
    const wfCtx = runtime.createContext({});
    assert.equal(typeof wfCtx.workflow, 'function');
  });

  it('workflow() throws when no resolver is configured', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'result',
    });
    const wfCtx = runtime.createContext({});
    await assert.rejects(
      () => wfCtx.workflow('nonexistent'),
      /no workflow resolver/,
    );
  });

  it('workflow() throws when resolver returns null', async () => {
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'result',
      workflowResolver: () => null,
    });
    const wfCtx = runtime.createContext({});
    await assert.rejects(
      () => wfCtx.workflow('nonexistent'),
      /Workflow not found/,
    );
  });

  it('workflow() executes a child workflow and returns its result', async () => {
    const childScript = `
export const meta = {
  name: 'child',
  description: 'A child workflow',
}
const result = await agent('do something')
return { fromChild: result }
`;
    const calls: string[] = [];
    const runtime = new WorkflowRuntime({
      agentHandler: async (prompt) => { calls.push(prompt); return `done: ${prompt}`; },
      workflowResolver: (nameOrRef) => {
        if (typeof nameOrRef === 'string' && nameOrRef === 'child') return childScript;
        return null;
      },
    });
    const wfCtx = runtime.createContext({});
    const result = await wfCtx.workflow('child') as any;
    assert.ok(result);
    assert.equal(result.fromChild, 'done: do something');
    assert.ok(calls.includes('do something'));
  });

  it('workflow() passes args to child', async () => {
    const childScript = `
export const meta = {
  name: 'child',
  description: 'Child with args',
}
return { received: args }
`;
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => childScript,
    });
    const wfCtx = runtime.createContext({});
    const result = await wfCtx.workflow('child', { key: 'value' }) as any;
    assert.deepEqual(result.received, { key: 'value' });
  });

  it('workflow() refuses double-nesting', async () => {
    const grandchildScript = `
export const meta = {
  name: 'grandchild',
  description: 'Too deep',
}
return 'deep'
`;
    const childScript = `
export const meta = {
  name: 'child',
  description: 'Tries to nest',
}
return await workflow('grandchild')
`;
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: (ref) => {
        if (ref === 'child') return childScript;
        if (ref === 'grandchild') return grandchildScript;
        return null;
      },
    });
    const wfCtx = runtime.createContext({});
    await assert.rejects(
      () => wfCtx.workflow('child'),
      /limited to one level/,
    );
  });

  it('workflow() aggregates agent count and logs from child', async () => {
    const childScript = `
export const meta = {
  name: 'child',
  description: 'Logs and agents',
}
log('child log entry')
await agent('child agent call')
return 'done'
`;
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: () => childScript,
    });
    const wfCtx = runtime.createContext({});
    await wfCtx.workflow('child');
    assert.ok(runtime.agentCount >= 1);
    assert.ok(runtime.logs.includes('child log entry'));
  });

  it('executeWorkflow passes workflowResolver through', async () => {
    const parentScript = `
export const meta = {
  name: 'parent',
  description: 'Calls child',
}
const childResult = await workflow('sub')
return { parent: true, child: childResult }
`;
    const childScript = `
export const meta = {
  name: 'sub',
  description: 'Sub workflow',
}
return { sub: true }
`;
    const result = await executeWorkflow(parentScript, async () => 'ok', {
      workflowResolver: (ref) => ref === 'sub' ? childScript : null,
    });
    const r = result.result as any;
    assert.ok(r.parent);
    assert.ok(r.child.sub);
  });

  it('workflow() accepts scriptPath ref', async () => {
    const childScript = `
export const meta = {
  name: 'file-child',
  description: 'From file',
}
return { fromFile: true }
`;
    const runtime = new WorkflowRuntime({
      agentHandler: async () => 'ok',
      workflowResolver: (ref) => {
        if (typeof ref === 'object' && ref.scriptPath === '/tmp/test.js') return childScript;
        return null;
      },
    });
    const wfCtx = runtime.createContext({});
    const result = await wfCtx.workflow({ scriptPath: '/tmp/test.js' }) as any;
    assert.ok(result.fromFile);
  });
});
