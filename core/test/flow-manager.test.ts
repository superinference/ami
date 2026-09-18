import { describe, it, beforeEach } from 'node:test';
import * as assert from 'node:assert/strict';
import { FlowManager, FlowInfo } from '../src/flow-manager';

describe('FlowManager', () => {
  let fm: FlowManager;

  beforeEach(() => {
    fm = new FlowManager();
  });

  describe('getOrCreateActiveFlow', () => {
    it('creates a new flow with flow- prefix', () => {
      const flowId = fm.getOrCreateActiveFlow('test flow');
      assert.ok(flowId.startsWith('flow-'), 'flow ID must start with flow-');
    });

    it('returns same flow within time window', () => {
      const id1 = fm.getOrCreateActiveFlow('first');
      const id2 = fm.getOrCreateActiveFlow('second');
      assert.equal(id1, id2, 'must return same flow within window');
    });

    it('creates new flow after window expires', async () => {
      const id1 = fm.getOrCreateActiveFlow('first', 10);
      await new Promise(r => setTimeout(r, 20));
      const id2 = fm.getOrCreateActiveFlow('second', 10);
      assert.notEqual(id1, id2, 'must create new flow after window');
    });

    it('stores description from first call', () => {
      const flowId = fm.getOrCreateActiveFlow('my description');
      const flow = fm.getFlow(flowId);
      assert.equal(flow?.description, 'my description');
    });

    it('sets status to active on creation', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const flow = fm.getFlow(flowId);
      assert.equal(flow?.status, 'active');
    });

    it('sets createdAt timestamp', () => {
      const before = Date.now();
      const flowId = fm.getOrCreateActiveFlow('test');
      const after = Date.now();
      const flow = fm.getFlow(flowId);
      assert.ok(flow!.createdAt >= before && flow!.createdAt <= after);
    });

    it('initializes empty agentIds array', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const flow = fm.getFlow(flowId);
      assert.deepEqual(flow?.agentIds, []);
    });
  });

  describe('addAgent', () => {
    it('adds agent to existing flow', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const result = fm.addAgent(flowId, 'agent-abc');
      assert.equal(result, true);
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, ['agent-abc']);
    });

    it('adds multiple agents', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      fm.addAgent(flowId, 'agent-3');
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, ['agent-1', 'agent-2', 'agent-3']);
    });

    it('returns false for non-existent flow', () => {
      const result = fm.addAgent('flow-nonexistent', 'agent-abc');
      assert.equal(result, false);
    });
  });

  describe('getFlow', () => {
    it('returns flow info for existing flow', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      const flow = fm.getFlow(flowId);
      assert.ok(flow);
      assert.equal(flow.flowId, flowId);
    });

    it('returns undefined for non-existent flow', () => {
      assert.equal(fm.getFlow('flow-nonexistent'), undefined);
    });
  });

  describe('listFlows', () => {
    it('returns empty array initially', () => {
      assert.deepEqual(fm.listFlows(), []);
    });

    it('returns all flows', async () => {
      fm.getOrCreateActiveFlow('flow-a', 10);
      await new Promise(r => setTimeout(r, 20));
      fm.getOrCreateActiveFlow('flow-b', 10);
      const flows = fm.listFlows();
      assert.equal(flows.length, 2);
    });

    it('returns copies that include all fields', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      const flows = fm.listFlows();
      assert.equal(flows.length, 1);
      const flow = flows[0];
      assert.equal(flow.flowId, flowId);
      assert.equal(flow.description, 'test');
      assert.deepEqual(flow.agentIds, ['agent-1']);
      assert.equal(flow.status, 'active');
      assert.ok(typeof flow.createdAt === 'number');
    });
  });

  describe('updateFlowStatus', () => {
    it('sets completed when all agents done successfully', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      fm.updateFlowStatus(flowId, (id) => 'completed');
      assert.equal(fm.getFlow(flowId)?.status, 'completed');
    });

    it('sets partial when any agent failed', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'failed' };
      fm.updateFlowStatus(flowId, (id) => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'partial');
    });

    it('sets partial when any agent killed', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'killed' };
      fm.updateFlowStatus(flowId, (id) => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'partial');
    });

    it('stays active when any agent still running', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      const statuses: Record<string, string> = { 'agent-1': 'completed', 'agent-2': 'running' };
      fm.updateFlowStatus(flowId, (id) => statuses[id]);
      assert.equal(fm.getFlow(flowId)?.status, 'active');
    });

    it('clears activeFlowId when flow completes', async () => {
      const flowId = fm.getOrCreateActiveFlow('test', 10);
      fm.addAgent(flowId, 'agent-1');
      fm.updateFlowStatus(flowId, () => 'completed');
      await new Promise(r => setTimeout(r, 20));
      const newFlowId = fm.getOrCreateActiveFlow('new', 10);
      assert.notEqual(flowId, newFlowId, 'completed flow must not be reused');
    });

    it('no-ops for non-existent flow', () => {
      fm.updateFlowStatus('flow-nonexistent', () => 'completed');
    });

    it('no-ops for flow with no agents', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.updateFlowStatus(flowId, () => 'completed');
      assert.equal(fm.getFlow(flowId)?.status, 'active');
    });

    it('treats unknown agent status as non-running', () => {
      const flowId = fm.getOrCreateActiveFlow('test');
      fm.addAgent(flowId, 'agent-1');
      fm.updateFlowStatus(flowId, () => undefined);
      assert.equal(fm.getFlow(flowId)?.status, 'completed');
    });
  });

  describe('reset', () => {
    it('clears all flows', () => {
      fm.getOrCreateActiveFlow('test');
      fm.reset();
      assert.deepEqual(fm.listFlows(), []);
    });

    it('clears active flow so next call creates new', () => {
      const id1 = fm.getOrCreateActiveFlow('test');
      fm.reset();
      const id2 = fm.getOrCreateActiveFlow('test');
      assert.notEqual(id1, id2);
    });

    it('resets lastSpawnTime', async () => {
      fm.getOrCreateActiveFlow('test', 100000);
      fm.reset();
      await new Promise(r => setTimeout(r, 5));
      const id = fm.getOrCreateActiveFlow('new', 1);
      assert.ok(fm.getFlow(id), 'must create new flow after reset');
    });
  });

  describe('integration: time-window grouping', () => {
    it('groups rapid spawns into one flow', () => {
      const flowId = fm.getOrCreateActiveFlow('batch');
      fm.addAgent(flowId, 'agent-1');
      const sameFlowId = fm.getOrCreateActiveFlow('batch');
      fm.addAgent(sameFlowId, 'agent-2');
      assert.equal(flowId, sameFlowId);
      assert.deepEqual(fm.getFlow(flowId)?.agentIds, ['agent-1', 'agent-2']);
    });

    it('separate flows for separate batches', async () => {
      const id1 = fm.getOrCreateActiveFlow('batch-1', 10);
      fm.addAgent(id1, 'agent-1');
      await new Promise(r => setTimeout(r, 20));
      const id2 = fm.getOrCreateActiveFlow('batch-2', 10);
      fm.addAgent(id2, 'agent-2');
      assert.notEqual(id1, id2);
      assert.deepEqual(fm.getFlow(id1)?.agentIds, ['agent-1']);
      assert.deepEqual(fm.getFlow(id2)?.agentIds, ['agent-2']);
    });

    it('flow lifecycle: active → completed', () => {
      const flowId = fm.getOrCreateActiveFlow('lifecycle');
      fm.addAgent(flowId, 'agent-1');
      fm.addAgent(flowId, 'agent-2');
      assert.equal(fm.getFlow(flowId)?.status, 'active');
      fm.updateFlowStatus(flowId, (id) => id === 'agent-1' ? 'completed' : 'running');
      assert.equal(fm.getFlow(flowId)?.status, 'active');
      fm.updateFlowStatus(flowId, () => 'completed');
      assert.equal(fm.getFlow(flowId)?.status, 'completed');
    });
  });
});
