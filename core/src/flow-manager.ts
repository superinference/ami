import * as crypto from 'crypto';

export interface FlowInfo {
  flowId: string;
  description: string;
  agentIds: string[];
  status: 'active' | 'completed' | 'partial' | 'failed';
  createdAt: number;
}

export class FlowManager {
  private flows = new Map<string, FlowInfo>();
  private activeFlowId: string | null = null;
  private lastSpawnTime = 0;

  getOrCreateActiveFlow(description: string, windowMs = 5000): string {
    const now = Date.now();
    if (this.activeFlowId && this.flows.has(this.activeFlowId) && (now - this.lastSpawnTime) < windowMs) {
      this.lastSpawnTime = now;
      return this.activeFlowId;
    }
    const flowId = `flow-${crypto.randomBytes(4).toString('hex')}`;
    this.flows.set(flowId, {
      flowId,
      description,
      agentIds: [],
      status: 'active',
      createdAt: now,
    });
    this.activeFlowId = flowId;
    this.lastSpawnTime = now;
    return flowId;
  }

  addAgent(flowId: string, agentId: string): boolean {
    const flow = this.flows.get(flowId);
    if (!flow) return false;
    flow.agentIds.push(agentId);
    return true;
  }

  getFlow(flowId: string): FlowInfo | undefined {
    return this.flows.get(flowId);
  }

  listFlows(): FlowInfo[] {
    return Array.from(this.flows.values());
  }

  updateFlowStatus(flowId: string, getAgentStatus: (agentId: string) => string | undefined): void {
    const flow = this.flows.get(flowId);
    if (!flow || flow.agentIds.length === 0) return;
    const statuses = flow.agentIds.map(id => getAgentStatus(id) || 'unknown');
    const allDone = statuses.every(s => s !== 'running');
    const anyFailed = statuses.some(s => s === 'failed' || s === 'killed');
    if (allDone) {
      flow.status = anyFailed ? 'partial' : 'completed';
      if (this.activeFlowId === flowId) this.activeFlowId = null;
    }
  }

  reset(): void {
    this.flows.clear();
    this.activeFlowId = null;
    this.lastSpawnTime = 0;
  }
}
