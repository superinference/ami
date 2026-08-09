import * as crypto from 'crypto';

/** @public Represents an approval record for tool execution in automated/library usage. */
export interface ToolApproval {
  scope: 'once' | 'session' | 'workspace';
  toolName: string;
  paramsHash?: string;
  approvedAt: number;
}

export class ToolConfirmationService {
  private approvals: Map<string, ToolApproval> = new Map();

  approve(toolName: string, params: Record<string, unknown> | null, scope: 'once' | 'session' | 'workspace'): void {
    const key = this.buildKey(toolName, params, scope);
    this.approvals.set(key, {
      scope,
      toolName,
      paramsHash: params ? this.hashParams(params) : undefined,
      approvedAt: Date.now(),
    });
  }

  isApproved(toolName: string, params: Record<string, unknown> | null): boolean {
    // Exact combination match (once, session, or workspace)
    const exactOnce = this.buildKey(toolName, params, 'once');
    if (this.approvals.has(exactOnce)) return true;
    const exactSession = this.buildKey(toolName, params, 'session');
    if (this.approvals.has(exactSession)) return true;
    const exactWs = this.buildKey(toolName, params, 'workspace');
    if (this.approvals.has(exactWs)) return true;
    // Tool-wide approval (no specific params)
    const toolSession = this.buildKey(toolName, null, 'session');
    if (this.approvals.has(toolSession)) return true;
    const toolWs = this.buildKey(toolName, null, 'workspace');
    if (this.approvals.has(toolWs)) return true;
    return false;
  }

  private buildKey(toolName: string, params: Record<string, unknown> | null, scope: string): string {
    const hash = params ? this.hashParams(params) : 'any';
    return `${scope}:${toolName}:${hash}`;
  }

  private hashParams(params: Record<string, unknown>): string {
    const sorted = JSON.stringify(params, Object.keys(params).sort());
    return crypto.createHash('sha256').update(sorted).digest('hex').slice(0, 16);
  }

  clearSession(): void {
    for (const [key] of this.approvals) {
      if (key.startsWith('once:') || key.startsWith('session:')) {
        this.approvals.delete(key);
      }
    }
  }
}
