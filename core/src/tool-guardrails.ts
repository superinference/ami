import * as crypto from 'crypto';

export interface GuardrailDecision {
  action: 'allow' | 'warn' | 'block';
  reason?: string;
}

const IDEMPOTENT_TOOLS = new Set([
  'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
  'web_search', 'web_fetch',
]);

const EXACT_FAILURE_WARN = 2;
const EXACT_FAILURE_BLOCK = 4;
const NO_PROGRESS_WARN = 3;
const NO_PROGRESS_BLOCK = 5;

function hashArgs(toolName: string, args: Record<string, unknown>): string {
  const sorted = JSON.stringify(args, Object.keys(args).sort());
  return crypto.createHash('sha256').update(`${toolName}:${sorted}`).digest('hex').slice(0, 16);
}

function hashResult(output: string): string {
  return crypto.createHash('sha256').update(output).digest('hex').slice(0, 16);
}

const MAX_TRACKED_SIGNATURES = 500;
const LOOP_HISTORY_SIZE = 20;
const LOOP_MIN_SEQUENCE = 2;

export class ToolCallGuardrailController {
  private exactFailures = new Map<string, number>();
  private noProgressHistory = new Map<string, { resultHash: string; count: number }>();
  private callHistory: string[] = [];

  private evictOldest(map: Map<string, unknown>): void {
    if (map.size > MAX_TRACKED_SIGNATURES) {
      const firstKey = map.keys().next().value;
      if (firstKey !== undefined) map.delete(firstKey);
    }
  }

  beforeCall(toolName: string, args: Record<string, unknown>): GuardrailDecision {
    const sig = hashArgs(toolName, args);
    const failCount = this.exactFailures.get(sig) || 0;

    if (failCount >= EXACT_FAILURE_BLOCK) {
      return { action: 'block', reason: `Tool "${toolName}" has failed ${failCount} times with identical arguments. Blocked to prevent infinite loop.` };
    }
    if (failCount >= EXACT_FAILURE_WARN) {
      return { action: 'warn', reason: `Tool "${toolName}" has failed ${failCount} times with identical arguments. Try a different approach.` };
    }

    return { action: 'allow' };
  }

  afterCall(toolName: string, args: Record<string, unknown>, output: string, failed: boolean): GuardrailDecision {
    const sig = hashArgs(toolName, args);

    if (failed) {
      this.exactFailures.set(sig, (this.exactFailures.get(sig) || 0) + 1);
      this.evictOldest(this.exactFailures);
    } else {
      this.exactFailures.delete(sig);
    }

    const combined = sig + ':' + hashResult(output);
    this.callHistory.push(combined);
    if (this.callHistory.length > LOOP_HISTORY_SIZE) {
      this.callHistory.shift();
    }

    const loop = this.detectLoop();
    if (loop) {
      return { action: 'warn', reason: `Repeating pattern detected: the last ${loop.length} tool calls match a previous sequence. You may be in a loop — try a fundamentally different approach.` };
    }

    if (!failed && IDEMPOTENT_TOOLS.has(toolName)) {
      const rHash = hashResult(output);
      const prev = this.noProgressHistory.get(sig);

      if (prev && prev.resultHash === rHash) {
        prev.count++;
        if (prev.count >= NO_PROGRESS_BLOCK) {
          return { action: 'block', reason: `Tool "${toolName}" returned identical results ${prev.count} times. The data hasn't changed — try a different query or approach.` };
        }
        if (prev.count >= NO_PROGRESS_WARN) {
          return { action: 'warn', reason: `Tool "${toolName}" is returning the same results repeatedly (${prev.count} times).` };
        }
      } else {
        this.noProgressHistory.set(sig, { resultHash: rHash, count: 1 });
        this.evictOldest(this.noProgressHistory);
      }
    }

    return { action: 'allow' };
  }

  detectLoop(): string[] | null {
    const h = this.callHistory;
    if (h.length < LOOP_MIN_SEQUENCE * 2) return null;

    for (let seqLen = LOOP_MIN_SEQUENCE; seqLen <= Math.floor(h.length / 2); seqLen++) {
      const tail = h.slice(h.length - seqLen);
      const prev = h.slice(h.length - seqLen * 2, h.length - seqLen);
      if (tail.every((v, i) => v === prev[i])) {
        return tail;
      }
    }
    return null;
  }

  reset(): void {
    this.exactFailures.clear();
    this.noProgressHistory.clear();
    this.callHistory = [];
  }
}
