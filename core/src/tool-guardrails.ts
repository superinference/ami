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

function recoveryHint(toolName: string): string {
  switch (toolName) {
    case 'bash':
      return 'Use file_read to re-read the files you edited, check for syntax errors, then fix the code before running tests again.';
    case 'file_edit':
      return 'Use file_read to see the current file content, then use the exact text from the file as old_string.';
    default:
      return 'Try a fundamentally different approach.';
  }
}

const MAX_TRACKED_SIGNATURES = 500;
const LOOP_HISTORY_SIZE = 20;
const LOOP_MIN_SEQUENCE = 2;

export interface ProgressSnapshot {
  totalToolCalls: number;
  totalBashCalls: number;
  totalEdits: number;
  totalReads: number;
  editsSinceLastBash: number;
  editFailsSinceLastBash: number;
  toolsSinceLastBash: number;
}

export class ToolCallGuardrailController {
  private exactFailures = new Map<string, number>();
  private noProgressHistory = new Map<string, { resultHash: string; count: number }>();
  private callHistory: string[] = [];
  private fileFailures = new Map<string, number>();
  private editsSinceLastBash = 0;
  private editFailsSinceLastBash = 0;
  private toolsSinceLastBash = 0;
  private totalToolCalls = 0;
  private totalBashCalls = 0;
  private totalEdits = 0;
  private totalReads = 0;

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
      return { action: 'block', reason: `Tool "${toolName}" has failed ${failCount} times with identical arguments. ${recoveryHint(toolName)} Do NOT retry the same command — change your approach.` };
    }
    if (failCount >= EXACT_FAILURE_WARN) {
      return { action: 'warn', reason: `Tool "${toolName}" has failed ${failCount} times with identical arguments. ${recoveryHint(toolName)}` };
    }

    if ((toolName === 'file_edit' || toolName === 'file_read') && this.editFailsSinceLastBash >= 3 && this.editsSinceLastBash >= 5) {
      return { action: 'warn', reason: 'You have made multiple file edits without running tests. Run the test command with bash to check your progress before making more edits.' };
    }

    if (toolName === 'file_read' && this.toolsSinceLastBash >= 10 && this.editsSinceLastBash === 0) {
      return { action: 'warn', reason: 'You have read files 10+ times without running any bash commands or making edits. Run the test suite with bash to validate your understanding before continuing to read.' };
    }

    return { action: 'allow' };
  }

  afterCall(toolName: string, args: Record<string, unknown>, output: string, failed: boolean): GuardrailDecision {
    const sig = hashArgs(toolName, args);

    this.totalToolCalls++;
    if (toolName === 'bash') {
      this.totalBashCalls++;
      this.editsSinceLastBash = 0;
      this.editFailsSinceLastBash = 0;
      this.toolsSinceLastBash = 0;
    } else {
      this.toolsSinceLastBash++;
      if (toolName === 'file_edit' || toolName === 'file_write') {
        this.totalEdits++;
        this.editsSinceLastBash++;
        if (failed) this.editFailsSinceLastBash++;
      }
      if (toolName === 'file_read') {
        this.totalReads++;
      }
    }

    if (failed) {
      this.exactFailures.set(sig, (this.exactFailures.get(sig) || 0) + 1);
      this.evictOldest(this.exactFailures);

      const failCount = this.exactFailures.get(sig) || 0;
      const recovery = this.getRecoveryAction(toolName, args, output);
      if (failCount >= EXACT_FAILURE_BLOCK) {
        const reason = `Tool "${toolName}" has failed ${failCount} times with identical arguments. ${recoveryHint(toolName)} Do NOT retry the same command — change your approach.${recovery ? '\n' + recovery : ''}`;
        return { action: 'block', reason };
      }
      if (failCount >= EXACT_FAILURE_WARN) {
        const reason = `Tool "${toolName}" has failed ${failCount} times with identical arguments. ${recoveryHint(toolName)}${recovery ? '\n' + recovery : ''}`;
        return { action: 'warn', reason };
      }

      // Per-file failure tracking — catches repeated failures on the same file with different args
      if ((toolName === 'file_edit' || toolName === 'file_write') && args.file_path) {
        const fileKey = `${toolName}:${args.file_path}`;
        const fileCount = (this.fileFailures.get(fileKey) || 0) + 1;
        this.fileFailures.set(fileKey, fileCount);
        if (fileCount >= EXACT_FAILURE_BLOCK) {
          return { action: 'block', reason: `${toolName} has failed ${fileCount} times on ${args.file_path}. Try file_write to replace the entire file instead of file_edit.` };
        }
        if (fileCount >= EXACT_FAILURE_WARN + 1) {
          return { action: 'warn', reason: `${toolName} has failed ${fileCount} times on ${args.file_path}. Consider using file_write to rewrite the file entirely.` };
        }
      }
    } else {
      this.exactFailures.delete(sig);
      // Clear per-file failure counter on success
      if (args.file_path) {
        this.fileFailures.delete(`${toolName}:${args.file_path}`);
      }
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

  private getRecoveryAction(toolName: string, _args: Record<string, unknown>, error: string): string {
    if (toolName === 'file_edit' && error.includes('modified since you last read')) {
      return 'Recovery: The file changed after your last read. Use file_write to replace the entire file content instead of file_edit.';
    }
    if (toolName === 'file_edit' && error.includes('not found')) {
      return 'Recovery: Run file_read on the file first to get the current content, then retry the edit.';
    }
    if (toolName === 'file_edit' && error.includes('multiple matches')) {
      return 'Recovery: Add more context to old_string to make it unique, or use replace_all: true.';
    }
    if (toolName === 'bash' && error.includes('command not found')) {
      return 'Recovery: Check the command name. Use tool_search to find the right tool.';
    }
    if (toolName === 'bash' && error.includes('permission denied')) {
      return 'Recovery: Check file permissions. You may need to use a different approach.';
    }
    if (toolName === 'bash' && (error.includes('LaTeX Error') || error.includes('! ') || error.includes('Undefined control sequence'))) {
      return 'Recovery: Use web_search to look up the specific error message before retrying compilation.';
    }
    if (toolName === 'grep' && error.includes('timed out')) {
      return 'Recovery: Use a more specific pattern or narrow the search path.';
    }
    if (toolName === 'web_fetch' && error.includes('SSRF')) {
      return 'Recovery: The URL points to a private/internal address. Use a public URL instead.';
    }
    return '';
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

  getProgress(): ProgressSnapshot {
    return {
      totalToolCalls: this.totalToolCalls,
      totalBashCalls: this.totalBashCalls,
      totalEdits: this.totalEdits,
      totalReads: this.totalReads,
      editsSinceLastBash: this.editsSinceLastBash,
      editFailsSinceLastBash: this.editFailsSinceLastBash,
      toolsSinceLastBash: this.toolsSinceLastBash,
    };
  }

  reset(): void {
    this.exactFailures.clear();
    this.noProgressHistory.clear();
    this.fileFailures.clear();
    this.callHistory = [];
    this.editsSinceLastBash = 0;
    this.editFailsSinceLastBash = 0;
    this.toolsSinceLastBash = 0;
    this.totalToolCalls = 0;
    this.totalBashCalls = 0;
    this.totalEdits = 0;
    this.totalReads = 0;
  }
}
