import * as crypto from 'crypto';

export interface GuardrailDecision {
  action: 'allow' | 'warn' | 'block';
  reason?: string;
}

const IDEMPOTENT_TOOLS = new Set([
  'file_read', 'grep', 'glob', 'list_dir', 'search_symbols',
  'web_search', 'web_fetch',
]);

const FILE_MUTATING_TOOLS = new Set(['file_edit', 'file_write', 'notebook_edit', 'multi_edit']);

const EXACT_FAILURE_WARN = 2;
const EXACT_FAILURE_BLOCK = 4;
const NO_PROGRESS_WARN = 3;
const NO_PROGRESS_BLOCK = 5;

function deepSortKeys(obj: unknown): unknown {
  if (Array.isArray(obj)) return obj.map(deepSortKeys);
  if (obj && typeof obj === 'object') {
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(obj as Record<string, unknown>).sort()) {
      sorted[k] = deepSortKeys((obj as Record<string, unknown>)[k]);
    }
    return sorted;
  }
  return obj;
}

function hashArgs(toolName: string, args: Record<string, unknown>): string {
  const sorted = JSON.stringify(deepSortKeys(args));
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
  private fileEditCounts = new Map<string, number>();
  private editsSinceLastBash = 0;
  private editFailsSinceLastBash = 0;
  private toolsSinceLastBash = 0;
  private bashTestRunsSinceLastEdit = 0;
  private successfulEdits = 0;
  private totalToolCalls = 0;
  private totalBashCalls = 0;
  private totalEdits = 0;
  private totalReads = 0;
  private testFailureSignatures: string[] = [];
  private consecutiveSameFailures = 0;

  constructor(private detachedMode = false) {}

  private evictOldest(map: Map<string, unknown>): void {
    if (map.size > MAX_TRACKED_SIGNATURES) {
      const firstKey = map.keys().next().value;
      if (firstKey !== undefined) map.delete(firstKey);
    }
  }

  beforeCall(toolName: string, args: Record<string, unknown>): GuardrailDecision {
    if (this.detachedMode && toolName === 'web_search' && this.totalReads < 2) {
      return { action: 'warn', reason: 'Read the relevant source files before searching the web. Use file_read to understand the codebase first.' };
    }

    if (this.detachedMode && FILE_MUTATING_TOOLS.has(toolName) && args.file_path) {
      const fp = String(args.file_path);
      const basename = fp.split('/').pop() || '';
      const isTestFile = /(?:^|\/)(tests?|test_[^/]+|[^/]+_test\.py|conftest\.py)(?:\/|$)/.test(fp)
        || /\.test\.[a-z]+$/.test(basename);
      if (isTestFile) {
        return { action: 'block', reason: `BLOCKED: You are editing a test file (${basename}). Do NOT modify test files — fix the source code instead. The test suite validates your fix; changing tests invalidates the evaluation.` };
      }
    }

    if (this.detachedMode && FILE_MUTATING_TOOLS.has(toolName)) {
      if (this.successfulEdits >= 8) {
        return { action: 'block', reason: 'BLOCKED: You have already made 8 successful edits. If the failing tests pass, STOP immediately. Do not make further changes.' };
      }
      if (args.file_path) {
        const editCount = this.fileEditCounts.get(String(args.file_path)) || 0;
        if (editCount >= 5) {
          return { action: 'warn', reason: `You have edited ${args.file_path} ${editCount} times. If the failing tests pass, STOP. If not, check if you are editing the correct file.` };
        }
      }
      if (args.replace_all) {
        return { action: 'warn', reason: 'Prefer targeted edits to specific lines instead of bulk find-and-replace. Only use replace_all when you are sure all occurrences should change.' };
      }
      if (args.old_string && args.new_string) {
        const oldStr = String(args.old_string);
        const newStr = String(args.new_string);
        const oldLines = oldStr.split('\n').length;
        const newLines = newStr.split('\n').length;
        const maxLines = Math.max(oldLines, newLines);
        if (maxLines > 30) {
          return { action: 'block', reason: `BLOCKED: Your edit spans ${maxLines} lines (${oldLines}→${newLines}). Edits must be small and targeted. Find the exact lines that need to change and edit only those. Break large edits into smaller ones.` };
        }
        if (maxLines > 15) {
          return { action: 'warn', reason: `Your edit spans ${maxLines} lines (${oldLines}→${newLines}). Prefer smaller, targeted edits.` };
        }
      }
    }

    if (this.detachedMode && toolName === 'bash' && args.command) {
      const cmd = String(args.command);
      if (/\b(black|autopep8|yapf|isort|prettier|ruff\s+format)\b/.test(cmd)) {
        return { action: 'block', reason: 'BLOCKED: Do not use code formatters. Use file_edit for targeted changes only.' };
      }
      if (/\bsed\s+(-[a-z]*i|-i[a-z]*)\b/.test(cmd)) {
        return { action: 'block', reason: 'BLOCKED: Do not use sed -i to modify files. Use file_edit for targeted changes only.' };
      }
      if (/\b(git\s+apply|git\s+am|patch\s+-|patch\s+<)\b/.test(cmd)) {
        return { action: 'block', reason: 'BLOCKED: Do not use git apply or patch. Use file_edit for targeted changes only.' };
      }
      if (/\bpython3?\s+-c\b/.test(cmd) && /\b(open|write|Path)\b/.test(cmd) && !/tests?\b|runtests|pytest|unittest/.test(cmd)) {
        return { action: 'block', reason: 'BLOCKED: Do not use python -c to write files. Use file_edit for targeted changes only.' };
      }
      if (/\bgit\s+checkout\b/.test(cmd) && !/\bgit\s+checkout\s+-b\b/.test(cmd)) {
        return { action: 'warn', reason: 'Reverting files with git checkout discards your progress. Instead of starting over, diagnose why your fix failed and make a targeted correction.' };
      }
      if (this.bashTestRunsSinceLastEdit >= 5 && /\b(pytest|runtests|unittest|\.test\()\b/.test(cmd)) {
        return { action: 'block', reason: 'BLOCKED: You have run tests 5+ times without making any edits. The result will not change. Make an edit first, then run tests.' };
      }
      if (this.bashTestRunsSinceLastEdit >= 3 && /\b(pytest|runtests|unittest|\.test\()\b/.test(cmd)) {
        return { action: 'warn', reason: 'You have run tests 3+ times without making any edits. Running the same tests again will give the same result. Either make an edit to fix the issue, or try a completely different approach.' };
      }
    }

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
      const cmd = String(args.command || '');
      if (/\b(pytest|runtests|unittest|test)\b/.test(cmd)) {
        this.bashTestRunsSinceLastEdit++;
      }
    } else {
      this.toolsSinceLastBash++;
      if (FILE_MUTATING_TOOLS.has(toolName)) {
        this.totalEdits++;
        this.editsSinceLastBash++;
        this.bashTestRunsSinceLastEdit = 0;
        if (failed) {
          this.editFailsSinceLastBash++;
        } else {
          this.successfulEdits++;
          if (args.file_path) {
            const fp = String(args.file_path);
            this.fileEditCounts.set(fp, (this.fileEditCounts.get(fp) || 0) + 1);
          }
        }
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
      const filePath = _args.file_path ? ` (${_args.file_path})` : '';
      return `Recovery: The old_string you specified does not exist in the file${filePath}. You MUST run file_read on this file to see its actual content before retrying. Do not guess — read first.`;
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

  recordTestFailure(failureSignature: string): { shouldRollback: boolean } {
    if (this.testFailureSignatures.length > 0 &&
        this.testFailureSignatures[this.testFailureSignatures.length - 1] === failureSignature) {
      this.consecutiveSameFailures++;
    } else {
      this.consecutiveSameFailures = 1;
    }
    this.testFailureSignatures.push(failureSignature);
    if (this.testFailureSignatures.length > 10) this.testFailureSignatures.shift();
    return { shouldRollback: this.consecutiveSameFailures >= 3 };
  }

  resetTestFailures(): void {
    this.consecutiveSameFailures = 0;
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
    this.fileEditCounts.clear();
    this.callHistory = [];
    this.editsSinceLastBash = 0;
    this.editFailsSinceLastBash = 0;
    this.toolsSinceLastBash = 0;
    this.bashTestRunsSinceLastEdit = 0;
    this.successfulEdits = 0;
    this.totalToolCalls = 0;
    this.totalBashCalls = 0;
    this.totalEdits = 0;
    this.totalReads = 0;
    this.testFailureSignatures = [];
    this.consecutiveSameFailures = 0;
  }
}
