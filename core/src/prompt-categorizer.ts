export type Intent = 'explain' | 'troubleshoot' | 'generate' | 'refactor' | 'review' | 'git_ops' | 'research' | 'other';
export type Scope = 'selection' | 'current_file' | 'few_files' | 'codebase';

export interface PromptCategory {
  intent: Intent;
  scope: Scope;
}

const INTENT_PATTERNS: Array<{ pattern: RegExp; intent: Intent }> = [
  { pattern: /\b(explain|what does|how does|why|understand)\b/i, intent: 'explain' },
  { pattern: /\b(fix|bug|error|broken|crash|fail|not working)\b/i, intent: 'troubleshoot' },
  { pattern: /\b(create|generate|write|build|implement|add|new)\b/i, intent: 'generate' },
  { pattern: /\b(refactor|clean|improve|optimize|simplify)\b/i, intent: 'refactor' },
  { pattern: /\b(review|check|audit|evaluate|assess)\b/i, intent: 'review' },
  { pattern: /\b(commit|push|pull|merge|branch|rebase|git)\b/i, intent: 'git_ops' },
  { pattern: /\b(research|analyze|compare|study|investigate|paper)\b/i, intent: 'research' },
];

const SCOPE_PATTERNS: Array<{ pattern: RegExp; scope: Scope }> = [
  { pattern: /\b(this function|this method|this block|selected|the code above)\b/i, scope: 'selection' },
  { pattern: /\b(this file|current file)\b/i, scope: 'current_file' },
  { pattern: /\b(these files|both files|all files in)\b/i, scope: 'few_files' },
  { pattern: /\b(codebase|project|repo|repository|everywhere|all)\b/i, scope: 'codebase' },
];

export function categorizePrompt(userMessage: string): PromptCategory {
  let intent: Intent = 'other';
  for (const { pattern, intent: i } of INTENT_PATTERNS) {
    if (pattern.test(userMessage)) { intent = i; break; }
  }

  let scope: Scope = 'codebase';
  for (const { pattern, scope: s } of SCOPE_PATTERNS) {
    if (pattern.test(userMessage)) { scope = s; break; }
  }

  return { intent, scope };
}
