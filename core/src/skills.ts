import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execSync } from 'child_process';
import { parseFrontmatter } from './memory';
import { discoverPlugins } from './plugins';

/**
 * Expand inline live-command injections of the form !`cmd` in a (trusted,
 * project-authored) skill/command body, substituting the command's stdout. Lets
 * a packaged skill embed live repo state (e.g. !`git diff --staged`). Failures
 * become a visible [error: ...] marker rather than throwing.
 */
function substituteBashInjections(content: string, cwd: string): string {
  return content.replace(/!`([^`]+)`/g, (_m, cmd) => {
    try {
      return execSync(cmd, { cwd, timeout: 10_000, encoding: 'utf-8', stdio: ['ignore', 'pipe', 'pipe'], maxBuffer: 1_000_000 }).trim();
    } catch (err) {
      return `[error: ${err instanceof Error ? err.message.split('\n')[0] : String(err)}]`;
    }
  });
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface SkillDefinition {
  name: string;
  description: string;
  content: string;        // Markdown body (the skill instructions)
  filePath: string;       // Where the skill file lives ('<builtin>' for built-ins)
  whenToUse?: string;     // Hint for auto-activation
  paths?: string[];       // File glob patterns that auto-activate this skill
  model?: string;         // Override model for this skill
  userInvocable?: boolean; // Can user invoke via /skill-name (default true)
  allowedTools?: string[]; // Restrict which tools this skill can use
  disallowedTools?: string[]; // Tools to exclude (blacklist, applied after allowedTools)
  argumentHint?: string;  // Hint text for the argument (e.g., "<file-path>")
  context?: 'inline' | 'fork'; // Execution mode: inline injects as system message, fork spawns subagent
  disableModelInvocation?: boolean; // If true, LLM cannot invoke this skill via the skill tool
  effort?: string;         // Thinking effort level for sub-engine (e.g., "low", "medium", "high")
}

export interface AgentDefinition {
  name: string;
  description: string;
  systemPrompt: string;   // The agent's system prompt (markdown body)
  filePath: string;
  model?: string;
  maxTurns?: number;
  tools?: string[];       // Allowed tool names (empty = all)
  disallowedTools?: string[];
  /** Permission mode override for sub-agent invocation. */
  permissionMode?: 'auto-allow' | 'ask' | 'deny-all';
  /** Token budget for auto-compaction inside this subagent's context window. */
  autoCompactWindow?: number;
}

// ---------------------------------------------------------------------------
// Built-in agents — always available, overridable by user/project definitions
// ---------------------------------------------------------------------------

const BUILTIN_AGENTS: AgentDefinition[] = [
  {
    name: 'verifier',
    description: 'Adversarial behavioral verification — prove a fix works by writing and running targeted tests',
    filePath: '<builtin>',
    permissionMode: 'auto-allow',
    tools: ['bash', 'file_read', 'grep', 'glob'],
    maxTurns: 15,
    systemPrompt: [
      'You are an adversarial verification agent. Your sole job: determine whether a specific behavior works correctly.',
      '',
      'Your stance is skeptical — assume the fix might be wrong and actively try to prove it.',
      '',
      '## Rules',
      '- NEVER modify project source files or test files.',
      '- Write all test scripts to /tmp only — they are ephemeral.',
      '- Read source code FIRST to get correct import paths, types, and signatures before writing any test.',
      '- Test at least the described case PLUS one edge case (empty input, boundary value, wrong type, etc.).',
      '',
      '## Process',
      '1. Read the relevant source file(s) to understand the actual API.',
      '2. Write a minimal test script to /tmp that calls the function and asserts the expected output.',
      '   Use the language\'s assertion mechanism: panic() in Go, assert in Python, assert_eq! in Rust, throw in JS.',
      '3. Run it — exit 0 = behavior correct, non-zero = behavior wrong.',
      '4. If it passes, run 1-2 edge cases to confirm robustness.',
      '5. Return EXACTLY one of:',
      '   VERIFIED: <what you tested, inputs, all passed>',
      '   FAILED: <exact error output, which assertion failed>',
      '',
      'Do not explain what you are about to do. Execute immediately.',
    ].join('\n'),
  },
  {
    name: 'code-graph',
    description: 'Code structure analysis — find related tests, callers, git history, and static issues for any file or function',
    filePath: '<builtin>',
    permissionMode: 'auto-allow',
    tools: ['bash', 'file_read', 'grep', 'glob'],
    maxTurns: 10,
    systemPrompt: [
      'You are a code structure analysis agent. Answer questions about code relationships.',
      '',
      '## Capabilities',
      '- Find test files that cover a source file (co-location + import analysis)',
      '- Find all callers of a function (grep + context filtering)',
      '- Show git history for a file or directory (git log, blame, diff via bash)',
      '- Detect static issues (run go vet, mypy, cargo clippy, eslint via bash)',
      '- Map import/dependency relationships between modules',
      '',
      '## Process',
      '- Use grep, glob, file_read, and bash to explore the codebase.',
      '- For test discovery: check co-located test files first, then grep imports.',
      '- For call graphs: grep for the function name, filter false positives by context.',
      '- For git history: bash with git log/blame/diff commands.',
      '- For static analysis: detect language from project files, run appropriate tool.',
      '',
      '## Output format',
      'Return structured findings:',
      '  Related tests: <files> | Run: <exact command>',
      '  Callers: <list of file:line references>',
      '  Recent changes: <git log summary>',
      '  Static issues: <linter output, or "none found">',
      '',
      'Be concise. Return only what was asked. Do not propose fixes.',
    ].join('\n'),
  },
];

// ---------------------------------------------------------------------------
// Built-in skills — defined inline, no physical files needed
// ---------------------------------------------------------------------------

const BUILTIN_SKILLS: SkillDefinition[] = [
  {
    name: 'code-review',
    description: 'Review code for bugs, security issues, and improvements',
    content: [
      'Review the provided code thoroughly. Check for:',
      '1. Bugs and logic errors',
      '2. Security vulnerabilities',
      '3. Performance issues',
      '4. Code style and readability',
      '5. Missing error handling',
      '',
      'Provide specific line references and suggested fixes.',
    ].join('\n'),
    filePath: '<builtin>',
    userInvocable: true,
  },
  {
    name: 'explain',
    description: 'Explain code in detail',
    content: [
      'Explain the provided code clearly and thoroughly:',
      '1. High-level purpose and architecture',
      '2. Key data structures and algorithms',
      '3. Control flow and edge cases',
      '4. External dependencies and their roles',
      '5. Any non-obvious design decisions',
      '',
      'Use simple language. Reference specific functions, variables, and line numbers.',
    ].join('\n'),
    filePath: '<builtin>',
    userInvocable: true,
  },
  {
    name: 'refactor',
    description: 'Suggest and apply refactoring improvements',
    content: [
      'Analyze the provided code for refactoring opportunities:',
      '1. Extract repeated logic into functions',
      '2. Simplify complex conditionals',
      '3. Improve naming for clarity',
      '4. Reduce coupling between modules',
      '5. Apply relevant design patterns',
      '',
      'Show before/after for each suggestion. Preserve existing behavior.',
    ].join('\n'),
    filePath: '<builtin>',
    userInvocable: true,
  },
  {
    name: 'test-gen',
    description: 'Generate tests for the given code',
    content: [
      'Generate comprehensive tests for the provided code:',
      '1. Happy-path tests for normal usage',
      '2. Edge cases (empty input, boundary values, null/undefined)',
      '3. Error cases and exception handling',
      '4. Integration tests if the code interacts with external systems',
      '',
      'Follow the existing test style and framework in the project.',
      'Use descriptive test names that explain the expected behavior.',
    ].join('\n'),
    filePath: '<builtin>',
    userInvocable: true,
  },
];

/**
 * Parse a string list from a frontmatter value. Accepts both the bracketed
 * JSON-style form (`["*.ts", "*.py"]` / `[file_read, file_write]`) and Claude
 * Code's bare comma-separated form (`Glob, Grep, Read`).
 */
function parseStringArray(raw: string | undefined): string[] | undefined {
  if (!raw) return undefined;
  const trimmed = raw.trim();
  const inner = (trimmed.startsWith('[') && trimmed.endsWith(']'))
    ? trimmed.slice(1, -1).trim()
    : trimmed;
  if (!inner) return [];
  return inner
    .split(',')
    .map(s => s.trim().replace(/^["']|["']$/g, ''))
    .filter(s => s.length > 0);
}

/**
 * Simple glob pattern matching.
 * Supports `*` (any non-separator chars) and `**` (any path).
 * Used to test whether a file path matches a skill's `paths` patterns.
 */
function matchGlob(pattern: string, filePath: string): boolean {
  // Normalise to forward slashes
  const normPath = filePath.replace(/\\/g, '/');
  const normPattern = pattern.replace(/\\/g, '/');

  // Convert glob pattern to regex
  let regex = '';
  let i = 0;
  while (i < normPattern.length) {
    const ch = normPattern[i];
    if (ch === '*') {
      if (normPattern[i + 1] === '*') {
        // ** matches any number of path segments
        regex += '(?:.*/)?';
        i += 2;
        // Skip trailing /
        if (normPattern[i] === '/') i++;
        continue;
      }
      // * matches anything except /
      regex += '[^/]*';
    } else if (ch === '?') {
      regex += '[^/]';
    } else if ('.+^${}()|[]\\'.includes(ch)) {
      regex += '\\' + ch;
    } else {
      regex += ch;
    }
    i++;
  }

  return new RegExp(`^${regex}$`).test(normPath) ||
    new RegExp(`(^|/)${regex}$`).test(normPath);
}

// ---------------------------------------------------------------------------
// Skill argument safety — prevent memory exhaustion from oversized args
// ---------------------------------------------------------------------------

export const MAX_SKILL_ARG_NAMES = 64;
export const MAX_SKILL_ARG_NAME_LENGTH = 64;
export const MAX_SKILL_ARG_VALUE_LENGTH = 10_000;
export const MAX_SKILL_ARG_TOTAL_CHARS = 100_000;

export function sanitizeSkillArgs(args: Record<string, string>): Record<string, string> {
  const keys = Object.keys(args);
  const limitedKeys = keys.slice(0, MAX_SKILL_ARG_NAMES);
  const result: Record<string, string> = {};
  let totalChars = 0;

  for (const rawKey of limitedKeys) {
    const key = rawKey.length > MAX_SKILL_ARG_NAME_LENGTH
      ? rawKey.slice(0, MAX_SKILL_ARG_NAME_LENGTH)
      : rawKey;
    let value = typeof args[rawKey] === 'string' ? args[rawKey] : String(args[rawKey]);
    if (value.length > MAX_SKILL_ARG_VALUE_LENGTH) {
      value = value.slice(0, MAX_SKILL_ARG_VALUE_LENGTH);
    }
    if (totalChars + key.length + value.length > MAX_SKILL_ARG_TOTAL_CHARS) {
      break;
    }
    totalChars += key.length + value.length;
    result[key] = value;
  }

  return result;
}

export function validateSkillArgs(args: Record<string, string>): { valid: boolean; warnings: string[] } {
  const warnings: string[] = [];
  const keys = Object.keys(args);

  if (keys.length > MAX_SKILL_ARG_NAMES) {
    warnings.push(`Too many arguments: ${keys.length} (max ${MAX_SKILL_ARG_NAMES})`);
  }

  let totalChars = 0;
  for (const key of keys) {
    if (key.length > MAX_SKILL_ARG_NAME_LENGTH) {
      warnings.push(`Argument name too long: "${key.slice(0, 20)}..." (${key.length} chars, max ${MAX_SKILL_ARG_NAME_LENGTH})`);
    }
    const value = typeof args[key] === 'string' ? args[key] : String(args[key]);
    if (value.length > MAX_SKILL_ARG_VALUE_LENGTH) {
      warnings.push(`Argument value too long for "${key.slice(0, 20)}": ${value.length} chars (max ${MAX_SKILL_ARG_VALUE_LENGTH})`);
    }
    totalChars += key.length + value.length;
  }

  if (totalChars > MAX_SKILL_ARG_TOTAL_CHARS) {
    warnings.push(`Total argument size ${totalChars} exceeds limit ${MAX_SKILL_ARG_TOTAL_CHARS}`);
  }

  return { valid: true, warnings };
}

// ---------------------------------------------------------------------------
// Variable substitution
// ---------------------------------------------------------------------------

function substituteVars(
  content: string,
  skillDir: string,
  cwd: string,
  model?: string,
): string {
  const date = new Date().toISOString().split('T')[0];
  return content
    .replace(/\$\{SKILL_DIR\}/g, skillDir)
    .replace(/\$\{CWD\}/g, cwd)
    .replace(/\$\{MODEL\}/g, model ?? 'default')
    .replace(/\$\{DATE\}/g, date);
}

// ---------------------------------------------------------------------------
// SkillManager
// ---------------------------------------------------------------------------

export class SkillManager {
  private skills: Map<string, SkillDefinition> = new Map();
  private agents: Map<string, AgentDefinition> = new Map();
  private cwd: string;

  constructor(cwd: string) {
    this.cwd = cwd;
    this.loadAll();
  }

  // ---- Public API ----------------------------------------------------------

  /** Get a skill by name. */
  getSkill(name: string): SkillDefinition | null {
    return this.skills.get(name) ?? null;
  }

  /** Get all user-invocable skills. */
  listSkills(): SkillDefinition[] {
    return Array.from(this.skills.values()).filter(
      s => s.userInvocable !== false,
    );
  }

  /** Get an agent by name. */
  getAgent(name: string): AgentDefinition | null {
    return this.agents.get(name) ?? null;
  }

  /** List all agents. */
  listAgents(): AgentDefinition[] {
    return Array.from(this.agents.values());
  }

  /**
   * Find skills that should activate for given file paths.
   * Matches each skill's `paths` globs against the provided file paths.
   */
  findMatchingSkills(filePaths: string[]): SkillDefinition[] {
    const matched: SkillDefinition[] = [];
    for (const skill of this.skills.values()) {
      if (!skill.paths || skill.paths.length === 0) continue;
      const matches = filePaths.some(fp =>
        skill.paths!.some(pattern => matchGlob(pattern, fp)),
      );
      if (matches) {
        matched.push(skill);
      }
    }
    return matched;
  }

  /**
   * Get skill content with variable substitution.
   * Accepts optional `args` for future user-supplied variables.
   */
  getSkillContent(
    name: string,
    args?: Record<string, string>,
  ): string | null {
    const skill = this.skills.get(name);
    if (!skill) return null;

    const skillDir =
      skill.filePath === '<builtin>'
        ? this.cwd
        : path.dirname(skill.filePath);

    let content = substituteVars(skill.content, skillDir, this.cwd, skill.model);

    // Apply user-supplied args (sanitized to prevent memory exhaustion)
    if (args) {
      const safeArgs = sanitizeSkillArgs(args);
      // $ARGUMENTS — the full argument string
      if (safeArgs.ARGUMENTS !== undefined) {
        content = content.replace(/\$ARGUMENTS/g, () => safeArgs.ARGUMENTS);
        content = content.replace(/\$\{ARGUMENTS\}/g, () => safeArgs.ARGUMENTS);
        // Positional: $0 = full, $1-$9 = tokens
        const tokens = safeArgs.ARGUMENTS.split(/\s+/).filter(Boolean);
        content = content.replace(/\$0/g, () => safeArgs.ARGUMENTS);
        for (let i = 1; i <= 9; i++) {
          const val = tokens[i - 1] || '';
          content = content.replace(new RegExp(`\\$${i}`, 'g'), () => val);
        }
      }
      for (const [key, value] of Object.entries(safeArgs)) {
        if (key === 'ARGUMENTS') continue;
        content = content.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), () => value);
      }
    }

    // Live command injection last, so $VARS/$ARGUMENTS can feed the commands.
    content = substituteBashInjections(content, skillDir);

    return content;
  }

  /**
   * Load AGENTS.md from workspace root (Hermes-style project agent instructions).
   * Returns the file content if found, empty string otherwise.
   */
  loadAgentsFile(): string {
    const agentsPath = path.join(this.cwd, 'AGENTS.md');
    try {
      if (fs.statSync(agentsPath).isFile()) {
        return fs.readFileSync(agentsPath, 'utf-8').trim();
      }
    } catch {
      // File doesn't exist
    }
    return '';
  }

  /**
   * Get all skill/agent context formatted for system prompt injection.
   * Returns empty string if no skills, agents, or AGENTS.md exist.
   */
  getSkillContext(): string {
    const sections: string[] = [];

    // AGENTS.md content (Hermes-style)
    const agentsContent = this.loadAgentsFile();
    if (agentsContent) {
      sections.push(`## AGENTS.md\n\n${agentsContent}`);
    }

    // Available skills
    const invocableSkills = this.listSkills();
    if (invocableSkills.length > 0) {
      const skillLines = invocableSkills.map(s => {
        const parts = [`- **/${s.name}** — ${s.description}`];
        if (s.whenToUse) {
          parts.push(`  When: ${s.whenToUse}`);
        }
        return parts.join('\n');
      });
      sections.push(`## Available Skills\n\n${skillLines.join('\n')}`);
    }

    // Available agents
    const agentList = this.listAgents();
    if (agentList.length > 0) {
      const agentLines = agentList.map(a => {
        const parts = [`- **${a.name}** — ${a.description}`];
        if (a.model) parts.push(`  Model: ${a.model}`);
        if (a.maxTurns) parts.push(`  Max turns: ${a.maxTurns}`);
        return parts.join('\n');
      });
      sections.push(`## Available Agents\n\n${agentLines.join('\n')}`);
    }

    return sections.join('\n\n');
  }

  // ---- Internal loading ----------------------------------------------------

  /** Load everything: built-ins, project, user-global, AGENTS.md. */
  private loadAll(): void {
    // 1. Built-in agents (lowest priority — overridable by user/project)
    for (const agent of BUILTIN_AGENTS) {
      this.agents.set(agent.name, agent);
    }

    // 2. Built-in skills (lowest priority — overridable)
    for (const skill of BUILTIN_SKILLS) {
      this.skills.set(skill.name, skill);
    }

    // 2. User-global skills and agents (~/.superinference/)
    const userDir = path.join(os.homedir(), '.superinference');
    this.loadSkillsDir(path.join(userDir, 'skills'));
    this.loadAgentsDir(path.join(userDir, 'agents'));

    // 2b. Plugin bundles (~/.superinference/plugins and .superinference/plugins) —
    // below project so project overrides, above built-ins.
    for (const pluginsDir of [path.join(userDir, 'plugins'), path.join(this.cwd, '.superinference', 'plugins')]) {
      for (const bundle of discoverPlugins(pluginsDir)) {
        if (bundle.skillsDir) this.loadSkillsDir(bundle.skillsDir);
        if (bundle.agentsDir) this.loadAgentsDir(bundle.agentsDir);
      }
    }

    // 3. Project skills and agents (.superinference/ in cwd) — highest priority
    this.loadSkillsDir(path.join(this.cwd, '.superinference', 'skills'));
    this.loadAgentsDir(path.join(this.cwd, '.superinference', 'agents'));
  }

  /** Load skill definitions from a directory of SKILL.md files. */
  private loadSkillsDir(dir: string): void {
    let entries: string[];
    try {
      entries = fs.readdirSync(dir);
    } catch {
      return; // Directory doesn't exist
    }

    for (const entry of entries) {
      const entryPath = path.join(dir, entry);
      try {
        const stat = fs.statSync(entryPath);
        if (stat.isDirectory()) {
          // Look for SKILL.md inside the subdirectory
          const skillFile = path.join(entryPath, 'SKILL.md');
          const parsed = this.parseSkillFile(skillFile);
          if (parsed) {
            this.skills.set(parsed.name, parsed);
          }
        } else if (stat.isFile() && entry.endsWith('.md')) {
          // Direct .md file in the skills directory
          const parsed = this.parseSkillFile(entryPath);
          if (parsed) {
            this.skills.set(parsed.name, parsed);
          }
        }
      } catch {
        // Skip unreadable entries
      }
    }
  }

  /** Load agent definitions from a directory of agent .md files. */
  private loadAgentsDir(dir: string): void {
    let entries: string[];
    try {
      entries = fs.readdirSync(dir).filter(f => f.endsWith('.md'));
    } catch {
      return; // Directory doesn't exist
    }

    for (const file of entries) {
      const filePath = path.join(dir, file);
      const parsed = this.parseAgentFile(filePath);
      if (parsed) {
        this.agents.set(parsed.name, parsed);
      }
    }
  }

  /** Parse a skill file (YAML frontmatter + markdown body). */
  private parseSkillFile(filePath: string): SkillDefinition | null {
    let raw: string;
    try {
      raw = fs.readFileSync(filePath, 'utf-8');
    } catch {
      return null;
    }

    const { frontmatter, body } = parseFrontmatter(raw);
    const basename = path.basename(filePath, path.extname(filePath));
    const name = frontmatter.name || (basename === 'SKILL' ? path.basename(path.dirname(filePath)) : basename);
    if (!name || !body.trim()) return null;

    const userInvocableRaw = frontmatter['user-invocable'];
    let userInvocable: boolean | undefined;
    if (userInvocableRaw === 'false') userInvocable = false;
    else if (userInvocableRaw === 'true') userInvocable = true;

    const contextRaw = frontmatter.context;
    const context: SkillDefinition['context'] =
      contextRaw === 'fork' ? 'fork' : contextRaw === 'inline' ? 'inline' : undefined;

    return {
      name,
      description: frontmatter.description || '',
      content: body.trim(),
      filePath,
      whenToUse: frontmatter['when-to-use'] || undefined,
      paths: parseStringArray(frontmatter.paths),
      model: frontmatter.model || undefined,
      userInvocable,
      allowedTools: parseStringArray(frontmatter['allowed-tools']),
      disallowedTools: parseStringArray(frontmatter['disallowed-tools']),
      argumentHint: frontmatter['argument-hint'] || undefined,
      context,
      disableModelInvocation: frontmatter['disable-model-invocation'] === 'true',
      effort: frontmatter.effort || undefined,
    };
  }

  /** Parse an agent definition file (YAML frontmatter + markdown body). */
  private parseAgentFile(filePath: string): AgentDefinition | null {
    let raw: string;
    try {
      raw = fs.readFileSync(filePath, 'utf-8');
    } catch {
      return null;
    }

    const { frontmatter, body } = parseFrontmatter(raw);
    const name = frontmatter.name || path.basename(filePath, '.md');
    if (!name || !body.trim()) return null;

    const maxTurnsRaw = frontmatter['max-turns'];
    const maxTurns = maxTurnsRaw ? parseInt(maxTurnsRaw, 10) : undefined;

    const pm = frontmatter['permission-mode'];
    const permissionMode = (pm === 'auto-allow' || pm === 'ask' || pm === 'deny-all') ? pm : undefined;

    const compactRaw = frontmatter['auto-compact-window'];
    const autoCompactWindow = compactRaw ? parseInt(compactRaw, 10) : undefined;

    return {
      name,
      description: frontmatter.description || '',
      systemPrompt: body.trim(),
      filePath,
      model: frontmatter.model || undefined,
      maxTurns: maxTurns && !isNaN(maxTurns) ? maxTurns : undefined,
      tools: parseStringArray(frontmatter.tools),
      disallowedTools: parseStringArray(frontmatter['disallowed-tools']),
      permissionMode,
      autoCompactWindow: autoCompactWindow && !isNaN(autoCompactWindow) ? autoCompactWindow : undefined,
    };
  }
}
