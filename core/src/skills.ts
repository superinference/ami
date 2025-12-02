import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { parseFrontmatter } from './memory';

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
  argumentHint?: string;  // Hint text for the argument (e.g., "<file-path>")
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
}

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
 * Parse a JSON-like array from a frontmatter value.
 * Accepts: `["*.ts", "*.py"]` or `[file_read, file_write]` (unquoted items).
 */
function parseStringArray(raw: string | undefined): string[] | undefined {
  if (!raw) return undefined;
  const trimmed = raw.trim();
  if (!trimmed.startsWith('[') || !trimmed.endsWith(']')) return undefined;
  const inner = trimmed.slice(1, -1).trim();
  if (!inner) return [];
  return inner.split(',').map(s => s.trim().replace(/^["']|["']$/g, ''));
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
        regex += '.*';
        i += 2;
        // Skip trailing /
        if (normPattern[i] === '/') i++;
        continue;
      }
      // * matches anything except /
      regex += '[^/]*';
    } else if (ch === '?') {
      regex += '[^/]';
    } else if (ch === '.') {
      regex += '\\.';
    } else {
      regex += ch;
    }
    i++;
  }

  return new RegExp(`^${regex}$`).test(normPath) ||
    new RegExp(`(^|/)${regex}$`).test(normPath);
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

    // Apply user-supplied args
    if (args) {
      // $ARGUMENTS — the full argument string
      if (args.ARGUMENTS !== undefined) {
        content = content.replace(/\$ARGUMENTS/g, args.ARGUMENTS);
        content = content.replace(/\$\{ARGUMENTS\}/g, args.ARGUMENTS);
        // Positional: $0 = full, $1-$9 = tokens
        const tokens = args.ARGUMENTS.split(/\s+/).filter(Boolean);
        content = content.replace(/\$0/g, args.ARGUMENTS);
        for (let i = 1; i <= 9; i++) {
          content = content.replace(new RegExp(`\\$${i}`, 'g'), tokens[i - 1] || '');
        }
      }
      for (const [key, value] of Object.entries(args)) {
        if (key === 'ARGUMENTS') continue;
        content = content.replace(new RegExp(`\\$\\{${key}\\}`, 'g'), value);
      }
    }

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
    // 1. Built-in skills (lowest priority — overridable)
    for (const skill of BUILTIN_SKILLS) {
      this.skills.set(skill.name, skill);
    }

    // 2. User-global skills and agents (~/.superinference/)
    const userDir = path.join(os.homedir(), '.superinference');
    this.loadSkillsDir(path.join(userDir, 'skills'));
    this.loadAgentsDir(path.join(userDir, 'agents'));

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
    const name = frontmatter.name || path.basename(path.dirname(filePath));
    if (!name || !body.trim()) return null;

    const userInvocableRaw = frontmatter['user-invocable'];
    let userInvocable: boolean | undefined;
    if (userInvocableRaw === 'false') userInvocable = false;
    else if (userInvocableRaw === 'true') userInvocable = true;

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

    return {
      name,
      description: frontmatter.description || '',
      systemPrompt: body.trim(),
      filePath,
      model: frontmatter.model || undefined,
      maxTurns: maxTurns && !isNaN(maxTurns) ? maxTurns : undefined,
      tools: parseStringArray(frontmatter.tools),
      disallowedTools: parseStringArray(frontmatter['disallowed-tools']),
    };
  }
}
