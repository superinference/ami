import * as fs from 'fs';
import * as fsp from 'fs/promises';
import * as path from 'path';
import type { SessionFact } from './session-memory';
import { log as coreLog } from './logger';

/**
 * Memory types for persistent knowledge across sessions.
 * - user: info about the user's role, goals, preferences
 * - feedback: guidance on how to approach work (corrections + confirmations)
 * - project: ongoing work context not derivable from code/git
 * - reference: pointers to external systems and resources
 */
export const MEMORY_TYPES = ['user', 'feedback', 'project', 'reference'] as const;
export type MemoryType = (typeof MEMORY_TYPES)[number];

export interface MemoryEntry {
  path: string;
  content: string;
  type: 'instructions' | 'memory';
  name?: string;
  description?: string;
  memoryType?: MemoryType;
  ageDays?: number;
  freshness?: string;
  mtimeMs?: number;
}

/**
 * Instruction source with level differentiation.
 * - managed: from .superinference/rules/ directory (team-managed rules)
 * - user: from memory/ or user-specific files (CLAUDE.local.md, SUPERINFERENCE.local.md)
 * - project: from project instruction files (CLAUDE.md, SUPERINFERENCE.md, etc.)
 */
export interface InstructionSource {
  content: string;
  level: 'managed' | 'user' | 'project';
  path: string;
}

/**
 * Files to look for as project instructions, in priority order.
 * The first one found wins for each directory level.
 */
const INSTRUCTION_FILES = [
  'CLAUDE.md',
  'AGENTS.md',
  'SUPERINFERENCE.md',
  'GEMINI.md',
  'CRUSH.md',
  '.cursorrules',
  '.cursor/rules',
  '.github/copilot-instructions.md',
] as const;

const LOCAL_VARIANTS = [
  'CLAUDE.local.md',
  'SUPERINFERENCE.local.md',
] as const;

/** Maximum number of parent directories to scan upward. */
const MAX_PARENT_SCAN_DEPTH = 3;

/** Maximum size for instruction files — reject oversized files to prevent context stuffing. */
const MAX_INSTRUCTION_FILE_SIZE = 10_240;

const INJECTION_PATTERNS = [
  /ignore .{0,10}previous instructions/i,
  /you are now /i,
  /new instructions?:/i,
  /\[SYSTEM\]/i,
  /override safety/i,
  /disregard .{0,10}(safety|rules|instructions)/i,
  /pretend you are/i,
  /from now on.{0,3}you/i,
];

/** Strip zero-width characters and apply NFKD normalization to defeat homoglyph/invisible-char bypasses. */
function normalizeForScanning(text: string): string {
  // Strip all Unicode format (Cf), control (Cc), and nonspacing mark (Mn) characters
  // This catches RTL overrides, null bytes, zero-width chars, combining marks
  // eslint-disable-next-line security/detect-bidi-characters
  let normalized = text.replace(/[--­؀-؅؜۝܏࣢᠎​-‏‪-‮⁠-⁤⁦-⁯︀-️﻿￰-￸￾￿]/g, '');
  // NFKD normalization — decomposes ligatures and compatibility characters
  normalized = normalized.normalize('NFKD');
  return normalized;
}

function sanitizeContent(content: string, filePath: string): string {
  if (content.length > MAX_INSTRUCTION_FILE_SIZE) {
    coreLog('memory', `Instruction file ${filePath} exceeds ${MAX_INSTRUCTION_FILE_SIZE} bytes — truncating`);
    content = content.slice(0, MAX_INSTRUCTION_FILE_SIZE);
  }

  const lines = content.split('\n');
  const clean: string[] = [];
  let injectionFound = false;
  for (const line of lines) {
    const normalized = normalizeForScanning(line);
    if (INJECTION_PATTERNS.some(p => p.test(normalized))) {
      injectionFound = true;
      clean.push(`[BLOCKED: potential prompt injection removed]`);
    } else {
      clean.push(line);
    }
  }
  if (injectionFound) {
    coreLog('memory', `Prompt injection patterns detected in ${filePath}`);
  }
  return clean.join('\n');
}

/**
 * Check whether a file path matches any of the given glob patterns from a rule's
 * `paths:` frontmatter. Supports `*` (any characters) and `?` (single character).
 */
export function matchesPaths(pathGlobs: string[], currentFile: string): boolean {
  for (const glob of pathGlobs) {
    // Escape regex special chars except * and ?, then convert glob wildcards
    let escaped = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&');
    escaped = escaped.replace(/\*\*/g, '⧫⧫');
    const pattern = escaped.replace(/\*/g, '[^/]*').replace(/⧫⧫/g, '.*').replace(/\?/g, '[^/]');
    if (new RegExp(`^${pattern}$`).test(currentFile)) return true;
  }
  return false;
}

/**
 * Parse a multi-value frontmatter field (comma-separated or YAML list).
 * Handles both `paths: src/*.ts, lib/*.ts` and multi-line YAML arrays.
 */
function parseFrontmatterList(value: string): string[] {
  if (!value) return [];
  return value.split(',').map(s => s.trim()).filter(s => s.length > 0);
}

/** Directory name for SuperInference memory storage. */
const MEMORY_DIR = '.superinference/memory';

const STALE_MEMORY_DAYS = 30;

function memoryAgeDays(mtimeMs: number): number {
  return Math.floor((Date.now() - mtimeMs) / (1000 * 60 * 60 * 24));
}

function memoryFreshness(ageDays: number): string {
  if (ageDays === 0) return 'today';
  if (ageDays === 1) return 'yesterday';
  if (ageDays <= 7) return `${ageDays} days ago`;
  if (ageDays <= 30) return `${Math.floor(ageDays / 7)} weeks ago`;
  return `${Math.floor(ageDays / 30)} months ago`;
}

/**
 * Parse YAML-like frontmatter from a markdown string.
 * Returns the extracted fields and the body after the frontmatter block.
 */
export function parseFrontmatter(raw: string): {
  frontmatter: Record<string, string>;
  body: string;
} {
  const trimmed = raw.trimStart();
  if (!trimmed.startsWith('---')) {
    return { frontmatter: {}, body: raw };
  }

  const endIndex = trimmed.indexOf('---', 3);
  if (endIndex === -1) {
    return { frontmatter: {}, body: raw };
  }

  const fmBlock = trimmed.slice(3, endIndex).trim();
  const body = trimmed.slice(endIndex + 3).replace(/^\r?\n/, '');

  const frontmatter: Record<string, string> = {};
  for (const line of fmBlock.split('\n')) {
    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) continue;
    const key = line.slice(0, colonIdx).trim();
    const value = line.slice(colonIdx + 1).trim();
    if (key) {
      frontmatter[key] = value;
    }
  }

  return { frontmatter, body };
}

/**
 * Validate a raw string as a MemoryType. Returns undefined for invalid values.
 */
function parseMemoryType(raw: unknown): MemoryType | undefined {
  if (typeof raw !== 'string') return undefined;
  return MEMORY_TYPES.find(t => t === raw);
}

/**
 * Generate frontmatter markdown for a memory file.
 */
function buildFrontmatter(
  name: string,
  description: string,
  memoryType: MemoryType,
): string {
  return `---\nname: ${name}\ndescription: ${description}\ntype: ${memoryType}\n---\n`;
}

function tokenize(text: string): string[] {
  return text.toLowerCase()
    .replace(/[^a-z0-9\s_-]/g, ' ')
    .split(/\s+/)
    .filter(t => t.length > 2);
}

function computeRelevanceScore(memoryText: string, queryTokens: string[], idfMap: Map<string, number>): number {
  const memTokens = tokenize(memoryText);
  if (memTokens.length === 0) return 0;

  const tf = new Map<string, number>();
  for (const t of memTokens) {
    tf.set(t, (tf.get(t) || 0) + 1);
  }

  let score = 0;
  const k1 = 1.2;
  const b = 0.75;
  const avgDl = 50;

  for (const qt of queryTokens) {
    const freq = tf.get(qt) || 0;
    if (freq === 0) continue;
    const idf = idfMap.get(qt) || 1;
    const tfNorm = (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * memTokens.length / avgDl));
    score += idf * tfNorm;
  }

  return score;
}

export class MemoryManager {
  private cwd: string;
  private _hookCallback: ((event: string, data: any) => void) | null = null;

  constructor(cwd: string) {
    this.cwd = cwd;
  }

  setHookCallback(cb: (event: string, data: any) => void): void { this._hookCallback = cb; }

  /**
   * Load project instructions from the workspace root and parent directories.
   *
   * Searches for instruction files (CLAUDE.md, SUPERINFERENCE.md, .cursorrules,
   * .github/copilot-instructions.md) starting from the workspace root and
   * scanning up to MAX_PARENT_SCAN_DEPTH parent directories.
   *
   * For each directory level, only the highest-priority file found is used.
   * Files are concatenated with headers indicating their source path.
   */
  loadProjectInstructions(): string {
    const sections: string[] = [];
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;

    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const found = this.findInstructionFile(currentDir);
      if (found) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            const content = sanitizeContent(raw, found);
            const relativePath = path.relative(this.cwd, found) || path.basename(found);
            const label = depth === 0 ? relativePath : `(parent) ${relativePath}`;
            sections.push(`## ${label}\n\n${content}`);
            if (this._hookCallback) this._hookCallback('instructionsLoaded', { filePath: found, loadReason: 'startup' });
          }
        } catch {
          // File unreadable — skip silently
        }
      }

      for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        try {
          if (fs.statSync(localPath).isFile()) {
            const raw = fs.readFileSync(localPath, 'utf-8').trim();
            if (raw) {
              const content = sanitizeContent(raw, localPath);
              const relativePath = path.relative(this.cwd, localPath) || path.basename(localPath);
              const label = depth === 0 ? relativePath : `(parent) ${relativePath}`;
              sections.push(`## ${label}\n\n${content}`);
            }
          }
        } catch {
          // File doesn't exist or inaccessible — expected
        }
      }

      const parentDir = path.dirname(currentDir);
      if (parentDir === currentDir || currentDir === root) {
        break;
      }
      // Stop at git repository root to avoid loading instruction files from parent projects
      try {
        if (fs.statSync(path.join(currentDir, '.git')).isDirectory()) {
          break;
        }
      } catch {
        // No .git directory — continue walking
      }
      currentDir = parentDir;
      depth++;
    }

    return sections.join('\n\n');
  }

  /**
   * Find the highest-priority instruction file in a directory.
   * Returns the absolute path if found, null otherwise.
   */
  private findInstructionFile(dir: string): string | null {
    for (const filename of INSTRUCTION_FILES) {
      const filePath = path.join(dir, filename);
      try {
        if (fs.statSync(filePath).isFile()) {
          return filePath;
        }
      } catch {
        // File doesn't exist or inaccessible
      }
    }
    return null;
  }

  /**
   * Resolve instruction files for a specific file path by walking up from the
   * file's directory to the workspace root. Each instruction file is only
   * included once (tracked via `claimedPaths`).
   */
  resolveFileInstructions(filePath: string, claimedPaths: Set<string>): string {
    const sections: string[] = [];
    let dir = path.dirname(path.resolve(this.cwd, filePath));
    const root = path.resolve(this.cwd);

    while (dir.startsWith(root)) {
      const found = this.findInstructionFile(dir);
      if (found && !claimedPaths.has(found)) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            claimedPaths.add(found);
            const content = sanitizeContent(raw, found);
            sections.push(content);
          }
        } catch {}
      }
      const parent = path.dirname(dir);
      if (parent === dir) break;
      dir = parent;
    }

    return sections.join('\n\n');
  }

  /**
   * Load all memory entries from the .superinference/memory/ directory.
   *
   * Scans for .md files, parses their frontmatter, and returns an array of
   * MemoryEntry objects sorted by modification time (newest first).
   */
  loadMemories(): MemoryEntry[] {
    const memoryDir = this.getMemoryDir();
    let files: string[];

    try {
      files = fs.readdirSync(memoryDir).filter(f => f.endsWith('.md'));
    } catch {
      // Directory doesn't exist yet — no memories
      return [];
    }

    const MAX_MEMORY_FILES = 200;
    files = files.slice(0, MAX_MEMORY_FILES);

    const entries: MemoryEntry[] = [];

    for (const file of files) {
      const filePath = path.join(memoryDir, file);
      try {
        const raw = fs.readFileSync(filePath, 'utf-8');
        const stat = fs.statSync(filePath);
        const { frontmatter, body } = parseFrontmatter(raw);
        const sanitizedBody = sanitizeContent(body.trim(), filePath);
        const ageDays = memoryAgeDays(stat.mtimeMs);
        const freshness = memoryFreshness(ageDays);

        let content = sanitizedBody;
        if (ageDays >= STALE_MEMORY_DAYS) {
          content = `[STALE — last updated ${freshness}. Verify before acting on this.]\n\n${sanitizedBody}`;
        }

        entries.push({
          path: filePath,
          content,
          type: 'memory',
          name: frontmatter.name || path.basename(file, '.md'),
          description: frontmatter.description || undefined,
          memoryType: parseMemoryType(frontmatter.type),
          ageDays,
          freshness,
          mtimeMs: stat.mtimeMs,
        });
      } catch {
        // Skip unreadable files
      }
    }

    entries.sort((a, b) => (b.mtimeMs ?? 0) - (a.mtimeMs ?? 0));

    return entries;
  }

  /**
   * Save a memory entry to the .superinference/memory/ directory.
   *
   * Creates the memory directory if it doesn't exist. The memory is written
   * as a markdown file with YAML frontmatter containing name, description,
   * and type fields.
   *
   * @param name - Memory name (used as filename and frontmatter name field)
   * @param content - Memory body content
   * @param description - One-line summary for relevance matching
   * @param memoryType - Memory type category (defaults to 'project')
   */
  saveMemory(
    name: string,
    content: string,
    description?: string,
    memoryType?: MemoryType,
  ): void {
    const memoryDir = this.getMemoryDir();
    fs.mkdirSync(memoryDir, { recursive: true });

    const safeName = name
      .replace(/[^a-zA-Z0-9_-]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '')
      .toLowerCase();

    const filename = `${safeName}.md`;
    const filePath = path.join(memoryDir, filename);

    const type = memoryType ?? 'project';
    const desc = description ?? name;
    const frontmatter = buildFrontmatter(name, desc, type);
    const fileContent = `${frontmatter}\n${content}\n`;

    fs.writeFileSync(filePath, fileContent, 'utf-8');
  }

  /**
   * Get all memory content formatted for the system prompt.
   *
   * Combines project instructions and stored memories into a single string
   * suitable for injection into the system prompt. Returns empty string if
   * there are no instructions or memories.
   */
  getMemoryContext(): string {
    const sections: string[] = [];

    const instructions = this.loadProjectInstructions();
    if (instructions) {
      sections.push(`## Project Instructions\n\n${instructions}`);
    }

    const memories = this.loadMemories();
    if (memories.length > 0) {
      const memoryLines = memories.map(m => {
        const typeTag = m.memoryType ? `[${m.memoryType}] ` : '';
        const ageTag = m.freshness ? ` (${m.freshness})` : '';
        const header = `### ${typeTag}${m.name || 'Untitled'}${ageTag}`;
        const desc = m.description ? `> ${m.description}` : '';
        let body = m.content;
        if ((m.ageDays ?? 0) > 1) {
          body = `[Note: This memory is ${m.freshness} old. Verify against current state before acting.]\n${body}`;
        }
        const parts = [header];
        if (desc) parts.push(desc);
        parts.push('', body);
        return parts.join('\n');
      });

      sections.push(`## Stored Memories\n\n${memoryLines.join('\n\n')}`);
    }

    const teamMemories = this.loadTeamMemories();
    if (teamMemories.length > 0) {
      sections.push(`## Team Memories\n\n${teamMemories.join('\n\n')}`);
    }

    if (memories.length > 0 || teamMemories.length > 0) {
      const hasOldMemories = memories.some(m => (m.ageDays ?? 0) > 1);
      const driftCaveat = hasOldMemories
        ? `\n\n## Memory Drift Warning\nMemories may be stale. Always verify against current file contents, git log, and observed behavior before acting on recalled information.`
        : '';

      const memoryGuidance = `## Memory Guidelines
### When to save memories
- User preferences, corrections, project conventions
- NOT: code patterns derivable from files, git history, debugging recipes, ephemeral task state

### Memory freshness
- Verify memories >1 day old against current state before acting
- Trust observed reality (file contents, git log) over stale memories

### Memory types
- user: Private role/goals/knowledge
- feedback: Corrections about how to work
- project: Ongoing context not in code/git
- reference: Pointers to external systems${driftCaveat}`;
      sections.push(memoryGuidance);
    }

    return sections.join('\n\n');
  }

  /**
   * Select the most relevant memories for a given query context using BM25 scoring.
   * Returns up to `maxResults` memories sorted by relevance.
   */
  selectRelevantMemories(query: string, maxResults: number = 5): MemoryEntry[] {
    const all = this.loadMemories();
    if (all.length <= maxResults) return all;

    const queryTokens = tokenize(query);
    if (queryTokens.length === 0) return all.slice(0, maxResults);

    const N = all.length;
    const docFreq = new Map<string, number>();
    const allMemTexts = all.map(m => `${m.name || ''} ${m.description || ''} ${m.content}`);
    for (const text of allMemTexts) {
      const unique = new Set(tokenize(text));
      for (const t of unique) {
        docFreq.set(t, (docFreq.get(t) || 0) + 1);
      }
    }

    const idfMap = new Map<string, number>();
    for (const qt of queryTokens) {
      const df = docFreq.get(qt) || 0;
      idfMap.set(qt, Math.log((N - df + 0.5) / (df + 0.5) + 1));
    }

    const scored = all.map((mem, i) => ({
      mem,
      score: computeRelevanceScore(allMemTexts[i], queryTokens, idfMap),
    }));

    scored.sort((a, b) => b.score - a.score);
    return scored.slice(0, maxResults).map(s => s.mem);
  }

  /**
   * Load team memories from the shared team memory directory.
   * Team memories live in .superinference/memory/team/ and are
   * typically committed to version control for shared context.
   */
  loadTeamMemories(): string[] {
    const teamDir = path.join(this.cwd, '.superinference', 'memory', 'team');
    if (!fs.existsSync(teamDir)) return [];
    return this.loadMemoriesFromDir(teamDir);
  }

  private loadMemoriesFromDir(dir: string): string[] {
    try {
      return fs.readdirSync(dir)
        .filter(f => f.endsWith('.md'))
        .map(f => {
          const raw = fs.readFileSync(path.join(dir, f), 'utf-8');
          const { body } = parseFrontmatter(raw);
          return sanitizeContent(body.trim(), path.join(dir, f));
        })
        .filter(c => c.length > 0);
    } catch {
      return [];
    }
  }

  /**
   * Returns the absolute path to the memory directory.
   */
  getMemoryDir(): string {
    return path.join(this.cwd, MEMORY_DIR);
  }

  /**
   * Save session memory facts to a session-specific markdown file.
   *
   * Facts are stored as `.superinference/memory/session-<sessionId>.md`
   * with each fact rendered as a bullet point grouped by category.
   */
  saveSessionMemory(sessionId: string, facts: SessionFact[]): void {
    if (facts.length === 0) return;

    const memoryDir = this.getMemoryDir();
    fs.mkdirSync(memoryDir, { recursive: true });

    const safeId = sessionId.replace(/[^a-zA-Z0-9_-]/g, '_');
    const filename = `session-${safeId}.md`;
    const filePath = path.join(memoryDir, filename);

    const lines: string[] = [
      '---',
      `name: session-${safeId}`,
      `description: Auto-extracted session facts`,
      'type: project',
      '---',
      '',
    ];

    // Group facts by category
    const grouped = new Map<string, SessionFact[]>();
    for (const fact of facts) {
      const group = grouped.get(fact.category) || [];
      group.push(fact);
      grouped.set(fact.category, group);
    }

    for (const [category, categoryFacts] of grouped) {
      lines.push(`### ${category}`);
      for (const f of categoryFacts) {
        lines.push(`- ${f.fact} (confidence: ${f.confidence})`);
      }
      lines.push('');
    }

    fs.writeFileSync(filePath, lines.join('\n'), 'utf-8');
  }

  /**
   * Load session memory facts from a session-specific markdown file.
   *
   * Parses the `.superinference/memory/session-<sessionId>.md` file and
   * returns an array of SessionFact objects.
   */
  loadSessionMemory(sessionId: string): SessionFact[] {
    const safeId = sessionId.replace(/[^a-zA-Z0-9_-]/g, '_');
    const filename = `session-${safeId}.md`;
    const filePath = path.join(this.getMemoryDir(), filename);

    let raw: string;
    try {
      raw = fs.readFileSync(filePath, 'utf-8');
    } catch {
      return [];
    }

    const { body } = parseFrontmatter(raw);
    const facts: SessionFact[] = [];
    const validCategories = new Set<SessionFact['category']>([
      'decision', 'file_modification', 'user_preference', 'error_fix', 'convention',
    ]);

    let currentCategory: SessionFact['category'] | null = null;

    for (const line of body.split('\n')) {
      const trimmed = line.trim();

      // Detect category headers like "### decision"
      const headerMatch = trimmed.match(/^###\s+(\S+)/);
      if (headerMatch) {
        const cat = headerMatch[1] as SessionFact['category'];
        if (validCategories.has(cat)) {
          currentCategory = cat;
        }
        continue;
      }

      // Parse fact lines like "- some fact (confidence: 0.9)"
      if (currentCategory && trimmed.startsWith('- ')) {
        const factMatch = trimmed.match(/^-\s+(.+?)\s+\(confidence:\s+([\d.]+)\)$/);
        if (factMatch) {
          const confidence = parseFloat(factMatch[2]);
          if (!isNaN(confidence)) {
            facts.push({
              fact: factMatch[1],
              category: currentCategory,
              confidence,
            });
          }
        }
      }
    }

    return facts;
  }

  // ── Rules directory scanning ──────────────────────────────────────────

  /**
   * Scan and load instruction files from `.superinference/rules/` recursively.
   *
   * Reads all `.md` and `.txt` files, sanitizes their content, and returns
   * each file's content as a string. Files with `paths:` frontmatter are
   * included unconditionally here — use `loadRulesForFile()` for filtered loading.
   */
  async loadRulesDirectory(cwd: string): Promise<string[]> {
    const rulesDir = path.join(cwd, '.superinference', 'rules');
    const rules: string[] = [];

    try {
      const entries = await fsp.readdir(rulesDir, { withFileTypes: true, recursive: true });
      for (const entry of entries) {
        if (!entry.isFile()) continue;
        if (!entry.name.endsWith('.md') && !entry.name.endsWith('.txt')) continue;
        const parentDir = entry.parentPath ?? (entry as any).path ?? rulesDir;
        const filePath = path.join(parentDir, entry.name);
        try {
          const raw = await fsp.readFile(filePath, 'utf-8');
          const content = sanitizeContent(raw.trim(), filePath);
          if (content) {
            rules.push(content);
          }
        } catch { /* skip unreadable files */ }
      }
    } catch { /* rules dir may not exist */ }

    return rules;
  }

  /**
   * Load rules from `.superinference/rules/` that apply to a specific file.
   *
   * Rules with a `paths:` frontmatter field are only included when the
   * current file matches one of the globs. Rules without `paths:` always apply.
   */
  async loadRulesForFile(cwd: string, currentFile: string): Promise<InstructionSource[]> {
    const rulesDir = path.join(cwd, '.superinference', 'rules');
    const sources: InstructionSource[] = [];

    try {
      const entries = await fsp.readdir(rulesDir, { withFileTypes: true, recursive: true });
      for (const entry of entries) {
        if (!entry.isFile()) continue;
        if (!entry.name.endsWith('.md') && !entry.name.endsWith('.txt')) continue;
        const parentDir = entry.parentPath ?? (entry as any).path ?? rulesDir;
        const filePath = path.join(parentDir, entry.name);
        try {
          const raw = await fsp.readFile(filePath, 'utf-8');
          const { frontmatter, body } = parseFrontmatter(raw);

          // Conditional rules: skip if paths: is set and doesn't match
          if (frontmatter.paths) {
            const globs = parseFrontmatterList(frontmatter.paths);
            if (globs.length > 0 && !matchesPaths(globs, currentFile)) {
              continue;
            }
          }

          const content = sanitizeContent(body.trim(), filePath);
          if (content) {
            sources.push({ content, level: 'managed', path: filePath });
          }
        } catch { /* skip unreadable files */ }
      }
    } catch { /* rules dir may not exist */ }

    return sources;
  }

  // ── @include directive resolution ─────────────────────────────────────

  /**
   * Resolve `@include <path>` directives in instruction/rule content.
   *
   * Recursively inlines referenced files up to a maximum depth of 5.
   * Tracks seen paths to prevent circular references — a circular inclusion
   * is replaced with a `[circular reference: ...]` marker.
   *
   * @param content - The raw content potentially containing @include directives
   * @param basePath - Absolute path of the file containing the content (for relative resolution)
   * @param depth - Current recursion depth (capped at 5)
   * @param seen - Set of already-visited absolute paths for cycle detection
   */
  async resolveIncludes(
    content: string,
    basePath: string,
    depth: number = 0,
    seen: Set<string> = new Set(),
  ): Promise<string> {
    if (depth > 5) return content;

    const includePattern = /@include\s+(.+)/g;
    let result = content;
    let match;

    // Collect all matches first to avoid issues with modifying the string during iteration
    const matches: { fullMatch: string; includePath: string }[] = [];
    while ((match = includePattern.exec(content)) !== null) {
      matches.push({
        fullMatch: match[0],
        includePath: path.resolve(path.dirname(basePath), match[1].trim()),
      });
    }

    for (const { fullMatch, includePath } of matches) {
      if (seen.has(includePath)) {
        result = result.replace(fullMatch, `[circular reference: ${path.basename(includePath)}]`);
        continue;
      }
      seen.add(includePath);
      try {
        let included = await fsp.readFile(includePath, 'utf-8');
        included = sanitizeContent(included, includePath);
        included = await this.resolveIncludes(included, includePath, depth + 1, seen);
        result = result.replace(fullMatch, included);
      } catch {
        result = result.replace(fullMatch, `[include not found: ${path.basename(includePath)}]`);
      }
    }

    return result;
  }

  // ── MEMORY.md index management ────────────────────────────────────────

  /**
   * Read the MEMORY.md index file and return all index entries.
   *
   * The index file lives at `.superinference/memory/MEMORY.md` and contains
   * lines like `- [topic](filename.md) — description` that serve as a
   * human-readable and machine-parseable table of contents.
   */
  async readMemoryIndex(cwd: string): Promise<string[]> {
    const indexPath = path.join(cwd, '.superinference', 'memory', 'MEMORY.md');
    try {
      const content = await fsp.readFile(indexPath, 'utf-8');
      return content.split('\n').filter(l => l.startsWith('- ['));
    } catch {
      return [];
    }
  }

  /**
   * Add an entry to the MEMORY.md index file if it doesn't already exist.
   *
   * Creates the file if it doesn't exist. Entries are expected in the format
   * `- [topic](filename.md) — description`.
   */
  async addToMemoryIndex(cwd: string, entry: string): Promise<void> {
    const indexPath = path.join(cwd, '.superinference', 'memory', 'MEMORY.md');
    try {
      await fsp.mkdir(path.dirname(indexPath), { recursive: true });
      let content = '';
      try { content = await fsp.readFile(indexPath, 'utf-8'); } catch { /* file doesn't exist */ }
      if (!content.includes(entry)) {
        content = content.trimEnd() + '\n' + entry + '\n';
        await fsp.writeFile(indexPath, content, 'utf-8');
      }
    } catch { /* best effort */ }
  }

  /**
   * Remove an entry from the MEMORY.md index file.
   * Matches by substring — if any line contains the given text, it is removed.
   */
  async removeFromMemoryIndex(cwd: string, entrySubstring: string): Promise<void> {
    const indexPath = path.join(cwd, '.superinference', 'memory', 'MEMORY.md');
    try {
      const content = await fsp.readFile(indexPath, 'utf-8');
      const lines = content.split('\n').filter(l => !l.includes(entrySubstring));
      await fsp.writeFile(indexPath, lines.join('\n'), 'utf-8');
    } catch { /* best effort */ }
  }

  // ── Instruction source collection ─────────────────────────────────────

  /**
   * Collect all instruction sources with level annotations.
   *
   * Returns an array of `InstructionSource` objects from all three levels:
   * - `managed`: from `.superinference/rules/` directory
   * - `project`: from project instruction files (CLAUDE.md, SUPERINFERENCE.md, etc.)
   * - `user`: from `.local.md` variants and memory entries
   *
   * @param currentFile - Optional file path for conditional rule filtering
   */
  async collectInstructionSources(currentFile?: string): Promise<InstructionSource[]> {
    const sources: InstructionSource[] = [];

    // 1. Managed rules from .superinference/rules/
    if (currentFile) {
      const managed = await this.loadRulesForFile(this.cwd, currentFile);
      sources.push(...managed);
    } else {
      const rules = await this.loadRulesDirectory(this.cwd);
      for (const content of rules) {
        sources.push({ content, level: 'managed', path: path.join(this.cwd, '.superinference', 'rules') });
      }
    }

    // 2. Project-level instruction files
    let currentDir = path.resolve(this.cwd);
    const root = path.parse(currentDir).root;
    let depth = 0;

    while (depth <= MAX_PARENT_SCAN_DEPTH) {
      const found = this.findInstructionFile(currentDir);
      if (found) {
        try {
          const raw = fs.readFileSync(found, 'utf-8').trim();
          if (raw) {
            const content = sanitizeContent(raw, found);
            sources.push({ content, level: 'project', path: found });
          }
        } catch { /* skip */ }
      }

      // 3. User-level local variants
      for (const localFile of LOCAL_VARIANTS) {
        const localPath = path.join(currentDir, localFile);
        try {
          if (fs.statSync(localPath).isFile()) {
            const raw = fs.readFileSync(localPath, 'utf-8').trim();
            if (raw) {
              const content = sanitizeContent(raw, localPath);
              sources.push({ content, level: 'user', path: localPath });
            }
          }
        } catch { /* skip */ }
      }

      const parentDir = path.dirname(currentDir);
      if (parentDir === currentDir || currentDir === root) break;
      try {
        if (fs.statSync(path.join(currentDir, '.git')).isDirectory()) break;
      } catch { /* no .git — continue */ }
      currentDir = parentDir;
      depth++;
    }

    // 4. User memories
    const memories = this.loadMemories();
    for (const mem of memories) {
      sources.push({ content: mem.content, level: 'user', path: mem.path });
    }

    return sources;
  }
}
