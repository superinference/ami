// Split from memory.ts by scripts/split-module.mjs (move-only; verified by scripts/verify-split.mjs).
import * as fs from 'fs';
import * as path from 'path';
import { log as coreLog } from './logger';
import { MEMORY_INDEX_FILENAME, MEMORY_TYPES, redactLogPath } from './memory-memory-diagnostic';
import type { MemoryEntry, MemoryHeader, MemoryRelevanceConfig, MemoryType } from './memory-memory-diagnostic';
import { DEFAULT_MEMORY_RELEVANCE, logUnexpectedReadError } from './memory-truncate-entrypoint-content';
import { parseInlineFlowSequence, stripSurroundingQuotes } from './memory-strip-surrounding-quotes';

export function parseFrontmatter(raw: string, sourcePath?: string): {
  frontmatter: Record<string, string>;
  body: string;
} {
  const rawLines = raw.split('\n');
  // Allow leading blank lines before the opener (mirrors the previous trimStart).
  let openIdx = 0;
  while (openIdx < rawLines.length && rawLines[openIdx].trim() === '') openIdx++;
  // Opener must be a line that is EXACTLY `---` (trailing whitespace / CR ok).
  if (openIdx >= rawLines.length || rawLines[openIdx].replace(/\r$/, '').trim() !== '---') {
    return { frontmatter: {}, body: raw };
  }

  // Closer: the first SUBSEQUENT line that is exactly `---` (line-anchored).
  let closeIdx = -1;
  for (let i = openIdx + 1; i < rawLines.length; i++) {
    if (rawLines[i].replace(/\r$/, '').trim() === '---') { closeIdx = i; break; }
  }
  if (closeIdx === -1) {
    // Opened but never closed → malformed. Surface it (named) rather than silently
    // treating the whole file as a frontmatter-less body.
    coreLog(
      'memory',
      `Failed to parse frontmatter${sourcePath ? ` in ${redactLogPath(sourcePath)}` : ''}: unterminated '---' block`,
      undefined,
      'warn',
    );
    return { frontmatter: {}, body: raw };
  }

  const fmBlock = rawLines.slice(openIdx + 1, closeIdx).join('\n').trim();
  const body = rawLines.slice(closeIdx + 1).join('\n').replace(/^\r?\n/, '');

  const frontmatter: Record<string, string> = {};
  const lines = fmBlock.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const colonIdx = line.indexOf(':');
    if (colonIdx === -1) {
      // A line that is neither a `key: value` pair nor an empty/comment line is
      // malformed YAML. AMI previously skipped it SILENTLY, so a typo'd header
      // (e.g. a bare `description` with no colon) vanished with no signal. Surface
      // it (named) rather than dropping it quietly — parity with the reference's
      // yaml.ts error surfacing on malformed input. Blank lines, `# comments` and
      // stray `- list items` (which the block-sequence loop consumes) are benign.
      const probe = line.trim();
      if (probe !== '' && !probe.startsWith('#') && !/^-(\s|$)/.test(probe)) {
        coreLog(
          'memory',
          `Failed to parse frontmatter${sourcePath ? ` in ${redactLogPath(sourcePath)}` : ''}: malformed line (no key) '${probe.slice(0, 80)}'`,
          undefined,
          'warn',
        );
      }
      continue;
    }
    const key = line.slice(0, colonIdx).trim();
    if (!key) continue;
    let value = line.slice(colonIdx + 1).trim();

    // YAML block scalars: `key: >` (folded → join with spaces) or `key: |`
    // (literal → keep newlines), with optional chomping indicator (-/+). The
    // value is the subsequent more-indented (or blank) lines. Without this a
    // folded description collapses to a bare ">" sentinel.
    const block = value.match(/^([>|])[-+]?$/);
    if (block) {
      const folded = block[1] === '>';
      const collected: string[] = [];
      while (i + 1 < lines.length && (lines[i + 1].trim() === '' || /^\s/.test(lines[i + 1]))) {
        i++;
        collected.push(lines[i].trim());
      }
      value = folded
        ? collected.join(' ').replace(/\s+/g, ' ').trim()
        : collected.join('\n').trim();
    }

    // YAML block SEQUENCE: `key:` on its own line followed by indented `- item`
    // entries (parity with the reference block-list frontmatter values such as
    // `allowed-tools:` / `arguments:`). Collected into a comma-joined scalar so
    // the existing {@link parseFrontmatterList} consumer yields the items. Only
    // triggers when the inline value is empty (a `key: value` scalar wins).
    if (value === '') {
      const items: string[] = [];
      while (i + 1 < lines.length && /^\s+-\s+/.test(lines[i + 1])) {
        i++;
        items.push(lines[i].replace(/^\s+-\s+/, '').trim());
      }
      if (items.length > 0) value = items.join(', ');
    }

    // Inline YAML FLOW SEQUENCE: `key: [a, b, "c, d"]` (parity with a real YAML
    // parser's flow syntax — AMI's hand-rolled parser previously understood only
    // block `- item` sequences, so `paths: [src/*.ts, lib/*.ts]` was read as the
    // literal string `[src/*.ts, lib/*.ts]`). A well-formed `[...]` is normalized
    // into a comma-joined scalar the existing parseFrontmatterList consumer reads;
    // an UNTERMINATED `[` is malformed and surfaced (named) via the diagnostic.
    if (value.startsWith('[')) {
      const seq = parseInlineFlowSequence(value);
      if (seq !== null) {
        value = seq.join(', ');
      } else {
        coreLog(
          'memory',
          `Failed to parse frontmatter${sourcePath ? ` in ${redactLogPath(sourcePath)}` : ''}: unterminated flow sequence for key '${key}'`,
          undefined,
          'warn',
        );
      }
    } else {
      // Scalar: strip a single layer of matching surrounding quotes (a real YAML
      // parser unquotes scalars; AMI previously kept them), so a value serialized
      // quoted to defeat YAML injection (sanitizeFrontmatterValue) round-trips.
      value = stripSurroundingQuotes(value);
    }

    frontmatter[key] = value;
  }

  return { frontmatter, body };
}

/**
 * Validate a raw string as a MemoryType. Returns undefined for invalid values.
 */
export function parseMemoryType(raw: unknown): MemoryType | undefined {
  if (typeof raw !== 'string') return undefined;
  return MEMORY_TYPES.find(t => t === raw);
}

/**
 * Collapse a value destined for a single-line YAML frontmatter field so it cannot
 * inject new keys or close the block early. Newlines (the only way to add a YAML
 * line) are flattened to spaces, and a leading char that YAML reads specially is
 * neutralized — so a crafted name/description cannot smuggle `type:`/`---` lines
 * into the serialized metadata. (YAML frontmatter injection defense)
 */
export function sanitizeFrontmatterValue(value: string): string {
  let v = value.replace(/\s+/g, ' ').trim();
  // A value opening with a YAML structural/indicator char is quoted so it is read
  // as a plain scalar (not a block/alias/anchor/tag/flow collection).
  if (/^[-?:,[\]{}#&*!|>'"%@`]/.test(v)) {
    v = `"${v.replace(/"/g, '\\"')}"`;
  }
  return v;
}

/**
 * Generate frontmatter markdown for a memory file. Name/description are
 * injection-sanitized so a hostile value cannot forge extra YAML keys.
 */
export function buildFrontmatter(
  name: string,
  description: string,
  memoryType: MemoryType,
): string {
  const modified = new Date().toISOString();
  return `---\nname: ${sanitizeFrontmatterValue(name)}\ndescription: ${sanitizeFrontmatterValue(description)}\ntype: ${memoryType}\nmodified: ${modified}\n---\n`;
}

/**
 * Slugify a memory name into its on-disk file-stem. Lower-cased, non-alnum
 * collapsed to a single `_`, leading/trailing `_` trimmed. Shared by
 * {@link MemoryManager.saveMemory} (which derives the write filename) and
 * {@link MemoryManager.deleteMemory} (which resolves which file to remove) so the
 * two never drift into subtly different normalisations (No-Duplication Rule).
 */
export function memoryFileSlug(name: string): string {
  return name
    .replace(/[^a-zA-Z0-9_-]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .toLowerCase();
}

/**
 * Resolve a non-clobbering memory file path. Two DISTINCT memory names can
 * normalize to the same `safeName` (e.g. `Foo Bar` and `Foo:Bar` both →
 * `foo_bar`), and {@link MemoryManager.saveMemory} overwrites by filename — so
 * without a guard, saving the second silently destroys the first. Overwriting is
 * correct ONLY when this is an UPDATE of the same memory (the existing file's
 * frontmatter `name` matches `incomingName`, case-insensitively); otherwise a
 * numeric suffix (`foo_bar-2.md`, `-3`, …) is chosen so the unrelated memory is
 * preserved. Mirrors the reference's non-destructive (wx + EEXIST) memory write.
 */
export function resolveNonClobberingPath(dir: string, filename: string, incomingName: string): string {
  // Normalize the same way the on-disk name is serialized (whitespace-collapse,
  // see sanitizeFrontmatterValue) + case-fold, so an UPDATE with the same memory
  // name reliably overwrites in place even when the name carries collapsible
  // whitespace or was quoted on write and unquoted on read.
  const norm = (s: string): string => s.replace(/\s+/g, ' ').trim().toLowerCase();
  const base = filename.replace(/\.md$/, '');
  const target = norm(incomingName);
  let candidate = path.join(dir, filename);
  let n = 1;
  for (;;) {
    let existing: string;
    try {
      existing = fs.readFileSync(candidate, 'utf-8');
    } catch (err) {
      // Not present (ENOENT) → free to use. Any other read error is logged (so an
      // inaccessible-but-present file is not silently clobbered) and still used,
      // preserving the pre-guard write behavior rather than failing the save.
      logUnexpectedReadError(err, candidate);
      return candidate;
    }
    const { frontmatter } = parseFrontmatter(existing, candidate);
    if (norm(frontmatter.name || '') === target) return candidate;
    n++;
    candidate = path.join(dir, `${base}-${n}.md`);
  }
}

/**
 * Pure index-append rule shared by every MEMORY.md writer (the two-step
 * {@link MemoryManager.saveMemory} and {@link MemoryManager.addToMemoryIndex}).
 * Given the existing index text and a one-line pointer entry, returns the new
 * index content — or `null` when the entry is already present (dedup: never
 * double-write the same pointer). Keeping this as a single canonical rule
 * prevents the two writers from drifting into two subtly different append
 * behaviors.
 */
export function computeIndexAppend(existing: string, entry: string): string | null {
  const clamped = clampIndexEntry(entry);
  if (existing.includes(clamped)) return null;
  // Near-duplicate guard (parity: the reference's consolidation prompt resolves drift by
  // "merging new signal into existing topic files rather than creating
  // near-duplicates"). The index is one pointer PER topic file: if a row already
  // points at the same target file — even under a different title or hook — adding
  // a second row would fragment that topic across near-duplicate pointers. So we
  // skip the append and let the content merge happen at the topic file instead of
  // the index. (Exact-substring dedup above handles the identical-entry case.)
  const target = indexEntryTarget(clamped);
  if (target !== null) {
    for (const line of existing.split('\n')) {
      if (indexEntryTarget(line) === target) return null;
    }
  }
  return existing.trimEnd() + '\n' + clamped + '\n';
}

/**
 * Extract the markdown link TARGET from a one-line MEMORY.md index pointer
 * (`- [Title](target.md) — hook` → `target.md`), or `null` when the line carries
 * no `](…)` link. Used by {@link computeIndexAppend} to detect near-duplicate
 * pointers to the same topic file (per-line parse, never a loose substring match
 * that a hook mentioning a filename could spoof).
 */
export function indexEntryTarget(entry: string): string | null {
  const m = /\]\(([^)]+)\)/.exec(entry);
  return m ? m[1].trim() : null;
}

/**
 * Recommended maximum length of a single MEMORY.md index pointer line. Parity
 * with the reference's consolidation index rule ("if an index line is over ~200
 * chars, it's carrying content that belongs in the topic file"). The whole-index
 * line/byte caps still apply on load ({@link truncateEntrypointContent}).
 */
export const MAX_INDEX_ENTRY_CHARS = 200;

/**
 * Demote an over-long index pointer: a MEMORY.md entry longer than
 * {@link MAX_INDEX_ENTRY_CHARS} is carrying body content that belongs in the
 * topic file, so it is truncated (with an ellipsis) on write rather than bloating
 * the always-loaded index. Shorter entries pass through unchanged. Applied by
 * {@link computeIndexAppend} so every index writer enforces the same per-entry
 * bound (resource-exhaustion defense on untrusted memory writes).
 */
export function clampIndexEntry(entry: string): string {
  if (entry.length <= MAX_INDEX_ENTRY_CHARS) return entry;
  return entry.slice(0, MAX_INDEX_ENTRY_CHARS - 1).trimEnd() + '…';
}

/**
 * Shared file-filter for a header scan: a `.md` file that is neither the
 * `MEMORY.md` index (a table of contents, not a memory body) nor inside the
 * `team/` subtree (loaded separately). Single-sourced so the sync
 * {@link MemoryManager.scanMemoryHeaders} and async
 * {@link MemoryManager.scanMemoryHeadersAsync} filter identically.
 */
export function isMemoryHeaderFile(f: unknown): f is string {
  return (
    typeof f === 'string' &&
    f.endsWith('.md') &&
    path.basename(f) !== MEMORY_INDEX_FILENAME &&
    f.split(/[\\/]/)[0] !== 'team'
  );
}

/**
 * Build a {@link MemoryHeader} from a bounded frontmatter `head` plus the
 * stat-derived `mtimeMs`/`bytes`. Single-sourced so the sync and async scans
 * construct headers identically (no duplicated frontmatter/name/type logic).
 */
export function buildMemoryHeader(
  filePath: string,
  relativePath: string,
  head: string,
  mtimeMs: number,
  bytes: number,
): MemoryHeader {
  const { frontmatter } = parseFrontmatter(head, filePath);
  return {
    path: filePath,
    name: frontmatter.name || path.basename(relativePath, '.md'),
    description: frontmatter.description || undefined,
    memoryType: parseMemoryType(frontmatter.type),
    mtimeMs,
    bytes,
  };
}

/**
 * Aggregate file-count ceiling for the shared TEAM memory store. Team memories are
 * committed to version control and contributed by everyone in the project, so —
 * unlike the private store — there is no per-author limit on how many accumulate.
 * Without an aggregate cap a repo carrying many team-memory files could stuff the
 * context unbounded (each body is already {@link MAX_MEMORY_FILE_BYTES}-capped, but
 * count was not). Parity with the reference's memdir hard caps on untrusted memory
 * content reaching the prompt; mirrors the primary store's 200-file ceiling in
 * {@link MemoryManager.loadMemories}. */
export const MAX_TEAM_MEMORY_FILES = 200;

export function tokenize(text: string): string[] {
  return text.toLowerCase()
    .replace(/[^a-z0-9\s_-]/g, ' ')
    .split(/\s+/)
    .filter(t => t.length > 2);
}

/**
 * Signals that a `reference`-type memory is a WARNING/gotcha/known-issue rather
 * than plain usage/API documentation. When a tool is in active use, its usage
 * reference is noise (the conversation already demonstrates it) — but warnings
 * matter MOST precisely then, so a reference memory matching this is never
 * suppressed. Mirrors the reference's findRelevantMemories recentTools rule.
 */
export const TOOL_WARNING_SIGNAL_RE =
  /\b(warn(?:ing)?s?|gotchas?|caveats?|pitfalls?|cautions?|bugs?|broken|fails?|failure|limitations?|avoid|deprecat\w*|known[ -]issues?|do ?n['’]?t)\b/i;

/**
 * Whether a memory is a pure usage/API reference for a tool that is already in
 * active use, and should therefore be suppressed from relevance selection. Only
 * `reference`-type memories qualify; a memory carrying a warning/gotcha/known-issue
 * signal is preserved even for an active tool (that is exactly when it matters).
 */
export function isSuppressedToolReference(mem: MemoryEntry, recentTools: readonly string[]): boolean {
  if (mem.memoryType !== 'reference') return false;
  const hay = `${mem.name || ''} ${mem.description || ''} ${mem.content}`.toLowerCase();
  const mentionsRecentTool = recentTools.some(t => {
    const name = (t || '').trim().toLowerCase();
    return name.length > 0 && hay.includes(name);
  });
  if (!mentionsRecentTool) return false;
  // Keep warnings/gotchas/known-issues about an active tool.
  return !TOOL_WARNING_SIGNAL_RE.test(hay);
}

export function computeRelevanceScore(
  memTokens: string[],
  queryTokens: string[],
  idfMap: Map<string, number>,
  cfg: MemoryRelevanceConfig = DEFAULT_MEMORY_RELEVANCE,
): number {
  if (memTokens.length === 0) return 0;

  const tf = new Map<string, number>();
  for (const t of memTokens) {
    tf.set(t, (tf.get(t) || 0) + 1);
  }

  let score = 0;
  const { k1, b, avgDl } = cfg;

  for (const qt of queryTokens) {
    const freq = tf.get(qt) || 0;
    if (freq === 0) continue;
    const idf = idfMap.get(qt) || 1;
    const tfNorm = (freq * (k1 + 1)) / (freq + k1 * (1 - b + b * memTokens.length / avgDl));
    score += idf * tfNorm;
  }

  return score;
}

// ---------------------------------------------------------------------------
// Per-agent persistent memory (parity: the reference AgentTool/agentMemory.ts).
// A spawned agent that declares a `memory` scope in its definition gets its OWN
// file-based memory store, namespaced by agent type and separate from the global
// auto-memory. Scopes: `user` (cross-project, global config dir), `project`
// (committed to VCS under <repo>/.ami/agent-memory/), `local` (project+machine,
// NOT checked in — <repo>/.ami/agent-memory-local/, redirectable to a mount).
// ---------------------------------------------------------------------------

/**
 * Environment variable redirecting LOCAL-scope agent memory to an external mount
 * (parity: the reference's `CLAUDE_CODE_REMOTE_MEMORY_DIR`). When set, local agent
 * memory persists to `<dir>/projects/<project-slug>/agent-memory-local/<agentType>/`
 * — namespaced per project so one mount can serve many repos — instead of the
 * in-repo `.ami/agent-memory-local/`. Only local scope is redirected; `user` and
 * `project` scopes keep their fixed homes.
 */
export const AGENT_REMOTE_MEMORY_DIR_ENV = 'AMI_REMOTE_MEMORY_DIR';

export const AGENT_MEMORY_DIRNAME = 'agent-memory';

export const AGENT_MEMORY_LOCAL_DIRNAME = 'agent-memory-local';

export const AGENT_MEMORY_DISPLAY_NAME = 'Persistent Agent Memory';
