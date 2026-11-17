import * as fs from 'fs';
import * as path from 'path';
import { randomInt } from './words';
import { stripBom } from './config';

/**
 * Graded helpfulness scale for a learned entry. The three coarse values
 * (`harmful`/`neutral`/`helpful`) are kept for backward-compatible counters and
 * filtering; the four extra graded values mirror the reference's finer
 * helpfulness granularity (unhelpful → essential) so a single "essential" tag
 * ranks a strategy above one that was merely "slightly_helpful" instead of every
 * positive tag being worth exactly the same. Every value maps to a signed weight
 * (see {@link FEEDBACK_WEIGHT}) accumulated into {@link SkillbookEntry.score}.
 */
export type SkillbookFeedback =
  | 'harmful'
  | 'unhelpful'
  | 'neutral'
  | 'slightly_helpful'
  | 'helpful'
  | 'very_helpful'
  | 'essential';

/** Signed weight each feedback grade contributes to {@link SkillbookEntry.score}. */
const FEEDBACK_WEIGHT: Record<SkillbookFeedback, number> = {
  harmful: -2,
  unhelpful: -1,
  neutral: 0,
  slightly_helpful: 1,
  helpful: 2,
  very_helpful: 3,
  essential: 4,
};

/** All feedback grades, exported so UIs can advertise the valid options. */
export const SKILLBOOK_FEEDBACK_VALUES = Object.keys(FEEDBACK_WEIGHT) as SkillbookFeedback[];

export interface SkillbookEntry {
  id: string;
  section: 'context' | 'harness';
  keywords: string[];
  issue: string;
  insight: string;
  helpfulCount: number;
  harmfulCount: number;
  neutralCount: number;
  /**
   * Cumulative weighted helpfulness from graded {@link SkillbookFeedback} tags.
   * Used to RANK entries within a section (finer than the coarse counters, which
   * still drive inclusion/exclusion filtering). Can be negative.
   */
  score: number;
  occurrences: Array<{ sessionId: string; timestamp: number; spanId?: string }>;
  createdAt: number;
  updatedAt: number;
}

export type SkillbookOperation =
  | { type: 'ADD'; entry: Omit<SkillbookEntry, 'id' | 'createdAt' | 'updatedAt' | 'helpfulCount' | 'harmfulCount' | 'neutralCount' | 'score' | 'occurrences'> }
  | { type: 'TAG'; id: string; feedback: SkillbookFeedback }
  | { type: 'UPDATE'; id: string; insight?: string; keywords?: string[] }
  | { type: 'REMOVE'; id: string };

/** Alphabet for the random suffix of a skillbook entry id (base36). */
const ID_ALPHABET = 'abcdefghijklmnopqrstuvwxyz0123456789';

/**
 * Collision-resistant entry id using a CSPRNG for the random suffix. Uses the
 * canonical unbiased {@link randomInt} helper (crypto.randomBytes-backed) rather
 * than `Math.random`, which is not cryptographically secure and biases toward
 * predictable values.
 */
function generateEntryId(now: number): string {
  let suffix = '';
  for (let i = 0; i < 6; i++) suffix += ID_ALPHABET[randomInt(ID_ALPHABET.length)];
  return `sk-${now}-${suffix}`;
}

/**
 * On-disk schema version written by {@link Skillbook.saveToDisk}. A persisted
 * file whose `version` does not match is discarded on load rather than trusted,
 * so a future incompatible schema can never silently feed stale entries into the
 * prompt. Bump this (and add a migration branch) when the shape changes.
 */
const SCHEMA_VERSION = 1;

/**
 * Prompt-budget / untrusted-content guards. The skillbook is a project-cwd file
 * (`.ami/skillbook.json`) that can be committed into any cloned repo, and its
 * `insight` text is injected verbatim into the system prompt by
 * {@link Skillbook.getContext}. Without bounds a single large or runaway-accumulated
 * entry would flood the context window (prompt-budget DoS). These mirror the
 * reference implementation's per-section and total-memory caps.
 */
const MAX_INSIGHT_LENGTH = 2000;
const MAX_ISSUE_LENGTH = 500;
const MAX_KEYWORD_LENGTH = 100;
const MAX_KEYWORDS = 20;
/** Max entries rendered per section in {@link Skillbook.getContext}. */
const MAX_CONTEXT_ENTRIES = 50;
/** Total byte budget for the string {@link Skillbook.getContext}/getCautions emit. */
const MAX_CONTEXT_BYTES = 25_000;

function clampString(value: string, max: number): string {
  return value.length > max ? value.slice(0, max) : value;
}

function sanitizeKeywords(keywords: string[]): string[] {
  return keywords
    .filter((k): k is string => typeof k === 'string')
    .slice(0, MAX_KEYWORDS)
    .map(k => clampString(k, MAX_KEYWORD_LENGTH));
}

function byteLength(text: string): number {
  return Buffer.byteLength(text, 'utf-8');
}

export class Skillbook {
  private entries: Map<string, SkillbookEntry> = new Map();
  private persistPath: string;

  constructor(cwd: string) {
    this.persistPath = path.join(cwd, '.ami', 'skillbook.json');
    this.loadFromDisk();
  }

  /**
   * Validate + normalize one raw object parsed from disk into a trusted
   * {@link SkillbookEntry}, or return null if it is malformed. The persisted file
   * is untrusted input (committable, hand-editable), so every field is checked
   * against the expected type/allowlist and over-long text is clamped — a hostile
   * or corrupt entry is dropped rather than injected into the prompt.
   */
  private static validateEntry(raw: unknown): SkillbookEntry | null {
    if (typeof raw !== 'object' || raw === null) return null;
    const e = raw as Record<string, unknown>;
    if (typeof e.id !== 'string' || e.id.length === 0) return null;
    if (e.section !== 'context' && e.section !== 'harness') return null;
    if (typeof e.insight !== 'string') return null;
    if (typeof e.issue !== 'string') return null;
    if (!Array.isArray(e.keywords)) return null;

    const toCount = (v: unknown): number =>
      typeof v === 'number' && Number.isFinite(v) && v >= 0 ? v : 0;
    const helpfulCount = toCount(e.helpfulCount);
    const harmfulCount = toCount(e.harmfulCount);
    // `score` is a signed accumulator, so it may be negative (unlike the
    // counters). A legacy entry persisted before graded feedback existed has no
    // `score`; backfill it from the coarse counters so its ranking is sensible.
    const score =
      typeof e.score === 'number' && Number.isFinite(e.score)
        ? e.score
        : helpfulCount - harmfulCount;
    const now = Date.now();
    const toTimestamp = (v: unknown): number =>
      typeof v === 'number' && Number.isFinite(v) ? v : now;

    const occurrences = Array.isArray(e.occurrences)
      ? (e.occurrences as unknown[]).filter(
          (o): o is SkillbookEntry['occurrences'][number] =>
            typeof o === 'object' && o !== null &&
            typeof (o as Record<string, unknown>).sessionId === 'string' &&
            typeof (o as Record<string, unknown>).timestamp === 'number',
        )
      : [];

    return {
      id: e.id,
      section: e.section,
      keywords: sanitizeKeywords(e.keywords as string[]),
      issue: clampString(e.issue, MAX_ISSUE_LENGTH),
      insight: clampString(e.insight, MAX_INSIGHT_LENGTH),
      helpfulCount,
      harmfulCount,
      neutralCount: toCount(e.neutralCount),
      score,
      occurrences,
      createdAt: toTimestamp(e.createdAt),
      updatedAt: toTimestamp(e.updatedAt),
    };
  }

  private loadFromDisk(): void {
    try {
      if (fs.existsSync(this.persistPath)) {
        // Strip a leading UTF-8 BOM before parsing: the skillbook file is a
        // project-cwd JSON (`.ami/skillbook.json`) that may be committed and
        // re-written by Windows editors / PowerShell 5.x (Out-File, Set-Content),
        // which prepend U+FEFF. Without this, JSON.parse throws "Unexpected
        // token" and the whole file reads as corrupt. Reuses the one canonical
        // {@link stripBom} helper rather than re-implementing it.
        const data = JSON.parse(stripBom(fs.readFileSync(this.persistPath, 'utf-8')));
        // Discard on schema mismatch so a future/incompatible format is never
        // trusted. (Add a migration branch here when SCHEMA_VERSION is bumped.)
        if (data?.version !== SCHEMA_VERSION) return;
        for (const raw of Array.isArray(data.entries) ? data.entries : []) {
          const entry = Skillbook.validateEntry(raw);
          if (entry) this.entries.set(entry.id, entry);
        }
      }
    } catch { /* corrupt file — start fresh */ }
  }

  saveToDisk(): void {
    try {
      fs.mkdirSync(path.dirname(this.persistPath), { recursive: true });
      fs.writeFileSync(this.persistPath, JSON.stringify({
        version: SCHEMA_VERSION,
        entries: [...this.entries.values()],
      }, null, 2));
    } catch { /* best-effort persistence */ }
  }

  apply(op: SkillbookOperation): SkillbookEntry | null {
    const now = Date.now();
    switch (op.type) {
      case 'ADD': {
        const id = generateEntryId(now);
        const entry: SkillbookEntry = {
          ...op.entry, id, helpfulCount: 0, harmfulCount: 0, neutralCount: 0, score: 0,
          occurrences: [], createdAt: now, updatedAt: now,
          // Clamp untrusted/unbounded author input before it is persisted or
          // ever injected into the prompt.
          keywords: sanitizeKeywords(op.entry.keywords),
          issue: clampString(op.entry.issue, MAX_ISSUE_LENGTH),
          insight: clampString(op.entry.insight, MAX_INSIGHT_LENGTH),
        };
        this.entries.set(id, entry);
        this.saveToDisk();
        return entry;
      }
      case 'TAG': {
        const entry = this.entries.get(op.id);
        if (!entry) return null;
        const weight = FEEDBACK_WEIGHT[op.feedback];
        // Coarse counters (used for include/exclude filtering) are driven by the
        // sign of the grade; the signed weight accumulates into `score` for
        // finer ranking.
        if (weight > 0) entry.helpfulCount++;
        else if (weight < 0) entry.harmfulCount++;
        else entry.neutralCount++;
        entry.score += weight;
        entry.updatedAt = now;
        this.saveToDisk();
        return entry;
      }
      case 'UPDATE': {
        const entry = this.entries.get(op.id);
        if (!entry) return null;
        if (op.insight) entry.insight = clampString(op.insight, MAX_INSIGHT_LENGTH);
        if (op.keywords) entry.keywords = sanitizeKeywords(op.keywords);
        entry.updatedAt = now;
        this.saveToDisk();
        return entry;
      }
      case 'REMOVE': {
        const entry = this.entries.get(op.id);
        this.entries.delete(op.id);
        this.saveToDisk();
        return entry ?? null;
      }
    }
  }

  getAll(): SkillbookEntry[] { return [...this.entries.values()]; }

  getBySection(section: 'context' | 'harness'): SkillbookEntry[] {
    return this.getAll().filter(e => e.section === section);
  }

  search(query: string): SkillbookEntry[] {
    const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
    return this.getAll().filter(e => {
      const text = `${e.issue} ${e.insight} ${e.keywords.join(' ')}`.toLowerCase();
      return terms.some(t => text.includes(t));
    }).sort((a, b) => b.helpfulCount - a.helpfulCount);
  }

  /**
   * Render a ranked, budget-bounded bullet list for one group of entries.
   * Entries are sorted by `rank` (desc), capped to {@link MAX_CONTEXT_ENTRIES},
   * and truncated at an entry boundary once the running output would exceed
   * {@link MAX_CONTEXT_BYTES}, with an explicit note of how many were omitted —
   * so the skillbook can never grow the prompt unboundedly.
   */
  private static renderGroup(
    entries: SkillbookEntry[],
    rank: (e: SkillbookEntry) => number,
    format: (e: SkillbookEntry) => string,
    budget: number,
  ): { text: string; bytes: number } {
    const ranked = [...entries].sort((a, b) => rank(b) - rank(a)).slice(0, MAX_CONTEXT_ENTRIES);
    const lines: string[] = [];
    let used = 0;
    let omitted = 0;
    for (const e of ranked) {
      const line = format(e);
      const cost = byteLength(line) + 1; // + newline
      if (used + cost > budget) { omitted = ranked.length - lines.length; break; }
      lines.push(line);
      used += cost;
    }
    if (omitted > 0) {
      lines.push(`- _(${omitted} more omitted to stay within the context budget)_`);
    }
    const text = lines.join('\n');
    return { text, bytes: byteLength(text) };
  }

  /**
   * Positive learned strategies to inject into the system prompt. Harmful
   * entries (harmfulCount >= helpfulCount) are intentionally excluded here —
   * see {@link Skillbook.getCautions} for the anti-pattern channel. Both the
   * `context` and `harness` classes are surfaced under distinct, self-describing
   * sub-headers so the model knows what kind of guidance it is reading, and the
   * total output is ranked + size-capped.
   */
  getContext(): string {
    const sections: Array<{ header: string; section: 'context' | 'harness' }> = [
      { header: '### Context strategies', section: 'context' },
      { header: '### Harness strategies', section: 'harness' },
    ];
    const blocks: string[] = [];
    let remaining = MAX_CONTEXT_BYTES;
    for (const { header, section } of sections) {
      const entries = this.getBySection(section).filter(e => e.helpfulCount > e.harmfulCount);
      if (entries.length === 0) continue;
      const { text, bytes } = Skillbook.renderGroup(
        entries,
        e => e.score,
        e => `- **${e.keywords.join(', ')}**: ${e.insight} (${e.helpfulCount} helpful)`,
        remaining,
      );
      if (text.length === 0) continue;
      blocks.push(`${header}\n\n${text}`);
      remaining -= bytes;
    }
    if (blocks.length === 0) return '';
    return '## Learned Strategies\n\n' + blocks.join('\n\n');
  }

  /**
   * Anti-pattern channel: surface hard-won negative learnings (entries the user
   * tagged harmful more often than helpful) so the model can be warned about
   * "what to avoid" instead of that knowledge being silently discarded. Ranked
   * by net-harm and size-capped like {@link Skillbook.getContext}.
   */
  getCautions(): string {
    const entries = this.getAll().filter(e => e.harmfulCount > e.helpfulCount);
    if (entries.length === 0) return '';
    const { text } = Skillbook.renderGroup(
      entries,
      e => -e.score,
      e => `- **${e.keywords.join(', ')}**: ${e.insight} (${e.harmfulCount} harmful)`,
      MAX_CONTEXT_BYTES,
    );
    if (text.length === 0) return '';
    return '## Learnings to Avoid\n\n' + text;
  }
}
