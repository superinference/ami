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

/**
 * A learned strategy older than this (in days, measured from `updatedAt`) is
 * surfaced with a staleness caveat in {@link Skillbook.getContext}. Point-in-time
 * observations — especially file:line citations — go stale as code changes, and a
 * confident-sounding old insight asserted as current fact is worse than none. The
 * caveat tells the model to verify before relying on it rather than silently
 * presenting a fresh and a months-old strategy with identical authority.
 */
const STALE_AFTER_DAYS = 30;
const MS_PER_DAY = 86_400_000;

/** Whole days elapsed since `updatedAt`; clamps future timestamps (clock skew) to 0. */
function ageDays(updatedAt: number): number {
  return Math.max(0, Math.floor((Date.now() - updatedAt) / MS_PER_DAY));
}

/**
 * Staleness caveat appended to a {@link Skillbook.getContext} bullet for entries
 * older than {@link STALE_AFTER_DAYS}; `''` for fresh entries (a caveat there is
 * noise). Kept short because it is injected once per stale entry into the prompt.
 */
function freshnessNote(updatedAt: number): string {
  const d = ageDays(updatedAt);
  return d > STALE_AFTER_DAYS
    ? ` _(learned ${d} days ago — may be outdated, verify before relying on it)_`
    : '';
}

export class Skillbook {
  private entries: Map<string, SkillbookEntry> = new Map();
  private persistPath: string;
  /**
   * Ids this session has deliberately REMOVE'd. Tracked so the concurrency-safe
   * merge in {@link Skillbook.saveToDisk} can preserve entries another session
   * added (present on disk, absent in memory) WITHOUT resurrecting the ones this
   * session intentionally deleted — the merge cannot otherwise tell the two apart.
   */
  private removedIds: Set<string> = new Set();

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

  /**
   * Read, validate, and return the current on-disk entries as a Map. Returns an
   * empty Map when the file is absent, unparseable, or a non-matching schema
   * version — in every "can't trust it" case the caller starts/merges from empty
   * rather than throwing. Shared by {@link Skillbook.loadFromDisk} (initial load)
   * and {@link Skillbook.saveToDisk} (merge-before-write) so there is one
   * canonical read+validate path.
   */
  private readDiskEntries(): Map<string, SkillbookEntry> {
    const out = new Map<string, SkillbookEntry>();
    try {
      if (!fs.existsSync(this.persistPath)) return out;
      // Strip a leading UTF-8 BOM before parsing: the skillbook file is a
      // project-cwd JSON (`.ami/skillbook.json`) that may be committed and
      // re-written by Windows editors / PowerShell 5.x (Out-File, Set-Content),
      // which prepend U+FEFF. Without this, JSON.parse throws "Unexpected
      // token" and the whole file reads as corrupt. Reuses the one canonical
      // {@link stripBom} helper rather than re-implementing it.
      const data = JSON.parse(stripBom(fs.readFileSync(this.persistPath, 'utf-8')));
      // Discard on schema mismatch so a future/incompatible format is never
      // trusted. (Add a migration branch here when SCHEMA_VERSION is bumped.)
      if (data?.version !== SCHEMA_VERSION) return out;
      for (const raw of Array.isArray(data.entries) ? data.entries : []) {
        const entry = Skillbook.validateEntry(raw);
        if (entry) out.set(entry.id, entry);
      }
    } catch { /* corrupt file — treat as empty */ }
    return out;
  }

  private loadFromDisk(): void {
    this.entries = this.readDiskEntries();
  }

  /**
   * Merge this session's in-memory entries over whatever is currently on disk,
   * so concurrent sessions sharing one `.ami/skillbook.json` (a normal case — the
   * path is cwd-scoped) don't clobber each other. An entry another session added
   * (on disk, not in memory) is preserved; for an id in both, the more recently
   * updated copy wins; ids this session REMOVE'd are dropped. Adopted back into
   * {@link Skillbook.entries} so the live session also gains peers' learnings.
   */
  private mergeWithDisk(): Map<string, SkillbookEntry> {
    const merged = this.readDiskEntries();
    for (const [id, entry] of this.entries) {
      const existing = merged.get(id);
      if (!existing || entry.updatedAt >= existing.updatedAt) merged.set(id, entry);
    }
    for (const id of this.removedIds) merged.delete(id);
    return merged;
  }

  saveToDisk(): void {
    const dir = path.dirname(this.persistPath);
    // Temp sibling in the SAME directory so the rename is atomic on one
    // filesystem. Randomised + pid-tagged to avoid collisions between concurrent
    // writers racing on the same book.
    const tmpPath = `${this.persistPath}.${process.pid}.${randomInt(1_000_000_000)}.tmp`;
    try {
      // 0o700: the directory may hold per-user captured context; keep it owner-only.
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      const merged = this.mergeWithDisk();
      this.entries = merged;
      this.removedIds.clear();
      // Atomic replace: write the full file to a temp sibling, then rename over
      // the target. A crash mid-write leaves the (ignored) temp file, never a
      // truncated skillbook.json that loadFromDisk would silently discard whole.
      // 0o600: the file stores learned issue/insight text + session ids harvested
      // from the user's work — keep it owner read/write only, not world-readable.
      fs.writeFileSync(tmpPath, JSON.stringify({
        version: SCHEMA_VERSION,
        entries: [...merged.values()],
      }, null, 2), { mode: 0o600 });
      fs.renameSync(tmpPath, this.persistPath);
    } catch {
      // Best-effort persistence; clean up a half-written temp file so it can't
      // accumulate across failed saves.
      try { fs.rmSync(tmpPath, { force: true }); } catch { /* ignore */ }
    }
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
        // Tombstone the id so the merge-on-write in saveToDisk does not resurrect
        // it from a concurrent session's on-disk copy.
        this.removedIds.add(op.id);
        this.saveToDisk();
        return entry ?? null;
      }
    }
  }

  getAll(): SkillbookEntry[] { return [...this.entries.values()]; }

  getBySection(section: 'context' | 'harness'): SkillbookEntry[] {
    return this.getAll().filter(e => e.section === section);
  }

  /**
   * Relevance-ranked lookup. An entry's rank is the number of DISTINCT query
   * terms it matches (term coverage), not a popularity poll — the best-matching
   * entry surfaces first even if a different entry has more upvotes. `helpfulCount`
   * then `score` break ties among equally-covering entries. A noise filter drops a
   * single incidental term match from a multi-term query (matching only "test" out
   * of "flaky test timeout" is almost always a false positive), while a
   * single-term query still matches on that one term.
   */
  search(query: string): SkillbookEntry[] {
    const terms = [...new Set(query.toLowerCase().split(/\s+/).filter(Boolean))];
    if (terms.length === 0) return [];
    const scored: Array<{ entry: SkillbookEntry; matched: number }> = [];
    for (const entry of this.getAll()) {
      const text = `${entry.issue} ${entry.insight} ${entry.keywords.join(' ')}`.toLowerCase();
      const matched = terms.filter(t => text.includes(t)).length;
      if (matched === 0) continue;
      // Noise filter: a lone incidental term match among a multi-term query is
      // dropped; a full single-term query (matched === terms.length === 1) stays.
      if (matched === 1 && terms.length > 1) continue;
      scored.push({ entry, matched });
    }
    return scored
      .sort((a, b) =>
        b.matched - a.matched ||
        b.entry.helpfulCount - a.entry.helpfulCount ||
        b.entry.score - a.entry.score)
      .map(s => s.entry);
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
        e => `- **${e.keywords.join(', ')}**: ${e.insight} (${e.helpfulCount} helpful)${freshnessNote(e.updatedAt)}`,
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
