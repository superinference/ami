/**
 * Generic fixed-capacity circular (ring) buffer — parity with the reference's
 * `utils/CircularBuffer.ts`.
 *
 * A single reusable data structure for "keep only the most recent N items"
 * windows. {@link CircularBuffer.add} overwrites the slot at the write head and
 * advances it modulo the capacity, giving O(1) amortized insertion with NO array
 * `shift()`/`slice()` re-allocation on overflow — the backing array is allocated
 * once and reused for the life of the buffer.
 *
 * This is the ONE canonical bounded-recency window helper (No-Duplication rule):
 * code that needs "retain the last N values" should use this rather than
 * hand-rolling a weaker O(n) `push()` + `shift()`/`slice(-N)` window (more code,
 * more allocation, and no shared oldest→newest ordering helper). It backs the
 * provider-health latency/outcome windows and is available for any other rolling
 * window (tool-guardrail decision history, recent-activity previews, …).
 *
 * {@link CircularBuffer.toArray} and {@link CircularBuffer.getRecent} both return
 * items in oldest→newest order, computing the logical start as
 * `size < capacity ? 0 : head` and indexing modulo the capacity so the wrap
 * boundary is handled transparently.
 */
export class CircularBuffer<T> {
  private readonly _capacity: number;
  private readonly _slots: (T | undefined)[];
  /** Index of the slot the NEXT {@link add} writes to (and, once full, the oldest item). */
  private _head = 0;
  private _size = 0;

  /**
   * @param capacity - Maximum retained items; must be a positive integer.
   * @throws if `capacity` is not a positive integer (a zero/negative/NaN window
   *   is a programming error, not a silently-tolerated empty buffer).
   */
  constructor(capacity: number) {
    if (!Number.isInteger(capacity) || capacity <= 0) {
      throw new RangeError(`CircularBuffer capacity must be a positive integer, got ${capacity}`);
    }
    this._capacity = capacity;
    this._slots = new Array<T | undefined>(capacity);
  }

  /** Maximum number of items the buffer retains. */
  get capacity(): number {
    return this._capacity;
  }

  /** Current number of retained items (0…capacity). */
  get size(): number {
    return this._size;
  }

  /** Whether the buffer holds no items. */
  get isEmpty(): boolean {
    return this._size === 0;
  }

  /**
   * Append `item`, evicting the oldest item in O(1) when the buffer is full.
   * @returns `this` for chaining.
   */
  add(item: T): this {
    this._slots[this._head] = item;
    this._head = (this._head + 1) % this._capacity;
    if (this._size < this._capacity) this._size++;
    return this;
  }

  /**
   * Append every item in `items` (oldest→newest), evicting as needed — parity
   * with the reference's `addAll`. Equivalent to calling {@link add} per item;
   * when `items.length` exceeds {@link capacity} only the trailing `capacity`
   * survive, exactly as sequential adds would.
   * @returns `this` for chaining.
   */
  addAll(items: Iterable<T>): this {
    for (const item of items) this.add(item);
    return this;
  }

  /** Items in oldest→newest order (a fresh array; safe to mutate/sort in place). */
  toArray(): T[] {
    const out: T[] = new Array<T>(this._size);
    // When not yet full the first write is at index 0; once full the oldest item
    // sits at the write head (the slot about to be overwritten next).
    const start = this._size < this._capacity ? 0 : this._head;
    for (let i = 0; i < this._size; i++) {
      out[i] = this._slots[(start + i) % this._capacity] as T;
    }
    return out;
  }

  /**
   * The trailing `count` items in oldest→newest order (correct across the wrap
   * boundary). A `count` ≥ {@link size} returns the whole buffer; a `count` ≤ 0
   * returns an empty array.
   */
  getRecent(count: number): T[] {
    if (count <= 0) return [];
    const take = Math.min(count, this._size);
    const all = this.toArray();
    return all.slice(this._size - take);
  }

  /** The most recently added item, or `undefined` when empty. */
  get last(): T | undefined {
    if (this._size === 0) return undefined;
    return this._slots[(this._head - 1 + this._capacity) % this._capacity];
  }

  /** Drop every retained item (capacity is preserved). */
  clear(): void {
    this._slots.fill(undefined);
    this._head = 0;
    this._size = 0;
  }
}
