/**
 * One canonical, reusable grouping helper — the single home for "bucket an
 * iterable by a derived key" logic that was previously re-implemented ad hoc at
 * every call site (session-fact grouping in `memory.ts`, LSP result grouping in
 * `tools/lsp.ts`). Parity with the reference's `utils/objectGroupBy.ts`
 * (`objectGroupBy`): an index-aware `keySelector(item, index)` is called with a
 * running index, and items are accumulated in first-seen key order.
 *
 * AMI returns a `Map<K, T[]>` rather than an `Object.create(null)` record: a Map
 * is inherently immune to prototype-pollution on hostile keys (`__proto__`,
 * `constructor`) — the same safety the reference's null-prototype object buys — while
 * also preserving insertion order, supporting non-string keys, and matching the
 * `Map`-based grouping convention already used across the AMI tree.
 */
export function groupBy<T, K>(
  items: Iterable<T>,
  keySelector: (item: T, index: number) => K,
): Map<K, T[]> {
  const result = new Map<K, T[]>();
  let index = 0;
  for (const item of items) {
    const key = keySelector(item, index++);
    const bucket = result.get(key);
    if (bucket) bucket.push(item);
    else result.set(key, [item]);
  }
  return result;
}
